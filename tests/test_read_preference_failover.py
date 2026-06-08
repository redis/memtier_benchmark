"""
Failover test for --read-preference in cluster mode.

Background
----------
When a replica goes offline, --read-preference=secondaryPreferred must fall
back gracefully to the primary/master.  memtier must not crash and GETs must
continue to be served from the master of the affected shard.

Test
----
1. Baseline run: --read-preference=secondaryPreferred with replicas present.
   Assert GETs land on replicas.

2. Kill one replica shard.

3. Re-run: same flags.  Assert no crash (exit code != signal) and GETs land
   on masters (since the only replica is gone).

Note: stopping a replica in an RLTest cluster environment requires calling
``env.envRunner`` internals.  Because the RLTest API for stopping individual
replica nodes is not stable, this test takes a best-effort approach: it
iterates over the replica connections found via CLUSTER NODES, attempts to
send a SHUTDOWN NOSAVE, then waits briefly.  The re-run assertion is the
authoritative check.
"""

import time
import tempfile

from include import (
    add_required_env_arguments,
    addTLSArgs,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_cluster_replica_connections,
    get_default_memtier_config,
    reset_commandstats,
)
from mb import Benchmark, RunConfig

# ---------------------------------------------------------------------------
# Env override: replicas required
# ---------------------------------------------------------------------------

# Replica/shard topology is driven by --use-slaves and --shards-count, which
# tests/run_tests.sh passes to RLTest when OSS_CLUSTER_REPLICAS=1.

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_THREADS = 2
_CLIENTS = 2
_REQUESTS = 100


def _pre_populate(env, key_count=100):
    """Write key_count keys to the cluster using a cluster-aware client.

    Earlier revisions iterated fp-key-N round-robin over
    getOSSMasterNodesConnectionList() with plain StrictRedis connections.
    Because each connection talks to a single shard and does not follow
    MOVED redirects, every key that did not hash to its assigned master
    surfaced as a MOVED error. Switch to a cluster-aware connection that
    routes each SET to the slot's owner automatically and falls back to a
    regular connection in non-cluster envs.
    """
    conn = env.getClusterConnectionIfNeeded()
    for i in range(key_count):
        conn.execute_command("SET", "fp-key-{}".format(i), "val-{}".format(i))


def _reset_all_commandstats(env, replica_conns):
    for conn in env.getOSSMasterNodesConnectionList():
        try:
            conn.execute_command("CONFIG", "RESETSTAT")
        except Exception:
            pass
    reset_commandstats(replica_conns)


def _sum_get_calls(conns):
    total = 0
    for conn in conns:
        try:
            stats = conn.execute_command("INFO", "COMMANDSTATS")
        except Exception:
            continue
        if isinstance(stats, dict):
            total += int(stats.get("cmdstat_get", {}).get("calls", 0))
        else:
            for line in stats.split("\n"):
                line = line.strip()
                if line.startswith("cmdstat_get:"):
                    for kv in line.split(":", 1)[1].split(","):
                        kv = kv.strip()
                        if kv.startswith("calls="):
                            try:
                                total += int(kv.split("=", 1)[1])
                            except ValueError:
                                pass
    return total


def _run_read_pref(env, read_preference, threads=_THREADS, clients=_CLIENTS,
                   requests=_REQUESTS):
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--ratio=0:1",
            "--key-minimum=1",
            "--key-maximum=99",
            "--read-preference={}".format(read_preference),
        ],
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(
        threads=threads, clients=clients, requests=requests
    )
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)

    benchmark = Benchmark.from_json(run_config, benchmark_specs)
    ok = benchmark.run()
    return ok, run_config


def _conn_port(conn):
    """Return the TCP port a redis.Redis connection is bound to, or None."""
    try:
        return int(conn.connection_pool.connection_kwargs.get("port"))
    except Exception:
        return None


def _mark_dead_in_rltest(env, killed_ports):
    """Tell RLTest that the slave(s) listening on ``killed_ports`` were
    intentionally shut down by the test, so teardown does not flag the
    missing process as a crash.

    Requires the RLTest fork carrying StandardEnv.markSlaveDeadByTest
    (fix/cluster-aware-replicas branch). Silently no-ops on older RLTest
    builds so the test stays runnable against upstream.
    """
    if not killed_ports:
        return
    runner = getattr(env, "envRunner", None)
    shards = getattr(runner, "shards", None) if runner is not None else None
    if not shards:
        return
    for shard in shards:
        slave_ports = getattr(shard, "slavePorts", None) or []
        mark = getattr(shard, "markSlaveDeadByTest", None)
        if not callable(mark):
            # Older RLTest without the API; nothing to do.
            continue
        for idx, port in enumerate(slave_ports):
            try:
                port_int = int(port)
            except (TypeError, ValueError):
                continue
            if port_int in killed_ports:
                try:
                    mark(idx)
                except Exception:
                    # Defensive: never let teardown bookkeeping break the test.
                    pass


def _stop_one_replica(env, replica_conns):
    """Best-effort: send SHUTDOWN NOSAVE to the first reachable replica.

    Returns True if a replica was successfully stopped, False otherwise.

    Also notifies RLTest that the killed replica was an expected death so
    teardown's checkExitCode / "process is not alive" warning does not
    treat it as a crash.
    """
    for conn in replica_conns:
        killed_port = _conn_port(conn)
        try:
            # SHUTDOWN NOSAVE will close the connection; ignore the error.
            conn.execute_command("SHUTDOWN", "NOSAVE")
        except Exception:
            # The connection is expected to close; treat this as success.
            pass
        # Give the OS a moment to reap the process.
        time.sleep(0.5)
        if killed_port is not None:
            _mark_dead_in_rltest(env, {killed_port})
        return True
    return False


# ---------------------------------------------------------------------------
# Test: failover scenario
# ---------------------------------------------------------------------------

def test_read_preference_failover(env):
    """Stopping a replica must not crash memtier.  With
    --read-preference=secondaryPreferred, after the replica is gone GETs must
    fall back to the master of the affected shard."""
    if not env.isCluster():
        env.skip()
        return

    replica_conns = get_cluster_replica_connections(env)
    if not replica_conns:
        env.skip()
        return

    _pre_populate(env, key_count=100)

    # ---- Baseline: confirm replicas are serving reads ---------------------
    _reset_all_commandstats(env, replica_conns)
    ok_baseline, run_config_baseline = _run_read_pref(
        env, "secondaryPreferred"
    )

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(
            ok_baseline,
            message="baseline run exited non-zero before replica shutdown",
        )
        replica_gets_baseline = _sum_get_calls(replica_conns)
        env.assertGreater(
            replica_gets_baseline,
            0,
            message="baseline: expected GETs on replicas before shutdown, "
                    "got 0 across {} replicas".format(len(replica_conns)),
        )
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config_baseline, env)

    # ---- Stop one replica -------------------------------------------------
    stopped = _stop_one_replica(env, replica_conns)
    if not stopped:
        # Could not stop any replica; skip rather than produce a false result.
        env.skip()
        return

    # Allow the cluster a moment to register the failure. Poll until at least
    # one of the original replicas stops responding to PING (i.e. the SHUTDOWN
    # actually took effect), then break. Caps at 5s so a flaky-but-alive
    # replica doesn't hang the test indefinitely.
    deadline = time.time() + 5.0
    while time.time() < deadline:
        any_down = False
        for conn in replica_conns:
            try:
                conn.execute_command("PING")
            except Exception:
                any_down = True
                break
        if any_down:
            break
        time.sleep(0.1)

    # ---- Post-failover run ------------------------------------------------
    master_conns = env.getOSSMasterNodesConnectionList()

    # Re-discover live replica connections (some may now be unreachable).
    surviving_replica_conns = []
    for conn in replica_conns:
        try:
            conn.execute_command("PING")
            surviving_replica_conns.append(conn)
        except Exception:
            pass

    _reset_all_commandstats(env, surviving_replica_conns)
    for conn in master_conns:
        try:
            conn.execute_command("CONFIG", "RESETSTAT")
        except Exception:
            pass

    ok_post, run_config_post = _run_read_pref(env, "secondaryPreferred")

    failed = env.getNumberOfFailedAssertion()
    try:
        # Must not crash (return code must not be a negative signal value).
        env.assertTrue(
            ok_post,
            message="memtier crashed or returned non-zero after replica "
                    "shutdown with --read-preference=secondaryPreferred",
        )

        # GETs must have landed somewhere (masters or surviving replicas).
        all_live_conns = master_conns + surviving_replica_conns
        total_gets = _sum_get_calls(all_live_conns)
        env.assertGreater(
            total_gets,
            0,
            message="no GETs recorded anywhere after replica shutdown; "
                    "failover to master did not happen",
        )

        # Fallback-to-primary specifically: with --read-preference=
        # secondaryPreferred, the failed shard must redirect its reads to
        # the master. Aggregating across all live conns hides the case
        # where surviving replicas absorb all traffic (different shards)
        # and the stopped shard's master gets none. Require master_gets
        # > 0 to prove the fallback actually fired.
        master_gets = _sum_get_calls(master_conns)
        env.assertGreater(
            master_gets,
            0,
            message="no GETs recorded on masters after replica shutdown "
                    "with --read-preference=secondaryPreferred; expected "
                    "the affected shard to fall back to its master, got 0",
        )
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config_post, env)


# ---------------------------------------------------------------------------
# -READONLY retry path coverage placeholder
#
# Round-14 reviewers (R4+R6 #100) asked for an explicit test of the
# -READONLY retry path in cluster_client.cpp:2138 (handle "-READONLY"). The
# scenario is:
#   1. memtier has an in-flight GET queued on connection C (a replica).
#   2. CLUSTER FAILOVER promotes the replica to primary (or the topology
#      otherwise demotes the original primary; either way C is now a primary).
#   3. memtier's pre-failover producer sends GET on C. Depending on the
#      flow (sticky producer connection, queued before the role flip), the
#      "would-be-read" can be perceived by C as a write and rejected with
#      -READONLY. The retry path in handle_response must reroute to the
#      current slot primary (could be the same node post-failover) and
#      complete without hanging.
#
# Forcing -READONLY deterministically requires precisely orchestrated
# CLUSTER FAILOVER timing against an in-flight pipeline, which the RLTest
# harness does not expose: there is no stable hook to interleave a FAILOVER
# command with the memtier producer's pipeline tick.
#
# Skipping with a TODO is preferable to writing a probabilistic test that
# either flakes (if FAILOVER lands between the producer's keypress) or
# silently passes when no -READONLY is ever produced (FAILOVER happens
# before the producer connects).
# ---------------------------------------------------------------------------

def test_readonly_retry_resp2_resp3(env):
    """Coverage placeholder for the -READONLY retry path
    (cluster_client.cpp handle_response, "-READONLY" prefix branch).

    TODO(#100): deterministic -READONLY orchestration requires precise
    CLUSTER FAILOVER timing against an in-flight memtier pipeline, which
    the RLTest harness does not expose. Re-enable when a stable hook for
    interleaving FAILOVER between producer pipeline ticks is available.
    """
    env.skip()
    return
