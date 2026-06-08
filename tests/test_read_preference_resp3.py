"""
RESP3 x cluster-replicas x --read-preference test cell.

Background
----------
The read-preference routing logic is shared across RESP2 and RESP3 paths, but
the parser, HELLO 3 negotiation, and per-shard reconnect ladder are
RESP3-specific.  Round-14 reviewers (R2, R3, R4, R5, R6, R7) flagged that the
existing read-preference tests only exercise the default RESP2 path; this
suite closes that gap by repeating the secondary / secondaryPreferred /
mget-secondary scenarios under --protocol=resp3.

The tests are gated by three capability probes so they degrade gracefully on
environments that do not support the matrix cell:

  - env.isCluster()           - cluster topology required
  - get_cluster_replica_connections(env) - at least one replica advertised
  - server_supports_resp3(env)            - Redis 6+ HELLO 3 support

Test matrix
-----------
1. test_resp3_read_preference_secondary
   --protocol=resp3 --read-preference=secondary --ratio=0:1
   Asserts cmdstat_get on replicas > 0 AND on masters == 0.

2. test_resp3_read_preference_mget_secondary
   --protocol=resp3 --multi-key-get=10 --read-preference=secondary --ratio=0:1
   Asserts cmdstat_mget on replicas > 0 AND on masters == 0.  Validates
   RESP3 nil parsing on the cluster MGET replica path (closes R3+R6 finding).

3. test_resp3_read_preference_secondaryPreferred_fallback
   --protocol=resp3 --read-preference=secondaryPreferred with one replica
   killed via SHUTDOWN NOSAVE.  Asserts memtier exits 0 and master_gets > 0
   on the affected shard, proving the HELLO 3 reconnect path against a
   fresh primary succeeds.
"""

import tempfile
import time

from include import (
    addTLSArgs,
    add_required_env_arguments,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_cluster_replica_connections,
    get_default_memtier_config,
    reset_commandstats,
    server_supports_resp3,
)
from mb import Benchmark, RunConfig


# ---------------------------------------------------------------------------
# Shared helpers (mirrored from test_read_preference_modes.py and
# test_read_preference_mget.py; kept local so this suite stays self-contained
# and does not introduce a cross-test import surface).
# ---------------------------------------------------------------------------

_HASH_TAG = "rpresp3"
_KEY_MIN = 1
_KEY_MAX = 200
# Only pre-populate the lower half of the key range so that memtier queries
# (_KEY_MIN.._KEY_MAX) include keys that were never SET.  Each MGET batch
# therefore contains a mix of existing values and nils, exercising the RESP3
# nil (_\r\n) parsing branch that the test claims to validate.
_MGET_KEY_POPULATED_MAX = 100
_MGET_BATCH = 10


def _pre_populate_keys(env, key_min=0, key_max=99, prefix="rp3-key-"):
    """Write plain keys via a cluster-aware connection (auto-follows MOVED).

    Round-robin over getOSSMasterNodesConnectionList() crashes with
    MovedError when a key hashes to a different shard than the connection
    picked by the modulus.
    """
    conn = env.getClusterConnectionIfNeeded()
    for i in range(key_min, key_max + 1):
        conn.execute_command("SET", "{}{}".format(prefix, i), "val-{}".format(i))


def _pre_populate_same_slot(env, hash_tag=_HASH_TAG, key_min=_KEY_MIN, key_max=_KEY_MAX):
    """Write same-slot keys via the {tag} hash-tag pattern.  Used by MGET.

    Earlier revisions iterated keys round-robin over
    getOSSMasterNodesConnectionList() with plain StrictRedis connections.
    When the cluster's CLUSTER SLOTS view has not fully converged across
    all masters at test setup, SETs hit MOVED redirects that the non-
    cluster-aware client cannot follow, failing the entire RESP3 cell.
    Mirror the round-19 a663c31 fix in test_read_preference_modes.py:
    switch to env.getClusterConnectionIfNeeded() which returns a cluster-
    aware client that auto-follows MOVED in cluster mode (and a plain
    connection in non-cluster envs).
    """
    conn = env.getClusterConnectionIfNeeded()
    for i in range(key_min, key_max + 1):
        conn.execute_command(
            "SET",
            "{{{}}}-key-{}".format(hash_tag, i),
            "val-{}".format(i),
        )


def _reset_all_commandstats(env, replica_conns):
    for conn in env.getOSSMasterNodesConnectionList():
        try:
            conn.execute_command("CONFIG", "RESETSTAT")
        except Exception:
            pass
    reset_commandstats(replica_conns)


def _sum_cmd_calls(conns, cmd):
    """Sum cmdstat_<cmd>.calls across connections."""
    needle = "cmdstat_{}".format(cmd)
    total = 0
    for conn in conns:
        try:
            stats = conn.execute_command("INFO", "COMMANDSTATS")
        except Exception:
            continue
        if isinstance(stats, dict):
            total += int(stats.get(needle, {}).get("calls", 0))
        else:
            for line in stats.split("\n"):
                line = line.strip()
                if line.startswith("{}:".format(needle)):
                    for kv in line.split(":", 1)[1].split(","):
                        kv = kv.strip()
                        if kv.startswith("calls="):
                            try:
                                total += int(kv.split("=", 1)[1])
                            except ValueError:
                                pass
    return total


def _run_workload(env, extra_args, threads=2, clients=4, requests=200, timeout=60):
    benchmark_specs = {
        "name": env.testName,
        "args": list(extra_args),
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
    ok = benchmark.run(timeout=timeout)
    return ok, run_config


def _capability_gates(env):
    """Return (replica_conns, ok) where ok=False means the test must skip.

    Mirrors the three-gate skip pattern recommended by round-14 reviewers:
    cluster mode + replicas advertised + HELLO 3 supported.  Each gate calls
    env.skip() and returns ok=False so the caller can early-return.
    """
    if not env.isCluster():
        env.skip()
        return [], False
    replica_conns = get_cluster_replica_connections(env)
    if not replica_conns:
        env.skip()
        return [], False
    if not server_supports_resp3(env):
        env.skip()
        return replica_conns, False
    return replica_conns, True


# ---------------------------------------------------------------------------
# Test 1: RESP3 + read-preference=secondary - all GETs to replicas
# ---------------------------------------------------------------------------

def test_resp3_read_preference_secondary(env):
    """--protocol=resp3 --read-preference=secondary must route all GETs to
    replicas (cmdstat_get on replicas > 0 AND masters == 0).  Validates the
    RESP3 secondary-routing path."""
    replica_conns, ok = _capability_gates(env)
    if not ok:
        return

    _pre_populate_keys(env, key_min=0, key_max=99, prefix="rp3-key-")
    _reset_all_commandstats(env, replica_conns)

    extra_args = [
        "--ratio=0:1",
        "--key-prefix=rp3-key-",
        "--key-minimum=1",
        "--key-maximum=99",
        "--read-preference=secondary",
        "--protocol=resp3",
    ]
    ok_run, run_config = _run_workload(
        env, extra_args, threads=2, clients=2, requests=100
    )

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(
            ok_run,
            message="memtier exited non-zero with --protocol=resp3 "
                    "--read-preference=secondary",
        )

        replica_gets = _sum_cmd_calls(replica_conns, "get")
        env.assertGreater(
            replica_gets,
            0,
            message="expected GETs on replicas under RESP3+secondary; got 0 "
                    "across {} replicas".format(len(replica_conns)),
        )

        master_gets = _sum_cmd_calls(env.getOSSMasterNodesConnectionList(), "get")
        env.assertEqual(
            master_gets,
            0,
            message="expected 0 GETs on masters under RESP3+secondary; got "
                    "{} (read-preference leaked to primary under RESP3)".format(
                        master_gets
                    ),
        )
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


# ---------------------------------------------------------------------------
# Test 2: RESP3 + --multi-key-get + read-preference=secondary - MGET to replicas
# ---------------------------------------------------------------------------

def test_resp3_read_preference_mget_secondary(env):
    """--protocol=resp3 --multi-key-get=10 --read-preference=secondary must
    route all MGETs to replicas (cmdstat_mget on replicas > 0 AND masters
    == 0).  Validates RESP3 nil parsing on the cluster MGET replica path
    (closes the R3+R6 round-14 finding)."""
    replica_conns, ok = _capability_gates(env)
    if not ok:
        return

    # Populate only keys _KEY_MIN.._MGET_KEY_POPULATED_MAX (1..100); memtier
    # will query _KEY_MIN.._KEY_MAX (1..200), so roughly half the keys in every
    # MGET batch are absent and Redis returns nil (_\r\n in RESP3), forcing
    # the nil-parsing branch to be exercised.
    _pre_populate_same_slot(env, key_max=_MGET_KEY_POPULATED_MAX)
    _reset_all_commandstats(env, replica_conns)

    extra_args = [
        "--ratio=0:{}".format(_MGET_BATCH),
        "--multi-key-get={}".format(_MGET_BATCH),
        # Match the prefix used by _pre_populate_same_slot ({rpresp3}-key-).
        # Without this, memtier queries with the default `memtier-` prefix
        # against keys that were SET as `{rpresp3}-key-N`, producing 100%
        # miss instead of the intended ~50% hit rate that exercises the
        # RESP3 nil parsing branch.
        "--key-prefix={{{}}}-key-".format(_HASH_TAG),
        "--key-minimum={}".format(_KEY_MIN),
        "--key-maximum={}".format(_KEY_MAX),
        "--read-preference=secondary",
        "--protocol=resp3",
    ]
    ok_run, run_config = _run_workload(
        env, extra_args, threads=1, clients=2, requests=50, timeout=60
    )

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(
            ok_run,
            message="memtier exited non-zero with --protocol=resp3 "
                    "--multi-key-get --read-preference=secondary",
        )

        replica_mgets = _sum_cmd_calls(replica_conns, "mget")
        env.assertGreater(
            replica_mgets,
            0,
            message="expected MGET calls on replicas under RESP3+secondary; "
                    "got 0 across {} replicas (RESP3 nil parsing on the "
                    "cluster MGET replica path may have regressed)".format(
                        len(replica_conns)
                    ),
        )

        master_mgets = _sum_cmd_calls(env.getOSSMasterNodesConnectionList(), "mget")
        env.assertEqual(
            master_mgets,
            0,
            message="expected 0 MGET calls on masters under RESP3+secondary; "
                    "got {} (MGET leaked to primary under RESP3)".format(
                        master_mgets
                    ),
        )
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


# ---------------------------------------------------------------------------
# Test 3: RESP3 + secondaryPreferred fallback after replica shutdown
# ---------------------------------------------------------------------------

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
                    pass


def _stop_one_replica(env, replica_conns):
    """Best-effort: SHUTDOWN NOSAVE the first reachable replica.  Returns
    True if at least one replica was sent the shutdown.

    Also notifies RLTest that the killed replica was an expected death so
    teardown does not flag it as a crash in checkExitCode.
    """
    for conn in replica_conns:
        killed_port = _conn_port(conn)
        try:
            conn.execute_command("SHUTDOWN", "NOSAVE")
        except Exception:
            # SHUTDOWN closes the connection; expected.
            pass
        time.sleep(0.5)
        if killed_port is not None:
            _mark_dead_in_rltest(env, {killed_port})
        return True
    return False


def test_resp3_read_preference_secondaryPreferred_fallback(env):
    """Kill one replica with SHUTDOWN NOSAVE, then run with --protocol=resp3
    --read-preference=secondaryPreferred.  Assert memtier exits 0 and at
    least one GET lands on a master - proving the HELLO 3 reconnect on a
    fresh shard succeeded and the fallback path is functional under RESP3.

    Mirrors test_read_preference_failover.py but adds --protocol=resp3 so
    the per-shard reconnect ladder runs the HELLO 3 negotiation against the
    primary it falls back to."""
    replica_conns, ok = _capability_gates(env)
    if not ok:
        return

    _pre_populate_keys(env, key_min=0, key_max=99, prefix="rp3fb-key-")

    # ---- Stop one replica ------------------------------------------------
    stopped = _stop_one_replica(env, replica_conns)
    if not stopped:
        env.skip()
        return

    # Wait for the cluster to notice the failure (poll PING against the
    # original replica list, break as soon as one stops responding).
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

    # Re-discover live replica connections.
    surviving_replica_conns = []
    for conn in replica_conns:
        try:
            conn.execute_command("PING")
            surviving_replica_conns.append(conn)
        except Exception:
            pass

    master_conns = env.getOSSMasterNodesConnectionList()
    _reset_all_commandstats(env, surviving_replica_conns)

    # ---- Run RESP3 secondaryPreferred workload ---------------------------
    extra_args = [
        "--ratio=0:1",
        "--key-prefix=rp3fb-key-",
        "--key-minimum=1",
        "--key-maximum=99",
        "--read-preference=secondaryPreferred",
        "--protocol=resp3",
    ]
    ok_run, run_config = _run_workload(
        env, extra_args, threads=2, clients=2, requests=100
    )

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(
            ok_run,
            message="memtier exited non-zero under --protocol=resp3 "
                    "--read-preference=secondaryPreferred after replica "
                    "SHUTDOWN; HELLO 3 reconnect to the fresh primary may "
                    "have regressed",
        )

        # GETs must have landed somewhere live.
        total_gets = _sum_cmd_calls(master_conns + surviving_replica_conns, "get")
        env.assertGreater(
            total_gets,
            0,
            message="no GETs recorded anywhere under RESP3+secondaryPreferred "
                    "after replica shutdown",
        )

        # The affected shard must fall back to its master.  Aggregated across
        # all live conns hides the case where surviving replicas (different
        # shards) absorb traffic and the stopped shard's master gets none.
        master_gets = _sum_cmd_calls(master_conns, "get")
        env.assertGreater(
            master_gets,
            0,
            message="no GETs recorded on masters under RESP3+"
                    "secondaryPreferred after replica shutdown; fallback to "
                    "primary did not fire (HELLO 3 reconnect on the fresh "
                    "primary may have regressed)",
        )
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)
