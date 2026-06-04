"""
Tests for --read-preference routing modes in cluster mode.

Background
----------
The --read-preference flag controls which cluster nodes receive GET traffic:
  primary           - all reads go to the master/primary shard
  secondary         - all reads go to replica nodes only
  secondaryPreferred - reads go to replicas when available, fall back to primary
  nearest           - reads go to any node (lowest latency); no strict assertion

These tests require a cluster started with replicas (useSlaves=True).  They
are skipped automatically when not in cluster mode or when no replicas are
advertised.

Test matrix
-----------
1. test_read_preference_primary
   With --read-preference=primary all GETs must land on masters; replicas
   must show 0 GET calls.

2. test_read_preference_secondary
   With --read-preference=secondary all GETs must land on replicas; masters
   must show 0 GET calls.

3. test_read_preference_secondary_preferred
   With --read-preference=secondaryPreferred GETs must land on replicas
   (masters near 0); if no replicas are available the test is skipped.

4. test_read_preference_nearest
   With --read-preference=nearest memtier must exit 0 and issue some GETs;
   no distribution assertion is made (nearest is inherently racy).
"""

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
# Env override: every test in this module needs replicas
# ---------------------------------------------------------------------------

# Replica/shard topology is driven by --use-slaves and --shards-count, which
# tests/run_tests.sh passes to RLTest when OSS_CLUSTER_REPLICAS=1.


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_THREADS = 2
_CLIENTS = 4
_REQUESTS = 200


def _pre_populate(env, key_count=100):
    """Write key_count keys directly to the cluster masters via SET."""
    master_conns = env.getOSSMasterNodesConnectionList()
    # Use a pipeline per master to fan out keys quickly.
    # We use a hash-tag so that the writes are distributed across all shards.
    for i in range(key_count):
        # pick a master in round-robin
        conn = master_conns[i % len(master_conns)]
        conn.execute_command("SET", "rp-key-{}".format(i), "val-{}".format(i))


def _reset_all_commandstats(env, replica_conns):
    """Reset commandstats on all masters and replicas."""
    for conn in env.getOSSMasterNodesConnectionList():
        try:
            conn.execute_command("CONFIG", "RESETSTAT")
        except Exception:
            pass
    reset_commandstats(replica_conns)


def _sum_get_calls(conns):
    """Sum cmdstat_get.calls across a list of Redis connections."""
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
    """Run a read-only memtier workload and return (ok, run_config)."""
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--ratio=0:1",
            "--key-minimum=0",
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


# ---------------------------------------------------------------------------
# Test 1 – primary: reads land only on masters
# ---------------------------------------------------------------------------

def test_read_preference_primary(env):
    """--read-preference=primary must route all GETs to master nodes.
    Replicas must record zero GET calls."""
    if not env.isCluster():
        env.skip()
        return
    replica_conns = get_cluster_replica_connections(env)
    if not replica_conns:
        env.skip()
        return

    _pre_populate(env, key_count=100)
    _reset_all_commandstats(env, replica_conns)

    ok, run_config = _run_read_pref(env, "primary")

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(
            ok,
            message="memtier exited non-zero with --read-preference=primary",
        )

        master_gets = _sum_get_calls(env.getOSSMasterNodesConnectionList())
        env.assertGreater(
            master_gets,
            0,
            message="expected GETs on master nodes with --read-preference=primary, "
                    "got 0",
        )

        replica_gets = _sum_get_calls(replica_conns)
        env.assertEqual(
            replica_gets,
            0,
            message="expected 0 GETs on replicas with --read-preference=primary, "
                    "got {} across {} replicas".format(
                        replica_gets, len(replica_conns)
                    ),
        )
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


# ---------------------------------------------------------------------------
# Test 2 – secondary: reads land only on replicas
# ---------------------------------------------------------------------------

def test_read_preference_secondary(env):
    """--read-preference=secondary must route all GETs to replicas.
    Masters must record zero GET calls."""
    if not env.isCluster():
        env.skip()
        return
    replica_conns = get_cluster_replica_connections(env)
    if not replica_conns:
        env.skip()
        return

    _pre_populate(env, key_count=100)
    _reset_all_commandstats(env, replica_conns)

    ok, run_config = _run_read_pref(env, "secondary")

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(
            ok,
            message="memtier exited non-zero with --read-preference=secondary",
        )

        replica_gets = _sum_get_calls(replica_conns)
        env.assertGreater(
            replica_gets,
            0,
            message="expected GETs on replicas with --read-preference=secondary, "
                    "got 0 across {} replicas".format(len(replica_conns)),
        )

        master_gets = _sum_get_calls(env.getOSSMasterNodesConnectionList())
        env.assertEqual(
            master_gets,
            0,
            message="expected 0 GETs on masters with --read-preference=secondary, "
                    "got {}".format(master_gets),
        )
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


# ---------------------------------------------------------------------------
# Test 3 – secondaryPreferred: reads prefer replicas; masters near 0
# ---------------------------------------------------------------------------

def test_read_preference_secondary_preferred(env):
    """--read-preference=secondaryPreferred must route GETs to replicas when
    replicas are available.  Masters should receive near-zero GET traffic."""
    if not env.isCluster():
        env.skip()
        return
    replica_conns = get_cluster_replica_connections(env)
    if not replica_conns:
        env.skip()
        return

    _pre_populate(env, key_count=100)
    _reset_all_commandstats(env, replica_conns)

    ok, run_config = _run_read_pref(env, "secondaryPreferred")

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(
            ok,
            message="memtier exited non-zero with "
                    "--read-preference=secondaryPreferred",
        )

        replica_gets = _sum_get_calls(replica_conns)
        env.assertGreater(
            replica_gets,
            0,
            message="expected GETs on replicas with "
                    "--read-preference=secondaryPreferred, got 0 across "
                    "{} replicas".format(len(replica_conns)),
        )

        # secondaryPreferred should route to replicas whenever they are live;
        # masters must not receive GET traffic in the steady state. Mirrors
        # the assertion in test_read_preference_secondary above.
        master_gets = _sum_get_calls(env.getOSSMasterNodesConnectionList())
        env.assertEqual(
            master_gets,
            0,
            message="secondaryPreferred should not hit masters when replicas "
                    "are live; got {} master GET(s)".format(master_gets),
        )
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


# ---------------------------------------------------------------------------
# Test 4 – nearest: exit 0, some GETs issued; no distribution assertion
# ---------------------------------------------------------------------------

def test_read_preference_nearest(env):
    """--read-preference=nearest must exit 0 and issue GETs somewhere in the
    cluster.  No strict distribution assertion is made — nearest is latency-
    driven and inherently non-deterministic in a test environment."""
    if not env.isCluster():
        env.skip()
        return
    replica_conns = get_cluster_replica_connections(env)
    if not replica_conns:
        env.skip()
        return

    all_conns = list(env.getOSSMasterNodesConnectionList()) + replica_conns

    _pre_populate(env, key_count=100)
    _reset_all_commandstats(env, replica_conns)

    ok, run_config = _run_read_pref(env, "nearest")

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(
            ok,
            message="memtier exited non-zero with --read-preference=nearest",
        )

        total_gets = _sum_get_calls(all_conns)
        env.assertGreater(
            total_gets,
            0,
            message="expected at least one GET to land somewhere in the cluster "
                    "with --read-preference=nearest, got 0",
        )

        # Distribution sanity: nearest must cold-seed replicas round-robin
        # until each accumulates LATENCY_EWMA_MIN_SAMPLES. If the cold-seed
        # path regressed (and selection fell back to primary-only routing)
        # the test would silently pass with total_gets > 0 but 0 on
        # replicas. Require at least one GET on a replica.
        replica_gets = _sum_get_calls(replica_conns)
        env.assertGreater(
            replica_gets,
            0,
            message="expected at least one GET on a replica with "
                    "--read-preference=nearest (cold-seed round-robin); got 0 "
                    "across {} replicas. nearest may have regressed back to "
                    "primary-pinned selection.".format(len(replica_conns)),
        )
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


# ---------------------------------------------------------------------------
# Test 5 - keyless arbitrary read (--command + --command-is-read) under
# rp_secondary must land on a replica. Smoke-level: prove the keyless
# arbitrary code path is wired through read-preference at all.
# ---------------------------------------------------------------------------

def _sum_dbsize_calls(conns):
    """Sum cmdstat_dbsize.calls across a list of Redis connections."""
    total = 0
    for conn in conns:
        try:
            stats = conn.execute_command("INFO", "COMMANDSTATS")
        except Exception:
            continue
        if isinstance(stats, dict):
            total += int(stats.get("cmdstat_dbsize", {}).get("calls", 0))
        else:
            for line in stats.split("\n"):
                line = line.strip()
                if line.startswith("cmdstat_dbsize:"):
                    for kv in line.split(":", 1)[1].split(","):
                        kv = kv.strip()
                        if kv.startswith("calls="):
                            try:
                                total += int(kv.split("=", 1)[1])
                            except ValueError:
                                pass
    return total


def test_read_preference_keyless_arbitrary_secondary(env):
    """A keyless arbitrary read (DBSIZE flagged with --command-is-read)
    under --read-preference=secondary must land on a replica. Asserts
    DBSIZE counter > 0 on replicas; primary may also see DBSIZE traffic
    from the connection-setup ladder, so the primary-zero check is not
    made here."""
    if not env.isCluster():
        env.skip()
        return
    replica_conns = get_cluster_replica_connections(env)
    if not replica_conns:
        env.skip()
        return

    _reset_all_commandstats(env, replica_conns)

    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--command=DBSIZE",
            "--command-is-read",
            "--command-ratio=1",
            "--read-preference=secondary",
        ],
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=1, clients=2, requests=20)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)

    benchmark = Benchmark.from_json(run_config, benchmark_specs)
    ok = benchmark.run()

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(
            ok,
            message="memtier exited non-zero for DBSIZE --command-is-read "
                    "--read-preference=secondary",
        )

        replica_dbsizes = _sum_dbsize_calls(replica_conns)
        env.assertGreater(
            replica_dbsizes,
            0,
            message="expected DBSIZE calls on replicas under "
                    "--read-preference=secondary; got 0 across {} replicas. "
                    "Keyless arbitrary read routing may have regressed.".format(
                        len(replica_conns)
                    ),
        )
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)
