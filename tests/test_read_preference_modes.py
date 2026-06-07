"""
Tests for --read-preference routing modes in cluster mode.

JSON schema validation
----------------------
test_endpoints_json_emitted exercises the mb.json schema for the Endpoints
and Read Routing blocks introduced for --read-preference observability.
It runs a secondary-mode workload and asserts that both blocks are present
and well-formed, then runs a primary-mode workload and asserts both are
absent (per the emit_read_routing gate in run_stats.cpp:1821).

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

import json
import os
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
    """Write key_count keys to the cluster using a cluster-aware client.

    Earlier revisions iterated rp-key-N round-robin over
    getOSSMasterNodesConnectionList() with plain StrictRedis connections.
    Because each connection talks to a single shard and does not follow
    MOVED redirects, every key that did not hash to its assigned master
    surfaced as a MOVED 101 error (R5 round-18). Switch to a cluster-aware
    connection that routes each SET to the slot's owner automatically and
    falls back to a regular connection in non-cluster envs.
    """
    conn = env.getClusterConnectionIfNeeded()
    for i in range(key_count):
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

        # Primary-leak bound: under --read-preference=secondary keyless reads
        # must NOT spray onto the primary, and no setup-ladder step issues
        # DBSIZE on the primary (AUTH / HELLO / SELECT only). Any non-zero
        # count here is a routing leak.
        master_dbsizes = _sum_dbsize_calls(env.getOSSMasterNodesConnectionList())
        env.assertEqual(
            master_dbsizes,
            0,
            message="primary observed {} DBSIZE calls under "
                    "--read-preference=secondary; expected 0 (no legitimate "
                    "primary DBSIZE source). Keyless arbitrary reads may have "
                    "leaked to primary.".format(master_dbsizes),
        )
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


# ---------------------------------------------------------------------------
# Test 6 - Endpoints / Read Routing JSON schema validation
#
# Verifies that the read-preference observability JSON blocks are emitted with
# the expected schema and that they are correctly suppressed for primary mode
# (per the emit_read_routing gate in run_stats.cpp:1821).
# ---------------------------------------------------------------------------

# Bump the workload size so each active endpoint accumulates at least
# LATENCY_EWMA_MIN_SAMPLES (defined as 10 in cluster_client.cpp / shard_connection.cpp).
# Lower values would race the warm threshold and make the "Latency Samples >= 10"
# assertion flaky.
_ENDPOINTS_REQUESTS = 200
_ENDPOINTS_CLIENTS = 4
_ENDPOINTS_THREADS = 2
_LATENCY_SAMPLES_WARM_THRESHOLD = 10


def _load_mb_json(run_config):
    json_path = os.path.join(run_config.results_dir, "mb.json")
    with open(json_path) as fh:
        return json.load(fh)


def test_endpoints_json_emitted(env):
    """When --read-preference != primary in cluster mode, mb.json must emit
    a "Read Routing" sub-object and an "Endpoints" sub-object under
    "ALL STATS".  The Read Routing block must include Ops from Primary,
    Ops from Replica, and Primary Fraction.  Each endpoint must include
    role, conn_id, Ops, Avg Latency (us), and Latency Samples.  At least
    one active endpoint must show Latency Samples >= the warm threshold
    (10 = LATENCY_EWMA_MIN_SAMPLES).

    The same workload run with --read-preference=primary must NOT emit
    either block (run_stats.cpp emit_read_routing gate)."""
    if not env.isCluster():
        env.skip()
        return
    replica_conns = get_cluster_replica_connections(env)
    if not replica_conns:
        env.skip()
        return

    _pre_populate(env, key_count=100)
    _reset_all_commandstats(env, replica_conns)

    # ---- Phase A: read_preference=secondary -> blocks present ----------
    ok_sec, run_config_sec = _run_read_pref(
        env,
        "secondary",
        threads=_ENDPOINTS_THREADS,
        clients=_ENDPOINTS_CLIENTS,
        requests=_ENDPOINTS_REQUESTS,
    )

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(
            ok_sec,
            message="memtier exited non-zero under "
                    "--read-preference=secondary",
        )

        result_sec = _load_mb_json(run_config_sec)
        env.assertContains("ALL STATS", result_sec)
        all_stats_sec = result_sec["ALL STATS"]

        # ---- Read Routing schema ----------------------------------------
        env.assertContains(
            "Read Routing",
            all_stats_sec,
            message="Read Routing block missing from ALL STATS under "
                    "--read-preference=secondary",
        )
        rr = all_stats_sec["Read Routing"]
        for key in ("Ops from Primary", "Ops from Replica", "Primary Fraction"):
            env.assertContains(
                key,
                rr,
                message="Read Routing missing required key '{}'".format(key),
            )

        # Secondary mode: ALL ops must come from replicas; Primary Fraction
        # must be 0.0.  The aggregate counter validates the schema AND the
        # routing-attribution code path at once.
        env.assertGreater(
            int(rr["Ops from Replica"]),
            0,
            message="Read Routing.Ops from Replica == 0 under secondary "
                    "(routing attribution counter not incrementing)",
        )
        env.assertEqual(
            int(rr["Ops from Primary"]),
            0,
            message="Read Routing.Ops from Primary > 0 under secondary "
                    "(read leaked to primary in attribution counter; "
                    "got {})".format(rr["Ops from Primary"]),
        )

        # ---- Endpoints schema -------------------------------------------
        env.assertContains(
            "Endpoints",
            all_stats_sec,
            message="Endpoints block missing from ALL STATS under "
                    "--read-preference=secondary",
        )
        endpoints = all_stats_sec["Endpoints"]
        env.assertTrue(
            len(endpoints) > 0,
            message="Endpoints block is empty under secondary; expected at "
                    "least one entry",
        )

        warm_endpoints = 0
        for addr, ep in endpoints.items():
            for key in ("role", "conn_id", "Ops", "Avg Latency (us)", "Latency Samples"):
                env.assertContains(
                    key,
                    ep,
                    message="Endpoints[{}] missing required key '{}'".format(
                        addr, key
                    ),
                )
            # role must be either "primary" or "replica" (string-typed)
            role_val = ep["role"]
            env.assertTrue(
                role_val in ("primary", "replica"),
                message="Endpoints[{}].role unexpected value '{}'; "
                        "expected 'primary' or 'replica'".format(addr, role_val),
            )
            samples = int(ep["Latency Samples"])
            if samples >= _LATENCY_SAMPLES_WARM_THRESHOLD:
                warm_endpoints += 1

        env.assertGreater(
            warm_endpoints,
            0,
            message="no Endpoints block entry reached Latency Samples >= {} "
                    "(LATENCY_EWMA_MIN_SAMPLES); EWMA seeding may have "
                    "regressed or the workload was too small".format(
                        _LATENCY_SAMPLES_WARM_THRESHOLD
                    ),
        )
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config_sec, env)

    # ---- Phase B: read_preference=primary -> blocks absent -------------
    _reset_all_commandstats(env, replica_conns)

    ok_pri, run_config_pri = _run_read_pref(
        env,
        "primary",
        threads=_ENDPOINTS_THREADS,
        clients=_ENDPOINTS_CLIENTS,
        requests=_ENDPOINTS_REQUESTS,
    )

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(
            ok_pri,
            message="memtier exited non-zero under --read-preference=primary",
        )

        result_pri = _load_mb_json(run_config_pri)
        all_stats_pri = result_pri["ALL STATS"]
        # Both blocks must be absent for primary mode -- the emit_read_routing
        # gate in run_stats.cpp is `read_preference != rp_primary`.
        env.assertNotContains(
            "Read Routing",
            all_stats_pri,
            message="Read Routing block present under primary; the "
                    "emit_read_routing gate (read_preference != rp_primary) "
                    "regressed",
        )
        env.assertNotContains(
            "Endpoints",
            all_stats_pri,
            message="Endpoints block present under primary; the "
                    "emit_read_routing gate (read_preference != rp_primary) "
                    "regressed",
        )
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config_pri, env)
