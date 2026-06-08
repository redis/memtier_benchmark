"""
Test --read-preference combined with --multi-key-get (MGET) in cluster mode.

Background
----------
In cluster mode --multi-key-get batches N GETs into a single MGET command and
routes the request to the shard that owns the selected key slot.  When
--read-preference=secondary is also supplied, every MGET must be routed to the
*replica* that serves the same slot rather than to the primary.

Test
----
test_read_preference_mget
  Pre-populate same-slot keys using the {tag}-key-NNN pattern so they all
  live on a single shard.  Run
    --multi-key-get=10 --read-preference=secondary --ratio=0:1
  and assert:
    - cmdstat_mget on replicas > 0
    - cmdstat_mget on masters  == 0
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
# Env override: replicas required
# ---------------------------------------------------------------------------

# Replica/shard topology is driven by --use-slaves and --shards-count, which
# tests/run_tests.sh passes to RLTest when OSS_CLUSTER_REPLICAS=1.

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HASH_TAG = "rpmget"
_KEY_MIN = 1
_KEY_MAX = 200
_MGET_BATCH = 10


def _pre_populate(env, hash_tag=_HASH_TAG, key_min=_KEY_MIN, key_max=_KEY_MAX):
    """Write same-slot keys to the cluster using a cluster-aware client.

    Earlier revisions iterated round-robin over
    getOSSMasterNodesConnectionList() with plain StrictRedis connections,
    which do not follow MOVED redirects. Every key that did not hash to
    its assigned master surfaced as a MOVED error. Switch to a
    cluster-aware connection that routes each SET to the slot's owner
    automatically and falls back to a regular connection in non-cluster
    envs.
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


def _sum_mget_calls(conns):
    """Sum cmdstat_mget.calls across a list of connections."""
    total = 0
    for conn in conns:
        try:
            stats = conn.execute_command("INFO", "COMMANDSTATS")
        except Exception:
            continue
        if isinstance(stats, dict):
            total += int(stats.get("cmdstat_mget", {}).get("calls", 0))
        else:
            for line in stats.split("\n"):
                line = line.strip()
                if line.startswith("cmdstat_mget:"):
                    for kv in line.split(":", 1)[1].split(","):
                        kv = kv.strip()
                        if kv.startswith("calls="):
                            try:
                                total += int(kv.split("=", 1)[1])
                            except ValueError:
                                pass
    return total


def _run_mget_workload(env, extra_args, threads=2, clients=4, requests=100, timeout=60):
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


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

def test_read_preference_mget(env):
    """--multi-key-get with --read-preference=secondary must route MGET to
    replicas.  Masters must record zero MGET calls."""
    if not env.isCluster():
        env.skip()
        return

    replica_conns = get_cluster_replica_connections(env)
    if not replica_conns:
        env.skip()
        return

    _pre_populate(env)
    _reset_all_commandstats(env, replica_conns)

    extra_args = [
        "--ratio=0:{}".format(_MGET_BATCH),
        "--multi-key-get={}".format(_MGET_BATCH),
        "--key-minimum={}".format(_KEY_MIN),
        "--key-maximum={}".format(_KEY_MAX),
        "--read-preference=secondary",
    ]
    ok, run_config = _run_mget_workload(
        env, extra_args, threads=1, clients=2, requests=50
    )

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(
            ok,
            message="memtier exited non-zero with --multi-key-get and "
                    "--read-preference=secondary",
        )

        replica_mgets = _sum_mget_calls(replica_conns)
        env.assertGreater(
            replica_mgets,
            0,
            message="expected MGET calls on replicas with "
                    "--read-preference=secondary, got 0 across {} "
                    "replicas".format(len(replica_conns)),
        )

        master_mgets = _sum_mget_calls(env.getOSSMasterNodesConnectionList())
        env.assertEqual(
            master_mgets,
            0,
            message="expected 0 MGET calls on masters with "
                    "--read-preference=secondary, got {}".format(master_mgets),
        )
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


# ---------------------------------------------------------------------------
# Spin-guard smoke: mixed SET+MGET with strict-secondary must not peg CPU.
#
# Verifies the pipeline-cap defer in create_mget_request bumps
# m_strict_no_route_attempts so the spin guard can trip and cap CPU.
# When the producer's pipeline never grows (pure-MGET) the defer loop
# becomes tight; for mixed SET+MGET the writes saturate the primary fast
# enough that the replica's --pipeline cap can still trip the defer
# repeatedly. We test the milder mixed case here: under --ratio=1:1
# --multi-key-get=10 --read-preference=secondary, memtier should produce
# a reasonable Ops/sec and exit 0, not hang or spin uncapped.
# ---------------------------------------------------------------------------

def test_read_preference_mget_strict_secondary_spin_guard(env):
    """Mixed SET+MGET workload under --read-preference=secondary must
    complete cleanly within a short test-time. If the defer counter is
    not bumped on pipeline-cap defer, hold_pipeline cannot trip the
    yield gate and the run either hangs or pegs CPU."""
    if not env.isCluster():
        env.skip()
        return
    replica_conns = get_cluster_replica_connections(env)
    if not replica_conns:
        env.skip()
        return

    _pre_populate(env)

    # Use a bounded --requests budget rather than --test-time so a hang
    # would manifest as the harness timeout; the requests count is small
    # enough that a healthy run completes in well under a second.
    extra_args = [
        "--ratio=1:1",
        "--multi-key-get={}".format(_MGET_BATCH),
        "--key-minimum={}".format(_KEY_MIN),
        "--key-maximum={}".format(_KEY_MAX),
        "--read-preference=secondary",
    ]
    ok, run_config = _run_mget_workload(
        env, extra_args, threads=1, clients=2, requests=200, timeout=20
    )

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(
            ok,
            message="memtier did not exit cleanly within requests=200 budget for "
                    "mixed SET+MGET --read-preference=secondary; possible "
                    "spin or hang in the MGET defer path",
        )
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


# ---------------------------------------------------------------------------
# Pure-MGET pipeline-cap spin-guard reproducer.
#
# create_mget_request bumps the strict-no-route counter on a pipeline-cap
# defer; hold_pipeline yields unconditionally once the counter trips, even
# when `any_route=true` (the destination is saturated-but-live). Pure MGET
# (ratio=0:1) with a small --pipeline cap stresses that path: the producer's
# own pipeline never grows, so the only way to release the event loop is the
# hold_pipeline yield.
#
# NOTE: smoke-only. Engineering a deterministic "slow replica" in a unit-test
# Docker/RLTest environment is fragile, so we just assert the benchmark exits
# within --test-time=5s. A timeout (hang or uncapped spin) means the spin
# guard regressed. TODO: add a CPU-time sample if/when RLTest gains a portable
# resource-usage hook.
# ---------------------------------------------------------------------------

def test_read_preference_mget_pure_pipeline_cap_spin_guard(env):
    """Pure-MGET workload (ratio=0:1) with --pipeline=4 and
    --read-preference=secondary must complete within --test-time=5s. A
    hang means the hold_pipeline yield-on-saturation regressed."""
    if not env.isCluster():
        env.skip()
        return
    replica_conns = get_cluster_replica_connections(env)
    if not replica_conns:
        env.skip()
        return

    _pre_populate(env)

    extra_args = [
        "--ratio=0:1",
        "--multi-key-get={}".format(_MGET_BATCH),
        "--pipeline=4",
        "--key-minimum={}".format(_KEY_MIN),
        "--key-maximum={}".format(_KEY_MAX),
        "--read-preference=secondary",
        "--test-time=5",
    ]
    # memtier rejects --requests + --test-time as mutually exclusive, so we
    # pass requests=None to suppress the auto-injected --requests and rely on
    # --test-time=5 above as the sole bound.
    ok, run_config = _run_mget_workload(
        env, extra_args, threads=1, clients=2, requests=None, timeout=20
    )

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(
            ok,
            message="memtier did not exit cleanly within --test-time=5 for "
                    "pure-MGET --pipeline=4 --read-preference=secondary; "
                    "possible spin or hang in the pipeline-cap defer path "
                    "(hold_pipeline yield-on-saturation regressed)",
        )
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)
