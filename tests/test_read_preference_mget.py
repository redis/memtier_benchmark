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

ENV_DEFAULTS = {"useSlaves": True, "shardsCount": 3}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HASH_TAG = "rpmget"
_KEY_MIN = 1
_KEY_MAX = 200
_MGET_BATCH = 10


def _pre_populate(env, hash_tag=_HASH_TAG, key_min=_KEY_MIN, key_max=_KEY_MAX):
    """Write same-slot keys via SET on the masters."""
    master_conns = env.getOSSMasterNodesConnectionList()
    for i in range(key_min, key_max + 1):
        conn = master_conns[i % len(master_conns)]
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


def _run_mget_workload(env, extra_args, threads=2, clients=4, requests=100):
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
    ok = benchmark.run()
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
