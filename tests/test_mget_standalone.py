"""
Integration tests for the --multi-key-get (MGET) feature.

Verifies that redis_protocol::write_command_multi_get() emits valid RESP wire
commands and that memtier_benchmark correctly accounts for hits/misses in JSON
output.

All tests are standalone-only (MGET in memtier is not slot-safe for cluster
mode when keys span multiple hash slots).

Run with:
  TEST=test_mget_standalone.py OSS_STANDALONE=1 ./tests/run_tests.sh
"""
import json
import os
import tempfile

from include import (
    add_required_env_arguments,
    addTLSArgs,
    agg_info_commandstats,
    assert_minimum_memtier_outcomes,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_mget_benchmark(env, test_dir, extra_args, threads=1, clients=1,
                           requests=100):
    """Return (Benchmark, RunConfig) for an MGET workload."""
    config = get_default_memtier_config(threads=threads, clients=clients,
                                        requests=requests)
    benchmark_specs = {"name": env.testName, "args": list(extra_args)}
    addTLSArgs(benchmark_specs, env)
    add_required_env_arguments(benchmark_specs, config, env,
                               env.getMasterNodesList())
    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)
    return Benchmark.from_json(run_config, benchmark_specs), run_config


def _preload_keys(env, test_dir, key_min, key_max):
    """
    Run a SET-only pass to populate keys [key_min, key_max] in Redis using
    P:P (sequential) pattern so every key in the range exists before we read.
    """
    config = get_default_memtier_config(threads=1, clients=1,
                                        requests="allkeys")
    benchmark_specs = {
        "name": env.testName + "_preload",
        "args": [
            "--ratio=1:0",
            "--key-pattern=P:P",
            "--key-minimum={}".format(key_min),
            "--key-maximum={}".format(key_max),
        ],
    }
    addTLSArgs(benchmark_specs, env)
    add_required_env_arguments(benchmark_specs, config, env,
                               env.getMasterNodesList())
    run_config = RunConfig(test_dir, env.testName + "_preload", config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)
    benchmark = Benchmark.from_json(run_config, benchmark_specs)
    ok = benchmark.run()
    # Surface preload errors immediately so test failures are easier to debug.
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(ok, message="preload SET pass failed")


def _reset_commandstats(env):
    """Issue CONFIG RESETSTAT on every master shard."""
    for conn in env.getOSSMasterNodesConnectionList():
        conn.execute_command("CONFIG", "RESETSTAT")


def _flushall(env):
    """FLUSHALL on every master shard."""
    for conn in env.getOSSMasterNodesConnectionList():
        conn.execute_command("FLUSHALL")


def _read_json(run_config):
    """Load and return the mb.json results dict."""
    json_filename = os.path.join(run_config.results_dir, "mb.json")
    with open(json_filename) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Test 1: basic commandstats verification
# ---------------------------------------------------------------------------

def test_mget_commandstats_basic(env):
    """
    Verify that MGET commands reach Redis and are counted correctly.

    With --ratio=0:10 --multi-key-get=10 -t 2 -c 5 -n 100 each 'request'
    is one wire MGET.  Total wire commands = threads * clients * requests
    = 2 * 5 * 100 = 1000.
    """
    env.skipOnCluster()

    threads, clients, requests = 2, 5, 100
    expected_mget_calls = threads * clients * requests  # 1000

    test_dir = tempfile.mkdtemp()
    _reset_commandstats(env)

    benchmark, run_config = _build_mget_benchmark(
        env, test_dir,
        extra_args=[
            "--ratio=0:10",
            "--multi-key-get=10",
        ],
        threads=threads,
        clients=clients,
        requests=requests,
    )

    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(run_config, env)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged = {"cmdstat_mget": {"calls": 0}}
    agg_info_commandstats(master_nodes_connections, merged)

    assert_minimum_memtier_outcomes(
        run_config, env, memtier_ok,
        expected_mget_calls,
        merged["cmdstat_mget"]["calls"],
    )


# ---------------------------------------------------------------------------
# Test 2: 72 keys per MGET command
# ---------------------------------------------------------------------------

def test_mget_72_keys_per_command(env):
    """
    Reproduce the user's motivating example: 72 keys per MGET.

    With --ratio=0:72 --multi-key-get=72 -t 1 -c 1 -n 20 each request is
    one MGET carrying 72 keys, so cmdstat_mget.calls must equal 20.
    """
    env.skipOnCluster()

    threads, clients, requests = 1, 1, 20
    expected_mget_calls = threads * clients * requests  # 20

    test_dir = tempfile.mkdtemp()
    _reset_commandstats(env)

    benchmark, run_config = _build_mget_benchmark(
        env, test_dir,
        extra_args=[
            "--ratio=0:72",
            "--multi-key-get=72",
        ],
        threads=threads,
        clients=clients,
        requests=requests,
    )

    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(run_config, env)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged = {"cmdstat_mget": {"calls": 0}}
    agg_info_commandstats(master_nodes_connections, merged)

    assert_minimum_memtier_outcomes(
        run_config, env, memtier_ok,
        expected_mget_calls,
        merged["cmdstat_mget"]["calls"],
    )


# ---------------------------------------------------------------------------
# Test 3: parametric key counts
# ---------------------------------------------------------------------------

def test_mget_various_key_counts(env):
    """
    Parametric test: verify that key_count in [4, 16, 72] all produce the
    expected number of MGET wire commands (10 each, with -t 1 -c 1 -n 10).
    """
    env.skipOnCluster()

    threads, clients, requests = 1, 1, 10
    expected_mget_calls = threads * clients * requests  # 10

    for key_count in [4, 16, 72]:
        test_dir = tempfile.mkdtemp()
        _reset_commandstats(env)

        benchmark, run_config = _build_mget_benchmark(
            env, test_dir,
            extra_args=[
                "--ratio=0:{}".format(key_count),
                "--multi-key-get={}".format(key_count),
            ],
            threads=threads,
            clients=clients,
            requests=requests,
        )

        memtier_ok = benchmark.run()
        debugPrintMemtierOnError(run_config, env)

        master_nodes_connections = env.getOSSMasterNodesConnectionList()
        merged = {"cmdstat_mget": {"calls": 0}}
        agg_info_commandstats(master_nodes_connections, merged)

        # Surface which key_count failed, if any.
        env.assertTrue(
            memtier_ok,
            message="memtier exit non-zero for key_count={}".format(key_count),
        )
        env.assertEqual(
            expected_mget_calls,
            merged["cmdstat_mget"]["calls"],
            message="key_count={}: expected {} MGET calls, got {}".format(
                key_count,
                expected_mget_calls,
                merged["cmdstat_mget"]["calls"],
            ),
        )


# ---------------------------------------------------------------------------
# Test 4: all hits
# ---------------------------------------------------------------------------

def test_mget_all_hits(env):
    """
    Preload keys 1-1000, then MGET from the same range.

    All responses must be hits: Gets.Hits/sec > 0 and Gets.Misses/sec == 0.
    """
    env.skipOnCluster()

    key_min, key_max = 1, 1000
    test_dir = tempfile.mkdtemp()

    # Ensure data exists in Redis.
    _preload_keys(env, test_dir, key_min, key_max)
    _reset_commandstats(env)

    benchmark, run_config = _build_mget_benchmark(
        env, test_dir,
        extra_args=[
            "--ratio=0:10",
            "--multi-key-get=10",
            "--key-pattern=R:R",
            "--key-minimum={}".format(key_min),
            "--key-maximum={}".format(key_max),
        ],
        threads=1,
        clients=1,
        requests=100,
    )

    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(memtier_ok)

    results = _read_json(run_config)
    gets = results["ALL STATS"]["Gets"]

    env.assertGreater(gets["Hits/sec"], 0,
                      message="Expected Hits/sec > 0 when all keys exist")
    env.assertEqual(gets["Misses/sec"], 0,
                    message="Expected Misses/sec == 0 when all keys exist")


# ---------------------------------------------------------------------------
# Test 5: all misses
# ---------------------------------------------------------------------------

def test_mget_all_misses(env):
    """
    FLUSHALL first, then MGET over a sparse range that was never populated.

    All responses must be misses: Gets.Hits/sec == 0, Gets.Misses/sec > 0.
    """
    env.skipOnCluster()

    test_dir = tempfile.mkdtemp()
    _flushall(env)
    _reset_commandstats(env)

    benchmark, run_config = _build_mget_benchmark(
        env, test_dir,
        extra_args=[
            "--ratio=0:10",
            "--multi-key-get=10",
            "--key-minimum=1",
            "--key-maximum=100000",
        ],
        threads=1,
        clients=1,
        requests=100,
    )

    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(memtier_ok)

    results = _read_json(run_config)
    gets = results["ALL STATS"]["Gets"]

    env.assertEqual(gets["Hits/sec"], 0,
                    message="Expected Hits/sec == 0 when keyspace is empty")
    env.assertGreater(gets["Misses/sec"], 0,
                      message="Expected Misses/sec > 0 when keyspace is empty")


# ---------------------------------------------------------------------------
# Test 6: partial hits
# ---------------------------------------------------------------------------

def test_mget_partial_hits(env):
    """
    Preload keys 1-100 only, then MGET over range 1-1000.

    Roughly 10 % of requests will hit; both Gets.Hits/sec and
    Gets.Misses/sec must be strictly positive.
    """
    env.skipOnCluster()

    test_dir = tempfile.mkdtemp()
    _flushall(env)

    # Preload a subset of the query range.
    _preload_keys(env, test_dir, key_min=1, key_max=100)
    _reset_commandstats(env)

    benchmark, run_config = _build_mget_benchmark(
        env, test_dir,
        extra_args=[
            "--ratio=0:10",
            "--multi-key-get=10",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=1000",
        ],
        threads=1,
        clients=1,
        requests=100,
    )

    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(memtier_ok)

    results = _read_json(run_config)
    gets = results["ALL STATS"]["Gets"]

    env.assertGreater(gets["Hits/sec"], 0,
                      message="Expected some hits when partial keyspace is populated")
    env.assertGreater(gets["Misses/sec"], 0,
                      message="Expected some misses when partial keyspace is populated")


# ---------------------------------------------------------------------------
# Test 7: JSON output structure
# ---------------------------------------------------------------------------

def test_mget_json_output_structure(env):
    """
    Verify that an MGET-only workload produces the correct JSON structure.

    Expected:
    - ALL STATS.Gets exists with Ops/sec > 0, Count > 0
    - Gets.Hits/sec and Gets.Misses/sec keys are present (>= 0)
    - ALL STATS.Sets does NOT exist (ratio = 0:N means no SETs)
    """
    env.skipOnCluster()

    test_dir = tempfile.mkdtemp()
    _reset_commandstats(env)

    benchmark, run_config = _build_mget_benchmark(
        env, test_dir,
        extra_args=[
            "--ratio=0:10",
            "--multi-key-get=10",
        ],
        threads=1,
        clients=1,
        requests=50,
    )

    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(memtier_ok)

    results = _read_json(run_config)
    all_stats = results["ALL STATS"]

    # Gets section must exist.
    env.assertTrue("Gets" in all_stats, message="ALL STATS.Gets missing from JSON")
    gets = all_stats["Gets"]

    env.assertGreater(gets.get("Ops/sec", 0), 0,
                      message="ALL STATS.Gets.Ops/sec must be > 0")
    env.assertGreater(gets.get("Count", 0), 0,
                      message="ALL STATS.Gets.Count must be > 0")

    # Hits/sec and Misses/sec must be present (value can be 0).
    env.assertTrue("Hits/sec" in gets,
                   message="ALL STATS.Gets.Hits/sec key missing from JSON")
    env.assertTrue(gets["Hits/sec"] >= 0,
                   message="ALL STATS.Gets.Hits/sec must be >= 0")

    env.assertTrue("Misses/sec" in gets,
                   message="ALL STATS.Gets.Misses/sec key missing from JSON")
    env.assertTrue(gets["Misses/sec"] >= 0,
                   message="ALL STATS.Gets.Misses/sec must be >= 0")

    # No SETs in a ratio=0:N run — JSON always includes the key, but Count must be 0.
    if "Sets" in all_stats:
        env.assertEqual(all_stats["Sets"].get("Count", 0), 0,
                        message="ALL STATS.Sets.Count must be 0 for ratio=0:N workloads")


# ---------------------------------------------------------------------------
# Test 8: ratio alignment (mixed SET + MGET)
# ---------------------------------------------------------------------------

def test_mget_ratio_alignment(env):
    """
    With --ratio=1:72 --multi-key-get=72 both SET and MGET commands must
    reach Redis.

    We do not assert exact counts because the ratio cycle arithmetic is
    complex (1 SET + 1 MGET = 2 wire commands per cycle of 73 logical
    requests); we only require both counters to be strictly positive.
    """
    env.skipOnCluster()

    test_dir = tempfile.mkdtemp()
    _reset_commandstats(env)

    benchmark, run_config = _build_mget_benchmark(
        env, test_dir,
        extra_args=[
            "--ratio=1:72",
            "--multi-key-get=72",
        ],
        threads=1,
        clients=1,
        requests=73,
    )

    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(memtier_ok)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged = {
        "cmdstat_set":  {"calls": 0},
        "cmdstat_mget": {"calls": 0},
    }
    agg_info_commandstats(master_nodes_connections, merged)

    env.assertGreater(merged["cmdstat_set"]["calls"], 0,
                      message="Expected SET calls > 0 with ratio=1:72")
    env.assertGreater(merged["cmdstat_mget"]["calls"], 0,
                      message="Expected MGET calls > 0 with ratio=1:72")


# ---------------------------------------------------------------------------
# Test 9: multi-slot key range on standalone
# ---------------------------------------------------------------------------

def test_mget_key_range_multislot(env):
    """
    Standalone has no slot restriction: MGET should work cleanly regardless
    of which hash slots the keys would map to.

    Use a very large key range (1 to 10,000,000) to simulate 'multi-slot'
    access that would fail in cluster mode.  Assert that the benchmark
    completes cleanly and at least one MGET was issued.
    """
    env.skipOnCluster()

    test_dir = tempfile.mkdtemp()
    _reset_commandstats(env)

    benchmark, run_config = _build_mget_benchmark(
        env, test_dir,
        extra_args=[
            "--ratio=0:10",
            "--multi-key-get=10",
            "--key-minimum=1",
            "--key-maximum=10000000",
        ],
        threads=1,
        clients=1,
        requests=50,
    )

    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(memtier_ok,
                   message="Benchmark must exit 0 for multi-slot key range on standalone")

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged = {"cmdstat_mget": {"calls": 0}}
    agg_info_commandstats(master_nodes_connections, merged)

    env.assertGreater(merged["cmdstat_mget"]["calls"], 0,
                      message="Expected at least one MGET to reach Redis")
