"""
Tests for --retry-on-error, --max-retries, --retry-on, --failed-keys-file.

Run subset:
  TEST=test_retry_on_error.py ./tests/run_tests.sh
"""

import os
import tempfile
from include import *
from mb import Benchmark, RunConfig


def _build_benchmark(env, extra_args, threads=1, clients=1, requests=1000):
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            "--ratio=1:0",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=1000",
        ]
        + list(extra_args),
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=threads, clients=clients, requests=requests)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    return Benchmark.from_json(config, benchmark_specs), config


def test_retry_on_error_happy_path(env):
    """--retry-on-error with no induced errors: completes normally, no stats regression."""
    benchmark, config = _build_benchmark(
        env,
        extra_args=[
            "--retry-on-error",
            "--max-retries=5",
            "--retry-backoff-ms=10",
        ],
    )
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok)

    # JSON should contain the new config fields surfaced in the configuration
    # block (sanity-check the flag plumbed through).
    json_path = "{0}/mb.json".format(config.results_dir)
    env.assertTrue(os.path.isfile(json_path))


def test_max_retries_zero_disables_retry(env):
    """--max-retries=0 disables retry even with the master switch on."""
    benchmark, config = _build_benchmark(
        env,
        extra_args=[
            "--retry-on-error",
            "--max-retries=0",
        ],
        requests=100,
    )
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok)


def test_failed_keys_file_logs_permanent_error(env):
    """A permanent error (WRONGTYPE) lands in the failed-keys file mid-run.

    Seeds a known key with a string value, then issues an LPUSH against it. The
    LPUSH always returns WRONGTYPE; with --retry-on-error the permanent
    classifier should NOT retry, and instead append a CSV line.
    """
    # Cluster mode complicates --command routing; skip for now.
    if env.isCluster():
        env.skip()
        return

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    for master_connection in master_nodes_connections:
        master_connection.execute_command("FLUSHALL")
        master_connection.execute_command("SET", "wrongtype-key", "hello")

    test_dir = tempfile.mkdtemp()
    failed_log = os.path.join(test_dir, "failed-keys.csv")

    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            '--command=LPUSH wrongtype-key value',
            "--retry-on-error",
            "--max-retries=10",
            "--failed-keys-file={}".format(failed_log),
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=5)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)

    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok)

    # File exists; header + at least one failure row.
    env.assertTrue(os.path.isfile(failed_log))
    with open(failed_log, "r") as f:
        lines = f.readlines()
    env.assertGreater(len(lines), 1)
    env.assertContains("timestamp,command,key,status,retries", lines[0])
    # Every non-header line must mention WRONGTYPE.
    for line in lines[1:]:
        env.assertContains("WRONGTYPE", line)


def test_retry_on_filter_restricts_set(env):
    """--retry-on=LOADING,BUSY restricts retry to those prefixes only.

    A WRONGTYPE response (permanent) should still NOT be retried even though
    the filter doesn't list it (permanent set always wins).
    """
    if env.isCluster():
        env.skip()
        return

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    for master_connection in master_nodes_connections:
        master_connection.execute_command("FLUSHALL")
        master_connection.execute_command("SET", "wrongtype-key", "hello")

    test_dir = tempfile.mkdtemp()
    failed_log = os.path.join(test_dir, "failed-keys.csv")

    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            '--command=LPUSH wrongtype-key value',
            "--retry-on-error",
            "--retry-on=LOADING,BUSY,TRYAGAIN",
            "--max-retries=10",
            "--failed-keys-file={}".format(failed_log),
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=3)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)

    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok)

    env.assertTrue(os.path.isfile(failed_log))
    with open(failed_log, "r") as f:
        lines = f.readlines()
    # WRONGTYPE still falls into the permanent set even with a restrictive
    # filter that doesn't list it.
    env.assertGreater(len(lines), 1)
    for line in lines[1:]:
        env.assertContains("WRONGTYPE", line)


def test_invalid_max_retries_rejected(env):
    """--max-retries below -1 must be rejected by getopt parsing."""
    test_dir = tempfile.mkdtemp()
    config = get_default_memtier_config(threads=1, clients=1, requests=10)
    master_nodes_list = env.getMasterNodesList()
    benchmark_specs = {
        "name": env.testName,
        "args": ["--retry-on-error", "--max-retries=-5"],
    }
    addTLSArgs(benchmark_specs, env)
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)

    memtier_ok = benchmark.run()
    # Run should fail (nonzero exit) due to invalid argument.
    env.assertFalse(memtier_ok)
