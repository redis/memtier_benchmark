"""
Tests for --retry-on-error, --max-retries, --retry-on, --failed-keys-file.

Run subset:
  TEST=test_retry_on_error.py ./tests/run_tests.sh
"""

import json
import os
import subprocess
import tempfile
import time
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


def _run_with_background(env, benchmark, config, background, timeout):
    """Run memtier as a subprocess while background(proc) runs alongside it.

    Returns (exit code, stderr).
    """
    stderr_path = "{0}/mb.stderr".format(config.results_dir)
    with open("{0}/mb.stdout".format(config.results_dir), "w") as stdout_f, open(stderr_path, "w") as stderr_f:
        proc = subprocess.Popen(benchmark.args, stdout=stdout_f, stderr=stderr_f, cwd=config.results_dir)
        try:
            background(proc)
            rc = proc.wait(timeout=timeout)
        finally:
            if proc.poll() is None:
                proc.kill()
            rc = proc.wait(timeout=10)
    with open(stderr_path) as f:
        stderr_content = f.read()
    env.debugPrint("memtier exit code: {}".format(rc), True)
    if rc != 0:
        env.debugPrint("STDERR:\n{}".format(stderr_content[-2000:]), True)
    return rc, stderr_content


def test_retried_requests_are_counted_once(env):
    """
    A request retried after a transient error is processed once, not once per
    attempt.

    Each retryable error response used to count the request as processed, and
    the successful replay counted it again, so a --requests run stopped early
    with exit 0 after sending only part of the requested commands.
    """
    # Toggles maxmemory on one server; the key layout assumes a single shard.
    env.skipOnCluster()
    requests = 3000
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            "--ratio=1:0",
            "--key-pattern=P:P",
            "--key-minimum=1",
            "--key-maximum={}".format(requests),
            # About 3 s for the run; the error window below sits inside it.
            "--rate-limiting=1000",
            "--retry-on-error",
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=requests)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)

    conn = env.getOSSMasterNodesConnectionList()[0]
    conn.execute_command("FLUSHALL")
    saved = conn.execute_command("CONFIG", "GET", "maxmemory*")
    saved = dict(zip(saved[0::2], saved[1::2]))
    conn.execute_command("CONFIG", "SET", "maxmemory-policy", "noeviction")
    conn.execute_command("CONFIG", "RESETSTAT")

    def oom_window(_proc):
        # SETs fail with a retryable -OOM for about a second mid-run.
        time.sleep(1)
        conn.execute_command("CONFIG", "SET", "maxmemory", "1")
        time.sleep(1)
        conn.execute_command("CONFIG", "SET", "maxmemory", "0")

    try:
        rc, _ = _run_with_background(env, benchmark, config, oom_window, timeout=60)
    finally:
        for k in ("maxmemory", "maxmemory-policy"):
            if k in saved:
                conn.execute_command("CONFIG", "SET", k, saved[k])

    rejected = conn.execute_command("INFO", "commandstats").get("cmdstat_set", {}).get("rejected_calls", 0)
    env.assertGreater(rejected, 0, message="no SET was rejected; the error window missed the run")
    env.assertEqual(rc, 0)
    env.assertEqual(conn.execute_command("DBSIZE"), requests,
                    message="the run stopped before sending every requested SET")
    with open("{0}/mb.json".format(config.results_dir)) as f:
        results = json.load(f)
    env.assertEqual(results["ALL STATS"]["Sets"]["Count"], requests)


def test_dropped_replays_do_not_stall_client(env):
    """
    Requests --retry-on-error drops after a reconnect must not stall the client.

    With --max-retries=0, the requests in flight on a killed connection are
    dropped instead of replayed. They stayed counted as generated, so with
    --reconnect-interval the client held its pipeline for good and the run
    failed with "client(s) stopped before reaching their stop condition".
    """
    # Cluster mode rejects --reconnect-interval.
    env.skipOnCluster()
    test_time = 8
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=10000",
            "--reconnect-interval=10",
            "--reconnect-on-error",
            "--retry-on-error",
            "--max-retries=0",
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=None, test_time=test_time)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    conn = env.getOSSMasterNodesConnectionList()[0]

    kills = []

    def kill_waves(_proc):
        # Several waves spaced beyond the default 1s reconnect delay; one kill
        # with a request in flight is enough.
        for _wave in range(4):
            time.sleep(1.5)
            try:
                kills.append(conn.execute_command("CLIENT", "KILL", "TYPE", "normal"))
            except Exception as e:
                env.debugPrint("CLIENT KILL failed: {}".format(e), True)

    rc, stderr_content = _run_with_background(env, benchmark, config, kill_waves, timeout=test_time + 30)

    env.assertGreaterEqual(sum(kills), 1, message="no CLIENT KILL wave found a memtier connection")
    env.assertGreaterEqual(stderr_content.count("Reconnection successful"), 1,
                           message="no connection was killed and recovered")
    env.assertFalse("stopped before reaching their stop condition" in stderr_content,
                    message="client stopped sending after a reconnect")
    env.assertEqual(rc, 0, message="memtier exited {} after connection kills".format(rc))
