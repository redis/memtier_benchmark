"""
Tests for SCAN incremental cursor iteration feature.

Validates --scan-incremental-iteration and --scan-incremental-max-iterations
options, including error validation for invalid configurations.

  TEST=test_scan.py OSS_STANDALONE=1 ./tests/run_tests.sh
"""
import json
import os
import tempfile
import logging

from include import (
    get_default_memtier_config,
    add_required_env_arguments,
    addTLSArgs,
    ensure_clean_benchmark_folder,
    debugPrintMemtierOnError,
)
from mb import Benchmark, RunConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_scan_benchmark(env, test_dir, extra_args, threads=1, clients=1,
                          requests=100, pipeline=1):
    """Build a Benchmark object for SCAN iteration tests."""
    config = get_default_memtier_config(threads=threads, clients=clients,
                                        requests=requests)
    config["memtier_benchmark"]["pipeline"] = pipeline
    benchmark_specs = {"name": env.testName, "args": extra_args}
    addTLSArgs(benchmark_specs, env)
    add_required_env_arguments(benchmark_specs, config, env,
                               env.getMasterNodesList())
    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)
    return Benchmark.from_json(run_config, benchmark_specs), run_config


def _preload_keys(env, count=100, prefix="scankey"):
    """Load some keys into Redis so SCAN has data to iterate."""
    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    for conn in master_nodes_connections:
        pipe = conn.pipeline()
        for i in range(count):
            pipe.set(f"{prefix}:{i}", f"value{i}")
        pipe.execute()


def _read_stdout(run_config):
    """Read the benchmark stdout output file."""
    path = os.path.join(run_config.results_dir, "mb.stdout")
    if os.path.isfile(path):
        with open(path) as f:
            return f.read()
    return ""


def _read_stderr(run_config):
    """Read the benchmark stderr output file."""
    path = os.path.join(run_config.results_dir, "mb.stderr")
    if os.path.isfile(path):
        with open(path) as f:
            return f.read()
    return ""


# ---------------------------------------------------------------------------
# Test: basic SCAN incremental iteration
# ---------------------------------------------------------------------------

def test_scan_incremental_basic(env):
    """Verify that basic SCAN cursor iteration works end-to-end."""
    env.skipOnCluster()
    _preload_keys(env, count=200)

    test_dir = tempfile.mkdtemp()
    try:
        extra_args = [
            '--command', 'SCAN 0 COUNT 10',
            '--scan-incremental-iteration',
            '--pipeline', '1',
        ]
        benchmark, run_config = _build_scan_benchmark(
            env, test_dir, extra_args, threads=1, clients=1, requests=100)
        ok = benchmark.run()

        stdout = _read_stdout(run_config)
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok)
            env.assertTrue('SCAN 0' in stdout,
                           message="Expected 'SCAN 0' stat line in output")
            env.assertTrue('SCAN <cursor>' in stdout,
                           message="Expected 'SCAN <cursor>' stat line in output")

            # Verify JSON output has separate entries for SCAN 0 and SCAN <cursor>
            json_path = os.path.join(run_config.results_dir, "mb.json")
            env.assertTrue(os.path.isfile(json_path),
                           message="Expected mb.json output file")
            with open(json_path) as f:
                results = json.load(f)
            all_stats = results.get("ALL STATS", {})
            env.assertTrue("Scan 0s" in all_stats,
                           message="Expected 'Scan 0s' key in JSON ALL STATS")
            env.assertTrue("Scan <cursor>s" in all_stats,
                           message="Expected 'Scan <cursor>s' key in JSON ALL STATS")
            env.assertGreater(all_stats["Scan 0s"]["Count"], 0,
                              message="Expected non-zero count for Scan 0s")
            env.assertGreater(all_stats["Scan <cursor>s"]["Count"], 0,
                              message="Expected non-zero count for Scan <cursor>s")
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


# ---------------------------------------------------------------------------
# Test: max iterations cap
# ---------------------------------------------------------------------------

def test_scan_incremental_max_iterations(env):
    """Verify --scan-incremental-max-iterations caps continuation SCANs.

    With max_iterations=3 and enough keys (COUNT 1 ensures cursor won't
    return to 0 within 3 iterations), each cycle is exactly:
      1 initial SCAN 0 + 3 continuation SCANs = 4 requests.
    With 40 total requests we expect 10 cycles: 10 initials, 30 continuations.
    """
    env.skipOnCluster()
    _preload_keys(env, count=200)

    max_iter = 3
    num_requests = 40  # must be divisible by (1 + max_iter)
    test_dir = tempfile.mkdtemp()
    try:
        extra_args = [
            '--command', 'SCAN 0 COUNT 1',
            '--scan-incremental-iteration',
            '--scan-incremental-max-iterations', str(max_iter),
            '--pipeline', '1',
        ]
        benchmark, run_config = _build_scan_benchmark(
            env, test_dir, extra_args, threads=1, clients=1,
            requests=num_requests)
        ok = benchmark.run()

        stdout = _read_stdout(run_config)
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok)
            env.assertTrue('SCAN 0' in stdout)
            env.assertTrue('SCAN <cursor>' in stdout)

            # Validate exact counts via JSON output
            json_path = os.path.join(run_config.results_dir, "mb.json")
            env.assertTrue(os.path.isfile(json_path))
            with open(json_path) as f:
                results = json.load(f)
            all_stats = results.get("ALL STATS", {})
            env.assertTrue("Scan 0s" in all_stats)
            env.assertTrue("Scan <cursor>s" in all_stats)

            scan0_count = all_stats["Scan 0s"]["Count"]
            scan_cursor_count = all_stats["Scan <cursor>s"]["Count"]

            # Each cycle: 1 initial + max_iter continuations
            expected_cycles = num_requests // (1 + max_iter)
            env.assertEqual(scan0_count, expected_cycles,
                            message=f"Expected {expected_cycles} initial SCANs, got {scan0_count}")
            env.assertEqual(scan_cursor_count, expected_cycles * max_iter,
                            message=f"Expected {expected_cycles * max_iter} continuation SCANs, got {scan_cursor_count}")
            env.assertEqual(scan0_count + scan_cursor_count, num_requests)
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


# ---------------------------------------------------------------------------
# Test: MATCH pattern
# ---------------------------------------------------------------------------

def test_scan_incremental_with_match(env):
    """Verify SCAN with MATCH pattern works under incremental iteration."""
    env.skipOnCluster()
    _preload_keys(env, count=100, prefix="scanmatch")

    test_dir = tempfile.mkdtemp()
    try:
        extra_args = [
            '--command', 'SCAN 0 MATCH scanmatch:* COUNT 10',
            '--scan-incremental-iteration',
            '--pipeline', '1',
        ]
        benchmark, run_config = _build_scan_benchmark(
            env, test_dir, extra_args, threads=1, clients=1, requests=50)
        ok = benchmark.run()

        stdout = _read_stdout(run_config)
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok)
            env.assertTrue('SCAN 0' in stdout)
            env.assertTrue('SCAN <cursor>' in stdout)
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_scan_incremental_literal_scan_cursor_in_match(env):
    """
    Regression test for #663: only the cursor slot of the internal
    continuation command is a cursor. A user argument that is literally
    "__scan_cursor__" (here the MATCH pattern) is sent as typed in both the
    initial SCAN 0 and the continuation. Before the fix it was dropped from
    the initial SCAN (the *N header still counted it) and the run hung.
    """
    env.skipOnCluster()
    _preload_keys(env, count=200, prefix="scanlit")

    # Log every command so the test can check what was actually sent.
    conn = env.getConnection()
    old_slower_than = conn.execute_command("CONFIG", "GET", "slowlog-log-slower-than")[1]
    old_max_len = conn.execute_command("CONFIG", "GET", "slowlog-max-len")[1]
    conn.execute_command("CONFIG", "SET", "slowlog-log-slower-than", "0")
    conn.execute_command("CONFIG", "SET", "slowlog-max-len", "1000")
    conn.execute_command("SLOWLOG", "RESET")

    test_dir = tempfile.mkdtemp()
    extra_args = [
        '--command', 'SCAN 0 MATCH __scan_cursor__ COUNT 10',
        '--scan-incremental-iteration',
        '--pipeline', '1',
    ]
    benchmark, run_config = _build_scan_benchmark(
        env, test_dir, extra_args, threads=1, clients=1, requests=50)
    ok = benchmark.run(timeout=30)
    entries = conn.execute_command("SLOWLOG", "GET", "1000")
    conn.execute_command("CONFIG", "SET", "slowlog-log-slower-than", old_slower_than)
    conn.execute_command("CONFIG", "SET", "slowlog-max-len", old_max_len)
    conn.execute_command("SLOWLOG", "RESET")

    failed_asserts = env.getNumberOfFailedAssertion()
    try:
        # Entry layout: [id, timestamp, duration, argv, ...].
        scans = [[a.decode() for a in e[3]] for e in entries
                 if e[3] and e[3][0].upper() == b"SCAN"]
        env.assertEqual(len(scans), 50, message="SCANs logged: {}".format(scans[:5]))
        # Every SCAN, initial and continuation, carries a numeric cursor in
        # argv[1] and the MATCH literal unchanged in argv[3].
        bad = [a for a in scans if len(a) != 6 or not a[1].isdigit() or a[2] != "MATCH" or a[3] != "__scan_cursor__"]
        env.assertEqual(bad, [], message="SCANs with a wrong cursor or MATCH: {}".format(bad[:5]))
        env.assertTrue(any(a[1] != "0" for a in scans),
                       message="no continuation with a real cursor: {}".format(scans[:5]))
        env.assertTrue(ok, message="run failed or hung (timeout)")
        if not ok:
            return
        stderr = _read_stderr(run_config)
        env.assertFalse('error response' in stderr,
                        message="server rejected a request: {}".format(stderr[-500:]))
        with open(os.path.join(run_config.results_dir, "mb.json")) as f:
            all_stats = json.load(f).get("ALL STATS", {})
        # 200 keys at COUNT 10 need many pages, so the continuation (with
        # the real cursor in its slot) must have been sent too.
        env.assertGreater(all_stats.get("Scan 0s", {}).get("Count", 0), 0)
        env.assertGreater(all_stats.get("Scan <cursor>s", {}).get("Count", 0), 0)
    finally:
        if env.getNumberOfFailedAssertion() > failed_asserts:
            debugPrintMemtierOnError(run_config, env)


# ---------------------------------------------------------------------------
# Test: bare SCAN (no MATCH, COUNT, or TYPE)
# ---------------------------------------------------------------------------

def test_scan_incremental_bare(env):
    """Verify bare SCAN 0 (no options) works under incremental iteration."""
    env.skipOnCluster()
    _preload_keys(env, count=200)

    test_dir = tempfile.mkdtemp()
    try:
        extra_args = [
            '--command', 'SCAN 0',
            '--scan-incremental-iteration',
            '--pipeline', '1',
        ]
        benchmark, run_config = _build_scan_benchmark(
            env, test_dir, extra_args, threads=1, clients=1, requests=50)
        ok = benchmark.run()

        stdout = _read_stdout(run_config)
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok)
            env.assertTrue('SCAN 0' in stdout)
            env.assertTrue('SCAN <cursor>' in stdout)

            json_path = os.path.join(run_config.results_dir, "mb.json")
            env.assertTrue(os.path.isfile(json_path))
            with open(json_path) as f:
                results = json.load(f)
            all_stats = results.get("ALL STATS", {})
            env.assertTrue("Scan 0s" in all_stats)
            env.assertTrue("Scan <cursor>s" in all_stats)
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


# ---------------------------------------------------------------------------
# Test: MATCH pattern only (no COUNT)
# ---------------------------------------------------------------------------

def test_scan_incremental_match_only(env):
    """Verify SCAN with only MATCH (no COUNT) works."""
    env.skipOnCluster()
    _preload_keys(env, count=200, prefix="matchonly")

    test_dir = tempfile.mkdtemp()
    try:
        extra_args = [
            '--command', 'SCAN 0 MATCH matchonly:*',
            '--scan-incremental-iteration',
            '--pipeline', '1',
        ]
        benchmark, run_config = _build_scan_benchmark(
            env, test_dir, extra_args, threads=1, clients=1, requests=50)
        ok = benchmark.run()

        stdout = _read_stdout(run_config)
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok)
            env.assertTrue('SCAN 0' in stdout)
            env.assertTrue('SCAN <cursor>' in stdout)

            json_path = os.path.join(run_config.results_dir, "mb.json")
            env.assertTrue(os.path.isfile(json_path))
            with open(json_path) as f:
                results = json.load(f)
            all_stats = results.get("ALL STATS", {})
            env.assertTrue("Scan 0s" in all_stats)
            env.assertTrue("Scan <cursor>s" in all_stats)
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


# ---------------------------------------------------------------------------
# Test: COUNT only (no MATCH or TYPE)
# ---------------------------------------------------------------------------

# Already covered by test_scan_incremental_basic (SCAN 0 COUNT 10)


# ---------------------------------------------------------------------------
# Test: TYPE filter only (no COUNT)
# ---------------------------------------------------------------------------

def test_scan_incremental_type_only(env):
    """Verify SCAN with only TYPE (no COUNT) works."""
    env.skipOnCluster()
    _preload_keys(env, count=200)

    test_dir = tempfile.mkdtemp()
    try:
        extra_args = [
            '--command', 'SCAN 0 TYPE string',
            '--scan-incremental-iteration',
            '--pipeline', '1',
        ]
        benchmark, run_config = _build_scan_benchmark(
            env, test_dir, extra_args, threads=1, clients=1, requests=50)
        ok = benchmark.run()

        stdout = _read_stdout(run_config)
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok)
            env.assertTrue('SCAN 0' in stdout)
            env.assertTrue('SCAN <cursor>' in stdout)

            json_path = os.path.join(run_config.results_dir, "mb.json")
            env.assertTrue(os.path.isfile(json_path))
            with open(json_path) as f:
                results = json.load(f)
            all_stats = results.get("ALL STATS", {})
            env.assertTrue("Scan 0s" in all_stats)
            env.assertTrue("Scan <cursor>s" in all_stats)
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


# ---------------------------------------------------------------------------
# Test: MATCH + COUNT (no TYPE)
# ---------------------------------------------------------------------------

# Already covered by test_scan_incremental_with_match (SCAN 0 MATCH scanmatch:* COUNT 10)


# ---------------------------------------------------------------------------
# Test: TYPE + COUNT (no MATCH)
# ---------------------------------------------------------------------------

def test_scan_incremental_with_type(env):
    """Verify SCAN with TYPE + COUNT works under incremental iteration."""
    env.skipOnCluster()
    _preload_keys(env, count=100)

    test_dir = tempfile.mkdtemp()
    try:
        extra_args = [
            '--command', 'SCAN 0 TYPE string COUNT 10',
            '--scan-incremental-iteration',
            '--pipeline', '1',
        ]
        benchmark, run_config = _build_scan_benchmark(
            env, test_dir, extra_args, threads=1, clients=1, requests=50)
        ok = benchmark.run()

        stdout = _read_stdout(run_config)
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok)
            env.assertTrue('SCAN 0' in stdout)
            env.assertTrue('SCAN <cursor>' in stdout)
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


# ---------------------------------------------------------------------------
# Test: MATCH + COUNT + TYPE (all options)
# ---------------------------------------------------------------------------

def test_scan_incremental_all_options(env):
    """Verify SCAN with MATCH, COUNT, and TYPE all combined."""
    env.skipOnCluster()
    _preload_keys(env, count=200, prefix="allopt")

    test_dir = tempfile.mkdtemp()
    try:
        extra_args = [
            '--command', 'SCAN 0 MATCH allopt:* COUNT 10 TYPE string',
            '--scan-incremental-iteration',
            '--pipeline', '1',
        ]
        benchmark, run_config = _build_scan_benchmark(
            env, test_dir, extra_args, threads=1, clients=1, requests=50)
        ok = benchmark.run()

        stdout = _read_stdout(run_config)
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok)
            env.assertTrue('SCAN 0' in stdout)
            env.assertTrue('SCAN <cursor>' in stdout)

            json_path = os.path.join(run_config.results_dir, "mb.json")
            env.assertTrue(os.path.isfile(json_path))
            with open(json_path) as f:
                results = json.load(f)
            all_stats = results.get("ALL STATS", {})
            env.assertTrue("Scan 0s" in all_stats)
            env.assertTrue("Scan <cursor>s" in all_stats)

            scan0_count = all_stats["Scan 0s"]["Count"]
            scan_cursor_count = all_stats["Scan <cursor>s"]["Count"]
            env.assertGreater(scan0_count, 0)
            env.assertGreater(scan_cursor_count, 0)
            env.assertEqual(scan0_count + scan_cursor_count, 50)
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


# ---------------------------------------------------------------------------
# Test: error - missing --command
# ---------------------------------------------------------------------------

def test_scan_incremental_error_no_command(env):
    """Verify error when --scan-incremental-iteration is used without --command."""
    env.skipOnCluster()
    test_dir = tempfile.mkdtemp()
    try:
        extra_args = [
            '--scan-incremental-iteration',
            '--pipeline', '1',
        ]
        benchmark, run_config = _build_scan_benchmark(
            env, test_dir, extra_args, threads=1, clients=1, requests=10)
        ok = benchmark.run()

        env.assertFalse(ok)
        stderr = _read_stderr(run_config)
        env.assertTrue('error' in stderr.lower(),
                       message="Expected error message about missing --command")
    finally:
        pass


# ---------------------------------------------------------------------------
# Test: error - non-SCAN command
# ---------------------------------------------------------------------------

def test_scan_incremental_error_non_scan_command(env):
    """Verify error when --scan-incremental-iteration is used with non-SCAN command."""
    env.skipOnCluster()
    test_dir = tempfile.mkdtemp()
    try:
        extra_args = [
            '--command', 'GET __key__',
            '--scan-incremental-iteration',
            '--pipeline', '1',
        ]
        benchmark, run_config = _build_scan_benchmark(
            env, test_dir, extra_args, threads=1, clients=1, requests=10)
        ok = benchmark.run()

        env.assertFalse(ok)
        stderr = _read_stderr(run_config)
        env.assertTrue('error' in stderr.lower(),
                       message="Expected error message about SCAN command required")
    finally:
        pass


# ---------------------------------------------------------------------------
# Test: error - pipeline > 1
# ---------------------------------------------------------------------------

def test_scan_incremental_error_pipeline_not_one(env):
    """Verify error when --scan-incremental-iteration is used with pipeline > 1."""
    env.skipOnCluster()
    test_dir = tempfile.mkdtemp()
    try:
        extra_args = [
            '--command', 'SCAN 0',
            '--scan-incremental-iteration',
        ]
        benchmark, run_config = _build_scan_benchmark(
            env, test_dir, extra_args, threads=1, clients=1, requests=10,
            pipeline=2)
        ok = benchmark.run()

        env.assertFalse(ok)
        stderr = _read_stderr(run_config)
        env.assertTrue('error' in stderr.lower(),
                       message="Expected error message about pipeline")
    finally:
        pass


# ---------------------------------------------------------------------------
# Test: large keyspace preloaded via memtier then incremental SCAN
# ---------------------------------------------------------------------------

def test_scan_incremental_large_keyspace(env):
    """Preload 10K keys via memtier, then run incremental SCAN over them."""
    env.skipOnCluster()
    key_count = 10000
    test_dir = tempfile.mkdtemp()

    # Phase 1: preload keys using memtier SET with P:P pattern
    preload_specs = {
        "name": env.testName,
        "args": [
            '--ratio=1:0',
            '--key-pattern=P:P',
            '--key-minimum=1',
            f'--key-maximum={key_count}',
            '--pipeline', '50',
        ],
    }
    addTLSArgs(preload_specs, env)
    preload_config = get_default_memtier_config(threads=2, clients=5,
                                                 requests='allkeys')
    add_required_env_arguments(preload_specs, preload_config, env,
                               env.getMasterNodesList())
    preload_run_config = RunConfig(test_dir, env.testName + "_preload",
                                   preload_config, {})
    ensure_clean_benchmark_folder(preload_run_config.results_dir)
    preload_benchmark = Benchmark.from_json(preload_run_config, preload_specs)
    preload_ok = preload_benchmark.run()
    env.assertTrue(preload_ok, message="Preload phase should succeed")

    # Verify keys were loaded
    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    total_keys = 0
    for conn in master_nodes_connections:
        total_keys += conn.dbsize()
    env.assertGreaterEqual(total_keys, key_count,
                           message=f"Expected at least {key_count} keys after preload")

    # Phase 2: incremental SCAN over the large keyspace
    scan_dir = tempfile.mkdtemp()
    try:
        num_requests = 200
        extra_args = [
            '--command', 'SCAN 0 COUNT 100',
            '--scan-incremental-iteration',
            '--pipeline', '1',
        ]
        benchmark, run_config = _build_scan_benchmark(
            env, scan_dir, extra_args, threads=1, clients=1,
            requests=num_requests)
        ok = benchmark.run()

        stdout = _read_stdout(run_config)
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok)
            env.assertTrue('SCAN 0' in stdout,
                           message="Expected 'SCAN 0' stat line in output")
            env.assertTrue('SCAN <cursor>' in stdout,
                           message="Expected 'SCAN <cursor>' stat line in output")

            # Verify JSON output has both command types with correct counts
            json_path = os.path.join(run_config.results_dir, "mb.json")
            env.assertTrue(os.path.isfile(json_path))
            with open(json_path) as f:
                results = json.load(f)
            all_stats = results.get("ALL STATS", {})
            env.assertTrue("Scan 0s" in all_stats,
                           message="Expected 'Scan 0s' key in JSON ALL STATS")
            env.assertTrue("Scan <cursor>s" in all_stats,
                           message="Expected 'Scan <cursor>s' key in JSON ALL STATS")

            scan0_count = all_stats["Scan 0s"]["Count"]
            scan_cursor_count = all_stats["Scan <cursor>s"]["Count"]
            env.assertGreater(scan0_count, 0)
            env.assertGreater(scan_cursor_count, 0)
            env.assertEqual(scan0_count + scan_cursor_count, num_requests,
                            message="SCAN 0 + SCAN <cursor> counts should sum to total requests")

            # With 10K keys and COUNT 100, continuations should outnumber initials
            env.assertGreater(scan_cursor_count, scan0_count,
                              message="With large keyspace, continuations should outnumber initial SCANs")
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


# ---------------------------------------------------------------------------
# Test: large keyspace with large COUNT
# ---------------------------------------------------------------------------

def test_scan_incremental_large_count(env):
    """Preload 10K keys via memtier, then SCAN with COUNT 1000."""
    env.skipOnCluster()
    key_count = 10000
    test_dir = tempfile.mkdtemp()

    # Phase 1: preload keys using memtier
    preload_specs = {
        "name": env.testName,
        "args": [
            '--ratio=1:0',
            '--key-pattern=P:P',
            '--key-minimum=1',
            f'--key-maximum={key_count}',
            '--pipeline', '50',
        ],
    }
    addTLSArgs(preload_specs, env)
    preload_config = get_default_memtier_config(threads=2, clients=5,
                                                 requests='allkeys')
    add_required_env_arguments(preload_specs, preload_config, env,
                               env.getMasterNodesList())
    preload_run_config = RunConfig(test_dir, env.testName + "_preload",
                                   preload_config, {})
    ensure_clean_benchmark_folder(preload_run_config.results_dir)
    preload_benchmark = Benchmark.from_json(preload_run_config, preload_specs)
    preload_ok = preload_benchmark.run()
    env.assertTrue(preload_ok, message="Preload phase should succeed")

    # Phase 2: SCAN with large COUNT (1000) - fewer iterations needed
    scan_dir = tempfile.mkdtemp()
    try:
        num_requests = 100
        extra_args = [
            '--command', 'SCAN 0 COUNT 1000',
            '--scan-incremental-iteration',
            '--pipeline', '1',
        ]
        benchmark, run_config = _build_scan_benchmark(
            env, scan_dir, extra_args, threads=1, clients=1,
            requests=num_requests)
        ok = benchmark.run()

        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok)

            json_path = os.path.join(run_config.results_dir, "mb.json")
            env.assertTrue(os.path.isfile(json_path))
            with open(json_path) as f:
                results = json.load(f)
            all_stats = results.get("ALL STATS", {})
            env.assertTrue("Scan 0s" in all_stats)
            env.assertTrue("Scan <cursor>s" in all_stats)

            scan0_count = all_stats["Scan 0s"]["Count"]
            scan_cursor_count = all_stats["Scan <cursor>s"]["Count"]
            env.assertEqual(scan0_count + scan_cursor_count, num_requests)

            # With COUNT 1000 over 10K keys, full iteration takes ~10 SCANs,
            # so SCAN 0 should fire more often than with COUNT 100
            env.assertGreater(scan0_count, 1,
                              message="With large COUNT, should complete multiple full iterations")
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


# ---------------------------------------------------------------------------
# Test: large keyspace with TYPE filter
# ---------------------------------------------------------------------------

def test_scan_incremental_large_keyspace_with_type(env):
    """Preload 10K keys via memtier, then SCAN with TYPE string filter."""
    env.skipOnCluster()
    key_count = 10000
    test_dir = tempfile.mkdtemp()

    # Phase 1: preload keys using memtier
    preload_specs = {
        "name": env.testName,
        "args": [
            '--ratio=1:0',
            '--key-pattern=P:P',
            '--key-minimum=1',
            f'--key-maximum={key_count}',
            '--pipeline', '50',
        ],
    }
    addTLSArgs(preload_specs, env)
    preload_config = get_default_memtier_config(threads=2, clients=5,
                                                 requests='allkeys')
    add_required_env_arguments(preload_specs, preload_config, env,
                               env.getMasterNodesList())
    preload_run_config = RunConfig(test_dir, env.testName + "_preload",
                                   preload_config, {})
    ensure_clean_benchmark_folder(preload_run_config.results_dir)
    preload_benchmark = Benchmark.from_json(preload_run_config, preload_specs)
    preload_ok = preload_benchmark.run()
    env.assertTrue(preload_ok, message="Preload phase should succeed")

    # Phase 2: SCAN with TYPE string filter
    scan_dir = tempfile.mkdtemp()
    try:
        num_requests = 100
        extra_args = [
            '--command', 'SCAN 0 TYPE string COUNT 100',
            '--scan-incremental-iteration',
            '--pipeline', '1',
        ]
        benchmark, run_config = _build_scan_benchmark(
            env, scan_dir, extra_args, threads=1, clients=1,
            requests=num_requests)
        ok = benchmark.run()

        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok)

            json_path = os.path.join(run_config.results_dir, "mb.json")
            env.assertTrue(os.path.isfile(json_path))
            with open(json_path) as f:
                results = json.load(f)
            all_stats = results.get("ALL STATS", {})
            env.assertTrue("Scan 0s" in all_stats)
            env.assertTrue("Scan <cursor>s" in all_stats)

            scan0_count = all_stats["Scan 0s"]["Count"]
            scan_cursor_count = all_stats["Scan <cursor>s"]["Count"]
            env.assertGreater(scan0_count, 0)
            env.assertGreater(scan_cursor_count, 0)
            env.assertEqual(scan0_count + scan_cursor_count, num_requests)
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


# ---------------------------------------------------------------------------
# Test: request count correctness
# ---------------------------------------------------------------------------

def test_scan_incremental_request_count(env):
    """Verify that all SCAN commands (initial + continuation) are counted."""
    env.skipOnCluster()
    _preload_keys(env, count=200)

    test_dir = tempfile.mkdtemp()
    try:
        num_requests = 50
        extra_args = [
            '--command', 'SCAN 0 COUNT 5',
            '--scan-incremental-iteration',
            '--pipeline', '1',
        ]
        benchmark, run_config = _build_scan_benchmark(
            env, test_dir, extra_args, threads=1, clients=1,
            requests=num_requests)
        ok = benchmark.run()

        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok)

            # Verify via JSON output that total ops matches expected
            json_path = os.path.join(run_config.results_dir, "mb.json")
            env.assertTrue(os.path.isfile(json_path))
            with open(json_path) as f:
                results = json.load(f)

            all_stats = results.get("ALL STATS", {})

            # The JSON "Totals" section has the total ops
            totals = all_stats.get("Totals", {})
            total_ops = totals.get("Count", 0)
            env.assertEqual(total_ops, num_requests)

            # Verify SCAN 0 and SCAN <cursor> are separate JSON entries
            env.assertTrue("Scan 0s" in all_stats,
                           message="Expected 'Scan 0s' key in JSON ALL STATS")
            env.assertTrue("Scan <cursor>s" in all_stats,
                           message="Expected 'Scan <cursor>s' key in JSON ALL STATS")

            # Verify individual counts sum to total
            scan0_count = all_stats["Scan 0s"]["Count"]
            scan_cursor_count = all_stats["Scan <cursor>s"]["Count"]
            env.assertEqual(scan0_count + scan_cursor_count, num_requests,
                            message="SCAN 0 + SCAN <cursor> counts should sum to total requests")
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass
