"""
Tests for client staircase ramp-up feature.

Validates that --clients-start, --clients-step, and --step-duration work
correctly: correct CLI validation, proper JSON output structure, and that
the per-second active client counts follow the expected staircase pattern.
"""
import tempfile
import json
import re
import os
import subprocess
import time
import threading

from include import (
    get_default_memtier_config,
    add_required_env_arguments,
    addTLSArgs,
    ensure_clean_benchmark_folder,
    debugPrintMemtierOnError,
    MEMTIER_BINARY,
)
from mb import Benchmark, RunConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_staircase_benchmark(env, test_dir, clients, clients_start,
                                clients_step, step_duration, test_time,
                                threads=2, extra_args=None):
    """Build a benchmark configured for staircase ramp-up."""
    config = get_default_memtier_config(threads=threads, clients=clients,
                                        test_time=test_time)
    # Clear requests since we use test_time
    config['memtier_benchmark']['requests'] = None

    master_nodes_list = env.getMasterNodesList()

    args = [
        '--clients-start', str(clients_start),
        '--clients-step', str(clients_step),
        '--step-duration', str(step_duration),
        '--ratio', '1:1',
        '--key-prefix', 'staircase-test-',
    ]
    if extra_args:
        args.extend(extra_args)

    benchmark_specs = {"name": env.testName, "args": args}
    addTLSArgs(benchmark_specs, env)
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)

    benchmark = Benchmark.from_json(run_config, benchmark_specs)
    return benchmark, run_config


def _load_json(run_config):
    """Load and return the JSON results dict."""
    json_path = os.path.join(run_config.results_dir, "mb.json")
    with open(json_path) as f:
        return json.load(f)


def _expected_clients_at_second(ts, clients_start, clients_step,
                                 step_duration, clients_max):
    """Calculate expected clients per thread at a given second."""
    steps_done = ts // step_duration
    active = clients_start + steps_done * clients_step
    return min(active, clients_max)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_staircase_json_config_fields(env):
    """Verify JSON configuration section includes staircase parameters."""
    env.skipOnCluster()

    clients = 10
    clients_start = 2
    clients_step = 2
    step_duration = 2
    test_time = 8

    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=clients, clients_start=clients_start,
            clients_step=clients_step, step_duration=step_duration,
            test_time=test_time, threads=2)

        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

            results = _load_json(run_config)

            # Check configuration section has staircase fields
            cfg = results.get("configuration", {})
            env.assertEqual(cfg.get("clients"), clients,
                            message="JSON config 'clients' should be max target")
            env.assertEqual(cfg.get("clients_start"), clients_start,
                            message="JSON config should have clients_start")
            env.assertEqual(cfg.get("clients_step"), clients_step,
                            message="JSON config should have clients_step")
            env.assertEqual(cfg.get("step_duration"), step_duration,
                            message="JSON config should have step_duration")
            env.assertEqual(cfg.get("test_time"), test_time,
                            message="JSON config should have test_time")
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_staircase_active_clients_section_exists(env):
    """Verify JSON output contains 'Active Clients' section with entries."""
    env.skipOnCluster()

    clients = 10
    clients_start = 2
    clients_step = 2
    step_duration = 2
    test_time = 8

    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=clients, clients_start=clients_start,
            clients_step=clients_step, step_duration=step_duration,
            test_time=test_time, threads=2)

        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

            results = _load_json(run_config)
            all_stats = results.get("ALL STATS", {})

            # Active Clients section must exist
            env.assertTrue("Active Clients" in all_stats,
                           message="JSON ALL STATS should contain 'Active Clients'")

            ac = all_stats["Active Clients"]
            env.assertGreater(len(ac), 0,
                              message="Active Clients should have per-second entries")

            # Each entry must have the required fields
            for ts_str, entry in ac.items():
                env.assertTrue("Clients per thread" in entry,
                               message="Each Active Clients entry should have 'Clients per thread'")
                env.assertTrue("Total clients" in entry,
                               message="Each Active Clients entry should have 'Total clients'")
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_staircase_active_clients_pattern(env):
    """Verify per-second active client counts follow the staircase pattern.

    With clients=10, clients_start=2, clients_step=2, step_duration=3,
    test_time=15, threads=2:
      t=0-2: 2 clients/thread (4 total)
      t=3-5: 4 clients/thread (8 total)
      t=6-8: 6 clients/thread (12 total)
      t=9-11: 8 clients/thread (16 total)
      t=12-14: 10 clients/thread (20 total)
    """
    env.skipOnCluster()

    clients = 10
    clients_start = 2
    clients_step = 2
    step_duration = 3
    test_time = 15
    threads = 2

    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=clients, clients_start=clients_start,
            clients_step=clients_step, step_duration=step_duration,
            test_time=test_time, threads=threads)

        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

            results = _load_json(run_config)
            all_stats = results.get("ALL STATS", {})
            ac = all_stats.get("Active Clients", {})

            # Verify each second's client count matches the expected staircase
            for ts_str, entry in ac.items():
                ts = int(ts_str)
                expected_per_thread = _expected_clients_at_second(
                    ts, clients_start, clients_step, step_duration, clients)
                expected_total = expected_per_thread * threads

                env.assertEqual(entry["Clients per thread"], expected_per_thread,
                                message="At t={}s, expected {} clients/thread, got {}".format(
                                    ts, expected_per_thread, entry["Clients per thread"]))
                env.assertEqual(entry["Total clients"], expected_total,
                                message="At t={}s, expected {} total clients, got {}".format(
                                    ts, expected_total, entry["Total clients"]))

            # Verify staircase is monotonically non-decreasing
            prev = 0
            for ts_str in sorted(ac.keys(), key=int):
                cur = ac[ts_str]["Clients per thread"]
                env.assertTrue(cur >= prev,
                               message="Staircase should be non-decreasing: t={}, prev={}, cur={}".format(
                                   ts_str, prev, cur))
                prev = cur

            # Verify we start at clients_start
            if "0" in ac:
                env.assertEqual(ac["0"]["Clients per thread"], clients_start,
                                message="First second should have clients_start clients")

        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_staircase_total_clients_equals_per_thread_times_threads(env):
    """Verify Total clients = Clients per thread * threads for all entries."""
    env.skipOnCluster()

    clients = 12
    clients_start = 3
    clients_step = 3
    step_duration = 2
    test_time = 10
    threads = 3

    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=clients, clients_start=clients_start,
            clients_step=clients_step, step_duration=step_duration,
            test_time=test_time, threads=threads)

        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

            results = _load_json(run_config)
            ac = results.get("ALL STATS", {}).get("Active Clients", {})

            for ts_str, entry in ac.items():
                per_thread = entry["Clients per thread"]
                total = entry["Total clients"]
                env.assertEqual(total, per_thread * threads,
                                message="At t={}s, Total clients ({}) should be "
                                        "Clients per thread ({}) * threads ({})".format(
                                            ts_str, total, per_thread, threads))
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_staircase_max_clients_reached(env):
    """Verify clients reach the max value and stay there."""
    env.skipOnCluster()

    clients = 8
    clients_start = 2
    clients_step = 2
    step_duration = 2
    test_time = 12
    threads = 2

    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=clients, clients_start=clients_start,
            clients_step=clients_step, step_duration=step_duration,
            test_time=test_time, threads=threads)

        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

            results = _load_json(run_config)
            ac = results.get("ALL STATS", {}).get("Active Clients", {})

            # Max should be reached at step_duration * num_steps = 2 * 3 = 6s
            max_reached = False
            for ts_str in sorted(ac.keys(), key=int):
                ts = int(ts_str)
                per_thread = ac[ts_str]["Clients per thread"]
                if per_thread == clients:
                    max_reached = True
                # Once max is reached, it should stay at max
                if max_reached:
                    env.assertEqual(per_thread, clients,
                                    message="After reaching max, clients should stay at {} "
                                            "(got {} at t={}s)".format(clients, per_thread, ts))

            env.assertTrue(max_reached,
                           message="Should reach max clients ({}) within test_time".format(clients))
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_staircase_ops_produced_every_step(env):
    """Verify that ops are produced in every step of the staircase, proving
    that clients added at each step are actively sending requests."""
    env.skipOnCluster()

    clients = 10
    clients_start = 2
    clients_step = 4
    step_duration = 4
    test_time = 12
    threads = 2

    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=clients, clients_start=clients_start,
            clients_step=clients_step, step_duration=step_duration,
            test_time=test_time, threads=threads)

        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

            results = _load_json(run_config)
            all_stats = results.get("ALL STATS", {})
            totals_ts = all_stats.get("Totals", {}).get("Time-Serie", {})
            totals_count = all_stats.get("Totals", {}).get("Count", 0)

            # Overall ops should be non-zero
            env.assertGreater(totals_count, 0,
                              message="Total ops should be greater than zero")

            # Each second in the time-series should have ops
            for ts_str, entry in totals_ts.items():
                count = entry.get("Count", 0)
                env.assertGreater(count, 0,
                                  message="Ops at t={}s should be > 0".format(ts_str))
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_no_staircase_no_active_clients_section(env):
    """Verify that when staircase is NOT used, there is no Active Clients section."""
    env.skipOnCluster()

    config = get_default_memtier_config(threads=2, clients=5, test_time=3)
    config['memtier_benchmark']['requests'] = None

    master_nodes_list = env.getMasterNodesList()
    benchmark_specs = {
        "name": env.testName,
        "args": ['--ratio', '1:1', '--key-prefix', 'no-staircase-test-'],
    }
    addTLSArgs(benchmark_specs, env)
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    try:
        run_config = RunConfig(test_dir, env.testName, config, {})
        ensure_clean_benchmark_folder(run_config.results_dir)
        benchmark = Benchmark.from_json(run_config, benchmark_specs)

        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

            results = _load_json(run_config)
            all_stats = results.get("ALL STATS", {})

            env.assertFalse("Active Clients" in all_stats,
                            message="Non-staircase run should NOT have Active Clients section")

            # Also verify config section doesn't have staircase fields
            cfg = results.get("configuration", {})
            env.assertFalse("clients_start" in cfg,
                            message="Non-staircase config should not have clients_start")
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_staircase_clients_never_exceed_max(env):
    """Verify that clients per thread never exceed --clients max."""
    env.skipOnCluster()

    # Use a step size that doesn't divide evenly into max
    clients = 10
    clients_start = 3
    clients_step = 4   # 3 -> 7 -> 10 (capped, not 11)
    step_duration = 3
    test_time = 12
    threads = 2

    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=clients, clients_start=clients_start,
            clients_step=clients_step, step_duration=step_duration,
            test_time=test_time, threads=threads)

        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

            results = _load_json(run_config)
            ac = results.get("ALL STATS", {}).get("Active Clients", {})

            for ts_str, entry in ac.items():
                per_thread = entry["Clients per thread"]
                env.assertTrue(per_thread <= clients,
                               message="Clients per thread ({}) should never exceed max ({}) "
                                       "at t={}s".format(per_thread, clients, ts_str))
                env.assertTrue(per_thread >= clients_start,
                               message="Clients per thread ({}) should never be below start ({}) "
                                       "at t={}s".format(per_thread, clients_start, ts_str))

            # Verify the uneven step: should go 3 -> 7 -> 10 (not 11)
            expected_sequence = [3, 7, 10]
            seen_values = sorted(set(entry["Clients per thread"]
                                     for entry in ac.values()))
            env.assertEqual(seen_values, expected_sequence,
                            message="Expected staircase values {} but got {}".format(
                                expected_sequence, seen_values))
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def _count_memtier_connections(master_connections):
    """Count memtier client connections via CLIENT LIST across all masters."""
    total = 0
    for conn in master_connections:
        clients = conn.execute_command("CLIENT", "LIST")
        if isinstance(clients, bytes):
            clients = clients.decode('utf-8')

        for line in clients.split("\n"):
            if not line.strip():
                continue
            client_info = {}
            for part in line.split():
                if "=" in part:
                    key, value = part.split("=", 1)
                    client_info[key] = value
            # Count connections that are not the test framework's own connection
            # memtier uses 'cmd=set' or 'cmd=get' etc., while idle conns show 'cmd=client' or 'cmd=command'
            if "flags" in client_info and "S" not in client_info.get("flags", ""):
                total += 1
    # Subtract monitoring connections (one per master node for this test)
    total -= len(master_connections)
    return max(total, 0)


def test_staircase_actual_connections_via_client_list(env):
    """Verify actual Redis connections match expected staircase at each step.

    Runs the benchmark in the background and polls CLIENT LIST at the midpoint
    of each staircase step to verify the actual connection count matches the
    expected number from the staircase pattern.
    """
    env.skipOnCluster()

    clients = 6
    clients_start = 2
    clients_step = 2
    step_duration = 4
    test_time = 16
    threads = 2

    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=clients, clients_start=clients_start,
            clients_step=clients_step, step_duration=step_duration,
            test_time=test_time, threads=threads)

        master_connections = env.getOSSMasterNodesConnectionList()

        # Start benchmark in background
        stdout_f = open(os.path.join(run_config.results_dir, "mb.stdout"), "w")
        stderr_f = open(os.path.join(run_config.results_dir, "mb.stderr"), "w")
        process = subprocess.Popen(
            benchmark.args,
            stdout=stdout_f, stderr=stderr_f,
            cwd=run_config.results_dir)

        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            # Check at the midpoint of each step
            # Step 0 (t=0-4):  2 clients/thread -> 4 total
            # Step 1 (t=4-8):  4 clients/thread -> 8 total
            # Step 2 (t=8-16): 6 clients/thread -> 12 total
            checks = [
                (2, 2 * threads),    # mid step 0
                (6, 4 * threads),    # mid step 1
                (10, 6 * threads),   # mid step 2 (max)
            ]

            # Wait for initial connections to establish
            time.sleep(2)
            last_check_time = 2

            for check_time, expected_total in checks:
                # Sleep the delta from the last check time
                if check_time > last_check_time:
                    time.sleep(check_time - last_check_time)

                actual = _count_memtier_connections(master_connections)
                # Allow tolerance of ±2 for connection setup timing
                env.assertTrue(
                    abs(actual - expected_total) <= 2,
                    message="At ~{}s, expected ~{} connections, got {} "
                            "(tolerance ±2)".format(check_time, expected_total, actual))

                last_check_time = check_time

            # Wait for benchmark to finish
            process.wait(timeout=test_time + 10)
            ok = process.returncode == 0
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            stdout_f.close()
            stderr_f.close()
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_staircase_timeserie_aligned_with_active_clients(env):
    """Verify Time-Serie and Active Clients cover the same seconds and
    that per-second throughput data is correctly aligned.

    The Time-Serie 'Totals' section should have entries for the same
    seconds as the Active Clients section. Each second should have
    non-zero ops count, confirming that staircase clients' stats are
    merged into the correct time buckets.
    """
    env.skipOnCluster()

    clients = 8
    clients_start = 2
    clients_step = 2
    step_duration = 3
    test_time = 12
    threads = 2

    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=clients, clients_start=clients_start,
            clients_step=clients_step, step_duration=step_duration,
            test_time=test_time, threads=threads)

        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

            results = _load_json(run_config)
            all_stats = results.get("ALL STATS", {})
            ac = all_stats.get("Active Clients", {})
            totals_ts = all_stats.get("Totals", {}).get("Time-Serie", {})

            env.assertGreater(len(ac), 0,
                              message="Active Clients should have entries")
            env.assertGreater(len(totals_ts), 0,
                              message="Time-Serie should have entries")

            ac_seconds = set(ac.keys())
            ts_seconds = set(totals_ts.keys())

            # Every Active Clients second should have a Time-Serie entry
            for sec in sorted(ac_seconds, key=int):
                env.assertTrue(sec in ts_seconds,
                               message="Active Clients second {} should have "
                                       "a Time-Serie entry".format(sec))

            # Every Time-Serie second should have an Active Clients entry
            for sec in sorted(ts_seconds, key=int):
                env.assertTrue(sec in ac_seconds,
                               message="Time-Serie second {} should have "
                                       "an Active Clients entry".format(sec))

            # Verify ops exist for every second (aligned stats should
            # not have empty seconds in the middle of the run)
            for sec_str in sorted(ts_seconds, key=int):
                sec = int(sec_str)
                count = totals_ts[sec_str].get("Count", 0)
                env.assertGreater(count, 0,
                                  message="Ops at t={}s should be > 0 "
                                          "(aligned stats)".format(sec))

            # Verify the average latency exists for each second
            # (proves per-second stats were properly merged, not empty)
            for sec_str in sorted(ts_seconds, key=int):
                entry = totals_ts[sec_str]
                if entry.get("Count", 0) > 0:
                    env.assertTrue("Average Latency" in entry,
                                   message="t={}s with ops should have "
                                           "'Average Latency'".format(sec_str))
                    avg_lat = entry["Average Latency"]
                    env.assertGreater(avg_lat, 0,
                                      message="Average Latency at t={}s "
                                              "should be > 0".format(sec_str))

        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_staircase_per_step_ops_consistency(env):
    """Verify that within each staircase step, the per-second ops are
    consistent (not mixing data from different time periods).

    Within a single step, all seconds should have a similar ops count
    since the client count is stable. The coefficient of variation
    within each step should be reasonable (< 100%), proving that data
    from different absolute times is not being mixed into wrong buckets.
    """
    env.skipOnCluster()

    clients = 6
    clients_start = 2
    clients_step = 2
    step_duration = 4
    test_time = 12
    threads = 2

    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=clients, clients_start=clients_start,
            clients_step=clients_step, step_duration=step_duration,
            test_time=test_time, threads=threads)

        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

            results = _load_json(run_config)
            all_stats = results.get("ALL STATS", {})
            ac = all_stats.get("Active Clients", {})
            totals_ts = all_stats.get("Totals", {}).get("Time-Serie", {})

            # Group seconds by their step (by client count)
            # Exclude the last second — the benchmark may finish mid-second,
            # resulting in a partial count that would look like an outlier.
            all_secs = sorted(ac.keys(), key=int)
            last_sec = int(all_secs[-1]) if all_secs else -1
            steps = {}
            for sec_str in all_secs:
                if int(sec_str) == last_sec:
                    continue
                per_thread = ac[sec_str]["Clients per thread"]
                if per_thread not in steps:
                    steps[per_thread] = []
                if sec_str in totals_ts:
                    ops = totals_ts[sec_str].get("Count", 0)
                    steps[per_thread].append((int(sec_str), ops))

            # For each step with >= 2 seconds, verify ops are reasonably
            # consistent (no wild outliers from misaligned merging)
            for client_count, seconds_data in sorted(steps.items()):
                if len(seconds_data) < 2:
                    continue

                ops_list = [ops for _, ops in seconds_data]
                avg_ops = sum(ops_list) / len(ops_list)
                if avg_ops == 0:
                    continue

                # Each second's ops should be within 5x of the step average
                # (generous bound, but catches gross misalignment)
                for sec, ops in seconds_data:
                    env.assertTrue(
                        ops > avg_ops / 5,
                        message="At t={}s ({} clients/thread), ops={} is "
                                "suspiciously low vs step avg={:.0f}".format(
                                    sec, client_count, ops, avg_ops))

        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_staircase_runs_full_test_time(env):
    """Verify a staircase run lasts the full --test-time (issue #713).

    The staircase deadline used to compare whole seconds of wall clock time,
    so a run that started late in a second ended up to 1 s early. Start
    memtier half way through a second, where the old check ended the run
    after about test_time - 0.5 s.
    """
    env.skipOnCluster()

    test_time = 3
    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=3, clients_start=1, clients_step=1,
            step_duration=1, test_time=test_time, threads=1)

        frac = time.time() % 1
        time.sleep((1.5 - frac) % 1)
        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

            runtime = _load_json(run_config)["ALL STATS"]["Runtime"]
            duration = runtime["Total duration"]
            env.assertTrue(test_time * 1000 - 100 <= duration <= test_time * 1000 + 300,
                           message="Total duration {} ms should be about --test-time {} s "
                                   "(start {}, finish {})".format(
                                       duration, test_time, runtime["Start time"],
                                       runtime["Finish time"]))
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_staircase_no_step_at_deadline(env):
    """Verify no clients are added by the step due at --test-time.

    With --test-time 3 and --step-duration 1, the steps at 1 s and 2 s each
    add a client per thread, and the timer firing at 3 s must only stop. It
    runs on libevent's monotonic clock and can fire a hair before the
    deadline by the wall clock, so a check on the elapsed time connected one
    more client per thread in about a third of the runs. Count the
    connections the server received.
    """
    env.skipOnCluster()

    threads = 8
    test_time = 3
    expected = threads * 3  # clients_start + 2 steps
    # Opened before the first snapshot, so it is not counted.
    conn = env.getConnection()
    for run in range(3):
        test_dir = tempfile.mkdtemp()
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=5, clients_start=1, clients_step=1,
            step_duration=1, test_time=test_time, threads=threads)

        before = conn.info("stats")["total_connections_received"]
        ok = benchmark.run()
        received = conn.info("stats")["total_connections_received"] - before
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")
            env.assertEqual(received, expected,
                            message="run {}: server received {} connections, expected {} "
                                    "(no clients added at the deadline)".format(
                                        run, received, expected))

            # The last requests can land in a bucket at t = test_time. Its
            # Active Clients entry must not count a step that was never taken.
            ac = _load_json(run_config)["ALL STATS"]["Active Clients"]
            for ts_str, entry in ac.items():
                want = 1 + min(int(ts_str), test_time - 1)
                env.assertEqual(entry["Clients per thread"], want,
                                message="run {}: Active Clients at t={}s is {}, expected {}".format(
                                    run, ts_str, entry["Clients per thread"], want))
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)



def test_staircase_exits_at_test_time(env):
    """Verify a staircase run exits at --test-time, not at the next step.

    The step timer is persistent, so after the clients stopped at the
    deadline it kept the event loop running until its next tick: --test-time 2
    --step-duration 20 took 20 s of wall time (issue #728).

    Measure each worker thread's lifetime with the JSON "CPU" cpu_wall_seconds
    (the longest event-loop thread, so process startup does not count). It
    must end within a moment of --test-time. Without per-thread CPU accounting
    (non-Linux), fall back to the process wall time.
    """
    # (test_time, step_duration): longer than the run, not dividing it, and
    # dividing it.
    cases = [(2, 20), (5, 2), (4, 2), (5, 3)]
    for test_time, step_duration in cases:
        test_dir = tempfile.mkdtemp()
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=4, clients_start=1, clients_step=1,
            step_duration=step_duration, test_time=test_time, threads=2)

        start = time.monotonic()
        ok = benchmark.run()
        process_wall = time.monotonic() - start
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")
            if not ok:
                continue
            cpu = _load_json(run_config)["ALL STATS"].get("CPU", {})
            if "cpu_wall_seconds" in cpu:
                thread_wall = float(cpu["cpu_wall_seconds"])
                env.assertTrue(thread_wall < test_time + 0.5,
                               message="--test-time {} --step-duration {}: worker threads ran {:.3f} s".format(
                                   test_time, step_duration, thread_wall))
            else:
                env.assertTrue(process_wall < test_time + 3,
                               message="--test-time {} --step-duration {}: run took {:.2f} s "
                                       "of wall time".format(test_time, step_duration, process_wall))
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)


def test_staircase_progress_clients_match_json(env):
    """Verify the progress line's client count stops at the last step taken.

    With --test-time 3 --step-duration 1, the steps at 1 s and 2 s run and
    the one at 3 s does not. The final progress line used to count it anyway
    (4 conns per thread instead of 3); it must agree with the last JSON
    "Active Clients" entry.
    """
    env.skipOnCluster()

    test_dir = tempfile.mkdtemp()
    benchmark, run_config = _build_staircase_benchmark(
        env, test_dir, clients=6, clients_start=1, clients_step=1,
        step_duration=1, test_time=3, threads=1)

    ok = benchmark.run()
    failed_asserts = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark should exit successfully")
        with open(os.path.join(run_config.results_dir, "mb.stderr")) as f:
            stderr = f.read()
        progress = re.findall(r"\[RUN #1 [^\]]*\]\s+\d+ threads\s+(\d+) conns",
                                          stderr.replace("\r", "\n"))
        env.assertTrue(len(progress) > 0, message="no progress lines in stderr")
        ac = _load_json(run_config)["ALL STATS"]["Active Clients"]
        last_ts = max(ac, key=int)
        json_clients = ac[last_ts]["Clients per thread"]
        env.assertEqual(json_clients, 3, message="last Active Clients entry (t={}s)".format(last_ts))
        if progress:
            env.assertEqual(int(progress[-1]), json_clients,
                            message="last progress line shows {} conns, JSON Active Clients at t={}s is {}".format(
                                progress[-1], last_ts, json_clients))
    finally:
        if env.getNumberOfFailedAssertion() > failed_asserts:
            debugPrintMemtierOnError(run_config, env)
