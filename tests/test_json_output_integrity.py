"""
Regression tests for the JSON output produced by --json-out-file.

These tests pin down the invariant that latency fields in the JSON output
are internally consistent. They were added after RED-191460, where the
"Totals" section reported "Latency": 1.000 (or 0.000 for sub-millisecond
workloads) regardless of the actual value, because the underlying field
was an integer that truncated the averaged result. Checking only that
Latency > 0 is not sufficient — the bug could re-emerge with a similarly
plausible-looking but wrong value. The invariant we check here is:

    Latency ~= Accumulated Latency / Count

for every section that exposes those fields (Sets, Gets, Totals, BEST/WORST/
AGGREGATED, and the per-second time-series buckets).

Run:
    TEST=test_json_output_integrity.py OSS_STANDALONE=1 ./tests/run_tests.sh
"""
import json
import os
import tempfile

from include import (
    get_default_memtier_config,
    add_required_env_arguments,
    addTLSArgs,
    ensure_clean_benchmark_folder,
    debugPrintMemtierOnError,
)
from mb import Benchmark, RunConfig


# Tolerance for the (Accumulated / Count) vs Latency comparison.
# Accumulated Latency is emitted with %lld (1 ms precision), Latency with
# %.3f (0.001 ms precision). For low op counts the rounding of Accumulated
# can dominate, so we accept the larger of an absolute 0.01 ms slack and
# a 2% relative gap.
ABS_TOLERANCE_MS = 0.01
REL_TOLERANCE = 0.02


def _assert_latency_consistent(env, section_name, section):
    """Verify Latency, Average Latency and Accumulated Latency agree.

    The invariant is: Latency == Average Latency == Accumulated / Count,
    within a small tolerance that accounts for the printf rounding used
    in the output. Catches any regression that stores the average as an
    integer or otherwise loses precision.
    """
    count = section.get("Count", 0)
    if count <= 0:
        # Empty sections (e.g., Sets when --ratio=0:1) are valid; nothing
        # to compare.
        return

    # Required fields. Their absence is itself a regression.
    for field in ("Latency", "Average Latency", "Accumulated Latency"):
        env.assertTrue(
            field in section,
            message=f"{section_name}: missing '{field}' in JSON output")

    latency = float(section["Latency"])
    avg_latency = float(section["Average Latency"])
    accumulated = float(section["Accumulated Latency"])
    derived = accumulated / count

    # Latency must equal Average Latency (they share a backing field today).
    env.assertEqual(
        latency, avg_latency,
        message=f"{section_name}: 'Latency' ({latency}) != 'Average Latency' "
                f"({avg_latency})")

    # Latency must be positive when there are ops with non-zero accumulated
    # time. The original RED-191460 bug surfaced as Latency=0 for sub-ms
    # workloads — this assertion catches that direction explicitly.
    if accumulated > 0:
        env.assertGreater(
            latency, 0.0,
            message=f"{section_name}: 'Latency' is 0 but Accumulated Latency "
                    f"({accumulated}) and Count ({count}) are non-zero — "
                    f"derived avg = {derived:.4f} ms")

    # Latency must match Accumulated/Count within tolerance. Catches the
    # bug where Latency was stored as an integer (e.g., 1.291 -> 1.000).
    tolerance = max(ABS_TOLERANCE_MS, REL_TOLERANCE * derived)
    env.assertTrue(
        abs(latency - derived) <= tolerance,
        message=f"{section_name}: 'Latency' {latency:.4f} ms inconsistent "
                f"with Accumulated/Count ({accumulated}/{count} = "
                f"{derived:.4f} ms); tolerance {tolerance:.4f} ms")


def _assert_time_series_consistent(env, section_name, section):
    """Verify each Time-Serie bucket is internally consistent."""
    ts = section.get("Time-Serie")
    if not ts:
        return
    for bucket_key, bucket in ts.items():
        count = bucket.get("Count", 0)
        if count <= 0:
            continue
        # Time-series buckets only emit "Average Latency", not "Latency".
        if "Average Latency" not in bucket or "Accumulated Latency" not in bucket:
            continue
        avg = float(bucket["Average Latency"])
        acc = float(bucket["Accumulated Latency"])
        derived = acc / count
        # Tolerance must include the quantization error on Accumulated Latency:
        # it's emitted with %lld (1 ms precision), so the reported value can
        # differ from the true sum by up to 1 ms. The derived avg is therefore
        # off by up to (1 / count) ms. For low-count buckets — e.g. the trailing
        # second of a short --test-time run with --reconnect-interval=1 — that
        # term dominates and the static 2% relative + 0.01 ms absolute floor
        # would false-positive (RED-197205 regression-test CI surfaced this).
        tolerance = max(ABS_TOLERANCE_MS, REL_TOLERANCE * derived, 1.0 / count)
        if acc > 0:
            env.assertGreater(
                avg, 0.0,
                message=f"{section_name} Time-Serie[{bucket_key}]: "
                        f"Average Latency is 0 but Accumulated={acc}, "
                        f"Count={count}")
        env.assertTrue(
            abs(avg - derived) <= tolerance,
            message=f"{section_name} Time-Serie[{bucket_key}]: "
                    f"Average Latency {avg:.4f} ms inconsistent with "
                    f"Accumulated/Count ({acc}/{count} = {derived:.4f} ms); "
                    f"tolerance {tolerance:.4f} ms")


def _validate_run_section(env, run_label, run_section):
    """Validate a top-level JSON run section (ALL STATS, BEST RUN..., etc)."""
    for sub in ("Sets", "Gets", "Totals"):
        if sub not in run_section:
            continue
        _assert_latency_consistent(env, f"{run_label}.{sub}", run_section[sub])
        _assert_time_series_consistent(env, f"{run_label}.{sub}",
                                       run_section[sub])


def _build_benchmark(env, test_dir, extra_args, threads=2, clients=5,
                     requests=5000, test_time=None):
    # memtier doesn't accept both --test-time and --requests; when the test
    # is time-bounded, drop the request count.
    if test_time is not None:
        requests = None
    config = get_default_memtier_config(threads=threads, clients=clients,
                                        requests=requests, test_time=test_time)
    benchmark_specs = {"name": env.testName, "args": extra_args}
    addTLSArgs(benchmark_specs, env)
    add_required_env_arguments(benchmark_specs, config, env,
                               env.getMasterNodesList())
    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)
    return Benchmark.from_json(run_config, benchmark_specs), run_config


def _read_json(run_config, env):
    json_path = os.path.join(run_config.results_dir, "mb.json")
    env.assertTrue(os.path.isfile(json_path),
                   message=f"Expected JSON file at {json_path}")
    with open(json_path) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_json_totals_latency_matches_accumulated_single_run(env):
    """RED-191460: Totals.Latency must be consistent with Accumulated/Count.

    The original bug stored the Totals average latency as an integer, so
    a real average of 1.291 ms appeared as 1.000, and a sub-millisecond
    average appeared as 0.000. We pick a workload that produces non-zero
    fractional sub-millisecond latency on a local Redis to exercise the
    sub-ms truncation path, which is the strictest regression check.
    """
    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_benchmark(
            env, test_dir,
            extra_args=["--ratio=1:1", "--key-pattern=R:R", "--pipeline=1"],
            threads=2, clients=5, requests=2000)
        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark exited non-zero")
            results = _read_json(run_config, env)
            env.assertTrue("ALL STATS" in results,
                           message="Expected 'ALL STATS' in JSON output")
            _validate_run_section(env, "ALL STATS", results["ALL STATS"])
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_json_totals_latency_matches_accumulated_set_only(env):
    """SET-only workload: Sets and Totals must agree, Gets must be empty."""
    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_benchmark(
            env, test_dir,
            extra_args=["--ratio=1:0", "--key-pattern=P:P", "--pipeline=1"],
            threads=2, clients=4, requests=2000)
        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok)
            results = _read_json(run_config, env)
            run = results["ALL STATS"]
            _validate_run_section(env, "ALL STATS", run)
            # Gets section, if present, must report zero ops.
            if "Gets" in run:
                env.assertEqual(run["Gets"].get("Count", 0), 0,
                                message="Expected zero GETs in SET-only run")
            # Totals must reflect the Sets work.
            env.assertGreater(run["Totals"]["Count"], 0)
            env.assertGreater(run["Totals"]["Latency"], 0.0)
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_json_totals_latency_matches_accumulated_get_only(env):
    """GET-only workload: Gets and Totals must agree, Sets must be empty."""
    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_benchmark(
            env, test_dir,
            extra_args=["--ratio=0:1", "--key-pattern=R:R", "--pipeline=1"],
            threads=2, clients=4, requests=2000)
        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok)
            results = _read_json(run_config, env)
            run = results["ALL STATS"]
            _validate_run_section(env, "ALL STATS", run)
            if "Sets" in run:
                env.assertEqual(run["Sets"].get("Count", 0), 0,
                                message="Expected zero SETs in GET-only run")
            env.assertGreater(run["Totals"]["Count"], 0)
            env.assertGreater(run["Totals"]["Latency"], 0.0)
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_reconnect_interval_does_not_zero_out_rates(env):
    """RED-197205: --reconnect-interval N must not zero out throughput rates.

    On v2.2.1 (and earlier), running with --reconnect-interval=1 produced a
    final summary where Ops/sec, Hits/sec, Misses/sec and KB/sec all rendered
    as 0.00 even though Count and Latency were correct. Root cause:
    client_group::merge_run_stats() iterated all clients (including those
    that were prepare()-d but never reached set_start_time()) and merged
    their zeroed m_start_time into the factorial average, dragging the
    aggregate m_start_time toward the epoch. With m_end_time correct but
    m_start_time near 0, test_duration_usec became a huge value and every
    `ops / test_duration_usec * 1000000` division collapsed to ~0.

    Fix (master, PR #350 / commit 0228bac): a run_stats::m_started atomic
    flag plus a has_started() guard in merge_run_stats() and
    aggregate_inst_histogram() that skips clients that never started.

    This test exercises the exact failing configuration from the bug report
    (multiple threads, multiple clients per thread, --reconnect-interval=1,
    --ratio=1:1) and asserts the rate fields are strictly positive. The
    Latency invariants are also re-checked in case a future fix in this
    area accidentally re-introduces the integer-truncation pattern.
    """
    # memtier refuses --reconnect-interval together with --cluster-mode:
    # "error: cluster mode dose not support reconnect-interval option".
    # The RED-197205 regression is not cluster-specific, so we only run
    # this check in single-endpoint mode.
    if env.isCluster():
        env.skip()
        return

    test_dir = tempfile.mkdtemp()
    try:
        # NB: the bug fires reliably with --test-time, not with -n. With
        # -n, all clients run to completion and set_start_time was reached
        # on every one of them, so the zero-state merge never happened.
        # With --test-time, the wall-clock cutoff occasionally leaves
        # late-spawning / reconnect-stuck clients in the zero-init state,
        # which is exactly the path PR #350 fixed.
        benchmark, run_config = _build_benchmark(
            env, test_dir,
            extra_args=["--ratio=1:1", "--key-pattern=R:R", "--pipeline=1",
                        "--reconnect-interval=1"],
            threads=4, clients=4, requests=None, test_time=5)
        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark exited non-zero")
            results = _read_json(run_config, env)
            env.assertTrue("ALL STATS" in results,
                           message="Expected 'ALL STATS' in JSON output")
            run = results["ALL STATS"]
            _validate_run_section(env, "ALL STATS", run)

            # Core regression assertions: with --reconnect-interval=1, the
            # rate fields must be > 0 (not the all-zero footprint of
            # RED-197205). We assert against the Totals section, which is
            # what the bug report cited verbatim.
            totals = run["Totals"]
            env.assertGreater(
                totals["Count"], 0,
                message="Totals.Count must be > 0 with --reconnect-interval=1")
            env.assertGreater(
                totals["Ops/sec"], 0.0,
                message="RED-197205 regression: Totals.Ops/sec is 0 with "
                        "--reconnect-interval=1 (Count={}, Latency={})".format(
                            totals.get("Count"), totals.get("Latency")))
            env.assertGreater(
                totals["KB/sec"], 0.0,
                message="RED-197205 regression: Totals.KB/sec is 0 with "
                        "--reconnect-interval=1")
            # With ratio=1:1 there's at least one GET per SET, so Misses/sec
            # must also be > 0 (keys are random and the SUT starts empty).
            env.assertGreater(
                totals["Misses/sec"], 0.0,
                message="RED-197205 regression: Totals.Misses/sec is 0 with "
                        "--reconnect-interval=1 and ratio=1:1")

            # And the per-command sections should also have non-zero rates,
            # since the bug zeroed everything uniformly via the shared
            # test_duration_usec.
            for cmd in ("Sets", "Gets"):
                if cmd in run and run[cmd].get("Count", 0) > 0:
                    env.assertGreater(
                        run[cmd]["Ops/sec"], 0.0,
                        message="RED-197205 regression: {}.Ops/sec is 0 with "
                                "--reconnect-interval=1".format(cmd))
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_json_multi_run_aggregated_sections_consistent(env):
    """With --run-count>1 we get BEST/WORST/AGGREGATED sections — each one
    must satisfy the same Latency invariants as ALL STATS does for a single
    run. Catches RED-191460 in the aggregated path and the related typo in
    totals::add() that conflated m_latency with m_total_latency.
    """
    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_benchmark(
            env, test_dir,
            extra_args=["--ratio=1:1", "--key-pattern=R:R", "--pipeline=1",
                        "--run-count=2"],
            threads=2, clients=4, requests=1500)
        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok)
            results = _read_json(run_config, env)
            # The expected sections for run-count=2.
            expected = ["BEST RUN RESULTS", "WORST RUN RESULTS"]
            for label in expected:
                env.assertTrue(label in results,
                               message=f"Expected '{label}' in JSON output")
                _validate_run_section(env, label, results[label])
            # The aggregated section name embeds the run count.
            agg_keys = [k for k in results
                        if k.startswith("AGGREGATED AVERAGE RESULTS")]
            env.assertEqual(len(agg_keys), 1,
                            message="Expected exactly one AGGREGATED section")
            _validate_run_section(env, agg_keys[0], results[agg_keys[0]])
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def _assert_worker_exception_stats(env, unknown, finalization_failure=False):
    """A failed worker must retain a finalized partial window, even on exceptions."""
    import shlex
    import shutil
    import subprocess
    import sys
    from pathlib import Path

    env.skipOnCluster()
    compiler = shlex.split(os.environ.get("CXX", "c++"))
    if (sys.platform != "linux" or not compiler or not shutil.which(compiler[0])
            or not shutil.which("ldd") or not shutil.which("pkg-config")):
        env.skip()
        return
    specs = {"name": env.testName, "args": ["--ratio=1:1", "--hide-histogram"]}
    addTLSArgs(specs, env)
    config = get_default_memtier_config(threads=2 if finalization_failure else 1,
                                        clients=1, requests=None, test_time=3)
    add_required_env_arguments(specs, config, env, env.getMasterNodesList())
    with tempfile.TemporaryDirectory() as directory:
        config = RunConfig(directory, env.testName, config, {})
        ensure_clean_benchmark_folder(config.results_dir)
        benchmark = Benchmark.from_json(config, specs)
        linked = subprocess.run(["ldd", benchmark.args[0]], capture_output=True, text=True, timeout=10)
        if linked.returncode != 0 or "libevent" not in linked.stdout:
            env.skip()  # Static executables cannot use this interposition test.
            return
        cflags = subprocess.run(["pkg-config", "--cflags", "libevent"], check=True,
                                capture_output=True, text=True, timeout=10).stdout
        library = Path(directory) / "worker_exception.so"
        subprocess.run(compiler + ["-std=c++11", "-shared", "-fPIC"] + shlex.split(cflags)
                       + [str(Path(__file__).with_name("worker_exception_injector.cpp")),
                          "-o", str(library), "-ldl"],
                       check=True, capture_output=True, timeout=30)
        # Keep the sanitizer runtime first when interposing into instrumented builds.
        runtimes = [line.split()[2] for line in linked.stdout.splitlines()
                    if line.strip().startswith(("libasan.so", "libtsan.so"))
                    and "=>" in line and len(line.split()) >= 3]
        # The fatal-path shim must own operator new, then forward to the
        # sanitizer allocator via RTLD_NEXT. Other cases keep runtimes first.
        preloads = ([str(library)] + runtimes if finalization_failure
                    else runtimes + [str(library)])
        if os.environ.get("LD_PRELOAD"):
            preloads.append(os.environ["LD_PRELOAD"])
        child_env = dict(os.environ, LD_PRELOAD=":".join(preloads))
        if finalization_failure and any("libasan" in runtime for runtime in runtimes):
            # Only relax preload ordering for this forwarding test shim;
            # preserve leak/error checks and every other caller option.
            child_env["ASAN_OPTIONS"] = child_env.get("ASAN_OPTIONS", "") + ":verify_asan_link_order=0"
        child_env.pop("MEMTIER_TEST_UNKNOWN_EXCEPTION", None)
        child_env.pop("MEMTIER_TEST_FINALIZATION_FAILURE", None)
        if finalization_failure:
            child_env["MEMTIER_TEST_FINALIZATION_FAILURE"] = "1"
        if unknown:
            child_env["MEMTIER_TEST_UNKNOWN_EXCEPTION"] = "1"
        result = subprocess.run(benchmark.args, env=child_env, capture_output=True, text=True, timeout=15)
        env.assertEqual(result.returncode, 1)
        expected = "caught unknown exception" if unknown else "caught exception: injected worker exception"
        env.assertIn(expected, result.stderr)
        env.assertNotIn("Restarting thread", result.stderr)
        if finalization_failure:
            env.assertIn("unable to finalize statistics; aborting benchmark", result.stderr)
            env.assertNotIn("test-only exit cleanup ran", result.stderr)
            # Immediate fatal termination need not flush/finish the JSON document,
            # but must not publish unfinalized ALL STATS as a usable result.
            with open(os.path.join(config.results_dir, "mb.json")) as output:
                env.assertNotIn('"ALL STATS"', output.read())
            return
        with open(os.path.join(config.results_dir, "mb.json")) as output:
            stats = json.load(output)["ALL STATS"]
        runtime = stats["Runtime"]
        env.assertGreater(runtime["Finish time"], runtime["Start time"])
        env.assertGreater(runtime["Total duration"], 0)
        env.assertLess(runtime["Total duration"], 3000)
        total = stats["Totals"]["Count"]
        env.assertGreater(total, 0)
        env.assertEqual(stats["Sets"]["Count"] + stats["Gets"]["Count"], total)
        for command in ("Sets", "Gets", "Totals"):
            env.assertEqual(sum(bucket["Count"] for bucket in stats[command]["Time-Serie"].values()),
                            stats[command]["Count"])


def test_worker_exception_finalizes_partial_json(env):
    _assert_worker_exception_stats(env, unknown=False)


def test_worker_unknown_exception_finalizes_partial_json(env):
    _assert_worker_exception_stats(env, unknown=True)


def test_worker_finalization_failure_skips_process_cleanup(env):
    _assert_worker_exception_stats(env, unknown=True, finalization_failure=True)


def test_json_configuration_escapes_string_values(env):
    """String values in the "configuration" section must be valid JSON.

    A quote or backslash in a configuration value (e.g. --key-prefix, or an
    --out-file path containing a backslash) used to be emitted verbatim,
    producing a document json.load() could not parse.
    """
    prefix = 'q"b\\s:'
    test_dir = tempfile.mkdtemp()
    benchmark, run_config = _build_benchmark(
        env, test_dir, extra_args=["--ratio=1:1", "--key-prefix=" + prefix],
        threads=1, clients=1, requests=100)
    ok = benchmark.run()
    failed_asserts = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")
        configuration = _read_json(run_config, env)["configuration"]
        env.assertEqual(configuration["key_prefix"], prefix)
        env.assertEqual(os.path.normcase(configuration["out_file"]),
                        os.path.normcase(os.path.join(run_config.results_dir, "mb.stdout")))
    finally:
        if env.getNumberOfFailedAssertion() > failed_asserts:
            debugPrintMemtierOnError(run_config, env)
