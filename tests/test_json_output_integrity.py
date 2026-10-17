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
import shutil
import subprocess
import sys
import tempfile
import threading

from include import (
    MEMTIER_BINARY,
    get_default_memtier_config,
    add_required_env_arguments,
    addTLSArgs,
    ensure_clean_benchmark_folder,
    debugPrintMemtierOnError,
)
from mb import Benchmark, RunConfig


# Tolerance for comparing an average latency with Accumulated / Count.
# "Accumulated Latency" is emitted with %lld (whole ms, truncated) and the
# averages with %.3f. Below Accumulated/Count we allow only a static slack,
# the larger of 0.01 ms and 2%. Above it we also allow the truncation and
# rounding bound computed in _latency_bounds().
ABS_TOLERANCE_MS = 0.01
REL_TOLERANCE = 0.02
# "Average Latency" is printed with %.3f, so it is rounded by up to 0.0005 ms.
AVG_PRINT_ROUNDING_MS = 0.0005


def _latency_bounds(derived, count):
    """Return (below, above): how far, in ms, a correct average may sit
    below or above Accumulated/Count.

    All fields come from the same whole-microsecond sum. "Accumulated
    Latency" is that sum truncated to whole ms by %lld, so Accumulated/Count
    is low by at most 0.999/count ms. A %.3f average is rounded by at most
    0.0005 ms. So a correct average is at most 0.999/count + 0.0005 ms above
    Accumulated/Count. We allow 1/count + 0.0005, which leaves at least
    0.001/count ms of headroom for float noise. For low-count buckets and
    sections (e.g. the trailing second of a short --test-time run, or Sets
    under a skewed --ratio) this term dominates the static floor.

    Below Accumulated/Count, a correct average can only be off by printing.
    The Time-Serie and Totals averages use double division, so they are off
    by up to 0.0005 ms of %.3f rounding. The Sets/Gets sections (and
    arbitrary-command sections with --command-stats-breakdown=line) use
    integer us division (totals_cmd::summarize), so they are exact multiples
    of 0.001 ms and at most (count-1)/(1000*count) ms low. The static floor
    covers both. These bounds do not hold for the default per-type
    arbitrary-command sections, whose average is an HDR histogram mean;
    _validate_run_section does not check those. Keeping this side tight is what catches an
    average truncated to an integer, for low counts too.

    AGGREGATED AVERAGE RESULTS is the exception: its averages are unweighted
    means of the per-run averages, while Count and Accumulated are per-run
    means of the totals (#703), so the invariant holds there only when every
    run has the same count (as with --requests). See #671.
    """
    below = max(ABS_TOLERANCE_MS, REL_TOLERANCE * derived)
    return below, max(below, 1.0 / count + AVG_PRINT_ROUNDING_MS)


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
    below, above = _latency_bounds(derived, count)
    env.assertTrue(
        -below <= latency - derived <= above,
        message=f"{section_name}: 'Latency' {latency:.4f} ms inconsistent "
                f"with Accumulated/Count ({accumulated}/{count} = "
                f"{derived:.4f} ms); allowed -{below:.4f}/+{above:.4f} ms")


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
        below, above = _latency_bounds(derived, count)
        if acc > 0:
            env.assertGreater(
                avg, 0.0,
                message=f"{section_name} Time-Serie[{bucket_key}]: "
                        f"Average Latency is 0 but Accumulated={acc}, "
                        f"Count={count}")
        env.assertTrue(
            -below <= avg - derived <= above,
            message=f"{section_name} Time-Serie[{bucket_key}]: "
                    f"Average Latency {avg:.4f} ms inconsistent with "
                    f"Accumulated/Count ({acc}/{count} = {derived:.4f} ms); "
                    f"allowed -{below:.4f}/+{above:.4f} ms")


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


def test_json_low_count_run_consistent(env):
    """A 3-request run exercises the low-count bound on real output: with so
    few ops, the %lld truncation of Accumulated Latency dominates."""
    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_benchmark(
            env, test_dir,
            extra_args=["--ratio=1:1", "--key-pattern=R:R", "--pipeline=1"],
            threads=1, clients=1, requests=3)
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
        shutil.rmtree(test_dir, ignore_errors=True)


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


def test_json_escapes_object_keys(env):
    """Object keys must be escaped like string values (issue #738).

    A --command name becomes a per-command key in the JSON output. A quote,
    backslash or control character in it used to be emitted verbatim,
    producing a document json.load() could not parse.
    """
    # The single quotes keep the parser from treating '"' as a quoting
    # character; they stay part of the command name.
    names = ['\'EC"HO\'', 'ECHO\\', '\'EC\x01HO\'']
    test_dir = tempfile.mkdtemp()
    benchmark, run_config = _build_benchmark(
        env, test_dir,
        extra_args=["--command=" + name + " x" for name in names]
        + ["--print-percentiles=50,99"],
        threads=1, clients=1, requests=10)
    ok = benchmark.run()
    failed_asserts = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")
        stats = _read_json(run_config, env)["ALL STATS"]
        for name in names:
            # Same label as command_row_name() in run_stats.cpp.
            key = name.lower().capitalize() + "s"
            env.assertTrue(key in stats,
                           message=f"{key!r} not in {list(stats)}")
    finally:
        if env.getNumberOfFailedAssertion() > failed_asserts:
            debugPrintMemtierOnError(run_config, env)


def _memtier_supports(option):
    """True when the memtier binary under test lists option in --help."""
    out = subprocess.run([MEMTIER_BINARY, "--help"], stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, timeout=30).stdout
    return option.encode() in out


def _arg(raw):
    """A command-line argument holding the bytes raw; Popen encodes the
    surrogate escapes back to the original (possibly non-UTF-8) bytes."""
    return os.fsdecode(raw)


def _row_name(raw):
    """command_row_name() of run_stats.cpp on bytes: ASCII-lowercase, ASCII-
    uppercase the first byte, append "s"."""
    name = bytes(b + 32 if 0x41 <= b <= 0x5A else b for b in raw)
    if name and 0x61 <= name[0] <= 0x7A:
        name = bytes([name[0] - 32]) + name[1:]
    return name + b"s"


def test_json_replaces_invalid_utf8(env):
    """User strings with invalid UTF-8 must not make the file invalid JSON
    (issue #745).

    --command names (object keys) and configuration values such as
    --key-prefix, --uri/--authenticate, --client-stats, --failed-keys-file
    and --prometheus-run-label were copied into --json-out-file byte for
    byte, so a non-UTF-8 byte made the file undecodable while memtier exited
    0. Each invalid sequence must now be written as U+FFFD, with the same
    maximal-subpart rule Python's errors="replace" uses, and valid
    multi-byte UTF-8 must be written unchanged.
    """
    env.skipOnVersionSmaller('6.0.0')  # The --uri user needs Redis ACLs.
    invalid = [
        b"A\xff",                  # byte that never starts a sequence
        b"B\xc3",                  # truncated 2-byte sequence
        b"C\xe2\x82",              # truncated 3-byte sequence
        b"D\x80",                  # lone continuation byte
        b"E\xc0\xaf",              # overlong 2-byte '/'
        b"F\xe0\x80\xaf",          # overlong 3-byte '/'
        b"G\xf0\x80\x80\xaf",      # overlong 4-byte '/'
        b"H\xed\xa0\x80",          # UTF-16 surrogate U+D800
        b"I\xf4\x90\x80\x80",      # U+110000, above U+10FFFF
        b"J\xf5\x80\x80\x80",      # lead byte beyond U+10FFFF
    ]
    valid = [
        "K\u043a\u043b\u044e\u0447",  # "Kключ"
        "L\u00e4\U0001F600",            # "Lä😀"
        # Boundary code points, which a too-strict decoder would reject.
        "M\u07ff",                       # DF BF, last 2-byte
        "N\u0800",                       # E0 A0 80, first 3-byte
        "O\ud7ff",                       # ED 9F BF, last before surrogates
        "P\uffff",                       # EF BF BF, last 3-byte
        "Q\U00010000",                   # F0 90 80 80, first 4-byte
        "R\U0010ffff",                   # F4 8F BF BF, last code point
    ]
    names = invalid + [v.encode() for v in valid]
    # Ends in a truncated sequence, so the string's NUL terminates it.
    prefix = b"k\xfe\xf0\x9f\x98"
    user, password = b"u\xff", b"p\xed\xa0\x80"

    test_dir = tempfile.mkdtemp()
    client_stats = os.path.join(os.fsencode(test_dir), b"cs\xff")
    failed_keys = os.path.join(os.fsencode(test_dir), b"fk\xc0")
    extra_args = [_arg(b"--command=" + name + b" x") for name in names]
    extra_args += ["--run-count=2", "--print-percentiles=50,99",
                   _arg(b"--key-prefix=" + prefix),
                   _arg(b"--client-stats=" + client_stats),
                   _arg(b"--failed-keys-file=" + failed_keys)]
    master_nodes = env.getMasterNodesList()
    if not env.isCluster() and not env.isUnixSocket():
        host = master_nodes[0].get("host", "127.0.0.1") or "127.0.0.1"
        extra_args.append(_arg(b"--uri=redis://" + user + b":" + password + b"@"
                               + "{}:{}".format(host, master_nodes[0]["port"]).encode()))
    else:
        extra_args.append(_arg(b"--authenticate=" + user + b":" + password))
    label = b"env=v\xf4\x90\x80\x80"
    with_prometheus = _memtier_supports("--prometheus-run-label")
    if with_prometheus:
        # Port 0: the kernel picks a free port for the exporter.
        extra_args += ["--prometheus-port=0",
                       _arg(b"--prometheus-run-label=" + label)]

    connections = env.getOSSMasterNodesConnectionList()
    benchmark, run_config = _build_benchmark(
        env, test_dir, extra_args=extra_args, threads=1, clients=1, requests=10)
    try:
        for conn in connections:
            conn.execute_command("ACL", "SETUSER", user, "on", b">" + password,
                                 "~*", "&*", "+@all")
        ok = benchmark.run()
    finally:
        for conn in connections:
            conn.execute_command("ACL", "DELUSER", user)
    failed_asserts = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")
        with open(os.path.join(run_config.results_dir, "mb.json"), "rb") as f:
            raw = f.read()
        try:
            text = raw.decode("utf-8")  # strict: JSON text must be UTF-8
        except UnicodeDecodeError as e:
            env.assertTrue(False, message=f"mb.json is not valid UTF-8: {e}")
            return
        doc = json.loads(text)
        for section in ("BEST RUN RESULTS", "WORST RUN RESULTS",
                        "AGGREGATED AVERAGE RESULTS (2 runs)"):
            stats = doc[section]
            for name in names:
                # The stats row ("Xs") and the percentile array ("X").
                for key in (_row_name(name), name):
                    key = key.decode("utf-8", "replace")
                    env.assertTrue(key in stats,
                                   message=f"{section}: {key!r} not in {list(stats)}")
        for v in valid:
            # Byte-identical, not \u-escaped.
            env.assertTrue(_row_name(v.encode()) in raw,
                           message=f"{v!r} not written as raw UTF-8")
        configuration = doc["configuration"]
        expected = {
            "key_prefix": prefix,
            "client_stats": client_stats,
            "failed_keys_file": failed_keys,
        }
        if with_prometheus:
            expected["prometheus-run-labels"] = label
        for field, value in expected.items():
            env.assertEqual(configuration[field], value.decode("utf-8", "replace"))
    finally:
        if env.getNumberOfFailedAssertion() > failed_asserts:
            debugPrintMemtierOnError(run_config, env)
        else:
            shutil.rmtree(test_dir, ignore_errors=True)


def test_json_invalid_utf8_keys_stay_unique(env):
    """Names that only differ in invalid UTF-8 must still get distinct keys.

    "A\\xff", "A\\xfe" and a literal "A\ufffd" are all written as "A\ufffd".
    Labels are deduplicated after the replacement, so each command keeps its
    own row and percentile array instead of json.load() silently keeping only
    the last of several equal keys.
    """
    names = [b"A\xff", b"A\xfe", "A\ufffd".encode()]
    test_dir = tempfile.mkdtemp()
    benchmark, run_config = _build_benchmark(
        env, test_dir,
        extra_args=[_arg(b"--command=" + name + b" x") for name in names],
        threads=1, clients=1, requests=10)
    ok = benchmark.run()
    failed_asserts = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")

        def no_duplicates(pairs):
            keys = [k for k, _ in pairs]
            env.assertEqual(len(keys), len(set(keys)),
                            message=f"duplicate JSON keys: {keys}")
            return dict(pairs)

        with open(os.path.join(run_config.results_dir, "mb.json"), "rb") as f:
            doc = json.loads(f.read().decode("utf-8"),
                             object_pairs_hook=no_duplicates)
        stats = doc["ALL STATS"]
        rows = [k for k, v in stats.items()
                if isinstance(v, dict) and "Count" in v and k != "Totals"]
        env.assertEqual(len(rows), len(names), message=f"rows: {rows}")
        env.assertEqual(sum(stats[k]["Count"] for k in rows),
                        stats["Totals"]["Count"])
    finally:
        if env.getNumberOfFailedAssertion() > failed_asserts:
            debugPrintMemtierOnError(run_config, env)
        else:
            shutil.rmtree(test_dir, ignore_errors=True)


def _assert_totals_bandwidth_is_sum_of_commands(env, section_name, section):
    """Totals KB/sec (and its RX/TX split) must equal the sum of the
    per-command rows. Each value is printed with %.2f, so allow a small
    rounding slack per row."""
    commands = [name for name, stats in section.items()
                if name != "Totals" and isinstance(stats, dict)
                and "KB/sec" in stats]
    env.assertGreater(len(commands), 0,
                      message=f"{section_name}: no per-command rows")
    for field in ("KB/sec", "KB/sec RX", "KB/sec TX"):
        expected = sum(float(section[name][field]) for name in commands)
        actual = float(section["Totals"][field])
        env.assertAlmostEqual(
            actual, expected, 0.01 * (len(commands) + 1),
            message=f"{section_name}: Totals '{field}' ({actual}) != sum of "
                    f"{commands} ({expected})")


def _aggregated_key(env, results):
    agg_keys = [k for k in results
                if k.startswith("AGGREGATED AVERAGE RESULTS")]
    env.assertEqual(len(agg_keys), 1,
                    message="Expected exactly one AGGREGATED section")
    return agg_keys[0]


def _assert_aggregated_totals_are_run_mean(env, results, run_count, fields):
    """Issue #577: each AGGREGATED Totals rate in `fields` must be the mean of
    the per-run values (needs --print-all-runs). Each value is printed with
    %.2f, so allow a small rounding slack."""
    agg_key = _aggregated_key(env, results)
    runs = [results.get(f"RUN #{i} RESULTS") for i in range(1, run_count + 1)]
    env.assertTrue(all(runs),
                   message=f"Expected RUN #1..#{run_count} RESULTS sections")
    if not all(runs):
        return
    totals = results[agg_key]["Totals"]
    for field in fields:
        values = [float(run["Totals"][field]) for run in runs]
        expected = sum(values) / run_count
        actual = float(totals[field])
        env.assertAlmostEqual(
            actual, expected, 0.02,
            message=f"{agg_key}: Totals '{field}' ({actual}) != mean of the "
                    f"per-run values {values} ({expected:.4f})")


def _mean_count(total, runs):
    """memtier's AGGREGATED count (#703): the per-run mean rounded to the
    nearest integer, except that a non-zero total never rounds to 0."""
    if total == 0:
        return 0
    return max(1, (total + runs // 2) // runs)


def _assert_aggregated_counts_are_run_mean(env, results, run_count, section,
                                           fields, slack=None):
    """Issue #703: each AGGREGATED count in results[...][section][field] must
    be the per-run mean of the counts, not their sum (needs --print-all-runs),
    and an integer like in every other section. `section` is a key or a tuple
    of nested keys. Without `slack` the value must be exactly _mean_count();
    with it, within `slack` of the exact mean (for a value each run prints
    truncated, like Accumulated Latency, or a Totals made of rounded rows)."""
    agg_key = _aggregated_key(env, results)
    runs = [results.get(f"RUN #{i} RESULTS") for i in range(1, run_count + 1)]
    env.assertTrue(all(runs),
                   message=f"Expected RUN #1..#{run_count} RESULTS sections")
    if not all(runs):
        return
    path = section if isinstance(section, tuple) else (section,)

    def lookup(stats):
        for key in path:
            stats = stats[key]
        return stats

    for field in fields:
        values = [lookup(run)[field] for run in runs]
        actual = lookup(results[agg_key])[field]
        env.assertTrue(
            isinstance(actual, int) and not isinstance(actual, bool),
            message=f"{agg_key}: {section} '{field}' ({actual!r}) is not an "
                    f"integer")
        if slack is None:
            expected = _mean_count(sum(values), run_count)
            env.assertEqual(
                actual, expected,
                message=f"{agg_key}: {section} '{field}' ({actual}) != rounded "
                        f"mean of the per-run values {values} ({expected})")
        else:
            expected = sum(values) / run_count
            env.assertLessEqual(
                abs(actual - expected), slack,
                message=f"{agg_key}: {section} '{field}' ({actual}) != mean of "
                        f"the per-run values {values} ({expected:.4f})")


def _assert_aggregated_totals_rx_tx(env, results, run_count):
    """Issue #577: the AGGREGATED Totals RX/TX split must be non-zero, match
    the per-command rows, add up to the combined KB/sec and be the mean of
    the per-run values."""
    agg_key = _aggregated_key(env, results)
    _assert_totals_bandwidth_is_sum_of_commands(env, agg_key, results[agg_key])
    totals = results[agg_key]["Totals"]
    kbs = float(totals["KB/sec"])
    rx = float(totals["KB/sec RX"])
    tx = float(totals["KB/sec TX"])
    env.assertGreater(rx, 0.0, message=f"{agg_key}: Totals 'KB/sec RX' is 0")
    env.assertGreater(tx, 0.0, message=f"{agg_key}: Totals 'KB/sec TX' is 0")
    env.assertAlmostEqual(
        rx + tx, kbs, 0.03,
        message=f"{agg_key}: Totals RX ({rx}) + TX ({tx}) != KB/sec ({kbs})")
    _assert_aggregated_totals_are_run_mean(
        env, results, run_count, ("KB/sec", "KB/sec RX", "KB/sec TX"))


def test_json_arbitrary_commands_totals_bandwidth(env):
    """Issue #566: in --command mode the arbitrary-command bytes were added
    to both the Totals RX and TX counters, so Totals KB/sec was about twice
    the sum of the per-command rows."""
    test_dir = tempfile.mkdtemp()
    benchmark, run_config = _build_benchmark(
        env, test_dir,
        extra_args=["--run-count=2", "--print-all-runs",
                    "--command=SET __key__ __data__", "--command=GET __key__"],
        threads=2, clients=4, requests=1000)
    ok = benchmark.run()
    failed_asserts = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")
        results = _read_json(run_config, env)
        for label in ("BEST RUN RESULTS", "WORST RUN RESULTS"):
            env.assertTrue(label in results,
                           message=f"Expected '{label}' in JSON output")
            _assert_totals_bandwidth_is_sum_of_commands(env, label,
                                                        results[label])
        _assert_aggregated_totals_rx_tx(env, results, 2)
    finally:
        if env.getNumberOfFailedAssertion() > failed_asserts:
            debugPrintMemtierOnError(run_config, env)


def test_json_aggregated_totals_rx_tx(env):
    """Issue #577: with --run-count the AGGREGATED Totals 'KB/sec RX' and
    'KB/sec TX' were never accumulated or averaged across runs, so they were
    always reported as 0.0. Three runs on two threads, so that a divisor of
    a literal 2 or of the thread count is not mistaken for the run count."""
    run_count = 3
    test_dir = tempfile.mkdtemp()
    benchmark, run_config = _build_benchmark(
        env, test_dir,
        extra_args=[f"--run-count={run_count}", "--print-all-runs",
                    "--ratio=1:1"],
        threads=2, clients=4, requests=1000)
    ok = benchmark.run()
    failed_asserts = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")
        results = _read_json(run_config, env)
        _assert_aggregated_totals_rx_tx(env, results, run_count)
        # No connection is broken here, so the averaged rate must stay 0.
        agg_key = _aggregated_key(env, results)
        env.assertEqual(
            float(results[agg_key]["Totals"]["Connection Errors/sec"]), 0.0,
            message=f"{agg_key}: Totals 'Connection Errors/sec' is not 0 "
                    f"without connection errors")
    finally:
        if env.getNumberOfFailedAssertion() > failed_asserts:
            debugPrintMemtierOnError(run_config, env)


def test_json_aggregated_connection_errors_sec(env):
    """Issue #577: the AGGREGATED Totals 'Connection Errors/sec' was the sum
    of the per-run rates instead of their mean. Kill memtier's connections
    throughout every run so each run records connection errors."""
    run_count = 3
    test_dir = tempfile.mkdtemp()
    benchmark, run_config = _build_benchmark(
        env, test_dir,
        extra_args=[f"--run-count={run_count}", "--print-all-runs",
                    "--ratio=1:1", "--reconnect-on-error",
                    "--max-reconnect-attempts=0"],
        threads=2, clients=2, test_time=2)
    connections = env.getOSSMasterNodesConnectionList()
    stop = threading.Event()

    def killer():
        while not stop.wait(0.25):
            for conn in connections:
                try:
                    conn.execute_command("CLIENT", "KILL", "TYPE", "normal",
                                         "SKIPME", "yes")
                except Exception:
                    pass

    killer_thread = threading.Thread(target=killer, daemon=True)
    killer_thread.start()
    try:
        ok = benchmark.run()
    finally:
        stop.set()
        killer_thread.join(timeout=5)
    failed_asserts = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")
        results = _read_json(run_config, env)
        errors = sum(int(results[f"RUN #{i} RESULTS"]["Totals"]
                         ["Connection Errors"])
                     for i in range(1, run_count + 1)
                     if f"RUN #{i} RESULTS" in results)
        env.assertGreater(errors, 0,
                          message="Expected connection errors from the "
                                  "CLIENT KILL loop")
        _assert_aggregated_totals_are_run_mean(
            env, results, run_count, ("Connection Errors/sec",))
        # Issue #703: the count was the sum of the per-run counts.
        _assert_aggregated_counts_are_run_mean(
            env, results, run_count, "Totals", ("Connection Errors",))
    finally:
        if env.getNumberOfFailedAssertion() > failed_asserts:
            debugPrintMemtierOnError(run_config, env)


def test_json_aggregated_duration_and_counts_are_run_mean(env):
    """Issue #703: the AGGREGATED AVERAGE Runtime said Start time 0 and a
    Total duration (and Finish time) equal to the SUM of the run durations,
    and its Count / Accumulated Latency were sums too, while the rates and
    latencies next to them are per-run means. Every value of the section must
    be the per-run mean: a --test-time run lasts test_time, and so must the
    average run."""
    run_count = 3
    test_time = 2
    test_dir = tempfile.mkdtemp()
    benchmark, run_config = _build_benchmark(
        env, test_dir,
        extra_args=[f"--run-count={run_count}", "--print-all-runs",
                    "--ratio=1:4"],
        threads=2, clients=2, test_time=test_time)
    ok = benchmark.run()
    failed_asserts = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")
        results = _read_json(run_config, env)
        agg_key = _aggregated_key(env, results)
        runs = [results.get(f"RUN #{i} RESULTS")
                for i in range(1, run_count + 1)]
        env.assertTrue(all(runs),
                       message=f"Expected RUN #1..#{run_count} RESULTS")
        if not all(runs):
            return
        runtime = results[agg_key]["Runtime"]
        durations = [run["Runtime"]["Total duration"] for run in runs]
        duration = runtime["Total duration"]
        # Each run's duration is printed truncated to the ms, and so is the
        # mean, so allow 2 ms.
        env.assertLessEqual(
            abs(duration - sum(durations) / run_count), 2,
            message=f"{agg_key}: Total duration {duration} ms is not the mean "
                    f"of the run durations {durations}; {runtime}")
        env.assertLessEqual(
            abs(duration - test_time * 1000), 200,
            message=f"{agg_key}: Total duration {duration} ms is not about "
                    f"--test-time {test_time} s; {runtime}")
        # Start time is a real timestamp, the start of the first run, and the
        # section stays self-consistent: Finish - Start == Total duration.
        env.assertEqual(
            runtime["Start time"], runs[0]["Runtime"]["Start time"],
            message=f"{agg_key}: Start time is not the first run's; {runtime}")
        env.assertEqual(
            runtime["Finish time"] - runtime["Start time"], duration,
            message=f"{agg_key}: Finish - Start != Total duration; {runtime}")
        for section in ("Sets", "Gets"):
            _assert_aggregated_counts_are_run_mean(
                env, results, run_count, section, ("Count",))
            _assert_aggregated_counts_are_run_mean(
                env, results, run_count, section, ("Accumulated Latency",),
                slack=1)
        # Totals is the sum of the rounded rows (Sets, Gets, Waits).
        _assert_aggregated_counts_are_run_mean(
            env, results, run_count, "Totals", ("Count",), slack=1.5)
        _assert_aggregated_counts_are_run_mean(
            env, results, run_count, "Totals", ("Accumulated Latency",),
            slack=3)
        _assert_aggregated_counts_are_run_mean(
            env, results, run_count, "Runtime", ("Unanswered requests",))
        _assert_totals_count_is_sum_of_rows(env, agg_key, results[agg_key])
        # The CPU block needs per-thread CPU accounting (RUSAGE_THREAD), so it
        # must be there on Linux.
        missing_cpu = [name for name in [agg_key] + [f"RUN #{i} RESULTS" for i in range(1, run_count + 1)]
                       if "CPU" not in results[name]]
        if sys.platform.startswith("linux"):
            env.assertEqual(missing_cpu, [], message="no CPU block on Linux")
        if not missing_cpu:
            for field in ("cpu_user_seconds", "cpu_sys_seconds",
                          "cpu_total_seconds", "cpu_wall_seconds"):
                values = [float(run["CPU"][field]) for run in runs]
                actual = float(results[agg_key]["CPU"][field])
                env.assertLessEqual(
                    abs(actual - sum(values) / run_count), 0.002,
                    message=f"{agg_key}: CPU '{field}' ({actual}) is not the "
                            f"mean of {values}")
    finally:
        if env.getNumberOfFailedAssertion() > failed_asserts:
            debugPrintMemtierOnError(run_config, env)
        shutil.rmtree(test_dir, ignore_errors=True)


def _command_rows(section):
    """The per-command rows of a results section (everything with a Count
    except Totals)."""
    return {name: row for name, row in section.items()
            if isinstance(row, dict) and "Count" in row and name != "Totals"}


def _assert_totals_count_is_sum_of_rows(env, section_name, section):
    """Issue #703: the AGGREGATED Totals Count and Accumulated Latency must
    be exactly the sum of the command rows; rounding Totals separately from
    the rows broke that."""
    rows = _command_rows(section)
    for field in ("Count", "Accumulated Latency"):
        expected = sum(row[field] for row in rows.values())
        actual = section["Totals"][field]
        env.assertEqual(
            actual, expected,
            message=f"{section_name}: Totals {field} ({actual}) != sum of the "
                    f"rows { {k: v[field] for k, v in rows.items()} } ({expected})")


def _check_aggregated_arbitrary_command_counts(env, breakdown, expected_rows):
    """Issue #703 with 9 --command lines of 8 types: each AGGREGATED row Count
    and the Per-Key Misses counts are the rounded per-run means, integers, and
    the Totals Count is exactly the sum of the rows."""
    env.skipOnCluster()  # MGET of random keys would cross slots
    run_count = 2
    # Two GET lines, so the default per-type breakdown merges two lines.
    commands = ["SET __key__ __data__", "GET __key__", "GET __key__",
                "MGET __key__ __key__ __key__", "EXISTS __key__",
                "STRLEN __key__", "TTL __key__", "TYPE __key__",
                "ECHO __data__"]
    env.flush()
    test_dir = tempfile.mkdtemp()
    benchmark, run_config = _build_benchmark(
        env, test_dir,
        extra_args=[f"--run-count={run_count}", "--print-all-runs",
                    f"--command-stats-breakdown={breakdown}",
                    "--key-minimum=1", "--key-maximum=50000"]
                   + [arg for c in commands
                      for arg in (f"--command={c}", "--command-key-pattern=R")],
        threads=2, clients=2, test_time=1)
    ok = benchmark.run()
    failed_asserts = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")
        results = _read_json(run_config, env)
        agg_key = _aggregated_key(env, results)
        rows = _command_rows(results[agg_key])
        env.assertEqual(len(rows), expected_rows,
                        message=f"{agg_key}: rows {list(rows)}")
        _assert_totals_count_is_sum_of_rows(env, agg_key, results[agg_key])
        for name in rows:
            _assert_aggregated_counts_are_run_mean(
                env, results, run_count, name, ("Count",))
        _assert_aggregated_counts_are_run_mean(
            env, results, run_count, "Totals", ("Count",),
            slack=0.5 * len(rows) + 0.5)
        # Most MGET keys of a 50000-key range are missing, so every run
        # records misses for each MGET position.
        per_key = results[agg_key].get("Per-Key Misses", {})
        mget = [k for k in per_key if k.startswith("MGET")]
        env.assertEqual(len(mget), 1, message=f"Per-Key Misses: {list(per_key)}")
        if len(mget) == 1:
            fields = [f for f in per_key[mget[0]] if f.startswith(("Total", "key["))]
            env.assertEqual(len(fields), 2 + 2 * 3, message=str(fields))
            env.assertGreater(float(per_key[mget[0]]["Total Misses"]), 0)
            key_fields = [f for f in fields if f.startswith("key[")]
            _assert_aggregated_counts_are_run_mean(
                env, results, run_count, ("Per-Key Misses", mget[0]), key_fields)
            # Each Total is the sum of its rounded key[i] values, like Totals
            # and its rows, and so within about one unit per key of the mean.
            for kind in ("Hits", "Misses"):
                keys_sum = sum(per_key[mget[0]][f] for f in key_fields
                               if f.endswith(" " + kind))
                env.assertEqual(per_key[mget[0]][f"Total {kind}"], keys_sum,
                                message=f"{agg_key}: MGET Total {kind} != sum "
                                        f"of the key[i] {kind}: {per_key[mget[0]]}")
                _assert_aggregated_counts_are_run_mean(
                    env, results, run_count, ("Per-Key Misses", mget[0]),
                    (f"Total {kind}",), slack=3)
    finally:
        if env.getNumberOfFailedAssertion() > failed_asserts:
            debugPrintMemtierOnError(run_config, env)
        shutil.rmtree(test_dir, ignore_errors=True)


def test_json_aggregated_arbitrary_command_counts_are_run_mean(env):
    """Default --command-stats-breakdown=command: one row per command type."""
    _check_aggregated_arbitrary_command_counts(env, "command", 8)


def test_json_aggregated_arbitrary_command_line_counts_are_run_mean(env):
    """--command-stats-breakdown=line: one row per --command line."""
    _check_aggregated_arbitrary_command_counts(env, "line", 9)


def test_json_aggregated_small_count_never_rounds_to_zero(env):
    """Issue #703: a count that is non-zero over the series must not print 0
    in the AGGREGATED section. One client sends GET then SET of one key per
    run, so only run 1's GET misses: 1 miss over 3 runs, whose rounded mean
    would be 0 next to a non-zero Misses/sec."""
    env.skipOnCluster()
    run_count = 3
    env.flush()
    test_dir = tempfile.mkdtemp()
    benchmark, run_config = _build_benchmark(
        env, test_dir,
        extra_args=[f"--run-count={run_count}", "--print-all-runs",
                    "--key-minimum=1", "--key-maximum=1",
                    "--command=GET __key__", "--command-key-pattern=S",
                    "--command=SET __key__ v", "--command-key-pattern=S"],
        threads=1, clients=1, requests=2)
    ok = benchmark.run()
    failed_asserts = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")
        results = _read_json(run_config, env)
        agg_key = _aggregated_key(env, results)
        misses = [results[f"RUN #{i} RESULTS"]["Per-Key Misses"]["GET"]["Total Misses"]
                  for i in range(1, run_count + 1)]
        env.assertEqual(misses, [1, 0, 0])
        per_key = results[agg_key]["Per-Key Misses"]["GET"]
        env.assertEqual(per_key["Total Misses"], 1, message=str(per_key))
        env.assertEqual(per_key["key[0] Misses"], 1, message=str(per_key))
        env.assertGreater(float(results[agg_key]["Gets"]["Misses/sec"]), 0)
        _assert_aggregated_counts_are_run_mean(
            env, results, run_count, ("Per-Key Misses", "GET"),
            ("Total Hits", "Total Misses", "key[0] Hits", "key[0] Misses"))
    finally:
        if env.getNumberOfFailedAssertion() > failed_asserts:
            debugPrintMemtierOnError(run_config, env)
        shutil.rmtree(test_dir, ignore_errors=True)


def test_json_aggregated_unanswered_requests_are_run_mean(env):
    """Issue #703: the AGGREGATED Runtime 'Unanswered requests' was the sum
    over the runs. Pause writes for the whole series, so every run abandons
    the SETs in flight when --test-time ends."""
    env.skipOnCluster()
    run_count = 2
    test_dir = tempfile.mkdtemp()
    benchmark, run_config = _build_benchmark(
        env, test_dir,
        extra_args=[f"--run-count={run_count}", "--print-all-runs",
                    "--ratio=1:0"],
        threads=1, clients=2, test_time=1)
    connections = env.getOSSMasterNodesConnectionList()
    for conn in connections:
        conn.execute_command("CLIENT", "PAUSE", "120000", "WRITE")
    try:
        ok = benchmark.run()
    finally:
        for conn in connections:
            conn.execute_command("CLIENT", "UNPAUSE")
    failed_asserts = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")
        results = _read_json(run_config, env)
        values = [results[f"RUN #{i} RESULTS"]["Runtime"]["Unanswered requests"]
                  for i in range(1, run_count + 1)]
        env.assertTrue(all(v > 0 for v in values),
                       message=f"Expected unanswered requests in every run: {values}")
        _assert_aggregated_counts_are_run_mean(
            env, results, run_count, "Runtime", ("Unanswered requests",))
    finally:
        if env.getNumberOfFailedAssertion() > failed_asserts:
            debugPrintMemtierOnError(run_config, env)
        shutil.rmtree(test_dir, ignore_errors=True)


def test_time_series_tolerance_covers_truncation_and_rounding(env):
    # Values from a TSAN CI failure: 6 ops whose true sum is just under 42 ms.
    # "Accumulated Latency" truncates to 41 and "Average Latency" (%.3f) rounds
    # to 7.000, so avg - acc/count is exactly 1/count (above the old 1/count
    # bound in floating point). memtier's output is consistent here; the
    # checker must accept it.
    # Bucket "6": 7 ops summing to 7.999 ms -> avg 1.142714 prints as 1.143,
    # acc truncates to 7. The gap 0.143 exceeds 1/7 (0.142857), so it is only
    # accepted thanks to the %.3f rounding term.
    section = {"Time-Serie": {"5": {"Count": 6,
                                    "Average Latency": "7.000",
                                    "Accumulated Latency": 41},
                              "6": {"Count": 7,
                                    "Average Latency": "1.143",
                                    "Accumulated Latency": 7}}}
    _assert_time_series_consistent(env, "synthetic.Sets", section)


def test_section_tolerance_covers_truncation(env):
    # A low-count section (e.g. Sets under a skewed --ratio on a short run):
    # 3 ops summing to 999 us -> Accumulated Latency truncates to 0 while
    # Latency/Average Latency print 0.333. Consistent output; the
    # section-level check must accept it like the per-second one does.
    section = {"Count": 3, "Latency": "0.333", "Average Latency": "0.333",
               "Accumulated Latency": 0}
    _assert_latency_consistent(env, "synthetic.Sets", section)


class _FailureCounter:
    """Stand-in env that counts failed assertions instead of failing."""

    def __init__(self):
        self.failures = 0

    def assertTrue(self, cond, message=None):
        self.failures += not cond

    def assertEqual(self, a, b, message=None):
        self.failures += a != b

    def assertGreater(self, a, b, message=None):
        self.failures += not a > b


def test_tolerance_rejects_truncated_average(env):
    # 3 ops summing to 4.2 ms: true avg 1.400, Accumulated Latency 4. An
    # average printed as an integer (1.000) is 0.333 ms below 4/3. The
    # symmetric 1/count slack accepted that; both checks must reject it.
    counter = _FailureCounter()
    _assert_latency_consistent(
        counter, "synthetic.Sets",
        {"Count": 3, "Latency": "1.000", "Average Latency": "1.000",
         "Accumulated Latency": 4})
    _assert_time_series_consistent(
        counter, "synthetic.Sets",
        {"Time-Serie": {"0": {"Count": 3, "Average Latency": "1.000",
                              "Accumulated Latency": 4}}})
    env.assertEqual(counter.failures, 2)


def test_tolerance_bounds_both_sides(env):
    counter = _FailureCounter()
    # Correct integer-us Sets output just below Accumulated/Count: 3 ops
    # summing to 2000 us print 0.666 against 2/3. Must be accepted.
    _assert_latency_consistent(
        counter, "synthetic.Sets",
        {"Count": 3, "Latency": "0.666", "Average Latency": "0.666",
         "Accumulated Latency": 2})
    env.assertEqual(counter.failures, 0)
    # Just above the upper bound: 100 ops, Accumulated 10 ms, so the bound is
    # max(0.01, 1/100 + 0.0005) = 0.0105. An average 0.0106 above 0.1 cannot
    # come from truncation plus rounding and must be rejected.
    _assert_latency_consistent(
        counter, "synthetic.Sets",
        {"Count": 100, "Latency": "0.1106", "Average Latency": "0.1106",
         "Accumulated Latency": 10})
    env.assertEqual(counter.failures, 1)
