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
        tolerance = max(ABS_TOLERANCE_MS, REL_TOLERANCE * derived)
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
                     requests=5000):
    config = get_default_memtier_config(threads=threads, clients=clients,
                                        requests=requests)
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
