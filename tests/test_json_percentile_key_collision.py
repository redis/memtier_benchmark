"""
Regression tests for JSON percentile-key formatting in result_print_to_json().

Both the "Percentile Latencies" and "Time-Serie" JSON blocks built their
key with a fixed-decimal snprintf() into an undersized buffer
(char[8], sizeof(buf)-1 passed as the size). The format rounds and the
buffer then truncates, so for --print-percentiles values with three or
more decimal places the key names a different number than the value it
holds: 99.999 is keyed "p99.99" or "p100.0". Where two requested
quantiles landed on the same wrecked key -- 99.99 and 99.991 both
"p99.99" -- the second value silently overwrote the first.

The fix keeps the historical fixed-decimal key ("p50.00", "p99.00",
"p99.90", ...) for every quantile it can still name exactly, and escalates
to "p%.10g", then "p%.17g", only for the ones it cannot. These tests pin
both halves of that invariant:

  1. Every requested quantile gets a key that parses back to it, and
     therefore a distinct one -- whether or not it collided with another
     request, since a mislabelled key is wrong on its own.
  2. Keys that already named their quantile correctly do not move, and do
     not move because of what else was requested alongside them. The
     default (50,99,99.9) is byte-for-byte unchanged.

Run:
    TEST=test_json_percentile_key_collision.py OSS_STANDALONE=1 ./tests/run_tests.sh
"""
import json
import os
import re
import tempfile

from include import (
    get_default_memtier_config,
    add_required_env_arguments,
    addTLSArgs,
    ensure_clean_benchmark_folder,
    debugPrintMemtierOnError,
)
from mb import Benchmark, RunConfig


def _build_benchmark(env, test_dir, extra_args, threads=2, clients=4, requests=2000):
    config = get_default_memtier_config(threads=threads, clients=clients, requests=requests)
    benchmark_specs = {"name": env.testName, "args": extra_args}
    addTLSArgs(benchmark_specs, env)
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)
    return Benchmark.from_json(run_config, benchmark_specs), run_config


def _read_json(run_config, env):
    json_path = os.path.join(run_config.results_dir, "mb.json")
    env.assertTrue(os.path.isfile(json_path), message=f"Expected JSON file at {json_path}")
    with open(json_path) as f:
        return json.load(f)


def _run_and_check(env, extra_args, check):
    test_dir = tempfile.mkdtemp()
    benchmark, run_config = _build_benchmark(env, test_dir, extra_args)
    ok = benchmark.run()
    failed_asserts = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")
        results = _read_json(run_config, env)
        check(results)
    finally:
        if env.getNumberOfFailedAssertion() > failed_asserts:
            debugPrintMemtierOnError(run_config, env)


# The PR's own repro. Under the old fixed 2/3-decimal format, only the last
# two of these six actually collide (99.99999 and 99.999999 both round/
# truncate to "p100.0") -- the other four (including 99.99) are already
# distinct; the test still requires all six to survive as separate keys.
DEEP_PERCENTILES = "50,99,99.9,99.99,99.99999,99.999999"


def test_percentile_keys_distinct_for_deep_tail_totals(env):
    """Totals."Percentile Latencies" must carry one distinct key per
    requested quantile, even for a request that used to collide."""
    def check(results):
        pl = results["ALL STATS"]["Totals"]["Percentile Latencies"]
        # "Percentile Latencies" also nests an unrelated "Histogram log
        # format" object (pre-existing, unrelated to this fix) -- restrict
        # to keys that actually look like a percentile ("p" + digit).
        percentile_keys = {k: v for k, v in pl.items() if re.match(r"^p\d", k)}
        requested = DEEP_PERCENTILES.split(",")
        env.assertEqual(
            len(percentile_keys), len(requested),
            message=f"Expected {len(requested)} distinct percentile keys, "
                    f"got {len(percentile_keys)}: {sorted(percentile_keys.keys())}")
        for p in requested:
            # Not asserting exact key strings: which quantiles keep the legacy
            # shape is the implementation detail under test elsewhere. What
            # every key must do is parse back to the quantile it holds, which
            # is also what makes two of them impossible to confuse.
            target = float(p)
            matches = [k for k in percentile_keys if float(k[1:]) == target]
            env.assertTrue(
                len(matches) == 1,
                message=f"Expected exactly one key naming quantile {p}, found {matches} "
                        f"in {sorted(percentile_keys.keys())}")

    _run_and_check(env, ["--print-percentiles={}".format(DEEP_PERCENTILES)], check)


def test_percentile_keys_distinct_for_deep_tail_time_serie(env):
    """Every non-empty Time-Serie bucket must also carry one distinct key
    per requested quantile -- this block had the identical defect."""
    def check(results):
        ts = results["ALL STATS"]["Totals"]["Time-Serie"]
        requested = DEEP_PERCENTILES.split(",")
        seen_a_populated_bucket = False
        for bucket in ts.values():
            if bucket.get("Count", 0) <= 0:
                continue
            seen_a_populated_bucket = True
            percentile_keys = {k for k in bucket if re.match(r"^p\d", k)}
            env.assertEqual(
                len(percentile_keys), len(requested),
                message=f"Time-Serie bucket: expected {len(requested)} distinct "
                        f"percentile keys, got {len(percentile_keys)}: "
                        f"{sorted(percentile_keys)}")
        env.assertTrue(seen_a_populated_bucket,
                       message="No populated Time-Serie bucket found to check")

    _run_and_check(env, ["--print-percentiles={}".format(DEEP_PERCENTILES)], check)


def test_percentile_keys_distinct_even_when_full_precision_also_collides(env):
    """Two quantiles can be close enough that they agree in their first 10
    significant digits, so "p%.10g" still can't name either of them
    exactly. That forces the second escalation to "p%.17g" -- a double's
    round-trip-exact precision -- so any two non-bit-identical doubles are
    still guaranteed distinct keys.

    Also pins that escalation is not contagious: 99.9 is named exactly by
    the legacy "p99.90", so it keeps that key even though 99.900000001
    sits right next to it and has to go all the way to %.17g. A key that
    was already correct must not change shape because of its neighbours."""
    close_percentiles = "99.9,99.900000001,99.99999991,99.99999992"

    def check(results):
        pl = results["ALL STATS"]["Totals"]["Percentile Latencies"]
        percentile_keys = {k for k in pl if re.match(r"^p\d", k)}
        env.assertEqual(
            len(percentile_keys), 4,
            message=f"Expected 4 distinct percentile keys for near-identical "
                    f"quantiles, got {len(percentile_keys)}: {sorted(percentile_keys)}")
        env.assertTrue(
            "p99.90" in percentile_keys,
            message=f"99.9 is named exactly by its legacy key and must keep it "
                    f"regardless of neighbouring quantiles, got {sorted(percentile_keys)}")

    _run_and_check(env, ["--print-percentiles={}".format(close_percentiles)], check)


# 99.9999 requested alone collides with nothing, so a collision-driven fix
# leaves it untouched -- but the legacy formats still round it up and
# truncate it to "p100.0", labelling a deep percentile as the maximum.
# Nothing is missing from the JSON and no key is duplicated; the document
# is just silently wrong, which is why the invariant has to be "the key
# names its quantile" rather than "the key is unique".
LONE_DEEP_PERCENTILE = "99.9999"


def test_percentile_key_names_its_quantile_without_any_collision(env):
    """A single deep percentile must still be keyed by the quantile it
    holds, not by whatever the legacy format rounded it to."""
    def check(results):
        totals = results["ALL STATS"]["Totals"]
        target = float(LONE_DEEP_PERCENTILE)

        def assert_named_exactly(keys, where):
            percentile_keys = {k for k in keys if re.match(r"^p\d", k)}
            env.assertEqual(
                len(percentile_keys), 1,
                message=f"{where}: expected exactly one percentile key, "
                        f"got {sorted(percentile_keys)}")
            key = percentile_keys.pop()
            env.assertEqual(
                float(key[1:]), target,
                message=f"{where}: key '{key}' does not name the requested "
                        f"quantile {LONE_DEEP_PERCENTILE} it holds")

        assert_named_exactly(totals["Percentile Latencies"].keys(), "Percentile Latencies")

        checked_a_bucket = False
        for bucket in totals["Time-Serie"].values():
            if bucket.get("Count", 0) <= 0:
                continue
            checked_a_bucket = True
            assert_named_exactly(bucket.keys(), "Time-Serie bucket")
        env.assertTrue(checked_a_bucket, message="No populated Time-Serie bucket found to check")

    _run_and_check(env, ["--print-percentiles={}".format(LONE_DEEP_PERCENTILE)], check)


def test_percentile_keys_backward_compatible_for_default_config(env):
    """The default percentile set (50,99,99.9) never collided under the old
    format -- this fix must not rename these keys. Pins the exact legacy
    key strings existing JSON consumers depend on."""
    def check(results):
        totals = results["ALL STATS"]["Totals"]
        pl = totals["Percentile Latencies"]
        for legacy_key in ("p50.00", "p99.00", "p99.90"):
            env.assertTrue(
                legacy_key in pl,
                message=f"Expected legacy key '{legacy_key}' in Percentile Latencies, "
                        f"got {sorted(pl.keys())} -- default config must not change key shape")

        ts = totals["Time-Serie"]
        checked_a_bucket = False
        for bucket in ts.values():
            if bucket.get("Count", 0) <= 0:
                continue
            checked_a_bucket = True
            for legacy_key in ("p50.00", "p99.00", "p99.90"):
                env.assertTrue(
                    legacy_key in bucket,
                    message=f"Expected legacy key '{legacy_key}' in Time-Serie bucket, "
                            f"got {sorted(k for k in bucket if k.startswith('p'))}")
        env.assertTrue(checked_a_bucket, message="No populated Time-Serie bucket found to check")

    # No --print-percentiles: exercises the actual default ("50,99,99.9").
    _run_and_check(env, [], check)
