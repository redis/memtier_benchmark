"""
Regression tests for JSON percentile-key formatting in result_print_to_json().

Both the "Percentile Latencies" and "Time-Serie" JSON blocks built their
key with a fixed-decimal snprintf() into an undersized buffer
(char[8], sizeof(buf)-1 passed as the size). For --print-percentiles
values with three or more decimal places this silently truncated/rounded
distinct quantiles onto the same key -- e.g. 99.99 and 99.991 both
collapsed to "p99.99", and the second value silently overwrote the first
in the JSON object.

The fix keeps the historical fixed-decimal key ("p50.00", "p99.00",
"p99.90", ...) for any quantile that doesn't collide with another
requested quantile under that format, and only falls back to full
("p%.10g") precision for the specific quantiles that would otherwise
collide. This test pins both halves of that invariant:

  1. Deep, colliding percentile requests no longer lose data -- every
     requested quantile gets its own distinct, correctly-valued key.
  2. The common case (the default 50,99,99.9, which never collided) keeps
     the exact legacy key shape ("p50.00" etc.) that existing JSON
     consumers already depend on -- this fix must not be a breaking
     change for anyone not already hitting the collision bug.

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
            # Every requested quantile must be recoverable from some key --
            # not asserting an exact key string here since the whole point
            # of the fix is that only the colliding entries change shape;
            # this just confirms none of them silently vanished. Compare
            # numerically (not by string-stripping trailing zeros, which
            # would mangle a whole number like "50" into "5").
            target = float(p)
            matches = [k for k in percentile_keys if abs(float(k[1:]) - target) < 1e-6]
            env.assertTrue(
                len(matches) == 1,
                message=f"Expected exactly one key for quantile {p}, found {matches} "
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
    """Two quantiles can be close enough that even the "p%.10g" fallback
    can't tell them apart (they agree in their first 10 significant
    digits). This requires a second escalation to "p%.17g" -- a double's
    round-trip-exact precision -- so any two non-bit-identical doubles are
    still guaranteed distinct keys. This is a stricter version of the
    deep-tail test above: those percentiles only collided at the legacy
    (2-3 decimal) format and were already resolved by %.10g; these don't
    separate until the second escalation."""
    close_percentiles = "99.9,99.900000001,99.99999991,99.99999992"

    def check(results):
        pl = results["ALL STATS"]["Totals"]["Percentile Latencies"]
        percentile_keys = {k for k in pl if re.match(r"^p\d", k)}
        env.assertEqual(
            len(percentile_keys), 4,
            message=f"Expected 4 distinct percentile keys for near-identical "
                    f"quantiles, got {len(percentile_keys)}: {sorted(percentile_keys)}")

    _run_and_check(env, ["--print-percentiles={}".format(close_percentiles)], check)


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
