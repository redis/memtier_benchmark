"""Regression tests for --retry-backoff-factor (issue #687).

--retry-backoff-factor must accept exactly the same inputs as
--reconnect-backoff-factor (#658): a finite, representable number >= 0, with
0 meaning constant backoff. nan, inf, overflow, empty and trailing-junk values
must fail during argument parsing, before any connection attempt.

Run with:
  TEST=test_cli_validation_retry_backoff.py ./tests/run_tests.sh
"""

import json
import os
import subprocess
import tempfile

from include import (
    MEMTIER_BINARY,
    add_required_env_arguments,
    addTLSArgs,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig


def _assert_benchmark_completes(env, factor):
    env.skipOnCluster()
    requests = 20
    args = ["--retry-on-error", "--retry-backoff-ms=1", "--ratio=1:0", "--hide-histogram"]
    if factor is not None:
        args.append("--retry-backoff-factor=" + factor)
    specs = {"name": env.testName, "args": args}
    config = get_default_memtier_config(threads=1, clients=1, requests=requests)
    addTLSArgs(specs, env)
    add_required_env_arguments(specs, config, env, env.getMasterNodesList())

    with tempfile.TemporaryDirectory() as directory:
        run_config = RunConfig(directory, env.testName, config, {})
        ensure_clean_benchmark_folder(run_config.results_dir)
        benchmark = Benchmark.from_json(run_config, specs)
        succeeded = benchmark.run()
        if not succeeded:
            debugPrintMemtierOnError(run_config, env)
        env.assertTrue(
            succeeded,
            message="retry backoff factor {!r} must run successfully".format(factor),
        )
        if not succeeded:
            return

        with open(os.path.join(run_config.results_dir, "mb.json")) as source:
            results = json.load(source)
        stats = results["ALL STATS"]
        env.assertEqual(stats["Sets"]["Count"], requests)
        env.assertEqual(stats["Totals"]["Count"], requests)
        recorded = results["configuration"]["retry_backoff_factor"]
        expected = 0.0 if factor is None else float(factor)
        env.assertEqual(float(recorded), expected, message=repr(recorded))


def test_retry_backoff_default_accepted(env):
    _assert_benchmark_completes(env, None)


def test_retry_backoff_zero_accepted(env):
    """0 is the documented constant-backoff value and must keep working."""
    _assert_benchmark_completes(env, "0")


def test_retry_backoff_positive_accepted(env):
    _assert_benchmark_completes(env, "2")


def test_retry_backoff_valid_values_parse(env):
    for value in ("0", "-0.0", "0.5", "1", "2", "1e308"):
        result = subprocess.run(
            [MEMTIER_BINARY, "--retry-backoff-factor=" + value, "--version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        label = "--retry-backoff-factor={!r}".format(value)
        env.assertEqual(result.returncode, 0, message=label + ": " + result.stderr)
        env.assertFalse("error: retry-backoff-factor" in result.stderr, message=label + ": " + result.stderr)


def test_retry_backoff_invalid_values_rejected(env):
    for value in (
        "-1", "-0.5", "", " ", "abc", "1junk", "0junk", "1 ",
        "nan", "NaN", "-nan", "inf", "+inf", "-inf", "infinity",
        "1e9999", "1e-9999", "-1e9999", "-1e-9999",
    ):
        # --version exits 0 immediately if the invalid value slips through,
        # so this stays independent of Redis.
        result = subprocess.run(
            [MEMTIER_BINARY, "--retry-backoff-factor=" + value, "--version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        label = "--retry-backoff-factor={!r}".format(value)
        env.assertEqual(result.returncode, 2, message=label + ": " + result.stderr)
        env.assertTrue(
            "error: retry-backoff-factor" in result.stderr,
            message=label + ": expected a parser diagnostic, got " + repr(result.stderr),
        )


def test_retry_backoff_ignores_errno_from_earlier_options(env):
    """An earlier option's strtoul overflow must not reject a valid factor."""
    result = subprocess.run(
        [
            MEMTIER_BINARY,
            "--max-reconnect-attempts=99999999999999999999999",
            "--retry-backoff-factor=1",
            "--version",
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    env.assertEqual(result.returncode, 0, message=result.stderr)
    env.assertFalse("error: retry-backoff-factor" in result.stderr, message=result.stderr)
