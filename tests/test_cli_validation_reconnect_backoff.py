"""Regression tests for --reconnect-backoff-factor (issue #658).

Explicit zero must work like the documented default. Accepted factors run a
small benchmark against the RLTest server and produce valid JSON; invalid
values must fail during argument parsing, before any connection attempt.

Run with:
  TEST=test_cli_validation_reconnect_backoff.py ./tests/run_tests.sh
"""

import json
import os
import re
import socket
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
    args = ["--reconnect-on-error", "--ratio=1:0", "--hide-histogram"]
    if factor is not None:
        args.append("--reconnect-backoff-factor=" + factor)
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
            message="reconnect backoff factor {!r} must run successfully".format(factor),
        )
        if not succeeded:
            return

        with open(os.path.join(run_config.results_dir, "mb.json")) as source:
            results = json.load(source)
        stats = results["ALL STATS"]
        env.assertEqual(stats["Sets"]["Count"], requests)
        env.assertEqual(stats["Totals"]["Count"], requests)
        env.assertEqual(stats["Totals"]["Connection Errors"], 0)


def test_reconnect_backoff_default_accepted(env):
    _assert_benchmark_completes(env, None)


def test_reconnect_backoff_zero_accepted(env):
    """Explicit zero previously exited 2 despite being the default."""
    _assert_benchmark_completes(env, "0")


def test_reconnect_backoff_negative_zero_accepted(env):
    _assert_benchmark_completes(env, "-0.0")


def test_reconnect_backoff_fraction_accepted(env):
    _assert_benchmark_completes(env, "0.5")


def test_reconnect_backoff_one_accepted(env):
    _assert_benchmark_completes(env, "1")


def test_reconnect_backoff_positive_accepted(env):
    _assert_benchmark_completes(env, "2")


def test_reconnect_backoff_large_finite_accepted(env):
    _assert_benchmark_completes(env, "1e308")


def test_reconnect_backoff_invalid_values_rejected(env):
    for value in (
        "-1", "-0.5", "", " ", "abc", "1junk", "0junk", "1 ",
        "nan", "NaN", "-nan", "inf", "+inf", "-inf", "infinity",
        "1e9999", "1e-9999", "-1e9999", "-1e-9999",
    ):
        # --version exits 0 immediately if the invalid value slips through.
        # This keeps rejection tests independent of Redis and avoids
        # reconnecting with a non-finite delay on a broken parser.
        result = subprocess.run(
            [MEMTIER_BINARY, "--reconnect-backoff-factor=" + value, "--version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        label = "--reconnect-backoff-factor={!r}".format(value)
        env.assertEqual(result.returncode, 2, message=label + ": " + result.stderr)
        env.assertTrue(
            "error: reconnect-backoff-factor" in result.stderr,
            message=label + ": expected a parser diagnostic, got " + repr(result.stderr),
        )


def test_reconnect_backoff_ignores_errno_from_earlier_options(env):
    """An earlier option's strtoul overflow must not reject a valid factor."""
    result = subprocess.run(
        [
            MEMTIER_BINARY,
            "--max-reconnect-attempts=99999999999999999999999",
            "--reconnect-backoff-factor=1",
            "--version",
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    env.assertEqual(result.returncode, 0, message=result.stderr)
    env.assertFalse("error: reconnect-backoff-factor" in result.stderr, message=result.stderr)


def test_reconnect_backoff_zero_keeps_one_second_delay(env):
    """Factor 0 must not scale the delay: every reconnect waits 1.00 s."""
    env.skipOnCluster()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    closed_port = sock.getsockname()[1]
    sock.close()

    result = subprocess.run(
        [
            MEMTIER_BINARY,
            "--server=127.0.0.1",
            "--port={}".format(closed_port),
            "--threads=1",
            "--clients=1",
            "--reconnect-on-error",
            "--reconnect-backoff-factor=0",
            "--max-reconnect-attempts=3",
            "--connection-timeout=1",
            "--test-time=30",
            "--hide-histogram",
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    delays = re.findall(r"attempting reconnection \d+/\d+ in ([0-9.]+) seconds", result.stderr)
    env.assertGreaterEqual(len(delays), 2, message=result.stderr[-2000:])
    env.assertEqual(set(delays), {"1.00"}, message=repr(delays))
