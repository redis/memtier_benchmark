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
import shutil
import socket
import subprocess
import tempfile
import time

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


def test_reconnect_backoff_fraction_has_delay_floor(env):
    """A factor below 1 must not shrink the delay to 0 s (issue #688).

    Without a floor, factor 0.5 reaches "in 0.00 seconds" after 8 attempts
    and a dead server is reconnected in a tight loop. Every delay must stay
    at or above the 0.1 s floor once the halving reaches it. Factor 0 keeps a
    constant 1 s delay; any positive factor below 1 settles at 0.1 s.
    """
    env.skipOnCluster()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    closed_port = sock.getsockname()[1]
    sock.close()

    attempts = 12
    started = time.monotonic()
    result = subprocess.run(
        [
            MEMTIER_BINARY,
            "--server=127.0.0.1",
            "--port={}".format(closed_port),
            "--threads=1",
            "--clients=1",
            "--reconnect-on-error",
            "--reconnect-backoff-factor=0.5",
            "--max-reconnect-attempts={}".format(attempts),
            "--connection-timeout=1",
            "--test-time=30",
            "--hide-histogram",
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    elapsed = time.monotonic() - started
    delays = [
        float(d)
        for d in re.findall(
            r"(?:attempting reconnection \d+/\d+ in|retrying in) ([0-9.]+) seconds", result.stderr
        )
    ]
    env.assertGreaterEqual(len(delays), attempts - 1, message=result.stderr[-2000:])
    env.assertLessEqual(len(delays), attempts, message=repr(delays))
    env.assertGreaterEqual(min(delays), 0.1, message=repr(delays))
    env.assertEqual(delays[:3], [0.5, 0.25, 0.12], message=repr(delays))
    env.assertEqual(set(delays[3:]), {0.1}, message=repr(delays))
    # The logged delays must also be the armed ones: 0.5 + 0.25 + 0.125 +
    # 9 * 0.1 = ~1.78 s of timers before the attempts run out.
    env.assertGreaterEqual(elapsed, 1.5, message="elapsed {:.2f}s, delays {!r}".format(elapsed, delays))


def test_reconnect_backoff_fraction_floor_on_failed_reconnect(env):
    """The retry path of a failed reconnect() must apply the floor too (#688).

    A closed TCP port reports ECONNREFUSED from the event callback, so every
    attempt goes through attempt_reconnect(). A unix socket whose server has
    exited fails synchronously with ENOENT instead, which exercises the
    "Reconnection attempt N failed, retrying in X seconds" path. SHUTDOWN
    NOSAVE makes redis exit and unlink its socket.
    """
    env.skipOnCluster()
    redis_server = os.environ.get("REDIS_SERVER") or shutil.which("redis-server")
    if not hasattr(socket, "AF_UNIX") or not redis_server:
        env.skip()
        return

    # RLTest log dirs can exceed the 108-byte sun_path limit; keep it short.
    directory = tempfile.mkdtemp(prefix="m688.", dir="/tmp")
    socket_path = os.path.join(directory, "s")
    server = None
    try:
        server = subprocess.Popen(
            [redis_server, "--port", "0", "--unixsocket", socket_path, "--save", "", "--dir", directory],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 10
        while not os.path.exists(socket_path) and server.poll() is None and time.monotonic() < deadline:
            time.sleep(0.05)
        if not os.path.exists(socket_path):
            env.skip()
            return

        attempts = 12
        result = subprocess.run(
            [
                MEMTIER_BINARY,
                "--unix-socket={}".format(socket_path),
                "--threads=1",
                "--clients=1",
                "--command=SHUTDOWN NOSAVE",
                "--reconnect-on-error",
                "--reconnect-backoff-factor=0.5",
                "--max-reconnect-attempts={}".format(attempts),
                "--test-time=30",
                "--hide-histogram",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        delays = [float(d) for d in re.findall(r"retrying in ([0-9.]+) seconds", result.stderr)]
        env.assertGreaterEqual(len(delays), attempts - 2, message=result.stderr[-2000:])
        env.assertGreaterEqual(min(delays or [0.0]), 0.1, message=repr(delays))
        env.assertEqual(set(delays[2:]), {0.1}, message=repr(delays))
    finally:
        if server is not None and server.poll() is None:
            server.kill()
        if server is not None:
            server.wait(timeout=10)
        shutil.rmtree(directory, ignore_errors=True)
