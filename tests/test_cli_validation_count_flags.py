"""
CLI validation regression tests for -c / -t / -n / --test-time count flags.

Covers 2.4 release-readiness review finding #24:

  ``strtoul`` was called WITHOUT ``errno=0`` checking and WITHOUT
  ``optarg_is_negative()`` on the four older count flags (-c, -t, -n,
  --test-time).  Negative input like ``-c -1`` wrapped through the
  unsigned cast (strtoul("-1") -> ULONG_MAX -> (unsigned int) UINT_MAX =
  4294967295) and the run loop then tried to spawn ~4 billion clients,
  producing an OOM / SIGABRT.

Same crash class as merged phases #428 / #429 which fixed --pipeline,
--data-size, and --run-count.

After the fix, negative and zero inputs must be rejected at parse time
with exit code 2 and a readable message on stderr.  No live Redis
connection is required for any of these tests.

Run with:
  TEST=test_cli_validation_count_flags.py OSS_STANDALONE=1 ./tests/run_tests.sh
"""

import subprocess

from include import MEMTIER_BINARY


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _run_memtier(args):
    """Run memtier_benchmark with *args* and return the CompletedProcess.

    No --server is supplied: validation must reject the bad value before
    any connection attempt is made.
    """
    return subprocess.run(
        [MEMTIER_BINARY] + args,
        capture_output=True,
        text=True,
        timeout=30,
    )


# ---------------------------------------------------------------------------
# -c / --clients
# ---------------------------------------------------------------------------


def test_clients_negative_rejected(env):
    """-c -1 must be rejected (was OOM / SIGABRT via strtoul wrap)."""
    result = _run_memtier(["-c", "-1"])

    env.assertNotEqual(
        result.returncode,
        0,
        message="-c -1 must exit non-zero (was OOM / SIGABRT via strtoul wrap)",
    )
    env.assertTrue(
        "clients must be" in result.stderr,
        message=(
            "Expected 'clients must be' in stderr, got: {!r}".format(result.stderr)
        ),
    )


def test_clients_zero_rejected(env):
    """-c 0 must be rejected with a clear error."""
    result = _run_memtier(["-c", "0"])

    env.assertNotEqual(
        result.returncode,
        0,
        message="-c 0 must exit non-zero",
    )
    env.assertTrue(
        "clients must be" in result.stderr,
        message=(
            "Expected 'clients must be' in stderr, got: {!r}".format(result.stderr)
        ),
    )


# ---------------------------------------------------------------------------
# -t / --threads
# ---------------------------------------------------------------------------


def test_threads_negative_rejected(env):
    """-t -1 must be rejected (was OOM / SIGABRT via strtoul wrap)."""
    result = _run_memtier(["-t", "-1"])

    env.assertNotEqual(
        result.returncode,
        0,
        message="-t -1 must exit non-zero (was OOM / SIGABRT via strtoul wrap)",
    )
    env.assertTrue(
        "threads must be" in result.stderr,
        message=(
            "Expected 'threads must be' in stderr, got: {!r}".format(result.stderr)
        ),
    )


def test_threads_zero_rejected(env):
    """-t 0 must be rejected with a clear error."""
    result = _run_memtier(["-t", "0"])

    env.assertNotEqual(
        result.returncode,
        0,
        message="-t 0 must exit non-zero",
    )
    env.assertTrue(
        "threads must be" in result.stderr,
        message=(
            "Expected 'threads must be' in stderr, got: {!r}".format(result.stderr)
        ),
    )


# ---------------------------------------------------------------------------
# -n / --requests  (note: -n -1 / -n allkeys is a valid sentinel — test -n -2)
# ---------------------------------------------------------------------------


def test_requests_negative_two_rejected(env):
    """-n -2 must be rejected; only the literal string 'allkeys' is a valid sentinel."""
    result = _run_memtier(["-n", "-2"])

    env.assertNotEqual(
        result.returncode,
        0,
        message="-n -2 must exit non-zero (only 'allkeys' is a valid non-numeric value)",
    )
    env.assertTrue(
        "requests must be" in result.stderr,
        message=(
            "Expected 'requests must be' in stderr, got: {!r}".format(result.stderr)
        ),
    )


def test_requests_zero_rejected(env):
    """-n 0 must be rejected with a clear error."""
    result = _run_memtier(["-n", "0"])

    env.assertNotEqual(
        result.returncode,
        0,
        message="-n 0 must exit non-zero",
    )
    env.assertTrue(
        "requests must be" in result.stderr,
        message=(
            "Expected 'requests must be' in stderr, got: {!r}".format(result.stderr)
        ),
    )


# ---------------------------------------------------------------------------
# --test-time
# ---------------------------------------------------------------------------


def test_test_time_negative_rejected(env):
    """--test-time=-1 must be rejected (was OOM / SIGABRT via strtoul wrap)."""
    result = _run_memtier(["--test-time=-1"])

    env.assertNotEqual(
        result.returncode,
        0,
        message="--test-time=-1 must exit non-zero (was OOM / SIGABRT via strtoul wrap)",
    )
    env.assertTrue(
        "test time must be" in result.stderr,
        message=(
            "Expected 'test time must be' in stderr, got: {!r}".format(result.stderr)
        ),
    )


def test_test_time_zero_rejected(env):
    """--test-time=0 must be rejected with a clear error."""
    result = _run_memtier(["--test-time=0"])

    env.assertNotEqual(
        result.returncode,
        0,
        message="--test-time=0 must exit non-zero",
    )
    env.assertTrue(
        "test time must be" in result.stderr,
        message=(
            "Expected 'test time must be' in stderr, got: {!r}".format(result.stderr)
        ),
    )


# ---------------------------------------------------------------------------
# Wrapped / overflowing values on every unsigned option (#705, #695)
# ---------------------------------------------------------------------------

# option -> diagnostic prefix printed on rejection.
UNSIGNED_OPTIONS = {
    "-n": "requests must be",
    "-c": "clients must be",
    "-t": "threads must be",
    "--test-time": "test time must be",
    "--pipeline": "pipeline must be",
    "-d": "data-size must be",
    "--reconnect-interval": "reconnect-interval must be",
    "--max-reconnect-attempts": "max-reconnect-attempts must be",
    "--retry-backoff-ms": "retry-backoff-ms must be",
    "--max-retry-queue": "max-retry-queue must be",
    "--connection-timeout": "connection-timeout must be",
    "--connection-stage-timeout": "connection-stage-timeout must be",
}

# strtoul() skips any isspace() character (not only ' ' and '\t') and then
# accepts a '-', so "\n-18446744073709551000" used to wrap to 616.
WRAPPED_VALUES = (
    "\n-18446744073709551000",
    "\n-18446744073709551615",
    "\n-1",
    "\v-1",
    "-1",
    " 5",
    "+5",
    "",
    "99999999999999999999999",
)


def test_unsigned_options_reject_wrapped_values(env):
    """No unsigned option may accept a value strtoul() would wrap or overflow.

    --version exits 0 right after parsing, so a value that slips through
    shows up as rc 0 without needing a server.
    """
    for option, diagnostic in UNSIGNED_OPTIONS.items():
        for value in WRAPPED_VALUES:
            result = _run_memtier([option, value, "--version"])
            label = "{} {!r}".format(option, value)
            env.assertEqual(result.returncode, 2, message=label + ": " + result.stderr)
            env.assertTrue(diagnostic in result.stderr, message=label + ": " + repr(result.stderr))


def test_unsigned_int_options_reject_values_above_uint_max(env):
    """Values above UINT_MAX must not wrap modulo 2^32 (4294967297 -> 1)."""
    for option, diagnostic in UNSIGNED_OPTIONS.items():
        if option == "-n":
            continue  # unsigned long long: 4294967297 is a valid request count
        result = _run_memtier([option, "4294967297", "--version"])
        label = "{} 4294967297".format(option)
        env.assertEqual(result.returncode, 2, message=label + ": " + result.stderr)
        env.assertTrue(diagnostic in result.stderr, message=label + ": " + repr(result.stderr))


def test_unsigned_options_accept_valid_values(env):
    for option in UNSIGNED_OPTIONS:
        result = _run_memtier([option, "1", "--version"])
        env.assertEqual(result.returncode, 0, message=option + " 1: " + result.stderr)
    for option in ("--max-reconnect-attempts", "--retry-backoff-ms", "--max-retry-queue", "--connection-timeout",
                   "--connection-stage-timeout"):
        for value in ("0", "4294967295"):
            result = _run_memtier([option, value, "--version"])
            env.assertEqual(result.returncode, 0, message="{} {}: {}".format(option, value, result.stderr))
    result = _run_memtier(["-n", "18446744073709551615", "--version"])
    env.assertEqual(result.returncode, 0, message=result.stderr)
