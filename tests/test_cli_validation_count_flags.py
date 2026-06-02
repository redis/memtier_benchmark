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
