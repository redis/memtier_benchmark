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
    " -5",
    "- 5",
    "+-5",
    "++5",
    "+ 5",
    "",
    " ",
    "5x",
    "5 ",
    "99999999999999999999999",
)

# What strtoul() accepted harmlessly before (leading whitespace, one '+') is
# still accepted, so existing command lines keep working.
RELAXED_VALUES = (" 5", "\t5", "\n5", "+5", " +5")


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
        for value in ("1",) + RELAXED_VALUES:
            result = _run_memtier([option, value, "--version"])
            env.assertEqual(result.returncode, 0, message="{} {!r}: {}".format(option, value, result.stderr))
    for option, top in (("--max-reconnect-attempts", "4294967295"), ("--retry-backoff-ms", "4294967295"),
                        ("--max-retry-queue", "4294967295"), ("--connection-timeout", "4294967295"),
                        ("--connection-stage-timeout", "4294967294")):
        for value in ("0", top):
            result = _run_memtier([option, value, "--version"])
            env.assertEqual(result.returncode, 0, message="{} {}: {}".format(option, value, result.stderr))
    result = _run_memtier(["-n", "18446744073709551614", "--version"])
    env.assertEqual(result.returncode, 0, message=result.stderr)


def test_unsigned_options_reject_sentinel_values(env):
    """Values equal to an internal "unset" sentinel must not be accepted.

    -n ULLONG_MAX is the allkeys marker and --connection-stage-timeout
    UINT_MAX means "not set" (replaced by the 30 s default).
    """
    for option, value, diagnostic in (("-n", "18446744073709551615", "requests must be"),
                                      ("--connection-stage-timeout", "4294967295",
                                       "connection-stage-timeout must be")):
        result = _run_memtier([option, value, "--version"])
        label = "{} {}".format(option, value)
        env.assertEqual(result.returncode, 2, message=label + ": " + result.stderr)
        env.assertTrue(diagnostic in result.stderr, message=label + ": " + repr(result.stderr))


def test_unsigned_options_keep_specific_messages(env):
    """Negative or malformed input keeps "must be a positive integer", zero
    says "greater than zero", and overflow on a capped option names the cap."""
    for option, prefix in (("-n", "requests"), ("-c", "clients"), ("-t", "threads"), ("--test-time", "test time")):
        for value in ("-1", "5x", "12abc", ""):
            result = _run_memtier([option, value, "--version"])
            env.assertTrue("error: {} must be a positive integer.".format(prefix) in result.stderr,
                           message="{} {!r}: {!r}".format(option, value, result.stderr))
        result = _run_memtier([option, "0", "--version"])
        env.assertTrue("error: {} must be greater than zero.".format(prefix) in result.stderr,
                       message="{} 0: {!r}".format(option, result.stderr))
    # Too large (above the field's range, or ERANGE) names the limit.
    for option, prefix, values in (("-c", "clients", ("4294967296", "99999999999999999999999")),
                                   ("-t", "threads", ("4294967296", "99999999999999999999999")),
                                   ("--test-time", "test time", ("4294967296", "99999999999999999999999")),
                                   ("-n", "requests", ("18446744073709551615", "99999999999999999999999"))):
        limit = "18446744073709551614" if option == "-n" else "4294967295"
        for value in values:
            result = _run_memtier([option, value, "--version"])
            env.assertTrue("error: {} must be <= {}.".format(prefix, limit) in result.stderr,
                           message="{} {}: {!r}".format(option, value, result.stderr))
    for option, cap in (("--pipeline", "pipeline must be <= 1024"), ("-d", "data-size must be <= 536870912 bytes")):
        for value in ("1025" if option == "--pipeline" else "536870913", "99999999999999999999999"):
            result = _run_memtier([option, value, "--version"])
            env.assertEqual(result.returncode, 2, message="{} {}: {}".format(option, value, result.stderr))
            env.assertTrue(cap in result.stderr, message="{} {}: {!r}".format(option, value, result.stderr))


def test_unsigned_options_ignore_errno_from_earlier_options(env):
    """parse_unsigned() must reset errno itself.

    --key-maximum does not check ERANGE yet (#725), so it is used here to
    leave errno set; once #725 lands, pick another errno source.
    """
    # Guard: if --key-maximum starts rejecting overflow, the errno-leak tests
    # below and in test_cli_validation_{ports,reconnect_backoff,retry_backoff}.py
    # would pass for the wrong reason (or fail confusingly). Fail loudly instead.
    probe = _run_memtier(["--key-maximum=99999999999999999999999", "--version"])
    env.assertEqual(
        probe.returncode, 0,
        message=("--key-maximum=99999999999999999999999 is now rejected (#725 fixed?). The errno-leak tests "
                 "use it to leave errno == ERANGE behind: pick a new errno source for them. stderr: "
                 + probe.stderr))
    result = _run_memtier(["--key-maximum=99999999999999999999999", "-c", "5", "--version"])
    env.assertEqual(result.returncode, 0, message=result.stderr)
