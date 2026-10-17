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

import ctypes
import ctypes.util
import errno
import signal
import subprocess

from include import MEMTIER_BINARY, addTLSArgs


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
    "-99999999999999999999999",
    "-0x",
    "- 0",
    "-+0",
    "+-0",
    "--0",
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


def _strtod_sets_erange(text):
    """True if this host's libc strtod() sets errno to ERANGE for *text*.

    C leaves ERANGE for a subnormal result implementation-defined, so check
    the libc memtier is linked against rather than assume it.
    """
    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
    libc.strtod.restype = ctypes.c_double
    libc.strtod.argtypes = [ctypes.c_char_p, ctypes.c_void_p]
    ctypes.set_errno(0)
    libc.strtod(text.encode(), None)
    return ctypes.get_errno() == errno.ERANGE


def test_unsigned_options_ignore_errno_from_earlier_options(env):
    """parse_unsigned() and parse_signed() must reset errno themselves.

    --key-median=1e-310 is accepted, but strtod() sets errno to ERANGE for
    the subnormal result, so it leaves a stale errno behind.
    """
    # Guard: if --key-median starts rejecting subnormal values, the errno-leak
    # tests below and in test_cli_validation_{ports,reconnect_backoff,retry_backoff}.py
    # would pass for the wrong reason (or fail confusingly). Fail loudly instead.
    probe = _run_memtier(["--key-median=1e-310", "--version"])
    env.assertEqual(
        probe.returncode, 0,
        message=("--key-median=1e-310 is now rejected. The errno-leak tests use it to leave "
                 "errno == ERANGE behind: pick a new errno source for them. stderr: " + probe.stderr))
    env.assertTrue(
        _strtod_sets_erange("1e-310"),
        message=("strtod(\"1e-310\") does not set ERANGE on this libc, so --key-median=1e-310 leaves no stale "
                 "errno and the errno-leak tests prove nothing: pick a new errno source for them."))
    for args in (["-c", "5"], ["--max-retries=5"], ["--max-retries=-1"]):
        result = _run_memtier(["--key-median=1e-310"] + args + ["--version"])
        env.assertEqual(result.returncode, 0, message="{}: {}".format(args, result.stderr))


# ---------------------------------------------------------------------------
# The remaining numeric options (#725)
# ---------------------------------------------------------------------------

UINT_MAX = "4294967295"
INT_MAX = "2147483647"
ULLONG_MAX = "18446744073709551615"

# Options where 0 is rejected: option -> (message prefix, maximum).
POSITIVE_OPTIONS = {
    "--key-minimum": ("key-minimum", ULLONG_MAX),
    "--key-maximum": ("key-maximum", ULLONG_MAX),
    # Capped where keylist's initial 256 * (N + 1) buffer reaches 4 GiB (MEMTIER_MULTI_KEY_GET_MAX).
    "--multi-key-get": ("multi-key-get", "16777214"),
    "--rate-limiting": ("rate-limiting", UINT_MAX),
    "--clients-start": ("clients-start", UINT_MAX),
    "--clients-step": ("clients-step", UINT_MAX),
    "--step-duration": ("step-duration", UINT_MAX),
    "--pipeline": ("pipeline", "1024"),
    "-d": ("data-size", "536870912"),
    "--reconnect-interval": ("reconnect-interval", UINT_MAX),
}

# Options where 0 is a valid value: option -> (message prefix, maximum).
NON_NEGATIVE_OPTIONS = {
    "--data-offset": ("data-offset", "536870911"),  # (1 << 29) - 1
    "--thread-conn-start-min-jitter-micros": ("thread-conn-start-min-jitter-micros", UINT_MAX),
    "--thread-conn-start-max-jitter-micros": ("thread-conn-start-max-jitter-micros", UINT_MAX),
    "--select-db": ("select-db", INT_MAX),
    "--scan-incremental-max-iterations": ("scan-incremental-max-iterations", UINT_MAX),
    "--replica-clients": ("replica-clients", UINT_MAX),
    "--replicas-per-shard": ("replicas-per-shard", UINT_MAX),
}

REMAINING_OPTIONS = dict(POSITIVE_OPTIONS, **NON_NEGATIVE_OPTIONS)


def _above(maximum):
    return str(int(maximum) + 1)


def test_remaining_options_reject_wrapped_values(env):
    """-1, overflow, junk and empty values used to wrap or saturate (#725)."""
    for option, (prefix, _) in REMAINING_OPTIONS.items():
        for value in WRAPPED_VALUES:
            result = _run_memtier([option, value, "--version"])
            label = "{} {!r}".format(option, value)
            env.assertEqual(result.returncode, 2, message=label + ": " + result.stderr)
            env.assertTrue("error: {} must be".format(prefix) in result.stderr,
                           message=label + ": " + repr(result.stderr))


def test_remaining_options_reject_values_above_their_type(env):
    """4294967297 used to become 1 through the narrowing cast; values just
    above the field's type, and above UINT_MAX, are rejected and name the limit."""
    for option, (prefix, maximum) in REMAINING_OPTIONS.items():
        values = {_above(maximum), "99999999999999999999999"}
        if int(maximum) < 4294967297:
            values.add("4294967297")
        for value in sorted(values):
            result = _run_memtier([option, value, "--version"])
            label = "{} {}".format(option, value)
            env.assertEqual(result.returncode, 2, message=label + ": " + result.stderr)
            env.assertTrue("error: {} must be <= {}".format(prefix, maximum) in result.stderr,
                           message=label + ": " + repr(result.stderr))


def test_remaining_options_messages(env):
    """Malformed input says what is expected; zero is rejected only where it
    has no meaning."""
    for option, (prefix, _) in POSITIVE_OPTIONS.items():
        for value in ("-1", "5x", ""):
            result = _run_memtier([option, value, "--version"])
            env.assertTrue("error: {} must be a positive integer.".format(prefix) in result.stderr,
                           message="{} {!r}: {!r}".format(option, value, result.stderr))
        result = _run_memtier([option, "0", "--version"])
        env.assertEqual(result.returncode, 2, message="{} 0: {}".format(option, result.stderr))
        env.assertTrue("error: {} must be greater than zero.".format(prefix) in result.stderr,
                       message="{} 0: {!r}".format(option, result.stderr))
    for option, (prefix, _) in NON_NEGATIVE_OPTIONS.items():
        for value in ("-1", "5x", ""):
            result = _run_memtier([option, value, "--version"])
            env.assertTrue("error: {} must be a non-negative integer.".format(prefix) in result.stderr,
                           message="{} {!r}: {!r}".format(option, value, result.stderr))
    result = _run_memtier(["-d", "536870913", "--version"])
    env.assertTrue("error: data-size must be <= 536870912 bytes (512 MiB)." in result.stderr,
                   message=repr(result.stderr))


def test_remaining_options_accept_valid_values(env):
    for option, (_, maximum) in REMAINING_OPTIONS.items():
        values = ("1", maximum) + RELAXED_VALUES
        if option in NON_NEGATIVE_OPTIONS:
            values += ("0",)
        for value in values:
            result = _run_memtier([option, value, "--version"])
            env.assertEqual(result.returncode, 0, message="{} {!r}: {}".format(option, value, result.stderr))


MAX_RETRIES_REJECTED = (
    "-2",
    "\n-2",
    "-4294967295",
    "-9223372036854775809",
    "-99999999999999999999999",
    "- 1",
    "+-1",
    "-+1",
    "--1",
    "++1",
    "+ 1",
    "-",
    "+",
    "",
    " ",
    "5x",
    "-1x",
    "5 ",
)


def test_max_retries_rejects_out_of_range_and_malformed(env):
    """--max-retries is an int with -1 = unlimited: strtol() was cast to int
    with no upper bound, so 4294967295 became -1 and 4294967297 became 1."""
    for value in MAX_RETRIES_REJECTED:
        result = _run_memtier(["--max-retries", value, "--version"])
        label = "--max-retries {!r}".format(value)
        env.assertEqual(result.returncode, 2, message=label + ": " + result.stderr)
        env.assertTrue("error: max-retries must be an integer >= -1 (-1 means unlimited)." in result.stderr,
                       message=label + ": " + repr(result.stderr))
    for value in ("2147483648", "4294967295", "4294967297", "9223372036854775808", "99999999999999999999999"):
        result = _run_memtier(["--max-retries", value, "--version"])
        label = "--max-retries {}".format(value)
        env.assertEqual(result.returncode, 2, message=label + ": " + result.stderr)
        env.assertTrue("error: max-retries must be <= 2147483647." in result.stderr,
                       message=label + ": " + repr(result.stderr))


def test_max_retries_accepts_valid_values(env):
    for value in ("-1", " -1", "\t-1", "0", "+0", "-0", "1", " +5", INT_MAX):
        result = _run_memtier(["--max-retries", value, "--version"])
        env.assertEqual(result.returncode, 0, message="--max-retries {!r}: {}".format(value, result.stderr))


def _show_config(args):
    """Parse *args* and return the --show-config dump (printed on stderr).

    The config is printed before memtier connects, so port 1 (nothing
    listens there) is enough; the connect failure that follows is ignored.
    """
    result = _run_memtier(["-s", "127.0.0.1", "-p", "1", "-n", "1", "-c", "1", "-t", "1", "--show-config"] + args)
    return result.stderr


def test_remaining_options_store_the_parsed_value(env):
    """The value that reaches the config is the one given: no wrap, no
    truncation, and the -1 sentinel of --max-retries survives."""
    cases = (
        (["--max-retries=-1"], "max_retries = -1"),
        (["--max-retries= -1"], "max_retries = -1"),
        (["--max-retries=" + INT_MAX], "max_retries = " + INT_MAX),
        (["--max-retries=+7"], "max_retries = 7"),
        (["--data-offset=+536870911"], "data_offset = 536870911"),
        (["--key-minimum=+2", "--key-maximum=" + ULLONG_MAX], "key_minimum = 2\n"),
        (["--key-maximum=" + ULLONG_MAX], "key_maximum = " + ULLONG_MAX),
        (["--multi-key-get= +64"], "multi_key_get = 64\n"),
        (["--multi-key-get=2000000"], "multi_key_get = 2000000\n"),
        (["--select-db=" + INT_MAX], "select-db = " + INT_MAX),
        (["--rate-limiting= 9"], "rate_limit = 9\n"),
        (["--thread-conn-start-max-jitter-micros=" + UINT_MAX], "thread_conn_start_max_jitter_micros = " + UINT_MAX),
    )
    for args, expected in cases:
        out = _show_config(args)
        env.assertTrue(expected in out, message="{}: expected {!r} in {!r}".format(args, expected, out))


def test_remaining_options_store_their_maximum(env):
    """Each option stores its full maximum, so a store narrowed to a smaller
    type (or dropped) is caught: every value here is above 65535."""
    # One run for the options without cross-checks. min == max jitter adds
    # no sleep, and -c 1 creates no second connection to jitter before.
    out = _show_config([
        "--key-minimum=18446744073709551614", "--key-maximum=" + ULLONG_MAX,
        "--reconnect-interval=" + UINT_MAX, "--rate-limiting=" + UINT_MAX,
        "--thread-conn-start-min-jitter-micros=" + UINT_MAX, "--thread-conn-start-max-jitter-micros=" + UINT_MAX,
        "--scan-incremental-max-iterations=" + UINT_MAX, "--replica-clients=" + UINT_MAX,
        "--replicas-per-shard=" + UINT_MAX,
    ])
    for expected in ("key_minimum = 18446744073709551614\n", "reconnect_interval = " + UINT_MAX,
                     "rate_limit = " + UINT_MAX, "thread_conn_start_min_jitter_micros = " + UINT_MAX,
                     "thread_conn_start_max_jitter_micros = " + UINT_MAX,
                     "scan_incremental_max_iterations = " + UINT_MAX, "replica_clients = " + UINT_MAX,
                     "replicas_per_shard = " + UINT_MAX):
        env.assertTrue(expected in out, message="expected {!r} in {!r}".format(expected, out))
    # Staircase: --requests is not allowed, and --clients-start must stay
    # below --clients.
    result = _run_memtier(["-s", "127.0.0.1", "-p", "1", "-t", "1", "-c", "2", "--test-time=1", "--show-config",
                           "--clients-start=1", "--clients-step=" + UINT_MAX, "--step-duration=" + UINT_MAX])
    for expected in ("clients_step = " + UINT_MAX, "step_duration = " + UINT_MAX):
        env.assertTrue(expected in result.stderr, message="expected {!r} in {!r}".format(expected, result.stderr))
    # --clients-start at its maximum is reported back by the staircase check,
    # which exits before any client is created.
    result = _run_memtier(["-s", "127.0.0.1", "-p", "1", "--test-time=1", "--clients-start=" + UINT_MAX,
                           "--clients-step=1", "--step-duration=1"])
    env.assertTrue("--clients-start ({}) must be less than --clients".format(UINT_MAX) in result.stderr,
                   message=repr(result.stderr))


def _assert_no_sigfpe(env, args, label):
    """Run memtier against a closed port and assert it did not die of SIGFPE.

    The run may end with a connection error, or still be sleeping in the
    connection jitter when the timeout hits; both are fine.
    """
    try:
        result = subprocess.run([MEMTIER_BINARY, "-s", "127.0.0.1", "-p", "1", "-t", "1", "-n", "1"] + args,
                                capture_output=True, text=True, timeout=5)
    except subprocess.TimeoutExpired:
        return
    env.assertNotEqual(result.returncode, -signal.SIGFPE, message=label + ": SIGFPE, stderr: " + result.stderr)
    env.assertNotEqual(result.returncode, 128 + signal.SIGFPE, message=label + ": SIGFPE, stderr: " + result.stderr)


def test_rate_limiting_max_does_not_divide_by_zero(env):
    """(request_rate + 49) / 50 wrapped to 0 above UINT_MAX - 49, then
    request_rate / 0 raised SIGFPE."""
    for value in ("4294967247", UINT_MAX):
        _assert_no_sigfpe(env, ["-c", "1", "--rate-limiting=" + value], "--rate-limiting=" + value)


def test_jitter_max_range_does_not_divide_by_zero(env):
    """A jitter range of UINT_MAX made jitter_range + 1 wrap to 0, and
    rand() % 0 raised SIGFPE after the first connection.

    The fixed binary then sleeps in the jitter (no smaller range has a 2^32
    span), so hitting the 5 s timeout is the expected pass path."""
    _assert_no_sigfpe(env, ["-c", "2", "--thread-conn-start-min-jitter-micros=0",
                            "--thread-conn-start-max-jitter-micros=" + UINT_MAX], "jitter 0.." + UINT_MAX)


def test_multi_key_get_cap(env):
    """INT_MAX used to be accepted; keylist's 32-bit 256 * (N + 1) buffer
    size then wrapped, and the startup allocation exhausted memory. The cap
    is where that size reaches 4 GiB, so values that used to work (2000000)
    still do."""
    for value in ("2000000", "16777214"):
        result = _run_memtier(["--multi-key-get=" + value, "--version"])
        env.assertEqual(result.returncode, 0, message="{}: {}".format(value, result.stderr))
    for value in ("16777215", "16777216", INT_MAX):
        result = _run_memtier(["--multi-key-get=" + value, "--version"])
        env.assertEqual(result.returncode, 2, message="{}: {}".format(value, result.stderr))
        env.assertTrue("error: multi-key-get must be <= 16777214." in result.stderr,
                       message="{}: {!r}".format(value, result.stderr))


def test_key_maximum_full_range_runs(env):
    """--key-maximum=ULLONG_MAX with the default --key-minimum (0) is a full
    64-bit key range: r_max - r_min + 1 wrapped to 0, so random keys hit
    rn % 0 (SIGFPE), and Gaussian keys never passed val > max + 1 == 0.

    Z:Z only checks that the full range runs: its clamp of a 2^64 sample is
    defensive, since reaching it needs p within ~1e-16 of 1, which no test
    can force."""
    env.skipOnCluster()
    master = env.getMasterNodesList()[0]
    for pattern, clients in (("R:R", "1"), ("G:G", "1"), ("Z:Z", "1"), ("S:S", "1"), ("P:P", "2")):
        label = "--key-pattern={}".format(pattern)
        specs = {"args": ["-s", "127.0.0.1", "-p", str(master["port"]), "-n", "3", "-c", clients, "-t", "1",
                          "--key-maximum=" + ULLONG_MAX, "--key-pattern=" + pattern, "--hide-histogram"]}
        addTLSArgs(specs, env)  # the TLS CI cells run a TLS-only server
        try:
            result = subprocess.run([MEMTIER_BINARY] + specs["args"], capture_output=True, text=True, timeout=30)
        except subprocess.TimeoutExpired:
            env.assertTrue(False, message=label + ": hung")
            continue
        env.assertEqual(result.returncode, 0, message="{}: rc {}: {}".format(label, result.returncode, result.stderr))


def test_data_offset_cap(env):
    """The parser enforces the (1 << 29) - 1 limit main() used to check later
    with "data-offset too long", so "must be <= N" states the real bound."""
    result = _run_memtier(["--data-offset=536870911", "--version"])
    env.assertEqual(result.returncode, 0, message=result.stderr)
    for value in ("536870912", UINT_MAX):
        result = _run_memtier(["--data-offset=" + value, "--version"])
        env.assertEqual(result.returncode, 2, message="{}: {}".format(value, result.stderr))
        env.assertTrue("error: data-offset must be <= 536870911." in result.stderr,
                       message="{}: {!r}".format(value, result.stderr))


def test_negative_zero_is_zero(env):
    """strtoul("-0") never wrapped, so "-0" and "-00" keep meaning 0 (#212):
    accepted where 0 is valid, "greater than zero" where it is not."""
    zero_ok = ["--max-reconnect-attempts", "--retry-backoff-ms", "--max-retry-queue", "--connection-timeout",
               "--connection-stage-timeout", "--max-retries"] + list(NON_NEGATIVE_OPTIONS)
    for option in zero_ok:
        for value in ("-0", "-00", " -0", "\n-0"):
            result = _run_memtier([option, value, "--version"])
            env.assertEqual(result.returncode, 0, message="{} {!r}: {}".format(option, value, result.stderr))
    for option, (prefix, _) in POSITIVE_OPTIONS.items():
        result = _run_memtier([option, "-0", "--version"])
        env.assertTrue("error: {} must be greater than zero.".format(prefix) in result.stderr,
                       message="{} -0: {!r}".format(option, result.stderr))
    for option, prefix in (("-n", "requests"), ("-c", "clients"), ("-t", "threads"), ("--test-time", "test time")):
        result = _run_memtier([option, "-0", "--version"])
        env.assertTrue("error: {} must be greater than zero.".format(prefix) in result.stderr,
                       message="{} -0: {!r}".format(option, result.stderr))
    out = _show_config(["--select-db=-0", "--data-offset=-00", "--replica-clients= -0"])
    for expected in ("select-db = 0\n", "data_offset = 0\n", "replica_clients = 0\n"):
        env.assertTrue(expected in out, message="expected {!r} in {!r}".format(expected, out))


def test_uri_database_number(env):
    """The --uri database number follows the --select-db rule: (int) strtol()
    turned 4294967297 into db 1, 4294967296 into db 0 and 6442450943 into
    INT_MAX. Parsing happens before connecting, so port 1 is enough."""
    def run(path):
        return _run_memtier(["--uri=redis://127.0.0.1:1/" + path, "-n", "1", "-c", "1", "-t", "1", "--show-config"])

    for path, db in (("0", "0"), ("7", "7"), ("+3", "3"), (" 5", "5"), ("-0", "0"), ("-00", "0"),
                     (INT_MAX, INT_MAX)):
        result = run(path)
        label = "--uri .../{!r}".format(path)
        env.assertFalse("invalid database number" in result.stderr, message=label + ": " + result.stderr)
        env.assertTrue("select-db = {}\n".format(db) in result.stderr, message=label + ": " + result.stderr)
    for path in ("-1", "-2147483648", "2147483648", "4294967296", "4294967297", "6442450943",
                 "99999999999999999999999", "-99999999999999999999999",
                 # (int) strtol() wrapped these into db 1 and db 0.
                 "-4294967295", "-4294967296",
                 # strtoull() would negate these into db 1 and db 2147483647.
                 "-18446744073709551615", "-18446744071562067969", "abc", "1x", "1 ", "+-1", "- 1", "++1",
                 " ", "0x10"):
        result = run(path)
        label = "--uri .../{!r}".format(path)
        env.assertNotEqual(result.returncode, 0, message=label + ": " + result.stderr)
        env.assertTrue("error: invalid database number '{}'.".format(path) in result.stderr,
                       message=label + ": " + result.stderr)
    # An empty path selects no database, so the --select-db value is kept.
    result = _run_memtier(["--select-db=3", "--uri=redis://127.0.0.1:1/", "-n", "1", "-c", "1", "-t", "1",
                           "--show-config"])
    env.assertTrue("select-db = 3\n" in result.stderr, message="empty path: " + result.stderr)
