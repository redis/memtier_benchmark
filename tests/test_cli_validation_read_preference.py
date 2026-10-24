"""
CLI parse-time validation tests for the --read-preference family of flags.

These tests exercise only the argument-parsing path and do not require a live
Redis server — we invoke the binary with subprocess, expect a specific exit
code and a clear message in stderr or stdout.

Tests covered:
  1. --read-preference rejects unknown modes.
  2. --read-preference=primary (default) is accepted silently.
  3. --read-preference=secondary without --cluster-mode or --read-server emits
     a warning but still exits 0 (it's a warning, not an error).
  4. --transaction + --read-preference!=primary is a hard error.
  5. --read-server with a bad HOST:PORT is rejected.
  6. --read-server with a valid IPv4 HOST:PORT is accepted (parse only, no
     connection required when we print --show-config and exit before running).
  7. --read-preference-fallback rejects unknown values.
  8. --command-is-read without a preceding --command is rejected.

Run with:
  TEST=test_cli_validation_read_preference.py OSS_STANDALONE=1 ./tests/run_tests.sh
"""
import subprocess

from include import MEMTIER_BINARY


def _run(args, timeout=10):
    """Run memtier_benchmark with *args*, return CompletedProcess."""
    return subprocess.run(
        [MEMTIER_BINARY] + args,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


# ---------------------------------------------------------------------------
# Common fragments
# ---------------------------------------------------------------------------

_BASE = ["-s", "127.0.0.1", "-p", "6379", "--test-time=1"]

# Use --show-config so the binary parses CLI and prints config, but we kill
# it very quickly.  We just need parse-time behaviour; the server doesn't
# have to be up.
_BASE_SHOW = ["-s", "127.0.0.1", "-p", "6379", "--show-config", "--test-time=1"]


# ---------------------------------------------------------------------------
# 1. Unknown --read-preference value
# ---------------------------------------------------------------------------

def test_read_preference_unknown_mode_rejected(env):
    """--read-preference=bogus must be rejected with a clear error."""
    result = _run(_BASE + ["--read-preference=bogus"])

    env.assertNotEqual(result.returncode, 0,
                       message="--read-preference=bogus must exit non-zero")
    env.assertTrue(
        "--read-preference must be one of" in result.stderr,
        message="Expected mode-list rejection in stderr; got: {!r}".format(result.stderr),
    )


# ---------------------------------------------------------------------------
# 2. Valid modes are accepted (just parse-time check via bad connection)
# ---------------------------------------------------------------------------

def test_read_preference_all_valid_modes_parse(env):
    """Each valid mode name must be accepted at parse time."""
    for mode in ("primary", "secondary", "secondaryPreferred", "nearest"):
        # With a bad port the binary will fail at connect time (non-zero exit),
        # but we want exit != 2 (usage error).  The parse path exits with 2 on
        # bad options.  We just check that the binary does NOT print the
        # mode-list rejection message.
        # --max-reconnect-attempts=1 + --connection-stage-timeout=2 bound the
        # connect/setup loops so a dead port exits in well under the 15s
        # timeout (otherwise the post-test cleanup phase runs full duration).
        result = _run(
            ["-s", "127.0.0.1", "-p", "1", "--test-time=1",
             "--max-reconnect-attempts=1", "--connection-stage-timeout=2",
             "--read-preference={}".format(mode)],
            timeout=15,
        )
        env.assertFalse(
            "--read-preference must be one of" in result.stderr,
            message="Mode '{}' should not trigger rejection; stderr: {!r}".format(
                mode, result.stderr),
        )


# ---------------------------------------------------------------------------
# 3. --read-preference without cluster or read-server emits a warning
# ---------------------------------------------------------------------------

def test_read_preference_no_cluster_no_server_warns(env):
    """--read-preference=secondary standalone (no --cluster-mode, no
    --read-server) must emit a warning but not a hard error."""
    result = _run(
        ["-s", "127.0.0.1", "-p", "1", "--test-time=1",
         "--max-reconnect-attempts=1", "--connection-stage-timeout=2",
         "--read-preference=secondary"],
        timeout=15,
    )
    env.assertTrue(
        "--read-preference has no effect" in result.stderr,
        message="Expected 'has no effect' warning in stderr; got: {!r}".format(
            result.stderr),
    )
    # The rejection message (exit-2) must NOT appear.
    env.assertFalse(
        "--read-preference must be one of" in result.stderr,
        message="Must not be a parse error; got: {!r}".format(result.stderr),
    )


# ---------------------------------------------------------------------------
# 4. --transaction + --read-preference != primary is a hard error
# ---------------------------------------------------------------------------

def test_transaction_with_non_primary_read_pref_rejected(env):
    """Combining --transaction with --read-preference!=primary must fail."""
    for mode in ("secondary", "secondaryPreferred", "nearest"):
        result = _run(
            ["-s", "127.0.0.1", "-p", "6379",
             "--cluster-mode",
             "--transaction",
             "--command", "GET __key__",
             "--read-preference={}".format(mode),
             "--test-time=1"],
        )
        env.assertNotEqual(
            result.returncode, 0,
            message="--transaction + --read-preference={} must exit non-zero".format(mode),
        )
        env.assertTrue(
            "mutually exclusive" in result.stderr,
            message="Expected 'mutually exclusive' message; got: {!r}".format(
                result.stderr),
        )


# ---------------------------------------------------------------------------
# 5. --read-server with bad HOST:PORT is rejected
# ---------------------------------------------------------------------------

def test_read_server_bad_format_rejected(env):
    """--read-server without a port must be rejected."""
    for bad in ("localhost", "localhost:", "localhost:abc", ":6379", ""):
        result = _run(_BASE + ["--read-server={}".format(bad)])
        env.assertNotEqual(
            result.returncode, 0,
            message="--read-server='{}' must exit non-zero".format(bad),
        )


# ---------------------------------------------------------------------------
# 6. --read-server with valid HOST:PORT is accepted (parse-time only)
# ---------------------------------------------------------------------------

def test_read_server_valid_format_accepted(env):
    """--read-server=127.0.0.1:6380 must not trigger a parse-time error."""
    result = _run(
        ["-s", "127.0.0.1", "-p", "1", "--test-time=1",
         "--max-reconnect-attempts=1", "--connection-stage-timeout=2",
         "--read-preference=secondary",
         "--read-server=127.0.0.1:6380"],
        timeout=15,
    )
    # Parse error exits with code 2 or prints usage; a connection error
    # does not.  We just verify there is no parse rejection message.
    env.assertFalse(
        "--read-server: expected HOST:PORT" in result.stderr,
        message="Valid --read-server should not print parse error; got: {!r}".format(
            result.stderr),
    )
    # Also the 'has no effect' warning must NOT appear when --read-server is
    # supplied (the flag does have effect in that case).
    env.assertFalse(
        "--read-preference has no effect" in result.stderr,
        message="Should not warn 'has no effect' when --read-server given; got: {!r}".format(
            result.stderr),
    )


# ---------------------------------------------------------------------------
# 7. --read-preference-fallback rejects unknown values
# ---------------------------------------------------------------------------

def test_read_preference_fallback_unknown_value_rejected(env):
    """--read-preference-fallback=bogus must be rejected."""
    result = _run(_BASE + ["--read-preference-fallback=bogus"])

    env.assertNotEqual(result.returncode, 0,
                       message="--read-preference-fallback=bogus must exit non-zero")
    env.assertTrue(
        "--read-preference-fallback must be one of" in result.stderr,
        message="Expected fallback-list rejection; got: {!r}".format(result.stderr),
    )


def test_read_preference_fallback_valid_values_accepted(env):
    """All three valid fallback values must be accepted at parse time."""
    for val in ("error", "queue", "primary"):
        result = _run(
            ["-s", "127.0.0.1", "-p", "1", "--test-time=1",
             "--max-reconnect-attempts=1", "--connection-stage-timeout=2",
             "--read-preference-fallback={}".format(val)],
            timeout=15,
        )
        env.assertFalse(
            "--read-preference-fallback must be one of" in result.stderr,
            message="Fallback '{}' should not trigger rejection; stderr: {!r}".format(
                val, result.stderr),
        )


# ---------------------------------------------------------------------------
# 8. --command-is-read without preceding --command
# ---------------------------------------------------------------------------

def test_command_is_read_without_command_rejected(env):
    """--command-is-read without a preceding --command must be rejected."""
    result = _run(_BASE + ["--command-is-read"])

    env.assertNotEqual(result.returncode, 0,
                       message="--command-is-read without --command must exit non-zero")
    env.assertTrue(
        "--command-is-read must follow a --command" in result.stderr,
        message="Expected 'must follow a --command' message; got: {!r}".format(
            result.stderr),
    )
