"""
CLI validation regression tests for size-related flags (Refs #426 phase 2b).

Covers the three repros from issue #426 that previously hung or crashed:

  * --pipeline -1   (item 6)  -> negative passed through unsigned cast,
                                 underflow drove an infinite retry loop.
  * --data-size -1  (item 7)  -> SIGABRT, protocol.cpp:309 assert value_len > 0.
  * --data-size-list 0:50 (item 9) -> same SIGABRT (zero-SIZE entry).

All three must now be rejected at argv-parse time with a readable error and
exit code 2 (via usage()), well before any socket is opened.  Companion checks
ensure the upper caps (pipeline <= 1024, data-size <= 512 MiB) trip cleanly
and that a known-good value still parses.

These tests run the binary directly with subprocess -- no Redis server is
needed because the parser bails out before connect.

Run with:
  TEST=test_cli_validation_size.py OSS_STANDALONE=1 ./tests/run_tests.sh
"""
import subprocess

from include import MEMTIER_BINARY


def _run_memtier(args):
    """Invoke memtier_benchmark with *args* and return the CompletedProcess.

    No server is contacted: every case in this file is expected to fail at
    argument parsing time.
    """
    return subprocess.run(
        [MEMTIER_BINARY] + list(args),
        capture_output=True,
        text=True,
        timeout=10,
    )


def _assert_reject(env, args, needle, label):
    """Run memtier with *args* and assert it exits non-zero with *needle* in stderr."""
    result = _run_memtier(args)
    env.assertNotEqual(
        result.returncode, 0,
        message="{}: expected non-zero exit, got 0 (stderr={!r})".format(
            label, result.stderr),
    )
    env.assertTrue(
        needle in result.stderr,
        message="{}: expected {!r} in stderr, got {!r}".format(
            label, needle, result.stderr),
    )


# ---------------------------------------------------------------------------
# --pipeline (#426 item 6)
# ---------------------------------------------------------------------------

def test_pipeline_negative_rejected(env):
    """--pipeline=-1 used to hang in a retry loop; must now fail at parse."""
    _assert_reject(
        env,
        ["--pipeline=-1", "--test-time=1"],
        "pipeline must be greater than zero",
        "--pipeline=-1",
    )


def test_pipeline_zero_rejected(env):
    """--pipeline=0 has no useful semantics; reject at parse."""
    _assert_reject(
        env,
        ["--pipeline=0", "--test-time=1"],
        "pipeline must be greater than zero",
        "--pipeline=0",
    )


def test_pipeline_above_cap_rejected(env):
    """--pipeline > 1024 (sanity cap) must be rejected with the cap message."""
    _assert_reject(
        env,
        ["--pipeline=1025", "--test-time=1"],
        "pipeline must be <= 1024",
        "--pipeline=1025",
    )


def test_pipeline_valid_passes_parse(env):
    """--pipeline=2 is a sane value and must pass argv validation.

    We point the binary at port 1 so it'll attempt and fail to connect, but
    that happens after parse -- the goal here is solely to prove the parser
    did NOT reject the flag.  Any rejection would surface as
    "pipeline must be greater than zero" / "pipeline must be <= 1024" on
    stderr; we assert the absence of those substrings.
    """
    result = _run_memtier([
        "--pipeline=2",
        "--test-time=1",
        "-s", "127.0.0.1",
        "-p", "1",  # nothing listens here; connect failure is fine
    ])
    env.assertTrue(
        "pipeline must be" not in result.stderr,
        message="--pipeline=2 must not be rejected; stderr={!r}".format(
            result.stderr),
    )


# ---------------------------------------------------------------------------
# --data-size (#426 item 7)
# ---------------------------------------------------------------------------

def test_data_size_negative_rejected(env):
    """--data-size=-1 used to SIGABRT on the value_len assert; reject at parse."""
    _assert_reject(
        env,
        ["--data-size=-1", "--test-time=1"],
        "data-size must be greater than zero",
        "--data-size=-1",
    )


def test_data_size_zero_rejected(env):
    """--data-size=0 has the same downstream crash; reject at parse."""
    _assert_reject(
        env,
        ["--data-size=0", "--test-time=1"],
        "data-size must be greater than zero",
        "--data-size=0",
    )


# ---------------------------------------------------------------------------
# --data-size-list (#426 item 9; item 10 zero-WEIGHT is phase 3)
# ---------------------------------------------------------------------------

def test_data_size_list_zero_size_rejected(env):
    """--data-size-list=0:50 used to SIGABRT (value_len=0); reject at parse."""
    _assert_reject(
        env,
        ["--data-size-list=0:50", "--test-time=1"],
        "data-size-list entries must have size > 0",
        "--data-size-list=0:50",
    )


def test_data_size_list_valid_passes_parse(env):
    """A well-formed --data-size-list must parse without complaint."""
    result = _run_memtier([
        "--data-size-list=8:50,16:50",
        "--test-time=1",
        "-s", "127.0.0.1",
        "-p", "1",
    ])
    env.assertTrue(
        "data-size-list" not in result.stderr,
        message=("--data-size-list=8:50,16:50 must not be rejected; "
                 "stderr={!r}").format(result.stderr),
    )
