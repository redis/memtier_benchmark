"""
Regression tests for issue #426 phase 3: sampler / Gaussian-distribution
safety.

Before these fixes, memtier_benchmark accepted several sampler configurations
that could never produce a value at runtime and either hung the worker loop
or aborted via assert. The parser now rejects them up front with a clear
error and exit code 2:

  Item 10 -- ``--data-size-list 8:0``: a zero-weight bucket made the size
             sampler skip every entry, so it spun forever picking nothing.

  Item 15 -- ``--key-pattern G:G --key-stddev inf`` (or ``nan``): the
             Gaussian rejection sampler can never satisfy ``val < min ||
             val > max + 1`` on a non-finite stddev, so the worker loop
             never produces a key.

  Item 16 -- ``--key-pattern G:G`` with a 1-key range: the Gaussian
             distribution requires ``median > min && median < max``, which
             is impossible on a degenerate range. Tripped an assert
             (SIGABRT) before the fix.

These tests only exercise the parser, so they do not need to talk to the
server. We still take an ``env`` arg (RLTest convention) and use the
master node only to keep the invocation shape close to the other validation
tests.
"""
import subprocess

from include import MEMTIER_BINARY


def _run_memtier(args):
    """Run memtier_benchmark with *args* and return the CompletedProcess."""
    return subprocess.run(
        [MEMTIER_BINARY] + args,
        capture_output=True,
        text=True,
        timeout=10,
    )


def _common_args(env):
    """Minimal connection args; the parser rejects before any connect."""
    master = env.getMasterNodesList()[0]
    return ["-s", "127.0.0.1", "-p", str(master["port"])]


# ---------------------------------------------------------------------------
# Item 10: --data-size-list with a zero-weight entry must be rejected
# ---------------------------------------------------------------------------

def test_data_size_list_zero_weight_rejected(env):
    """``--data-size-list 8:0`` previously hung; must now exit 2 at parse."""
    env.skipOnCluster()

    result = _run_memtier(_common_args(env) + [
        "--data-size-list=8:0",
        "--test-time=1",
    ])

    env.assertEqual(
        result.returncode, 2,
        message="--data-size-list=8:0 must exit with parser error code 2",
    )
    env.assertTrue(
        "data-size-list" in result.stderr and "weight" in result.stderr,
        message="Expected weight-rejection diagnostic in stderr; got: {!r}".format(
            result.stderr[:400]
        ),
    )


# ---------------------------------------------------------------------------
# Item 15: --key-stddev must be finite and > 0
# ---------------------------------------------------------------------------

def test_key_stddev_inf_rejected(env):
    """``--key-stddev inf`` previously hung; must now exit 2 at parse."""
    env.skipOnCluster()

    result = _run_memtier(_common_args(env) + [
        "--key-pattern", "G:G",
        "--key-stddev", "inf",
        "--test-time=1",
    ])

    env.assertEqual(
        result.returncode, 2,
        message="--key-stddev inf must exit with parser error code 2",
    )
    env.assertTrue(
        "key-stddev" in result.stderr and "finite" in result.stderr,
        message="Expected finite-stddev diagnostic in stderr; got: {!r}".format(
            result.stderr[:400]
        ),
    )


def test_key_stddev_nan_rejected(env):
    """``--key-stddev nan`` previously slipped through (NaN compares false to
    everything); must now exit 2 at parse."""
    env.skipOnCluster()

    result = _run_memtier(_common_args(env) + [
        "--key-pattern", "G:G",
        "--key-stddev", "nan",
        "--test-time=1",
    ])

    env.assertEqual(
        result.returncode, 2,
        message="--key-stddev nan must exit with parser error code 2",
    )
    env.assertTrue(
        "key-stddev" in result.stderr and "finite" in result.stderr,
        message="Expected finite-stddev diagnostic in stderr; got: {!r}".format(
            result.stderr[:400]
        ),
    )


# ---------------------------------------------------------------------------
# Item 16: G:G with a degenerate key range must be rejected
# ---------------------------------------------------------------------------

def test_key_pattern_g_one_key_range_rejected(env):
    """``--key-pattern G:G --key-minimum=1 --key-maximum=1`` previously
    SIGABRTed on the Gaussian median assert; must now exit 2 at parse."""
    env.skipOnCluster()

    result = _run_memtier(_common_args(env) + [
        "--key-pattern", "G:G",
        "--key-minimum=1",
        "--key-maximum=1",
        "--test-time=1",
    ])

    env.assertEqual(
        result.returncode, 2,
        message="G:G with a 1-key range must exit with parser error code 2",
    )
    env.assertTrue(
        "key-pattern=G" in result.stderr,
        message="Expected G-range diagnostic in stderr; got: {!r}".format(
            result.stderr[:400]
        ),
    )


# ---------------------------------------------------------------------------
# Positive case: a healthy Gaussian config must still parse successfully.
# ---------------------------------------------------------------------------

def test_key_pattern_g_valid_range_accepted(env):
    """``--key-pattern G:G --key-minimum=1 --key-maximum=1000 --key-stddev=100``
    must still parse cleanly (no regression on the happy path).

    The point of this test is to verify the *parser* accepts the config -- the
    subsequent network workload is incidental. The test harness doesn't inject
    --tls/--cert args here, so on TLS-only CI cells the workload itself fails
    on the plain-TCP connection. To keep this a pure parser-accept regression
    test we assert only that none of the parser-rejection diagnostics for the
    sampler configs (Items 10/15/16) appear in stderr, and that the parser did
    not exit with the dedicated parser-error code (rc==2). The workload's
    runtime outcome is intentionally ignored."""
    env.skipOnCluster()

    # NOTE: we don't pass --tls/--cert flags; on TLS-only CI cells the
    # workload below cannot connect. That's fine -- we only care about the
    # parser path. Bound the run tightly so a TLS-only environment can't
    # keep the subprocess alive past _run_memtier's 10s timeout:
    # --connection-timeout=1 fails fast on the bad handshake and
    # --max-reconnect-attempts=1 caps the post-fail thread-restart loop.
    # --requests=1 keeps the successful-connect path trivially short on
    # plaintext cells.
    result = _run_memtier(_common_args(env) + [
        "--key-pattern", "G:G",
        "--key-minimum=1",
        "--key-maximum=1000",
        "--key-stddev=100",
        "--requests=1",
        "--threads=1",
        "--clients=1",
        "--pipeline=1",
        "--connection-timeout=1",
        "--max-reconnect-attempts=1",
        "--hide-histogram",
    ])

    # Parser must NOT have rejected this valid G:G config.
    env.assertNotEqual(
        result.returncode, 2,
        message=("Valid G:G config must not trip the parser; got rc=2 with "
                 "stderr={!r}".format(result.stderr[:400])),
    )
    # Belt-and-suspenders: none of the sampler-rejection diagnostics from the
    # negative tests above (Items 10/15/16) should be present in stderr.
    parser_rejected = (
        ("key-pattern=G requires" in result.stderr) or
        ("key-stddev" in result.stderr and "finite" in result.stderr) or
        ("data-size-list" in result.stderr and "weight" in result.stderr)
    )
    env.assertFalse(
        parser_rejected,
        message=("Parser must not emit a sampler-rejection diagnostic for the "
                 "valid G:G config; stderr={!r}".format(result.stderr[:400])),
    )
