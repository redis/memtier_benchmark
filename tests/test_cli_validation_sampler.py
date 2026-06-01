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
    must still parse and run cleanly (no regression on the happy path)."""
    env.skipOnCluster()

    result = _run_memtier(_common_args(env) + [
        "--key-pattern", "G:G",
        "--key-minimum=1",
        "--key-maximum=1000",
        "--key-stddev=100",
        "--test-time=1",
    ])

    env.assertEqual(
        result.returncode, 0,
        message=("Valid G:G config must run cleanly; got rc={} stderr={!r}"
                 .format(result.returncode, result.stderr[:400])),
    )
