"""
CLI parse-time validation tests for --client-no-touch.

These tests exercise only the argument-parsing path and do not require a live
Redis server — we invoke the binary with subprocess, expect a specific exit
code and a clear message in stderr.

Tests covered:
  1. --client-no-touch with -P memcache_text is a hard error.
  2. --client-no-touch with -P memcache_binary is a hard error.
  3. --client-no-touch with the (default) redis protocol is accepted at
     parse time.
  4. --show-config / mb.json echo client-no-touch correctly, on and off
     (same shape as test_cli_validation_prometheus.py's V17 test).

Run with:
  TEST=test_cli_validation_client_no_touch.py OSS_STANDALONE=1 ./tests/run_tests.sh
"""
import json
import os
import subprocess
import tempfile

from include import MEMTIER_BINARY


def _run(args, timeout=10):
    """Run memtier_benchmark with *args*, return CompletedProcess."""
    return subprocess.run(
        [MEMTIER_BINARY] + args,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


# Dead port + short timeouts: parses cleanly and proceeds to connect, but
# exits fast instead of hitting the subprocess timeout. --show-config prints
# before the connection loop starts either way.
_DEAD_PORT_BASE = [
    "-s", "127.0.0.1", "-p", "1", "--test-time=1",
    "--max-reconnect-attempts=1", "--connection-stage-timeout=2",
]


def test_client_no_touch_rejected_for_memcache_text(env):
    """--client-no-touch with -P memcache_text must be rejected."""
    result = _run(
        ["-s", "127.0.0.1", "-p", "6379", "--test-time=1",
         "-P", "memcache_text", "--client-no-touch"],
    )
    env.assertNotEqual(
        result.returncode, 0,
        message="--client-no-touch with memcache_text must exit non-zero",
    )
    env.assertTrue(
        "client-no-touch can only be used with redis protocol" in result.stderr,
        message="Expected protocol-rejection message; got: {!r}".format(result.stderr),
    )


def test_client_no_touch_rejected_for_memcache_binary(env):
    """--client-no-touch with -P memcache_binary must be rejected."""
    result = _run(
        ["-s", "127.0.0.1", "-p", "6379", "--test-time=1",
         "-P", "memcache_binary", "--client-no-touch"],
    )
    env.assertNotEqual(
        result.returncode, 0,
        message="--client-no-touch with memcache_binary must exit non-zero",
    )
    env.assertTrue(
        "client-no-touch can only be used with redis protocol" in result.stderr,
        message="Expected protocol-rejection message; got: {!r}".format(result.stderr),
    )


def test_client_no_touch_accepted_for_redis_protocol(env):
    """--client-no-touch with the default (redis) protocol must not be
    rejected at parse time. Uses a dead port so the binary fails at connect
    time instead of actually running."""
    result = _run(
        ["-s", "127.0.0.1", "-p", "1", "--test-time=1",
         "--max-reconnect-attempts=1", "--connection-stage-timeout=2",
         "--client-no-touch"],
        timeout=15,
    )
    env.assertFalse(
        "client-no-touch can only be used with redis protocol" in result.stderr,
        message="redis protocol should not trigger rejection; stderr: {!r}".format(
            result.stderr),
    )
    # Guard against a vacuous pass: the assertion above would also go green
    # if --client-no-touch were missing from long_options[] entirely and
    # getopt rejected it as unrecognized, since that message doesn't
    # contain "client-no-touch can only be used with redis protocol" either.
    env.assertFalse(
        "unrecognized option" in result.stderr,
        message="--client-no-touch was not recognized as a valid option; stderr: {!r}".format(
            result.stderr),
    )


def test_client_no_touch_show_config(env):
    """--show-config echoes 'client-no-touch = yes'/'no' correctly (config_print()
    uses yes/no, not true/false -- config_print_to_json() is the true/false one,
    checked separately below), and mb.json's client-no-touch key matches. Nothing
    previously asserted on either, so a config_print()/config_print_to_json()
    ordering slip or a dropped field would have gone unnoticed."""
    result_on = _run(_DEAD_PORT_BASE + ["--show-config", "--client-no-touch"])
    out_on = result_on.stderr + result_on.stdout
    env.assertTrue(
        "client-no-touch = yes" in out_on,
        message="--show-config with --client-no-touch must print 'client-no-touch = yes'; "
                "out={!r}".format(out_on),
    )

    result_off = _run(_DEAD_PORT_BASE + ["--show-config"])
    out_off = result_off.stderr + result_off.stdout
    env.assertTrue(
        "client-no-touch = no" in out_off,
        message="--show-config without --client-no-touch must print 'client-no-touch = no'; "
                "out={!r}".format(out_off),
    )

    test_dir = tempfile.mkdtemp()
    json_path = os.path.join(test_dir, "mb.json")
    result_json = _run(_DEAD_PORT_BASE + ["--client-no-touch", "--json-out-file", json_path])
    with open(json_path) as f:
        data = json.load(f)
    env.assertEqual(
        data["configuration"]["client-no-touch"], "true",
        message="mb.json configuration.client-no-touch should be \"true\"; got {!r}".format(
            data.get("configuration", {}).get("client-no-touch")),
    )
