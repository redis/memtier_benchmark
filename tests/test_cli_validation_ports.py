"""Regression tests for -p/--port, --statsd-port and --graphite-port (issue #697).

Values outside [1,65535] used to be narrowed to unsigned short before the range
check, so 65537 became port 1 and 88536 became port 23000; negative values were
negated by strtoul and accepted too. All of these must now fail during argument
parsing. --version exits 0 right after parsing, so these checks need no server.

Run with:
  TEST=test_cli_validation_ports.py ./tests/run_tests.sh
"""

import subprocess

from include import MEMTIER_BINARY

# option -> parser diagnostic prefix
PORT_OPTIONS = {
    "--port": "error: port must be",
    "--statsd-port": "error: statsd-port must be",
    "--graphite-port": "error: graphite-port must be",
}

INVALID_VALUES = (
    "65536", "65537", "88536", "131072", "70000",
    "99999999999999999999999",
    "-1", "-65535", "-0", " -1",
    "0", "", " ", "abc", "1x", "6379 ", "0x50",
)

VALID_VALUES = ("1", "6379", "65535")


def _run(args):
    return subprocess.run(
        [MEMTIER_BINARY] + args + ["--version"],
        capture_output=True,
        text=True,
        timeout=10,
    )


def test_port_options_reject_out_of_range_and_malformed(env):
    for option, diagnostic in PORT_OPTIONS.items():
        for value in INVALID_VALUES:
            result = _run([option + "=" + value])
            label = "{}={!r}".format(option, value)
            env.assertEqual(result.returncode, 2, message=label + ": " + result.stderr)
            env.assertTrue(
                diagnostic in result.stderr,
                message=label + ": expected a parser diagnostic, got " + repr(result.stderr),
            )


def test_short_port_option_rejects_out_of_range(env):
    for value in ("65537", "88536", "-65535"):
        result = _run(["-p", value])
        label = "-p {!r}".format(value)
        env.assertEqual(result.returncode, 2, message=label + ": " + result.stderr)
        env.assertTrue("error: port must be" in result.stderr, message=label + ": " + repr(result.stderr))


def test_port_options_accept_valid_values(env):
    for option, diagnostic in PORT_OPTIONS.items():
        for value in VALID_VALUES:
            result = _run([option + "=" + value])
            label = "{}={!r}".format(option, value)
            env.assertEqual(result.returncode, 0, message=label + ": " + result.stderr)
            env.assertFalse(diagnostic in result.stderr, message=label + ": " + result.stderr)


def test_port_ignores_errno_from_earlier_options(env):
    """An earlier option's strtoul overflow must not reject a valid port."""
    result = _run(["--max-reconnect-attempts=99999999999999999999999", "--port=6379", "--statsd-port=8125"])
    env.assertEqual(result.returncode, 0, message=result.stderr)
