"""Regression tests for port values: -p/--port, --statsd-port, --graphite-port,
--read-server HOST:PORT and the --uri port (issue #697).

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
    # Negatives that strtoul() wraps back into 1..65535, with and without
    # leading whitespace strtoul() skips.
    "-18446744073709551615", "-18446744073709545916", "\n-18446744073709551615",
    "\v-18446744073709551615", "- 6379", "+-6379", "++6379",
    "0", "", " ", "abc", "1x", "6379 ", "0x50",
)

# Leading whitespace and one '+' were accepted by strtoul() and still are.
VALID_VALUES = ("1", "6379", "65535", "+6379", " 6379", "\t6379")


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
    """An earlier option's strtoul overflow must not reject a valid port.

    --key-maximum does not check ERANGE yet (#725), so it is used here to
    leave errno set; once #725 lands, pick another errno source.
    """
    result = _run(["--key-maximum=99999999999999999999999", "--port=6379", "--statsd-port=8125",
                   "--graphite-port=2003"])
    env.assertEqual(result.returncode, 0, message=result.stderr)


def test_read_server_port_rejects_out_of_range_and_malformed(env):
    for value in ("65537", "0", "-1", "-18446744073709551615", "++6379", "6379x"):
        result = _run(["--read-server=127.0.0.1:" + value])
        label = "--read-server port {!r}".format(value)
        env.assertEqual(result.returncode, 2, message=label + ": " + result.stderr)
        env.assertTrue("error: --read-server: invalid port" in result.stderr, message=label + ": " + result.stderr)
    for spec in ("127.0.0.1:6379", "[::1]:6379"):
        result = _run(["--read-server=" + spec])
        env.assertEqual(result.returncode, 0, message=spec + ": " + result.stderr)


def test_uri_port_rejects_out_of_range_and_malformed(env):
    # The URI is parsed after --version would exit, so this runs a real
    # command line; it needs no server because parsing fails before any
    # connection attempt.
    for value in ("65537", "0", "-18446744073709551615", "-80"):
        result = subprocess.run(
            [MEMTIER_BINARY, "--uri=redis://127.0.0.1:" + value, "-n", "1", "-c", "1", "-t", "1"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        label = "--uri port {!r}".format(value)
        env.assertNotEqual(result.returncode, 0, message=label + ": " + result.stderr)
        env.assertTrue("error: invalid port number" in result.stderr, message=label + ": " + result.stderr)


def test_uri_rejects_malformed_ipv6_host(env):
    # Bracketed IPv6 hosts (issue #701): the bracket must be closed, non-empty
    # and followed by ':PORT', '/DB' or nothing. Parsing fails before any
    # connection attempt, so no server is needed.
    cases = {
        "redis://[::1": "error: invalid IPv6 host",
        "redis://[::1]x": "error: invalid IPv6 host",
        "redis://[::1]x:6379": "error: invalid IPv6 host",
        "redis://[]:6379": "error: invalid IPv6 host",
        "redis://[::1]:": "error: invalid port number",
        "redis://[::1]:65537": "error: invalid port number '65537'",
        "redis://u:s3cret@[::1": "error: invalid IPv6 host",
    }
    for uri, diagnostic in cases.items():
        result = subprocess.run(
            [MEMTIER_BINARY, "--uri=" + uri, "-n", "1", "-c", "1", "-t", "1"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        label = "--uri {!r}".format(uri)
        env.assertNotEqual(result.returncode, 0, message=label + ": " + result.stderr)
        env.assertTrue(diagnostic in result.stderr, message=label + ": " + result.stderr)
        # The diagnostic must not echo URI credentials.
        env.assertFalse("s3cret" in result.stderr, message=label + ": " + result.stderr)


def test_uri_accepts_ipv6_host_without_port(env):
    # Bracketed hosts without a port, with or without /DB, parse and reach the
    # connect stage (issue #701). --version exits before the URI is parsed, so
    # point at a port nobody listens on and check only for parse diagnostics.
    for uri in ("redis://[::1]/0", "redis://[::1]", "redis://[::1]/"):
        result = subprocess.run(
            [MEMTIER_BINARY, "--uri=" + uri, "--port=14599", "-n", "1", "-c", "1", "-t", "1",
             "--connection-stage-timeout=2"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        label = "--uri {!r}".format(uri)
        for diagnostic in ("invalid IPv6 host", "invalid port number", "invalid database number"):
            env.assertFalse(diagnostic in result.stderr, message=label + ": " + result.stderr)
