"""
CLI parse-time validation tests for the --prometheus-* family of flags
(V1-V19 from the Prometheus-exporter plan, section 10).

These tests exercise the argument-parsing / start() path of the exporter.
Most cases need no live Redis: we invoke the binary with subprocess, expect a
specific exit code and a canonical message (the §5 E1-E12 / W1 / W2 / W2b
strings) as a stderr substring.  This mirrors test_cli_validation_read_preference.py.

A subtlety pinned by this suite: on this CI substrate a *connection failure*
(dead port) also exits with code 2, so exit code alone cannot distinguish a
parse rejection from a failed run.  The canonical error/warning *string* is
therefore the real discriminator:
  * reject cases    -> exit 2 AND the canonical message present;
  * accept cases    -> the canonical reject message absent (rc may be 0 or a
                       connection-failure 2; we never run a real workload);
  * warn-accept     -> the canonical reject message absent AND the warning
                       present exactly once.

A few cases need a live server (V14 bind collision; V19 exit(1)-after-creation
is reached pre-connect so needs none); those read the address from the RLTest
`env` fixture.

Run with:
  TEST=test_cli_validation_prometheus.py OSS_STANDALONE=1 \
    REDIS_SERVER=/usr/local/bin/redis-server \
    MEMTIER_BINARY=$PWD/memtier_benchmark ./tests/run_tests.sh
"""
import json
import logging
import os
import socket
import subprocess

from include import MEMTIER_BINARY

# A compiled-out (no-prometheus) binary for V16.  Optional: the test SKIPs
# gracefully when the env var is unset.
MEMTIER_NOPROM_BINARY = os.environ.get("MEMTIER_NOPROM_BINARY")

# ---------------------------------------------------------------------------
# Canonical strings (single source of truth: PLAN.md §5).  Tests assert these
# verbatim as substrings.
# ---------------------------------------------------------------------------
E1_PORT = "prometheus-port must be a number in the range [0-65535]"
E2_ADDR = "prometheus-bind-addr must be a numeric IPv4 or IPv6 address"
E3_KV = "--prometheus-run-label expects KEY=VALUE."
E4_NAME = "invalid label name"
E5_DUNDER = "names beginning with __ are reserved"
E6_RESERVED = "is reserved."
E7_KEYLEN = "label name exceeds 128 bytes."
E8_VALLEN = "exceeds 256 bytes."
E9_DUP = "duplicate label name"
E10_MANY = "too many labels (max 16)."
E11_BUCKETS = "prometheus-latency-buckets must be 1-64 comma-separated"
E12_DEP = "requires --prometheus-port."
W1_LOOPBACK = "is not a loopback address"
W2_FLOOR = "is below the 1e-05 s recordable latency floor"
W2B_CAP = "is above the 600 s recordable latency cap"


def _run(args, timeout=15):
    """Run memtier_benchmark with *args*, return CompletedProcess."""
    return subprocess.run(
        [MEMTIER_BINARY] + args,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


# Reject vectors are parsed and rejected before any connection is attempted,
# so a dead port is fine and fastest.
_BASE = ["-s", "127.0.0.1", "-p", "6379"]

# Accept/warn vectors parse cleanly and would proceed to connect; bound the
# connect loop hard so a dead port exits quickly instead of hitting the
# subprocess timeout (same trick as the read-preference template).
_BASE_ACCEPT = [
    "-s", "127.0.0.1", "-p", "1", "--test-time=1",
    "--max-reconnect-attempts=1", "--connection-stage-timeout=2",
]


def _out(result):
    """Combined stderr+stdout for substring checks."""
    return result.stderr + result.stdout


# ===========================================================================
# V1-V5: --prometheus-port rejects (E1; V5 = bare flag, getopt owns the text)
# ===========================================================================

def test_v1_v4_port_rejects_e1(env):
    """Non-numeric / wrap-guard / negative / trailing-junk / empty -> exit 2 + E1."""
    for bad in ("abc", "70000", "-1", "9219x", ""):
        result = _run(_BASE + ["--prometheus-port={}".format(bad)])
        env.assertEqual(result.returncode, 2,
                        message="--prometheus-port={!r} must exit 2; got {}".format(
                            bad, result.returncode))
        env.assertTrue(E1_PORT in result.stderr,
                       message="--prometheus-port={!r} expected E1; stderr={!r}".format(
                           bad, result.stderr))


def test_v5_bare_port_flag_exit2(env):
    """Bare --prometheus-port (no value) -> getopt 'requires an argument', exit 2.
    V5 pins the exit code only; getopt owns the text."""
    result = _run(_BASE + ["--prometheus-port"])
    env.assertEqual(result.returncode, 2,
                    message="bare --prometheus-port must exit 2; got {}".format(
                        result.returncode))


# ===========================================================================
# V6: --prometheus-port=0 accepted (ephemeral)
# ===========================================================================

def test_v6_port_zero_accepted(env):
    """--prometheus-port=0 parses (no E1)."""
    result = _run(_BASE_ACCEPT + ["--prometheus-port=0"])
    env.assertFalse(E1_PORT in result.stderr,
                    message="--prometheus-port=0 must not be rejected; stderr={!r}".format(
                        result.stderr))


# ===========================================================================
# V7 / V7b: --prometheus-bind-addr rejects (E2) and accepts
# ===========================================================================

def test_v7_bind_addr_rejects_e2(env):
    """Hostnames / bad literals / bad brackets -> exit 2 + E2."""
    for bad in ("999.999.1.1", "not_an_addr", "localhost", "example.com",
                "[::1", "[1.2.3.4]"):
        result = _run(_BASE + ["--prometheus-port=0",
                               "--prometheus-bind-addr={}".format(bad)])
        env.assertEqual(result.returncode, 2,
                        message="--prometheus-bind-addr={!r} must exit 2; got {}".format(
                            bad, result.returncode))
        env.assertTrue(E2_ADDR in result.stderr,
                       message="--prometheus-bind-addr={!r} expected E2; stderr={!r}".format(
                           bad, result.stderr))


def test_v7b_bind_addr_accepts(env):
    """Numeric IPv4/IPv6 literals (incl. bracketed [::1]) parse (no E2)."""
    for good in ("127.0.0.1", "127.255.0.7", "0.0.0.0", "::1", "::", "[::1]"):
        result = _run(_BASE_ACCEPT + ["--prometheus-port=0",
                                      "--prometheus-bind-addr={}".format(good)])
        env.assertFalse(E2_ADDR in result.stderr,
                        message="--prometheus-bind-addr={!r} must be accepted; stderr={!r}".format(
                            good, result.stderr))


# ===========================================================================
# V8: dependency error (E12) for every non-port flag without --prometheus-port
# ===========================================================================

def test_v8_dependency_e12(env):
    """Any --prometheus-* flag (other than the port) without --prometheus-port -> E12."""
    for flag in ("--prometheus-bind-addr=127.0.0.1",
                 "--prometheus-run-label=a=1",
                 "--prometheus-latency-buckets=0.001,0.005"):
        result = _run(_BASE + [flag])
        env.assertEqual(result.returncode, 2,
                        message="{} without port must exit 2; got {}".format(
                            flag, result.returncode))
        env.assertTrue(E12_DEP in result.stderr,
                       message="{} expected E12; stderr={!r}".format(flag, result.stderr))


# ===========================================================================
# V9-V11: --prometheus-run-label rejects (E3-E10) and accepts
# ===========================================================================

def test_v9_v11_run_label_rejects(env):
    """Run-label rejection matrix: E3-E10 (incl. 129-byte key, 257-byte value,
    duplicate, 17th label)."""
    long_key = "k" * 129
    long_val = "v" * 257
    cases = [
        (["--prometheus-run-label=nofeq"], E3_KV),         # E3 missing '='
        (["--prometheus-run-label=1bad=x"], E4_NAME),      # E4 bad name
        (["--prometheus-run-label=k-ey=x"], E4_NAME),      # E4 bad charset
        (["--prometheus-run-label=__x=1"], E5_DUNDER),     # E5 __ prefix
        (["--prometheus-run-label=le=1"], E6_RESERVED),    # E6 reserved
        (["--prometheus-run-label=version=1"], E6_RESERVED),
        (["--prometheus-run-label=git_sha=1"], E6_RESERVED),
        (["--prometheus-run-label={}=x".format(long_key)], E7_KEYLEN),  # E7
        (["--prometheus-run-label=k={}".format(long_val)], E8_VALLEN),  # E8
        (["--prometheus-run-label=a=1", "--prometheus-run-label=a=2"], E9_DUP),  # E9 dup
    ]
    for extra, needle in cases:
        result = _run(_BASE + ["--prometheus-port=0"] + extra)
        env.assertEqual(result.returncode, 2,
                        message="run-label {!r} must exit 2; got {}".format(
                            extra, result.returncode))
        env.assertTrue(needle in result.stderr,
                       message="run-label {!r} expected {!r}; stderr={!r}".format(
                           extra, needle, result.stderr))


def test_v10_too_many_labels_e10(env):
    """17 labels -> E10 (max 16)."""
    extra = ["--prometheus-run-label=k{}=v".format(i) for i in range(17)]
    result = _run(_BASE + ["--prometheus-port=0"] + extra)
    env.assertEqual(result.returncode, 2,
                    message="17 labels must exit 2; got {}".format(result.returncode))
    env.assertTrue(E10_MANY in result.stderr,
                   message="17 labels expected E10; stderr={!r}".format(result.stderr))


def test_v11_run_label_accepts(env):
    """phase=x, _x9=1, k= (empty value) all parse (no rejection)."""
    for label in ("phase=x", "_x9=1", "k="):
        result = _run(_BASE_ACCEPT + ["--prometheus-port=0",
                                      "--prometheus-run-label={}".format(label)])
        for needle in (E3_KV, E4_NAME, E5_DUNDER, E6_RESERVED,
                       E7_KEYLEN, E8_VALLEN, E9_DUP, E10_MANY):
            env.assertFalse(needle in result.stderr,
                            message="label {!r} should not be rejected ({!r}); stderr={!r}".format(
                                label, needle, result.stderr))


# ===========================================================================
# V12-V13: --prometheus-latency-buckets rejects (E11) and warn-accepts (W2/W2b)
# ===========================================================================

def test_v12_buckets_reject_e11(env):
    """Bucket-list rejection vectors -> exit 2 + E11.

    Covers: descending, over-cap as a list, single under-floor, over-cap
    bound, llround-overflow canaries (1e13 / 1e300), top-of-range slot
    collision (86300,86400), and same-slot collision (0.001,0.001001)."""
    for bad in ("5,3,1", "0.001,100000", "1e-7", "86400.000001",
                "1e13", "1e300", "86300,86400", "0.001,0.001001"):
        result = _run(_BASE + ["--prometheus-port=0",
                               "--prometheus-latency-buckets={}".format(bad)])
        env.assertEqual(result.returncode, 2,
                        message="buckets={!r} must exit 2; got {}".format(
                            bad, result.returncode))
        env.assertTrue(E11_BUCKETS in result.stderr,
                       message="buckets={!r} expected E11; stderr={!r}".format(
                           bad, result.stderr))


def test_v13_buckets_accept(env):
    """A plain in-range list parses with no rejection and no warning."""
    result = _run(_BASE_ACCEPT + ["--prometheus-port=0",
                                  "--prometheus-latency-buckets=0.001,0.005,0.05"])
    env.assertFalse(E11_BUCKETS in result.stderr,
                    message="in-range buckets must be accepted; stderr={!r}".format(
                        result.stderr))
    env.assertEqual(result.stderr.count("prometheus-latency-buckets:"), 0,
                    message="in-range buckets must emit no warning; stderr={!r}".format(
                        result.stderr))


def test_v13_buckets_warn_accept(env):
    """Warn-accept vectors: exactly one warning line each, no E11 rejection.

    First-out-of-range offender wins: 0.000005,700 -> only the floor warning."""
    cases = [
        ("0.000005,1", W2_FLOOR, W2B_CAP),    # floor warning only
        ("1,700", W2B_CAP, W2_FLOOR),         # cap warning only
        ("1,86400", W2B_CAP, W2_FLOOR),       # cap warning (= the 86400 cap)
        ("0.000005,700", W2_FLOOR, W2B_CAP),  # first offender (floor) wins
    ]
    for buckets, present, absent in cases:
        result = _run(_BASE_ACCEPT + ["--prometheus-port=0",
                                      "--prometheus-latency-buckets={}".format(buckets)])
        env.assertFalse(E11_BUCKETS in result.stderr,
                        message="warn-accept buckets={!r} must not be E11-rejected; stderr={!r}".format(
                            buckets, result.stderr))
        env.assertEqual(result.stderr.count("prometheus-latency-buckets:"), 1,
                        message="buckets={!r} must emit exactly one warning; stderr={!r}".format(
                            buckets, result.stderr))
        env.assertTrue(present in result.stderr,
                       message="buckets={!r} expected warning {!r}; stderr={!r}".format(
                           buckets, present, result.stderr))
        env.assertFalse(absent in result.stderr,
                        message="buckets={!r} must NOT carry {!r}; stderr={!r}".format(
                            buckets, absent, result.stderr))


# ===========================================================================
# V14: bind collision (pre-bound ephemeral socket) -> exit 1, bind message,
#      and NO sanitizer output (load-bearing under asan: a free-without-NULL
#      regression double-frees on this exact path).
# ===========================================================================

def test_v14_bind_collision_exit1(env):
    """Point the exporter at an already-bound port; start() must fail loud
    (exit 1 + 'failed to bind <addr>:<port>') with no sanitizer output."""
    nodes = env.getMasterNodesList()
    host = nodes[0].get("host", "127.0.0.1")
    port = str(nodes[0]["port"])

    squat = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    squat.bind(("127.0.0.1", 0))
    squat.listen(1)
    busy_port = squat.getsockname()[1]
    try:
        result = _run(["-s", host, "-p", port, "--test-time=2",
                       "--prometheus-port={}".format(busy_port)],
                      timeout=30)
    finally:
        squat.close()

    env.assertEqual(result.returncode, 1,
                    message="bind collision must exit 1; got {}".format(result.returncode))
    env.assertTrue("failed to bind 127.0.0.1:{}".format(busy_port) in _out(result),
                   message="expected bind-failure message; out={!r}".format(_out(result)))
    for needle in ("AddressSanitizer", "LeakSanitizer"):
        env.assertFalse(needle in _out(result),
                        message="bind-collision path emitted {!r}; out={!r}".format(
                            needle, _out(result)))


# ===========================================================================
# V15: --prometheus-bind-addr=0.0.0.0 -> W1 emitted exactly once.
# ===========================================================================

def test_v15_non_loopback_warning_once(env):
    """0.0.0.0 is a valid but non-loopback bind addr: parse succeeds and the
    W1 warning is emitted exactly once."""
    result = _run(_BASE_ACCEPT + ["--prometheus-port=0",
                                  "--prometheus-bind-addr=0.0.0.0"])
    env.assertFalse(E2_ADDR in result.stderr,
                    message="0.0.0.0 must be accepted; stderr={!r}".format(result.stderr))
    env.assertEqual(result.stderr.count(W1_LOOPBACK), 1,
                    message="W1 must appear exactly once; stderr={!r}".format(result.stderr))


# ===========================================================================
# V16: compiled-out binary -> 'unrecognized option', exit 2.  SKIP if the
#      no-prometheus binary path is not provided.
# ===========================================================================

def test_v16_compiled_out_unrecognized(env):
    """The four flags are unknown options on a --disable-prometheus build."""
    if not MEMTIER_NOPROM_BINARY:
        # Graceful skip: a no-prometheus binary is optional for this suite
        # (PLAN.md §10 V16: "graceful [SKIP] when unset").
        logging.info("[SKIP] V16: MEMTIER_NOPROM_BINARY not set; "
                     "compiled-out 'unrecognized option' check not run")
        return
    for flag in ("--prometheus-port=9100", "--prometheus-bind-addr=127.0.0.1",
                 "--prometheus-run-label=a=1", "--prometheus-latency-buckets=0.001"):
        result = subprocess.run(
            [MEMTIER_NOPROM_BINARY, "-s", "127.0.0.1", "-p", "6379", flag],
            capture_output=True, text=True, timeout=15)
        env.assertEqual(result.returncode, 2,
                        message="compiled-out {} must exit 2; got {}".format(
                            flag, result.returncode))
        env.assertTrue("unrecognized option" in result.stderr,
                       message="compiled-out {} expected 'unrecognized option'; stderr={!r}".format(
                           flag, result.stderr))


# ===========================================================================
# V17: --show-config echoes the four fields; flagless run prints the disabled
#      sentinel and the 'default' bucket marker.  (config dump -> stderr)
# ===========================================================================

def test_v17_show_config_disabled(env):
    """Flagless --show-config prints prometheus-port = -1 and
    prometheus-latency-buckets = default; no secrets in the four fields."""
    result = _run(_BASE + ["--show-config", "--test-time=1",
                           "--max-reconnect-attempts=1",
                           "--connection-stage-timeout=2"], timeout=20)
    out = _out(result)
    env.assertTrue("prometheus-port = -1" in out,
                   message="disabled show-config must print 'prometheus-port = -1'; out={!r}".format(out))
    env.assertTrue("prometheus-latency-buckets = default" in out,
                   message="disabled show-config must print bucket 'default'; out={!r}".format(out))


def test_v17_show_config_enabled(env):
    """--show-config with all four flags echoes parsed values, buckets as
    parsed floats."""
    result = _run(_BASE + ["--show-config", "--test-time=1",
                           "--max-reconnect-attempts=1", "--connection-stage-timeout=2",
                           "--prometheus-port=0", "--prometheus-bind-addr=127.0.0.1",
                           "--prometheus-run-label=phase=load",
                           "--prometheus-latency-buckets=0.001,0.005"], timeout=20)
    out = _out(result)
    env.assertTrue("prometheus-port = 0" in out,
                   message="show-config must echo port; out={!r}".format(out))
    env.assertTrue("prometheus-bind-addr = 127.0.0.1" in out,
                   message="show-config must echo bind-addr; out={!r}".format(out))
    env.assertTrue("phase=load" in out,
                   message="show-config must echo run-label; out={!r}".format(out))
    # Bucket line compared as parsed floats (precision-agnostic).
    bucket_line = None
    for line in out.splitlines():
        if line.startswith("prometheus-latency-buckets ="):
            bucket_line = line.split("=", 1)[1].strip()
            break
    env.assertTrue(bucket_line is not None,
                   message="show-config missing bucket line; out={!r}".format(out))
    parsed = [float(x) for x in bucket_line.split(",")]
    env.assertEqual(parsed, [0.001, 0.005],
                    message="bucket line floats mismatch; got {!r}".format(bucket_line))


# ===========================================================================
# V18: mb.json carries the four keys; a tricky run-label round-trips through
#      JSON unescaping; no secrets.
# ===========================================================================

def test_v18_mb_json_round_trip(env):
    """--json-out-file produces valid JSON with the four prometheus keys; a
    run-label value containing '"' and '\\' round-trips after JSON parsing."""
    import tempfile
    fd, path = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    try:
        result = _run(_BASE + ["--show-config", "--test-time=1",
                               "--max-reconnect-attempts=1", "--connection-stage-timeout=2",
                               "--json-out-file={}".format(path),
                               "--prometheus-port=12345",
                               "--prometheus-bind-addr=127.0.0.1",
                               "--prometheus-run-label=x=a\"b\\c",
                               "--prometheus-latency-buckets=0.001,0.005"], timeout=20)
        with open(path) as f:
            doc = json.load(f)  # must be valid JSON (V18 parseability pin)
        cfg = doc.get("configuration", {})
        for key in ("prometheus-port", "prometheus-bind-addr",
                    "prometheus-run-labels", "prometheus-latency-buckets"):
            env.assertTrue(key in cfg,
                           message="mb.json missing key {!r}; cfg keys={!r}".format(
                               key, list(cfg.keys())))
        env.assertEqual(cfg["prometheus-port"], 12345,
                        message="mb.json prometheus-port mismatch: {!r}".format(
                            cfg.get("prometheus-port")))
        # The label value survives JSON encode+decode byte-for-byte.
        env.assertEqual(cfg["prometheus-run-labels"], "x=a\"b\\c",
                        message="run-label JSON round-trip failed: {!r}".format(
                            cfg.get("prometheus-run-labels")))
    finally:
        os.unlink(path)


# ===========================================================================
# V19: exit(1)-after-creation must stay leak-silent (g_prom_exporter anchor).
#      Cluster multi-key --command is rejected post-creation, pre-connect, so
#      no server is needed.
# ===========================================================================

def test_v19_lsan_exit1_after_creation(env):
    """--prometheus-port=0 --cluster-mode with a multi-key --command rejects
    after the exporter is created -> rc 1, the single-key message, and no
    sanitizer/leak output (the deliberately-leaked exporter stays reachable
    via g_prom_exporter)."""
    # The single-key rejection counts placeholder keys (__key__), so the
    # multi-key command must use two __key__ placeholders to trip keys_count>1.
    # (PLAN.md spells it 'mset k1 v1 k2 v2'; on this build literal args are not
    # counted as keys -- the placeholder form is the equivalent trigger.)
    result = _run(["--prometheus-port=0", "--cluster-mode",
                   "--command=MSET __key__ v1 __key__ v2"], timeout=20)
    env.assertEqual(result.returncode, 1,
                    message="cluster multi-key command must exit 1; got {}".format(
                        result.returncode))
    env.assertTrue("Cluster mode supports only a single key" in _out(result),
                   message="expected single-key rejection; out={!r}".format(_out(result)))
    for needle in ("LeakSanitizer", "AddressSanitizer"):
        env.assertFalse(needle in _out(result),
                        message="exit(1)-after-creation emitted {!r}; out={!r}".format(
                            needle, _out(result)))
