"""
Hypothesis-based CLI argument fuzzer for memtier_benchmark.

See https://github.com/redis/memtier_benchmark/issues/410 for motivation.

Goal: every CLI flag is an input surface (CI templates, Helm charts, REST APIs
interpolate user-supplied values into flags). Existing tests exercise happy
paths only; this test feeds intentionally weird values into a curated set of
long flags and asserts that memtier_benchmark either runs cleanly or exits
with a parser-error return code -- never crashes, never hangs, never trips a
sanitizer.

Gated behind MEMTIER_FUZZ=1 so default CI is unaffected. Intended to run
nightly under ASAN/UBSan builds where any banned-needle hit becomes a fast
regression signal.

Out of scope (per issue #410): --tls*, --uri, --data-import,
--failed-keys-file, --monitor-input (need setup harness; follow-up).

Usage:
    MEMTIER_FUZZ=1 MEMTIER_BINARY=$PWD/memtier_benchmark \\
        pytest tests/cli_fuzz.py --hypothesis-seed=0 -x -v
"""

import os
import shutil
import subprocess

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import HealthCheck, given, settings, strategies as st  # noqa: E402

FUZZ_ENABLED = os.environ.get("MEMTIER_FUZZ") == "1"
pytestmark = pytest.mark.skipif(
    not FUZZ_ENABLED, reason="CLI fuzzer disabled (set MEMTIER_FUZZ=1 to enable)"
)

MEMTIER = os.environ.get("MEMTIER_BINARY") or shutil.which("memtier_benchmark")
HOST = os.environ.get("MEMTIER_FUZZ_HOST", "127.0.0.1")
PORT = os.environ.get("MEMTIER_FUZZ_PORT", "6379")

# Stderr substrings that indicate a real crash or sanitizer hit. Hitting any
# of these is an automatic fail -- the parser is allowed to reject input, but
# never to crash on it.
BANNED_NEEDLES = (
    "stack smashing",
    "Segmentation fault",
    "Aborted",
    "AddressSanitizer",
    "UndefinedBehaviorSanitizer",
    "runtime error:",
    "munmap_chunk",
    "double free",
    "SIGSEGV",
    "SIGABRT",
)

# Acceptable exit codes: 0 success, 1/2 parser/runtime error.
OK_RETURN_CODES = {0, 1, 2}

# Per-issue strategies. We deliberately mix a curated corpus of known-weird
# tokens (negatives, INT_MAX, scientific notation, comma decimal, format
# specifiers, etc.) with a small hypothesis-generated random branch so the
# engine can shrink to a minimal failing example if anything trips.
# Weird-int corpus geared at the **parser**, not the allocator. Several
# count-style flags (--run-count, --pipeline, --data-size, ...) are piped
# straight into vector resizes / mallocs without bounds-checking, so feeding
# them INT_MAX / LLONG_MAX or a negative value reliably aborts the process
# with std::bad_alloc or an assertion. That's a known bug class -- tracked
# as a follow-up for the same #410 series -- and orthogonal to what we're
# trying to catch here, which is *parser* sanity (empty string, leading
# space, fractional, comma decimal, hex, scientific notation, plain
# overflow-as-string). We therefore keep all generated values small enough
# that they cannot themselves cause an OOM regardless of which flag they
# land on, while still exercising every interesting *string* shape.
weird_int = st.one_of(
    st.sampled_from(
        [
            "0",
            "1",
            "2",
            "10",
            "100",
            "0x7f",
            "1e3",
            "1e9999",  # parser-level overflow, *not* allocator
            "",
            " ",
            "1.5",
            "1,5",
            # Negative values: #436 parser rejects these before any
            # allocation or workload loop can run.
            "-1",
            "-100",
        ]
    ),
    st.integers(min_value=0, max_value=10000).map(str),
)

weird_float = st.sampled_from(
    [
        "nan",
        "inf",
        "-inf",
        "1e308",
        "1e-308",
        "0",
        "0.0",
        "1,5",
        "-1.0",
        "1.5",
        "",
    ]
)

# Note: embedded NULs are filtered out -- POSIX execve() rejects them with
# E2BIG/EINVAL before memtier ever sees the argv, and that confounds the
# subprocess layer (Python raises ValueError) rather than exercising the
# parser. The parser-level fuzz is still very much in scope via everything
# else here.
weird_str = st.one_of(
    st.sampled_from(
        [
            "",
            " ",
            "a",
            "%n%n%s",
            "%s%s%s%s",
            "\x1b[31mANSI\x1b[0m",
            "$(reboot)",
            "`reboot`",
            "../" * 16 + "etc/passwd",
            "\U0001f525" * 256,
            "a" * 4096,
        ]
    ),
    st.text(
        alphabet=st.characters(blacklist_categories=("Cs",), blacklist_characters="\x00"),
        min_size=0,
        max_size=128,
    ),
)

ratio_like = st.sampled_from(
    [
        "1:0",
        "0:1",
        "1:1",
        "1:1:1",
        "-1:1",
        "1:-1",
        "1:",
        ":1",
        "a:b",
        "",
        "1:10",
    ]
)

# G:G (Gaussian) is now included: #430 parser rejects degenerate
# key-range / key-stddev combinations before the sampler runs, so the
# previous risk of an infinite spin no longer applies.
key_pattern_like = st.sampled_from(
    [
        "R:R",
        "S:S",
        "P:P",
        "Z:Z",
        "G:G",
        "X:X",
        "",
        "RR",
        "R:",
        ":R",
        "R:R:R",
    ]
)

range_like = st.sampled_from(
    [
        "1-10",
        "10-1",  # reversed
        "1-1000",  # large but well under Redis bulk-length cap
        "0-0",
        "-1-10",
        "1-",
        "-10",
        "",
        "a-b",
    ]
)

# memcache_text / memcache_binary are intentionally absent: against a Redis
# backend they enter a parse-fail / reconnect loop and never honor
# --test-time. Tracked as a follow-up bug; fuzzing them properly needs a
# memcached harness.
protocol_like = st.sampled_from(["redis", "resp2", "resp3", "", "RESP3", "garbage"])

retry_on_like = st.sampled_from(
    ["", "LOADING", "LOADING,BUSY", "GARBAGE", ",", "LOADING,", ",BUSY"]
)

monitor_pattern_like = st.sampled_from(["S", "R", "", "X", "SR"])
data_size_pattern_like = st.sampled_from(["R", "S", "", "X", "RS"])
breakdown_like = st.sampled_from(["command", "line", "", "lines", "COMMAND"])
miss_tracking_like = st.sampled_from(["auto", "off", "", "on", "AUTO"])
percentiles_like = st.sampled_from(
    ["50,99,99.9", "50.50.50", "50,", ",50", "", "100", "-1", "50,abc"]
)
# --data-size-list:
#   * "8:0"  -> now safe: #430 parser rejects zero-weight entries.
#   * "0:50" -> now safe: #428 parser rejects zero-size entries.
data_size_list_like = st.sampled_from(
    ["8:50,16:50", "", "8", "8:50,", ",8:50", "8:50:50", "8:0", "0:50"]
)

# --run-count is multiplied into wall-clock: N iterations of --test-time=1
# trivially exceed the 10s subprocess deadline once N>~6. Cap it at small
# values plus the parser-error inputs we actually care about.
run_count_like = st.sampled_from(
    ["1", "2", "0", "", " ", "1.5", "1,5", "0x1", "1e3", "1e9999"]
)

# Redis defaults to 16 DBs; selecting >=16 hangs memtier in a reconnect loop
# instead of exiting (tracked as a follow-up bug, see PR). Limit the
# strategy to in-range values plus parser-error inputs.
select_db_like = st.sampled_from(
    ["0", "1", "15", "", " ", "1.5", "1,5", "abc", "1e9999"]
)

# --command-ratio=0 hangs (the command is bound but its weight in the
# round-robin is zero, so the workload loop never makes progress and
# --test-time is not honored). Empty / whitespace-only strings are parsed
# as 0 and hit the same code path. Tracked as a follow-up; keep the
# strategy at numerically >=1 here so the test stays focused on the
# parser surface for non-zero shapes.
command_ratio_like = st.sampled_from(
    ["1", "2", "10", "1.5", "1,5", "0x1", "1e3", "1e9999"]
)

# --command takes a free-form Redis command string. We deliberately do NOT
# pipe the generic weird_str corpus here: that would let hypothesis stumble
# into things like SHUTDOWN / DEBUG SLEEP / FLUSHALL and either kill the
# fuzzer's Redis (false positive) or hang the test (true positive but
# orthogonal to the parser surface we're stressing). Instead we exercise
# the placeholder parser (__key__, __data__) with a mix of valid and
# benign edge-case inputs.
#
# Known bug classes intentionally excluded (filed as follow-ups, see PR):
#   * empty / whitespace-only command -> hangs (no command to run, reconnect
#     loop instead of parser-error exit).
#   * bare "__key__" / "__data__" placeholder with no leading command name
#     -> assertion failure (protocol.cpp:774, "first arg is not command
#     name?").
command_like = st.sampled_from(
    [
        "SET __key__ __data__",
        "GET __key__",
        "SET __key__ 5",
        "SET __key__ __data__ EX 100",
        "SET %s %n",  # format specifiers as arg values
        "PING",  # zero-key
        "MGET __key__ __key__ __key__",
    ]
)

# Flag-set schema. Each entry is (flag, value-strategy or None for boolean).
# Per issue #410, --tls*, --uri, --data-import, --failed-keys-file,
# --monitor-input are out of scope. File-output flags (--out-file,
# --json-out-file, --client-stats, --hdr-file-prefix, --cert, --key,
# --cacert) are also excluded because their failure mode is filesystem, not
# parser; future iterations can stub a tmpdir for them.
FLAG_SPECS = [
    # General / connection.
    #
    # --authenticate and --cluster-mode are now included: the
    # --connection-stage-timeout supervisor (#431) terminates the connect-
    # loop before the 10s subprocess deadline is hit, so both are safe
    # to fuzz against a standalone server.
    ("--authenticate", weird_str),
    ("--cluster-mode", None),
    ("--protocol", protocol_like),
    ("--run-count", run_count_like),
    ("--ipv4", None),
    ("--ipv6", None),
    # Results
    ("--show-config", None),
    ("--print-percentiles", percentiles_like),
    ("--print-all-runs", None),
    ("--realtime-latencies", None),
    ("--command-stats-breakdown", breakdown_like),
    ("--command-miss-tracking", miss_tracking_like),
    ("--miss-rate-threshold", weird_float),
    ("--statsd-host", weird_str),
    ("--statsd-port", weird_int),
    ("--statsd-prefix", weird_str),
    ("--statsd-run-label", weird_str),
    ("--graphite-port", weird_int),
    # Test
    ("--rate-limiting", weird_int),
    ("--clients-start", weird_int),
    ("--clients-step", weird_int),
    ("--step-duration", weird_int),
    ("--ratio", ratio_like),
    ("--pipeline", weird_int),
    ("--reconnect-interval", weird_int),
    ("--reconnect-on-error", None),
    ("--max-reconnect-attempts", weird_int),
    ("--reconnect-backoff-factor", weird_float),
    ("--retry-on-error", None),
    ("--max-retries", weird_int),
    ("--retry-backoff-ms", weird_int),
    ("--retry-backoff-factor", weird_float),
    ("--retry-on", retry_on_like),
    ("--max-retry-queue", weird_int),
    ("--connection-timeout", weird_int),
    ("--thread-conn-start-min-jitter-micros", weird_int),
    ("--thread-conn-start-max-jitter-micros", weird_int),
    ("--multi-key-get", weird_int),
    ("--select-db", select_db_like),
    ("--distinct-client-seed", None),
    ("--randomize", None),
    # Arbitrary command. --command uses a curated strategy rather than the
    # free weird_str corpus to avoid accidentally generating server-killing
    # tokens like SHUTDOWN; see command_like.
    ("--command", command_like),
    ("--command-ratio", command_ratio_like),
    ("--command-key-pattern", key_pattern_like),
    ("--monitor-pattern", monitor_pattern_like),
    ("--scan-incremental-iteration", None),
    ("--scan-incremental-max-iterations", weird_int),
    # Object
    ("--data-size", weird_int),
    ("--data-offset", weird_int),
    ("--data-size-range", range_like),
    ("--data-size-list", data_size_list_like),
    ("--data-size-pattern", data_size_pattern_like),
    ("--expiry-range", range_like),
    # Imported-data toggles that don't require a file
    ("--data-verify", None),
    ("--generate-keys", None),
    ("--no-expiry", None),
    # Key
    ("--key-prefix", weird_str),
    ("--key-minimum", weird_int),
    ("--key-maximum", weird_int),
    ("--key-pattern", key_pattern_like),
    ("--key-stddev", weird_float),
    ("--key-median", weird_float),
    ("--key-zipf-exp", weird_float),
    # WAIT-family flags: --wait-ratio with an unsatisfiable value is now
    # safe because #431 bounds the connect-loop; the parser rejects or the
    # supervisor terminates before the 10s subprocess deadline.
    ("--wait-ratio", ratio_like),
    ("--num-slaves", weird_int),
    ("--wait-timeout", weird_int),
]


@st.composite
def argv(draw):
    """Draw between 1 and 8 (flag, value) pairs from FLAG_SPECS without
    repetition, returning the flat argv list."""
    indices = draw(
        st.lists(
            st.integers(min_value=0, max_value=len(FLAG_SPECS) - 1),
            min_size=1,
            max_size=8,
            unique=True,
        )
    )
    out = []
    for idx in indices:
        flag, strategy = FLAG_SPECS[idx]
        out.append(flag)
        if strategy is not None:
            out.append(draw(strategy))
    return out


def _run_memtier(extra_args):
    """Invoke memtier with the canonical fast-exit harness flags plus
    `extra_args`. Returns CompletedProcess; raises TimeoutExpired on hang."""
    cmd = [
        MEMTIER,
        "--server=" + HOST,
        "--port=" + PORT,
        "--test-time=1",
        "--threads=1",
        "--clients=1",
        "--hide-histogram",
    ] + list(extra_args)
    return subprocess.run(
        cmd,
        capture_output=True,
        timeout=10,
        check=False,
    )


@pytest.mark.skipif(MEMTIER is None, reason="memtier_benchmark binary not found")
@settings(
    max_examples=int(os.environ.get("MEMTIER_FUZZ_MAX_EXAMPLES", "200")),
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
@given(extra=argv())
def test_cli_fuzz_does_not_crash(extra):
    """For every generated argv: clean exit code, no crash-class needle, no
    hang. Parser is free to reject input -- only crashes are forbidden."""
    try:
        proc = _run_memtier(extra)
    except subprocess.TimeoutExpired as exc:
        pytest.fail(
            "memtier_benchmark hung (>10s) on argv={!r}; stdout={!r}; stderr={!r}".format(
                extra,
                (exc.stdout or b"")[:512],
                (exc.stderr or b"")[:512],
            )
        )

    stderr = proc.stderr.decode("utf-8", errors="replace")
    stdout = proc.stdout.decode("utf-8", errors="replace")

    assert proc.returncode in OK_RETURN_CODES, (
        "unexpected returncode {} on argv={!r}\nstdout={}\nstderr={}".format(
            proc.returncode, extra, stdout[-2048:], stderr[-2048:]
        )
    )
    for needle in BANNED_NEEDLES:
        assert needle not in stderr, (
            "stderr contains crash-class needle {!r} on argv={!r}\nstderr={}".format(
                needle, extra, stderr[-2048:]
            )
        )
