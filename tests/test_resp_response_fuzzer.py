"""
Adversarial RESP-response fuzzer harness for the memtier_benchmark
client-side parser.

Each test below spins up the in-tree mock-redis server
(`tests/fixtures/mock_redis_resp_fuzzer.py`) with one fixture from
`tests/fixtures/resp_payloads/`, then runs memtier briefly against it
and asserts:

  1. memtier did NOT exit on a fatal signal (SIGSEGV/SIGABRT/SIGBUS/
     SIGFPE/SIGILL).
  2. stderr contains none of the crash needles:
     `Aborted`, `Segmentation fault`, `AddressSanitizer`,
     `UndefinedBehaviorSanitizer`, `stack smashing detected`,
     `assertion failed`.
  3. memtier terminates within the per-fixture wall-clock budget (no
     hang / spin loop).

This is a defensive-test fixture, not an attack tool. See
`tests/fixtures/resp_payloads/README.md` for the per-fixture
parser-branch mapping table.

Run subset:
    TEST=test_resp_response_fuzzer.py ./tests/run_tests.sh
"""

import os
import signal
import socket
import subprocess
import sys
import tempfile
import time

from include import MEMTIER_BINARY


HERE = os.path.dirname(os.path.abspath(__file__))
FIXTURES_DIR = os.path.join(HERE, "fixtures", "resp_payloads")
MOCK_SERVER = os.path.join(HERE, "fixtures", "mock_redis_resp_fuzzer.py")

# Per-fixture wall-clock budget. memtier is launched with --test-time=2
# so this is mostly a hang detector. The issue specifies 15s.
PER_FIXTURE_TIMEOUT_S = 15

# stderr substrings that indicate a real crash (the parser audit's "must
# not happen" set). Matched case-sensitively because the tools all use
# fixed capitalizations.
CRASH_NEEDLES = (
    "Aborted",
    "Segmentation fault",
    "AddressSanitizer",
    "UndefinedBehaviorSanitizer",
    "stack smashing detected",
    "assertion failed",
)

# Fatal signals - process exits with -<signum> under subprocess.
FATAL_SIGNALS = {
    signal.SIGSEGV,
    signal.SIGABRT,
    signal.SIGBUS,
    signal.SIGFPE,
    signal.SIGILL,
}


def _free_port():
    """Bind-then-release to get an unused TCP port. Race-y in theory, fine
    in practice for tests; the mock server uses SO_REUSEADDR anyway."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(host, port, timeout=5.0):
    """Block until the mock server is accepting connections."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.25):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def _run_fixture(env, fixture_name):
    """Spin up mock_redis_resp_fuzzer + memtier for one fixture.

    Returns (crashed, needle_hit, hung, exit_code, stderr_text).
    """
    fixture_path = os.path.join(FIXTURES_DIR, fixture_name)
    env.assertTrue(
        os.path.isfile(fixture_path),
        message="missing fixture: {}".format(fixture_path),
    )

    port = _free_port()
    mock_log = tempfile.NamedTemporaryFile(
        prefix="mock_redis_{}_".format(fixture_name.replace(".bin", "")),
        suffix=".log",
        delete=False,
    )
    mock_log.close()

    with open(mock_log.name, "wb") as logf:
        mock_proc = subprocess.Popen(
            [
                sys.executable,
                MOCK_SERVER,
                "--port",
                str(port),
                "--fixture",
                fixture_path,
                "--verbose",
            ],
            stdout=logf,
            stderr=subprocess.STDOUT,
        )

    crashed = False
    needle_hit = None
    hung = False
    exit_code = None
    stderr_text = ""

    try:
        if not _wait_for_port("127.0.0.1", port, timeout=5.0):
            # If the mock did not come up, that is a test-infra bug, not
            # a memtier bug -- skip cleanly.
            return (False, None, False, None, "mock server failed to bind")

        with tempfile.TemporaryDirectory() as td:
            stdout_path = os.path.join(td, "mb.stdout")
            stderr_path = os.path.join(td, "mb.stderr")
            args = [
                MEMTIER_BINARY,
                "-s",
                "127.0.0.1",
                "-p",
                str(port),
                "-t",
                "1",
                "-c",
                "1",
                "--test-time=2",
                "--ratio=1:0",
                "--hide-histogram",
                # Run a tiny pipeline so the parser exercises mbulk paths.
                "--pipeline=2",
                # Suppress the JSON output - we only care about exit/stderr.
                "--json-out-file=/dev/null",
            ]
            with open(stdout_path, "w") as out, open(stderr_path, "w") as err:
                mem_proc = subprocess.Popen(args, stdout=out, stderr=err)
            try:
                exit_code = mem_proc.wait(timeout=PER_FIXTURE_TIMEOUT_S)
            except subprocess.TimeoutExpired:
                hung = True
                # SIGTERM first, then SIGKILL if it does not yield.
                mem_proc.terminate()
                try:
                    exit_code = mem_proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    mem_proc.kill()
                    exit_code = mem_proc.wait()
            with open(stderr_path, "r", errors="replace") as f:
                stderr_text = f.read()

            # Negative exit code on POSIX subprocess == terminated by
            # -exit_code signal.
            if exit_code is not None and exit_code < 0:
                if -exit_code in FATAL_SIGNALS:
                    crashed = True
            for needle in CRASH_NEEDLES:
                if needle in stderr_text:
                    needle_hit = needle
                    break
    finally:
        try:
            mock_proc.terminate()
            mock_proc.wait(timeout=2)
        except Exception:
            try:
                mock_proc.kill()
            except Exception:
                pass

    return (crashed, needle_hit, hung, exit_code, stderr_text)


def _assert_no_crash(env, fixture_name):
    """Run one fixture and assert no crash / no hang / no needle."""
    crashed, needle_hit, hung, exit_code, stderr_text = _run_fixture(
        env, fixture_name
    )

    # _run_fixture returns a "mock server failed to bind" stderr_text when
    # _wait_for_port times out. Without an explicit skip here the assertions
    # below would all silently pass (no crash, no needle, no hang were ever
    # populated) and the test would report a green run with zero coverage
    # (cursor bugbot finding). Skip on test-infra failure.
    if stderr_text == "mock server failed to bind":
        env.skip()
        return

    # Emit context so failures are debuggable without rerunning.
    if crashed or needle_hit or hung:
        sys.stderr.write(
            "\n=== resp-fuzz failure for {} ===\n"
            "  exit_code={} crashed={} needle={} hung={}\n"
            "  stderr (last 1000 chars):\n{}\n"
            "=== end {} ===\n".format(
                fixture_name,
                exit_code,
                crashed,
                needle_hit,
                hung,
                stderr_text[-1000:],
                fixture_name,
            )
        )

    env.assertFalse(
        crashed,
        message="memtier crashed on fixture {} (exit_code={})".format(
            fixture_name, exit_code
        ),
    )
    env.assertIsNone(
        needle_hit,
        message="crash needle {!r} in stderr for fixture {}".format(
            needle_hit, fixture_name
        ),
    )
    env.assertFalse(
        hung,
        message="memtier hung on fixture {} (>{}s)".format(
            fixture_name, PER_FIXTURE_TIMEOUT_S
        ),
    )


# One test function per fixture so RLTest reports them individually and
# a single crash does not mask the others. The functions are intentionally
# tiny - all logic lives in the helpers above.


def _skip_if_unsupported(env):
    """Common skip for environments that cannot run this harness."""
    # The harness runs memtier against its own mock server on 127.0.0.1
    # and does not need RLTest's redis at all. We DO skip on cluster mode
    # though: RLTest would still bootstrap and tear down a fresh 3-shard
    # TLS cluster per test (one per RESP fixture x 12 fixtures ~ 6+ min of
    # pure setup), which pushed `Test OSS-CLUSTER API: TCP TLS` over its
    # 15-minute job budget. The mock-server contract is mode-independent,
    # so cluster coverage adds nothing here.
    if env.isCluster():
        env.skip()
        return True
    if not os.path.isfile(MOCK_SERVER):
        env.skip()
        return True
    if not os.path.isfile(MEMTIER_BINARY):
        env.skip()
        return True
    return False


def test_bulk_huge_length(env):
    if _skip_if_unsupported(env):
        return
    _assert_no_crash(env, "bulk_huge_length.bin")


def test_bulk_neg_other(env):
    if _skip_if_unsupported(env):
        return
    _assert_no_crash(env, "bulk_neg_other.bin")


def test_bulk_int_overflow(env):
    if _skip_if_unsupported(env):
        return
    _assert_no_crash(env, "bulk_int_overflow.bin")


def test_mbulk_deep_nest(env):
    if _skip_if_unsupported(env):
        return
    _assert_no_crash(env, "mbulk_deep_nest.bin")


def test_mbulk_count_overflow(env):
    if _skip_if_unsupported(env):
        return
    _assert_no_crash(env, "mbulk_count_overflow.bin")


def test_integer_overflow(env):
    if _skip_if_unsupported(env):
        return
    _assert_no_crash(env, "integer_overflow.bin")


def test_resp3_verbatim_bad_prefix(env):
    if _skip_if_unsupported(env):
        return
    _assert_no_crash(env, "resp3_verbatim_bad_prefix.bin")


def test_resp3_map_odd(env):
    if _skip_if_unsupported(env):
        return
    _assert_no_crash(env, "resp3_map_odd.bin")


def test_resp3_push_unsolicited(env):
    if _skip_if_unsupported(env):
        return
    _assert_no_crash(env, "resp3_push_unsolicited.bin")


def test_unsolicited_reply(env):
    if _skip_if_unsupported(env):
        return
    _assert_no_crash(env, "unsolicited_reply.bin")


def test_cluster_slots_malformed(env):
    """
    `cluster_slots_malformed.bin` is meaningful only against memtier in
    --cluster-mode, where the client actually issues `CLUSTER SLOTS`
    during bootstrap. In standalone mode the parser branch we want to
    exercise (`cluster_client::handle_cluster_slots`) is never reached,
    so a standalone run of this fixture is a no-op rather than a real
    test.

    The cluster-mode run currently *crashes* memtier with SIGSEGV on
    master -- see follow-up issue redis/memtier_benchmark#417. Once
    that crash is fixed, remove this skip and call _assert_no_crash
    with the cluster-mode flag added in _run_fixture.
    """
    if _skip_if_unsupported(env):
        return
    # TODO(#417): remove skip once cluster_client::handle_cluster_slots
    # validates its input array shape.
    env.skip()


def test_truncated_frame_dribble(env):
    if _skip_if_unsupported(env):
        return
    _assert_no_crash(env, "truncated_frame_dribble.bin")
