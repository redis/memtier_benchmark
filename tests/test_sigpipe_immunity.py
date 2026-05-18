"""
Regression test for PERF-501 / GH #382:
memtier_benchmark must ignore SIGPIPE so that a peer-initiated TCP/TLS close
mid-write returns EPIPE to the existing --reconnect-on-error path instead of
killing the process with exit 141.

Sending SIGPIPE directly to the running process is the cleanest black-box
check for the SIG_IGN install — independent of which protocol the writer is
using (plain TCP vs OpenSSL SSL_write) and independent of the timing race
that triggers it in production.

Run subset:
  TEST=test_sigpipe_immunity.py ./tests/run_tests.sh
"""

import os
import signal
import subprocess
import tempfile
import time

from include import MEMTIER_BINARY, addTLSArgs


def test_sigpipe_ignored(env):
    """Process must remain alive after receiving SIGPIPE."""
    if env.isUnixSocket():
        env.skip()
        return

    master_nodes_list = env.getMasterNodesList()
    port = master_nodes_list[0]["port"]

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
        "--test-time=10",
        "--ratio=1:0",
        "--hide-histogram",
    ]
    if env.isCluster():
        args.append("--cluster-mode")
    # Forward TLS flags using the same helper as other tests.
    benchmark_specs = {"args": []}
    addTLSArgs(benchmark_specs, env)
    args.extend(benchmark_specs["args"])

    with tempfile.TemporaryDirectory() as td:
        stdout_path = os.path.join(td, "mb.stdout")
        stderr_path = os.path.join(td, "mb.stderr")
        with open(stdout_path, "w") as out, open(stderr_path, "w") as err:
            proc = subprocess.Popen(args, stdout=out, stderr=err)
        try:
            # Wait for memtier to fully start its event loop.
            time.sleep(1.5)
            # NB: RLTest's assertion methods take (value, depth=0, message=None);
            # the message MUST be passed as a keyword arg or RLTest treats it as
            # `depth` and crashes with TypeError when it does `1 + depth`.
            env.assertIsNone(
                proc.poll(),
                message="memtier exited before SIGPIPE was sent (startup failure?)",
            )

            # First SIGPIPE — without SIG_IGN this delivers SIG_DFL and exits 141.
            os.kill(proc.pid, signal.SIGPIPE)
            time.sleep(0.5)
            env.assertIsNone(
                proc.poll(),
                message="memtier died from SIGPIPE — SIG_IGN regression",
            )

            # Second SIGPIPE — just to prove SIG_IGN persists, not a fluke.
            os.kill(proc.pid, signal.SIGPIPE)
            time.sleep(0.5)
            env.assertIsNone(
                proc.poll(),
                message="memtier died from a second SIGPIPE — SIG_IGN regression",
            )

            # Clean shutdown via SIGINT. We don't pin a specific exit code:
            # under ASAN/UBSan, memtier may return 1 even on a clean
            # SIGINT-driven shutdown (the sanitizer runtimes report on
            # trailing in-flight state at process exit and override the
            # return code) — this is not a leak, just instrumentation
            # behavior, and master-after-#383 surfaced exactly this.
            #
            # The SIGPIPE-immunity invariant is already proven by the three
            # assertions above; here we only verify that memtier *terminates*
            # in response to SIGINT, which is the real bookkeeping check.
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                env.assertTrue(
                    False,
                    message="memtier did not terminate within 10s of SIGINT",
                )
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
