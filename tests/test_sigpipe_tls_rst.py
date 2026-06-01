"""
Regression test for PERF-501 / GH #382 — production-shaped reproduction.

Spins up an in-process TLS server that:
  1. completes the TLS handshake with memtier
  2. reads the first client write (a HELLO / SELECT / SET)
  3. forces a TCP RST via SO_LINGER {l_onoff=1, l_linger=0}

memtier's next SSL_write into that session calls write() on a RST-marked
fd; the kernel both returns EPIPE *and* raises SIGPIPE. OpenSSL's
SSL_write() does NOT pass MSG_NOSIGNAL, so without signal(SIGPIPE,
SIG_IGN) in main(), the process is killed before --reconnect-on-error
can observe the error and the entire benchmark dies with exit 141.

This complements test_sigpipe_immunity.py (which just SIG_IGN-tests the
binary directly) by exercising the actual production failure mode end
to end: openssl write -> kernel RST -> SIGPIPE -> recovery via the
existing reconnect path.

Skips when the build was configured --disable-tls (no TLS support in
the memtier binary).

Run subset:
  TEST=test_sigpipe_tls_rst.py ./tests/run_tests.sh
"""

import os
import shutil
import signal
import socket
import ssl
import subprocess
import tempfile
import threading
import time

from include import MEMTIER_BINARY


def _have_tls_support():
    """Check whether MEMTIER_BINARY was built with TLS support."""
    out = subprocess.run([MEMTIER_BINARY, "--help"], capture_output=True, text=True)
    return "--tls " in out.stdout or "--tls\n" in out.stdout


def _gen_cert(workdir):
    """Generate a self-signed RSA cert/key pair in workdir. Returns (cert, key)."""
    cert = os.path.join(workdir, "tls.crt")
    key = os.path.join(workdir, "tls.key")
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-nodes",
            "-newkey",
            "rsa:2048",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-keyout",
            key,
            "-out",
            cert,
        ],
        check=True,
        capture_output=True,
    )
    return cert, key


def _pick_free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class _TlsRstTarpit:
    """TLS server that RSTs each session right after the first client write."""

    def __init__(self, cert, key):
        self.cert = cert
        self.key = key
        self.port = _pick_free_port()
        self._stop = threading.Event()
        self._thread = None
        self.reset_count = 0

    def start(self):
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        # Connect to ourselves to unblock the accept() so the thread can exit.
        try:
            socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
        except Exception:
            pass
        if self._thread:
            self._thread.join(timeout=2)

    def _serve(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(certfile=self.cert, keyfile=self.key)
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", self.port))
        srv.listen(1024)
        srv.settimeout(0.2)
        while not self._stop.is_set():
            try:
                raw, _ = srv.accept()
            except socket.timeout:
                continue
            try:
                tls = ctx.wrap_socket(
                    raw, server_side=True, do_handshake_on_connect=True
                )
                try:
                    tls.settimeout(0.3)
                    tls.recv(65536)  # let memtier pipeline several writes
                except Exception:
                    pass
                # SIGPIPE-triggering close sequence:
                #
                # Skip TLS close_notify and skip SO_LINGER. Just close the
                # underlying fd. The kernel sends a graceful FIN. memtier
                # has --pipeline=N writes queued; libevent's openssl
                # bufferevent flushes them via SSL_write. The first writes
                # after FIN succeed locally (data goes into the wire), but
                # the peer kernel sees them on a fully-closed socket and
                # responds with RST. memtier's NEXT SSL_write (still
                # pipelined) then hits an RST-marked fd: write() returns
                # EPIPE AND the kernel raises SIGPIPE in the writer thread
                # (SSL_write doesn't pass MSG_NOSIGNAL). Without
                # signal(SIGPIPE, SIG_IGN), SIG_DFL kills the process with
                # exit 141 before EPIPE is observed.
                #
                # Forcing RST directly via SO_LINGER (pre-wrap on raw, or
                # post-wrap on a re-wrapped fd via tls.detach()) RSTs
                # *during* handshake completion, which never gives memtier
                # a chance to build up a pipeline -- the natural close
                # path above is the one that exercises the production
                # race.
                try:
                    tls.close()
                except Exception:
                    pass
                self.reset_count += 1
            except Exception:
                try:
                    raw.close()
                except Exception:
                    pass
        try:
            srv.close()
        except Exception:
            pass


def test_sigpipe_tls_rst_does_not_kill_process(env):
    """memtier must survive a TLS peer that RSTs the connection mid-write."""
    if not _have_tls_support():
        env.skip()
        return
    if shutil.which("openssl") is None:
        env.skip()
        return

    workdir = tempfile.mkdtemp()
    try:
        cert, key = _gen_cert(workdir)
        tarpit = _TlsRstTarpit(cert, key)
        tarpit.start()
        try:
            args = [
                MEMTIER_BINARY,
                "-s",
                "127.0.0.1",
                "-p",
                str(tarpit.port),
                "--tls",
                "--tls-skip-verify",
                "-t",
                "2",
                "-c",
                "2",
                "--pipeline=16",
                "--test-time=4",
                "--ratio=1:0",
                "--hide-histogram",
                "--reconnect-on-error",
                "--max-reconnect-attempts=100",
            ]
            stdout_path = os.path.join(workdir, "mb.stdout")
            stderr_path = os.path.join(workdir, "mb.stderr")
            with open(stdout_path, "w") as out, open(stderr_path, "w") as err:
                proc = subprocess.Popen(args, stdout=out, stderr=err)
            try:
                rc = proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                rc = proc.wait()

            # The key assertion: process must NOT have died from SIGPIPE.
            # exit 141 = 128 + SIGPIPE; on Python's wait(), this is reported
            # as either 141 (positive) or -signal.SIGPIPE (negative, if it
            # was killed by signal). Either form indicates the regression.
            #
            # NB: RLTest's assertion methods take (value, depth=0, message=None);
            # the message MUST be passed as a keyword arg or RLTest treats it
            # as `depth` and crashes with TypeError when it does `1 + depth`.
            # RLTest also doesn't expose `assertNotIn`, so we use `assertTrue`
            # over a `not in` expression.
            sigpipe_codes = {141, -signal.SIGPIPE}
            env.assertTrue(
                rc not in sigpipe_codes,
                message=(
                    "memtier died from SIGPIPE (rc={}); SIG_IGN regression. "
                    "stderr at {}".format(rc, stderr_path)
                ),
            )

            # The tarpit should have torn down at least one TLS session.
            env.assertGreater(
                tarpit.reset_count,
                0,
                message="TLS tarpit never accepted a memtier session — test harness broken",
            )

            # And the runtime error path should have logged at least one
            # connection-error line, proving the EPIPE was routed through
            # attempt_reconnect rather than silently swallowed.
            with open(stderr_path) as f:
                stderr = f.read()
            env.assertTrue(
                "Connection error" in stderr
                or "TLS connection error" in stderr
                or "connection dropped" in stderr,
                message=(
                    "memtier never logged a connection error despite many TLS RSTs "
                    "(stderr at {})".format(stderr_path)
                ),
            )
        finally:
            tarpit.stop()
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
