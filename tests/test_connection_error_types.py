"""Connection errors must query the error queue for the actual transport type."""

import os
import socket
import ssl
import subprocess
import threading

from include import MEMTIER_BINARY, TLS_CERT, TLS_KEY


def _tls_enabled():
    version = subprocess.run([MEMTIER_BINARY, "--version"], capture_output=True,
                             text=True, timeout=10, check=True).stdout
    return " openssl=" in version


def _run_failure(env, port, tls_args=()):
    args = [MEMTIER_BINARY, "--server=127.0.0.1", "--port={}".format(port),
            "--threads=1", "--clients=1", "--test-time=5",
            "--reconnect-on-error", "--max-reconnect-attempts=1",
            "--connection-timeout=1"] + list(tls_args)
    result = subprocess.run(args, capture_output=True, text=True, timeout=15)
    # A negative return code means memtier died from a signal (e.g. a
    # sanitizer abort); a normal failed run exits with a non-negative code.
    env.assertGreaterEqual(result.returncode, 0, message=result.stderr)
    env.assertContains("Connection error", result.stderr, message=result.stderr)
    env.assertContains("attempting reconnection", result.stderr, message=result.stderr)
    env.assertNotContains("AddressSanitizer", result.stderr, message=result.stderr)
    env.assertNotContains("runtime error:", result.stderr, message=result.stderr)
    return result.stderr


def _serve(listener, handle):
    """Accept connections on listener and pass each to handle until stopped."""
    stop = threading.Event()
    accepted = []

    def loop():
        while not stop.is_set():
            try:
                conn, _ = listener.accept()
            except socket.timeout:
                continue
            with conn:
                accepted.append(True)
                conn.settimeout(1)
                try:
                    handle(conn)
                except (OSError, ssl.SSLError):
                    pass

    worker = threading.Thread(target=loop, daemon=True)
    worker.start()
    return stop, worker, accepted


def _listener():
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    listener.settimeout(0.2)
    return listener


def test_plaintext_connection_refused(env):
    # Keep the port reserved but not listening throughout the test. A TLS-enabled
    # binary must not pass this plain socket bufferevent to the SSL accessor.
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        stderr = _run_failure(env, reserved.getsockname()[1])
    env.assertNotContains("TLS connection error:", stderr, message=stderr)


def test_tls_handshake_failure(env):
    if not _tls_enabled():
        env.skip()
        return

    def reject_handshake(conn):
        conn.recv(4096)
        conn.sendall(b"HTTP/1.0 400 Bad Request\r\n\r\n")

    with _listener() as listener:
        stop, worker, accepted = _serve(listener, reject_handshake)
        try:
            stderr = _run_failure(env, listener.getsockname()[1],
                                  ["--tls", "--tls-skip-verify"])
        finally:
            stop.set()
            worker.join(timeout=3)
        env.assertFalse(worker.is_alive())
    env.assertGreaterEqual(len(accepted), 2)
    # libevent 2.1 reports the OpenSSL reason; 2.2.2 returns none, which memtier
    # must still label as a TLS failure rather than staying silent.
    env.assertContains("TLS connection error:", stderr, message=stderr)


def test_tls_certificate_verify_failure(env):
    # The suite's server certificate is signed by the test CA. Without --cacert
    # (and without --tls-skip-verify) memtier must reject it and say why, on
    # every libevent version: 2.2.2 no longer returns the queued OpenSSL reason.
    if not _tls_enabled() or not TLS_CERT or not TLS_KEY or not os.path.exists(TLS_CERT):
        env.skip()
        return

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(TLS_CERT, TLS_KEY)

    def handshake(conn):
        with context.wrap_socket(conn, server_side=True) as tls:
            tls.recv(4096)

    with _listener() as listener:
        stop, worker, accepted = _serve(listener, handshake)
        try:
            stderr = _run_failure(env, listener.getsockname()[1], ["--tls"])
        finally:
            stop.set()
            worker.join(timeout=3)
        env.assertFalse(worker.is_alive())
    env.assertGreaterEqual(len(accepted), 2)
    env.assertContains("certificate verify failed", stderr, message=stderr)
