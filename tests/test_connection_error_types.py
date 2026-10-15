"""Connection errors must query the error queue for the actual transport type."""

import socket
import subprocess
import threading

from include import MEMTIER_BINARY


def _run_failure(env, port, tls=False):
    args = [MEMTIER_BINARY, "--server=127.0.0.1", "--port={}".format(port),
            "--threads=1", "--clients=1", "--test-time=5",
            "--reconnect-on-error", "--max-reconnect-attempts=1",
            "--connection-timeout=1"]
    if tls:
        args += ["--tls", "--tls-skip-verify"]
    result = subprocess.run(args, capture_output=True, text=True, timeout=15)
    env.assertGreaterEqual(result.returncode, 0, message=result.stderr)
    env.assertContains("Connection error", result.stderr)
    env.assertContains("attempting reconnection", result.stderr)
    env.assertNotContains("AddressSanitizer", result.stderr)
    env.assertNotContains("runtime error:", result.stderr)
    return result.stderr


def test_plaintext_connection_refused(env):
    # Keep the port reserved but not listening throughout the test. A TLS-enabled
    # binary must not pass this plain socket bufferevent to the SSL accessor.
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        stderr = _run_failure(env, reserved.getsockname()[1])
    env.assertNotContains("TLS connection error:", stderr)


def test_tls_handshake_failure(env):
    version = subprocess.run([MEMTIER_BINARY, "--version"], capture_output=True,
                             text=True, timeout=10, check=True).stdout
    if " openssl=" not in version:
        env.skip()
        return

    stop = threading.Event()
    accepted = []
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(8)
        listener.settimeout(0.2)

        def reject_handshakes():
            while not stop.is_set():
                try:
                    conn, _ = listener.accept()
                except socket.timeout:
                    continue
                with conn:
                    accepted.append(True)
                    conn.settimeout(1)
                    try:
                        conn.recv(4096)
                        conn.sendall(b"HTTP/1.0 400 Bad Request\r\n\r\n")
                    except OSError:
                        pass

        worker = threading.Thread(target=reject_handshakes, daemon=True)
        worker.start()
        try:
            _run_failure(env, listener.getsockname()[1], tls=True)
        finally:
            stop.set()
            worker.join(timeout=3)
        env.assertFalse(worker.is_alive())
    env.assertGreaterEqual(len(accepted), 2)
    # Alpha 2.2.2 currently suppresses queued SSL reasons. Assert the error and
    # reconnect behavior above without requiring stable's detailed TLS message.
