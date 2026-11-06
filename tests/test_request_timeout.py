"""--request-timeout: a connection the server never serves is treated as failed.

A listener that never calls accept() completes the TCP handshake in the kernel
backlog, so memtier sees a connected socket that never answers. Without
--request-timeout nothing errors, and --reconnect-on-error never fires.
"""

import socket
import subprocess
import threading
import time

from include import MEMTIER_BINARY


def _listener():
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(1024)
    return sock, sock.getsockname()[1]


def _args(port, *extra):
    return [MEMTIER_BINARY, "--server=127.0.0.1", "--port={}".format(port), "--threads=1", "--clients=1",
            "--ratio=1:0", "--data-size=8", "--hide-histogram"] + list(extra)


def _run(args, timeout=60):
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


def test_unserved_connection_times_out_and_reconnects(env):
    sock, port = _listener()
    try:
        start = time.monotonic()
        result = _run(_args(port, "--test-time=5", "--request-timeout=1", "--reconnect-on-error"))
        elapsed = time.monotonic() - start
    finally:
        sock.close()
    env.assertContains("Request timeout: no reply after 1 seconds (1 requests outstanding)", result.stderr,
                       message=result.stderr)
    env.assertContains("Request timeout, attempting reconnection", result.stderr, message=result.stderr)
    # The timeout fires more than once: the reconnect lands on the same dead listener.
    env.assertGreater(result.stderr.count("Request timeout: no reply"), 1, message=result.stderr)
    env.assertLess(elapsed, 40)


def test_unserved_connection_without_reconnect_fails_run(env):
    sock, port = _listener()
    try:
        result = _run(_args(port, "--test-time=30", "--request-timeout=1"))
    finally:
        sock.close()
    env.assertContains("Request timeout: no reply after 1 seconds", result.stderr, message=result.stderr)
    env.assertContains("Request timeout and --reconnect-on-error is not set", result.stderr, message=result.stderr)
    env.assertNotEqual(result.returncode, 0, message=result.stderr)


class _ReplyingServer(object):
    """Replies +OK to each SET after `delay` seconds."""

    def __init__(self, delay):
        self.delay = delay
        self.sock, self.port = _listener()
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()

    def _accept(self):
        self.sock.settimeout(0.2)
        while not self.stop.is_set():
            try:
                conn, _ = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn):
        conn.settimeout(0.2)
        pending = b""
        while not self.stop.is_set():
            try:
                data = conn.recv(65536)
            except socket.timeout:
                continue
            except OSError:
                break
            if not data:
                break
            pending += data
            count = pending.count(b"SET")
            if count:
                pending = b""
                time.sleep(self.delay)
                try:
                    conn.sendall(b"+OK\r\n" * count)
                except OSError:
                    break
        conn.close()

    def close(self):
        self.stop.set()
        self.sock.close()
        self.thread.join(timeout=5)


def test_slow_but_answering_server_does_not_time_out(env):
    # Every reply takes 0.6 s against a 1 s timeout, so the watchdog fires with
    # a younger request at the head of the pipeline and must re-arm, not trip.
    server = _ReplyingServer(0.6)
    try:
        result = _run(_args(server.port, "--test-time=4", "--request-timeout=1"))
    finally:
        server.close()
    env.assertNotContains("Request timeout", result.stderr, message=result.stderr)
    env.assertEqual(result.returncode, 0, message=result.stderr)


def test_idle_connection_does_not_time_out(env):
    # One request a second, answered at once: the connection is often idle when
    # the watchdog fires, which must not count as a timeout.
    server = _ReplyingServer(0)
    try:
        result = _run(_args(server.port, "--test-time=4", "--request-timeout=1", "--rate-limiting=1"))
    finally:
        server.close()
    env.assertNotContains("Request timeout", result.stderr, message=result.stderr)
    env.assertEqual(result.returncode, 0, message=result.stderr)


def test_request_timeout_in_json_config(env):
    server = _ReplyingServer(0)
    try:
        import json
        import os
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out.json")
            result = _run(_args(server.port, "--test-time=2", "--request-timeout=7", "--json-out-file=" + out))
            env.assertEqual(result.returncode, 0, message=result.stderr)
            with open(out) as f:
                config = json.load(f)["configuration"]
    finally:
        server.close()
    env.assertEqual(config["request_timeout"], 7)
