"""A worker that already ended is not a --connection-stage-timeout abort.

The main thread sleeps one second, then asks the connection-stage supervisor
whether to abort, and only then counts the workers still running. A worker
that failed on its own during that sleep, e.g. because the server closed the
connection before any reply, was therefore also reported as a supervisor
abort when the timeout had expired by the next poll: memtier printed the
worker failure and "aborting after N seconds of connection-stage failures",
and exited 2 instead of 1.

A scripted peer reads the first command and closes the connection without
replying, well before the first poll. With --connection-stage-timeout=1 that
poll is the one where the timeout has expired, so the order of the two checks
decides the result deterministically.

The supervisor is skipped only when no worker is running: with two workers,
one whose connection was closed and one stuck in setup, it must still abort
the run at the timeout instead of waiting for the stuck worker forever.
"""
import socketserver
import subprocess
import threading
import time

from include import MEMTIER_BINARY

# Close this long after the first command arrives: well before the first
# supervisor poll, which comes after a one-second sleep.
_CLOSE_AFTER_SECS = 0.2


class _Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, close_count):
        socketserver.ThreadingTCPServer.__init__(self, ("127.0.0.1", 0), _Handler)
        self.lock = threading.Lock()
        self.close_count = close_count
        self.connections = 0


class _Handler(socketserver.BaseRequestHandler):
    def handle(self):
        with self.server.lock:
            self.server.connections += 1
            close = self.server.connections <= self.server.close_count
        try:
            if self.request.recv(4096):
                if close:
                    time.sleep(_CLOSE_AFTER_SECS)
                    return
                # Never reply: this connection stays in setup.
                while self.request.recv(4096):
                    pass
        except OSError:
            pass


def _run_memtier(threads, close_count, timeout):
    """Run memtier against a peer that closes the first close_count connections
    after their first command and never answers the others. Returns
    (returncode, stderr, connections, elapsed); returncode is None when memtier
    was killed at timeout."""
    server = _Server(close_count)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    args = [MEMTIER_BINARY, "--server=127.0.0.1", "--port={}".format(server.server_address[1]),
            "--threads={}".format(threads), "--clients=1", "--requests=100", "--ratio=1:0", "--hide-histogram",
            "--connection-stage-timeout=1"]
    start = time.monotonic()
    try:
        result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                                timeout=timeout, text=True)
        returncode, stderr = result.returncode, result.stderr
    except subprocess.TimeoutExpired as e:
        returncode = None
        stderr = e.stderr.decode(errors="replace") if isinstance(e.stderr, bytes) else (e.stderr or "")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
    return returncode, stderr, server.connections, time.monotonic() - start


def test_worker_failed_before_poll_is_not_supervisor_abort(env):
    if env.isCluster() or env.useTLS or env.isUnixSocket():
        # Only the scripted peer is used; run once, in the plaintext standalone cell.
        env.skip()
    returncode, stderr, connections, _ = _run_memtier(threads=1, close_count=1, timeout=60)
    tail = stderr[-2000:]
    env.assertEqual(connections, 1, message=tail)
    env.assertTrue("client(s) stopped before reaching their stop condition" in stderr, message=tail)
    env.assertFalse("See --connection-stage-timeout." in stderr, message=tail)
    env.assertEqual(returncode, 1, message=tail)


def test_worker_still_in_setup_is_supervisor_abort(env):
    if env.isCluster() or env.useTLS or env.isUnixSocket():
        env.skip()
    # Three workers fail before the first poll and the fourth never gets a
    # reply. The supervisor must still bound the stuck one: exit 2 at the
    # timeout, not a hang until the subprocess timeout.
    returncode, stderr, connections, elapsed = _run_memtier(threads=4, close_count=3, timeout=30)
    tail = stderr[-2000:]
    env.assertEqual(connections, 4, message=tail)
    env.assertIsNotNone(returncode, message="memtier still running after {:.0f} s".format(elapsed))
    env.assertEqual(returncode, 2, message=tail)
    env.assertTrue("See --connection-stage-timeout." in stderr, message=tail)
    env.assertLess(elapsed, 15, message=tail)
