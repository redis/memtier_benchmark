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

    def __init__(self):
        socketserver.ThreadingTCPServer.__init__(self, ("127.0.0.1", 0), _Handler)
        self.connections = 0


class _Handler(socketserver.BaseRequestHandler):
    def handle(self):
        self.server.connections += 1
        try:
            if self.request.recv(4096):
                time.sleep(_CLOSE_AFTER_SECS)
        except OSError:
            pass


def test_worker_failed_before_poll_is_not_supervisor_abort(env):
    if env.isCluster() or env.useTLS or env.isUnixSocket():
        # Only the scripted peer is used; run once, in the plaintext standalone cell.
        env.skip()
    server = _Server()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    args = [MEMTIER_BINARY, "--server=127.0.0.1", "--port={}".format(server.server_address[1]),
            "--threads=1", "--clients=1", "--requests=100", "--ratio=1:0", "--hide-histogram",
            "--connection-stage-timeout=1"]
    try:
        result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                                timeout=60, text=True)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
    stderr = result.stderr[-2000:]
    env.assertEqual(server.connections, 1, message=stderr)
    env.assertTrue("client(s) stopped before reaching their stop condition" in result.stderr, message=stderr)
    env.assertFalse("See --connection-stage-timeout." in result.stderr, message=stderr)
    env.assertEqual(result.returncode, 1, message=stderr)
