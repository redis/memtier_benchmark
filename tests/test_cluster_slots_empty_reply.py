"""A CLUSTER SLOTS reply that is empty or an error (#648).

A scripted RESP peer acts as a one-node cluster that owns every slot. It
answers some CLUSTER SLOTS requests with an empty array or an error, as a
node that has no slots yet does. memtier used to log "cluster slot failed"
and never ask again: the connection sent no more requests, so the run hung
past --test-time and --requests. It must ask again, and finish once the node
answers.
"""
import socketserver
import subprocess
import threading
import time

from include import MEMTIER_BINARY


class _Node(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, bad_slots_replies, moved_after=0, bad_reply=b"*0\r\n"):
        super().__init__(("127.0.0.1", 0), _Handler)
        self.lock = threading.Lock()
        # CLUSTER SLOTS requests answered with bad_reply: every one after
        # the first if moved_after is set, otherwise the first ones.
        self.bad_slots_replies = bad_slots_replies
        self.bad_reply = bad_reply
        # Answer this data command with -MOVED to this node, so memtier
        # refreshes the slot map mid-run.
        self.moved_after = moved_after
        self.slots_requests = 0
        self.data_requests = 0
        self.errors = []

    def slots_reply(self):
        with self.lock:
            self.slots_requests += 1
            n = self.slots_requests
            bad = (self.moved_after and 1 < n <= 1 + self.bad_slots_replies) or \
                (not self.moved_after and n <= self.bad_slots_replies)
        if bad:
            return self.bad_reply
        port = str(self.server_address[1]).encode()
        return b"*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:" + port + b"\r\n"


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        node = self.server
        try:
            while True:
                line = self.rfile.readline()
                if not line:
                    return
                args = []
                for _ in range(int(line[1:])):
                    size = self.rfile.readline()
                    args.append(self.rfile.read(int(size[1:]) + 2)[:-2])
                verb = args[0].upper()
                if verb == b"CLUSTER":
                    reply = node.slots_reply()
                elif verb in (b"SET", b"GET"):
                    with node.lock:
                        node.data_requests += 1
                        moved = node.data_requests == node.moved_after
                    if moved:
                        reply = "-MOVED 0 127.0.0.1:{}\r\n".format(node.server_address[1]).encode()
                    else:
                        reply = b"+OK\r\n" if verb == b"SET" else b"$-1\r\n"
                else:
                    reply = b"+OK\r\n"
                self.wfile.write(reply)
                self.wfile.flush()
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as error:
            node.errors.append(repr(error))


def _run(node, extra_args, timeout=20):
    thread = threading.Thread(target=node.serve_forever, daemon=True)
    thread.start()
    args = [MEMTIER_BINARY, "--server=127.0.0.1", "--port={}".format(node.server_address[1]),
            "--cluster-mode", "--threads=1", "--clients=1", "--hide-histogram"] + extra_args
    start = time.time()
    try:
        result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=timeout, text=True)
        returncode, stderr = result.returncode, result.stderr
    except subprocess.TimeoutExpired as expired:
        returncode, stderr = "timeout", expired.stderr or ""
        if isinstance(stderr, bytes):
            stderr = stderr.decode(errors="replace")
    finally:
        node.shutdown()
        node.server_close()
        thread.join(timeout=5)
    return returncode, stderr, time.time() - start


def test_cluster_slots_empty_reply_mid_run_is_retried(env):
    # The refresh after -MOVED gets two empty replies, then a good one.
    node = _Node(bad_slots_replies=2, moved_after=50)
    returncode, stderr, _ = _run(node, ["--requests=200"])
    env.assertEqual(node.errors, [])
    env.assertEqual(returncode, 0, message=stderr[-2000:])
    env.assertContains("cluster slot failed", stderr)
    env.assertEqual(node.slots_requests, 4)
    env.assertGreaterEqual(node.data_requests, 200)


def test_cluster_slots_error_reply_at_startup_is_retried(env):
    # A node that has no slots for a moment at startup is asked again.
    node = _Node(bad_slots_replies=2, bad_reply=b"-ERR This instance has cluster support disabled\r\n")
    returncode, stderr, _ = _run(node, ["--requests=200"])
    env.assertEqual(node.errors, [])
    env.assertEqual(returncode, 0, message=stderr[-2000:])
    env.assertEqual(node.slots_requests, 3)
    env.assertGreaterEqual(node.data_requests, 200)


def test_cluster_slots_empty_reply_mid_run_honours_test_time(env):
    # The node never answers again. With --reconnect-on-error each failure
    # reconnects, and the run stops reconnecting at --test-time, failing as
    # incomplete instead of hanging.
    node = _Node(bad_slots_replies=1000, moved_after=50)
    returncode, stderr, elapsed = _run(node, ["--test-time=2", "--reconnect-on-error"])
    env.assertEqual(node.errors, [])
    env.assertNotEqual(returncode, "timeout", message=stderr[-2000:])
    env.assertNotEqual(returncode, 0)
    env.assertContains("CLUSTER SLOTS error, attempting reconnection", stderr)
    env.assertLess(elapsed, 10)
