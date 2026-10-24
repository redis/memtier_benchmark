"""Setup-only clients must not weight final duration; unanswered workload must (#819)."""
import json
import os
import signal
import socketserver
import subprocess
import tempfile
import threading
import time

from include import MEMTIER_BINARY
from test_cluster_slots_bad_reply import _Node, _run


def test_setup_only_client_does_not_shorten_duration(env):
    node = _Node(bad_slots_replies=1000, first_good=True)
    with tempfile.TemporaryDirectory() as directory:
        path = os.path.join(directory, "stats.json")
        code, stderr, _ = _run(node, ["--authenticate=duration-dummy", "--test-time=3",
                                     "--reconnect-on-error", "--rate-limiting=1000",
                                     "--ratio=1:0", "--json-out-file=" + path], clients=2)
        env.assertEqual(code, 1, message=stderr[-2000:])
        env.assertEqual(node.errors, [])
        with open(path) as source:
            stats = json.load(source)["ALL STATS"]
        env.assertEqual(stats["Totals"]["Count"], node.data_requests)
        env.assertGreater(node.slots_requests, 2)
        env.assertGreater(stats["Totals"]["Connection Errors"], 0)
        env.assertEqual(stats["Runtime"]["Interrupted"], "true")
        # The only working client runs for three seconds. Before #819 its
        # interval was averaged with the last ~one second of failed setup.
        env.assertGreaterEqual(stats["Runtime"]["Total duration"], 2900)
        env.assertLess(stats["Runtime"]["Total duration"], 4000)


class _Peer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, mode):
        super().__init__(("127.0.0.1", 0), _Handler)
        self.mode = mode
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.late_workload = threading.Event()
        self.first_setup = None
        self.first_workload = {}
        self.requests = {"early": 0, "late": 0}
        self.errors = []


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        self.connection.settimeout(15)
        peer = self.server
        authenticated = False
        role = None
        try:
            while not peer.stop.is_set():
                line = self.rfile.readline()
                if not line:
                    return
                args = []
                for _ in range(int(line[1:])):
                    size = int(self.rfile.readline()[1:])
                    args.append(self.rfile.read(size))
                    assert self.rfile.read(2) == b"\r\n"
                if args == [b"AUTH", b"duration-dummy"]:
                    authenticated = True
                    reply = b"+OK\r\n"
                else:
                    assert authenticated, "work before authentication"
                    if args == [b"CLUSTER", b"SLOTS"]:
                        with peer.lock:
                            now = time.monotonic()
                            if peer.first_setup is None:
                                peer.first_setup = now
                                role = "early"
                            else:
                                role = "late"
                            accept = role == "early" or now - peer.first_setup >= 1.5
                        reply = (b"*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:"
                                 + str(peer.server_address[1]).encode() + b"\r\n") if accept else b"*0\r\n"
                    else:
                        assert args[0] == b"SET" and role is not None, args
                        with peer.lock:
                            peer.first_workload.setdefault(role, time.monotonic())
                            peer.requests[role] += 1
                        reply = b"+OK\r\n"
                        if role == "late":
                            peer.late_workload.set()
                            if peer.mode == "unanswered":
                                peer.stop.wait(15)
                                return
                            if peer.mode == "error":
                                reply = b"-ERR workload rejected\r\n"
                            elif peer.mode == "retry":
                                reply = b"-TRYAGAIN temporary rejection\r\n"
                self.wfile.write(reply)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as error:
            peer.errors.append(repr(error))


def _late_participant(env, mode):
    if os.name == "nt":
        # Popen SIGINT is not a console Ctrl+C on Windows. Exact participation
        # arithmetic and connection deletion are also covered by the unit test.
        env.skip()
    with _Peer(mode) as peer, tempfile.TemporaryDirectory() as directory:
        thread = threading.Thread(target=peer.serve_forever)
        thread.start()
        child = None
        try:
            path = os.path.join(directory, "stats.json")
            args = [MEMTIER_BINARY, "-s", "127.0.0.1", "-p", str(peer.server_address[1]),
                    "--authenticate=duration-dummy", "-t", "1", "-c", "2", "--cluster-mode",
                    "--requests=1000000000", "--pipeline=1", "--ratio=1:0", "--rate-limiting=1000",
                    "--reconnect-on-error", "--hide-histogram", "--json-out-file=" + path]
            if mode == "retry":
                args += ["--retry-on-error", "--max-retries=-1", "--retry-backoff-ms=10"]
            child = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            assert peer.late_workload.wait(10), "late client never submitted workload"
            time.sleep(0.35)
            interrupted = time.monotonic()
            child.send_signal(signal.SIGINT)
            stdout, stderr = child.communicate(timeout=5)
            env.assertEqual(child.returncode, 0, message=stdout + stderr)
            env.assertEqual(peer.errors, [])
            with open(path) as source:
                stats = json.load(source)["ALL STATS"]
            env.assertEqual(stats["Runtime"]["Interrupted"], "true")
            early = interrupted - peer.first_workload["early"]
            late = interrupted - peer.first_workload["late"]
            env.assertGreater(early - late, 1.5)
            expected_ms = (early + late) * 500
            # Wrongly excluding the late zero-completion client produces the
            # early interval (~2.35s), rather than the two-client mean (~1.35s).
            env.assertLess(abs(stats["Runtime"]["Total duration"] - expected_ms), 350)
            if mode == "unanswered":
                env.assertEqual(peer.requests["late"], 1)
                # SIGINT can stop the early client before its final reply is
                # processed. With pipeline=1 and no early retries, at most one
                # received command can be absent from its completed Count.
                outstanding = peer.requests["early"] - stats["Totals"]["Count"]
                env.assertGreaterEqual(outstanding, 0)
                env.assertLessEqual(outstanding, 1)
            if mode == "retry":
                env.assertGreater(peer.requests["late"], 1)
                env.assertGreater(stats["Totals"]["Retry Attempts"], 0)
        finally:
            if child is not None and child.poll() is None:
                child.kill()
                child.communicate(timeout=3)
            peer.stop.set()
            peer.shutdown()
            thread.join(timeout=3)


def test_late_successful_client_participates(env):
    _late_participant(env, "success")


def test_late_unanswered_client_participates(env):
    _late_participant(env, "unanswered")


def test_late_error_only_client_participates(env):
    _late_participant(env, "error")


def test_late_retrying_client_participates(env):
    _late_participant(env, "retry")
