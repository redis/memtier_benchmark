"""A reconnect must not restart the clock of a client that sent workload (#833).

Two identical clients run against a scripted peer. The first to authenticate
(A) is answered normally. The other (B) sends a SET that is never answered:
the peer closes its connection, and every reconnect of B is closed again.
B submitted workload, so it takes part in the duration average (#819), and
its start time must stay at its first connect. memtier used to restart it on
every reconnect, which shortened Total duration and inflated Ops/sec.

The peer sets the pace: SIGINT is sent once A has had a fixed number of
replies, so the expected interval is measured, not slept.
"""
import json
import os
import signal
import socketserver
import subprocess
import tempfile
import threading
import time

from include import MEMTIER_BINARY

_REPLIES = 700  # 3.5s at --rate-limiting=200: B reconnects after ~1s and ~3s.
_RATE = 200


class _Peer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, mode, cluster, port=0):
        super().__init__(("127.0.0.1", port), _Handler)
        # "repeat": every B connection authenticates and is closed on its SET.
        # "first": only B's first one gets that far; the later ones are closed
        # on AUTH, before they can send workload.
        self.mode = mode
        self.cluster = cluster
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.ready = threading.Event()
        self.first_auth = {}
        self.b_connections = 0
        self.b_sets = 0
        self.a_replies = 0
        self.errors = []


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        self.connection.settimeout(20)
        peer = self.server
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
                if args == [b"AUTH", b"dummy-833"]:
                    # The clients are identical: whichever authenticates first
                    # is A. A is never dropped, so every later one is B.
                    with peer.lock:
                        role = "A" if "A" not in peer.first_auth else "B"
                        peer.first_auth.setdefault(role, time.monotonic())
                        if role == "B":
                            peer.b_connections += 1
                            close = peer.mode == "first" and peer.b_connections > 1
                    if role == "B" and close:
                        return
                    reply = b"+OK\r\n"
                elif args == [b"CLUSTER", b"SLOTS"]:
                    assert peer.cluster and role is not None, args
                    reply = (b"*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:"
                             + str(peer.server_address[1]).encode() + b"\r\n")
                else:
                    assert args[0] == b"SET" and role is not None, args
                    if role == "B":
                        with peer.lock:
                            peer.b_sets += 1
                        return  # received, never answered
                    with peer.lock:
                        if peer.a_replies == _REPLIES:
                            # Hold A's later requests, so Count is exact
                            # however long SIGINT takes to land.
                            continue
                        peer.a_replies += 1
                        if peer.a_replies == _REPLIES:
                            peer.ready.set()
                    reply = b"+OK\r\n"
                self.wfile.write(reply)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as error:
            peer.errors.append(repr(error))


def _scenario(mode, cluster, port=0, binary=MEMTIER_BINARY):
    """Run one scenario. Returns (stats, expected_ms, peer counters, output)."""
    with _Peer(mode, cluster, port) as peer, tempfile.TemporaryDirectory() as directory:
        thread = threading.Thread(target=peer.serve_forever)
        thread.start()
        child = None
        try:
            path = os.path.join(directory, "stats.json")
            args = [binary, "-s", "127.0.0.1", "-p", str(peer.server_address[1]),
                    "--authenticate=dummy-833", "-t", "1", "-c", "2", "--test-time=30",
                    "--pipeline=1", "--ratio=1:0", "--rate-limiting=" + str(_RATE),
                    "--reconnect-on-error", "--hide-histogram", "--json-out-file=" + path]
            if cluster:
                args.append("--cluster-mode")
            child = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            assert peer.ready.wait(20), "client A never reached %d replies" % _REPLIES
            interrupted = time.monotonic()
            child.send_signal(signal.SIGINT)
            stdout, stderr = child.communicate(timeout=10)
            with open(path) as source:
                stats = json.load(source)["ALL STATS"]
            # Both clients run from their first connect to the interrupt.
            expected_ms = sum(interrupted - t for t in peer.first_auth.values()) * 1000 / len(peer.first_auth)
            counters = {"returncode": child.returncode, "errors": list(peer.errors),
                        "b_connections": peer.b_connections, "b_sets": peer.b_sets,
                        "a_replies": peer.a_replies}
            return stats, expected_ms, counters, stdout + stderr
        finally:
            if child is not None and child.poll() is None:
                child.kill()
                child.communicate(timeout=3)
            peer.stop.set()
            peer.shutdown()
            thread.join(timeout=3)


def _check(env, mode, cluster):
    if os.name == "nt":
        # Popen SIGINT is not a console Ctrl+C on Windows.
        env.skip()
    stats, expected_ms, peer, output = _scenario(mode, cluster)
    env.assertEqual(peer["returncode"], 0, message=output[-2000:])
    env.assertEqual(peer["errors"], [])
    env.assertEqual(stats["Runtime"]["Interrupted"], "true")
    # B was dropped at least twice after its first connect, so a restarted
    # clock would have covered only its last ~0.5s.
    env.assertGreaterEqual(peer["b_connections"], 3)
    if mode == "first":
        env.assertEqual(peer["b_sets"], 1)
    else:
        env.assertEqual(peer["b_sets"], peer["b_connections"])
    duration = stats["Runtime"]["Total duration"]
    # Before #833 this was ~2000ms instead of ~3500ms.
    env.assertLess(abs(duration - expected_ms), 250, message="duration %s, expected %.0f" % (duration, expected_ms))
    # Only A completes work, so the rate is A's alone (~200/s, not ~350/s).
    count = stats["Totals"]["Count"]
    # The peer answers A exactly _REPLIES times; SIGINT can land before the
    # last of them is processed.
    env.assertGreaterEqual(count, _REPLIES - 1)
    env.assertLessEqual(count, _REPLIES)
    expected_ops = count * 1000.0 / expected_ms
    env.assertLess(abs(stats["Totals"]["Ops/sec"] - expected_ops), expected_ops * 0.08,
                   message="ops/sec %s, expected %.1f" % (stats["Totals"]["Ops/sec"], expected_ops))


def test_unanswered_client_reconnects_keep_start_time(env):
    _check(env, "repeat", False)


def test_unanswered_client_setup_drops_keep_start_time(env):
    _check(env, "first", False)


def test_cluster_unanswered_client_reconnects_keep_start_time(env):
    _check(env, "repeat", True)


def test_cluster_unanswered_client_setup_drops_keep_start_time(env):
    _check(env, "first", True)
