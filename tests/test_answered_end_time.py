"""A --test-time client with replies ends at its deadline or its last reply, whichever is later (#870).

Two identical clients run against a scripted peer. The first to authenticate
(A) is answered normally and ends at the --test-time deadline. The other (B)
is answered for a while, then its server stalls: the peer holds a SET until A
has ended (so past the deadline), then some more, then closes B's connection,
and B reconnects (--reconnect-on-error). B's last reply came long before its
deadline, so its end must be capped there. memtier used to end it when its
post-deadline reconnect completed, which stretched Total duration past
--test-time and deflated Ops/sec. #867 fixed this for a client that never had
a reply; this covers the one that had some.

- "stalled": B is answered _ANSWERED times, then stalls as above.
- "cluster": two shards. B is answered on the first and its first SET to the
  second is held, so its replies and its stall are on different shard
  connections of the same client.
- "late": with --pipeline=2, B's held SET is answered past its deadline, then
  the one behind it is held and B is closed. B ends at that late reply: the
  time spent draining it counts, the idle time after it does not.
- "split": two shards, with B's keys alternating between them. The first
  shard's connection is closed and its reconnect's AUTH held past the
  deadline, while B's next SET on the second shard is answered past the
  deadline. B ends from the first shard once its setup completes, at the late
  reply on the second: the last reply is the client's, whichever shard it came
  on, and a setup reply is not one.

The peer sets the pace: it holds and closes B relative to A's close, so the
timing is past the deadline however slowly memtier starts. No SIGINT is sent.
"""
import json
import os
import socketserver
import subprocess
import tempfile
import threading

from include import MEMTIER_BINARY

_TEST_TIME = 3
_RATE = 100
# B's replies before its server stalls (0.2 s at _RATE).
_ANSWERED = 20
# How long past A's end the peer holds B before closing it. Without the fix B
# ends at least this long past its deadline, so Total duration (the mean of
# A's and B's intervals) is at least _OVERRUN_S / 2 too long.
_OVERRUN_S = 2.0
# In "late" mode, how long past A's end the held SET is answered. B's interval
# is then about --test-time + _LATE_S, and Total duration _LATE_S / 2 above
# --test-time: 500 ms from both a cap at the deadline and an end at the
# reconnect (_LATE_S + _OVERRUN_S past the deadline).
_LATE_S = 1.0
# A's interval ends at the first rate tick past its deadline (10 ms), so a few
# ms are expected; 300 ms leaves room for a loaded host's scheduling delays.
_TOLERANCE_MS = 300


class _Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, peer):
        super().__init__(("127.0.0.1", 0), _Handler)
        self.peer = peer


class _Peer:
    def __init__(self, mode):
        self.mode = mode
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.a_closed = threading.Event()
        # Cluster mode: A's connection to the second shard authenticated.
        self.a_second = threading.Event()
        self.servers = [_Server(self) for _ in range(1 if mode in ("stalled", "late") else 2)]
        self.ports = [server.server_address[1] for server in self.servers]
        self.auths = {port: 0 for port in self.ports}
        self.a_open = 0
        self.a_replies = 0
        self.b_replies = 0
        self.b_held = 0
        self.b_late = 0
        self.b_connections = 0
        # "split": B's connection to the first shard was closed.
        self.b_dropped = False
        self.errors = []

    def slots(self):
        ranges = [(0, 16383)] if len(self.ports) == 1 else [(0, 8191), (8192, 16383)]
        reply = b"*%d\r\n" % len(ranges)
        for (first, last), port in zip(ranges, self.ports):
            reply += b"*3\r\n:%d\r\n:%d\r\n*2\r\n$9\r\n127.0.0.1\r\n:%d\r\n" % (first, last, port)
        return reply

    def hold(self):
        # Held until A has ended (so past the deadline), then some more.
        with self.lock:
            self.b_held += 1
        if self.a_closed.wait(30):
            self.stop.wait(_OVERRUN_S)

    def answer_late(self):
        # Answered, but only past the deadline.
        with self.lock:
            self.b_late += 1
        if self.a_closed.wait(30):
            self.stop.wait(_LATE_S)


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        self.connection.settimeout(30)
        peer = self.server.peer
        port = self.server.server_address[1]
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
                if args == [b"AUTH", b"dummy-870"]:
                    # The clients are identical: on each port, whichever
                    # authenticates first is A. A is never dropped, so every
                    # later connection is B's.
                    with peer.lock:
                        peer.auths[port] += 1
                        role = "A" if peer.auths[port] == 1 else "B"
                        if role == "A":
                            peer.a_open += 1
                            if port != peer.ports[0]:
                                peer.a_second.set()
                        else:
                            peer.b_connections += 1
                    if peer.b_dropped and port == peer.ports[0]:
                        # "split": B's reconnect to the first shard.
                        peer.hold()
                    reply = b"+OK\r\n"
                elif args == [b"CLUSTER", b"SLOTS"]:
                    assert len(peer.ports) == 2 and role is not None, args
                    # Hold B's slot map until A has connected to the second
                    # shard, so the first AUTH on each port is always A's.
                    if role == "B":
                        assert peer.a_second.wait(10)
                    reply = peer.slots()
                else:
                    assert args[0] == b"SET" and role is not None, args
                    if role == "A":
                        with peer.lock:
                            peer.a_replies += 1
                    elif peer.mode == "cluster" and port != peer.ports[0]:
                        peer.hold()
                        return
                    elif peer.mode == "split" and peer.b_replies >= _ANSWERED:
                        if port == peer.ports[0] or peer.b_late:
                            peer.b_dropped = True
                            return
                        peer.answer_late()
                    elif peer.mode in ("stalled", "late") and peer.b_replies >= _ANSWERED:
                        if peer.mode == "late" and peer.b_late == 0:
                            peer.answer_late()
                        else:
                            peer.hold()
                            return
                    if role == "B":
                        with peer.lock:
                            peer.b_replies += 1
                    reply = b"+OK\r\n"
                self.wfile.write(reply)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as error:
            peer.errors.append(repr(error))
        finally:
            if role == "A":
                with peer.lock:
                    peer.a_open -= 1
                    if peer.a_open == 0:
                        peer.a_closed.set()


def _scenario(mode, extra, binary=MEMTIER_BINARY):
    """Run one scenario. Returns (stats, peer counters, output)."""
    peer = _Peer(mode)
    threads = [threading.Thread(target=server.serve_forever) for server in peer.servers]
    for thread in threads:
        thread.start()
    child = None
    try:
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "stats.json")
            args = [binary, "-s", "127.0.0.1", "-p", str(peer.ports[0]),
                    "--authenticate=dummy-870", "-t", "1", "-c", "2", "--test-time=" + str(_TEST_TIME),
                    "--pipeline=" + ("2" if mode == "late" else "1"), "--ratio=1:0",
                    "--rate-limiting=" + str(_RATE), "--reconnect-on-error", "--hide-histogram",
                    "--json-out-file=" + path] + extra
            if len(peer.ports) == 2:
                args.append("--cluster-mode")
            child = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            stdout, stderr = child.communicate(timeout=30)
            with open(path) as source:
                stats = json.load(source)["ALL STATS"]
        counters = {"returncode": child.returncode, "errors": list(peer.errors), "a_replies": peer.a_replies,
                    "b_replies": peer.b_replies, "b_held": peer.b_held, "b_late": peer.b_late,
                    "b_connections": peer.b_connections, "a_closed": peer.a_closed.is_set()}
        return stats, counters, stdout + stderr
    finally:
        if child is not None and child.poll() is None:
            child.kill()
            child.communicate(timeout=3)
        peer.stop.set()
        for server in peer.servers:
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join(timeout=3)


def _check(env, mode, extra, expected_ms):
    stats, peer, output = _scenario(mode, extra)
    env.assertEqual(peer["returncode"], 0, message=output[-2000:])
    env.assertEqual(peer["errors"], [])
    env.assertEqual(stats["Runtime"]["Interrupted"], "false")
    env.assertTrue(peer["a_closed"])
    # B was answered, then one SET was held and B's connection closed.
    env.assertGreater(peer["b_replies"], 0)
    env.assertEqual(peer["b_held"], 1)
    env.assertContains("connection dropped", output)
    # Every reply counts, B's included.
    count = stats["Totals"]["Count"]
    env.assertEqual(count, peer["a_replies"] + peer["b_replies"])
    duration = stats["Runtime"]["Total duration"]
    env.assertLess(abs(duration - expected_ms), _TOLERANCE_MS, message="duration %s ms" % duration)
    expected_ops = count * 1000.0 / expected_ms
    env.assertLess(abs(stats["Totals"]["Ops/sec"] - expected_ops), expected_ops * 0.08,
                   message="ops/sec %s, expected %.1f" % (stats["Totals"]["Ops/sec"], expected_ops))
    return peer


def _check_stalled(env, mode, extra):
    # Before #870 this was >= 4000 ms (>= 4500 ms with finite reconnect attempts).
    _check(env, mode, extra, _TEST_TIME * 1000)


def test_answered_client_end_capped_unlimited_reconnects(env):
    _check_stalled(env, "stalled", [])


def test_answered_client_end_capped_finite_reconnects(env):
    _check_stalled(env, "stalled", ["--max-reconnect-attempts=5"])


def test_cluster_answered_client_end_capped_unlimited_reconnects(env):
    _check_stalled(env, "cluster", [])


def test_cluster_answered_client_end_capped_finite_reconnects(env):
    _check_stalled(env, "cluster", ["--max-reconnect-attempts=5"])


def test_answered_client_ends_at_last_reply_past_deadline(env):
    # B's interval runs to its late reply, _LATE_S past A's end: about 3500
    # ms. A cap at the deadline gives ~3000 ms, an end at B's reconnect ~4500.
    peer = _check(env, "late", [], _TEST_TIME * 1000 + _LATE_S * 1000 / 2)
    env.assertEqual(peer["b_late"], 1)


def test_cluster_answered_client_ends_at_last_reply_on_another_shard(env):
    # memtier-1 is in slot 3498 (first shard), memtier-2 in 15817 (second), so
    # B's SETs alternate between the shards. Its interval runs to the late
    # reply on the second shard, though the end comes from the first: about
    # 3500 ms. Taking the first shard's last reply gives ~3000 ms, counting
    # its reconnect's AUTH reply ~4000.
    peer = _check(env, "split", ["--key-pattern=S:S", "--key-minimum=1", "--key-maximum=2"],
                  _TEST_TIME * 1000 + _LATE_S * 1000 / 2)
    env.assertEqual(peer["b_late"], 1)
