"""A --test-time client that sent workload but never had a reply ends at its deadline (#867).

Two identical clients run against a scripted peer. The first to authenticate
(A) is answered normally and ends at the --test-time deadline. The other (B)
sends a SET that is never answered: the peer holds it past the deadline and
then closes B's connection, and B reconnects (--reconnect-on-error). B
submitted workload, so it takes part in the duration average (#819), and its
end must be capped at its deadline, as for a client that had replies. memtier
used to end it when its post-deadline reconnect completed, which stretched
Total duration past --test-time and deflated Ops/sec.

The peer sets the pace: it closes B only once A has ended and closed its own
connection, plus _OVERRUN_S, so B's drop is always past the deadline however
slowly memtier starts. No SIGINT is sent.

Three variations pin the rest of the rule:
- "late": B's first connection is closed on AUTH, before any workload, so its
  clock starts at its reconnect, about 1 s late. Its end must be capped at its
  own start plus --test-time, not at the benchmark's start plus --test-time.
- "drain": B is answered, but one SET is held until past the deadline. A client
  with replies ends when its last reply is drained, so that time still counts.
- "early": B's SET is dropped and its reconnect is closed on AUTH, so B gives
  up before the deadline and the run fails. Its end is when it gave up, not a
  deadline that has not come yet.
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
# How long past A's end the peer holds B's SET. Each B drop after this ends B
# at least _OVERRUN_S past its deadline without the fix, so Total duration (the
# mean of A's and B's intervals) is at least _OVERRUN_S / 2 too long.
_OVERRUN_S = 2.0
# Total duration is a mean of wall-clock intervals. With the fix B's is
# exactly --test-time and A's ends at the first rate tick past its deadline
# (10 ms), so a few ms are expected; 300 ms leaves room for a loaded host's
# scheduling delays and is still far below the >= 1000 ms the bug adds.
_TOLERANCE_MS = 300
# In "drain" mode, the number of B's SETs answered before the one held past
# the deadline (0.5 s at _RATE), so B has replies when that one is drained.
_DRAIN_AFTER = 50
# In "early" mode, Total duration is a mean of client intervals, which lie
# inside the worker thread's wall time that memtier reports (CPU
# cpu_wall_seconds, taken around the thread's whole run). So it cannot exceed
# that wall time; the margin covers rounding only.
_WALL_MARGIN_MS = 50


class _Peer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, mode, cluster):
        super().__init__(("127.0.0.1", 0), _Handler)
        # "held", "late", "drain" or "early": see the module docstring.
        self.mode = mode
        self.cluster = cluster
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.a_closed = threading.Event()
        self.roles = []
        self.b_sets = 0
        self.a_replies = 0
        self.b_replies = 0
        self.errors = []


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        self.connection.settimeout(30)
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
                if args == [b"AUTH", b"dummy-867"]:
                    # The clients are identical: whichever authenticates first
                    # is A. A is never dropped, so every later one is B.
                    with peer.lock:
                        role = "B" if "A" in peer.roles else "A"
                        peer.roles.append(role)
                        b_connections = peer.roles.count("B")
                    if role == "B" and peer.mode == "late" and b_connections == 1:
                        return  # Closed before workload: B's clock restarts.
                    if role == "B" and peer.mode == "early" and b_connections > 1:
                        return  # Every reconnect fails, so B gives up.
                    reply = b"+OK\r\n"
                elif args == [b"CLUSTER", b"SLOTS"]:
                    assert peer.cluster and role is not None, args
                    reply = (b"*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:"
                             + str(peer.server_address[1]).encode() + b"\r\n")
                else:
                    assert args[0] == b"SET" and role is not None, args
                    if role == "B" and peer.mode == "drain":
                        if peer.b_replies == _DRAIN_AFTER and peer.a_closed.wait(30):
                            # Answered, but only past the deadline.
                            peer.stop.wait(_OVERRUN_S)
                        with peer.lock:
                            peer.b_replies += 1
                        reply = b"+OK\r\n"
                    elif role == "B":
                        with peer.lock:
                            peer.b_sets += 1
                        if peer.mode == "early":
                            return  # Dropped before the deadline, unanswered.
                        # Received, never answered: held until A has ended
                        # (so past the deadline), then some more, then closed.
                        if peer.a_closed.wait(30):
                            peer.stop.wait(_OVERRUN_S)
                        return
                    else:
                        with peer.lock:
                            peer.a_replies += 1
                        reply = b"+OK\r\n"
                self.wfile.write(reply)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as error:
            peer.errors.append(repr(error))
        finally:
            if role == "A":
                peer.a_closed.set()


def _scenario(cluster, extra, mode="held", binary=MEMTIER_BINARY):
    """Run one scenario. Returns (stats, peer counters, output)."""
    with _Peer(mode, cluster) as peer, tempfile.TemporaryDirectory() as directory:
        thread = threading.Thread(target=peer.serve_forever)
        thread.start()
        child = None
        try:
            path = os.path.join(directory, "stats.json")
            args = [binary, "-s", "127.0.0.1", "-p", str(peer.server_address[1]),
                    "--authenticate=dummy-867", "-t", "1", "-c", "2", "--test-time=" + str(_TEST_TIME),
                    "--pipeline=1", "--ratio=1:0", "--rate-limiting=" + str(_RATE),
                    "--reconnect-on-error", "--hide-histogram", "--json-out-file=" + path] + extra
            if cluster:
                args.append("--cluster-mode")
            child = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            stdout, stderr = child.communicate(timeout=30)
            with open(path) as source:
                stats = json.load(source)["ALL STATS"]
            counters = {"returncode": child.returncode, "errors": list(peer.errors), "b_sets": peer.b_sets,
                        "a_replies": peer.a_replies, "b_replies": peer.b_replies, "roles": list(peer.roles),
                        "a_closed": peer.a_closed.is_set()}
            return stats, counters, stdout + stderr
        finally:
            if child is not None and child.poll() is None:
                child.kill()
                child.communicate(timeout=3)
            peer.stop.set()
            peer.shutdown()
            thread.join(timeout=3)


def _check(env, cluster, extra, mode="held"):
    stats, peer, output = _scenario(cluster, extra, mode)
    env.assertEqual(peer["returncode"], 0, message=output[-2000:])
    env.assertEqual(peer["errors"], [])
    env.assertEqual(stats["Runtime"]["Interrupted"], "false")
    env.assertTrue(peer["a_closed"])
    # B sent its first SET (so it takes part in the average) and nothing more.
    env.assertEqual(peer["b_sets"], 1)
    if mode == "late":
        # B's first connection was closed on AUTH, and it then sent its SET.
        env.assertEqual(peer["roles"][:3], ["A", "B", "B"])
    else:
        env.assertEqual(peer["roles"][:2], ["A", "B"])
    env.assertContains("connection dropped", output)
    duration = stats["Runtime"]["Total duration"]
    expected_ms = _TEST_TIME * 1000
    # Before #867 this was >= 4000 ms (>= 4500 ms with finite reconnect attempts).
    # In "late" mode B's drop is still ~1 s past its own deadline, so it was
    # >= 3500 ms; a deadline from the benchmark's start cuts B's interval by
    # its ~1 s late start instead, to ~2500 ms.
    env.assertLess(abs(duration - expected_ms), _TOLERANCE_MS, message="duration %s ms" % duration)
    # Only A completes work, so the rate is A's Count over --test-time.
    count = stats["Totals"]["Count"]
    env.assertEqual(count, peer["a_replies"])
    env.assertGreater(count, 0)
    expected_ops = count / float(_TEST_TIME)
    env.assertLess(abs(stats["Totals"]["Ops/sec"] - expected_ops), expected_ops * 0.08,
                   message="ops/sec %s, expected %.1f" % (stats["Totals"]["Ops/sec"], expected_ops))


def test_unanswered_client_end_capped_unlimited_reconnects(env):
    _check(env, False, [])


def test_unanswered_client_end_capped_finite_reconnects(env):
    _check(env, False, ["--max-reconnect-attempts=5"])


def test_cluster_unanswered_client_end_capped_unlimited_reconnects(env):
    _check(env, True, [])


def test_cluster_unanswered_client_end_capped_finite_reconnects(env):
    _check(env, True, ["--max-reconnect-attempts=5"])


def test_late_started_unanswered_client_end_capped_at_own_deadline(env):
    _check(env, False, [], "late")


def test_answered_client_end_includes_drain_past_deadline(env):
    stats, peer, output = _scenario(False, [], "drain")
    env.assertEqual(peer["returncode"], 0, message=output[-2000:])
    env.assertEqual(peer["errors"], [])
    env.assertTrue(peer["a_closed"])
    # B had replies before the held one, and the held one was answered too.
    env.assertGreater(peer["b_replies"], _DRAIN_AFTER)
    env.assertEqual(stats["Totals"]["Count"], peer["a_replies"] + peer["b_replies"])
    # B's last reply came _OVERRUN_S past A's end, so B's interval is about
    # --test-time + _OVERRUN_S and Total duration about _OVERRUN_S / 2 (1000
    # ms) above --test-time. Capping B at its deadline gives ~3000 ms, so the
    # same _TOLERANCE_MS above --test-time separates the two.
    duration = stats["Runtime"]["Total duration"]
    env.assertGreater(duration, _TEST_TIME * 1000 + _TOLERANCE_MS, message="duration %s ms" % duration)


def test_unanswered_client_giving_up_before_deadline_ends_then(env):
    stats, peer, output = _scenario(False, ["--max-reconnect-attempts=1"], "early")
    # B's reconnect failed before the deadline, so the run fails.
    env.assertEqual(peer["returncode"], 1, message=output[-2000:])
    env.assertEqual(peer["errors"], [])
    env.assertEqual(peer["b_sets"], 1)
    env.assertEqual(peer["roles"][:3], ["A", "B", "B"])
    duration = stats["Runtime"]["Total duration"]
    wall_ms = float(stats["CPU"]["cpu_wall_seconds"]) * 1000
    message = "duration %s ms, wall %.0f ms" % (duration, wall_ms)
    # Both clients end when B gives up, about 1 s in, so Total duration is
    # about that wall time: the clients connect a few ms after the thread
    # starts. A deadline stamped in the future puts B at --test-time instead,
    # and Total duration ~1000 ms above the wall time.
    env.assertLess(duration, wall_ms + _WALL_MARGIN_MS, message=message)
    env.assertGreater(duration, wall_ms - _TOLERANCE_MS, message=message)
