"""A connection setup step (AUTH, SELECT, HELLO) that the server rejects (#737).

Without --reconnect-on-error a rejected connection used to stay open with its
setup unfinished. It never sent a request, so its client never ended, and once
another connection had reached steady state --connection-stage-timeout was
disarmed and nothing ended the run.

- Under --read-preference primary a replica connection is not needed: it is
  closed, not redialed by slot map refreshes, and the run ends normally.
- Any other rejected connection ends its thread once the run has reached
  steady state, so the run fails (exit 1). Before that it is still left to
  --connection-stage-timeout (exit 2).

The peers are the scripted nodes of test_cluster_slots_bad_reply.
"""
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time

from include import MEMTIER_BINARY
from test_cluster_slots_bad_reply import _Node, _run

_NO_PASSWORD = b"-ERR AUTH <password> called without any password configured for the default user\r\n"
_CLOSED_REPLICA = "it is not needed under --read-preference primary, closing it"


def _shard(*nodes, first=0, last=16383):
    reply = b"*" + str(2 + len(nodes)).encode() + b"\r\n:" + str(first).encode() + b"\r\n:" + \
        str(last).encode() + b"\r\n"
    for node in nodes:
        reply += b"*2\r\n$9\r\n127.0.0.1\r\n:" + str(node.server_address[1]).encode() + b"\r\n"
    return reply


def _primary_with_replica_rejecting_auth(**primary_args):
    replica = _Node(bad_slots_replies=0, auth_error=_NO_PASSWORD, auth_error_with_next=True)
    return _Node(bad_slots_replies=0, replica=replica, **primary_args)


def _run_json(env, node, extra_args, **run_args):
    """_run() with --json-out-file; also returns the ALL STATS Totals, or {}
    (with a failed assertion) if memtier was killed before writing them."""
    with tempfile.TemporaryDirectory() as directory:
        path = os.path.join(directory, "stats.json")
        returncode, stderr, elapsed = _run(node, extra_args + ["--json-out-file=" + path], **run_args)
        try:
            with open(path) as source:
                stats = json.load(source)["ALL STATS"]["Totals"]
        except (OSError, ValueError, KeyError) as error:
            env.assertTrue(False, message="no complete JSON (exit {}): {!r}".format(returncode, error))
            stats = {}
    return returncode, stderr, elapsed, stats


def test_replica_rejecting_auth_does_not_hold_requests_run(env):
    node = _primary_with_replica_rejecting_auth()
    returncode, stderr, elapsed, stats = _run_json(env, node, ["-a", "dummy-pw-737", "--requests=200",
                                                               "--connection-stage-timeout=5"], clients=2)
    env.assertEqual(node.errors + node.replica.errors, [])
    env.assertEqual(returncode, 0, message=stderr[-2000:])
    env.assertContains("authentication failed", stderr)
    env.assertEqual(stderr.count(_CLOSED_REPLICA), 2, message=stderr[-2000:])
    env.assertEqual(node.data_requests, 400)
    env.assertEqual(node.replica.data_requests, 0)
    # A replica closed because it is not needed is not a connection error.
    env.assertEqual(stats.get("Connection Errors"), 0)
    env.assertLess(elapsed, 8)


def test_replica_rejecting_auth_does_not_hold_test_time_run(env):
    node = _primary_with_replica_rejecting_auth()
    returncode, stderr, elapsed = _run(node, ["-a", "dummy-pw-737", "--test-time=2",
                                              "--connection-stage-timeout=5"], clients=2)
    env.assertEqual(node.errors + node.replica.errors, [])
    env.assertEqual(returncode, 0, message=stderr[-2000:])
    env.assertFalse("got no reply" in stderr, message=stderr[-2000:])
    env.assertEqual(node.replica.data_requests, 0)
    # The 10 s grace would push it past this.
    env.assertLess(elapsed, 8)


def test_replica_rejecting_auth_with_reconnect_is_closed_not_redialed(env):
    # --reconnect-on-error must not turn the unneeded replica into a
    # connection redialed about once a second, only to be rejected again: its
    # reconnect, due at the --test-time deadline, would then find the run over
    # and fail a run the primary served in full (#656). It is closed as
    # without --reconnect-on-error, also with bounded attempts.
    for extra in ([], ["--max-reconnect-attempts=50"]):
        node = _primary_with_replica_rejecting_auth()
        returncode, stderr, elapsed, stats = _run_json(env, node, ["-a", "dummy-pw-737", "--test-time=2",
                                                                   "--reconnect-on-error",
                                                                   "--connection-stage-timeout=5"] + extra,
                                                       clients=2)
        label = " ".join(extra) or "unlimited attempts"
        env.assertEqual(node.errors + node.replica.errors, [])
        env.assertEqual(returncode, 0, message="{}: {}".format(label, stderr[-2000:]))
        env.assertEqual(stderr.count(_CLOSED_REPLICA), 2, message="{}: {}".format(label, stderr[-2000:]))
        env.assertFalse("attempting reconnection" in stderr, message="{}: {}".format(label, stderr[-2000:]))
        env.assertEqual(node.replica.auth_requests, 2, message=label)
        env.assertEqual(node.replica.data_requests, 0)
        env.assertEqual(stats.get("Connection Errors"), 0, message=label)
        env.assertLess(elapsed, 8)


def test_replica_rejecting_auth_with_reconnect_under_secondary_preferred_is_closed(env):
    # Under secondaryPreferred the reads fall back to the primary, so with
    # --reconnect-on-error a replica whose AUTH is rejected is closed as an
    # unneeded one, not redialed until its retry at the deadline fails the
    # run (#656). (Without --reconnect-on-error it is kept, see
    # test_replica_rejecting_auth_is_kept_when_it_serves_reads.)
    node = _primary_with_replica_rejecting_auth()
    returncode, stderr, elapsed, stats = _run_json(env, node, ["-a", "dummy-pw-737", "--test-time=2", "--reconnect-on-error",
                                              "--read-preference=secondaryPreferred",
                                              "--connection-stage-timeout=5"], clients=2)
    env.assertEqual(node.errors + node.replica.errors, [])
    env.assertEqual(returncode, 0, message=stderr[-2000:])
    env.assertEqual(stderr.count("not needed under --read-preference secondaryPreferred, closing it"), 2,
                    message=stderr[-2000:])
    env.assertFalse("attempting reconnection" in stderr, message=stderr[-2000:])
    env.assertEqual(node.replica.auth_requests, 2)
    env.assertEqual(node.replica.data_requests, 0)
    env.assertGreater(node.data_requests, 0)
    env.assertEqual(stats.get("Connection Errors"), 0)
    env.assertLess(elapsed, 8)


def test_replica_rejecting_auth_with_reconnect_under_secondary_fails_run(env):
    # Under --read-preference secondary the replica is required: with
    # --reconnect-on-error it is redialed, and a setup still rejected at the
    # deadline fails the run (#630).
    node = _primary_with_replica_rejecting_auth()
    returncode, stderr, elapsed = _run(node, ["-a", "dummy-pw-737", "--test-time=2", "--reconnect-on-error",
                                              "--read-preference=secondary", "--read-preference-fallback=primary",
                                              "--connection-stage-timeout=5"], clients=2)
    env.assertEqual(node.errors + node.replica.errors, [])
    env.assertEqual(returncode, 1, message=stderr[-2000:])
    env.assertFalse(_CLOSED_REPLICA in stderr, message=stderr[-2000:])
    env.assertFalse("closing it" in stderr, message=stderr[-2000:])
    env.assertLess(elapsed, 8)


def test_replica_rejecting_select_does_not_hold_requests_run(env):
    replica = _Node(bad_slots_replies=0, select_error=b"-ERR DB index is out of range\r\n")
    node = _Node(bad_slots_replies=0, replica=replica)
    returncode, stderr, elapsed = _run(node, ["--select-db=1", "--requests=200", "--connection-stage-timeout=5"],
                                       clients=2)
    env.assertEqual(node.errors + replica.errors, [])
    env.assertEqual(returncode, 0, message=stderr[-2000:])
    env.assertEqual(stderr.count("SELECT error on replica connection"), 2, message=stderr[-2000:])
    env.assertEqual(node.data_requests, 400)
    env.assertLess(elapsed, 8)


def test_replica_rejecting_auth_is_not_redialed_by_slot_refreshes(env):
    # Every 50th request answers -MOVED, so the slot map is refreshed about
    # 20 times. The closed replica must not be redialed on each refresh, only
    # to be rejected and counted again.
    node = _primary_with_replica_rejecting_auth(moved_every=50)
    returncode, stderr, elapsed, stats = _run_json(env, node, ["-a", "dummy-pw-737", "--requests=500",
                                                               "--connection-stage-timeout=5"], clients=2)
    env.assertEqual(node.errors + node.replica.errors, [])
    env.assertEqual(returncode, 0, message=stderr[-2000:])
    env.assertGreater(node.slots_requests, 10)
    env.assertEqual(node.replica.auth_requests, 2)
    env.assertEqual(stderr.count(_CLOSED_REPLICA), 2, message=stderr[-2000:])
    # A replica closed because it is not needed is not a connection error.
    env.assertEqual(stats.get("Connection Errors"), 0)
    env.assertLess(elapsed, 10)


def test_replica_rejecting_auth_fails_run_once_promoted(env):
    # A failover: after 100 requests the primary answers -MOVED to its
    # replica, and the refreshed slot map names the replica the only primary.
    # memtier dials it as a primary and its AUTH is rejected again. The run
    # has reached steady state, so --connection-stage-timeout no longer ends
    # it: the thread ends instead and the run fails.
    node = _primary_with_replica_rejecting_auth(moved_after=100)
    replica = node.replica
    # As a primary, the connection sends AUTH alone: answer it at once.
    replica.auth_error_with_next = False
    node.moved_to = replica
    node.slots_fn = lambda n: b"*1\r\n" + (_shard(replica) if n.data_requests >= 100 else _shard(n, replica))
    replica.slots_fn = lambda n: b"*1\r\n" + _shard(replica)
    returncode, stderr, elapsed = _run(node, ["-a", "dummy-pw-737", "--test-time=3",
                                              "--connection-stage-timeout=5"])
    env.assertEqual(node.errors + replica.errors, [])
    env.assertEqual(returncode, 1, message=stderr[-2000:])
    env.assertContains("AUTH error and --reconnect-on-error is not set, ending this thread", stderr)
    env.assertEqual(replica.data_requests, 0)
    env.assertLess(elapsed, 10)


def test_primary_rejecting_auth_fails_run_once_another_primary_runs(env):
    # Two primaries, each with half the slots; the second rejects AUTH. The
    # first primary holds its first data reply back, so the rejection always
    # comes before steady state and is left to --connection-stage-timeout.
    # Once the first primary has run, the one-second re-check ends the thread
    # and the run fails, instead of waiting for requests that cannot be sent.
    bad = _Node(bad_slots_replies=0, auth_error=_NO_PASSWORD)
    node = _Node(bad_slots_replies=0, replica=bad, first_data_delay=0.5)
    node.slots_fn = lambda n: b"*2\r\n" + _shard(n, last=8191) + _shard(bad, first=8192)
    returncode, stderr, elapsed = _run(node, ["-a", "dummy-pw-737", "--requests=200",
                                              "--connection-stage-timeout=5"])
    env.assertEqual(node.errors + bad.errors, [])
    env.assertEqual(returncode, 1, message=stderr[-2000:])
    env.assertContains("AUTH error and --reconnect-on-error is not set, ending this thread", stderr)
    env.assertFalse(_CLOSED_REPLICA in stderr, message=stderr[-2000:])
    env.assertLess(elapsed, 8)


def test_primary_rejecting_auth_at_startup_is_bounded_by_connection_stage_timeout(env):
    # No connection reaches steady state: --connection-stage-timeout (exit 2).
    node = _Node(bad_slots_replies=0, auth_error=_NO_PASSWORD)
    returncode, stderr, elapsed = _run(node, ["-a", "dummy-pw-737", "--requests=200",
                                              "--connection-stage-timeout=3"])
    env.assertEqual(node.errors, [])
    env.assertEqual(returncode, 2, message=stderr[-2000:])
    env.assertContains("See --connection-stage-timeout.", stderr)
    env.assertLess(elapsed, 8)


def test_replica_rejecting_auth_is_kept_when_it_serves_reads(env):
    # Under secondaryPreferred the replica would carry the reads, so it is not
    # closed as an unneeded one: its rejection is a startup failure for
    # --connection-stage-timeout (exit 2).
    node = _primary_with_replica_rejecting_auth()
    returncode, stderr, _ = _run(node, ["-a", "dummy-pw-737", "--requests=200",
                                        "--read-preference=secondaryPreferred", "--connection-stage-timeout=3"],
                                 clients=2)
    env.assertEqual(node.errors + node.replica.errors, [])
    env.assertEqual(returncode, 2, message=stderr[-2000:])
    env.assertContains("See --connection-stage-timeout.", stderr)
    env.assertFalse(_CLOSED_REPLICA in stderr, message=stderr[-2000:])


def test_replica_rejecting_auth_under_secondary_does_not_hang(env):
    # The replica's AUTH error is answered on its own, so each client's first
    # SET can reach the primary and the run reaches steady state, which
    # disarms --connection-stage-timeout. The rejected replica must then end
    # the thread (exit 1) rather than wait forever; if the stage timeout
    # fires first, it ends the run with exit 2. Which one depends on timing,
    # so this only guards against the hang:
    # test_replica_rejecting_auth_under_secondary_ends_thread_while_peer_in_setup
    # pins exit 1.
    replica = _Node(bad_slots_replies=0, auth_error=_NO_PASSWORD)
    node = _Node(bad_slots_replies=0, replica=replica)
    returncode, stderr, elapsed = _run(node, ["-a", "dummy-pw-737", "--requests=200",
                                              "--read-preference=secondary", "--connection-stage-timeout=3"],
                                       clients=2)
    env.assertEqual(node.errors + replica.errors, [])
    env.assertContains(returncode, (1, 2), message=stderr[-2000:])
    env.assertFalse(_CLOSED_REPLICA in stderr, message=stderr[-2000:])
    env.assertLess(elapsed, 8)


def test_replica_rejecting_auth_under_secondary_ends_thread_while_peer_in_setup(env):
    # P1 (replica A) and P2 (replicas B and C). A rejects AUTH; C holds
    # READONLY back. With --read-preference-fallback=primary the primaries do
    # not wait for the replicas, so the run reaches steady state while C is
    # still in its setup. A dropped connection is left dead while a peer is in
    # its setup, but A was rejected and stays needed for P1's reads: the
    # thread must end and the run fail, not exit 0.
    a = _Node(bad_slots_replies=0, auth_error=_NO_PASSWORD)
    b = _Node(bad_slots_replies=0)
    c = _Node(bad_slots_replies=0, readonly_delay=4)
    p2 = _Node(bad_slots_replies=0)
    p1 = _Node(bad_slots_replies=0, replica=a)
    p1.extra = [p2, b, c]

    def topology(_):
        return b"*2\r\n" + _shard(p1, a, last=8191) + _shard(p2, b, c, first=8192)

    for n in (p1, p2, a, b, c):
        n.slots_fn = topology
    returncode, stderr, elapsed = _run(p1, ["-a", "dummy-pw-737", "--requests=200", "--read-preference=secondary",
                                            "--read-preference-fallback=primary", "--connection-stage-timeout=10"])
    env.assertEqual(p1.errors + p2.errors + a.errors + b.errors + c.errors, [])
    env.assertEqual(returncode, 1, message=stderr[-2000:])
    env.assertContains("AUTH error and --reconnect-on-error is not set, ending this thread", stderr)
    env.assertFalse("leaving conn" in stderr, message=stderr[-2000:])
    env.assertLess(elapsed, 8)


def test_standalone_auth_rejected_on_some_connections_fails_run(env):
    # Standalone: the server rejects AUTH on 4 of the 16 connections. The
    # others run, which disarms --connection-stage-timeout, and their threads
    # cannot end while a rejected connection is open. Each rejected one must
    # end its thread, so the run fails well before the 20 s kill.
    node = _Node(bad_slots_replies=0, auth_error=_NO_PASSWORD, auth_reject_first=4)
    returncode, stderr, elapsed = _run(node, ["-a", "dummy-pw-737", "--requests=2000",
                                              "--connection-stage-timeout=1"], workers=4, clients=4, cluster=False)
    env.assertEqual(node.errors, [])
    # A thread ended by a rejected connection fails the run, and threads that
    # have not dialed yet then never send AUTH: only bound the counts.
    env.assertGreaterEqual(node.auth_requests, 5)
    rejected = stderr.count("authentication failed")
    env.assertTrue(1 <= rejected <= 4, message=stderr[-2000:])
    env.assertEqual(returncode, 1, message=stderr[-2000:])
    env.assertContains("AUTH error and --reconnect-on-error is not set, ending this thread", stderr)
    env.assertLess(elapsed, 8)


class _SlowRedialNode:
    """One of two scripted cluster nodes (port 0) that RESP-serves every
    connection it accepts. With a listen backlog of 0, pause() stops accepting
    and fills the accept queue, so a new connect's SYN is dropped by the
    kernel and that connect stays in progress until release()."""

    def __init__(self, backlog, reject_auth_after_first=False):
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(backlog)
        self.listener.settimeout(0.05)
        self.port = self.listener.getsockname()[1]
        self.reject_auth_after_first = reject_auth_after_first
        self.slots = b""
        self.lock = threading.Lock()
        self.accepted = 0
        self.auths_after_first = 0
        self.conns = []
        self.fillers = []
        self.paused = threading.Event()
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()

    def _accept(self):
        while not self.stop.is_set():
            if self.paused.is_set():
                time.sleep(0.01)
                continue
            try:
                conn, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with self.lock:
                self.accepted += 1
                self.conns.append(conn)
                index = self.accepted
            threading.Thread(target=self._serve, args=(conn, index), daemon=True).start()

    def _serve(self, conn, index):
        reader = conn.makefile("rb")
        try:
            while True:
                line = reader.readline()
                if not line:
                    return
                args = []
                for _ in range(int(line[1:])):
                    length = int(reader.readline()[1:])
                    args.append(reader.read(length + 2)[:-2])
                verb = args[0].upper()
                if verb == b"CLUSTER":
                    reply = self.slots
                elif verb == b"AUTH" and index > 1 and self.reject_auth_after_first:
                    with self.lock:
                        self.auths_after_first += 1
                    reply = b"-WRONGPASS invalid username-password pair or user is disabled.\r\n"
                else:
                    reply = b"+OK\r\n"
                conn.sendall(reply)
        except (OSError, ValueError):
            pass

    def pause_and_drop(self):
        self.paused.set()
        time.sleep(0.1)
        for _ in range(2):  # a backlog of 0 queues one connect: fill it
            filler = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            filler.setblocking(False)
            try:
                filler.connect(("127.0.0.1", self.port))
            except BlockingIOError:
                pass
            self.fillers.append(filler)
        time.sleep(0.1)
        with self.lock:
            for conn in self.conns:
                try:
                    conn.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                conn.close()

    def release(self):
        while True:  # drain the fillers, so memtier's retransmitted SYN gets in
            try:
                conn, _ = self.listener.accept()
            except (socket.timeout, OSError):
                break
            conn.close()
        self.paused.clear()

    def close(self):
        self.stop.set()
        self.thread.join(timeout=5)
        for sock in self.fillers + [self.listener]:
            sock.close()


def test_rejected_setup_after_slow_redial_past_deadline_fails_run(env):
    # Two primaries, each with half the slots. The second drops its
    # connection at 1 s of a 3 s run, and its listen queue is then kept full
    # until 3.5 s, so memtier's redial (connect() cleared the pending
    # reconnect) is still in progress at the deadline, with nothing pending.
    # The first connection, idle past its deadline, must not end the client
    # then: once the connect completes, the redial's AUTH goes out, is
    # rejected, and fails the run (#630).
    if sys.platform != "linux":
        env.skip()  # relies on Linux dropping the SYN when the accept queue is full
    first = _SlowRedialNode(16)
    second = _SlowRedialNode(0, reject_auth_after_first=True)
    slots = b"*2\r\n"
    for node, low, high in ((first, 0, 8191), (second, 8192, 16383)):
        slots += b"*3\r\n:%d\r\n:%d\r\n*2\r\n$9\r\n127.0.0.1\r\n:%d\r\n" % (low, high, node.port)
    first.slots = second.slots = slots
    timer = threading.Timer(1.0, second.pause_and_drop)
    releaser = threading.Timer(3.5, second.release)
    args = [MEMTIER_BINARY, "--server=127.0.0.1", "--port={}".format(first.port), "--cluster-mode",
            "--authenticate=pw", "--threads=1", "--clients=1", "--test-time=3", "--rate-limiting=50",
            "--ratio=1:0", "--key-maximum=1000", "--reconnect-on-error", "--hide-histogram"]
    start = time.time()
    timer.start()
    releaser.start()
    try:
        result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20, text=True)
        returncode, stderr = result.returncode, result.stderr
    except subprocess.TimeoutExpired as expired:
        returncode, stderr = "timeout", expired.stderr or ""
        if isinstance(stderr, bytes):
            stderr = stderr.decode(errors="replace")
    finally:
        elapsed = time.time() - start
        timer.cancel()
        releaser.cancel()
        first.close()
        second.close()
    env.assertEqual(second.auths_after_first, 1, message=stderr[-2000:])
    env.assertEqual(returncode, 1, message=stderr[-2000:])
    env.assertContains("authentication failed", stderr)
    env.assertLess(elapsed, 10)
