"""A CLUSTER SLOTS reply that is empty, an error (#648), or malformed (#670).

A scripted RESP peer acts as a one-node cluster that owns every slot. It
answers some CLUSTER SLOTS requests with an empty array or an error, as a
node that has no slots yet does, or with shards that all fail validation.
memtier used to log "cluster slot failed" and never ask again: the connection
sent no more requests, so the run hung past --test-time and --requests. At
startup it must ask again, without charging the wait to the run; mid-run it
must reconnect with --reconnect-on-error, or else fail the run.

test_connection_setup_rejected reuses the scripted _Node and _run().
"""
import socketserver
import subprocess
import threading
import time

from include import MEMTIER_BINARY


class _Node(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, bad_slots_replies, moved_after=0, bad_reply=b"*0\r\n", replica=None, readonly_delay=0,
                 first_good=False, bad_delay=0, auth_error=None, auth_error_with_next=False, select_error=None,
                 moved_every=0, auth_reject_first=0, first_data_delay=0):
        super().__init__(("127.0.0.1", 0), _Handler)
        self.lock = threading.Lock()
        # CLUSTER SLOTS requests answered with bad_reply: the ones after the
        # first if moved_after or first_good is set, otherwise the first ones.
        self.bad_slots_replies = bad_slots_replies
        self.bad_reply = bad_reply
        # Answer this data command with -MOVED to this node, so memtier
        # refreshes the slot map mid-run.
        self.moved_after = moved_after
        self.first_good = first_good
        # How long to wait before sending bad_reply.
        self.bad_delay = bad_delay
        # Reply to AUTH, if not +OK, and whether to send it together with
        # the next reply, as a server answering a pipelined setup does.
        self.auth_error = auth_error
        self.auth_error_with_next = auth_error_with_next
        # Reject only the first auth_reject_first AUTH requests (all if 0).
        self.auth_reject_first = auth_reject_first
        # How long to wait before the first data reply.
        self.first_data_delay = first_data_delay
        # More nodes for _run() to serve, beside replica.
        self.extra = []
        # Reply to SELECT, if not +OK.
        self.select_error = select_error
        # Answer every moved_every-th data command with -MOVED to moved_to
        # (this node if None), so memtier refreshes the slot map again and again.
        self.moved_every = moved_every
        self.moved_to = None
        # Replaces slots_reply() when set: called with this node.
        self.slots_fn = None
        self.auth_requests = 0
        # A node listed as this node's replica in a good reply, and how long
        # a node waits before answering READONLY.
        self.replica = replica
        self.readonly_delay = readonly_delay
        self.slots_requests = 0
        self.data_requests = 0
        self.errors = []

    def slots_reply(self):
        if self.slots_fn is not None:
            with self.lock:
                self.slots_requests += 1
            return self.slots_fn(self)
        with self.lock:
            self.slots_requests += 1
            n = self.slots_requests
            after_first = self.moved_after or self.first_good
            bad = (after_first and 1 < n <= 1 + self.bad_slots_replies) or \
                (not after_first and n <= self.bad_slots_replies)
        if bad:
            time.sleep(self.bad_delay)
            return self.bad_reply
        nodes = [self] + ([self.replica] if self.replica else [])
        reply = b"*1\r\n*" + str(2 + len(nodes)).encode() + b"\r\n:0\r\n:16383\r\n"
        for node in nodes:
            reply += b"*2\r\n$9\r\n127.0.0.1\r\n:" + str(node.server_address[1]).encode() + b"\r\n"
        return reply


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        node = self.server
        held = b""
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
                elif verb == b"SELECT" and node.select_error:
                    reply = node.select_error
                elif verb == b"AUTH" and node.auth_error:
                    with node.lock:
                        node.auth_requests += 1
                        rejected = not node.auth_reject_first or node.auth_requests <= node.auth_reject_first
                    if not rejected:
                        reply = b"+OK\r\n"
                    elif node.auth_error_with_next:
                        held = node.auth_error
                        continue
                    else:
                        reply = node.auth_error
                elif verb == b"READONLY":
                    time.sleep(node.readonly_delay)
                    reply = b"+OK\r\n"
                elif verb in (b"SET", b"GET"):
                    with node.lock:
                        node.data_requests += 1
                        moved = node.data_requests == node.moved_after or \
                            (node.moved_every and node.data_requests % node.moved_every == 0)
                        first = node.data_requests == 1
                    if first:
                        time.sleep(node.first_data_delay)
                    if moved:
                        target = node.moved_to or node
                        reply = "-MOVED 0 127.0.0.1:{}\r\n".format(target.server_address[1]).encode()
                    else:
                        reply = b"+OK\r\n" if verb == b"SET" else b"$-1\r\n"
                else:
                    reply = b"+OK\r\n"
                self.wfile.write(held + reply)
                self.wfile.flush()
                held = b""
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as error:
            node.errors.append(repr(error))


def _run(node, extra_args, timeout=20, workers=1, clients=1, cluster=True, start_fraction=None):
    others = ([node.replica] if node.replica else []) + node.extra
    threads = [threading.Thread(target=n.serve_forever, daemon=True) for n in [node] + others]
    for thread in threads:
        thread.start()
    args = [MEMTIER_BINARY, "--server=127.0.0.1", "--port={}".format(node.server_address[1]),
            "--threads={}".format(workers), "--clients={}".format(clients),
            "--hide-histogram"] + (["--cluster-mode"] if cluster else []) + extra_args
    if start_fraction is not None:
        # Start the run this far into a wall-clock second.
        time.sleep((start_fraction - time.time() % 1) % 1)
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
        for n, thread in zip([node] + others, threads):
            n.shutdown()
            n.server_close()
            thread.join(timeout=5)
    return returncode, stderr, time.time() - start


def test_cluster_slots_empty_reply_mid_run_is_retried(env):
    # The refresh after -MOVED gets two empty replies, then a good one.
    node = _Node(bad_slots_replies=2, moved_after=50)
    returncode, stderr, _ = _run(node, ["--requests=200", "--reconnect-on-error"])
    env.assertEqual(node.errors, [])
    env.assertEqual(returncode, 0, message=stderr[-2000:])
    env.assertContains("cluster slot failed", stderr)
    env.assertEqual(node.slots_requests, 4)
    env.assertGreaterEqual(node.data_requests, 200)


def _assert_startup_retry_not_charged(env, node, returncode, stderr, elapsed):
    # About two seconds of retries, then the full --test-time=2 run. If the
    # retries counted towards --test-time, the run would end right after the
    # first good reply, with only a handful of requests.
    env.assertEqual(node.errors, [])
    env.assertEqual(returncode, 0, message=stderr[-2000:])
    env.assertEqual(node.slots_requests, 3)
    env.assertGreaterEqual(elapsed, 3.5)
    env.assertGreaterEqual(node.data_requests, 1000)


def test_cluster_slots_error_reply_at_startup_is_retried(env):
    # A node that has no slots for a moment at startup is asked again.
    node = _Node(bad_slots_replies=2, bad_reply=b"-ERR This instance has cluster support disabled\r\n")
    returncode, stderr, elapsed = _run(node, ["--test-time=2", "--pipeline=4"])
    _assert_startup_retry_not_charged(env, node, returncode, stderr, elapsed)


def test_cluster_slots_empty_reply_mid_run_fails_without_reconnect(env):
    # Without --reconnect-on-error a refresh that fails mid-run ends the
    # thread, as a dropped connection does, so the run fails.
    node = _Node(bad_slots_replies=1000, moved_after=50)
    returncode, stderr, elapsed = _run(node, ["--test-time=3"])
    env.assertEqual(node.errors, [])
    env.assertNotEqual(returncode, "timeout", message=stderr[-2000:])
    env.assertNotEqual(returncode, 0)
    env.assertContains("CLUSTER SLOTS error and --reconnect-on-error is not set, ending this thread", stderr)
    env.assertEqual(node.slots_requests, 2)
    # Before the fix the run hung until the 20 s kill.
    env.assertLess(elapsed, 10)


def test_cluster_slots_empty_reply_mid_run_with_replica_in_setup(env):
    # With --read-preference other than primary, a connection that fails
    # while a peer is still in its setup is left disconnected instead of
    # ending the thread. Here the primary's refresh fails while the
    # replica's READONLY is pending. Once the replica is ready, a write for
    # the dead primary makes it refresh the map itself; that fails too and
    # ends the thread, so the run fails before --test-time.
    replica = _Node(bad_slots_replies=1000, readonly_delay=1)
    node = _Node(bad_slots_replies=1000, moved_after=50, replica=replica)
    returncode, stderr, elapsed = _run(node, ["--test-time=5", "--read-preference=secondary", "--ratio=1:0"])
    env.assertEqual(node.errors + replica.errors, [])
    env.assertNotEqual(returncode, "timeout", message=stderr[-2000:])
    env.assertNotEqual(returncode, 0)
    env.assertContains("leaving conn 0 dead and continuing while peers are still mid-setup", stderr)
    env.assertContains("CLUSTER SLOTS error and --reconnect-on-error is not set, ending this thread", stderr)
    env.assertEqual(node.slots_requests, 2)
    env.assertEqual(replica.slots_requests, 1)
    env.assertLess(elapsed, 10)


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


# The node answers the first CLUSTER SLOTS, then never again: one client runs,
# the other never gets a slot map. Its retries must not go on forever once
# the first client has disarmed --connection-stage-timeout.
def test_cluster_slots_empty_reply_for_one_client_fails_without_reconnect(env):
    node = _Node(bad_slots_replies=1000, first_good=True)
    returncode, stderr, elapsed = _run(node, ["--test-time=2"], clients=2)
    env.assertEqual(node.errors, [])
    env.assertNotEqual(returncode, "timeout", message=stderr[-2000:])
    env.assertNotEqual(returncode, 0)
    env.assertContains("CLUSTER SLOTS error and --reconnect-on-error is not set, ending this thread", stderr)
    # The second client may have asked once more before the first one's
    # reply disarmed the supervisor.
    env.assertGreaterEqual(node.slots_requests, 2)
    env.assertLess(elapsed, 10)


def test_cluster_slots_empty_reply_for_one_client_fails_with_requests(env):
    # Under --requests the run has no deadline, so the second client's
    # failure ends the thread at once instead of being retried forever.
    node = _Node(bad_slots_replies=1000, first_good=True)
    returncode, stderr, elapsed = _run(node, ["--requests=200"], clients=2)
    env.assertEqual(node.errors, [])
    env.assertNotEqual(returncode, "timeout", message=stderr[-2000:])
    env.assertNotEqual(returncode, 0)
    env.assertContains("CLUSTER SLOTS error and --reconnect-on-error is not set, ending this thread", stderr)
    env.assertLess(elapsed, 10)


def test_cluster_slots_empty_reply_for_late_staircase_clients_fails(env):
    # With --clients-start, clients added later never get a slot map. They
    # never send a request, so the run must fail at the deadline rather
    # than report one client's throughput as a full run.
    node = _Node(bad_slots_replies=1000, first_good=True)
    returncode, stderr, elapsed = _run(node, ["--test-time=4", "--clients-start=1", "--clients-step=1",
                                              "--step-duration=1"], clients=3)
    env.assertEqual(node.errors, [])
    env.assertNotEqual(returncode, "timeout", message=stderr[-2000:])
    env.assertNotEqual(returncode, 0)
    env.assertContains("CLUSTER SLOTS error and --reconnect-on-error is not set, ending this thread", stderr)
    env.assertLess(elapsed, 12)


def test_cluster_slots_empty_reply_for_late_staircase_clients_fails_late_in_second(env):
    # As above, with the run started 0.9 s into a second, and each bad reply
    # delayed 0.3 s, so the late clients' retries fire 3.3 s and 3.6 s into
    # the run. finished() compares whole seconds under --clients-start and
    # is already true then, 0.6 s before the deadline: the retry must not
    # let these clients count as done.
    node = _Node(bad_slots_replies=1000, first_good=True, bad_delay=0.3)
    returncode, stderr, elapsed = _run(node, ["--test-time=4", "--clients-start=1", "--clients-step=1",
                                              "--step-duration=1"], clients=3, start_fraction=0.9)
    env.assertEqual(node.errors, [])
    env.assertNotEqual(returncode, "timeout", message=stderr[-2000:])
    env.assertNotEqual(returncode, 0, message=stderr[-2000:])
    env.assertContains("CLUSTER SLOTS error and --reconnect-on-error is not set, ending this thread", stderr)
    env.assertLess(elapsed, 12)


def test_cluster_slots_empty_reply_for_one_client_honours_test_time(env):
    node = _Node(bad_slots_replies=1000, first_good=True)
    returncode, stderr, elapsed = _run(node, ["--test-time=2", "--reconnect-on-error"], workers=2)
    env.assertEqual(node.errors, [])
    env.assertNotEqual(returncode, "timeout", message=stderr[-2000:])
    env.assertNotEqual(returncode, 0)
    env.assertContains("CLUSTER SLOTS error, attempting reconnection", stderr)
    env.assertContains("Connection setup failed and the run is over, not reconnecting.", stderr)
    env.assertLess(elapsed, 10)


def test_cluster_slots_transient_empty_reply_for_one_client_is_retried(env):
    # The second client's CLUSTER SLOTS gets one empty reply, late enough
    # that the first client is already running. It is asked again a second
    # later and the run completes.
    node = _Node(bad_slots_replies=1, first_good=True, bad_delay=0.5)
    returncode, stderr, elapsed = _run(node, ["--test-time=3"], clients=2)
    env.assertEqual(node.errors, [])
    env.assertEqual(returncode, 0, message=stderr[-2000:])
    env.assertContains("cluster slot failed", stderr)
    env.assertFalse("ending this thread" in stderr, message=stderr[-2000:])
    env.assertEqual(node.slots_requests, 3)


def test_rejected_setup_at_startup_is_bounded_by_connection_stage_timeout(env):
    # No connection ever reaches steady state, so --connection-stage-timeout
    # (exit 2), not the --test-time deadline, ends the reconnects.
    node = _Node(bad_slots_replies=0,
                 auth_error=b"-ERR AUTH <password> called without any password configured for the default user\r\n")
    returncode, stderr, elapsed = _run(node, ["-a", "wrongpw", "--reconnect-on-error", "--test-time=1",
                                              "--connection-stage-timeout=5"], cluster=False)
    env.assertEqual(node.errors, [])
    env.assertEqual(returncode, 2, message=stderr[-2000:])
    env.assertContains("See --connection-stage-timeout.", stderr)
    env.assertGreaterEqual(elapsed, 4)


# Every shard fails validation: the slot range is out of bounds, or the node
# has no port. The reply is not empty, so it takes a different path.
MALFORMED_RANGE = b"*1\r\n*3\r\n:0\r\n:16384\r\n*2\r\n$9\r\n127.0.0.1\r\n:1\r\n"
MALFORMED_NODE = b"*1\r\n*3\r\n:0\r\n:16383\r\n*1\r\n$9\r\n127.0.0.1\r\n"


def test_cluster_slots_malformed_reply_mid_run_is_retried(env):
    node = _Node(bad_slots_replies=2, moved_after=50, bad_reply=MALFORMED_RANGE)
    returncode, stderr, _ = _run(node, ["--requests=200", "--reconnect-on-error"])
    env.assertEqual(node.errors, [])
    env.assertEqual(returncode, 0, message=stderr[-2000:])
    env.assertContains("every shard in the reply was malformed", stderr)
    env.assertEqual(node.slots_requests, 4)
    env.assertGreaterEqual(node.data_requests, 200)


def test_cluster_slots_malformed_reply_at_startup_is_retried(env):
    node = _Node(bad_slots_replies=2, bad_reply=MALFORMED_NODE)
    returncode, stderr, elapsed = _run(node, ["--test-time=2", "--pipeline=4"])
    _assert_startup_retry_not_charged(env, node, returncode, stderr, elapsed)


def test_cluster_slots_malformed_reply_mid_run_honours_test_time(env):
    node = _Node(bad_slots_replies=1000, moved_after=50, bad_reply=MALFORMED_RANGE)
    returncode, stderr, elapsed = _run(node, ["--test-time=2", "--reconnect-on-error"])
    env.assertEqual(node.errors, [])
    env.assertNotEqual(returncode, "timeout", message=stderr[-2000:])
    env.assertNotEqual(returncode, 0)
    env.assertContains("CLUSTER SLOTS error, attempting reconnection", stderr)
    env.assertLess(elapsed, 10)


def test_cluster_slots_empty_reply_at_startup_still_aborts(env):
    # A node that never returns a slot map: memtier keeps asking (master
    # asked once), and --connection-stage-timeout still ends the run with
    # exit 2. A short --test-time does not end it first: with no reply the
    # run has no duration, whether or not the retries reset its clock.
    node = _Node(bad_slots_replies=1000)
    returncode, stderr, _ = _run(node, ["--test-time=1", "--connection-stage-timeout=5"])
    env.assertEqual(node.errors, [])
    env.assertEqual(returncode, 2, message=stderr[-2000:])
    env.assertContains("See --connection-stage-timeout.", stderr)
    env.assertGreater(node.slots_requests, 1)
