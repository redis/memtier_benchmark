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
import tempfile

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
