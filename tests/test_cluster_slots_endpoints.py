"""CLUSTER SLOTS empty/null hosts use the actual replying endpoint (#684)."""
import json
import os
import tempfile

from test_cluster_slots_bad_reply import _Node, _run


def _bulk(value):
    return b"$%d\r\n%s\r\n" % (len(value), value)


def _endpoint(host, node):
    return b"*2\r\n" + host + b":%d\r\n" % node.server_address[1]


def _slots(primary, replicas=(), start=0, end=16383):
    return (b"*1\r\n*%d\r\n:%d\r\n:%d\r\n" % (3 + len(replicas), start, end) +
            primary + b"".join(replicas))


def _check(env, node, extra=(), success=True):
    with tempfile.TemporaryDirectory() as directory:
        path = os.path.join(directory, "result.json")
        rc, stderr, _ = _run(node, ["--requests=20", "--ratio=1:0", "--authenticate=dummy-684",
                                   "--connection-stage-timeout=2", "--json-out-file=" + path] + list(extra),
                             timeout=8)
        for peer in [node] + node.extra + ([node.replica] if node.replica else []):
            env.assertEqual(peer.errors, [], message=stderr[-2000:])
        if success:
            env.assertEqual(rc, 0, message=stderr[-2000:])
            with open(path) as stream:
                result = json.load(stream)
            env.assertEqual(result["ALL STATS"]["Totals"]["Count"], 20)
        else:
            env.assertNotEqual(rc, 0, message=stderr[-2000:])
            env.assertNotEqual(rc, "timeout", message=stderr[-2000:])
        return stderr


def test_cluster_slots_empty_and_null_primary_hosts(env):
    for protocol, host in [("redis", _bulk(b"")), ("redis", b"$-1\r\n"),
                           ("resp3", _bulk(b"")), ("resp3", b"_\r\n"),
                           ("redis", _bulk(b"127.0.0.1"))]:
        node = _Node(0)
        node.slots_fn = lambda n: _slots(_endpoint(host, n))
        _check(env, node, ["--protocol=" + protocol])
        env.assertEqual(node.data_requests, 20)


def test_cluster_slots_empty_and_null_replica_hosts(env):
    for protocol, host in [("redis", _bulk(b"")), ("redis", b"$-1\r\n"),
                           ("resp3", b"_\r\n")]:
        replica = _Node(0)
        node = _Node(0, replica=replica)
        node.slots_fn = lambda n: _slots(_endpoint(_bulk(b"127.0.0.1"), n), [_endpoint(host, replica)])
        _check(env, node, ["--protocol=" + protocol, "--ratio=0:1", "--read-preference=secondary"])
        env.assertEqual(node.data_requests, 0)
        env.assertEqual(replica.data_requests, 20)
        env.assertGreaterEqual(replica.readonly_requests, 1)


def test_cluster_slots_refresh_uses_actual_sender_host_and_advertised_port(env):
    # The seed's 127.0.0.1 cannot reach target, which only binds 127.0.0.2.
    # Reusing the sender's port would send back to sender instead of target.
    for host in [_bulk(b""), b"$-1\r\n", b"_\r\n"]:
        target = _Node(0, host="127.0.0.2")
        sender = _Node(0, moved_after=1, host="127.0.0.2")
        seed = _Node(0)
        seed.extra = [sender, target]
        seed.slots_fn = lambda n: _slots(_endpoint(_bulk(b"127.0.0.2"), sender))
        sender.slots_fn = lambda n: _slots(_endpoint(host, target))
        _check(env, seed, ["--protocol=resp3", "--reconnect-on-error"])
        env.assertGreaterEqual(sender.slots_requests, 1)
        env.assertEqual(seed.data_requests, 0)
        env.assertGreaterEqual(target.data_requests, 19)


def test_cluster_slots_unknown_or_malformed_hosts_do_not_fall_back(env):
    for host in [_bulk(b"?"), _bulk(b"_"), _bulk(b"127.0.0.1\0"), b"*0\r\n"]:
        node = _Node(0)
        node.slots_fn = lambda n: _slots(_endpoint(host, n))
        _check(env, node, ["--protocol=resp3"], success=False)
        env.assertEqual(node.data_requests, 0)


def test_cluster_slots_normalized_primary_keeps_role_over_replica_alias(env):
    # First establish the later primary so the next reply's earlier replica
    # alias would otherwise rearm READONLY on that existing connection.
    later = _Node(0, moved_after=1)
    seed = _Node(0)
    seed.extra = [later]
    seed.slots_fn = lambda n: _slots(_endpoint(_bulk(b"127.0.0.1"), later))
    def refresh(n):
        first = _slots(_endpoint(_bulk(b"127.0.0.1"), seed),
                       [_endpoint(_bulk(b"127.0.0.1"), later)], 0, 8191)
        second = _slots(_endpoint(_bulk(b""), later), start=8192)
        return b"*2\r\n" + first[4:] + second[4:]
    later.slots_fn = refresh
    stderr = _check(env, seed, ["--reconnect-on-error"])
    env.assertContains("preserving primary role", stderr)
    env.assertEqual(later.readonly_requests, 0)
