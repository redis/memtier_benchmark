"""A request retried after -READONLY is answered once, by the slot primary.

With --retry-on-error, cluster_client::handle_response resends a request
rejected with -READONLY to the slot's current primary, when that is a live
connection other than the one that rejected it. The tests in
test_cluster_readonly_drop.py cover the cases where it cannot (#592, #651).
These cover the resend itself, under RESP2 and RESP3, with scripted peers
A, B, C and D and no failover timing. C and D each own one slot that no key
maps to. C is the seed, and D comes last in the map, so their connections
are first and last; they are up but must get no SET, and a resend to a
connection other than the slot primary B fails the tests.

- "stale": the map names A the primary of every slot but one, which B owns.
  B answers the first SET it gets with -CLUSTERDOWN, as during a failover,
  and memtier refreshes the map through B. The new map makes B the primary
  of every slot and A its replica. A holds the SETs it has, reading on until
  memtier sends it READONLY: memtier only does that once it has applied the
  new map. A then answers them with -READONLY, and memtier must resend them
  to B. Keys memtier queued for A's connection before the refresh still go
  to A after it; A rejects those too, and they must be resent to B as well.
- "replica": the map names B the primary and A its replica. The workload is
  a SET marked --command-is-read, so --read-preference=secondary sends each
  one to A, which answers -READONLY. memtier must resend it to B, whatever
  --read-preference says. --read-preference-fallback=queue holds the
  requests until A is ready, so every one reaches A first.

Each key is a separate request. The tests check that every key is accepted
exactly once, by B, after at most one rejection by A, and none reaches C or D;
that the JSON counts
each request once and every resend as a retry, with no errors; and that the
--requests run ends with exit code 0.
"""
import collections
import json
import os
import socketserver
import subprocess
import tempfile
import threading
import time

from include import MEMTIER_BINARY
from test_hdr_histogram import _decode_v2

_OK_REPLY = b"+OK\r\n"
_READONLY_REPLY = b"-READONLY You can't write against a read only replica.\r\n"
_CLUSTERDOWN_REPLY = b"-CLUSTERDOWN The cluster is down\r\n"
_WAIT_SECONDS = 20
# In "replica" mode A delays each -READONLY by this much, so the latency of a
# request answered by B has a floor only if it spans both attempts.
_REPLY_DELAY_MS = 5
_PIPELINE = 4
_REQUESTS = 12


def _crc16(data):
    crc = 0
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc


def _key(index):
    return b"memtier-%d" % index


def _slot(key):
    return _crc16(key) % 16384


def _bulk(value):
    return b"$%d\r\n%s\r\n" % (len(value), value)


def _node(port, node_id):
    return b"*3\r\n" + _bulk(b"127.0.0.1") + b":%d\r\n" % port + _bulk(node_id * 40)


def _range(first, last, *nodes):
    return b"*%d\r\n:%d\r\n:%d\r\n" % (2 + len(nodes), first, last) + b"".join(nodes)


def _slots_reply(owners, rest):
    """A CLUSTER SLOTS reply: each slot in owners goes to its nodes, every
    other slot to the nodes in rest."""
    ranges = []
    first = 0
    for slot in sorted(owners):
        if slot > first:
            ranges.append(_range(first, slot - 1, *rest))
        ranges.append(_range(slot, slot, *owners[slot]))
        first = slot + 1
    if first <= 16383:
        ranges.append(_range(first, 16383, *rest))
    return b"*%d\r\n" % len(ranges) + b"".join(ranges)


# C and D own the lowest and the highest slot that no key maps to.
_FREE_SLOTS = set(range(16384)) - {_slot(_key(i)) for i in range(1, _REQUESTS + 1)}
_C_SLOT = min(_FREE_SLOTS)
_D_SLOT = max(_FREE_SLOTS)


class _Cluster(object):
    def __init__(self, mode):
        self.mode = mode
        self.lock = threading.Lock()
        self.a = _Peer(self, "A")
        self.b = _Peer(self, "B")
        self.c = _Peer(self, "C")
        self.d = _Peer(self, "D")
        self.failed_over = False  # "stale": B answered -CLUSTERDOWN, the map is the new one
        self.a_got_set = threading.Event()
        self.sets = {name: collections.Counter() for name in "ABCD"}
        self.cluster_slots = {name: 0 for name in "ABCD"}
        self.readonly_cmds = {name: 0 for name in "ABCD"}
        self.clusterdown = 0
        self.held = 0  # SETs A held until READONLY
        self.late = 0  # SETs A got after READONLY
        self.hello = []  # HELLO protocol versions
        self.errors = []

    def slots_reply(self):
        a = _node(self.a.server_address[1], b"a")
        b = _node(self.b.server_address[1], b"b")
        decoys = {_C_SLOT: (_node(self.c.server_address[1], b"c"),),
                  _D_SLOT: (_node(self.d.server_address[1], b"d"),)}
        if self.mode == "replica" or self.failed_over:
            return _slots_reply(decoys, (b, a))
        # Before the failover: B owns only the slot of the first key.
        decoys[_slot(_key(1))] = (b,)
        return _slots_reply(decoys, (a,))

    def __enter__(self):
        self.peers = (self.a, self.b, self.c, self.d)
        self.threads = [threading.Thread(target=peer.serve_forever, daemon=True) for peer in self.peers]
        for thread in self.threads:
            thread.start()
        return self

    def __exit__(self, *exc):
        for peer in self.peers:
            peer.shutdown()
            peer.server_close()
        for thread in self.threads:
            thread.join(timeout=5)


class _Peer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, cluster, name):
        super().__init__(("127.0.0.1", 0), _Handler)
        self.cluster = cluster
        self.name = name


class _Handler(socketserver.StreamRequestHandler):
    def read_command(self):
        line = self.rfile.readline()
        if not line:
            return None
        assert line.startswith(b"*"), line
        args = []
        for _ in range(int(line[1:])):
            size = self.rfile.readline()
            assert size.startswith(b"$"), size
            args.append(self.rfile.read(int(size[1:])))
            assert self.rfile.read(2) == b"\r\n"
        return args

    def handle(self):
        cluster = self.server.cluster
        name = self.server.name
        held = []  # SETs A has not answered yet ("stale")
        try:
            while True:
                args = self.read_command()
                if args is None:
                    return
                verb = args[0].upper()
                if verb == b"CLUSTER":
                    with cluster.lock:
                        cluster.cluster_slots[name] += 1
                        reply = cluster.slots_reply()
                elif verb == b"SET":
                    reply = self.handle_set(args[1], held)
                    if reply is None:
                        continue
                elif verb == b"READONLY":
                    with cluster.lock:
                        cluster.readonly_cmds[name] += 1
                    # memtier sends READONLY on A only once it applied a map
                    # naming A a replica: answer the SETs read before it.
                    reply = _READONLY_REPLY * len(held) + _OK_REPLY
                    with cluster.lock:
                        cluster.held += len(held)
                    del held[:]
                elif verb == b"HELLO":
                    with cluster.lock:
                        cluster.hello.append(args[1:2])
                    reply = _OK_REPLY
                elif verb in (b"AUTH", b"SELECT", b"CLIENT"):
                    reply = _OK_REPLY
                elif verb == b"COMMAND":
                    reply = b"*0\r\n"
                else:
                    raise AssertionError("unexpected command: {!r}".format(args))
                self.wfile.write(reply)
                self.wfile.flush()
        except (ConnectionResetError, BrokenPipeError):
            pass  # memtier may close with a topology refresh still in flight.
        except Exception as error:
            cluster.errors.append("{}: {!r}".format(name, error))

    def handle_set(self, key, held):
        """Returns the reply to the SET, or None to hold it."""
        cluster = self.server.cluster
        name = self.server.name
        with cluster.lock:
            cluster.sets[name][key] += 1
            failed_over = cluster.failed_over
        if name in ("C", "D"):
            return _OK_REPLY  # the tests fail on any SET C or D gets
        if name == "B":
            if cluster.mode == "stale" and not failed_over:
                # Fail over only once A holds a SET, so at least one is
                # rejected by A after the new map is applied.
                if not cluster.a_got_set.wait(_WAIT_SECONDS):
                    cluster.errors.append("A never got a SET")
                with cluster.lock:
                    cluster.failed_over = True
                    cluster.clusterdown += 1
                return _CLUSTERDOWN_REPLY
            return _OK_REPLY
        if cluster.mode == "replica":
            time.sleep(_REPLY_DELAY_MS / 1000.0)
            return _READONLY_REPLY
        if cluster.readonly_cmds["A"]:
            # Sent after memtier made A a replica: keys it had already
            # staged for A's connection still go there. A rejects them, and
            # memtier must resend them to B too.
            with cluster.lock:
                cluster.late += 1
            return _READONLY_REPLY
        held.append(key)
        cluster.a_got_set.set()
        return None


def _run(env, mode, protocol):
    label = "{} {}".format(mode, protocol)
    with _Cluster(mode) as cluster, tempfile.TemporaryDirectory() as directory:
        path = os.path.join(directory, "stats.json")
        failed_keys = os.path.join(directory, "failed-keys.csv")
        args = [MEMTIER_BINARY, "--server=127.0.0.1",
                "--port={}".format(cluster.c.server_address[1]), "--cluster-mode",
                "--protocol=" + protocol,
                "--threads=1", "--clients=1", "--pipeline={}".format(_PIPELINE),
                "--requests={}".format(_REQUESTS),
                "--key-minimum=1", "--key-maximum={}".format(_REQUESTS),
                "--retry-on-error", "--max-retries=3",
                "--failed-keys-file=" + failed_keys,
                "--hide-histogram", "--json-out-file=" + path]
        if mode == "stale":
            args += ["--ratio=1:0", "--key-pattern=S:S"]
        else:
            args += ["--command=SET __key__ value", "--command-key-pattern=S", "--command-is-read",
                     "--read-preference=secondary", "--read-preference-fallback=queue"]
        try:
            result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=60, text=True)
        except subprocess.TimeoutExpired as error:
            env.assertTrue(False, message=label + ": --requests run did not end\n" + str(error.stderr))
            return
        env.assertEqual(result.returncode, 0, message=label + "\n" + result.stderr)
        env.assertEqual(cluster.errors, [], message=label)
        if result.returncode:
            return

        # All four connections negotiate the requested protocol.
        version = [protocol[-1:].encode()]
        env.assertEqual(cluster.hello, [version] * len(cluster.hello), message=label + " HELLO")
        env.assertGreaterEqual(len(cluster.hello), 4, message=label + " HELLO")
        # No key maps to C's or D's slot: a SET there is a resend that went
        # to a connection other than the slot primary.
        env.assertEqual(dict(cluster.sets["C"]), {}, message=label + " C")
        env.assertEqual(dict(cluster.sets["D"]), {}, message=label + " D")

        keys = [_key(i) for i in range(1, _REQUESTS + 1)]
        a_sets = cluster.sets["A"]
        b_sets = cluster.sets["B"]
        # A rejects each SET it gets. None is sent to A twice: the resend
        # goes to B.
        env.assertEqual(max(a_sets.values() or [0]), 1, message=label + " A: " + str(a_sets))
        env.assertEqual(set(a_sets) - set(keys), set(), message=label)
        if mode == "stale":
            # The first key went to B, which answered -CLUSTERDOWN and then
            # accepted the retry; every SET A got was rejected after READONLY:
            # held until it, or sent after it.
            env.assertEqual(cluster.clusterdown, 1, message=label)
            env.assertGreaterEqual(len(a_sets), 1, message=label)
            env.assertGreaterEqual(cluster.held, 1, message=label)
            env.assertEqual(cluster.held + cluster.late, len(a_sets), message=label)
            env.assertEqual(cluster.readonly_cmds["A"], 1, message=label)
            env.assertGreaterEqual(cluster.cluster_slots["B"], 1, message=label)
            expected_b = collections.Counter(keys)
            expected_b[_key(1)] += 1
            retries = 1 + len(a_sets)
        else:
            env.assertEqual(sorted(a_sets), sorted(keys), message=label)
            expected_b = collections.Counter(keys)
            retries = _REQUESTS
        # B accepts every key exactly once (the first key in "stale" mode is
        # also sent once before, and rejected with -CLUSTERDOWN).
        env.assertEqual(dict(b_sets), dict(expected_b), message=label)

        with open(path) as source:
            stats = json.load(source)["ALL STATS"]
        totals = stats["Totals"]
        env.assertEqual(stats["Sets"]["Count"], _REQUESTS, message=label + " Sets")
        env.assertEqual(totals["Count"], _REQUESTS, message=label + " Totals")
        if mode == "stale":
            env.assertEqual(stats["Gets"]["Count"], 0, message=label + " Gets")
        env.assertEqual(totals["Errors"], 0, message=label + " Errors")
        env.assertEqual(totals["Retry Attempts"], retries, message=label + " Retry Attempts")
        env.assertEqual(totals["Retried Ops"], retries, message=label + " Retried Ops")
        env.assertEqual(totals["MOVED/sec"], 0, message=label + " MOVED/sec")
        env.assertEqual(totals["Connection Errors"], 0, message=label + " Connection Errors")
        histogram = stats["Sets"]["Percentile Latencies"]["Histogram log format"]["Compressed Histogram"]
        env.assertEqual(_decode_v2(histogram)[0], _REQUESTS, message=label + " histogram")
        if mode == "replica":
            # Latency runs from the first attempt, so it includes A's delay.
            env.assertGreaterEqual(stats["Sets"]["Min Latency"], _REPLY_DELAY_MS * 0.5,
                                   message=label + " Min Latency")
        # A request that succeeds on retry is not a failed key.
        with open(failed_keys) as source:
            rows = source.readlines()[1:]
        env.assertEqual(rows, [], message=label + " failed keys")
        series = stats["Sets"]["Time-Serie"].values()
        env.assertEqual(sum(second["Count"] for second in series), _REQUESTS, message=label + " Time-Serie")


def test_readonly_retry_stale_primary_resp2(env):
    _run(env, "stale", "resp2")


def test_readonly_retry_stale_primary_resp3(env):
    _run(env, "stale", "resp3")


def test_readonly_retry_replica_read_resp2(env):
    _run(env, "replica", "resp2")


def test_readonly_retry_replica_read_resp3(env):
    _run(env, "replica", "resp3")
