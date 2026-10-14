"""Accounting for a request dropped after -READONLY (#592).

A dropped request must still be counted as an op, with its bytes and
latency, and logged to --failed-keys-file. The handler drops at two sites,
and each test drives one:

- "stale": a single scripted peer A names itself the only primary and
  answers every SET with -READONLY. The map still names the rejecting
  connection as the primary, so memtier refreshes and drops.
- "replica": CLUSTER SLOTS names B the primary for every slot and A its
  replica. The write is a --command marked --command-is-read, so
  --read-preference=secondary sends it to A, which answers -READONLY.
  memtier re-routes it to B, a different live connection, but
  --max-retries=0 refuses the retry. A few writes can reach B through
  --read-preference-fallback=primary before A is ready; B accepts those.
"""
import json
import os
import socketserver
import subprocess
import tempfile
import threading
import time

from include import MEMTIER_BINARY
from test_hdr_histogram import _decode_v2

_READONLY_REPLY = b"-READONLY You can't write against a read only replica.\r\n"
_OK_REPLY = b"+OK\r\n"
# Each SET reply is delayed by this much, so the recorded latency has a floor.
_REPLY_DELAY_MS = 5


def _bulk(value):
    return b"$%d\r\n%s\r\n" % (len(value), value)


def _node(port, node_id):
    return b"*3\r\n" + _bulk(b"127.0.0.1") + b":%d\r\n" % port + _bulk(node_id * 40)


class _Pair(object):
    """A answers SET with -READONLY; B, the slot primary in "replica" mode, accepts it."""

    def __init__(self, mode):
        self.mode = mode
        self.lock = threading.Lock()
        self.readonly = 0  # SETs A rejected
        self.accepted = 0  # SETs B accepted
        self.rx = 0
        self.tx = 0
        self.errors = []
        self.a = _Peer(self, "A")
        self.b = _Peer(self, "B")

    def slots_reply(self):
        if self.mode == "stale":
            return b"*1\r\n*3\r\n:0\r\n:16383\r\n" + _node(self.a.server_address[1], b"a")
        return (b"*1\r\n*4\r\n:0\r\n:16383\r\n" + _node(self.b.server_address[1], b"b") +
                _node(self.a.server_address[1], b"a"))

    def set_reply(self, name, wire):
        reply = _READONLY_REPLY if name == "A" else _OK_REPLY
        with self.lock:
            if name == "A":
                self.readonly += 1
            else:
                self.accepted += 1
            self.rx += len(reply)
            self.tx += len(wire)
        return reply

    def __enter__(self):
        self.threads = [threading.Thread(target=peer.serve_forever, daemon=True) for peer in (self.a, self.b)]
        for thread in self.threads:
            thread.start()
        return self

    def __exit__(self, *exc):
        for peer in (self.a, self.b):
            peer.shutdown()
            peer.server_close()
        for thread in self.threads:
            thread.join(timeout=5)


class _Peer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, pair, name):
        super().__init__(("127.0.0.1", 0), _Handler)
        self.pair = pair
        self.name = name


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        pair = self.server.pair
        try:
            while True:
                line = self.rfile.readline()
                if not line:
                    return
                wire = line
                args = []
                for _ in range(int(line[1:])):
                    size = self.rfile.readline()
                    value = self.rfile.read(int(size[1:]) + 2)
                    wire += size + value
                    args.append(value[:-2])
                verb = args[0].upper()
                if verb == b"CLUSTER":
                    reply = pair.slots_reply()
                elif verb == b"SET":
                    time.sleep(_REPLY_DELAY_MS / 1000.0)
                    reply = pair.set_reply(self.server.name, wire)
                elif verb in (b"HELLO", b"AUTH", b"SELECT", b"CLIENT", b"READONLY"):
                    reply = _OK_REPLY
                elif verb == b"COMMAND":
                    reply = b"*0\r\n"
                else:
                    raise AssertionError("unexpected command: {!r}".format(args))
                self.wfile.write(reply)
                self.wfile.flush()
        except (ConnectionResetError, BrokenPipeError, ValueError):
            pass  # memtier may close with a topology refresh still in flight.
        except Exception as error:
            pair.errors.append(repr(error))


def _check_dropped_readonly_counted(env, mode):
    requests = 20
    with _Pair(mode) as pair, tempfile.TemporaryDirectory() as directory:
        path = os.path.join(directory, "stats.json")
        failed_keys = os.path.join(directory, "failed-keys.csv")
        args = [MEMTIER_BINARY, "--server=127.0.0.1",
                "--port={}".format(pair.a.server_address[1]), "--cluster-mode",
                "--threads=1", "--clients=1", "--pipeline=1",
                "--requests={}".format(requests), "--key-minimum=1", "--key-maximum=1",
                "--retry-on-error", "--max-retries=0",
                "--failed-keys-file=" + failed_keys,
                "--hide-histogram", "--json-out-file=" + path]
        if mode == "stale":
            args.append("--ratio=1:0")
        else:
            args += ["--command=SET __key__ value", "--command-is-read",
                     "--read-preference=secondary", "--read-preference-fallback=primary"]
        result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=30, text=True)
        env.assertEqual(result.returncode, 0, message=mode + "\n" + result.stderr)
        env.assertEqual(pair.errors, [], message=mode)
        if result.returncode:
            return
        # Every request is sent once: rejected by A or accepted by B. How many
        # reach B before A is ready depends on timing; at least one must be
        # rejected by A for the drop to be exercised.
        env.assertEqual(pair.readonly + pair.accepted, requests, message=mode)
        env.assertGreaterEqual(pair.readonly, requests if mode == "stale" else 1, message=mode)

        with open(failed_keys) as source:
            rows = source.readlines()[1:]
        env.assertEqual(len(rows), pair.readonly, message=mode + " failed-keys rows")
        for row in rows:
            env.assertContains("READONLY", row)

        with open(path) as source:
            stats = json.load(source)["ALL STATS"]
        for name in ("Sets", "Totals"):
            entry = stats[name]
            env.assertEqual(entry["Count"], requests, message=mode + " " + name)
            histogram = entry["Percentile Latencies"]["Histogram log format"]["Compressed Histogram"]
            env.assertEqual(_decode_v2(histogram)[0], requests, message=mode + " " + name + " histogram")
            # Loose floor: well below the peers' reply delay, far above zero.
            for latency in ("Min Latency", "Average Latency"):
                env.assertGreaterEqual(entry[latency], _REPLY_DELAY_MS * 0.5,
                                       message=mode + " " + name + " " + latency)
            env.assertGreater(entry["Ops/sec"], 0, message=mode + " " + name + " Ops/sec")
        if mode == "stale":
            env.assertEqual(stats["Gets"]["Count"], 0)
        series = stats["Sets"]["Time-Serie"].values()
        env.assertEqual(sum(second["Count"] for second in series), requests)
        env.assertEqual(sum(second["Bytes RX"] for second in series), pair.rx)
        env.assertEqual(sum(second["Bytes TX"] for second in series), pair.tx)


def test_readonly_stale_primary_drop_counted(env):
    """Drop site 1: no live primary other than the rejecting connection."""
    _check_dropped_readonly_counted(env, "stale")


def test_readonly_refused_retry_counted(env):
    """Drop site 2: the retry to the slot primary is refused."""
    _check_dropped_readonly_counted(env, "replica")
