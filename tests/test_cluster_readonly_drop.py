"""Accounting for a -READONLY reply whose primary retry is refused (#592).

A scripted RESP peer names itself the only primary and answers every SET with
-READONLY. memtier re-routes such a request to the slot primary, here the same
live connection, but --max-retries=0 refuses the retry, so every request is
dropped as a terminal error. Each one must still be counted as an op, with
its bytes and latency, and logged to --failed-keys-file.
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
# Each -READONLY is delayed by this much, so the recorded latency has a floor.
_REPLY_DELAY_MS = 5


class _ReadonlyPeer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), _ReadonlyHandler)
        self.readonly = 0
        self.rx = 0
        self.tx = 0
        self.errors = []


class _ReadonlyHandler(socketserver.StreamRequestHandler):
    def handle(self):
        try:
            while True:
                line = self.rfile.readline()
                if not line:
                    return
                wire = line
                if line.startswith(b"*"):
                    args = []
                    for _ in range(int(line[1:])):
                        size = self.rfile.readline()
                        value = self.rfile.read(int(size[1:]) + 2)
                        wire += size + value
                        args.append(value[:-2])
                else:
                    args = line.strip().split()
                verb = args[0].upper()
                if verb == b"CLUSTER":
                    port = self.server.server_address[1]
                    reply = (b"*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n"
                             b"$9\r\n127.0.0.1\r\n:" + str(port).encode() + b"\r\n")
                elif verb == b"SET":
                    self.server.readonly += 1
                    self.server.rx += len(_READONLY_REPLY)
                    self.server.tx += len(wire)
                    time.sleep(_REPLY_DELAY_MS / 1000.0)
                    reply = _READONLY_REPLY
                elif verb in (b"HELLO", b"AUTH", b"SELECT", b"CLIENT", b"READONLY"):
                    reply = b"+OK\r\n"
                elif verb == b"COMMAND":
                    reply = b"*0\r\n"
                else:
                    raise AssertionError("unexpected command: {!r}".format(args))
                self.wfile.write(reply)
                self.wfile.flush()
        except (ConnectionResetError, BrokenPipeError):
            pass  # memtier may close with a topology refresh still in flight.
        except Exception as error:
            self.server.errors.append(repr(error))


def test_readonly_refused_retry_counted(env):
    requests = 20
    with _ReadonlyPeer() as peer, tempfile.TemporaryDirectory() as directory:
        thread = threading.Thread(target=peer.serve_forever, daemon=True)
        thread.start()
        path = os.path.join(directory, "stats.json")
        failed_keys = os.path.join(directory, "failed-keys.csv")
        args = [MEMTIER_BINARY, "--server=127.0.0.1",
                "--port={}".format(peer.server_address[1]), "--cluster-mode",
                "--threads=1", "--clients=1", "--pipeline=1", "--ratio=1:0",
                "--requests={}".format(requests), "--key-minimum=1", "--key-maximum=1",
                "--retry-on-error", "--max-retries=0",
                "--failed-keys-file=" + failed_keys,
                "--hide-histogram", "--json-out-file=" + path]
        try:
            result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=30, text=True)
        finally:
            peer.shutdown()
            thread.join(timeout=5)
        env.assertEqual(result.returncode, 0, message=result.stderr)
        env.assertEqual(peer.errors, [])
        if result.returncode:
            return
        env.assertEqual(peer.readonly, requests)

        with open(failed_keys) as source:
            rows = source.readlines()[1:]
        env.assertEqual(len(rows), requests, message="failed-keys rows")
        for row in rows:
            env.assertContains("READONLY", row)

        with open(path) as source:
            stats = json.load(source)["ALL STATS"]
        for name in ("Sets", "Totals"):
            entry = stats[name]
            env.assertEqual(entry["Count"], requests, message=name)
            histogram = entry["Percentile Latencies"]["Histogram log format"]["Compressed Histogram"]
            env.assertEqual(_decode_v2(histogram)[0], requests, message=name + " histogram")
            # Loose floor: well below the peer's reply delay, far above zero.
            for latency in ("Min Latency", "Average Latency"):
                env.assertGreaterEqual(entry[latency], _REPLY_DELAY_MS * 0.5, message=name + " " + latency)
            env.assertGreater(entry["Ops/sec"], 0, message=name + " Ops/sec")
        env.assertEqual(stats["Gets"]["Count"], 0)
        series = stats["Sets"]["Time-Serie"].values()
        env.assertEqual(sum(second["Count"] for second in series), requests)
        env.assertEqual(sum(second["Bytes RX"] for second in series), peer.rx)
        env.assertEqual(sum(second["Bytes TX"] for second in series), peer.tx)
