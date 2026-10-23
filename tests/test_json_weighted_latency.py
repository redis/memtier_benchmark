"""Issue #671: aggregate JSON averages samples, not per-run averages."""
import json
import os
import socketserver
import subprocess
import tempfile
import threading
import time

from include import MEMTIER_BINARY


class _LatencyPeer(socketserver.ThreadingTCPServer):
    allow_reuse_address = False
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), _LatencyHandler)
        self.lock = threading.Lock()
        self.connections = 0
        self.errors = []


class _LatencyHandler(socketserver.StreamRequestHandler):
    def handle(self):
        with self.server.lock:
            self.server.connections += 1
            run = self.server.connections
        authenticated = False
        try:
            while True:
                line = self.rfile.readline()
                if not line:
                    return
                assert line.startswith(b"*")
                command = []
                for _ in range(int(line[1:])):
                    header = self.rfile.readline()
                    assert header.startswith(b"$")
                    command.append(self.rfile.read(int(header[1:])))
                    assert self.rfile.read(2) == b"\r\n"
                if command[0] == b"AUTH":
                    assert command[1:] == [b"dummy-671"]
                    authenticated = True
                    reply = b"+OK\r\n"
                else:
                    assert authenticated
                    assert command[0] in (b"GET", b"SET")
                    # Two timed runs have deliberately different sample counts
                    # and latencies. Assertions use observed sums, never these
                    # requested sleep durations as a timing oracle.
                    if run != 1:
                        time.sleep(0.05)
                    reply = b"$-1\r\n" if command[0] == b"GET" else b"+OK\r\n"
                self.wfile.write(reply)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as error:
            self.server.errors.append(repr(error))


def test_json_aggregate_latency_weights_samples(env):
    if env.isCluster() or env.useTLS:
        env.skip()  # The fixture owns its plaintext peer and authentication.
    for breakdown, expected_rows in (("command", 2), ("line", 3)):
        with _LatencyPeer() as peer, tempfile.TemporaryDirectory() as directory:
            thread = threading.Thread(target=peer.serve_forever)
            thread.start()
            try:
                path = os.path.join(directory, "stats.json")
                command = [MEMTIER_BINARY, "-s", "127.0.0.1", "-p", str(peer.server_address[1]),
                           "--authenticate=dummy-671", "--threads=1", "--clients=1", "--pipeline=1",
                           "--test-time=1", "--run-count=2", "--print-all-runs", "--hide-histogram",
                           "--command=GET a", "--command=GET b", "--command=SET a b",
                           "--command-stats-breakdown=" + breakdown, "--json-out-file=" + path]
                result = subprocess.run(command, capture_output=True, text=True, timeout=20)
                env.assertEqual(result.returncode, 0, message=result.stderr)
                with open(path) as source:
                    document = json.load(source)
            finally:
                peer.shutdown()
                thread.join(timeout=3)
            env.assertEqual(peer.errors, [])
            env.assertEqual(peer.connections, 2)
        runs = [document[f"RUN #{i} RESULTS"] for i in (1, 2)]
        aggregate = document[next(key for key in document if key.startswith("AGGREGATED AVERAGE RESULTS"))]
        names = [name for name, row in aggregate.items() if isinstance(row, dict) and "Count" in row]
        env.assertEqual(len(names), expected_rows + 1)  # Includes Totals.
        # Guard the discrimination itself: equal-sized runs cannot expose the
        # historical mean-of-means error. The slow peer gives ample margin.
        env.assertGreater(runs[0]["Totals"]["Count"], 2 * runs[1]["Totals"]["Count"])
        for name in names:
            count = sum(run[name]["Count"] for run in runs)
            accumulated = sum(run[name]["Accumulated Latency"] for run in runs)
            actual = aggregate[name]["Latency"]
            env.assertEqual(actual, aggregate[name]["Average Latency"])
            if breakdown == "command" and name != "Totals":
                # Grouped rows already use a sample-weighted HDR mean. Retain
                # that convention; both individual and aggregate means print
                # to .001ms, so their combined rounding error is at most .001ms.
                expected = sum(run[name]["Latency"] * run[name]["Count"] for run in runs) / count
                rounding = 0.001 + 1e-9  # Include floating-point comparison noise.
                lower, upper = expected - rounding, expected + rounding
            else:
                # Each real run's accumulated latency truncates less than 1ms;
                # averages print to .001ms. The aggregate's independently rounded
                # Count/Accumulated are deliberately not used as the oracle.
                lower = accumulated / count - 0.0005
                upper = (accumulated + len(runs)) / count + 0.0005
            env.assertGreaterEqual(actual, lower, message=f"{breakdown}/{name}: {actual} < {lower}")
            env.assertLessEqual(actual, upper, message=f"{breakdown}/{name}: {actual} > {upper}")
            env.assertAlmostEqual(aggregate[name]["Ops/sec"],
                                  sum(run[name]["Ops/sec"] for run in runs) / 2, 0.02)
        rows = [name for name in names if name != "Totals"]
        for name in rows:
            total = sum(run[name]["Count"] for run in runs)
            # mean_count() rounds to nearest, retaining one for nonzero totals.
            env.assertEqual(aggregate[name]["Count"], max(1, (total + 1) // 2))
        env.assertEqual(aggregate["Totals"]["Count"], sum(aggregate[name]["Count"] for name in rows))
