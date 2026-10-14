"""Deterministic MOVED/ASK accounting, including failed and disabled retries.

A scripted RESP peer keeps the topology fixed while returning two redirects
before each successful command. This avoids timing a real cluster failover
and makes the terminal response bytes and histogram sample counts exact.
"""
import json
import os
import socketserver
import subprocess
import tempfile
import threading

from include import MEMTIER_BINARY
from test_hdr_histogram import _decode_v2


class _RedirectPeer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, redirect, mode):
        super().__init__(("127.0.0.1", 0), _RedirectHandler)
        self.redirect = redirect
        self.mode = mode
        self.attempts = 0
        self.redirects = 0
        self.terminal_rx = 0
        self.terminal_tx = 0
        self.errors = []


class _RedirectHandler(socketserver.StreamRequestHandler):
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
                elif verb in (b"SET", b"GET"):
                    self.server.attempts += 1
                    period = {"success": 3, "refused": 2, "disabled": 1, "transaction": 1}[self.server.mode]
                    terminal = self.server.attempts % period == 0
                    if self.server.mode == "success" and terminal:
                        reply = b"+OK\r\n" if verb == b"SET" else b"$3\r\nfoo\r\n"
                    else:
                        self.server.redirects += 1
                        reply = ("-{} 0 127.0.0.1:{}\r\n".format(
                            self.server.redirect, self.server.server_address[1])).encode()
                    if terminal:
                        self.server.terminal_rx += len(reply)
                        self.server.terminal_tx += len(wire)
                elif verb in (b"HELLO", b"AUTH", b"SELECT", b"CLIENT", b"READONLY", b"ASKING"):
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


def _check_redirect_stats(env, redirect, mode, command, backoff_ms=1, run_count=1):
    requests = 12
    with _RedirectPeer(redirect, mode) as peer, tempfile.TemporaryDirectory() as directory:
        thread = threading.Thread(target=peer.serve_forever, daemon=True)
        thread.start()
        path = os.path.join(directory, "stats.json")
        args = [MEMTIER_BINARY, "--server=127.0.0.1",
                "--port={}".format(peer.server_address[1]), "--cluster-mode",
                "--threads=1", "--clients=1", "--pipeline=1",
                "--requests={}".format(requests), "--key-minimum=1", "--key-maximum=1",
                "--hide-histogram", "--json-out-file=" + path]
        if command == "set":
            args.append("--ratio=1:0")
            name = "Sets"
        elif command == "get":
            args.append("--ratio=0:1")
            name = "Gets"
        else:
            verb = command.split("-")[1].upper()
            args.append("--command={} __key__{}".format(verb, " value" if verb == "SET" else ""))
            name = verb.capitalize() + "s"
        if mode != "disabled":
            args += ["--retry-on-error", "--retry-backoff-ms={}".format(backoff_ms),
                     "--max-retries={}".format(3 if mode == "success" else 1)]
        if mode == "transaction":
            args.append("--transaction")
        if run_count > 1:
            args.append("--run-count={}".format(run_count))
        try:
            result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=30, text=True)
        finally:
            peer.shutdown()
            thread.join(timeout=5)
        label = "{} {} {}".format(redirect, mode, command)
        env.assertEqual(result.returncode, 0, message=label + "\n" + result.stderr)
        env.assertEqual(peer.errors, [], message=label)
        if result.returncode:
            return
        expected_ratio = 1 if mode in ("disabled", "transaction") else 2
        _check_text_totals(env, result.stdout, redirect, name, label)
        if run_count > 1:
            with open(path) as source:
                average = json.load(source)["AGGREGATED AVERAGE RESULTS ({} runs)".format(run_count)]
            env.assertEqual(peer.redirects, requests * expected_ratio * run_count, message=label)
            _check_redirect_rates(env, average, redirect, name, expected_ratio, label + " AVERAGE")
            return
        with open(path) as source:
            stats = json.load(source)["ALL STATS"]
        expected_redirects = requests * (1 if mode in ("disabled", "transaction") else 2)
        env.assertEqual(peer.redirects, expected_redirects, message=label)
        env.assertEqual(peer.attempts, requests * (3 if mode == "success" else
                                                2 if mode == "refused" else 1), message=label)
        for entry in (stats[name], stats["Totals"]):
            env.assertEqual(entry["Count"], requests, message=label)
            histogram = entry["Percentile Latencies"]["Histogram log format"]["Compressed Histogram"]
            env.assertEqual(_decode_v2(histogram)[0], requests, message=label + " histogram")
            if backoff_ms > 1:
                # Refused retries still represent the whole logical request.
                # Include its backoff in the one final sample; leave a margin
                # for histogram quantization and timer resolution.
                minimum_ms = backoff_ms * 0.9
                env.assertGreaterEqual(entry["Min Latency"], minimum_ms, message=label)
                env.assertGreaterEqual(entry["Percentile Latencies"]["p50.00"], minimum_ms,
                                       message=label)
            # Both rates use the same duration. Their ratio recovers the
            # redirect count without depending on rounded run timestamps.
            # A zero rate is reported as a failure rather than raised, so the
            # remaining cases still run.
            env.assertGreater(entry["Ops/sec"], 0, message=label + " Ops/sec")
            if entry["Ops/sec"] > 0:
                actual_redirects = entry[redirect + "/sec"] / entry["Ops/sec"] * requests
                env.assertAlmostEqual(actual_redirects, expected_redirects, delta=0.05, message=label)
        series = stats[name]["Time-Serie"].values()
        env.assertEqual(sum(second["Count"] for second in series), requests, message=label)
        env.assertEqual(sum(second["Bytes RX"] for second in series), peer.terminal_rx, message=label)
        env.assertEqual(sum(second["Bytes TX"] for second in series), peer.terminal_tx, message=label)


def _check_redirect_rates(env, stats, redirect, name, expected_ratio, label):
    """Every request is redirected expected_ratio times, in every run, so the
    redirect rate over the ops rate must equal it for the command and Totals,
    and Totals must equal the command (the only one with traffic)."""
    for entry in (stats[name], stats["Totals"]):
        env.assertGreater(entry["Ops/sec"], 0, message=label + " Ops/sec")
        if entry["Ops/sec"] > 0:
            env.assertAlmostEqual(entry[redirect + "/sec"] / entry["Ops/sec"], expected_ratio, delta=0.01,
                                  message=label)
    for rate in ("MOVED/sec", "ASK/sec"):
        env.assertAlmostEqual(stats["Totals"][rate], stats[name][rate], delta=0.011, message=label + " " + rate)


def _check_text_totals(env, stdout, redirect, name, label):
    """The last text table (ALL STATS, or the AVERAGE with --run-count) must
    show the same non-zero redirect rate in the Totals row as in the command row."""
    lines = stdout.splitlines()
    headers = [i for i, line in enumerate(lines) if line.startswith("Type ") and "MOVED/sec" in line]
    env.assertTrue(headers, message=label + " no text table:\n" + stdout)
    if not headers:
        return
    columns = lines[headers[-1]].split()
    rows = {}
    for line in lines[headers[-1] + 1:]:
        fields = line.split()
        if fields and fields[0] in (name, "Totals"):
            rows.setdefault(fields[0], fields)
    env.assertEqual(sorted(rows), sorted([name, "Totals"]), message=label + " text rows:\n" + stdout)
    if len(rows) != 2:
        return
    for rate in ("MOVED/sec", "ASK/sec"):
        index = columns.index(rate)
        env.assertEqual(rows["Totals"][index], rows[name][index], message=label + " text " + rate)
    env.assertGreater(float(rows["Totals"][columns.index(redirect + "/sec")]), 0, message=label + " text")


def test_moved_redirect_stats(env):
    for mode in ("success", "disabled", "refused"):
        for command in ("set", "get", "arbitrary-set", "arbitrary-get"):
            _check_redirect_stats(env, "MOVED", mode, command)


def test_ask_redirect_stats(env):
    for mode in ("success", "disabled", "refused"):
        for command in ("set", "get", "arbitrary-set", "arbitrary-get"):
            _check_redirect_stats(env, "ASK", mode, command)


def test_transaction_redirect_stats(env):
    for redirect in ("MOVED", "ASK"):
        for command in ("arbitrary-set", "arbitrary-get"):
            _check_redirect_stats(env, redirect, "transaction", command)


def test_exhausted_redirect_latency_includes_backoff(env):
    for redirect in ("MOVED", "ASK"):
        for command in ("set", "get", "arbitrary-set", "arbitrary-get"):
            _check_redirect_stats(env, redirect, "refused", command, backoff_ms=50)


def test_run_count_average_redirect_stats(env):
    for redirect in ("MOVED", "ASK"):
        for command in ("set", "get", "arbitrary-set", "arbitrary-get"):
            _check_redirect_stats(env, redirect, "success", command, run_count=2)
