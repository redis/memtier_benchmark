"""Deterministic MOVED/ASK accounting, including failed and disabled retries.

A scripted RESP peer keeps the topology fixed while returning two redirects
before each successful command. This avoids timing a real cluster failover
and makes the terminal response bytes and histogram sample counts exact.

The slot-move tests at the end use a second scripted peer: two nodes, one of
which hands its slots to the other (#590). It paces its replies on timing
(it answers once memtier stops sending, or once a resend arrives) rather
than on a fixed script, but the state it drives memtier into does not
depend on that timing.
"""
import json
import os
import select
import socketserver
import subprocess
import tempfile
import threading
import time

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


def _read_commands(buf):
    """Split the complete RESP commands off the front of buf."""
    commands = []
    while buf.startswith(b"*"):
        end = buf.find(b"\r\n")
        if end < 0:
            break
        pos = end + 2
        args = []
        for _ in range(int(buf[1:end])):
            end = buf.find(b"\r\n", pos)
            if end < 0:
                return commands, buf
            size = int(buf[pos + 1:end])
            if len(buf) < end + 2 + size + 2:
                return commands, buf
            args.append(buf[end + 2:end + 2 + size])
            pos = end + 2 + size + 2
        commands.append(args)
        buf = buf[pos:]
    return commands, buf


def _is_data(args):
    return args[0].upper() in (b"SET", b"GET")


def _data_reply(args):
    return b"+OK\r\n" if args[0].upper() == b"SET" else b"$-1\r\n"


class _SlotMoveNode(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, cluster, keeps_slots):
        super().__init__(("127.0.0.1", 0), _SlotMoveHandler)
        self.cluster = cluster
        self.keeps_slots = keeps_slots


class _SlotMoveCluster:
    """Two scripted nodes. One of them gives up its slots to the other while
    memtier has more requests in flight on it than --pipeline allows.

    That node first answers whole batches with -TRYAGAIN. While those
    requests wait for their retry memtier refills the pipeline, up to the
    in-flight budget of all its connections, so the connection ends up
    deeper than --pipeline. After tryagain_batches batches, and no sooner
    than move_after seconds, the node answers MOVED, one reply at a time,
    and only CLUSTER SLOTS shows the slots on the other node.

    With burst, the node answers CLUSTER SLOTS together with everything
    queued behind it in one write, as a real node would.
    """

    def __init__(self, tryagain_batches, move_after=0.0, burst=False):
        self.tryagain_batches = tryagain_batches
        self.move_after = move_after
        self.burst = burst
        self.batches = 0
        self.move = False
        self.moved = 0
        self.moved_at = None
        self.max_depth = 0
        self.errors = []
        self.started = time.monotonic()
        self.owner = _SlotMoveNode(self, True)
        self.leaver = _SlotMoveNode(self, False)

    def moving(self):
        return self.move

    def batch_answered(self):
        # Only switch to MOVED between batches, once memtier has the whole
        # last -TRYAGAIN batch.
        self.batches += 1
        if self.batches >= self.tryagain_batches and time.monotonic() - self.started >= self.move_after:
            self.move = True

    def slots_reply(self):
        def shard(first, last, node):
            return (b"*3\r\n:%d\r\n:%d\r\n*2\r\n$9\r\n127.0.0.1\r\n:%d\r\n"
                    % (first, last, node.server_address[1]))
        if self.moving():
            return b"*1\r\n" + shard(0, 16383, self.owner)
        return b"*2\r\n" + shard(0, 8191, self.leaver) + shard(8192, 16383, self.owner)

    def __enter__(self):
        self.started = time.monotonic()
        for node in (self.owner, self.leaver):
            threading.Thread(target=node.serve_forever, daemon=True).start()
        return self

    def __exit__(self, *exc):
        for node in (self.owner, self.leaver):
            node.shutdown()
            node.server_close()


class _SlotMoveHandler(socketserver.BaseRequestHandler):
    def reply(self, args):
        cluster = self.server.cluster
        if args[0].upper() == b"CLUSTER":
            return cluster.slots_reply()
        if not _is_data(args):
            return b"+OK\r\n"
        if self.server.keeps_slots:
            return _data_reply(args)
        if not cluster.moving():
            return b"-TRYAGAIN batch\r\n"
        if cluster.moved_at is None:
            cluster.moved_at = time.monotonic()
        cluster.moved += 1
        return b"-MOVED 0 127.0.0.1:%d\r\n" % cluster.owner.server_address[1]

    def handle(self):
        cluster = self.server.cluster
        leaving = not self.server.keeps_slots
        buf = b""
        queued = []
        # After a MOVED, wait for memtier to resend it before the next one,
        # so it reads one reply at a time and the connection stays as deep.
        sent_at = None
        try:
            while True:
                trickle = leaving and cluster.moving()
                # The owner answers at once. The leaving node answers a whole
                # batch of -TRYAGAIN once memtier stops sending.
                timeout = (0.01 if trickle else 0.05) if leaving else None
                if select.select([self.request], [], [], timeout)[0]:
                    data = self.request.recv(65536)
                    if not data:
                        return
                    commands, buf = _read_commands(buf + data)
                    queued += commands
                    if leaving:
                        depth = sum(1 for args in queued if _is_data(args))
                        cluster.max_depth = max(cluster.max_depth, depth)
                        sent_at = None
                        if not trickle:
                            continue
                if not queued:
                    continue
                if trickle and not (cluster.burst and any(args[0].upper() == b"CLUSTER" for args in queued)):
                    if sent_at is not None and time.monotonic() - sent_at < 0.1:
                        continue
                    self.request.sendall(self.reply(queued.pop(0)))
                    sent_at = time.monotonic()
                else:
                    batch = any(_is_data(args) for args in queued)
                    self.request.sendall(b"".join(self.reply(args) for args in queued))
                    queued = []
                    if leaving and batch and not trickle:
                        cluster.batch_answered()
        except (ConnectionResetError, BrokenPipeError, OSError):
            pass  # memtier may disconnect with replies still queued.
        except Exception as error:
            cluster.errors.append(repr(error))


def _run_slot_move(env, run_args, label, **cluster_args):
    """Run memtier against a _SlotMoveCluster and check that it finishes
    with a bounded number of MOVED replies. Returns (cluster, start time,
    Totals) or None if memtier did not finish."""
    pipeline = 4
    with _SlotMoveCluster(**cluster_args) as cluster, tempfile.TemporaryDirectory() as directory:
        path = os.path.join(directory, "stats.json")
        args = [MEMTIER_BINARY, "--server=127.0.0.1",
                "--port={}".format(cluster.leaver.server_address[1]), "--cluster-mode",
                "--threads=1", "--clients=1", "--pipeline={}".format(pipeline),
                "--ratio=1:1", "--key-minimum=1", "--key-maximum=1000",
                "--retry-on-error", "--hide-histogram", "--json-out-file=" + path] + run_args
        started = time.monotonic()
        try:
            result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=30, text=True)
        except subprocess.TimeoutExpired:
            result = None
        label = "{}: depth {} MOVED {}".format(label, cluster.max_depth, cluster.moved)
        env.assertTrue(result is not None, message="memtier did not finish, " + label)
        # The bug needs a connection deeper than --pipeline once MOVED starts.
        # The depth only drops once memtier stops resending, so the maximum
        # seen by the leaving node is the depth MOVED starts at.
        env.assertGreater(cluster.max_depth, pipeline, message=label)
        env.assertEqual(cluster.errors, [], message=label)
        if result is None:
            return None
        env.assertEqual(result.returncode, 0, message=label + "\n" + result.stderr[-2000:])
        # A refresh lands after a pipeline's worth of MOVED replies, not
        # after hundreds of thousands.
        env.assertGreater(cluster.moved, 0, message=label)
        env.assertLess(cluster.moved, 1000, message=label)
        if result.returncode:
            return None
        with open(path) as source:
            totals = json.load(source)["ALL STATS"]["Totals"]
        return cluster, started, totals


def test_moved_refreshes_slots_on_overfull_connection(env):
    """A MOVED must refresh the slot map even on a connection with
    --pipeline or more requests in flight. Otherwise every retry goes back
    to the node that gave up the slot, which answers MOVED again, forever."""
    for burst in (False, True):
        label = "burst" if burst else "one reply at a time"
        run = _run_slot_move(env, ["--requests=2000"], label, tryagain_batches=8, burst=burst)
        if run is not None:
            env.assertEqual(run[2]["Count"], 2000, message=label)


def test_moved_after_test_time_refreshes_slots(env):
    """Once --test-time is up memtier still waits for the requests in
    flight, retries included. A MOVED that arrives then must still refresh
    the slot map, or the run never ends."""
    test_time = 1
    run = _run_slot_move(env, ["--test-time={}".format(test_time)], "test-time",
                         tryagain_batches=8, move_after=test_time + 0.5)
    if run is not None:
        cluster, started, _ = run
        env.assertGreater(cluster.moved_at - started, test_time,
                          message="MOVED must start after --test-time is up")


class _BatchPeer(socketserver.ThreadingTCPServer):
    """Answers everything a connection sent once it stops sending, and
    records the most requests it ever had to answer at once."""
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), _BatchHandler)
        self.max_depth = 0


class _BatchHandler(socketserver.BaseRequestHandler):
    def handle(self):
        buf = b""
        queued = []
        try:
            while True:
                if select.select([self.request], [], [], 0.02)[0]:
                    data = self.request.recv(65536)
                    if not data:
                        return
                    commands, buf = _read_commands(buf + data)
                    queued += commands
                    self.server.max_depth = max(self.server.max_depth, len(queued))
                elif queued:
                    self.request.sendall(b"".join(
                        _data_reply(args) if _is_data(args) else b"+OK\r\n" for args in queued))
                    queued = []
        except (ConnectionResetError, BrokenPipeError, OSError):
            pass


def test_pipeline_depth_stays_at_cap(env):
    """fill_pipeline() stops at exactly --pipeline requests in flight."""
    for pipeline in (1, 4):
        with _BatchPeer() as peer:
            threading.Thread(target=peer.serve_forever, daemon=True).start()
            args = [MEMTIER_BINARY, "--server=127.0.0.1",
                    "--port={}".format(peer.server_address[1]),
                    "--threads=1", "--clients=1", "--pipeline={}".format(pipeline),
                    "--requests=100", "--hide-histogram"]
            try:
                result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                        timeout=30, text=True)
            finally:
                peer.shutdown()
            env.assertEqual(result.returncode, 0, message=result.stderr[-2000:])
            env.assertEqual(peer.max_depth, pipeline)
