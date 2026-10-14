"""Cluster routing with keys captured for --failed-keys-file / --retry-on-error.

Two scripted RESP peers, A and B, so the routing and the number of refreshes
are exact. Modes:

- "readonly": memtier's first CLUSTER SLOTS reply says A is the primary for
  every slot; every later reply says B is and A is its replica. A answers
  writes with -READONLY, B accepts them.
- "moved": the first reply says A owns every slot; every later reply says B
  owns 0-8191 and A keeps 8192-16383. A answers every write with -MOVED to B
  for the first key's slot, B accepts them. A stays in the map, so a retry
  that is not routed by the request's key goes back to A and is redirected
  again.
- "replica": a stable topology, A primary and B its replica. B answers writes
  with -READONLY (a write sent to a replica), A accepts them.
- "txn": A and B first split the slots. B's first SET answers -MOVED, which
  makes memtier refresh; every later map says B owns every slot and A is its
  replica. A answers SET with -READONLY and is slow, so MULTI blocks are in
  flight on A when the new map lands. B counts SETs that arrive outside MULTI.
"""
import binascii
import csv
import json
import os
import socketserver
import subprocess
import tempfile
import threading
import time

from include import MEMTIER_BINARY


def _slot(key):
    """Redis cluster slot of a key, honouring a {hash tag}."""
    start = key.find(b"{")
    if start >= 0:
        end = key.find(b"}", start + 1)
        if end > start + 1:
            key = key[start + 1:end]
    return binascii.crc_hqx(key, 0) % 16384


def _bulk(value):
    return b"$%d\r\n%s\r\n" % (len(value), value)


def _node(port, node_id):
    return b"*3\r\n" + _bulk(b"127.0.0.1") + b":%d\r\n" % port + _bulk(node_id * 40)


class _Peer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, cluster, name):
        super().__init__(("127.0.0.1", 0), _Handler)
        self.cluster = cluster
        self.name = name


class _Cluster(object):
    def __init__(self, mode):
        self.mode = mode  # "readonly", "moved", "replica" or "txn"
        self.lock = threading.Lock()
        self.slots_calls = 0
        self.slots_by = {"A": 0, "B": 0}
        self.writes = {"A": 0, "B": 0}
        self.naked_b_writes = 0  # "txn": SETs that reached B outside MULTI
        self.late_readonly = 0  # "txn": -READONLY A sent after serving the new map
        self.moved_sent = False
        self.errors = []
        self.stats = None
        self.a = _Peer(self, "A")
        self.b = _Peer(self, "B")

    def slots_reply(self, name):
        with self.lock:
            self.slots_calls += 1
            self.slots_by[name] += 1
            first = self.slots_calls == 1
            moved = self.moved_sent
        a_port, b_port = self.a.server_address[1], self.b.server_address[1]
        if self.mode == "replica":
            return b"*1\r\n*4\r\n:0\r\n:16383\r\n" + _node(a_port, b"a") + _node(b_port, b"b")
        if self.mode == "txn":
            if not moved:
                return (b"*2\r\n*3\r\n:0\r\n:8191\r\n" + _node(a_port, b"a") +
                        b"*3\r\n:8192\r\n:16383\r\n" + _node(b_port, b"b"))
            return b"*1\r\n*4\r\n:0\r\n:16383\r\n" + _node(b_port, b"b") + _node(a_port, b"a")
        if first:
            return b"*1\r\n*3\r\n:0\r\n:16383\r\n" + _node(a_port, b"a")
        if self.mode == "readonly":
            return b"*1\r\n*4\r\n:0\r\n:16383\r\n" + _node(b_port, b"b") + _node(a_port, b"a")
        return (b"*2\r\n*3\r\n:0\r\n:8191\r\n" + _node(b_port, b"b") +
                b"*3\r\n:8192\r\n:16383\r\n" + _node(a_port, b"a"))

    def write_reply(self, name, in_multi, key):
        readonly = b"-READONLY You can't write against a read only replica.\r\n"
        with self.lock:
            self.writes[name] += 1
            if self.mode == "txn" and name == "B":
                first = not self.moved_sent
                self.moved_sent = True
                if not first and not in_multi:
                    self.naked_b_writes += 1
        if self.mode == "replica":
            return readonly if name == "B" else b"+OK\r\n"
        if self.mode == "txn":
            if name == "A":
                with self.lock:
                    if self.moved_sent and self.slots_calls >= 2:
                        self.late_readonly += 1
                return readonly
            if first:
                return b"-MOVED 9000 127.0.0.1:%d\r\n" % self.b.server_address[1]
            return b"+QUEUED\r\n" if in_multi else b"+OK\r\n"
        if name == "B":
            return b"+OK\r\n"
        if self.mode == "readonly":
            return b"-READONLY You can't write against a read only replica.\r\n"
        return b"-MOVED %d 127.0.0.1:%d\r\n" % (_slot(key), self.b.server_address[1])

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


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        cluster = self.server.cluster
        name = self.server.name
        in_multi = False
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
                if cluster.mode == "txn" and name == "A" and verb != b"CLUSTER":
                    time.sleep(0.05)  # keep MULTI blocks in flight on A
                elif cluster.mode == "replica" and name == "A" and verb == b"SET":
                    # A slow primary leaves time for B's connection to become
                    # ready, so most writes go to B, not to A through the
                    # primary fallback.
                    time.sleep(0.01)
                if verb == b"CLUSTER":
                    reply = cluster.slots_reply(name)
                elif verb == b"SET":
                    reply = cluster.write_reply(name, in_multi, args[1])
                elif verb == b"MULTI":
                    in_multi = True
                    reply = b"+OK\r\n"
                elif verb == b"EXEC":
                    if not in_multi:
                        reply = b"-ERR EXEC without MULTI\r\n"
                    elif name == "A":
                        reply = b"-EXECABORT Transaction discarded because of previous errors.\r\n"
                    else:
                        reply = b"*1\r\n+OK\r\n"
                    in_multi = False
                elif verb in (b"HELLO", b"AUTH", b"SELECT", b"CLIENT", b"READONLY", b"ASKING"):
                    reply = b"+OK\r\n"
                elif verb == b"COMMAND":
                    reply = b"*0\r\n"
                else:
                    raise AssertionError("unexpected command: {!r}".format(args))
                self.wfile.write(reply)
                self.wfile.flush()
        except (ConnectionResetError, BrokenPipeError, ValueError):
            pass  # memtier may close mid-reply at shutdown.
        except Exception as error:
            cluster.errors.append(repr(error))


def _run(env, mode, command, extra_args, requests=50, pipeline=1):
    """Run memtier against a scripted A/B pair; return (cluster, failed-keys rows)."""
    with _Cluster(mode) as cluster, tempfile.TemporaryDirectory() as directory:
        args = [MEMTIER_BINARY, "--server=127.0.0.1", "--port={}".format(cluster.a.server_address[1]),
                "--cluster-mode", "--threads=1", "--clients=1", "--pipeline={}".format(pipeline),
                "--requests={}".format(requests), "--hide-histogram",
                "--json-out-file=" + os.path.join(directory, "stats.json")]
        if mode != "txn":
            args += ["--key-minimum=1", "--key-maximum=1"]
        if command == "set":
            args.append("--ratio=1:0")
        elif command == "tagged":
            args.append("--command=SET {b}:__key__ value")
        elif command == "transaction":
            args += ["--command=MULTI", "--command=SET __key__ value", "--command=EXEC", "--transaction"]
        else:
            args.append("--command=SET __key__ value")
        failed_log = None
        if "--failed-keys-file" in extra_args:
            failed_log = os.path.join(directory, "failed-keys.csv")
            extra_args = [a for a in extra_args if a != "--failed-keys-file"]
            args.append("--failed-keys-file=" + failed_log)
        args += extra_args
        label = "{} {} {}".format(mode, command, " ".join(extra_args))
        result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60, text=True)
        env.assertEqual(result.returncode, 0, message=label + "\n" + result.stderr[-2000:])
        env.assertEqual(cluster.errors, [], message=label)
        stats_path = os.path.join(directory, "stats.json")
        if os.path.isfile(stats_path):
            with open(stats_path) as f:
                cluster.stats = json.load(f)["ALL STATS"]
        rows = []
        if failed_log and os.path.isfile(failed_log):
            with open(failed_log) as f:
                rows = list(csv.reader(f))[1:]
        return cluster, rows, label


def test_failed_keys_file_does_not_change_readonly_recovery(env):
    """A demoted primary answers -READONLY; memtier must refresh and move to B.

    Recording failures in --failed-keys-file fills request::m_key. That must
    not change the -READONLY handler's recovery: the stale slot map still
    names A as the primary, so routing by key would send every later write
    back to A and never refresh.
    """
    for command in ("set", "arbitrary"):
        for extra in ([], ["--failed-keys-file"], ["--retry-on-error", "--max-retries=3"],
                      ["--retry-on-error", "--max-retries=3", "--failed-keys-file"]):
            cluster, rows, label = _run(env, "readonly", command, extra)
            # One write hits A before the refresh; every later one goes to B.
            env.assertEqual(cluster.writes["A"], 1, message=label + " {}".format(cluster.writes))
            env.assertEqual(cluster.writes["B"], 49, message=label + " {}".format(cluster.writes))
            env.assertGreaterEqual(cluster.slots_calls, 2, message=label)
            if "--failed-keys-file" in extra:
                env.assertEqual([row[2] for row in rows], ["memtier-1"], message=label)
                env.assertContains("READONLY", rows[0][3] if rows else "", message=label)


def test_moved_retry_goes_to_key_owner(env):
    """A request retried after -MOVED is resent to its key's new owner.

    --retry-on-error must capture the key on its own, without
    --failed-keys-file. After the refresh A still owns slots 8192-16383, so a
    retry routed without the key goes back to A, is redirected again and runs
    out of --max-retries. Routed by the key, it reaches B within one or two
    redirects. The "tagged" key must be routed by its {hash tag}, which is in
    B's half; the whole string hashes into A's.
    """
    env.assertLess(_slot(b"memtier-1"), 8192)
    env.assertLess(_slot(b"{b}:memtier-1"), 8192)
    env.assertGreaterEqual(binascii.crc_hqx(b"{b}:memtier-1", 0) % 16384, 8192)
    requests = 20
    for command in ("set", "arbitrary", "tagged"):
        for failed_keys in ([], ["--failed-keys-file"]):
            cluster, rows, label = _run(env, "moved", command,
                                        ["--retry-on-error", "--max-retries=5", "--retry-backoff-ms=20"] +
                                        failed_keys, requests=requests)
            env.assertEqual(rows, [], message=label)
            stats = cluster.stats
            env.assertTrue(stats is not None, message=label)
            if stats is None:
                continue
            entry = stats["Sets"]
            env.assertEqual(entry["Count"], requests, message=label)
            env.assertEqual(stats["Totals"]["Count"], requests, message=label)
            env.assertEqual(entry["Connection Errors"], 0, message=label)
            # Recover the redirect count from the two rates, as
            # test_cluster_redirect_stats does. Only the first request is
            # redirected: once or twice, depending on whether its first retry
            # is chosen before the refresh lands. A keyless retry would loop on
            # A until --max-retries (6 redirects).
            redirects = entry["MOVED/sec"] / entry["Ops/sec"] * requests
            env.assertGreaterEqual(redirects, 0.95, message=label)
            env.assertLessEqual(redirects, 2.05, message=label + " {}".format(cluster.writes))


def test_readonly_from_replica_refreshes_without_retry_on_error(env):
    """A write sent to a replica gets -READONLY in a stable topology.

    The write is a --command marked --command-is-read, so
    --read-preference=secondary sends it to B, B's replica. Without
    --retry-on-error the handler must refresh and drop, as it does on master,
    whether or not --failed-keys-file filled the request's key. Only
    --retry-on-error resends the write to the slot primary A.

    Writes sent before B's connection is ready go to A through
    --read-preference-fallback=primary, and how many depends on timing, so
    the checks are relative to the writes that reached B.
    """
    read_pref = ["--command-is-read", "--read-preference=secondary", "--read-preference-fallback=primary"]
    for extra in ([], ["--failed-keys-file"]):
        cluster, rows, label = _run(env, "replica", "arbitrary", read_pref + extra)
        refreshes = cluster.slots_by["B"]
        # Every -READONLY from B schedules a refresh on B; a few land while
        # one is already pending (47 of 48 measured, also under ASAN), so
        # require half. Routing by key instead gives no refresh at all.
        env.assertGreaterEqual(cluster.writes["B"], 1, message=label + " {}".format(cluster.writes))
        env.assertGreaterEqual(2 * refreshes, cluster.writes["B"],
                               message=label + " {} {}".format(cluster.slots_by, cluster.writes))
        if "--failed-keys-file" in extra:
            env.assertEqual(len(rows), cluster.writes["B"], message=label)
    cluster, rows, label = _run(env, "replica", "arbitrary",
                                read_pref + ["--retry-on-error", "--max-retries=3", "--failed-keys-file"])
    # Each write that reaches B is rejected once, then resent to A without a
    # refresh, so every one of the 50 writes completes on A.
    env.assertEqual(cluster.writes["A"], 50, message=label + " {}".format(cluster.writes))
    env.assertGreaterEqual(cluster.writes["B"], 1, message=label + " {}".format(cluster.writes))
    env.assertEqual(cluster.slots_by["B"], 0, message=label + " {}".format(cluster.slots_by))
    env.assertEqual(rows, [], message=label)


def test_readonly_in_transaction_is_not_retried_outside_multi(env):
    """A -READONLY inside a --transaction block is dropped, not resent.

    Resending one SET of a MULTI/EXEC block to the slot primary would run it
    on another connection outside any MULTI, splitting the block. MOVED/ASK
    already refuse to do that under --transaction; -READONLY must too.
    """
    cluster, rows, label = _run(env, "txn", "transaction",
                                ["--retry-on-error", "--max-retries=3", "--failed-keys-file"],
                                requests=60, pipeline=4)
    env.assertGreaterEqual(cluster.slots_calls, 2, message=label)
    # The scenario needs at least one -READONLY to reach memtier after it has
    # the new map (B owns the slot). If A's delay ever stops covering the
    # refresh, fail here instead of passing without exercising the handler.
    env.assertGreaterEqual(cluster.late_readonly, 1, message=label + " {}".format(cluster.writes))
    env.assertEqual(cluster.naked_b_writes, 0, message=label + " {}".format(cluster.writes))
