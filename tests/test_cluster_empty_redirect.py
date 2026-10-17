"""Empty MOVED/ASK hosts inherit the replying connection, including non-seeds."""
import csv
import errno
import json
import os
import socket
import socketserver
import subprocess
import tempfile
import threading

from include import MEMTIER_BINARY


class _Node(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, host, role, cluster):
        super().__init__((host, 0), _Handler)
        self.role = role
        self.cluster = cluster
        self.commands = []
        self.completed = 0
        self.errors = []


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        node = self.server
        cluster = node.cluster
        asking = False
        try:
            while True:
                line = self.rfile.readline()
                if not line:
                    return
                args = []
                for _ in range(int(line[1:])):
                    length = int(self.rfile.readline()[1:])
                    args.append(self.rfile.read(length + 2)[:-2])
                verb = args[0].upper()
                node.commands.append(verb)
                if verb == b"CLUSTER":
                    # Keep the old owner even after MOVED: successful replay
                    # must use the redirect address rather than a fresh map.
                    reply = cluster.topology()
                elif verb == b"ASKING":
                    asking = True
                    reply = b"+OK\r\n"
                elif verb in (b"SET", b"GET"):
                    if node.role == "source":
                        if not cluster.target_seen.wait(5):
                            raise AssertionError("target was not discovered")
                        host = "" if cluster.empty else cluster.target.server_address[0]
                        reply = ("-{} 15495 {}:{}\r\n".format(
                            cluster.redirect, host, cluster.target.server_address[1])).encode()
                    elif node.role != "target":
                        raise AssertionError("workload reached the seed")
                    elif asking != (cluster.redirect == "ASK") or (
                            asking and node.commands[-2] != b"ASKING"):
                        raise AssertionError("wrong ASKING state for replay")
                    else:
                        node.completed += 1
                        reply = b"+OK\r\n" if verb == b"SET" else b"$1\r\nv\r\n"
                    asking = False
                elif verb in (b"AUTH", b"SELECT", b"CLIENT", b"READONLY"):
                    reply = b"+OK\r\n"
                else:
                    raise AssertionError("unexpected command: {!r}".format(args))
                self.wfile.write(reply)
                self.wfile.flush()
                if node.role == "target" and verb == b"AUTH":
                    cluster.target_seen.set()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as error:
            node.errors.append(repr(error))


class _Cluster:
    def __init__(self, redirect, empty, nonseed):
        self.redirect = redirect
        self.empty = empty
        self.target_seen = threading.Event()
        # A distinct loopback address makes using the seed host fail even
        # though the redirect's port identifies a real connected primary.
        source_host = "127.0.0.2" if nonseed else "127.0.0.1"
        self.nodes = []
        try:
            self.source = _Node(source_host, "source", self)
            self.nodes.append(self.source)
            self.target = _Node(source_host, "target", self)
            self.nodes.append(self.target)
            self.seed = _Node("127.0.0.1", "seed", self) if nonseed else self.source
            if nonseed:
                self.nodes.append(self.seed)
        except Exception:
            for node in self.nodes:
                node.server_close()
            raise

    def topology(self):
        def shard(first, last, node):
            host, port = node.server_address
            return ("*3\r\n:{}\r\n:{}\r\n*2\r\n${}\r\n{}\r\n:{}\r\n".format(
                first, last, len(host), host, port)).encode()
        return b"*2\r\n" + shard(0, 1, self.target) + shard(2, 16383, self.source)

    def __enter__(self):
        self.threads = []
        for node in self.nodes:
            thread = threading.Thread(target=node.serve_forever, daemon=True)
            thread.start()
            self.threads.append(thread)
        return self

    def __exit__(self, *exc):
        for node in self.nodes:
            node.shutdown()
            node.server_close()
        for thread in self.threads:
            thread.join(timeout=5)


def _check_redirect(env, redirect, empty, nonseed, read):
    label = "{} empty={} nonseed={} read={}".format(redirect, empty, nonseed, read)
    with _Cluster(redirect, empty, nonseed) as cluster, tempfile.TemporaryDirectory() as directory:
        stats_path = os.path.join(directory, "stats.json")
        failed_path = os.path.join(directory, "failed.csv")
        args = [MEMTIER_BINARY, "--server=127.0.0.1", "--port=" + str(cluster.seed.server_address[1]),
                "--cluster-mode", "--threads=1", "--clients=1", "--pipeline=4", "--requests=20",
                "--authenticate=dummy-empty-redirect", "--key-prefix={a}:", "--key-minimum=1",
                "--key-maximum=1", "--ratio=" + ("0:1" if read else "1:0"), "--read-preference=primary",
                "--retry-on-error", "--max-retries=2", "--connection-stage-timeout=5",
                "--hide-histogram", "--json-out-file=" + stats_path, "--failed-keys-file=" + failed_path]
        result = subprocess.run(args, capture_output=True, text=True, timeout=20)
        env.assertEqual(result.returncode, 0, message=label + "\n" + result.stderr[-3000:])
        for node in cluster.nodes:
            env.assertEqual(node.errors, [], message=label)
        env.assertEqual(cluster.source.commands.count(b"GET" if read else b"SET"), 20, message=label)
        env.assertEqual(cluster.target.completed, 20, message=label)
        env.assertEqual(cluster.target.commands.count(b"ASKING"), 20 if redirect == "ASK" else 0,
                        message=label)
        with open(stats_path) as stream:
            stats = json.load(stream)["ALL STATS"]
        env.assertEqual(stats["Totals"]["Count"], 20, message=label)
        env.assertEqual(stats["Gets" if read else "Sets"]["Count"], 20, message=label)
        with open(failed_path) as stream:
            env.assertEqual(list(csv.DictReader(stream)), [], message=label)


def _check_redirect_cases(env, nonseed):
    for redirect in ("ASK", "MOVED"):
        for empty in (False, True):
            for read in (False, True):
                _check_redirect(env, redirect, empty, nonseed, read)


def test_empty_redirect_host(env):
    _check_redirect_cases(env, nonseed=False)


def test_empty_redirect_host_uses_nonseed_sender(env):
    # Some platforms require explicitly configuring this loopback alias.
    # Only these cases need it; retain all seed cases on those platforms.
    try:
        with socket.socket() as probe:
            probe.bind(("127.0.0.2", 0))
    except OSError as error:
        if error.errno in (errno.EADDRNOTAVAIL, errno.EAFNOSUPPORT, 10049):
            env.skip()
        raise
    _check_redirect_cases(env, nonseed=True)
