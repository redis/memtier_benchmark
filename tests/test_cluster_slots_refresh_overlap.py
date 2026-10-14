"""A redirect that arrives while a CLUSTER SLOTS refresh is in flight.

Two scripted RESP peers form a one-shard cluster: a primary (the seed) and a
replica. With --read-preference=secondary the GETs go to the replica, which
answers the first two with -READONLY, pausing after each so memtier handles
them one at a time. Without --retry-on-error a request carries no key to
reroute by, so each -READONLY asks the replica connection for a CLUSTER
SLOTS refresh. The second one arrives while the first refresh is still
unanswered, and memtier must not send another then. If it does, the first
reply stops keeping the reply value the second needs, the second is
reported as "cluster slot failed", and the connection stops reading, so the
run never ends.
"""
import socketserver
import subprocess
import threading
import time

from include import MEMTIER_BINARY

READONLY_REPLIES = 2


class _Peer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, cluster, replica):
        super().__init__(("127.0.0.1", 0), _Handler)
        self.cluster = cluster
        self.replica = replica


class _Cluster(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.primary = _Peer(self, False)
        self.replica = _Peer(self, True)
        self.readonly = 0
        self.replica_slots_requests = 0
        self.errors = []

    def slots_reply(self):
        nodes = b""
        for peer in (self.primary, self.replica):
            nodes += b"*2\r\n$9\r\n127.0.0.1\r\n:" + str(peer.server_address[1]).encode() + b"\r\n"
        return b"*1\r\n*4\r\n:0\r\n:16383\r\n" + nodes


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        cluster = self.server.cluster
        try:
            while True:
                line = self.rfile.readline()
                if not line:
                    return
                if line.startswith(b"*"):
                    args = []
                    for _ in range(int(line[1:])):
                        size = self.rfile.readline()
                        args.append(self.rfile.read(int(size[1:]) + 2)[:-2])
                else:
                    args = line.strip().split()
                verb = args[0].upper()
                pause = False
                if verb == b"CLUSTER":
                    if self.server.replica:
                        with cluster.lock:
                            cluster.replica_slots_requests += 1
                    reply = cluster.slots_reply()
                elif verb in (b"SET", b"GET"):
                    readonly = False
                    if self.server.replica:
                        with cluster.lock:
                            readonly = cluster.readonly < READONLY_REPLIES
                            if readonly:
                                cluster.readonly += 1
                    if readonly:
                        reply = b"-READONLY You can't write against a read only replica.\r\n"
                        pause = True
                    else:
                        reply = b"+OK\r\n" if verb == b"SET" else b"$3\r\nfoo\r\n"
                elif verb in (b"HELLO", b"AUTH", b"SELECT", b"CLIENT", b"READONLY", b"ASKING"):
                    reply = b"+OK\r\n"
                elif verb == b"COMMAND":
                    reply = b"*0\r\n"
                else:
                    raise AssertionError("unexpected command: {!r}".format(args))
                self.wfile.write(reply)
                self.wfile.flush()
                if pause:
                    # Let memtier handle this -READONLY, and send the
                    # refresh it asks for, before the next reply arrives.
                    time.sleep(0.3)
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as error:
            cluster.errors.append(repr(error))


def test_readonly_during_cluster_slots_refresh(env):
    cluster = _Cluster()
    threads = []
    for peer in (cluster.primary, cluster.replica):
        thread = threading.Thread(target=peer.serve_forever, daemon=True)
        thread.start()
        threads.append(thread)
    args = [MEMTIER_BINARY, "--server=127.0.0.1",
            "--port={}".format(cluster.primary.server_address[1]), "--cluster-mode",
            "--read-preference=secondary", "--threads=1", "--clients=1", "--pipeline=4",
            "--ratio=0:1", "--requests=20", "--key-minimum=1", "--key-maximum=1",
            "--hide-histogram"]
    try:
        result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=20, text=True)
        returncode, stderr = result.returncode, result.stderr
    except subprocess.TimeoutExpired as expired:
        returncode, stderr = "timeout", expired.stderr or ""
        if isinstance(stderr, bytes):
            stderr = stderr.decode(errors="replace")
    finally:
        for peer, thread in zip((cluster.primary, cluster.replica), threads):
            peer.shutdown()
            peer.server_close()
            thread.join(timeout=5)
    env.assertEqual(cluster.errors, [])
    env.assertEqual(cluster.readonly, READONLY_REPLIES)
    env.assertEqual(returncode, 0, message=stderr[-2000:])
    env.assertNotContains("cluster slot failed", stderr)
    # Only the first -READONLY sends a refresh: the second arrived while
    # that refresh was in flight.
    env.assertEqual(cluster.replica_slots_requests, 1)
