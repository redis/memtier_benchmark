"""Interrupting a timed run's drain must not extend its throughput denominator (#747)."""
import json
import os
import signal
import socketserver
import subprocess
import tempfile
import threading
import time

from include import MEMTIER_BINARY


class _Peer(socketserver.StreamRequestHandler):
    def handle(self):
        self.connection.settimeout(20)
        try:
            while not self.server.stop.is_set():
                line = self.rfile.readline()
                if not line:
                    return
                args = []
                for _ in range(int(line[1:])):
                    size = int(self.rfile.readline()[1:])
                    args.append(self.rfile.read(size))
                    assert self.rfile.read(2) == b"\r\n"
                if args == [b"AUTH", b"dummy-747"]:
                    self.wfile.write(b"+OK\r\n")
                elif args[0] == b"SET":
                    self.server.received += 1
                    if self.server.received <= 20:
                        self.wfile.write(b"+OK\r\n")
                    else:
                        self.server.stalled.set()
                        self.server.stop.wait(20)
                        return
                else:
                    raise AssertionError(args)
        except Exception as error:
            self.server.errors.append(repr(error))


class _Server(socketserver.ThreadingTCPServer):
    daemon_threads = True


def _run(env, mode, delay):
    if os.name == "nt":
        # Popen.send_signal(SIGINT) is not a Windows console Ctrl+C event.
        # The exact timestamp unit cases also run on native Windows.
        env.skip()
    with _Server(("127.0.0.1", 0), _Peer) as peer, tempfile.TemporaryDirectory() as directory:
        peer.stop = threading.Event()
        peer.stalled = threading.Event()
        peer.errors = []
        peer.received = 0
        thread = threading.Thread(target=peer.serve_forever)
        thread.start()
        child = None
        try:
            path = os.path.join(directory, "stats.json")
            args = [MEMTIER_BINARY, "-s", "127.0.0.1", "-p", str(peer.server_address[1]),
                    "--authenticate=dummy-747", "-t", "1", "-c", "1", "--pipeline=1", "--ratio=1:0",
                    "--hide-histogram", "--json-out-file=" + path] + mode
            child = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            assert peer.stalled.wait(5), "peer never reached the unanswered request"
            # Start the wait after replies, so a slow startup cannot turn the
            # late-interrupt case into an early one.
            time.sleep(delay)
            child.send_signal(signal.SIGINT)
            stdout, stderr = child.communicate(timeout=10)
            env.assertEqual(child.returncode, 0, message=stdout + stderr)
            with open(path) as source:
                stats = json.load(source)["ALL STATS"]
            env.assertEqual(peer.errors, [])
            env.assertEqual(peer.received, 21)
            env.assertEqual(stats["Totals"]["Count"], 20)
            env.assertEqual(stats["Runtime"]["Interrupted"], "true")
            env.assertEqual(stats["Runtime"]["Unanswered requests"], 0)
            return stats
        finally:
            if child is not None and child.poll() is None:
                child.kill()
                child.communicate(timeout=3)
            peer.stop.set()
            peer.shutdown()
            thread.join(timeout=3)


def test_interrupt_during_timed_drain(env):
    for seconds in (1, 2):
        stats = _run(env, ["--test-time=" + str(seconds)], seconds + 0.5)
        env.assertEqual(stats["Runtime"]["Total duration"], seconds * 1000)
        env.assertEqual(stats["Totals"]["Ops/sec"], 20 / seconds)


def test_early_interrupt_keeps_elapsed_duration(env):
    stats = _run(env, ["--test-time=2"], 0.2)
    env.assertGreater(stats["Runtime"]["Total duration"], 0)
    env.assertLess(stats["Runtime"]["Total duration"], 2000)
    env.assertGreater(stats["Totals"]["Ops/sec"], 10)


def test_request_count_interrupt_keeps_elapsed_duration(env):
    stats = _run(env, ["--requests=100"], 2.5)
    env.assertGreaterEqual(stats["Runtime"]["Total duration"], 2500)
    env.assertLess(stats["Totals"]["Ops/sec"], 8.01)
