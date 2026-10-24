"""Planned standalone reconnects must drain whole weighted command rotations."""

import json
import os
import socketserver
import subprocess
import tempfile
import threading

from include import MEMTIER_BINARY

_PASSWORD = b"transaction-interval-test"


class _Peer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

    def __init__(self, drop_queued=False):
        super().__init__(("127.0.0.1", 0), _Handler)
        self.lock = threading.Lock()
        self.connections = []
        self.executed = []
        self.discarded = 0
        self.errors = []
        self.drop_queued = drop_queued


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        self.connection.settimeout(3)
        queued = None
        authenticated = False
        commands = []
        with self.server.lock:
            self.server.connections.append(commands)
        try:
            while True:
                header = self.rfile.readline()
                if not header:
                    break
                args = []
                for _ in range(int(header[1:])):
                    length = int(self.rfile.readline()[1:])
                    args.append(self.rfile.read(length))
                    assert self.rfile.read(2) == b"\r\n"
                verb = args[0].upper()
                if verb == b"AUTH":
                    authenticated = args[1:] == [_PASSWORD]
                    self.wfile.write(b"+OK\r\n" if authenticated else b"-WRONGPASS\r\n")
                    continue
                assert authenticated
                commands.append(verb.decode())
                if verb == b"MULTI":
                    assert queued is None, "nested MULTI"
                    queued = []
                    reply = b"+OK\r\n"
                elif verb in (b"EXEC", b"DISCARD"):
                    assert queued is not None, "close without MULTI"
                    if verb == b"EXEC":
                        with self.server.lock:
                            self.server.executed.extend(queued)
                        reply = (
                            b"*"
                            + str(len(queued)).encode()
                            + b"\r\n"
                            + b"+OK\r\n" * len(queued)
                        )
                    else:
                        reply = b"+OK\r\n"
                    queued = None
                elif queued is not None:
                    queued.append(args)
                    with self.server.lock:
                        drop = self.server.drop_queued
                        self.server.drop_queued = False
                    if drop:
                        return  # Lose MULTI before acknowledging its first queued command.
                    reply = b"+QUEUED\r\n"
                elif verb in (b"WATCH", b"UNWATCH", b"PING"):
                    reply = b"+OK\r\n"
                elif verb == b"GET":
                    reply = b"$-1\r\n"
                else:
                    raise AssertionError("write outside MULTI: " + repr(args))
                self.wfile.write(reply)
        except OSError:
            pass
        except Exception as error:
            self.server.errors.append(str(error))
        finally:
            if queued:
                with self.server.lock:
                    self.server.discarded += len(queued)


def _run(env, commands, requests, interval, pipeline, drop_queued=False):
    peer = _Peer(drop_queued)
    thread = threading.Thread(target=peer.serve_forever)
    thread.start()
    try:
        with tempfile.TemporaryDirectory() as directory:
            output = os.path.join(directory, "result.json")
            args = [
                MEMTIER_BINARY,
                "-s",
                "127.0.0.1",
                "-p",
                str(peer.server_address[1]),
                "-a",
                _PASSWORD.decode(),
                "-t",
                "1",
                "-c",
                "1",
                "-n",
                str(requests),
                "--transaction",
                "--pipeline=" + str(pipeline),
                "--hide-histogram",
                "--json-out-file=" + output,
            ]
            if interval:
                args.append("--reconnect-interval=" + str(interval))
            if drop_queued:
                args.extend(
                    [
                        "--reconnect-on-error",
                        "--max-reconnect-attempts=2",
                        "--reconnect-backoff-factor=0.01",
                    ]
                )
            for command, ratio in commands:
                args.extend(
                    [
                        "--command=" + command,
                        "--command-ratio=" + str(ratio),
                        "--command-key-pattern=S",
                    ]
                )
            process = subprocess.run(args, capture_output=True, text=True, timeout=12)
            env.assertEqual(process.returncode, 0, message=process.stderr)
            with open(output) as stream:
                stats = json.load(stream)["ALL STATS"]
    finally:
        peer.shutdown()
        peer.server_close()
        thread.join(timeout=5)
    env.assertEqual(peer.errors, [])
    return peer, stats


def test_transaction_interval_whole_rotations(env):
    # Explicit plaintext peers are independent of the harness's Redis/TLS mode.
    commands = [("MULTI", 1), ("SET __key__ __data__", 1), ("EXEC", 1)]
    for pipeline in (1, 8):
        for interval in (0, 6, 7, 8):
            peer, stats = _run(env, commands, 27, interval, pipeline)
            env.assertEqual(stats["Sets"]["Count"], 9)
            env.assertEqual(stats["Execs"]["Count"], 9)
            env.assertEqual(len(peer.executed), 9)
            env.assertEqual(peer.discarded, 0)
            env.assertEqual(len(peer.connections), {0: 1, 6: 5, 7: 3, 8: 3}[interval])
            for wire in peer.connections:
                env.assertEqual(wire, ["MULTI", "SET", "EXEC"] * (len(wire) // 3))


def test_transaction_interval_watch_and_ratios(env):
    commands = [
        ("WATCH __key__", 2),
        ("GET __key__", 1),
        ("MULTI", 1),
        ("SET __key__ __data__", 3),
        ("EXEC", 1),
        ("UNWATCH", 1),
    ]
    rotation = [
        "WATCH",
        "WATCH",
        "GET",
        "MULTI",
        "SET",
        "SET",
        "SET",
        "EXEC",
        "UNWATCH",
    ]
    for pipeline in (1, 16):
        for interval in (1, 4, 8, 10):
            peer, stats = _run(env, commands, 45, interval, pipeline)
            env.assertEqual(len(peer.executed), 15)
            env.assertEqual(stats["Sets"]["Count"], 15)
            env.assertEqual(peer.discarded, 0)
            for wire in peer.connections:
                env.assertEqual(wire, rotation * (len(wire) // len(rotation)))
            env.assertGreater(len(peer.connections), 1)


def test_transaction_interval_preserves_lost_multi_skip(env):
    commands = [("MULTI", 1), ("SET __key__ __data__", 1), ("EXEC", 1)]
    for interval in (0, 1, 2, 8):
        peer, stats = _run(env, commands, 28, interval, 1, drop_queued=True)
        env.assertEqual(peer.discarded, 1)
        env.assertEqual(stats["Sets"]["Count"], len(peer.executed))
        env.assertEqual(peer.connections[0], ["MULTI", "SET"])
        # The lost block's EXEC is skipped, then normal rotations resume.
        for wire in peer.connections[1:]:
            env.assertEqual(wire, ["MULTI", "SET", "EXEC"] * (len(wire) // 3))
