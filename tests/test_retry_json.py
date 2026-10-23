"""Final retry counters must describe logical requests and per-run means."""

import collections
import json
import os
import signal
import socketserver
import subprocess
import tempfile
import threading

from include import MEMTIER_BINARY


_FIELDS = ("Errors", "Retry Attempts", "Retried Ops")


class _RetryJsonPeer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

    def __init__(self, modes, clients):
        self.modes = modes
        self.clients = clients
        self.connections = 0
        self.attempts = collections.Counter()
        self.failures = []
        self.lock = threading.Lock()
        self.pause_run = None
        self.paused = threading.Event()
        self.stop = threading.Event()
        super().__init__(("127.0.0.1", 0), _RetryJsonHandler)


class _RetryJsonHandler(socketserver.StreamRequestHandler):
    def handle(self):
        peer = self.server
        self.connection.settimeout(20)
        with peer.lock:
            connection = peer.connections
            peer.connections += 1
        run = connection // peer.clients
        authenticated = False
        try:
            while True:
                line = self.rfile.readline()
                if not line:
                    return
                assert line.startswith(b"*"), line
                args = []
                for _ in range(int(line[1:])):
                    size = self.rfile.readline()
                    assert size.startswith(b"$"), size
                    args.append(self.rfile.read(int(size[1:])))
                    assert self.rfile.read(2) == b"\r\n"
                command = args[0].upper()
                if command == b"AUTH":
                    authenticated = args[-1] == b"dummy-646"
                    reply = b"+OK\r\n" if authenticated else b"-WRONGPASS invalid\r\n"
                else:
                    assert authenticated
                    assert command in (b"SET", b"INCR"), command
                    key = (run, connection, command, args[1])
                    with peer.lock:
                        peer.attempts[key] += 1
                        attempt = peer.attempts[key]
                    if run == peer.pause_run and attempt == 2:
                        peer.paused.set()
                        peer.stop.wait(15)
                        return
                    mode = peer.modes[run]
                    fail = mode == "fail" or (mode == "mixed" and command == b"INCR")
                    retry_once = mode in ("once", "mixed") and attempt == 1
                    reply = b"-TRYAGAIN injected\r\n" if fail or retry_once else b"+OK\r\n"
                self.wfile.write(reply)
                self.wfile.flush()
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as error:
            with peer.lock:
                peer.failures.append(repr(error))


def _run_retry_json(modes, *, threads=1, clients=1, requests=6, retry=True, arbitrary=False):
    with tempfile.TemporaryDirectory() as directory, _RetryJsonPeer(modes, threads * clients) as peer:
        serving = threading.Thread(target=peer.serve_forever)
        serving.start()
        output = os.path.join(directory, "result.json")
        args = [MEMTIER_BINARY, "-s", "127.0.0.1", "-p", str(peer.server_address[1]),
                "-a", "dummy-646", "-t", str(threads), "-c", str(clients), "-n", str(requests),
                "--pipeline=2", "--key-minimum=1", "--key-maximum=100000",
                "--hide-histogram", "--json-out-file=" + output,
                "--run-count=" + str(len(modes)), "--print-all-runs"]
        if retry:
            args += ["--retry-on-error", "--max-retries=2", "--retry-backoff-ms=0"]
        if arbitrary:
            args += ["--command=SET __key__ __data__", "--command-key-pattern=S",
                     "--command=INCR __key__", "--command-key-pattern=S"]
        else:
            args += ["--ratio=1:0", "--key-pattern=S:S"]
        try:
            process = subprocess.run(args, capture_output=True, text=True, timeout=45)
        finally:
            peer.shutdown()
            serving.join(timeout=5)
        assert process.returncode == 0, process.stderr[-3000:]
        assert not peer.failures, peer.failures
        assert peer.connections == len(modes) * threads * clients
        with open(output) as stream:
            result = json.load(stream)
        expected = []
        for run, mode in enumerate(modes):
            entries = [(key, attempts) for key, attempts in peer.attempts.items() if key[0] == run]
            assert len(entries) == requests * threads * clients
            errors = sum(mode == "fail" or (mode == "mixed" and key[2] == b"INCR")
                         for key, _ in entries)
            retries = sum(attempts - 1 for _, attempts in entries)
            retried = sum(attempts > 1 for _, attempts in entries)
            expected.append((errors, retries, retried))
        return result, expected


def _assert_totals(section, expected, enabled=True):
    assert "Totals" in section, section
    for label, row in section.items():
        if not isinstance(row, dict):
            continue
        if label == "Totals" and enabled:
            assert tuple(row[field] for field in _FIELDS) == expected, row
            assert all(type(row[field]) is int for field in _FIELDS)
        else:
            assert all(field not in row for field in _FIELDS), (label, row)


def test_retry_json_totals(env):
    for mode, arbitrary, retry in (("ok", False, True), ("once", False, True),
                                   ("fail", False, True), ("mixed", True, True),
                                   ("ok", False, False), ("fail", False, False)):
        result, expected = _run_retry_json((mode,), threads=2, clients=2,
                                          arbitrary=arbitrary, retry=retry)
        _assert_totals(result["ALL STATS"], expected[0], retry)


def test_retry_json_run_averages(env):
    result, expected = _run_retry_json(("once", "fail"), requests=3)
    runs = [result["RUN #{} RESULTS".format(i + 1)] for i in range(2)]
    for section, counts in zip(runs, expected):
        _assert_totals(section, counts)
    for label in ("BEST RUN RESULTS", "WORST RUN RESULTS"):
        selected = result[label]["Totals"]
        # Selection must retain the actual run object, including its counters.
        assert selected in [section["Totals"] for section in runs]
    average = tuple(max(1, (a + b + 1) // 2) if a + b else 0
                    for a, b in zip(*expected))
    assert average == (2, 5, 3)
    _assert_totals(result["AGGREGATED AVERAGE RESULTS (2 runs)"], average)


def test_retry_json_interrupted_series(env):
    if os.name == "nt":
        # Popen SIGINT is not a Windows console Ctrl+C event.
        env.skip()
    with tempfile.TemporaryDirectory() as directory, _RetryJsonPeer(("once", "once"), 1) as peer:
        peer.pause_run = 1
        serving = threading.Thread(target=peer.serve_forever)
        serving.start()
        child = None
        try:
            output = os.path.join(directory, "result.json")
            args = [MEMTIER_BINARY, "-s", "127.0.0.1", "-p", str(peer.server_address[1]),
                    "-a", "dummy-646", "-t", "1", "-c", "1", "-n", "6", "--pipeline=1",
                    "--ratio=1:0", "--key-pattern=S:S", "--key-minimum=1", "--key-maximum=100000",
                    "--retry-on-error", "--max-retries=2", "--retry-backoff-ms=0",
                    "--run-count=5", "--print-all-runs", "--hide-histogram", "--json-out-file=" + output]
            child = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            assert peer.paused.wait(10), "second run did not reach its replay"
            child.send_signal(signal.SIGINT)
            stdout, stderr = child.communicate(timeout=10)
            assert child.returncode == 0, stdout + stderr
            with open(output) as stream:
                result = json.load(stream)
            assert peer.connections == 2
            assert not peer.failures, peer.failures
            assert "RUN #3 RESULTS" not in result
            first, second = (result["RUN #{} RESULTS".format(i)] for i in (1, 2))
            _assert_totals(first, (0, 6, 6))
            assert second["Runtime"]["Interrupted"] == "true"
            # Every first attempt received TRYAGAIN before the peer paused a replay.
            queued = sum(key[0] == 1 for key in peer.attempts)
            assert queued > 0
            assert second["Totals"]["Retry Attempts"] == queued, second["Totals"]
            assert second["Totals"]["Retried Ops"] == queued, second["Totals"]
            average = tuple(max(1, (first["Totals"][field] + second["Totals"][field] + 1) // 2)
                            if first["Totals"][field] + second["Totals"][field] else 0 for field in _FIELDS)
            # The existing header uses the configured run count, even after
            # interruption. Counter means must use the two actual run objects.
            aggregates = [section for label, section in result.items()
                          if label.startswith("AGGREGATED AVERAGE RESULTS")]
            assert len(aggregates) == 1
            _assert_totals(aggregates[0], average)
        finally:
            if child is not None and child.poll() is None:
                child.kill()
                child.communicate(timeout=3)
            peer.stop.set()
            peer.shutdown()
            serving.join(timeout=5)
