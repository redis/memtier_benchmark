"""BEST/WORST prefer complete runs, keeping partial runs in other outputs (#711)."""
import json
import os
import signal
import socketserver
import subprocess
import tempfile
import threading
import time

from include import MEMTIER_BINARY


class _Peer(socketserver.ThreadingTCPServer):
    daemon_threads = True

    def __init__(self, modes):
        super().__init__(("127.0.0.1", 0), _Handler)
        self.modes = modes
        self.lock = threading.Lock()
        self.connections = 0
        self.errors = []
        self.started = [threading.Event() for _ in modes]
        self.stop = threading.Event()


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        with self.server.lock:
            number = self.server.connections
            self.server.connections += 1
        try:
            mode = self.server.modes[number]
            self.connection.settimeout(20)
            commands = 0
            while not self.server.stop.is_set():
                line = self.rfile.readline()
                if not line:
                    return
                args = []
                for _ in range(int(line[1:])):
                    size = int(self.rfile.readline()[1:])
                    args.append(self.rfile.read(size))
                    assert self.rfile.read(2) == b"\r\n"
                if args[0] == b"SET":
                    self.server.started[number].set()
                    commands += 1
                    if mode in ("drop", "slow-drop", "late-drop") and commands == 4:
                        if mode == "late-drop":
                            time.sleep(1.3)
                        return
                    if mode == "stall":
                        self.server.stop.wait(20)
                        return
                    if mode in ("slow", "slow-drop", "slower"):
                        time.sleep(0.04 if mode == "slower" else 0.02)
                else:
                    assert args == [b"AUTH", b"dummy-711"], args
                self.wfile.write(b"-ERR dummy command rejection\r\n" if mode == "error" and args[0] == b"SET"
                                 else b"+OK\r\n")
        except (BrokenPipeError, ConnectionResetError):
            pass  # The interrupted run can close while a delayed reply is sent.
        except Exception as error:
            self.server.errors.append(repr(error))


def _run(env, modes, interrupt=None, requests=False, expected_exit=0):
    if env.isCluster() or env.useTLS or (interrupt is not None and os.name == "nt"):
        # Scripted plaintext peer; POSIX SIGINT does not emulate Windows Ctrl+C.
        env.skip()
    with _Peer(modes) as peer, tempfile.TemporaryDirectory() as directory:
        thread = threading.Thread(target=peer.serve_forever)
        thread.start()
        child = None
        try:
            args = [MEMTIER_BINARY, "-s", "127.0.0.1", "-p", str(peer.server_address[1]),
                    "--authenticate=dummy-711", "-t", "1", "-c", "1", "--pipeline=1", "--ratio=1:0",
                    "--print-all-runs", "--run-count=" + str(3 if interrupt is not None else len(modes)),
                    "--hide-histogram",
                    "--json-out-file=" + os.path.join(directory, "stats.json"),
                    "--hdr-file-prefix=" + os.path.join(directory, "hist"),
                    "--requests=50" if requests else "--test-time=1"]
            child = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if interrupt is not None:
                assert peer.started[interrupt - 1].wait(18), "interrupted run never started"
                time.sleep(0.3)
                child.send_signal(signal.SIGINT)
            stdout, stderr = child.communicate(timeout=20)
            env.assertEqual(child.returncode, expected_exit, message=stderr[-3000:])
            with open(os.path.join(directory, "stats.json")) as source:
                results = json.load(source)
            # External HDR files retain run identities independently of ranking.
            for number in range(1, len(modes) + 1):
                stats = results["RUN #{} RESULTS".format(number)]
                path = os.path.join(directory, "hist_FULL_RUN_{}.hgrm".format(number))
                if stats["Totals"]["Count"]:
                    with open(path) as source:
                        encoded = [line.strip().split(",")[-1] for line in source
                                   if line and line[0].isdigit()][-1]
                    env.assertEqual(encoded, stats["Totals"]["Percentile Latencies"]
                                    ["Histogram log format"]["Compressed Histogram"])
                else:
                    env.assertFalse(os.path.exists(path))
            return results, stdout
        finally:
            if child is not None and child.poll() is None:
                child.kill()
                child.communicate(timeout=3)
            peer.stop.set()
            peer.shutdown()
            thread.join(timeout=3)
            env.assertEqual(peer.errors, [])
            env.assertEqual(peer.connections, len(modes))


def _check_selection(env, results, stdout, number, label):
    run_label = "RUN #{} RESULTS".format(number)
    # Compare the full selected object, including its compressed histogram,
    # timestamps, interruption flag, and command-specific counts.
    env.assertEqual(results[label], results[run_label])
    def table(title):
        section = stdout.split(title + "\n", 1)[1].split("\n\n", 1)[0]
        return section
    env.assertEqual(table(label), table(run_label))


def _check_partial(env, modes, fastest, requests=False):
    results, stdout = _run(env, modes, interrupt=2, requests=requests)
    complete = results["RUN #1 RESULTS"]
    partial = results["RUN #2 RESULTS"]
    env.assertEqual(complete["Runtime"]["Interrupted"], "false")
    env.assertEqual(partial["Runtime"]["Interrupted"], "true")
    if fastest:
        env.assertGreater(partial["Totals"]["Ops/sec"], complete["Totals"]["Ops/sec"])
    else:
        env.assertLess(partial["Totals"]["Ops/sec"], complete["Totals"]["Ops/sec"])
    for label in ("BEST RUN RESULTS", "WORST RUN RESULTS"):
        _check_selection(env, results, stdout, 1, label)
    env.assertEqual(results["run information"]["Runs completed"], 2)
    aggregate = results["AGGREGATED AVERAGE RESULTS (3 runs)"]
    env.assertEqual(aggregate["Runtime"]["Interrupted"], "true")
    env.assertEqual(aggregate["Totals"]["Count"],
                    (complete["Totals"]["Count"] + partial["Totals"]["Count"] + 1) // 2)
    return complete


def test_partial_fastest_does_not_win(env):
    _check_partial(env, ["slow", "fast"], fastest=True)


def test_partial_slowest_does_not_win(env):
    _check_partial(env, ["fast", "slow"], fastest=False)


def test_zero_throughput_complete_run_is_eligible(env):
    # A timed run with no terminal reply expires its drain grace, without an
    # interrupt. Zero throughput is a measurement, not a missing candidate.
    complete = _check_partial(env, ["stall", "fast"], fastest=True)
    env.assertEqual(complete["Totals"]["Count"], 0)
    env.assertEqual(complete["Runtime"]["Unanswered requests"], 1)


def test_only_partial_run_is_fallback(env):
    results, stdout = _run(env, ["slow"], interrupt=1)
    for label in ("BEST RUN RESULTS", "WORST RUN RESULTS"):
        _check_selection(env, results, stdout, 1, label)
        env.assertEqual(results[label]["Runtime"]["Interrupted"], "true")
    env.assertEqual(results["run information"]["Runs completed"], 1)


def test_request_count_partial_slowest_does_not_win(env):
    _check_partial(env, ["fast", "slow"], fastest=False, requests=True)


def test_complete_request_count_runs_rank_by_throughput(env):
    results, stdout = _run(env, ["slow", "fast", "slower"], requests=True)
    rates = [results["RUN #{} RESULTS".format(i)]["Totals"]["Ops/sec"] for i in range(1, 4)]
    env.assertGreater(rates[1], rates[0])
    env.assertGreater(rates[0], rates[2])
    _check_selection(env, results, stdout, 2, "BEST RUN RESULTS")
    _check_selection(env, results, stdout, 3, "WORST RUN RESULTS")
    env.assertEqual(results["AGGREGATED AVERAGE RESULTS (3 runs)"]["Runtime"]["Interrupted"], "false")


def test_failed_worker_then_complete_run(env):
    results, stdout = _run(env, ["drop", "slow"], requests=True, expected_exit=1)
    failed = results["RUN #1 RESULTS"]
    complete = results["RUN #2 RESULTS"]
    env.assertEqual(failed["Totals"]["Count"], 3)
    env.assertEqual(complete["Totals"]["Count"], 50)
    env.assertGreater(failed["Totals"]["Ops/sec"], complete["Totals"]["Ops/sec"])
    env.assertEqual(failed["Runtime"]["Interrupted"], "true")
    env.assertEqual(complete["Runtime"]["Interrupted"], "false")
    for label in ("BEST RUN RESULTS", "WORST RUN RESULTS"):
        _check_selection(env, results, stdout, 2, label)
    aggregate = results["AGGREGATED AVERAGE RESULTS (2 runs)"]
    env.assertEqual(aggregate["Runtime"]["Interrupted"], "true")
    env.assertEqual(aggregate["Totals"]["Count"], 27)


def test_all_failed_workers_keep_fallback(env):
    results, stdout = _run(env, ["drop", "slow-drop"], requests=True, expected_exit=1)
    for number in (1, 2):
        stats = results["RUN #{} RESULTS".format(number)]
        env.assertEqual(stats["Totals"]["Count"], 3)
        env.assertEqual(stats["Runtime"]["Interrupted"], "true")
    env.assertGreater(results["RUN #1 RESULTS"]["Totals"]["Ops/sec"],
                      results["RUN #2 RESULTS"]["Totals"]["Ops/sec"])
    _check_selection(env, results, stdout, 1, "BEST RUN RESULTS")
    _check_selection(env, results, stdout, 2, "WORST RUN RESULTS")
    env.assertEqual(results["AGGREGATED AVERAGE RESULTS (2 runs)"]["Runtime"]["Interrupted"], "true")


def test_command_errors_remain_eligible(env):
    results, stdout = _run(env, ["error", "slow"], requests=True)
    for number in (1, 2):
        stats = results["RUN #{} RESULTS".format(number)]
        env.assertEqual(stats["Totals"]["Count"], 50)
        env.assertEqual(stats["Runtime"]["Interrupted"], "false")
    env.assertGreater(results["RUN #1 RESULTS"]["Totals"]["Ops/sec"],
                      results["RUN #2 RESULTS"]["Totals"]["Ops/sec"])
    _check_selection(env, results, stdout, 1, "BEST RUN RESULTS")
    _check_selection(env, results, stdout, 2, "WORST RUN RESULTS")
    env.assertEqual(results["AGGREGATED AVERAGE RESULTS (2 runs)"]["Runtime"]["Interrupted"], "false")


def test_worker_failure_after_timed_deadline_is_partial(env):
    # The existing worker lifecycle still classifies a dropped pending reply
    # as failure after the timer expired. Do not infer validity from duration.
    results, stdout = _run(env, ["late-drop", "slow"], expected_exit=1)
    failed = results["RUN #1 RESULTS"]
    env.assertGreater(failed["Runtime"]["Total duration"], 1000)
    env.assertEqual(failed["Totals"]["Count"], 3)
    env.assertEqual(failed["Runtime"]["Interrupted"], "true")
    env.assertEqual(results["RUN #2 RESULTS"]["Runtime"]["Interrupted"], "false")
    for label in ("BEST RUN RESULTS", "WORST RUN RESULTS"):
        _check_selection(env, results, stdout, 2, label)
