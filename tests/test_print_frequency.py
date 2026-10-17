"""Progress cadence is independent of benchmark supervision and sampling."""
import json
import os
import re
import socket
import subprocess
import tempfile

from include import (MEMTIER_BINARY, add_required_env_arguments, addTLSArgs,
                     get_default_memtier_config)
from mb import Benchmark, RunConfig


def _run(env, extra, seconds=None, requests=None):
    specs = {"name": env.testName, "args": ["--ratio=1:0", "--rate-limiting=100"] + extra}
    addTLSArgs(specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=requests, test_time=seconds)
    add_required_env_arguments(specs, config, env, env.getMasterNodesList())
    password = env.getConnection().connection_pool.connection_kwargs.get("password")
    if password:
        specs["args"].append("--authenticate=" + password)
    with tempfile.TemporaryDirectory() as directory:
        config = RunConfig(directory, env.testName, config, {})
        os.makedirs(config.results_dir)
        benchmark = Benchmark.from_json(config, specs)
        ok = benchmark.run(timeout=25)
        with open(os.path.join(config.results_dir, "mb.stderr")) as source:
            stderr = source.read()
        env.assertTrue(ok, message=stderr)
        with open(os.path.join(config.results_dir, "mb.json")) as source:
            results = json.load(source)
    frames = [line for line in stderr.splitlines()
              if re.search(r"\[RUN #\d+ .*\]", line) and
              (" ops," in line or "] throughput " in line)]
    return frames, results


def test_print_frequency_validation(env):
    for value in ("0", "-1", "-0", "", "1.5", "2x", "4294967296", "999999999999999999999"):
        result = subprocess.run([MEMTIER_BINARY, "--print-frequency=" + value, "--version"],
                                capture_output=True, text=True, timeout=10)
        env.assertEqual(result.returncode, 2, message=repr(value) + result.stderr)
        env.assertContains("print-frequency must be", result.stderr)
    for value in ("1", "2", "4294967295"):
        result = subprocess.run([MEMTIER_BINARY, "-f", value, "--version"],
                                capture_output=True, text=True, timeout=10)
        env.assertEqual(result.returncode, 0, message=repr(value) + result.stderr)


def test_print_frequency_default_and_short_option(env):
    frames, results = _run(env, [], seconds=3)
    env.assertGreaterEqual(len(frames), 3)
    env.assertEqual(results["configuration"]["print_frequency"], 1)
    env.assertGreater(results["ALL STATS"]["Totals"]["Count"], 0)
    # The short spelling and the largest accepted value must still render the
    # final frame even when no periodic frame was due.
    frames, results = _run(env, ["-f", "4294967295"], requests=10)
    env.assertEqual(len(frames), 1)
    env.assertContains("100%", frames[0])
    env.assertEqual(results["configuration"]["print_frequency"], 4294967295)
    env.assertEqual(results["ALL STATS"]["Totals"]["Count"], 10)


def test_print_frequency_throttles_both_displays(env):
    for display in ([], ["--realtime-latencies"]):
        frames, results = _run(env, ["--print-frequency=2"] + display, seconds=5)
        # At least one periodic frame plus completion; fewer frames than the
        # unthrottled five-second run. Bounds allow a delayed supervisor tick.
        env.assertGreaterEqual(len(frames), 2, message=str(frames))
        env.assertLessEqual(len(frames), 4, message=str(frames))
        env.assertContains("100%", frames[-1])
        env.assertEqual(results["configuration"]["print_frequency"], 2)
        env.assertGreater(results["ALL STATS"]["Totals"]["Count"], 0)


def test_print_frequency_final_frame_each_run(env):
    frames, results = _run(env, ["--print-frequency=100", "--run-count=2"], requests=10)
    env.assertEqual(len(frames), 2, message=str(frames))
    if len(frames) == 2:
        env.assertContains("RUN #1", frames[0])
        env.assertContains("RUN #2", frames[1])
    env.assertTrue(all("100%" in frame for frame in frames))
    env.assertEqual(results["configuration"]["run_count"], 2)


def test_print_frequency_does_not_throttle_metrics(env):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver, socket.socket() as graphite:
        # Reserve an unlistened TCP endpoint so event annotations cannot reach
        # an unrelated Graphite service on the default port.
        graphite.bind(("127.0.0.1", 0))
        receiver.bind(("127.0.0.1", 0))
        receiver.settimeout(0.1)
        frames, results = _run(env, ["--print-frequency=100", "--statsd-host=127.0.0.1",
                                   "--statsd-port=" + str(receiver.getsockname()[1]),
                                   "--graphite-port=" + str(graphite.getsockname()[1]),
                                   "--statsd-prefix=frequency"], seconds=3)
        packets = []
        while True:
            try:
                packets.append(receiver.recv(65535))
            except socket.timeout:
                break
    env.assertEqual(len(frames), 1, message=str(frames))
    # One ops_sec gauge per supervisor tick, plus a final zero on completion.
    samples = [packet for packet in packets if packet.startswith(b"frequency.default.ops_sec:")]
    env.assertGreaterEqual(len(samples), 4, message=repr(packets))
    env.assertGreaterEqual(len(results["ALL STATS"]["CPU Stats"]), 3)
    env.assertGreater(results["ALL STATS"]["Totals"]["Count"], 0)


def test_print_frequency_keeps_latency_samples_from_hidden_ticks(env):
    """A slow first request must survive until the sole final latency frame."""
    import socketserver
    import threading
    import time

    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            first_set = True
            while True:
                try:
                    line = self.rfile.readline()
                except ConnectionResetError:
                    return
                if not line:
                    return
                parts = []
                for _ in range(int(line[1:])):
                    size = int(self.rfile.readline()[1:])
                    parts.append(self.rfile.read(size))
                    self.rfile.read(2)
                if parts[0].upper() == b"SET" and first_set:
                    first_set = False
                    time.sleep(0.25)
                self.wfile.write(b"+OK\r\n")
                self.wfile.flush()

    with socketserver.TCPServer(("127.0.0.1", 0), Handler) as peer:
        thread = threading.Thread(target=peer.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                path = os.path.join(directory, "stats.json")
                result = subprocess.run(
                    [MEMTIER_BINARY, "-s", "127.0.0.1", "-p", str(peer.server_address[1]),
                     "-a", "dummy-frequency", "-t", "1", "-c", "1", "--ratio=1:0",
                     "--requests=250", "--rate-limiting=100", "--print-frequency=100",
                     "--realtime-latencies", "--print-percentiles=100", "--json-out-file=" + path],
                    capture_output=True, text=True, timeout=15)
                env.assertEqual(result.returncode, 0, message=result.stderr)
                latencies = re.findall(r"latency\s+p100\s+[^\n]*?\(avg:\s*([\d.]+)\)", result.stderr)
                env.assertEqual(len(latencies), 1, message=result.stderr)
                if latencies:
                    env.assertGreater(float(latencies[0]), 200, message=result.stderr)
                with open(path) as source:
                    env.assertEqual(json.load(source)["ALL STATS"]["Totals"]["Count"], 250)
        finally:
            peer.shutdown()
            thread.join(timeout=5)
