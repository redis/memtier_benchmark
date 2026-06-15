"""
Tests for StatsD metrics output feature.

Uses a lightweight in-process UDP server to capture StatsD packets emitted
by memtier_benchmark, so there is no dependency on a real Graphite/StatsD
installation.  Infrastructure issues (e.g. can't bind socket, benchmark
didn't start) cause a graceful skip; once metrics are received, assertions
are enforced.

Local Graphite testing:
  docker compose -f docker-compose.statsd.yml up -d
  # wait ~10s for Graphite to be ready, then:
  STATSD_HOST=localhost TEST=test_statsd OSS_STANDALONE=1 ./tests/run_tests.sh
"""
import socket
import threading
import tempfile
import shutil
import time
import os
import re
import logging

from include import (
    get_default_memtier_config,
    add_required_env_arguments,
    addTLSArgs,
    ensure_clean_benchmark_folder,
    MEMTIER_BINARY,
)
from mb import Benchmark, RunConfig


# ---------------------------------------------------------------------------
# StatsD wire pins (PLAN §10 "StatsD wire pins", Decisions #46)
#
# Per-metric value-format regexes that pin the exact printf shape of every
# StatsD payload, so a wire regression (a dropped (long) cast, a
# gauge(double)->gauge(long) overload swap, a renamed/new metric) turns the
# test RED instead of passing silently.
#
# Exact printf shapes: line = "<prefix><name>:<value>|<type>" (statsd.cpp:141);
#   gauge(long)   "%ld"   |g   (statsd.cpp:156-160)  -> integer, no decimal
#   gauge(double) "%.6f"  |g   (statsd.cpp:149-153)  -> exactly 6 decimals
#   timing(double)"%.3f"  |ms  (statsd.cpp:163-167)  -> exactly 3 decimals
#
# Two distinct emit sites carry these gauges (memtier_benchmark.cpp):
#   - the 1 Hz tick (:3221-3253): ops_sec/bytes_sec/connections via gauge(long)
#     "%ld"; progress_pct via gauge(double) "%.6f"; latency_p* via gauge(double).
#   - the run-end zeroing block (:3270-3274): ops_sec, ops_sec_avg, bytes_sec,
#     bytes_sec_avg AND progress_pct are all forced to gauge((long) 0) -> "0|g".
# So progress_pct legitimately appears in BOTH the "%.6f" tick form and the
# integer "0|g" run-end form; its pin must accept both (this is today's wire).
# ---------------------------------------------------------------------------

INT_G = re.compile(r":-?\d+\|g$")          # integer gauge: no decimal point, ever
F6_G = re.compile(r":\d+\.\d{6}\|g$")      # double gauge: exactly 6 decimals
F3_MS = re.compile(r":\d+\.\d{3}\|ms$")    # timing: exactly 3 decimals
# progress_pct: "%.6f" gauge at tick time, but the run-end block zeroes it via
# gauge((long) 0) -> exactly ":0|g". Anything else (e.g. a non-zero integer, or
# "0.000000" from a dropped overload) is a regression.
F6_G_OR_ZERO = re.compile(r":(?:\d+\.\d{6}|0)\|g$")

VALUE_FORMAT_RES = {
    "ops_sec": INT_G,
    "ops_sec_avg": INT_G,
    "bytes_sec": INT_G,
    "bytes_sec_avg": INT_G,
    "connections": INT_G,
    "connection_errors": INT_G,
    "progress_pct": F6_G_OR_ZERO,
    "latency_ms": F3_MS,
    "latency_avg_ms": F3_MS,
}
LATENCY_PCT_NAME = re.compile(r"^latency_p[0-9_]+$")   # value class F6_G


# ---------------------------------------------------------------------------
# Lightweight mock StatsD UDP server
# ---------------------------------------------------------------------------

class MockStatsdServer:
    """
    Binds a UDP socket on a random local port and collects every datagram
    received until stop() is called.

    Each StatsD datagram may contain multiple metrics separated by '\\n'.
    We split them so self.metrics is a flat list of individual metric lines.
    """

    def __init__(self):
        self.sock = None
        self.port = None
        self.metrics = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread = None

    def start(self):
        """Bind socket and launch receiver thread.  Returns False on failure."""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind(("127.0.0.1", 0))
            self.sock.settimeout(0.1)
            self.port = self.sock.getsockname()[1]
        except OSError as exc:
            logging.warning("MockStatsdServer: could not bind UDP socket: %s", exc)
            return False

        self._thread = threading.Thread(target=self._recv_loop, daemon=True)
        self._thread.start()
        return True

    def _recv_loop(self):
        while not self._stop_event.is_set():
            try:
                data, _ = self.sock.recvfrom(65535)
                lines = data.decode("utf-8", errors="replace").strip().split("\n")
                with self._lock:
                    self.metrics.extend(lines)
            except socket.timeout:
                pass
            except OSError:
                break

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2)
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass

    def get_metrics(self):
        with self._lock:
            return list(self.metrics)

    def metric_names(self):
        """Return the set of metric names seen (part before the first ':')."""
        names = set()
        for line in self.get_metrics():
            if ":" in line:
                names.add(line.split(":")[0])
        return names


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_benchmark(env, test_dir, extra_args):
    """Build a Benchmark object adapted to the current RLTest environment."""
    benchmark_specs = {
        "name": env.testName,
        "args": extra_args,
    }
    addTLSArgs(benchmark_specs, env)

    # Short run: 3 seconds is enough to emit several metric packets
    config = get_default_memtier_config(
        threads=2, clients=4, requests=None, test_time=3
    )
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)

    return Benchmark.from_json(run_config, benchmark_specs)


def _skip(env, reason):
    """Log a warning and mark the test as passed (best-effort behaviour)."""
    logging.warning("test_statsd: skipping — %s", reason)
    print(f"[SKIP] test_statsd: {reason}")
    env.assertTrue(True)


# ---------------------------------------------------------------------------
# Test: basic metric delivery
# ---------------------------------------------------------------------------

def test_statsd_metrics_emitted(env):
    """
    Verify that memtier_benchmark sends StatsD gauges/timings over UDP when
    --statsd-host is supplied.

    Expected metric names (prefix.run_label.name):
      memtier.ci_run.ops_sec
      memtier.ci_run.ops_sec_avg
      memtier.ci_run.bytes_sec
      memtier.ci_run.bytes_sec_avg
      memtier.ci_run.latency_ms       (sent as timing |ms)
      memtier.ci_run.latency_avg_ms   (sent as timing |ms)
      memtier.ci_run.latency_p50      (gauge; one per --print-percentiles entry)
      memtier.ci_run.latency_p99
      memtier.ci_run.latency_p99_9
      memtier.ci_run.connections
      memtier.ci_run.progress_pct
      memtier.ci_run.connection_errors  (only when connection errors occur)
    """
    server = MockStatsdServer()
    if not server.start():
        _skip(env, "could not bind UDP socket for mock StatsD server")
        return

    test_dir = tempfile.mkdtemp()
    try:
        run_label = "ci_run"
        prefix = "memtier"

        extra_args = [
            "--statsd-host=127.0.0.1",
            f"--statsd-port={server.port}",
            f"--statsd-prefix={prefix}",
            f"--statsd-run-label={run_label}",
        ]

        benchmark = _build_benchmark(env, test_dir, extra_args)
        print(f"Running: {' '.join(benchmark.args)}")
        ok = benchmark.run()

        # Give the receiver thread a moment to drain the socket buffer
        time.sleep(0.3)
        server.stop()

        # This test supplies --statsd-host over loopback UDP with --test-time=3
        # (>=2 ticks of the 1 Hz loop), so silence here IS the wire regression:
        # hard-fail instead of skip-pass (PLAN §10 pin 2, Decisions #46).
        if not ok:
            env.assertTrue(
                False,
                message="statsd hard-fail: memtier_benchmark exited with non-zero status",
            )
            return

        received = server.get_metrics()
        if not received:
            env.assertTrue(
                False,
                message="statsd hard-fail: no StatsD datagrams received "
                "(benchmark may have finished too quickly)",
            )
            return

        print(f"Received {len(received)} StatsD metric lines")
        for line in received[:20]:          # show first 20 for diagnostics
            print(f"  {line}")
        if len(received) > 20:
            print(f"  ... ({len(received) - 20} more)")

        # Every metric line must start with "<prefix>.<run_label>."
        expected_prefix = f"{prefix}.{run_label}."
        bad_lines = [m for m in received if not m.startswith(expected_prefix)]
        env.assertEqual(
            len(bad_lines), 0,
            message=f"Lines without expected prefix '{expected_prefix}': {bad_lines[:5]}"
        )

        # Check that the core gauge/timing metrics were seen
        names = server.metric_names()
        print(f"Distinct metric names seen: {sorted(names)}")

        required_metrics = [
            f"{expected_prefix}ops_sec",
            f"{expected_prefix}ops_sec_avg",
            f"{expected_prefix}bytes_sec",
            f"{expected_prefix}latency_ms",
            f"{expected_prefix}connections",
        ]

        missing = [m for m in required_metrics if m not in names]
        env.assertEqual(
            len(missing), 0,
            message=f"Missing required metrics (received {len(received)} lines, "
            f"names: {sorted(names)}): {missing}"
        )

        # Verify StatsD wire format: each line must contain ':' and '|'
        malformed = [m for m in received if ":" not in m or "|" not in m]
        env.assertEqual(
            len(malformed), 0,
            message=f"Malformed metric lines (missing ':' or '|'): {malformed[:5]}"
        )

        # Per-metric value-format pins (PLAN §10 pin 1, Decisions #46): strip the
        # prefix to recover each metric name, then assert its payload matches the
        # pinned printf shape. An unknown metric name is a wire change and FAILS
        # (it must update these pins); a value-format mismatch (e.g. progress_pct
        # routed through gauge(long), or a dropped (long) cast) also FAILS.
        bad_fmt = []
        unknown = []
        for line in received:
            if not line.startswith(expected_prefix):
                continue  # prefix violations already asserted above
            name = line[len(expected_prefix):].split(":", 1)[0]
            if name in VALUE_FORMAT_RES:
                regex = VALUE_FORMAT_RES[name]
            elif LATENCY_PCT_NAME.match(name):
                regex = F6_G
            else:
                unknown.append(line)
                continue
            if not regex.search(line):
                bad_fmt.append(line)
        env.assertEqual(
            len(bad_fmt) + len(unknown), 0,
            message=f"StatsD wire value-format regressions — "
            f"unknown metric names {unknown[:5]}; "
            f"bad value formats {bad_fmt[:5]}"
        )

    finally:
        server.stop()
        shutil.rmtree(test_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Test: custom prefix and run-label are applied correctly
# ---------------------------------------------------------------------------

def test_statsd_prefix_and_label(env):
    """
    Verify that --statsd-prefix and --statsd-run-label are reflected in the
    metric names sent over the wire.
    """
    server = MockStatsdServer()
    if not server.start():
        _skip(env, "could not bind UDP socket for mock StatsD server")
        return

    test_dir = tempfile.mkdtemp()
    try:
        custom_prefix = "mybench"
        custom_label = "label_test_42"

        extra_args = [
            "--statsd-host=127.0.0.1",
            f"--statsd-port={server.port}",
            f"--statsd-prefix={custom_prefix}",
            f"--statsd-run-label={custom_label}",
        ]

        benchmark = _build_benchmark(env, test_dir, extra_args)
        ok = benchmark.run()

        time.sleep(0.3)
        server.stop()

        # This test supplies --statsd-host over loopback UDP; silence IS the
        # regression -> hard-fail, not skip-pass (PLAN §10 pin 2, Decisions #46).
        if not ok:
            env.assertTrue(
                False,
                message="statsd hard-fail: memtier_benchmark exited with non-zero status",
            )
            return

        received = server.get_metrics()
        if not received:
            env.assertTrue(
                False,
                message="statsd hard-fail: no StatsD datagrams received",
            )
            return

        expected_prefix = f"{custom_prefix}.{custom_label}."
        matching = [m for m in received if m.startswith(expected_prefix)]
        non_matching = [m for m in received if not m.startswith(expected_prefix)]

        print(f"Received {len(received)} total metric lines, "
              f"{len(matching)} with prefix '{expected_prefix}'")

        env.assertGreater(
            len(matching), 0,
            message=f"No metrics matched expected prefix '{expected_prefix}'. "
            f"Sample received: {received[:5]}"
        )
        env.assertEqual(
            len(non_matching), 0,
            message=f"All metrics should use prefix '{expected_prefix}': {non_matching[:5]}"
        )

    finally:
        server.stop()
        shutil.rmtree(test_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Test: no metrics when --statsd-host is omitted (default behaviour)
# ---------------------------------------------------------------------------

def test_statsd_disabled_by_default(env):
    """
    When --statsd-host is not passed, memtier_benchmark must still complete
    successfully and no StatsD traffic should be sent to port 8125.

    We bind a UDP socket on 8125 only if it is free; otherwise we skip.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 8125))
        sock.settimeout(0.1)
    except OSError:
        _skip(env, "port 8125 already in use — cannot verify silence")
        return

    test_dir = tempfile.mkdtemp()
    try:
        # No --statsd-host argument
        extra_args = []
        benchmark = _build_benchmark(env, test_dir, extra_args)
        ok = benchmark.run()

        # Drain for a moment
        packets = []
        deadline = time.time() + 0.5
        while time.time() < deadline:
            try:
                data, _ = sock.recvfrom(65535)
                packets.append(data)
            except socket.timeout:
                break

        env.assertTrue(ok, message="memtier_benchmark should succeed without --statsd-host")
        env.assertEqual(
            len(packets), 0,
            message=f"Received {len(packets)} unexpected UDP packets on port 8125 "
            f"when --statsd-host was not set"
        )

    finally:
        sock.close()
        shutil.rmtree(test_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Test: end-to-end Graphite integration (requires STATSD_HOST env var)
# ---------------------------------------------------------------------------

def test_statsd_graphite_integration(env):
    """Verify metrics appear in Graphite when STATSD_HOST is set."""
    import urllib.request
    import json as _json

    statsd_host = os.environ.get("STATSD_HOST", "")
    if not statsd_host:
        _skip(env, "STATSD_HOST not set")
        return

    graphite_base = f"http://{statsd_host}:8080"
    try:
        urllib.request.urlopen(f"{graphite_base}/render", timeout=5)
    except Exception as exc:
        _skip(env, f"Graphite not reachable at {graphite_base}: {exc}")
        return

    test_dir = tempfile.mkdtemp()
    try:
        run_label = "ci_graphite"
        prefix = "memtier"
        extra_args = [
            f"--statsd-host={statsd_host}", "--statsd-port=8125",
            f"--statsd-prefix={prefix}", f"--statsd-run-label={run_label}",
        ]
        benchmark = _build_benchmark(env, test_dir, extra_args)
        ok = benchmark.run()
        if not ok:
            _skip(env, "memtier_benchmark exited with non-zero status")
            return

        # Poll Graphite until metrics appear (handles variable flushInterval)
        pattern = f"stats.gauges.{prefix}.{run_label}.*"
        query_url = f"{graphite_base}/metrics/find?query={pattern}"
        found = []
        for attempt in range(15):
            time.sleep(2)
            try:
                resp = urllib.request.urlopen(query_url, timeout=5)
                found = _json.loads(resp.read())
                if found:
                    break
            except Exception:
                pass
            print(f"  Graphite poll {attempt + 1}/15: {len(found)} metrics so far")

        env.assertGreater(
            len(found), 0,
            message=f"No metrics found in Graphite after 30s polling at {query_url}"
        )
        print(f"Found {len(found)} metrics in Graphite: {[m['id'] for m in found[:5]]}")
    finally:
        shutil.rmtree(test_dir, ignore_errors=True)
