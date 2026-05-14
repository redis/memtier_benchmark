"""
Tests for --realtime-latencies.

When the flag is set, memtier_benchmark replaces the per-second
'[RUN #N ...] ops/sec' progress line with a per-tick block:

  [RUN #N  P%  Ts] throughput <cur> (avg: <avg>) ops/sec   <cur>/sec (avg: <avg>/sec)   miss X.XX% (avg: Y.YY%)
  [RUN #N  P%  Ts] latency  p50  cur (avg: avg)  p99  cur (avg: avg)  p99.9  cur (avg: avg) ms
  [RUN #N  P%  Ts] latency  p99.99  cur (avg: avg)  ...                                                          (if >3 percentiles)

The block redraws in place on a TTY (cursor-up + erase-EOL); on non-TTY
stderr (file capture / pipe) each tick is just appended, producing a clean
per-second time series. The RLTest harness runs memtier as a subprocess
with stderr captured to a pipe, so these tests exercise the non-TTY path.
"""
import json
import os
import re
import tempfile

from include import (
    get_default_memtier_config,
    add_required_env_arguments,
    addTLSArgs,
    ensure_clean_benchmark_folder,
    debugPrintMemtierOnError,
)
from mb import Benchmark, RunConfig


def _read_stderr(config):
    path = os.path.join(config.results_dir, "mb.stderr")
    with open(path, "rb") as fh:
        return fh.read().decode("utf-8", errors="replace")


def _read_json(config):
    path = os.path.join(config.results_dir, "mb.json")
    with open(path) as fh:
        return json.load(fh)


def test_realtime_latencies_emits_two_line_block(env):
    """Per tick we get one 'throughput' line and one 'latency' line (when
    the configured percentile list fits on one wrapped line). Each line
    carries an immediate value plus '(avg: X)' for the overall counterpart,
    miss% is present, and the legacy absolute ops counter is gone."""
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=10000",
            "--realtime-latencies",
            "--print-percentiles=50,99,99.9",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=2, requests=None, test_time=3)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)
    memtier_ok = benchmark.run()
    try:
        env.assertTrue(memtier_ok, message="memtier_benchmark --realtime-latencies exited non-zero")
        stderr = _read_stderr(config)

        thr_lines = [ln for ln in stderr.splitlines() if re.search(r"\[RUN #\d+ +\d+%.*\] throughput ", ln)]
        lat_lines = [ln for ln in stderr.splitlines() if re.search(r"\[RUN #\d+ +\d+%.*\] latency ", ln)]
        env.assertTrue(len(thr_lines) >= 1, message="expected at least one 'throughput' line; stderr=\n%s" % stderr[:2000])
        env.assertTrue(len(lat_lines) >= 1, message="expected at least one 'latency' line; stderr=\n%s" % stderr[:2000])

        # Throughput line must carry both immediate ops/sec and a parenthesized
        # avg, plus a miss% with its own avg (e.g. "miss 99.50% (avg: 99.40%)").
        ops_avg_ok = any(re.search(r"\(avg:\s*[\d,]+\)\s*ops/sec", ln) for ln in thr_lines)
        env.assertTrue(ops_avg_ok, message="ops/sec line missing '(avg: N)'; got:\n%s" % "\n".join(thr_lines[:5]))
        miss_ok = any(re.search(r"miss\s+\d+\.\d+%\s+\(avg:\s*\d+\.\d+%\)", ln) for ln in thr_lines)
        env.assertTrue(miss_ok, message="no 'miss X (avg: Y)' found on any throughput line; got:\n%s" % "\n".join(thr_lines[:5]))

        # Every configured percentile must appear on at least one latency line
        # together with its '(avg: X.XXX)' overall counterpart.
        for label in ("p50", "p99", "p99.9"):
            pat = re.escape(label) + r"\s+\d+\.\d+\s+\(avg:\s*\d+\.\d+\)"
            hit = any(re.search(pat, ln) for ln in lat_lines)
            env.assertTrue(
                hit,
                message="percentile %s missing immediate+avg on latency lines; got:\n%s"
                % (label, "\n".join(lat_lines[:5])),
            )

        # The legacy absolute ops counter ("NNNN ops,") must NOT appear under --realtime-latencies.
        env.assertFalse(
            any(re.search(r"\d+ ops,", ln) for ln in thr_lines + lat_lines),
            message="--realtime-latencies must not show absolute ops count; got:\n%s" % stderr[:2000],
        )

        # JSON must still parse and contain stats.
        results = _read_json(config)
        env.assertTrue("ALL STATS" in results, message="JSON output is missing 'ALL STATS' section")
    finally:
        if not memtier_ok:
            debugPrintMemtierOnError(config, env)


def test_realtime_latencies_wraps_and_handles_deep_tail(env):
    """When --print-percentiles lists more than 3 entries, the latency block
    wraps across multiple `[RUN ...] latency ...` lines (3 per line). Also
    asserts that deep-tail percentiles like 99.99999 don't round to "p100":
    the legacy `p%.*f` formatter (capped at 3 fractional digits) used to
    print both 99.99999 and 99.999999 as `p100`, colliding in the table.
    """
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=10000",
            "--realtime-latencies",
            "--print-percentiles=50,99,99.9,99.99,99.99999,99.999999",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=2, clients=4, requests=None, test_time=3)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)
    memtier_ok = benchmark.run()
    try:
        env.assertTrue(memtier_ok)
        stderr = _read_stderr(config)
        stdout_path = os.path.join(config.results_dir, "mb.stdout")
        stdout = open(stdout_path).read() if os.path.exists(stdout_path) else ""

        # 6 percentiles → ceil(6/3) = 2 latency lines per tick. Group lines by
        # their [RUN ... Ts] tag, since the tag changes each tick.
        lat_lines = [ln for ln in stderr.splitlines() if "] latency " in ln]
        env.assertTrue(len(lat_lines) >= 2, message="expected wrapped latency lines (>=2); got:\n%s" % stderr[:2000])

        # Group consecutive latency lines that share a tick tag; each tick
        # should produce exactly 2 latency lines for 6 percentiles.
        tag_re = re.compile(r"(\[RUN #\d+ +\d+% +\d+s\]) latency")
        per_tick = {}
        for ln in lat_lines:
            m = tag_re.match(ln)
            if m:
                per_tick.setdefault(m.group(1), []).append(ln)
        full_ticks = [v for v in per_tick.values() if len(v) == 2]
        env.assertTrue(
            len(full_ticks) >= 1,
            message="expected at least one tick with exactly 2 latency lines; got:\n%s" % stderr[:2000],
        )

        # Deep-tail percentile labels must use enough precision that they
        # neither round to p100 nor collide with each other.
        env.assertTrue(
            any("p99.99999 " in ln for ln in lat_lines),
            message="'p99.99999' label missing — formatter likely rounded to p100; got:\n%s" % stderr[:2000],
        )
        env.assertTrue(
            any("p99.999999 " in ln for ln in lat_lines),
            message="'p99.999999' label missing — formatter likely rounded to p100; got:\n%s" % stderr[:2000],
        )
        env.assertFalse(
            any(re.search(r"\bp100\b", ln) for ln in lat_lines),
            message="found bogus 'p100' label — deep-tail percentile rounded incorrectly; got:\n%s" % stderr[:2000],
        )

        # Final stats table must also avoid the rounding collision: the header
        # row should contain distinct p99.99999 and p99.999999 columns and no
        # 'p100' column.
        env.assertTrue("p99.99999 Latency" in stdout)
        env.assertTrue("p99.999999 Latency" in stdout)
        env.assertFalse(
            re.search(r"\bp100(\.\d+)?\s+Latency", stdout) is not None,
            message="'p100 Latency' column in stdout indicates header-formatter rounding bug",
        )
    finally:
        if not memtier_ok:
            debugPrintMemtierOnError(config, env)


def test_default_progress_line_unchanged(env):
    """Regression: without --realtime-latencies, the periodic stderr line
    must NOT contain any 'pNN' suffix or ANSI erase-to-EOL escape. Existing
    log scrapers depend on the exact legacy shape."""
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=10000",
            "--print-percentiles=50,99",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=2, requests=None, test_time=2)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)
    memtier_ok = benchmark.run()
    try:
        env.assertTrue(memtier_ok)
        stderr = _read_stderr(config)
        env.assertFalse(
            re.search(r"\bp\d+(\.\d+)?:\d+\.\d+", stderr) is not None,
            message="legacy mode must not emit per-second percentile suffix",
        )
        env.assertFalse(
            "\033[K" in stderr,
            message="legacy mode must not emit ANSI erase-EOL on the progress line",
        )
    finally:
        if not memtier_ok:
            debugPrintMemtierOnError(config, env)
