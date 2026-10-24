"""
Tests for --realtime-latencies.

When the flag is set, memtier_benchmark replaces the per-second
'[RUN #N ...] ops/sec' progress line with a per-tick block:

  [RUN #N  P%  Ts] throughput <cur> (avg: <avg>) ops/sec   <cur>/sec (avg: <avg>/sec)   miss X.XX% (avg: Y.YY%)
  [RUN #N  P%  Ts] latency [inst    ] n=<count>  p50  X.XXX   p99  X.XXX   p99.9  X.XXX*
  [RUN #N  P%  Ts] latency [winNs   ] n=<count>  p50  X.XXX   p99  X.XXX   p99.9  X.XXX
  [RUN #N  P%  Ts] latency [overall ] n=<count>  p50  X.XXX   p99  X.XXX   p99.9  X.XXX  ms

The three latency rows are:
  - inst   : each client's live second
  - winNs  : the completed seconds of the last N ticks (--latency-window,
             default 60s; 0 disables)
  - overall: the completed seconds of the whole run

n is the row's sample count. A percentile p computed from fewer than
ceil(1 / (1 - p/100)) samples (1M for p99.9999) is just the row's maximum
and is marked with '*'.

The block redraws in place on a TTY (cursor-up + erase-EOL); on non-TTY
stderr (file capture / pipe) each tick is just appended, producing a clean
per-second time series. The RLTest harness runs memtier as a subprocess
with stderr captured to a pipe, so these tests exercise the non-TTY path.
"""

import json
import math
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


def test_realtime_latencies_emits_three_row_block(env):
    """Per tick we get one 'throughput' line plus three labeled latency rows
    (inst, winNs, overall). The throughput line still carries cur+avg ops and
    miss%; each latency row exposes every configured percentile."""
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=10000",
            "--realtime-latencies",
            "--latency-window=60",
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
        env.assertTrue(
            len(thr_lines) >= 1, message="expected at least one 'throughput' line; stderr=\n%s" % stderr[:2000]
        )

        # Throughput line must carry both immediate ops/sec and a parenthesized
        # avg, plus a miss% with its own avg (e.g. "miss 99.50% (avg: 99.40%)").
        ops_avg_ok = any(re.search(r"\(avg:\s*[\d,]+\)\s*ops/sec", ln) for ln in thr_lines)
        env.assertTrue(ops_avg_ok, message="ops/sec line missing '(avg: N)'; got:\n%s" % "\n".join(thr_lines[:5]))
        miss_ok = any(re.search(r"miss\s+\d+\.\d+%\s+\(avg:\s*\d+\.\d+%\)", ln) for ln in thr_lines)
        env.assertTrue(
            miss_ok, message="no 'miss X (avg: Y)' found on any throughput line; got:\n%s" % "\n".join(thr_lines[:5])
        )

        # Each tick must emit all three labeled latency rows.
        inst_lines = [ln for ln in stderr.splitlines() if re.search(r"\] latency \[inst", ln)]
        win_lines = [ln for ln in stderr.splitlines() if re.search(r"\] latency \[win60s", ln)]
        overall_lines = [ln for ln in stderr.splitlines() if re.search(r"\] latency \[overall", ln)]
        env.assertTrue(len(inst_lines) >= 1, message="no [inst] latency row found; stderr=\n%s" % stderr[:2000])
        env.assertTrue(len(win_lines) >= 1, message="no [win60s] latency row found; stderr=\n%s" % stderr[:2000])
        env.assertTrue(len(overall_lines) >= 1, message="no [overall] latency row found; stderr=\n%s" % stderr[:2000])

        # Every configured percentile must appear on each row at least once
        # with a numeric value (or '-' on the very first tick before samples arrive).
        for row_name, lines in (("inst", inst_lines), ("win60s", win_lines), ("overall", overall_lines)):
            for label in ("p50", "p99", "p99.9"):
                pat = re.escape(label) + r"\s+(?:\d+\.\d+|-)"
                hit = any(re.search(pat, ln) for ln in lines)
                env.assertTrue(
                    hit,
                    message="percentile %s missing on [%s] row; got:\n%s" % (label, row_name, "\n".join(lines[:5])),
                )

        # The legacy absolute ops counter ("NNNN ops,") must NOT appear under --realtime-latencies.
        env.assertFalse(
            any(re.search(r"\d+ ops,", ln) for ln in thr_lines + inst_lines + win_lines + overall_lines),
            message="--realtime-latencies must not show absolute ops count; got:\n%s" % stderr[:2000],
        )

        # JSON must still parse and contain stats.
        results = _read_json(config)
        env.assertTrue("ALL STATS" in results, message="JSON output is missing 'ALL STATS' section")
    finally:
        if not memtier_ok:
            debugPrintMemtierOnError(config, env)


def test_realtime_latencies_wraps_and_handles_deep_tail(env):
    """When --print-percentiles lists more than 3 entries, each row wraps
    across multiple `[RUN ...] latency [label] ...` lines (3 percentiles per
    line). Also asserts that deep-tail percentiles like 99.99999 don't round
    to "p100" (regression: legacy formatter capped at 3 fractional digits).
    """
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=10000",
            "--realtime-latencies",
            "--latency-window=5",
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

        # 6 percentiles → ceil(6/3) = 2 chunks per row. With three rows (inst,
        # win, overall) that's 6 latency lines per tick. Group by tick tag.
        lat_lines = [ln for ln in stderr.splitlines() if "] latency [" in ln]
        env.assertTrue(len(lat_lines) >= 6, message="expected wrapped latency lines (>=6); got:\n%s" % stderr[:2000])

        tag_re = re.compile(r"(\[RUN #\d+ +\d+% +\d+s\]) latency")
        per_tick = {}
        for ln in lat_lines:
            m = tag_re.match(ln)
            if m:
                per_tick.setdefault(m.group(1), []).append(ln)
        full_ticks = [v for v in per_tick.values() if len(v) == 6]
        env.assertTrue(
            len(full_ticks) >= 1,
            message="expected at least one tick with 6 latency lines (3 rows x 2 chunks); got:\n%s" % stderr[:2000],
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


def test_latency_window_zero_drops_window_row(env):
    """`--latency-window=0` must suppress the [winNs] row entirely, leaving
    just inst + overall. Validates the user-facing 'disable' contract."""
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=10000",
            "--realtime-latencies",
            "--latency-window=0",
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
        inst_lines = [ln for ln in stderr.splitlines() if re.search(r"\] latency \[inst", ln)]
        overall_lines = [ln for ln in stderr.splitlines() if re.search(r"\] latency \[overall", ln)]
        win_lines = [ln for ln in stderr.splitlines() if re.search(r"\] latency \[win", ln)]
        env.assertTrue(len(inst_lines) >= 1, message="[inst] row missing with window disabled")
        env.assertTrue(len(overall_lines) >= 1, message="[overall] row missing with window disabled")
        env.assertEqual(len(win_lines), 0, message="[win*] row must be suppressed when --latency-window=0")
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


def test_latency_window_ring_smooths_spikes(env):
    """A short window (2s) plus a long-enough run must produce a [win2s] row
    whose p99 falls into the same numeric range as the inst row over time
    (smoothed but not zero). Sanity check that the ring is actually populated
    and computed, not silently empty."""
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=10000",
            "--realtime-latencies",
            "--latency-window=2",
            "--print-percentiles=50,99",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=2, clients=4, requests=None, test_time=5)
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
        win_lines = [ln for ln in stderr.splitlines() if re.search(r"\] latency \[win2s", ln)]
        env.assertTrue(len(win_lines) >= 2, message="expected multiple [win2s] rows; stderr=\n%s" % stderr[:2000])

        # Extract numeric p99 values from each [win2s] row and require at
        # least one positive reading once the ring has data.
        p99_re = re.compile(r"\bp99\s+(\d+\.\d+)")
        win_p99 = []
        for ln in win_lines:
            m = p99_re.search(ln)
            if m:
                win_p99.append(float(m.group(1)))
        env.assertTrue(
            any(v > 0.0 for v in win_p99), message="[win2s] p99 never produced a positive value; got %r" % win_p99
        )
    finally:
        if not memtier_ok:
            debugPrintMemtierOnError(config, env)


_ROW_RE = re.compile(r"^(\[RUN #\d+ +\d+% +\d+s\]) latency \[(\w+) *\] n=(\S+)\s+(.*)$")
_PCT_RE = re.compile(r"p([\d.]+)\s+(\d+\.\d+)(\*?)")


def _parse_count(text):
    """Inverse of the compact n= format (987, 12.3K, 1.23M); exact below 10000."""
    scale = {"K": 1e3, "M": 1e6, "G": 1e9}
    if text[-1] in scale:
        return float(text[:-1]) * scale[text[-1]]
    return int(text)


def _min_samples(p):
    return 1 if p >= 100 else math.ceil(100.0 / (100.0 - p) * (1 - 1e-9))


def _frames(stderr):
    """Map each tick tag to {row label: (count text, [(p, value, marked)])}."""
    frames = {}
    order = []
    for ln in stderr.splitlines():
        m = _ROW_RE.match(ln)
        if not m:
            continue
        tag, label, count, rest = m.groups()
        if tag not in frames:
            frames[tag] = {}
            order.append(tag)
        pcts = [(float(p), float(v), mark == "*") for p, v, mark in _PCT_RE.findall(rest)]
        frames[tag][label] = (count, pcts)
    return [frames[t] for t in order]


def _run(env, args, threads, clients, requests=None, test_time=None):
    benchmark_specs = {"name": env.testName, "args": args}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=threads, clients=clients, requests=requests, test_time=test_time)
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
    config = RunConfig(tempfile.mkdtemp(), env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    return config, benchmark.run()


def test_latency_window_counts_every_sample_and_marks_unresolved(env):
    """The win and overall rows are built from drained completed seconds, so
    the last frame counts exactly the run's samples. With n=1000, p99.9 is
    resolved (needs 1000) and p99.99 is not (needs 10000): only the latter is
    marked, on every row, and the bounded --requests run warns up front."""
    config, memtier_ok = _run(
        env,
        [
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=10000",
            "--realtime-latencies",
            "--latency-window=60",
            "--print-percentiles=50,99.9,99.99",
        ],
        threads=1,
        clients=4,
        requests=250,
    )
    try:
        env.assertTrue(memtier_ok)
        stderr = _read_stderr(config)
        total = _read_json(config)["ALL STATS"]["Totals"]["Count"]
        env.assertEqual(total, 1000)

        env.assertTrue(
            re.search(
                r"warning: p99\.99 needs at least 10000 latency samples, but this run records at most 1000;", stderr
            )
            is not None,
            message=stderr[:2000],
        )
        env.assertFalse("warning: p99.9 needs" in stderr, message=stderr[:2000])
        env.assertFalse("warning: p50 needs" in stderr, message=stderr[:2000])

        frames = _frames(stderr)
        env.assertTrue(len(frames) >= 1, message=stderr[:2000])
        last = frames[-1]
        for label in ("win60s", "overall"):
            env.assertTrue(label in last, message="missing [%s] row; stderr=\n%s" % (label, stderr[:2000]))
            if label not in last:
                continue
            count, pcts = last[label]
            env.assertEqual(int(count), total, message="[%s] n=%s, JSON Count %d" % (label, count, total))
            env.assertEqual([p for p, _, _ in pcts], [50.0, 99.9, 99.99])
            env.assertEqual([marked for _, _, marked in pcts], [False, False, True], message=str(pcts))

        # Every rendered value's marker matches its row's count.
        for frame in frames:
            for label, (count, pcts) in frame.items():
                n = _parse_count(count)
                for p, _, marked in pcts:
                    if n >= 10000:
                        continue  # compact count is truncated; exact below 10000
                    env.assertEqual(marked, n < _min_samples(p), message="[%s] n=%s p%s" % (label, count, p))
    finally:
        if not memtier_ok:
            debugPrintMemtierOnError(config, env)


def test_latency_window_warns_when_rate_limit_cannot_fill_it(env):
    """2 connections per endpoint at 100 req/s give at most 200 samples per
    endpoint per 1s window and 1200 per 6s run: p99.9 (needs 1000) is
    resolvable for the run but never for the window, so only the window
    warning is printed. In cluster mode the limit applies per endpoint
    connection, so the bound scales with the primaries."""
    config, memtier_ok = _run(
        env,
        [
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=10000",
            "--rate-limiting=100",
            "--realtime-latencies",
            "--latency-window=1",
            "--print-percentiles=50,99.9",
        ],
        threads=1,
        clients=2,
        test_time=6,
    )
    try:
        env.assertTrue(memtier_ok)
        stderr = _read_stderr(config)
        endpoints = len(env.getMasterNodesList()) if env.isCluster() else 1
        expected = (
            "warning: p99.9 needs at least 1000 latency samples, but the 1s latency window holds at most %d; "
            "the win1s row marks it with '*'." % (200 * endpoints)
        )
        env.assertEqual(stderr.count(expected), 1, message=stderr[:2000])
        env.assertFalse("this run records at most" in stderr, message=stderr[:2000])
        env.assertFalse("warning: p50 needs" in stderr, message=stderr[:2000])
    finally:
        if not memtier_ok:
            debugPrintMemtierOnError(config, env)


def test_latency_window_expires_old_seconds(env):
    """A 1s window holds roughly one tick of samples while overall keeps
    growing, so the ring really drops expired seconds."""
    config, memtier_ok = _run(
        env,
        [
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=10000",
            "--rate-limiting=100",
            "--realtime-latencies",
            "--latency-window=1",
            "--print-percentiles=50",
        ],
        threads=1,
        clients=2,
        test_time=5,
    )
    try:
        env.assertTrue(memtier_ok)
        stderr = _read_stderr(config)
        frames = [f for f in _frames(stderr) if "win1s" in f and "overall" in f]
        env.assertTrue(len(frames) >= 4, message=stderr[:3000])
        last = frames[-1]
        win_n = _parse_count(last["win1s"][0])
        overall_n = _parse_count(last["overall"][0])
        # At least ~200 samples per second (more in cluster mode, where the
        # limit applies per endpoint connection) for ~5 seconds; the window
        # keeps only the last second of them.
        env.assertGreater(overall_n, 600, message=stderr[:3000])
        env.assertLess(win_n, overall_n / 2, message=stderr[:3000])
    finally:
        if not memtier_ok:
            debugPrintMemtierOnError(config, env)
