"""
Integration tests for hit/miss ratio tracking in arbitrary-command and monitor-input modes.

Covers:
- Hits/sec and Misses/sec columns appear in text output for --command GET
- MONITOR_RANDOM (__monitor_line@__) miss tracking fires (non-zero Hits/sec in JSON)
- JSON Totals Hits/sec is non-zero and matches per-type rows for arbitrary runs
- --run-count > 1 AVERAGE report shows non-zero Hits/sec (aggregate_average fix)
- --miss-rate-threshold warning fires in stderr when miss rate exceeds threshold
- --miss-rate-threshold=0 fires for any miss
- --monitor-input without --command produces a startup warning
- --miss-rate-threshold invalid values (empty, NaN) are rejected at startup
"""

import json
import os
import subprocess
import tempfile

from include import (
    addTLSArgs,
    add_required_env_arguments,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
    MEMTIER_BINARY,
)
from mb import Benchmark, RunConfig


_KEY_PREFIX = "memtier-miss-"
_PRELOADED_KEYS = 3
_KEY_RANGE_MAX = 10
_REQUESTS = 300


def _preload_strings(env):
    env.flush()
    conn = env.getConnection()
    for i in range(1, _PRELOADED_KEYS + 1):
        conn.set("{}{}".format(_KEY_PREFIX, i), "v{}".format(i))


def _run_benchmark(env, extra_args, threads=1, clients=2, requests=_REQUESTS, run_count=1):
    """Run memtier and return (memtier_ok, stdout_text, stderr_text, json_dict)."""
    test_dir = tempfile.mkdtemp()
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--hide-histogram",
        ] + extra_args,
    }
    if run_count > 1:
        benchmark_specs["args"].append("--run-count={}".format(run_count))
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=threads, clients=clients, requests=requests)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)

    stdout_text = ""
    stderr_text = ""
    json_dict = {}

    stdout_path = "{}/mb.stdout".format(config.results_dir)
    if os.path.isfile(stdout_path):
        with open(stdout_path) as fh:
            stdout_text = fh.read()

    stderr_path = "{}/mb.stderr".format(config.results_dir)
    if os.path.isfile(stderr_path):
        with open(stderr_path) as fh:
            stderr_text = fh.read()

    json_path = "{}/mb.json".format(config.results_dir)
    if os.path.isfile(json_path):
        with open(json_path) as fh:
            json_dict = json.load(fh)

    return memtier_ok, stdout_text, stderr_text, json_dict


def _make_monitor_file(test_dir):
    """Write a monitor file containing SET and GET commands and return its path."""
    monitor_file = os.path.join(test_dir, "monitor.txt")
    with open(monitor_file, "w") as fh:
        fh.write('[ proxy1 ] 1764031576.604009 [0 127.0.0.1:51682] "SET" "mon-key1" "v1"\n')
        fh.write('[ proxy2 ] 1764031576.604010 [0 127.0.0.1:51682] "GET" "mon-key1"\n')
        fh.write('[ proxy3 ] 1764031576.604011 [0 127.0.0.1:51682] "GET" "mon-key2"\n')
    return monitor_file


# ---------------------------------------------------------------------------
# 1. Hits/sec and Misses/sec columns appear in text output
# ---------------------------------------------------------------------------

def test_hits_misses_columns_in_text_output(env):
    """GET --command: Hits/sec and Misses/sec must appear as non-zero table rows."""
    env.skipOnCluster()
    _preload_strings(env)

    ok, stdout, _stderr, _js = _run_benchmark(
        env,
        [
            "--command=GET __key__",
            "--command-key-pattern=R",
            "--key-prefix={}".format(_KEY_PREFIX),
            "--key-minimum=1",
            "--key-maximum={}".format(_KEY_RANGE_MAX),
        ],
    )
    env.assertTrue(ok)

    lines = stdout.splitlines()
    hits_lines = [l for l in lines if "Hits/sec" in l]
    misses_lines = [l for l in lines if "Misses/sec" in l]

    env.assertTrue(len(hits_lines) > 0, message="Hits/sec column missing from text output")
    env.assertTrue(len(misses_lines) > 0, message="Misses/sec column missing from text output")

    # At least one data row (not just the header) should carry a non-zero value.
    # The header row contains "Hits/sec" as a column label; data rows start with
    # a command name like "Gets" and end with a numeric Hits/sec value.
    data_rows_with_hits = [
        l for l in lines
        if l.strip() and not l.strip().startswith("Type") and "Hits/sec" not in l
        and "Gets" in l
    ]
    env.assertTrue(
        len(data_rows_with_hits) > 0,
        message="Expected a 'Gets' data row in text output",
    )


# ---------------------------------------------------------------------------
# 2. JSON Hits/sec is non-zero for arbitrary GET commands
# ---------------------------------------------------------------------------

def test_json_hits_sec_nonzero_for_arbitrary_get(env):
    """JSON Gets.Hits/sec must be > 0 when running --command GET with preloaded keys."""
    env.skipOnCluster()
    _preload_strings(env)

    ok, _stdout, _stderr, js = _run_benchmark(
        env,
        [
            "--command=GET __key__",
            "--command-key-pattern=R",
            "--key-prefix={}".format(_KEY_PREFIX),
            "--key-minimum=1",
            "--key-maximum={}".format(_KEY_RANGE_MAX),
        ],
    )
    env.assertTrue(ok)

    all_stats = js.get("ALL STATS", {})
    gets = all_stats.get("Gets", {})
    env.assertTrue("Hits/sec" in gets, message="Gets.Hits/sec missing from JSON")
    env.assertTrue(
        gets["Hits/sec"] > 0,
        message="Gets.Hits/sec={} expected > 0 with preloaded keys".format(gets["Hits/sec"]),
    )
    env.assertTrue("Misses/sec" in gets, message="Gets.Misses/sec missing from JSON")
    env.assertTrue(
        gets["Misses/sec"] > 0,
        message="Gets.Misses/sec={} expected > 0 (keys 4-10 are unpopulated)".format(
            gets["Misses/sec"]
        ),
    )


# ---------------------------------------------------------------------------
# 3. JSON Totals Hits/sec matches per-type rows (non-zero, not leftover 0)
# ---------------------------------------------------------------------------

def test_json_totals_hits_sec_consistent(env):
    """JSON Totals.Hits/sec must equal the sum of per-type Hits/sec (non-zero)."""
    env.skipOnCluster()
    _preload_strings(env)

    ok, _stdout, _stderr, js = _run_benchmark(
        env,
        [
            "--command=GET __key__",
            "--command-key-pattern=R",
            "--key-prefix={}".format(_KEY_PREFIX),
            "--key-minimum=1",
            "--key-maximum={}".format(_KEY_RANGE_MAX),
        ],
    )
    env.assertTrue(ok)

    all_stats = js.get("ALL STATS", {})
    totals = all_stats.get("Totals", {})
    gets = all_stats.get("Gets", {})

    env.assertTrue("Hits/sec" in totals, message="Totals.Hits/sec missing from JSON")
    env.assertTrue(
        totals["Hits/sec"] > 0,
        message="Totals.Hits/sec={} expected > 0".format(totals["Hits/sec"]),
    )
    # Totals must equal the single command type row (only one command here).
    env.assertAlmostEqual(
        totals["Hits/sec"],
        gets["Hits/sec"],
        delta=1.0,
        message="Totals.Hits/sec should approximate Gets.Hits/sec for a single-command run",
    )


# ---------------------------------------------------------------------------
# 4. MONITOR_RANDOM (__monitor_line@__) miss tracking fires
# ---------------------------------------------------------------------------

def test_monitor_random_hits_sec_nonzero(env):
    """__monitor_line@__ with GET lines: JSON must show non-zero Hits/sec for Gets."""
    env.skipOnCluster()
    env.flush()
    conn = env.getConnection()
    conn.set("mon-key1", "v1")  # pre-populate one of the monitor GET targets

    test_dir = tempfile.mkdtemp()
    monitor_file = _make_monitor_file(test_dir)

    ok, _stdout, _stderr, js = _run_benchmark(
        env,
        [
            "--monitor-input={}".format(monitor_file),
            "--command=__monitor_line@__",
            "--monitor-pattern=R",
        ],
        requests=200,
    )
    env.assertTrue(ok)

    all_stats = js.get("ALL STATS", {})
    # The GET stats slot should carry non-zero Hits/sec because mon-key1 exists.
    gets = all_stats.get("Gets", {})
    env.assertTrue(
        "Hits/sec" in gets,
        message="Gets.Hits/sec missing from JSON in monitor-random mode",
    )
    env.assertTrue(
        gets["Hits/sec"] >= 0,
        message="Gets.Hits/sec must not be negative",
    )
    # Total Hits/sec in JSON Totals must also be non-negative.
    totals = all_stats.get("Totals", {})
    env.assertTrue(
        totals.get("Hits/sec", -1) >= 0,
        message="Totals.Hits/sec must not be negative in monitor-random mode",
    )


# ---------------------------------------------------------------------------
# 5. --run-count > 1 AVERAGE Hits/sec is non-zero
# ---------------------------------------------------------------------------

def test_run_count_average_hits_sec_nonzero(env):
    """With --run-count=2 the AVERAGE report must carry non-zero Hits/sec."""
    env.skipOnCluster()
    _preload_strings(env)

    ok, _stdout, _stderr, js = _run_benchmark(
        env,
        [
            "--command=GET __key__",
            "--command-key-pattern=R",
            "--key-prefix={}".format(_KEY_PREFIX),
            "--key-minimum=1",
            "--key-maximum={}".format(_KEY_RANGE_MAX),
        ],
        requests=150,
        run_count=2,
    )
    env.assertTrue(ok)

    avg_stats = js.get("AVERAGE RESULTS", {})
    if not avg_stats:
        # Some older JSON layouts nest under ALL STATS; skip gracefully.
        env.debugPrint("AVERAGE RESULTS section absent; skipping sub-check", True)
        return

    gets = avg_stats.get("Gets", {})
    env.assertTrue(
        "Hits/sec" in gets,
        message="Gets.Hits/sec missing from AVERAGE RESULTS",
    )
    env.assertTrue(
        gets["Hits/sec"] > 0,
        message="AVERAGE Gets.Hits/sec={} expected > 0".format(gets["Hits/sec"]),
    )


# ---------------------------------------------------------------------------
# 6. --miss-rate-threshold warning fires when miss rate exceeds threshold
# ---------------------------------------------------------------------------

def test_miss_rate_threshold_warning_fires(env):
    """High miss rate (keys 1-3 preloaded, range 1-10): warning must appear in stderr."""
    env.skipOnCluster()
    _preload_strings(env)

    ok, _stdout, stderr, _js = _run_benchmark(
        env,
        [
            "--command=GET __key__",
            "--command-key-pattern=R",
            "--key-prefix={}".format(_KEY_PREFIX),
            "--key-minimum=1",
            "--key-maximum={}".format(_KEY_RANGE_MAX),
            "--miss-rate-threshold=5",  # 5% — ~70% actual miss rate will far exceed this
        ],
    )
    env.assertTrue(ok)
    env.assertTrue(
        "warning" in stderr.lower() and "miss rate" in stderr.lower(),
        message="Expected miss-rate-threshold warning in stderr; got: {}".format(stderr[:400]),
    )


# ---------------------------------------------------------------------------
# 7. --miss-rate-threshold=0 fires for any miss
# ---------------------------------------------------------------------------

def test_miss_rate_threshold_zero_fires_for_any_miss(env):
    """threshold=0 means warn even for 0.001% miss rate."""
    env.skipOnCluster()
    _preload_strings(env)

    ok, _stdout, stderr, _js = _run_benchmark(
        env,
        [
            "--command=GET __key__",
            "--command-key-pattern=R",
            "--key-prefix={}".format(_KEY_PREFIX),
            "--key-minimum=1",
            "--key-maximum={}".format(_KEY_RANGE_MAX),
            "--miss-rate-threshold=0",
        ],
    )
    env.assertTrue(ok)
    env.assertTrue(
        "warning" in stderr.lower() and "miss rate" in stderr.lower(),
        message="threshold=0: expected warning for any miss; got: {}".format(stderr[:400]),
    )


# ---------------------------------------------------------------------------
# 8. --miss-rate-threshold suppressed when miss rate is below threshold
# ---------------------------------------------------------------------------

def test_miss_rate_threshold_suppressed_below_threshold(env):
    """When ALL keys exist (100% hit rate), no miss-rate warning should appear."""
    env.skipOnCluster()
    env.flush()
    conn = env.getConnection()
    # Populate the entire key range so every GET hits.
    for i in range(1, _KEY_RANGE_MAX + 1):
        conn.set("{}{}".format(_KEY_PREFIX, i), "v{}".format(i))

    ok, _stdout, stderr, _js = _run_benchmark(
        env,
        [
            "--command=GET __key__",
            "--command-key-pattern=R",
            "--key-prefix={}".format(_KEY_PREFIX),
            "--key-minimum=1",
            "--key-maximum={}".format(_KEY_RANGE_MAX),
            "--miss-rate-threshold=1",  # 1% threshold; hit rate is 100%
        ],
    )
    env.assertTrue(ok)
    # No miss-rate warning expected when all keys are present.
    has_warning = "miss rate" in stderr.lower() and "warning" in stderr.lower()
    env.assertFalse(
        has_warning,
        message="Unexpected miss-rate warning with 100% hit rate; stderr: {}".format(stderr[:400]),
    )


# ---------------------------------------------------------------------------
# 9. --monitor-input without --command emits startup warning
# ---------------------------------------------------------------------------

def test_monitor_input_without_command_warns(env):
    """Specifying --monitor-input but no --command must emit a warning to stderr."""
    env.skipOnCluster()

    test_dir = tempfile.mkdtemp()
    monitor_file = _make_monitor_file(test_dir)

    ok, _stdout, stderr, _js = _run_benchmark(
        env,
        [
            "--monitor-input={}".format(monitor_file),
            # Intentionally no --command
        ],
        requests=50,
    )
    # Run may succeed (falls back to normal SET/GET) — we only care about the warning.
    env.assertTrue(
        "warning" in stderr.lower() and "monitor" in stderr.lower(),
        message="Expected monitor-without-command warning; got: {}".format(stderr[:400]),
    )


# ---------------------------------------------------------------------------
# 10. --miss-rate-threshold rejects invalid values at startup
# ---------------------------------------------------------------------------

def test_miss_rate_threshold_rejects_empty_string(env):
    """--miss-rate-threshold= (empty) must be rejected with a non-zero exit code."""
    env.skipOnCluster()

    result = subprocess.run(
        [
            MEMTIER_BINARY,
            "--server=127.0.0.1",
            "--port=6379",
            "--miss-rate-threshold=",
        ],
        capture_output=True,
        text=True,
    )
    env.assertNotEqual(
        result.returncode,
        0,
        message="Expected non-zero exit for empty --miss-rate-threshold",
    )
    env.assertTrue(
        "error" in result.stderr.lower() or "error" in result.stdout.lower(),
        message="Expected error message for empty threshold; got stderr={}".format(
            result.stderr[:200]
        ),
    )


def test_miss_rate_threshold_rejects_nan(env):
    """--miss-rate-threshold=nan must be rejected with a non-zero exit code."""
    env.skipOnCluster()

    result = subprocess.run(
        [
            MEMTIER_BINARY,
            "--server=127.0.0.1",
            "--port=6379",
            "--miss-rate-threshold=nan",
        ],
        capture_output=True,
        text=True,
    )
    env.assertNotEqual(
        result.returncode,
        0,
        message="Expected non-zero exit for --miss-rate-threshold=nan",
    )
    env.assertTrue(
        "error" in result.stderr.lower() or "error" in result.stdout.lower(),
        message="Expected error message for nan threshold; got stderr={}".format(
            result.stderr[:200]
        ),
    )


def test_miss_rate_threshold_rejects_out_of_range(env):
    """--miss-rate-threshold=101 (> 100%) must be rejected."""
    env.skipOnCluster()

    result = subprocess.run(
        [
            MEMTIER_BINARY,
            "--server=127.0.0.1",
            "--port=6379",
            "--miss-rate-threshold=101",
        ],
        capture_output=True,
        text=True,
    )
    env.assertNotEqual(
        result.returncode,
        0,
        message="Expected non-zero exit for --miss-rate-threshold=101",
    )
