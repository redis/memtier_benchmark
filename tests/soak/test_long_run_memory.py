"""
S1 -- Long soak + memory growth.

Runs memtier_benchmark for ~30 minutes against a standalone Redis while a
sidecar Python thread samples ``VmRSS`` from ``/proc/<pid>/status`` every
60 seconds. Targets slow leaks in the run-count cleanup path and in the
histogram accumulator.

Pass conditions:
  * memtier exits 0
  * RSS slope (linear regression over the samples) stays below 2 MB/min
  * final / initial RSS ratio stays below 1.5x

Tunable via ``MEMTIER_SOAK_TEST_TIME`` (seconds) so the harness can be
smoke-tested locally without waiting 30 minutes.
"""

import logging
import os
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from include import (  # noqa: E402
    addTLSArgs,
    add_required_env_arguments,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig  # noqa: E402


def _read_rss_kb(pid):
    try:
        with open("/proc/{}/status".format(pid), "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except (OSError, IOError):
        return None
    return None


def _linear_slope_per_minute(samples):
    """Least-squares slope of RSS (KB) vs time (seconds) -> MB/min."""
    n = len(samples)
    if n < 2:
        return 0.0
    mean_t = sum(t for t, _ in samples) / n
    mean_v = sum(v for _, v in samples) / n
    num = sum((t - mean_t) * (v - mean_v) for t, v in samples)
    den = sum((t - mean_t) ** 2 for t, _ in samples)
    if den == 0:
        return 0.0
    slope_kb_per_s = num / den
    return slope_kb_per_s * 60.0 / 1024.0


def test_long_run_memory_growth(env):
    env.skipOnCluster()

    # Default 30 min; can be shortened for local smoke runs.
    test_time = int(os.environ.get("MEMTIER_SOAK_TEST_TIME", "1800"))
    sample_interval = int(os.environ.get("MEMTIER_SOAK_SAMPLE_INTERVAL", "60"))

    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=4",
            "-d", "1024",
            "--test-time={}".format(test_time),
            "--run-count=1",
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(
        threads=4, clients=50, requests=None, test_time=test_time
    )
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    samples = []
    stop_sampler = threading.Event()
    start_ts = time.time()
    pid_holder = [None]

    def sampler():
        while not stop_sampler.is_set():
            pid = pid_holder[0]
            if pid is not None:
                rss = _read_rss_kb(pid)
                if rss is not None:
                    samples.append((time.time() - start_ts, rss))
            stop_sampler.wait(sample_interval)

    sampler_thread = threading.Thread(target=sampler, daemon=True)
    sampler_thread.start()

    logging.debug("  Command: %s", " ".join(benchmark.args))
    proc = subprocess.Popen(
        stdin=None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        executable=benchmark.binary,
        args=benchmark.args,
    )
    pid_holder[0] = proc.pid

    try:
        _stdout, _stderr = proc.communicate()
    finally:
        stop_sampler.set()
        sampler_thread.join(timeout=5)

    if _stderr:
        benchmark.write_file("mb.stderr", _stderr)
    memtier_ok = proc.wait() == 0

    if not memtier_ok:
        debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok)

    env.debugPrint(
        "Collected {} RSS samples over {:.1f}s".format(
            len(samples), time.time() - start_ts
        ),
        True,
    )

    # Need at least two samples to compute slope; long soaks should easily
    # collect 20+ samples.
    if len(samples) >= 2:
        slope_mb_per_min = _linear_slope_per_minute(samples)
        initial = samples[0][1] / 1024.0  # MB
        final = samples[-1][1] / 1024.0
        elapsed = samples[-1][0] - samples[0][0]
        env.debugPrint(
            "RSS slope: {:.3f} MB/min, initial: {:.1f} MB, final: {:.1f} MB, span: {:.1f}s".format(
                slope_mb_per_min, initial, final, elapsed
            ),
            True,
        )
        # Slope only meaningful once we're past the warmup phase. Apply
        # the < 2 MB/min check only when we have at least 5 minutes of
        # samples; for shorter (smoke) runs only enforce the ratio cap.
        if elapsed >= 300:
            env.assertTrue(slope_mb_per_min < 2.0)
        if initial > 0:
            env.assertTrue(final / initial < 1.5)
