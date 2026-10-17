"""
S3 -- High concurrency + FD hygiene.

Spins up memtier with -t 32 -c 256 --pipeline=32 for ~5 minutes while a
sidecar thread polls ``/proc/<pid>/fd`` every 10 seconds. Targets FD leaks
and per-connection map races.

Pass conditions:
  * memtier exits 0
  * max observed fd_count < 4 * threads * clients
  * steady-state slope of fd_count over the second half of the run is ~0
    (we use a loose ceiling: |slope| < 5 fds/min)
"""

import os
import sys
import subprocess
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


def _count_fds(pid):
    try:
        return len(os.listdir("/proc/{}/fd".format(pid)))
    except (OSError, IOError):
        return None


def _slope_per_minute(samples):
    n = len(samples)
    if n < 2:
        return 0.0
    mean_t = sum(t for t, _ in samples) / n
    mean_v = sum(v for _, v in samples) / n
    num = sum((t - mean_t) * (v - mean_v) for t, v in samples)
    den = sum((t - mean_t) ** 2 for t, _ in samples)
    if den == 0:
        return 0.0
    return (num / den) * 60.0


def test_high_concurrency_fd_hygiene(env):
    env.skipOnCluster()

    test_time = int(os.environ.get("MEMTIER_SOAK_TEST_TIME", "300"))
    sample_interval = int(os.environ.get("MEMTIER_SOAK_SAMPLE_INTERVAL", "10"))

    threads = int(os.environ.get("MEMTIER_SOAK_THREADS", "32"))
    clients = int(os.environ.get("MEMTIER_SOAK_CLIENTS", "256"))
    fd_ceiling = 4 * threads * clients

    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=32",
            "--distinct-client-seed",
            "--test-time={}".format(test_time),
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(
        threads=threads, clients=clients, requests=None, test_time=test_time
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
                count = _count_fds(pid)
                if count is not None:
                    samples.append((time.time() - start_ts, count))
            stop_sampler.wait(sample_interval)

    sampler_thread = threading.Thread(target=sampler, daemon=True)
    sampler_thread.start()

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

    if samples:
        max_fds = max(v for _, v in samples)
        env.debugPrint(
            "FD samples: {}, peak: {}, ceiling: {}".format(
                len(samples), max_fds, fd_ceiling
            ),
            True,
        )
        env.assertTrue(max_fds < fd_ceiling)

        # Slope check on the back half of the run -- once we hit steady
        # state, fd count should not be climbing.
        if len(samples) >= 4:
            tail = samples[len(samples) // 2 :]
            slope = _slope_per_minute(tail)
            env.debugPrint(
                "FD slope (steady-state half): {:.2f} fds/min".format(slope),
                True,
            )
            env.assertTrue(abs(slope) < 5.0)
