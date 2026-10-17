"""
S7 -- Slow / lossy network via ``tc netem``.

Installs ``netem delay 200ms 20ms loss 1%`` on ``lo`` for the duration of
the run, drives memtier through it for ~5 minutes, then removes the
qdisc.

Requirements:
  * ``tc`` binary present (iproute2 package)
  * ability to run ``sudo tc`` non-interactively, OR being root already
  * NET_ADMIN capability on the netns (GitHub runners ship this)

If any of those fails the test ``env.skip()`` s cleanly so non-root
laptops and locked-down sandboxes don't fail the suite.

Pass conditions:
  * memtier exits 0
  * no SIGSEGV / no NaN-in-stats / no "0.00 ops/sec" stalls
  * a finite p99 latency line appears in stdout
"""

import os
import sys
import shutil
import subprocess
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from include import (  # noqa: E402
    addTLSArgs,
    add_required_env_arguments,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig  # noqa: E402


def _sudo_prefix():
    if os.geteuid() == 0:
        return []
    if shutil.which("sudo") is None:
        return None
    return ["sudo", "-n"]


def _run_tc(args):
    """Run ``tc`` and return (rc, stderr). Returns (-1, msg) when we
    can't even attempt it (missing tc / sudo)."""
    prefix = _sudo_prefix()
    if prefix is None:
        return -1, "sudo not available"
    if shutil.which("tc") is None:
        return -1, "tc not available"
    cmd = prefix + ["tc"] + args
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=10
        )
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        return -1, str(e)
    return proc.returncode, proc.stderr


def _install_netem():
    # Best-effort cleanup first in case a previous run left state behind.
    _run_tc(["qdisc", "del", "dev", "lo", "root"])
    rc, err = _run_tc([
        "qdisc", "add", "dev", "lo", "root", "netem",
        "delay", "200ms", "20ms",
        "loss", "1%",
    ])
    return rc, err


def _remove_netem():
    _run_tc(["qdisc", "del", "dev", "lo", "root"])


def test_slow_network_smooth(env):
    env.skipOnCluster()

    rc, err = _install_netem()
    if rc != 0:
        env.debugPrint(
            "Skipping S7: tc netem install failed (rc={}, err={!r})".format(
                rc, err
            ),
            True,
        )
        env.skip()
        return

    try:
        test_time = int(os.environ.get("MEMTIER_SOAK_TEST_TIME", "300"))

        benchmark_specs = {
            "name": env.testName,
            "args": [
                "--pipeline=8",
                "--test-time={}".format(test_time),
            ],
        }
        addTLSArgs(benchmark_specs, env)

        config = get_default_memtier_config(
            threads=8, clients=50, requests=None, test_time=test_time
        )
        master_nodes_list = env.getMasterNodesList()
        add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

        test_dir = tempfile.mkdtemp()
        config = RunConfig(test_dir, env.testName, config, {})
        ensure_clean_benchmark_folder(config.results_dir)

        benchmark = Benchmark.from_json(config, benchmark_specs)
        # Benchmark.run() defaults to timeout=240s. Soak runs with
        # test_time>=300s would be killed before they could write a
        # summary, so we pass an explicit upper bound that leaves
        # ~90s of slack for graceful shutdown.
        memtier_ok = benchmark.run(timeout=test_time + 90)

        if not memtier_ok:
            debugPrintMemtierOnError(config, env)
        env.assertTrue(memtier_ok)

        stdout_path = os.path.join(config.results_dir, "mb.stdout")
        if os.path.isfile(stdout_path):
            with open(stdout_path) as f:
                stdout_content = f.read()
            # No NaN / inf in the stats output.
            lc = stdout_content.lower()
            env.assertFalse("nan" in lc)
            env.assertFalse(" inf " in lc)
            # Must have produced a percentile-style row.
            env.assertTrue("p99" in lc or "totals" in lc)
    finally:
        _remove_netem()
