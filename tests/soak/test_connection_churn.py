"""
S4 -- Connection churn vs server restart.

Runs memtier with ``--reconnect-interval=10`` for ~5 minutes against a
standalone Redis. A sidecar thread issues ``CLIENT KILL TYPE normal``
every 30 seconds, mimicking a server restart from memtier's POV (forced
EOF on every client socket).

Pass conditions:
  * memtier exits 0
  * the run survives all the killing
  * we observed at least one kill round

NOTE: We use ``CLIENT KILL`` rather than ``SHUTDOWN`` because RLTest does
not auto-restart standalone servers. ``CLIENT KILL TYPE normal`` exercises
the exact same reconnect codepath (forced socket close) without needing
external process supervision.
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


def test_connection_churn_survives_kills(env):
    env.skipOnCluster()

    test_time = int(os.environ.get("MEMTIER_SOAK_TEST_TIME", "300"))
    churn_interval = int(os.environ.get("MEMTIER_SOAK_CHURN_INTERVAL", "30"))

    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--reconnect-interval=10",
            "--test-time={}".format(test_time),
            "--hide-histogram",
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

    master_nodes_connections = env.getOSSMasterNodesConnectionList()

    stop_churner = threading.Event()
    kill_rounds = [0]

    def churner():
        while not stop_churner.is_set():
            stop_churner.wait(churn_interval)
            if stop_churner.is_set():
                break
            try:
                for conn in master_nodes_connections:
                    # CLIENT KILL TYPE normal cuts every non-control client
                    # at once -- mimicks "server restart" from memtier's POV.
                    conn.execute_command("CLIENT", "KILL", "TYPE", "normal")
                kill_rounds[0] += 1
            except Exception as e:
                env.debugPrint("Churner round error: {}".format(e), True)

    churner_thread = threading.Thread(target=churner, daemon=True)
    churner_thread.start()

    proc = subprocess.Popen(
        stdin=None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        executable=benchmark.binary,
        args=benchmark.args,
    )

    try:
        _stdout, _stderr = proc.communicate()
    finally:
        stop_churner.set()
        churner_thread.join(timeout=5)

    if _stderr:
        benchmark.write_file("mb.stderr", _stderr)
    memtier_ok = proc.wait() == 0

    if not memtier_ok:
        debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok)
    env.assertTrue(kill_rounds[0] > 0)

    env.debugPrint(
        "Survived {} kill rounds across {}s".format(
            kill_rounds[0], test_time
        ),
        True,
    )
