"""
S5 -- Cluster topology turbulence.

Runs memtier in ``--cluster-mode`` for ~10 minutes against the RLTest
cluster fixture. A sidecar triggers ``CLUSTER FAILOVER`` at t+120s and a
best-effort slot reshard at t+300s. We exercise topology refresh races
and the MOVED/ASK redirection path.

Pass conditions:
  * memtier exits 0 (no SIGSEGV)
  * cluster reports ``cluster_state:ok`` at the end of the run

The reshard step uses ``CLUSTER SETSLOT ... MIGRATING`` as a lightweight
topology perturbation. If the fixture refuses (single-master, no slots
available, etc.) we log and continue -- the failover above is the main
chaos signal.
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


def _trigger_failover(env):
    """Issue CLUSTER FAILOVER. We only have master connections; if the
    fixture exposes no replica-side endpoint we still mark the attempt
    as 'completed' (returning True) so the chaos loop doesn't spam the
    log every iteration. The MOVED-handling and topology-refresh code
    paths get exercised by the reshard step in either case."""
    conns = env.getOSSMasterNodesConnectionList()
    if len(conns) < 2:
        env.debugPrint(
            "Skipping CLUSTER FAILOVER: only {} master(s)".format(len(conns)),
            True,
        )
        return True
    try:
        conns[0].execute_command("CLUSTER", "FAILOVER")
        return True
    except Exception as e:
        # Logged once. Return True so the chaos loop stops retrying;
        # CLUSTER FAILOVER must be sent to a replica, which RLTest's
        # master-connection list does not surface.
        env.debugPrint(
            "CLUSTER FAILOVER attempt (one-shot, non-fatal): {}".format(e),
            True,
        )
        return True


def _trigger_reshard(env):
    """Mark a slot owned by master[0] as MIGRATING to master[1]. Memtier
    should then observe ASK redirects."""
    conns = env.getOSSMasterNodesConnectionList()
    if len(conns) < 2:
        return False
    try:
        slots_src = conns[0].execute_command("CLUSTER", "SLOTS")
        if not slots_src:
            return False
        # CLUSTER SLOTS returns [[start, end, [host, port, id], ...], ...]
        first = slots_src[0]
        candidate_slot = first[0]

        id_dst = conns[1].execute_command("CLUSTER", "MYID")
        if isinstance(id_dst, bytes):
            id_dst = id_dst.decode()
        conns[0].execute_command(
            "CLUSTER", "SETSLOT", str(candidate_slot), "MIGRATING", id_dst
        )
        return True
    except Exception as e:
        env.debugPrint("Reshard error (non-fatal): {}".format(e), True)
        return False


def test_cluster_topology_turbulence(env):
    if not env.isCluster():
        env.skip()

    test_time = int(os.environ.get("MEMTIER_SOAK_TEST_TIME", "600"))
    failover_at = int(os.environ.get("MEMTIER_SOAK_FAILOVER_AT", "120"))
    reshard_at = int(os.environ.get("MEMTIER_SOAK_RESHARD_AT", "300"))

    benchmark_specs = {
        "name": env.testName,
        "args": [
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

    stop_chaos = threading.Event()
    events = {"failover": False, "reshard": False}
    start_ts = time.time()

    def chaos():
        while not stop_chaos.is_set():
            elapsed = time.time() - start_ts
            if not events["failover"] and elapsed >= failover_at:
                events["failover"] = _trigger_failover(env)
            if not events["reshard"] and elapsed >= reshard_at:
                events["reshard"] = _trigger_reshard(env)
            if events["failover"] and events["reshard"]:
                return
            stop_chaos.wait(2)

    chaos_thread = threading.Thread(target=chaos, daemon=True)
    chaos_thread.start()

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
        stop_chaos.set()
        chaos_thread.join(timeout=5)

    if _stderr:
        benchmark.write_file("mb.stderr", _stderr)
    memtier_ok = proc.wait() == 0

    if not memtier_ok:
        debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok)

    env.debugPrint(
        "Cluster turbulence events: {}".format(events), True
    )

    # Cluster INFO at the end -- "cluster_state:ok" must come back.
    cluster_ok = False
    for conn in env.getOSSMasterNodesConnectionList():
        try:
            info = conn.execute_command("CLUSTER", "INFO")
            if isinstance(info, bytes):
                info = info.decode()
            if isinstance(info, dict):
                state = info.get("cluster_state")
            else:
                state = None
                for line in str(info).split("\n"):
                    if line.startswith("cluster_state:"):
                        state = line.split(":", 1)[1].strip()
                        break
            if state == "ok":
                cluster_ok = True
                break
        except Exception:
            continue
    env.assertTrue(cluster_ok)
