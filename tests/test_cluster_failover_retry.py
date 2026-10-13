"""
--retry-on-error across a primary failover in cluster mode (issue #568).

When a primary dies, its connection keeps the requests that were in flight
(and any waiting for a retry) so it can resend them after reconnecting. Once
the replica is promoted, the CLUSTER SLOTS refresh retires that connection:
it no longer owns any slot, so it never reconnects. Those requests were
counted as generated, so a --requests run used to stop a few requests short
of its count and exit 1. They are now resent on the connection that owns
their slot.

Needs a cluster with replicas that take part in failover:

  OSS_STANDALONE=0 OSS_CLUSTER_REPLICAS=1 \\
      TEST=test_cluster_failover_retry.py ./tests/run_tests.sh
"""

import json
import os
import signal
import subprocess
import tempfile
import time

from include import (
    add_required_env_arguments,
    addTLSArgs,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig


def _cluster_nodes(conn):
    raw = conn.execute_command("CLUSTER", "NODES")
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    nodes = []
    for line in raw.strip().split("\n"):
        parts = line.split()
        if len(parts) < 8:
            continue
        addr = parts[1].partition("@")[0]
        host, _, port = addr.rpartition(":")
        nodes.append({
            "id": parts[0],
            "host": host,
            "port": int(port),
            "flags": parts[2].split(","),
            "master_id": parts[3],
        })
    return nodes


def _node_conn(env, node):
    # Same connection settings (TLS, password) RLTest uses for its own nodes.
    for conn in env.getOSSMasterNodesConnectionList():
        kwargs = conn.connection_pool.connection_kwargs
        if int(kwargs.get("port")) == node["port"]:
            return conn
    import redis
    base = env.getOSSMasterNodesConnectionList()[0].connection_pool
    kwargs = dict(base.connection_kwargs)
    kwargs["host"] = node["host"]
    kwargs["port"] = node["port"]
    return redis.Redis(connection_pool=redis.ConnectionPool(
        connection_class=base.connection_class, **kwargs))


def _info_field(conn, section, field):
    info = conn.execute_command("INFO", section)
    if isinstance(info, dict):
        return info.get(field)
    for line in info.split("\n"):
        if line.startswith(field + ":"):
            return line.split(":", 1)[1].strip()
    return None


def test_requests_complete_after_primary_failover(env):
    """Kill a primary mid-run; the promoted replica's connection must take
    over the requests the dead primary's connection held for resend."""
    if not env.isCluster():
        env.skip()
        return

    seed = env.getOSSMasterNodesConnectionList()[0]
    nodes = _cluster_nodes(seed)
    seed_port = env.getMasterNodesList()[0]["port"]
    victim = None
    replica = None
    for node in nodes:
        if "master" not in node["flags"] or node["port"] == seed_port:
            continue
        replicas = [n for n in nodes if n["master_id"] == node["id"]]
        if replicas:
            victim, replica = node, replicas[0]
            break
    if victim is None:
        # No replica takes part in cluster failover (see the RLTest
        # --use-slaves note in get_cluster_replica_connections).
        env.skip()
        return

    # RLTest starts the nodes with a 15 s node timeout; fail over faster.
    for node in nodes:
        conn = _node_conn(env, node)
        conn.execute_command("CONFIG", "SET", "cluster-node-timeout", "2000")
        conn.execute_command("CONFIG", "SET", "cluster-replica-validity-factor", "0")

    threads, clients, requests = 1, 2, 500000
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--ratio=1:1",
            "--key-minimum=1",
            "--key-maximum=100000",
            "--retry-on-error",
            "--reconnect-on-error",
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=threads, clients=clients, requests=requests)
    config["memtier_benchmark"]["pipeline"] = 16
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
    run_config = RunConfig(tempfile.mkdtemp(), env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)
    benchmark = Benchmark.from_json(run_config, benchmark_specs)

    stderr_path = os.path.join(run_config.results_dir, "mb.stderr")
    with open(stderr_path, "wb") as stderr_file:
        proc = subprocess.Popen(benchmark.args, stdout=subprocess.DEVNULL, stderr=stderr_file)
        try:
            # Kill the primary once memtier is writing to it at full speed,
            # so its connections have requests in flight.
            victim_conn = _node_conn(env, victim)
            pid = int(_info_field(victim_conn, "server", "process_id"))
            deadline = time.time() + 60
            while time.time() < deadline and proc.poll() is None:
                if int(_info_field(victim_conn, "stats", "total_commands_processed")) > 20000:
                    break
                time.sleep(0.05)
            running = proc.poll() is None
            env.assertTrue(running, message="memtier finished before the primary was killed; raise --requests")
            os.kill(pid, signal.SIGKILL)
            try:
                rc = proc.wait(timeout=180)
            except subprocess.TimeoutExpired:
                proc.kill()
                rc = proc.wait()
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()

    with open(stderr_path, "rb") as f:
        stderr = f.read().decode("utf-8", errors="replace")
    if rc != 0:
        env.debugPrint(stderr[-8000:], True)

    # The replica took over the slots (sanity check of the scenario itself).
    role = _node_conn(env, replica).execute_command("ROLE")
    role = role[0].decode() if isinstance(role[0], bytes) else role[0]
    env.assertEqual(role, "master")

    env.assertEqual(rc, 0)
    env.assertFalse("ended before the requested request count was reached" in stderr)
    json_path = os.path.join(run_config.results_dir, "mb.json")
    env.assertTrue(os.path.isfile(json_path))
    if os.path.isfile(json_path):
        with open(json_path) as f:
            totals = json.load(f)["ALL STATS"]["Totals"]["Count"]
        env.assertEqual(totals, threads * clients * requests,
                        message="{} ops, expected {}".format(totals, threads * clients * requests))
