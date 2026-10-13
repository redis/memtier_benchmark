"""
Cluster slot map refreshes that retire a connection mid-run.

Primary failover with --retry-on-error (issue #568): when a primary dies, its
connection keeps the requests that were in flight (and any waiting for a
retry) so it can resend them after reconnecting. Once the replica is
promoted, the CLUSTER SLOTS refresh retires that connection: it no longer
owns any slot, so it never reconnects. Those requests were counted as
generated, so a --requests run used to stop a few requests short of its
count and exit 1. They are now resent on the connection that owns their
slot. Needs a cluster with replicas that take part in failover (skipped
otherwise):

  OSS_STANDALONE=0 OSS_CLUSTER_REPLICAS=1 \\
      TEST=test_cluster_failover_retry.py ./tests/run_tests.sh

Slot move off a live primary (issue #578): the primary's own connection
sends the CLUSTER SLOTS refresh after -MOVED and is retired while handling
the reply. It used to go on parsing the replies buffered behind it (abort in
pop_req() with --retry-on-error) or leave the other connections parked
(early stop without it). These tests need no replicas and also run on a
plain cluster:

  OSS_STANDALONE=0 OSS_CLUSTER=1 \\
      TEST=test_cluster_failover_retry.py ./tests/run_tests.sh
"""

import json
import os
import signal
import subprocess
import tempfile
import time

import redis

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


def _slot_ranges(conn, node_id):
    raw = conn.execute_command("CLUSTER", "NODES")
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    for line in raw.strip().split("\n"):
        parts = line.split()
        if not parts or parts[0] != node_id:
            continue
        ranges = []
        for token in parts[8:]:
            if token.startswith("["):
                continue  # slot being imported or migrated
            start, _, end = token.partition("-")
            ranges.append((int(start), int(end or start)))
        return ranges
    return []


SLOT_MOVE_CYCLES = 6


def _move_slots(env, nodes, ranges, old_owner, new_owner):
    """Reassign ranges from old_owner to new_owner on every node. Gossip from
    a node that has not dropped the slots yet can hand them back to the old
    owner in between, so ADDSLOTSRANGE may find them busy: delete again (old
    owner first) and retry."""
    others = [n for n in nodes if n["id"] != old_owner["id"]]
    new_conn = _node_conn(env, new_owner)
    deadline = time.time() + 30
    while True:
        for node in [old_owner] + others:
            conn = _node_conn(env, node)
            for start, end in ranges:
                try:
                    conn.execute_command("CLUSTER", "DELSLOTSRANGE", start, end)
                except redis.exceptions.ResponseError:
                    pass  # already unassigned in this node's view
        try:
            for start, end in ranges:
                new_conn.execute_command("CLUSTER", "ADDSLOTSRANGE", start, end)
            break
        except redis.exceptions.ResponseError as e:
            if "busy" not in str(e) or time.time() > deadline:
                raise
            time.sleep(0.05)
    new_conn.execute_command("CLUSTER", "BUMPEPOCH")


def _run_slots_move_off_primary(env, retry_on_error):
    """Move every slot of a primary to another primary mid-run. The CLUSTER
    SLOTS refresh that the victim's own connection sends after -MOVED
    retires that connection from inside its own reply handler (issue #578)."""
    if not env.isCluster():
        env.skip()
        return

    seed = env.getOSSMasterNodesConnectionList()[0]
    seed_port = env.getMasterNodesList()[0]["port"]
    nodes = _cluster_nodes(seed)
    target = None
    victim = None
    victim_ranges = []
    for node in nodes:
        if "master" not in node["flags"]:
            continue
        if node["port"] == seed_port:
            target = node
        elif victim is None:
            ranges = _slot_ranges(seed, node["id"])
            if ranges:
                victim, victim_ranges = node, ranges
    if victim is None or target is None:
        env.skip()
        return

    for node in nodes:
        _node_conn(env, node).execute_command("CONFIG", "SET", "cluster-node-timeout", "2000")

    threads, clients, requests = 1, 2, 1500000
    args = [
        "--ratio=1:1",
        "--key-minimum=1",
        "--key-maximum=100000",
        "--reconnect-on-error",
        "--hide-histogram",
    ]
    if retry_on_error:
        args.append("--retry-on-error")
    benchmark_specs = {"name": env.testName, "args": args}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=threads, clients=clients, requests=requests)
    # Replies left in the input buffer behind the CLUSTER SLOTS reply are
    # what the retired connection used to go on parsing.
    config["memtier_benchmark"]["pipeline"] = 16
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
    run_config = RunConfig(tempfile.mkdtemp(), env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)
    benchmark = Benchmark.from_json(run_config, benchmark_specs)

    stderr_path = os.path.join(run_config.results_dir, "mb.stderr")
    with open(stderr_path, "wb") as stderr_file:
        proc = subprocess.Popen(benchmark.args, stdout=subprocess.DEVNULL, stderr=stderr_file)
        try:
            victim_conn = _node_conn(env, victim)
            # Whether the retiring connection still has replies buffered
            # behind its CLUSTER SLOTS reply is a matter of timing, so move
            # the slots off the victim (and back) a few times per run.
            moves = 0
            for cycle in range(SLOT_MOVE_CYCLES):
                # Wait for memtier to be writing to the victim (again).
                base = int(_info_field(victim_conn, "stats", "total_commands_processed"))
                deadline = time.time() + 60
                while time.time() < deadline and proc.poll() is None:
                    if int(_info_field(victim_conn, "stats", "total_commands_processed")) > base + 20000:
                        break
                    time.sleep(0.05)
                if proc.poll() is not None:
                    break
                # The victim keeps serving (no failover): it just owns no
                # slot any more, so memtier retires its connections.
                _move_slots(env, nodes, victim_ranges, victim, target)
                moves += 1
                if cycle + 1 < SLOT_MOVE_CYCLES:
                    time.sleep(1)
                    _move_slots(env, nodes, victim_ranges, target, victim)
            env.assertTrue(moves > 0, message="memtier finished before the slots were moved; raise --requests")
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

    env.assertEqual(rc, 0)
    env.assertFalse("Assertion" in stderr)
    env.assertFalse("stopped before reaching their stop condition" in stderr)
    env.assertFalse("ended before the requested request count was reached" in stderr)
    json_path = os.path.join(run_config.results_dir, "mb.json")
    env.assertTrue(os.path.isfile(json_path))
    # An aborted run leaves a truncated JSON file behind.
    if rc == 0 and os.path.isfile(json_path):
        with open(json_path) as f:
            totals = json.load(f)["ALL STATS"]["Totals"]["Count"]
        env.assertEqual(totals, threads * clients * requests,
                        message="{} ops, expected {}".format(totals, threads * clients * requests))


def test_requests_complete_after_slots_move_off_primary(env):
    """With --retry-on-error the retired connection's pipeline goes to the
    replay queue; the replies still buffered behind its CLUSTER SLOTS reply
    used to be matched against the emptied pipeline and abort memtier."""
    _run_slots_move_off_primary(env, retry_on_error=True)


def test_requests_complete_after_slots_move_off_primary_without_retry(env):
    """Without --retry-on-error the retired connection's pipeline is dropped.
    Producers parked on the in-flight budget used to be left asleep, so the
    event loop ran out of events before --requests was reached."""
    _run_slots_move_off_primary(env, retry_on_error=False)
