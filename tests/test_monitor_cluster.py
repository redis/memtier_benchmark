"""
Cluster-mode coverage for `--monitor-input` (route-then-stage).

Background
----------
In `--cluster-mode`, `--monitor-input` replays a captured command stream by
routing each command to the shard that owns its key's slot: a shard connection
reads a monitor line, computes the key slot, and pushes the command into the
slot owner's staged queue (waking it via schedule_fill), or — if that queue is
full — sends it inline and lets the resulting MOVED refresh topology. The
draining shard formats and sends the staged command.

Until now this entire route-then-stage path had NO automated cluster coverage:
every test in `tests/test_monitor_input.py` begins with `env.skipOnCluster()`.
These tests close that gap with DETERMINISTIC assertions (routing correctness,
run completion / accounting balance, and per-type stats attribution). They
deliberately avoid topology-churn-during-run (slot migration mid-replay), which
is inherently racy in this harness; that remains tracked as a separate gap.

Run:
    TEST=test_monitor_cluster.py OSS_CLUSTER=1 SHARDS=3 ./tests/run_tests.sh
"""

import json
import os
import tempfile

from redis.cluster import key_slot

from include import (
    add_required_env_arguments,
    addTLSArgs,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig


# ---------------------------------------------------------------------------
# Cluster helpers (same pattern as test_mget_cluster.py / test_cluster_transaction.py)
# ---------------------------------------------------------------------------

def _master_conns(env):
    return list(env.getOSSMasterNodesConnectionList())


def _flush_cluster(env):
    for conn in _master_conns(env):
        conn.execute_command("FLUSHALL")


def _dbsize_total_and_shards(env):
    """Return (total_keys, number_of_shards_with_data)."""
    total = 0
    shards_with_data = 0
    for conn in _master_conns(env):
        n = int(conn.execute_command("DBSIZE"))
        total += n
        if n > 0:
            shards_with_data += 1
    return total, shards_with_data


def _owning_port(env, key):
    """Master port owning the slot for *key*, via CLUSTER SLOTS (layout-agnostic)."""
    slot = key_slot(key.encode())
    any_master = _master_conns(env)[0]
    for entry in any_master.execute_command("CLUSTER", "SLOTS"):
        if int(entry[0]) <= slot <= int(entry[1]):
            return int(entry[2][1])
    raise AssertionError("no owner for slot {} of key {!r}".format(slot, key))


def _get_from_cluster(env, key):
    """GET a key from whichever master owns it; None if absent/unreachable."""
    for conn in _master_conns(env):
        try:
            val = conn.execute_command("GET", key)
        except Exception:
            continue  # non-owner replies MOVED
        if val is not None:
            return val.decode() if isinstance(val, bytes) else val
    return None


def _conn_for_port(env, port):
    for conn in _master_conns(env):
        if int(conn.connection_pool.connection_kwargs["port"]) == port:
            return conn
    return None


def _write_monitor_file(test_dir, lines):
    path = os.path.join(test_dir, "monitor.txt")
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    return path


def _set_line(key, value):
    return '[ proxy ] 1764031576.604009 [0 127.0.0.1:5000] "SET" "{}" "{}"'.format(key, value)


def _incr_line(key):
    return '[ proxy ] 1764031576.604009 [0 127.0.0.1:5000] "INCR" "{}"'.format(key)


def _require_multi_shard(env):
    """Skip on a single-master cluster (SHARDS=1): the cross-shard routing
    assertions below are meaningful only with >= 2 masters."""
    if len(_master_conns(env)) < 2:
        env.skip()
        return False
    return True


def _run_monitor_cluster(env, monitor_file, command, monitor_pattern=None,
                         threads=2, clients=4, requests=200, extra=None):
    """Run a monitor-input cluster workload; return (ok, run_config, json_dict)."""
    args = [
        "--monitor-input={}".format(monitor_file),
        "--command={}".format(command),
        "--hide-histogram",
    ]
    if monitor_pattern is not None:
        args.append("--monitor-pattern={}".format(monitor_pattern))
    if extra:
        args.extend(extra)

    benchmark_specs = {"name": env.testName, "args": args}
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=threads, clients=clients, requests=requests)
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())

    test_dir = tempfile.mkdtemp()
    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)

    benchmark = Benchmark.from_json(run_config, benchmark_specs)
    ok = benchmark.run()
    # Note: callers print mb output via debugPrintMemtierOnError in their own
    # finally-on-failure blocks, so we don't print here (avoids double output).

    js = {}
    json_path = "{}/mb.json".format(run_config.results_dir)
    if os.path.isfile(json_path):
        with open(json_path) as fh:
            js = json.load(fh)
    return ok, run_config, js


# ---------------------------------------------------------------------------
# 1. A single staged command (__monitor_line@__) routes to the slot owner
# ---------------------------------------------------------------------------

def test_monitor_cluster_single_command_staged_to_owner(env):
    """A monitor SET replayed via __monitor_line@__ (the route-then-stage path,
    create_monitor_request_cluster) must be staged to and commit on its slot
    owner — and ONLY there (exactly one copy cluster-wide, no MOVED leakage).

    Note: __monitor_line@__ (not __monitor_lineN__) is required to exercise
    staging — specific-line placeholders are expanded at config time into an
    ordinary keyed command that takes the normal routing path."""
    if not env.isCluster():
        env.skip()
        return

    _flush_cluster(env)
    test_dir = tempfile.mkdtemp()
    monitor_file = _write_monitor_file(test_dir, [_set_line("mon-key1", "value1")])

    # Single line => __monitor_line@__ selection always picks it; it is staged
    # to mon-key1's slot owner and drained there.
    ok, run_config, _js = _run_monitor_cluster(
        env, monitor_file, "__monitor_line@__", monitor_pattern="S",
        threads=1, clients=1, requests=20)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier did not complete the specific-line cluster run")
        env.assertEqual(_get_from_cluster(env, "mon-key1"), "value1",
                        message="mon-key1 not committed via routed staged command")
        # Exactly one copy across the cluster (no MOVED leakage onto a wrong shard),
        # and it lives on the computed slot owner. EXISTS is queried only on the
        # owner connection — a non-owner plain node would reply MOVED (raise).
        total, _shards = _dbsize_total_and_shards(env)
        env.assertEqual(total, 1, message="expected exactly 1 key in cluster, got {}".format(total))
        owner_conn = _conn_for_port(env, _owning_port(env, "mon-key1"))
        env.assertTrue(owner_conn is not None and owner_conn.execute_command("EXISTS", "mon-key1"),
                       message="mon-key1 not on its slot-owning shard")
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


# ---------------------------------------------------------------------------
# 2. Sequential replay: every SET is routed and committed exactly once
# ---------------------------------------------------------------------------

def test_monitor_cluster_sequential_each_command_applied_once(env):
    """__monitor_line@__ sequential over N distinct INCR keys spanning shards:
    every INCR must be routed to the correct owner and applied EXACTLY once.

    INCR (not SET) is used deliberately: it is not idempotent, so the per-key
    value pins down the regression class precisely — a dropped command leaves
    the key absent, a duplicated command leaves it at 2, and a wrong-shard send
    lands it off its computed owner. SET would mask duplication. Combined with
    `total DBSIZE == N` this proves the staged-queue accounting nets out (no
    hang) and routing is correct."""
    if not env.isCluster():
        env.skip()
        return
    if not _require_multi_shard(env):
        return

    _flush_cluster(env)
    test_dir = tempfile.mkdtemp()
    n = 30
    lines = [_incr_line("mc:{}".format(i)) for i in range(n)]
    monitor_file = _write_monitor_file(test_dir, lines)

    # Sequential pattern + requests == number of lines => each line runs once.
    ok, run_config, _js = _run_monitor_cluster(
        env, monitor_file, "__monitor_line@__", monitor_pattern="S",
        threads=1, clients=1, requests=n)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="sequential monitor-cluster run did not complete")
        total, shards = _dbsize_total_and_shards(env)
        env.assertEqual(total, n,
                        message="expected {} committed keys, got {} (commands lost/duplicated?)".format(n, total))
        env.assertTrue(shards >= 2,
                       message="keys landed on only {} shard(s) — cross-shard staging not exercised".format(shards))
        # Each INCR must have been applied exactly once -> value "1" on its owner.
        for i in (0, n // 2, n - 1):
            key = "mc:{}".format(i)
            owner_conn = _conn_for_port(env, _owning_port(env, key))
            val = owner_conn.execute_command("GET", key) if owner_conn is not None else None
            if isinstance(val, bytes):
                val = val.decode()
            env.assertEqual(val, "1",
                            message="{} = {!r} on its owner (expected '1'; drop/dup/misroute?)".format(key, val))
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


# ---------------------------------------------------------------------------
# 3. Random mixed-type replay: completes (no hang), distributes across shards,
#    and attributes per-type stats correctly after cross-shard routing.
# ---------------------------------------------------------------------------

def test_monitor_cluster_random_distributes_and_attributes_stats(env):
    """A mixed-type random stream (__monitor_line@__, --monitor-pattern=R) must:
    complete without hanging (accounting nets out), land data on >= 2 shards
    (cross-shard routing actually happened), and attribute per-command stats
    correctly even though commands are staged to and drained from other shards
    (Sets and Gets each non-zero and summing to Totals)."""
    if not env.isCluster():
        env.skip()
        return
    if not _require_multi_shard(env):
        return

    _flush_cluster(env)
    test_dir = tempfile.mkdtemp()
    # Distinct keys per type so they spread across shards; types: SET + GET.
    lines = []
    for i in range(20):
        lines.append(_set_line("st:{}".format(i), "v{}".format(i)))
        lines.append('[ proxy ] 1764031576.604010 [0 127.0.0.1:5000] "GET" "st:{}"'.format(i))
    monitor_file = _write_monitor_file(test_dir, lines)

    ok, run_config, js = _run_monitor_cluster(
        env, monitor_file, "__monitor_line@__", monitor_pattern="R",
        threads=1, clients=2, requests=150)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="mixed-type monitor-cluster run did not complete")
        all_stats = js.get("ALL STATS", {})
        # Default breakdown aggregates by type -> "Sets" and "Gets" sections.
        env.assertContains("Sets", all_stats)
        env.assertContains("Gets", all_stats)
        sets_count = all_stats["Sets"].get("Count", 0)
        gets_count = all_stats["Gets"].get("Count", 0)
        env.assertTrue(sets_count > 0, message="no SET ops attributed after cross-shard routing")
        env.assertTrue(gets_count > 0, message="no GET ops attributed after cross-shard routing")
        totals_count = all_stats.get("Totals", {}).get("Count", 0)
        env.assertEqual(sets_count + gets_count, totals_count,
                        message="per-type counts ({}+{}) != Totals ({})".format(
                            sets_count, gets_count, totals_count))
        # Server-side placement: the SETs must actually have landed on >= 2
        # shards. This is what proves routing happened (the count equality above
        # is only an accounting-conservation check and holds even if every op
        # ran inline on the seed node).
        _total, shards = _dbsize_total_and_shards(env)
        env.assertTrue(shards >= 2,
                       message="SET data reached only {} shard(s) — cross-shard routing not exercised".format(shards))
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)
