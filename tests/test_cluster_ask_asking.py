"""
--retry-on-error after -ASK during a slot migration.

While a slot is MIGRATING on its owner and IMPORTING on another primary, the
owner answers -ASK for keys it no longer holds. The importing node serves such
a key only to a command sent right after ASKING; anything else is answered
with -MOVED back to the owner. The retry must therefore go to the node named
in the -ASK reply, preceded by ASKING, or it bounces until --max-retries runs
out and ends as an error.

  OSS_STANDALONE=0 TEST=test_cluster_ask_asking.py ./tests/run_tests.sh
"""

import csv
import json
import os
import subprocess
import tempfile

from include import (
    add_required_env_arguments,
    addTLSArgs,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig
from test_cluster_failover_retry import _cluster_nodes, _node_conn

KEY_MAXIMUM = 200


def _migrate_half(env):
    """Load memtier-1..KEY_MAXIMUM, then leave every slot of one primary that
    holds a key mid-migration to another primary, with every other key moved.
    Returns (source, target, slots)."""
    seed = env.getOSSMasterNodesConnectionList()[0]
    masters = [n for n in _cluster_nodes(seed) if "master" in n["flags"]]
    source, target = masters[0], masters[1]
    src, dst = _node_conn(env, source), _node_conn(env, target)

    owner = {}
    for start, end, primary, *_ in seed.execute_command("CLUSTER", "SLOTS"):
        port = int(primary[1])
        for slot in range(int(start), int(end) + 1):
            owner[slot] = port
    by_slot = {}
    for i in range(1, KEY_MAXIMUM + 1):
        key = "memtier-{}".format(i)
        slot = int(seed.execute_command("CLUSTER", "KEYSLOT", key))
        _node_conn(env, {"port": owner[slot], "host": source["host"]}).execute_command("SET", key, "v")
        if owner[slot] == source["port"]:
            by_slot.setdefault(slot, []).append(key)

    password = src.connection_pool.connection_kwargs.get("password")
    moved = 0
    for slot, keys in by_slot.items():
        dst.execute_command("CLUSTER", "SETSLOT", slot, "IMPORTING", source["id"])
        src.execute_command("CLUSTER", "SETSLOT", slot, "MIGRATING", target["id"])
        half = keys[::2]
        args = ["MIGRATE", target["host"], target["port"], "", 0, 5000]
        if password:
            args += ["AUTH", password]
        src.execute_command(*(args + ["KEYS"] + half))
        moved += len(half)
    env.assertGreater(moved, 0)
    return source, target, list(by_slot)


def _restore(env, source, target, slots):
    for node in (source, target):
        conn = _node_conn(env, node)
        conn.execute_command("FLUSHALL")
        for slot in slots:
            conn.execute_command("CLUSTER", "SETSLOT", slot, "STABLE")


def _run(env, label, extra_args, pipeline, transaction=False):
    threads, clients, requests = 1, 2, 2000
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--key-minimum=1",
            "--key-maximum={}".format(KEY_MAXIMUM),
            "--retry-on-error",
            "--max-retries=3",
            "--hide-histogram",
        ] + extra_args,
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=threads, clients=clients, requests=requests)
    config["memtier_benchmark"]["pipeline"] = pipeline
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
    run_config = RunConfig(tempfile.mkdtemp(), env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)
    failed_keys = os.path.join(run_config.results_dir, "failed-keys.csv")
    benchmark_specs["args"].append("--failed-keys-file=" + failed_keys)
    benchmark = Benchmark.from_json(run_config, benchmark_specs)

    source, target, slots = _migrate_half(env)
    try:
        result = subprocess.run(benchmark.args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                                timeout=120)
    finally:
        _restore(env, source, target, slots)
    stderr = result.stderr.decode("utf-8", errors="replace")
    env.assertEqual(result.returncode, 0, message=label + "\n" + stderr[-4000:])
    with open(os.path.join(run_config.results_dir, "mb.json")) as f:
        stats = json.load(f)["ALL STATS"]
    totals = stats["Totals"]
    env.assertEqual(totals["Count"], threads * clients * requests, message=label)
    # The migration was really in progress: the source answered -ASK.
    name = "Gets" if stats["Gets"]["Count"] else "Sets"
    env.assertGreater(stats[name]["ASK/sec"], 0, message=label)
    failures = []
    if os.path.isfile(failed_keys):
        with open(failed_keys, newline="") as f:
            failures = [row["status"] for row in csv.DictReader(f)]
    if transaction:
        # A redirect inside a MULTI block is dropped, never resent, so the
        # block is not split (ASKING would have to precede MULTI itself).
        # Nothing may reach the importing node without ASKING, which it
        # would answer with -MOVED.
        env.assertEqual([s for s in failures if s.startswith("-MOVED")], [], message=label)
        return
    # Every request ASKed to the importing node then completes there.
    env.assertEqual(len(failures), 0, message="{}: {}".format(label, failures[:3]))
    if name == "Gets":
        env.assertEqual(totals["Misses/sec"], 0, message=label)


def test_ask_retry_sends_asking(env):
    if not env.isCluster():
        env.skip()
        return
    for pipeline in (1, 16):
        _run(env, "get pipeline={}".format(pipeline), ["--ratio=0:1", "--key-pattern=R:R"], pipeline)
        _run(env, "set pipeline={}".format(pipeline), ["--ratio=1:0", "--key-pattern=R:R"], pipeline)
        _run(env, "arbitrary get pipeline={}".format(pipeline),
             ["--command=GET __key__", "--command-key-pattern=R"], pipeline)
        _run(env, "transaction pipeline={}".format(pipeline),
             ["--command=MULTI", "--command=GET __key__", "--command=SET __key__ __data__",
              "--command=EXEC", "--command-key-pattern=R", "--transaction"], pipeline, transaction=True)
