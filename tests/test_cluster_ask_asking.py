"""
--retry-on-error after -ASK during a slot migration.

While a slot is MIGRATING on its owner and IMPORTING on another primary, the
owner answers -ASK for keys it no longer holds. The importing node serves such
a key only to a command sent right after ASKING; anything else is answered
with -MOVED back to the owner. The retry must therefore go to the node named
in the -ASK reply, preceded by ASKING, or it bounces until --max-retries runs
out and ends as an error. That holds while the connection to the importing
node is down and waiting to reconnect, too.

  OSS_STANDALONE=0 TEST=test_cluster_ask_asking.py ./tests/run_tests.sh
"""

import csv
import json
import os
import subprocess
import tempfile
import threading

import redis

from include import (
    add_required_env_arguments,
    addTLSArgs,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig
from test_cluster_failover_retry import _cluster_nodes

KEY_MAXIMUM = 200


def _conn(env, node):
    """A new connection to `node`, with the settings (TLS, password) RLTest
    uses. Never one of RLTest's own: CLIENT KILL may have closed those."""
    base = env.getOSSMasterNodesConnectionList()[0].connection_pool
    kwargs = dict(base.connection_kwargs)
    kwargs["host"] = node["host"]
    kwargs["port"] = node["port"]
    return redis.Redis(connection_pool=redis.ConnectionPool(connection_class=base.connection_class, **kwargs))


def _masters(env):
    seed = env.getOSSMasterNodesConnectionList()[0]
    return [n for n in _cluster_nodes(seed) if "master" in n["flags"]]


def _migrate_even_keys(env, masters):
    """Load memtier-1..KEY_MAXIMUM, put every slot of the source primary that
    holds one of them mid-migration to the target primary, and move the
    even-numbered keys only. The odd ones stay on the source, which serves
    them; the source answers -ASK for the others. Returns the slots."""
    source, target = masters[0], masters[1]
    conns = dict((n["port"], _conn(env, n)) for n in masters)
    src, dst = conns[source["port"]], conns[target["port"]]

    owner = {}
    for start, end, primary, *_ in src.execute_command("CLUSTER", "SLOTS"):
        for slot in range(int(start), int(end) + 1):
            owner[slot] = int(primary[1])
    by_slot = {}
    for i in range(1, KEY_MAXIMUM + 1):
        key = "memtier-{}".format(i)
        slot = int(src.execute_command("CLUSTER", "KEYSLOT", key))
        conns[owner[slot]].execute_command("SET", key, "v")
        if owner[slot] == source["port"]:
            by_slot.setdefault(slot, []).append(i)

    password = src.connection_pool.connection_kwargs.get("password")
    moved = kept = 0
    for slot, ids in by_slot.items():
        dst.execute_command("CLUSTER", "SETSLOT", slot, "IMPORTING", source["id"])
        src.execute_command("CLUSTER", "SETSLOT", slot, "MIGRATING", target["id"])
        even = ["memtier-{}".format(i) for i in ids if i % 2 == 0]
        kept += len(ids) - len(even)
        if not even:
            continue
        args = ["MIGRATE", target["host"], target["port"], "", 0, 5000]
        if password:
            args += ["AUTH", password]
        src.execute_command(*(args + ["KEYS"] + even))
        moved += len(even)
    env.assertGreater(moved, 0)
    env.assertGreater(kept, 0)
    for conn in conns.values():
        conn.execute_command("CONFIG", "RESETSTAT")
    return list(by_slot)


def _restore(env, masters, slots):
    for node in masters:
        conn = _conn(env, node)
        conn.execute_command("FLUSHALL")
        for slot in slots:
            conn.execute_command("CLUSTER", "SETSLOT", slot, "STABLE")


def _asking_calls(env, node):
    stats = _conn(env, node).info("commandstats")
    return int(stats.get("cmdstat_asking", {}).get("calls", 0))


def _kill_clients(env, node, stop):
    # Drop every client connection to the importing node (but this one) while
    # memtier runs, so its connection keeps waiting on a reconnect timer.
    conn = _conn(env, node)
    while not stop.wait(0.5):
        try:
            conn.execute_command("CLIENT", "KILL", "TYPE", "normal")
        except redis.RedisError:
            pass


def _run(env, label, extra_args, pipeline, transaction=False, kill=False, requests=2000, timeout=30):
    threads, clients = 1, 2
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
    password = env.getOSSMasterNodesConnectionList()[0].connection_pool.connection_kwargs.get("password")
    if password:
        # e.g. --oss_password: resends after a reconnect must follow AUTH (#527).
        benchmark_specs["args"].append("--authenticate=" + password)
    run_config = RunConfig(tempfile.mkdtemp(), env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)
    failed_keys = os.path.join(run_config.results_dir, "failed-keys.csv")
    benchmark_specs["args"].append("--failed-keys-file=" + failed_keys)
    benchmark = Benchmark.from_json(run_config, benchmark_specs)

    masters = _masters(env)
    source, target, other = masters[0], masters[1], masters[2]
    slots = _migrate_even_keys(env, masters)
    stop = threading.Event()
    killer = threading.Thread(target=_kill_clients, args=(env, target, stop), daemon=True)
    if kill:
        killer.start()
    try:
        result = subprocess.run(benchmark.args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                                timeout=timeout)
    finally:
        stop.set()
        if kill:
            killer.join()
        asking = dict((n["port"], _asking_calls(env, n)) for n in masters)
        _restore(env, masters, slots)
    stderr = result.stderr.decode("utf-8", errors="replace")
    env.assertEqual(result.returncode, 0, message=label + "\n" + stderr[-4000:])
    with open(os.path.join(run_config.results_dir, "mb.json")) as f:
        stats = json.load(f)["ALL STATS"]
    totals = stats["Totals"]
    env.assertEqual(totals["Count"], threads * clients * requests, message=label)
    # The migration was really in progress: the source answered -ASK.
    name = "Gets" if stats["Gets"]["Count"] else "Sets"
    env.assertGreater(stats[name]["ASK/sec"], 0, message=label)
    # ASKING went to the importing node only. All nodes share one host, so a
    # wrong match on the address could still land on the right port by luck.
    env.assertEqual(asking[source["port"]], 0, message=label)
    env.assertEqual(asking[other["port"]], 0, message=label)
    failures = []
    if os.path.isfile(failed_keys):
        with open(failed_keys, newline="") as f:
            failures = [row["status"] for row in csv.DictReader(f)]
    if transaction:
        # A redirect inside a MULTI block is dropped, never resent, so the
        # block is not split (ASKING would have to precede MULTI itself).
        # Nothing may reach the importing node without ASKING, which it
        # would answer with -MOVED.
        env.assertEqual(asking[target["port"]], 0, message=label)
        env.assertEqual([s for s in failures if s.startswith("-MOVED")], [], message=label)
        return
    env.assertGreater(asking[target["port"]], 0, message=label)
    if kill:
        # A request in flight on a killed connection is resent after the
        # reconnect, and each resend counts toward --max-retries, so some may
        # end as "connection-dropped". None may end on a redirect.
        failures = [s for s in failures if s.startswith(("-ASK", "-MOVED"))]
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


def test_ask_retry_waits_for_reconnect(env):
    """-ASK retries while the importing node keeps dropping memtier's
    connections wait for the reconnect instead of bouncing on the slot map."""
    if not env.isCluster():
        env.skip()
        return
    _run(env, "reconnect", ["--ratio=1:1", "--key-pattern=R:R", "--reconnect-on-error",
                            "--reconnect-backoff-factor=1", "--rate-limiting=500"],
         8, kill=True, requests=1500, timeout=60)
