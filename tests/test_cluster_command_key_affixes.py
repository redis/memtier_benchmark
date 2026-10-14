"""Cluster routing must hash the key that --command actually sends.

A key argument such as '{tag}-__key__' or 'user:__key__' puts literal text
around the generated key. Redis hashes the whole argument (only the tag when
there is one), so memtier must do the same when it picks the shard, or the
request comes back with -MOVED.
"""
import json
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


def _masters_by_port(env):
    return {int(conn.connection_pool.connection_kwargs["port"]): conn
            for conn in env.getOSSMasterNodesConnectionList()}


def _owning_port(env, tag):
    slot = key_slot(tag.encode())
    any_master = next(iter(env.getOSSMasterNodesConnectionList()))
    for entry in any_master.execute_command("CLUSTER", "SLOTS"):
        if int(entry[0]) <= slot <= int(entry[1]):
            return int(entry[2][1])
    raise AssertionError("no master owns slot {} of tag {!r}".format(slot, tag))


def _tags_one_per_shard(env):
    masters = len(env.getOSSMasterNodesConnectionList())
    chosen = {}
    for tag in ['a', 'b', 'c', 'd', 'f', 'g', 'h', 'j', 'k', 'ask']:
        chosen.setdefault(_owning_port(env, tag), tag)
        if len(chosen) == masters:
            break
    env.assertEqual(len(chosen), masters, message="need one tag per shard")
    return sorted(chosen.items())


def _moved_errors(conn):
    stats = conn.execute_command("INFO", "ERRORSTATS")
    entry = stats.get("errorstat_MOVED", {})
    return int(entry.get("count", 0)) if isinstance(entry, dict) else 0


def _check(env, command, row, owner=None):
    """Run --command and assert no server replied -MOVED. With owner set,
    also assert every key landed on that shard."""
    for conn in env.getOSSMasterNodesConnectionList():
        conn.execute_command("FLUSHALL")
        conn.execute_command("CONFIG", "RESETSTAT")

    benchmark_specs = {"name": env.testName,
                       "args": ["--command=" + command, "--key-minimum=1",
                                "--key-maximum=1000"]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=2, requests=200)
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
    run_config = RunConfig(tempfile.mkdtemp(), env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)
    ok = Benchmark.from_json(run_config, benchmark_specs).run()

    label = repr(command)
    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message=label)
        for port, conn in _masters_by_port(env).items():
            env.assertEqual(_moved_errors(conn), 0,
                            message="{}: port {} replied -MOVED".format(label, port))
            if owner is not None and port != owner:
                env.assertEqual(int(conn.execute_command("DBSIZE")), 0,
                                message="{}: keys on port {}".format(label, port))
        with open("{}/mb.json".format(run_config.results_dir)) as source:
            stats = json.load(source)["ALL STATS"]
        env.assertEqual(stats[row]["MOVED/sec"], 0, message=label)
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


def test_cluster_command_key_hash_tag_no_moved(env):
    if not env.isCluster():
        env.skip()
        return
    for owner, tag in _tags_one_per_shard(env):
        _check(env, "SET {" + tag + "}-__key__ __data__", "Sets", owner=owner)
        _check(env, "SET __key__:{" + tag + "} __data__", "Sets", owner=owner)
        _check(env, "GET {" + tag + "}-__key__", "Gets")


def test_cluster_command_key_prefix_no_moved(env):
    """No tag: the slot comes from the whole 'user:<key>' string."""
    if not env.isCluster():
        env.skip()
        return
    _check(env, "SET user:__key__ __data__", "Sets")
    _check(env, "GET user:__key__", "Gets")
