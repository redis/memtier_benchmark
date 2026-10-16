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

import csv
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
                         threads=2, clients=4, requests=200, extra=None, timeout=240):
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
    ok = benchmark.run(timeout=timeout)
    # Note: callers print mb output via debugPrintMemtierOnError in their own
    # finally-on-failure blocks, so we don't print here (avoids double output).

    js = {}
    json_path = "{}/mb.json".format(run_config.results_dir)
    # A hung (killed) or aborted run can leave a truncated JSON file; callers
    # report it through `ok` ("did not complete"), not a JSONDecodeError.
    if ok and os.path.isfile(json_path):
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


# ---------------------------------------------------------------------------
# 4. __key__ / __data__ placeholders in a staged (__monitor_line@__) line
# ---------------------------------------------------------------------------

def _all_keys_by_port(env):
    """{master_port: [keys]} across the cluster."""
    out = {}
    for conn in _master_conns(env):
        port = int(conn.connection_pool.connection_kwargs["port"])
        out[port] = [k.decode() for k in conn.execute_command("KEYS", "*")]
    return out


def test_monitor_cluster_line_placeholders_routed_by_full_key(env):
    """Regression test for #654: a __monitor_line@__ line with __key__ (with
    text around it) or __data__ must send every argument, and be routed by
    the key actually sent (prefix + generated key + suffix).

    Before the fix the cluster monitor send loops wrote only constant
    arguments, so the RESP command was one argument short of its *N header,
    the server never replied and the run hung."""
    if not env.isCluster():
        env.skip()
        return
    if not _require_multi_shard(env):
        return

    _flush_cluster(env)
    test_dir = tempfile.mkdtemp()
    lines = [
        '[ proxy ] 1764031576.604009 [0 127.0.0.1:5000] "SET" "user:__key__" "v"',
        '[ proxy ] 1764031576.604010 [0 127.0.0.1:5000] "SET" "{mtag}__key__" "__data__"',
        # Only the first __key__ is replaced; routing hashes the full text.
        '[ proxy ] 1764031576.604011 [0 127.0.0.1:5000] "SET" "a:__key__:b:__key__" "v"',
    ]
    monitor_file = _write_monitor_file(test_dir, lines)

    requests = 60
    ok, run_config, js = _run_monitor_cluster(
        env, monitor_file, "__monitor_line@__", monitor_pattern="S",
        threads=1, clients=2, requests=requests,
        extra=["--key-prefix=mon-", "--key-maximum=200", "--data-size=20"],
        timeout=60)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="monitor-cluster run with key/data placeholders did not complete")
        by_port = _all_keys_by_port(env)
        user_keys = []
        tag_keys = []
        rep_keys = []
        for port, keys in by_port.items():
            for k in keys:
                # Every key must sit on the shard that owns the full key's slot.
                env.assertEqual(_owning_port(env, k), port,
                                message="{} stored on {} but owned by {}".format(k, port, _owning_port(env, k)))
                if k.startswith("user:mon-"):
                    user_keys.append(k)
                elif k.startswith("{mtag}mon-"):
                    tag_keys.append(k)
                elif k.startswith("a:mon-") and k.endswith(":b:__key__"):
                    rep_keys.append(k)
                else:
                    env.assertTrue(False, message="unexpected key {!r} (affixes dropped?)".format(k))
        env.assertTrue(len(user_keys) > 0, message="no user:mon-* keys written")
        env.assertTrue(len(tag_keys) > 0, message="no {mtag}mon-* keys written")
        env.assertTrue(len(rep_keys) > 0, message="no a:mon-*:b:__key__ keys written")
        # user:__key__ keys hash across the cluster, so routing by the full
        # key must spread them over more than one shard.
        user_ports = set(p for p, keys in by_port.items() for k in keys if k.startswith("user:"))
        env.assertTrue(len(user_ports) >= 2,
                       message="user:mon-* keys on only {} shard(s)".format(len(user_ports)))
        owner = _conn_for_port(env, _owning_port(env, tag_keys[0])) if tag_keys else None
        if owner is not None:
            # __data__ is replaced by a generated --data-size value.
            env.assertEqual(len(owner.execute_command("GET", tag_keys[0])), 20)
        sets_count = js.get("ALL STATS", {}).get("Sets", {}).get("Count", 0)
        env.assertEqual(sets_count, requests * 2, message="Sets count {} != {}".format(sets_count, requests * 2))
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


def test_monitor_cluster_line_data_first_arg_routed(env):
    """A bare __data__ as the first argument of a __monitor_line@__ line is
    the key cluster mode routes by, so it is filled in before routing: every
    value-named key must sit on the shard that owns it. Variable value sizes
    give keys x, xx, xxx, ... in many slots."""
    if not env.isCluster():
        env.skip()
        return
    if not _require_multi_shard(env):
        return

    _flush_cluster(env)
    test_dir = tempfile.mkdtemp()
    monitor_file = _write_monitor_file(
        test_dir, ['[ proxy ] 1764031576.604009 [0 127.0.0.1:5000] "SET" "__data__" "v"'])
    ok, run_config, _ = _run_monitor_cluster(
        env, monitor_file, "__monitor_line@__", threads=1, clients=2, requests=60,
        extra=["--data-size-range=1-40", "--data-size-pattern=R"], timeout=60)
    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="monitor-cluster __data__ key run did not complete")
        by_port = _all_keys_by_port(env)
        ports = set()
        for port, keys in by_port.items():
            for k in keys:
                env.assertEqual(set(k), {"x"}, message="unexpected key {!r}".format(k))
                env.assertEqual(_owning_port(env, k), port,
                                message="{} stored on {} but owned by {}".format(k, port, _owning_port(env, k)))
                ports.add(port)
        env.assertTrue(len(ports) >= 2, message="__data__ keys on only {} shard(s): {}".format(len(ports), by_port))
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


# ---------------------------------------------------------------------------
# 5. __monitor_line@__: failed-keys key and multi-key lines
# ---------------------------------------------------------------------------

def test_monitor_cluster_line_failed_keys_file(env):
    """--failed-keys-file records the routed key of a __monitor_line@__
    request (affixes included), not an empty key. request::m_key is also what
    --retry-on-error routes MOVED/ASK/-READONLY retries by, so it must be
    argument 1 (the text the router hashes), even when __key__ is only in a
    later argument: "INCRBY" "fixed" "x:__key__" logs "fixed"."""
    if not env.isCluster():
        env.skip()
        return

    _flush_cluster(env)
    test_dir = tempfile.mkdtemp()
    monitor_file = _write_monitor_file(
        test_dir, ['[ proxy ] 1764031576.604009 [0 127.0.0.1:5000] "INCRBY" "user:__key__" "x"',
                   '[ proxy ] 1764031576.604010 [0 127.0.0.1:5000] "INCRBY" "fixed" "x:__key__"'])
    failed_log = os.path.join(test_dir, "failed_keys.csv")

    requests = 20
    ok, run_config, js = _run_monitor_cluster(
        env, monitor_file, "__monitor_line@__", threads=1, clients=2, requests=requests,
        extra=["--key-prefix=mon-", "--key-maximum=200", "--failed-keys-file={}".format(failed_log)],
        timeout=60)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="monitor-cluster failed-keys run did not complete")
        with open(failed_log) as fh:
            rows = [r for r in csv.reader(fh) if r and r[0] != "timestamp"]
        env.assertEqual(len(rows), requests * 2, message="failed-keys rows: {}".format(rows[:5]))
        keys = [r[2] for r in rows]
        bad = [k for k in keys if not k.startswith("user:mon-") and k != "fixed"]
        env.assertEqual(bad, [], message="logged keys other than user:mon-* / fixed: {}".format(bad[:5]))
        # The default --monitor-pattern=S alternates the two lines.
        env.assertTrue("fixed" in keys, message="no row for the fixed-key line: {}".format(keys[:10]))
        env.assertTrue(any(k.startswith("user:mon-") for k in keys),
                       message="no user:mon-* rows: {}".format(keys[:10]))
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


def test_monitor_cluster_line_multi_key_needs_hash_tag(env):
    """Cluster mode supports only single-key commands, so a __monitor_line@__
    line with two __key__ arguments that can hash to different slots is
    rejected at startup, as --command and __monitor_lineN__ are. A line whose
    keys share one {hash tag} stays in one slot and runs."""
    if not env.isCluster():
        env.skip()
        return

    test_dir = tempfile.mkdtemp()
    untagged = _write_monitor_file(
        test_dir, ['[ proxy ] 1764031576.604009 [0 127.0.0.1:5000] "SET" "s:__key__" "v"',
                   '[ proxy ] 1764031576.604010 [0 127.0.0.1:5000] "MSET" "__key__" "v" "__key__" "v"'])
    _flush_cluster(env)
    ok, run_config, _ = _run_monitor_cluster(
        env, untagged, "__monitor_line@__", threads=1, clients=1, requests=10,
        extra=["--key-prefix=mon-"], timeout=60)
    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertFalse(ok, message="multi-key line without a hash tag was accepted")
        with open("{}/mb.stderr".format(run_config.results_dir)) as fh:
            err = fh.read()
        env.assertTrue("monitor line 2 has 2 __key__ arguments" in err, message=err[-500:])
        env.assertEqual(_dbsize_total_and_shards(env)[0], 0)
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)

    # Same rule as Redis: an empty {} is not a hash tag, and --key-prefix is
    # part of the key, so prefix "{" makes the tag of __key__{t} include the
    # generated number ({NNN{t}).
    for case, (line, prefix) in enumerate([
            ('"MSET" "{}a:__key__" "v" "{}b:__key__" "v"', "mon-"),
            ('"MSET" "__key__{t}" "v" "__key__{t}:b" "v"', "{"),
    ]):
        case_dir = os.path.join(test_dir, "reject{}".format(case))
        os.makedirs(case_dir)
        rejected = _write_monitor_file(case_dir, ['[ proxy ] 1764031576.604009 [0 127.0.0.1:5000] ' + line])
        ok, run_config, _ = _run_monitor_cluster(
            env, rejected, "__monitor_line@__", threads=1, clients=1, requests=10,
            extra=["--key-prefix={}".format(prefix)], timeout=60)
        failed = env.getNumberOfFailedAssertion()
        try:
            env.assertFalse(ok, message="accepted {} with --key-prefix={}".format(line, prefix))
            with open("{}/mb.stderr".format(run_config.results_dir)) as fh:
                err = fh.read()
            env.assertTrue("monitor line 1 has 2 __key__ arguments" in err, message=err[-500:])
        finally:
            if env.getNumberOfFailedAssertion() > failed:
                debugPrintMemtierOnError(run_config, env)

    os.makedirs(os.path.join(test_dir, "tagged"))
    tagged = _write_monitor_file(
        os.path.join(test_dir, "tagged"),
        ['[ proxy ] 1764031576.604009 [0 127.0.0.1:5000] "MSET" "{t}a:__key__" "__data__" "{t}b:__key__" "v"'])
    _flush_cluster(env)
    for conn in _master_conns(env):
        conn.execute_command("CONFIG", "RESETSTAT")
    ok, run_config, _ = _run_monitor_cluster(
        env, tagged, "__monitor_line@__", threads=1, clients=2, requests=20,
        extra=["--key-prefix=mon-", "--key-maximum=200", "--data-size=20"], timeout=60)
    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="tagged multi-key monitor run did not complete")
        keys = [k for keys in _all_keys_by_port(env).values() for k in keys]
        env.assertTrue(any(k.startswith("{t}a:mon-") for k in keys), message=str(keys[:10]))
        env.assertTrue(any(k.startswith("{t}b:mon-") for k in keys), message=str(keys[:10]))
        for conn in _master_conns(env):
            errs = conn.execute_command("INFO", "errorstats")
            env.assertEqual(errs, {}, message="server errors: {}".format(errs))
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)
