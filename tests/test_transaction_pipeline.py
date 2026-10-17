"""
Regression tests for --transaction with --pipeline > 1 in cluster mode.

Background: --transaction pins one full rotation of --command entries
(e.g. MULTI/.../EXEC) to a single shard connection. Earlier a hard guard
rejected --pipeline > 1 with any transaction-lifecycle command, on the
assumption that depth > 1 would interleave MULTI/EXEC blocks on the pin
connection. In practice the per-rotation pin holds for the whole rotation and
the command index advances strictly, so EXEC is always emitted before the next
MULTI: rotations stay contiguous on the wire at any pipeline depth, and
multiple whole transactions can be in flight without interleaving.

These tests run only against OSS-CLUSTER and assert:
1. No server-side transaction-breakage errors at pipeline > 1 (the symptoms
   that interleaving would produce: nested MULTI, EXEC without MULTI,
   EXECABORT, etc.).
2. Commit correctness is identical at pipeline=1 and pipeline=8 — a fixed-key
   counter incremented inside MULTI/EXEC reaches the exact same value
   regardless of pipeline depth (proves no lost/dropped/interleaved
   transactions).

Coverage note: these tests run against a STATIC cluster topology, so no MOVED/
ASK is emitted mid-run. The transaction-mode redirect handling (drop the
command without retrying it elsewhere, reset the pin only on the current pin)
is therefore not exercised here — triggering a slot migration at the exact
moment a rotation is mid-flight on the pin is inherently racy/flaky in this
harness. That path is validated by code review and by reasoning about the
accounting (every sent command is processed exactly once, so the run cannot
hang or mis-count); see cluster_client.cpp handle_response MOVED/ASK branches.

Run:
    TEST=test_transaction_pipeline.py OSS_CLUSTER=1 SHARDS=3 ./tests/run_tests.sh
"""

import os
import tempfile

from include import (
    add_required_env_arguments,
    addTLSArgs,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig


# Server-side error fragments that would appear if pipelining tore the
# MULTI/EXEC block apart (interleaved rotations / split transaction state).
TRANSACTION_BREAKAGE_PATTERNS = [
    "unwatch inside MULTI",
    "EXEC without MULTI",
    "MULTI calls can not be nested",
    "EXECABORT",
    "DISCARD without MULTI",
    "CROSSSLOT",
]


def _read_stderr(run_config):
    path = "{0}/mb.stderr".format(run_config.results_dir)
    if not os.path.isfile(path):
        return ""
    with open(path) as f:
        return f.read()


def _assert_no_transaction_breakage(env, stderr_text):
    for needle in TRANSACTION_BREAKAGE_PATTERNS:
        env.assertTrue(
            needle not in stderr_text,
            message="server-side transaction error '{}' present at pipeline > 1 — "
                    "rotations appear to have interleaved on the pin connection".format(needle),
        )


def _flush_cluster(env):
    for conn in env.getOSSMasterNodesConnectionList():
        conn.execute_command("FLUSHALL")


def _cluster_dbsize(env):
    """Total number of keys across all master shards."""
    total = 0
    for conn in env.getOSSMasterNodesConnectionList():
        try:
            total += int(conn.execute_command("DBSIZE"))
        except Exception:
            continue
    return total


def _get_from_cluster(env, key):
    """GET a key from whichever master owns its slot. Returns the decoded
    string value or None if unset/unreachable."""
    for conn in env.getOSSMasterNodesConnectionList():
        try:
            val = conn.execute_command("GET", key)
        except Exception:
            # non-owner shards reply MOVED; skip them
            continue
        if val is not None:
            return val.decode() if isinstance(val, bytes) else val
    return None


def _run_transaction(env, cmds, pipeline, threads=1, clients=1, requests=150,
                     extra_args=None):
    """Run a --transaction workload at the given pipeline depth; return
    (ok, run_config, stderr_text)."""
    benchmark_specs = {"name": env.testName, "args": ["--transaction", "--pipeline={}".format(pipeline)]}
    addTLSArgs(benchmark_specs, env)
    benchmark_specs["args"].extend(cmds)
    if extra_args:
        benchmark_specs["args"].extend(extra_args)

    config = get_default_memtier_config(threads=threads, clients=clients, requests=requests)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)

    benchmark = Benchmark.from_json(run_config, benchmark_specs)
    ok = benchmark.run()
    return ok, run_config, _read_stderr(run_config)


def test_transaction_pipelined_no_breakage(env):
    """WATCH/GET/MULTI/SET/EXEC/UNWATCH at pipeline=8 must complete with zero
    server-side transaction-breakage errors (no interleaving)."""
    if not env.isCluster():
        env.skip()
        return

    cmds = [
        '--command=WATCH {txp}-__key__',
        '--command=GET   {txp}-__key__',
        '--command=MULTI',
        '--command=SET   {txp}-__key__ __data__',
        '--command=EXEC',
        '--command=UNWATCH',
    ]
    _flush_cluster(env)
    ok, run_config, stderr = _run_transaction(env, cmds, pipeline=8, threads=2, clients=4, requests=600)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero at --pipeline=8")
        _assert_no_transaction_breakage(env, stderr)
        # Side-effect check: the SET inside MULTI/EXEC must actually commit data,
        # so a silently-dropped/interleaved transaction (no stderr error, wrong
        # data) cannot pass this test.
        env.assertTrue(_cluster_dbsize(env) > 0,
                       message="no keys committed — pipelined transactions may have been dropped")
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


def test_transaction_pipeline_commit_count_matches_baseline(env):
    """A fixed-key counter incremented inside MULTI/EXEC must reach the exact
    same committed value at pipeline=1 and pipeline=8. This proves pipelining
    neither drops nor interleaves transactions.

    The rotation is MULTI / INCR {txp}-__key__ / EXEC with the key range fixed
    to a single value (--key-minimum=--key-maximum=1), so every rotation
    increments the same key. INCR is the keyed command, so it drives the pin
    correctly. With --requests=150 and a 3-command rotation, exactly 50 full
    rotations run per client, so the counter == 50 * threads * clients."""
    if not env.isCluster():
        env.skip()
        return

    cmds = [
        '--command=MULTI',
        '--command=INCR {txp}-__key__',
        '--command=EXEC',
    ]
    # Empty --key-prefix so __key__ expands to the bare index "1" (default
    # prefix is "memtier-"), making the committed key deterministically "{txp}-1".
    key_args = ['--command-key-pattern=S', '--key-minimum=1', '--key-maximum=1', '--key-prefix=']
    threads, clients, requests = 1, 1, 150
    expected = (requests // 3) * threads * clients  # 50 full rotations -> 50 increments
    counter_key = "{txp}-1"

    results = {}
    for pipeline in (1, 8):
        _flush_cluster(env)
        ok, run_config, stderr = _run_transaction(
            env, cmds, pipeline=pipeline, threads=threads, clients=clients,
            requests=requests, extra_args=key_args)

        failed = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier exited non-zero at --pipeline={}".format(pipeline))
            _assert_no_transaction_breakage(env, stderr)
            val = _get_from_cluster(env, counter_key)
            env.assertTrue(val is not None,
                           message="counter {} not found after pipeline={} run".format(
                               counter_key, pipeline))
            results[pipeline] = int(val)
            env.assertEqual(
                results[pipeline], expected,
                message="pipeline={}: committed counter {} != expected {} "
                        "(transactions lost or mis-committed)".format(
                            pipeline, results[pipeline], expected))
        finally:
            if env.getNumberOfFailedAssertion() > failed:
                debugPrintMemtierOnError(run_config, env)

    # Baseline parity: pipeline depth must not change the committed result.
    env.assertEqual(
        results.get(1), results.get(8),
        message="commit count differs between pipeline=1 ({}) and pipeline=8 ({})".format(
            results.get(1), results.get(8)))


def test_transaction_pipelined_minimal_multi_exec(env):
    """Minimal MULTI/SET/INCR/EXEC (leading keyless MULTI, two keyed commands
    sharing a hash tag) at pipeline=4. The shared {mx}-counter is INCR'd once
    per committed rotation, so its final value must equal the exact number of
    full rotations — proving no transaction was dropped, duplicated, or split."""
    if not env.isCluster():
        env.skip()
        return

    cmds = [
        '--command=MULTI',
        '--command=SET   {mx}-__key__ __data__',
        '--command=INCR  {mx}-counter',
        '--command=EXEC',
    ]
    # 4-command rotation; total commands = threads*clients*requests, all divisible
    # by 4, so the counter == number of full rotations exactly.
    threads, clients, requests = 2, 4, 400
    expected = (threads * clients * requests) // 4  # 800 committed INCRs

    _flush_cluster(env)
    ok, run_config, stderr = _run_transaction(env, cmds, pipeline=4, threads=threads,
                                              clients=clients, requests=requests)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok)
        _assert_no_transaction_breakage(env, stderr)
        # Exact commit count: a dropped/duplicated/split transaction would move
        # this off `expected`.
        counter = _get_from_cluster(env, "{mx}-counter")
        env.assertTrue(counter is not None,
                       message="{mx}-counter not committed — pipelined MULTI/EXEC may have been dropped")
        env.assertEqual(int(counter), expected,
                        message="{{mx}}-counter={} != expected {} committed rotations".format(
                            counter, expected))
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)
