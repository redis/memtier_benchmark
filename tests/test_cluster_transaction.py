"""
Regression tests for issue #389 / `--transaction`.

Background: in `--cluster-mode`, memtier routes each `--command` by hashing
the first key argument, so the keyless transaction-lifecycle commands
(`MULTI`, `EXEC`, `UNWATCH`) end up on a different shard connection than the
keyed commands they wrap, breaking transaction state. The `--transaction`
flag pins one full rotation of `--command` entries to a single shard
connection (the slot owner of the first keyed command in the rotation) so
that a `WATCH`/`MULTI`/.../`EXEC` block stays together on one connection.

These tests run only against the OSS-CLUSTER environment; they assert that
memtier exits cleanly and that the Redis-side stderr never reports a
broken-transaction error (`unwatch inside MULTI`, `EXEC without MULTI`,
`MULTI calls can not be nested`, `EXECABORT`).
"""

import os
import re
import tempfile

from include import (
    add_required_env_arguments,
    addTLSArgs,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig

# Server-side error fragments that indicate the transaction state machine
# has been torn between two connections — the exact symptoms from #389.
TRANSACTION_BREAKAGE_PATTERNS = [
    "unwatch inside MULTI",
    "EXEC without MULTI",
    "MULTI calls can not be nested",
    "EXECABORT",
    "DISCARD without MULTI",
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
            message="server-side transaction error '{}' present — keyless "
                    "commands appear to have been routed to a different "
                    "shard connection than the keyed ones".format(needle),
        )


def _run_transaction_workload(env, extra_command_args, threads=2, clients=4,
                              requests=500):
    """Common helper: run a short --transaction workload and return
    (memtier_ok, run_config, stderr_text)."""
    benchmark_specs = {"name": env.testName, "args": ["--transaction"]}
    addTLSArgs(benchmark_specs, env)
    benchmark_specs["args"].extend(extra_command_args)

    config = get_default_memtier_config(threads=threads, clients=clients,
                                        requests=requests)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)

    benchmark = Benchmark.from_json(run_config, benchmark_specs)
    ok = benchmark.run()
    return ok, run_config, _read_stderr(run_config)


def test_transaction_watch_multi_exec_unwatch(env):
    """The exact failure mode from #389: WATCH/GET/MULTI/SET/EXEC/UNWATCH
    with hash-tagged keys. Must succeed end-to-end with zero server-side
    transaction errors."""
    if not env.isCluster():
        env.skip()
        return

    # Hash-tag forces every key to the same slot. With --transaction we also
    # pin all the keyless commands (MULTI/EXEC/UNWATCH) to that slot's shard.
    cmds = [
        '--command=WATCH {tx}-__key__',
        '--command=GET   {tx}-__key__',
        '--command=MULTI',
        '--command=SET   {tx}-__key__ __data__',
        '--command=EXEC',
        '--command=UNWATCH',
        '--command-key-pattern=R',
    ]
    ok, run_config, stderr = _run_transaction_workload(env, cmds)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")
        _assert_no_transaction_breakage(env, stderr)
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


def test_transaction_minimal_multi_exec(env):
    """Smaller surface: just MULTI / SET / EXEC. Validates the pin-on-first-
    keyed-command path with no preceding WATCH."""
    if not env.isCluster():
        env.skip()
        return

    cmds = [
        '--command=MULTI',
        '--command=SET   {mx}-__key__ __data__',
        '--command=INCR  {mx}-counter',
        '--command=EXEC',
        '--command-key-pattern=R',
    ]
    ok, run_config, stderr = _run_transaction_workload(env, cmds)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok)
        _assert_no_transaction_breakage(env, stderr)
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


def test_transaction_with_discard(env):
    """DISCARD is also a keyless transaction terminator and must follow the
    same pinning as EXEC."""
    if not env.isCluster():
        env.skip()
        return

    cmds = [
        '--command=MULTI',
        '--command=SET   {dx}-__key__ __data__',
        '--command=DISCARD',
        '--command-key-pattern=R',
    ]
    ok, run_config, stderr = _run_transaction_workload(env, cmds)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok)
        _assert_no_transaction_breakage(env, stderr)
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


def test_transaction_in_standalone_is_noop(env):
    """In standalone, --transaction is accepted but does nothing: each client
    already runs on a single connection, so the rotation order is naturally
    preserved. The benchmark must complete cleanly with no server-side
    transaction breakage."""
    if env.isCluster():
        env.skip()
        return

    # No hash tag needed here — there's only one shard.
    cmds = [
        '--command=MULTI',
        '--command=SET   __key__ __data__',
        '--command=INCR  counter',
        '--command=EXEC',
        '--command-key-pattern=R',
    ]
    ok, run_config, stderr = _run_transaction_workload(env, cmds)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")
        _assert_no_transaction_breakage(env, stderr)
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


def test_transaction_requires_command(env):
    """--transaction with no --command must be rejected (any env)."""
    import subprocess
    from include import MEMTIER_BINARY

    master_nodes_list = env.getMasterNodesList()
    port = master_nodes_list[0]["port"]
    args = [
        MEMTIER_BINARY,
        "-s", "127.0.0.1", "-p", str(port),
        "-t", "1", "-c", "1", "--requests", "1",
        "--transaction",
    ]
    if env.isCluster():
        args.append("--cluster-mode")
    if hasattr(env, "envRunner") and env.envRunner is not None:
        if getattr(env.envRunner, "useTLS", False):
            args.append("--tls")

    proc = subprocess.run(args, capture_output=True, timeout=15)
    env.assertNotEqual(
        proc.returncode, 0,
        message="--transaction without --command should fail validation")
    env.assertTrue(
        b"--transaction requires" in proc.stderr,
        message="expected stderr to mention requirement on --command; "
                "got: {!r}".format(proc.stderr[:400]))
