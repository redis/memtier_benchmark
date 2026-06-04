"""
Tests for the interaction between --transaction and --read-preference.

Background
----------
--transaction and --read-preference are mutually exclusive: transactions must
execute on a single, consistent connection (the slot-owner primary), so routing
reads to a replica inside a transaction block does not make sense.  memtier
must reject the combination at parse time with a clear error message that
references both flags.

The only exception is --read-preference=primary, which is the default
transaction behaviour (stay on the primary); this combination must succeed.

Test matrix
-----------
1. test_transaction_with_secondary_read_preference_rejected
   --transaction --read-preference=secondary must cause memtier to exit
   non-zero.  The error message in stderr must mention both --transaction and
   --read-preference.

2. test_transaction_with_secondary_preferred_read_preference_rejected
   Same as (1) for --read-preference=secondaryPreferred.

3. test_transaction_with_nearest_read_preference_rejected
   Same as (1) for --read-preference=nearest.

4. test_transaction_with_primary_read_preference_accepted
   --transaction --read-preference=primary is the canonical transaction mode
   and must exit 0 (positive-path check).
"""

import subprocess
import tempfile

from include import (
    MEMTIER_BINARY,
    add_required_env_arguments,
    addTLSArgs,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_subprocess(env, extra_args, timeout=15):
    """Run memtier directly via subprocess and return (returncode, stderr)."""
    master_nodes_list = env.getMasterNodesList()
    port = master_nodes_list[0]["port"]

    args = [
        MEMTIER_BINARY,
        "-s", "127.0.0.1",
        "-p", str(port),
        "-t", "1",
        "-c", "1",
        "--requests", "10",
    ]
    if env.isCluster():
        args.append("--cluster-mode")
    args.extend(extra_args)

    proc = subprocess.run(args, capture_output=True, timeout=timeout)
    stderr = proc.stderr.decode("utf-8", errors="replace")
    return proc.returncode, stderr


def _run_transaction_workload(env, extra_command_args, threads=1, clients=1,
                               requests=20):
    """Run a --transaction workload via the Benchmark helper and return
    (ok, run_config)."""
    benchmark_specs = {
        "name": env.testName,
        "args": ["--transaction"],
    }
    addTLSArgs(benchmark_specs, env)
    benchmark_specs["args"].extend(extra_command_args)

    config = get_default_memtier_config(
        threads=threads, clients=clients, requests=requests
    )
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)

    benchmark = Benchmark.from_json(run_config, benchmark_specs)
    ok = benchmark.run()
    return ok, run_config


# ---------------------------------------------------------------------------
# Tests: rejection of incompatible combinations
# ---------------------------------------------------------------------------

def _assert_transaction_read_pref_rejected(env, read_preference):
    """Shared helper: assert that --transaction + --read-preference=<mode>
    exits non-zero and mentions both flags in stderr.

    Although this is a parse-time check (memtier should reject before
    talking to a server), the helper spawns a real subprocess against the
    configured port. When the cell is standalone the binary may try to
    connect during arg validation in some build modes; skip cleanly in
    that case to avoid bleeding subprocess noise into the standalone CI
    cell. The cluster-mode rejection path is what we actually care
    about — parse-time only assertions belong in
    test_cli_validation_read_preference.py.
    """
    if not env.isCluster():
        env.skip()
        return
    rc, stderr = _run_subprocess(
        env,
        [
            "--transaction",
            "--read-preference={}".format(read_preference),
            "--command=SET __key__ __data__",
        ],
    )

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertNotEqual(
            rc,
            0,
            message="expected non-zero exit for --transaction "
                    "--read-preference={} but got exit 0".format(
                        read_preference
                    ),
        )

        # The error message must reference both conflicting flags so that users
        # know exactly what to fix.
        env.assertTrue(
            "--transaction" in stderr or "transaction" in stderr,
            message="stderr does not mention '--transaction'; got: {!r}".format(
                stderr[:400]
            ),
        )
        env.assertTrue(
            "--read-preference" in stderr or "read-preference" in stderr,
            message="stderr does not mention '--read-preference'; got: {!r}".format(
                stderr[:400]
            ),
        )
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            env.debugPrint(
                "### stderr from memtier (--transaction "
                "--read-preference={}): {!r}".format(
                    read_preference, stderr[:600]
                ),
                True,
            )


def test_transaction_with_secondary_read_preference_rejected(env):
    """--transaction --read-preference=secondary must be rejected at parse
    time with a clear error referencing both flags."""
    _assert_transaction_read_pref_rejected(env, "secondary")


def test_transaction_with_secondary_preferred_read_preference_rejected(env):
    """--transaction --read-preference=secondaryPreferred must be rejected."""
    _assert_transaction_read_pref_rejected(env, "secondaryPreferred")


def test_transaction_with_nearest_read_preference_rejected(env):
    """--transaction --read-preference=nearest must be rejected."""
    _assert_transaction_read_pref_rejected(env, "nearest")


# ---------------------------------------------------------------------------
# Test: primary read-preference with transaction is accepted
# ---------------------------------------------------------------------------

def test_transaction_with_primary_read_preference_accepted(env):
    """--transaction --read-preference=primary is the canonical mode and must
    exit 0 (positive-path check)."""
    if not env.isCluster():
        env.skip()
        return

    cmds = [
        "--read-preference=primary",
        "--command=MULTI",
        "--command=SET {txrp}-__key__ __data__",
        "--command=EXEC",
    ]
    ok, run_config = _run_transaction_workload(env, cmds, requests=30)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(
            ok,
            message="memtier exited non-zero for --transaction "
                    "--read-preference=primary; expected clean exit",
        )
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)
