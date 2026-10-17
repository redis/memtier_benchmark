"""
S2 -- Giant payloads + huge MONITOR lines.

Direct regression guard for PR #405 / issue #404. A user replay file with
multi-megabyte MONITOR lines crashed memtier_benchmark via
``*** stack smashing detected ***`` because
``arbitrary_command::split_command_to_args()`` used a VLA that overflowed
the worker thread's 8-12 MB stack.

We generate a fresh monitor file at test time (intentionally huge: not
checked in) containing:

  * one 20 MB SET value line
  * one 100 MB SET value line
  * a handful of 1 KB filler lines

Then we feed it via ``--monitor-input`` and ``--command=__monitor_line@__``.
If the binary survives (no SIGSEGV, no "stack smashing detected" in stderr,
clean exit, server STRLEN matches the blob we generated), the suite passes.
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from include import (  # noqa: E402
    addTLSArgs,
    add_required_env_arguments,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig  # noqa: E402


# Sizing knobs. CI runs the full-fat profile; local smoke can shrink them.
_BIG_BLOB_MB = int(os.environ.get("MEMTIER_SOAK_BIG_BLOB_MB", "100"))
_MID_BLOB_MB = int(os.environ.get("MEMTIER_SOAK_MID_BLOB_MB", "20"))
_FILLER_LINES = int(os.environ.get("MEMTIER_SOAK_FILLER_LINES", "10"))


def _make_monitor_line(key, value):
    # Mirror the redis-cli MONITOR capture format. The exact prefix is
    # not load-bearing -- memtier just has to tokenize past it.
    return '1764031576.604009 [0 127.0.0.1:51682] "SET" "{}" "{}"\n'.format(
        key, value
    )


def test_large_payloads_no_stack_smash(env):
    env.skipOnCluster()

    test_dir = tempfile.mkdtemp()
    monitor_file = os.path.join(test_dir, "big.txt")

    big_key = "soak_big_blob"
    mid_key = "soak_mid_blob"
    big_value_bytes = _BIG_BLOB_MB * 1024 * 1024
    mid_value_bytes = _MID_BLOB_MB * 1024 * 1024

    # Stream-write so we don't hold both blobs in Python memory at once.
    with open(monitor_file, "w") as f:
        # 20 MB line first, then 100 MB, then filler. Order is intentional
        # so the parser hits a non-trivial line before the largest one.
        f.write(_make_monitor_line(mid_key, "M" * mid_value_bytes))
        f.write(_make_monitor_line(big_key, "B" * big_value_bytes))
        for i in range(_FILLER_LINES):
            f.write(_make_monitor_line(
                "filler_{}".format(i), "F" * 1024
            ))

    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--monitor-input={}".format(monitor_file),
            "--command=__monitor_line@__",
            "--monitor-pattern=S",
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)

    # A small request count is plenty -- the regression triggers on PARSE,
    # not on volume.
    config = get_default_memtier_config(threads=1, clients=1, requests=20)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    memtier_ok = benchmark.run()

    if not memtier_ok:
        debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok)

    # Explicitly check stderr for the canonical stack-smash signature so
    # the test fails loudly instead of silently if exit code is forced to 0
    # by something exotic.
    stderr_path = os.path.join(config.results_dir, "mb.stderr")
    if os.path.isfile(stderr_path):
        with open(stderr_path) as f:
            stderr_content = f.read()
        env.assertFalse("stack smashing detected" in stderr_content)
        env.assertFalse("Segmentation fault" in stderr_content)

    # Server-side oracle: at least one of the giant blobs should have made
    # it through. STRLEN tolerates either ordering of the run.
    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    saw_blob = False
    for master_connection in master_nodes_connections:
        for key, expected in (
            (big_key, big_value_bytes),
            (mid_key, mid_value_bytes),
        ):
            try:
                length = master_connection.execute_command("STRLEN", key)
            except Exception:
                continue
            if length == expected:
                saw_blob = True
                break
        if saw_blob:
            break
    env.assertTrue(saw_blob)

    # Free the multi-GB monitor file before RLTest moves on so the runner
    # doesn't carry the artifact between tests.
    try:
        os.remove(monitor_file)
    except OSError:
        pass
