"""
Sanitizer stress tests (issue #411).

This file is intentionally NOT part of the default `tests/run_tests.sh`
test set: it is selected only when the workflow sets ``STRESS=1``
(see the STRESS matrix axis in ``.github/workflows/{asan,ubsan,tsan}.yml``).

Each test here is a deliberate stress scenario that the pre-#405 sanitizer
matrix never exercised:

  * ``test_huge_data_size_under_load`` -- 2 MiB ``--data-size`` under
    realistic concurrency. Targets large-value SET/GET path allocation
    accounting, which the default matrix runs at <=8 KiB.

  * ``test_huge_monitor_line_cluster`` -- the #404/#405 reproducer adapted
    to cluster mode. The original VLA stack-overflow regression test runs
    standalone-only; here we drive the same parser under
    ``--cluster-mode`` using hash-tagged keys so all routes land on a
    single shard while the monitor file itself contains multi-MiB lines.

  * ``test_huge_key_prefix_cluster_pipeline`` -- 16 KiB ``--key-prefix``
    + ``--pipeline=50`` on cluster, beyond the 4 KiB cap of the
    standalone-only ``test_large_key_prefix.py`` regression set.

  * ``test_reconnect_churn_long_run`` -- ``--reconnect-interval`` plus a
    long-ish ``--test-time`` (capped via env so total CI wall < 10 min)
    to make sure repeated socket teardown/restart under sanitizers does
    not leak FDs, libevent state or per-connection allocations.

  * ``test_debug_flag_paths`` -- runs a tiny workload with ``--debug``
    (a separate codepath that the default matrix never enters).

Tunable knobs (all default to CI-sized values):

  * ``MEMTIER_STRESS_BIG_BLOB_MB`` -- size of the largest monitor blob.
  * ``MEMTIER_STRESS_RECONNECT_TIME`` -- ``--test-time`` for the churn test.

Run locally:
  STRESS=1 OSS_STANDALONE=1 ./tests/run_tests.sh
  STRESS=1 OSS_CLUSTER=1   ./tests/run_tests.sh
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


# CI-sized defaults; local runs can shrink/grow via env.
_BIG_BLOB_MB = int(os.environ.get("MEMTIER_STRESS_BIG_BLOB_MB", "8"))
_RECONNECT_TEST_TIME = int(os.environ.get("MEMTIER_STRESS_RECONNECT_TIME", "30"))

# RLTest autodiscovers every ``tests/test_*.py``; without an in-test gate the
# stress scenarios run on every normal sanitizer cell too and blow the
# (raised, but still bounded) 45-minute job timeout. The dedicated STRESS
# matrix axis in ``.github/workflows/{asan,ubsan,tsan}.yml`` sets STRESS=1;
# elsewhere we no-op via ``env.skip()`` so the file is harmless to discover.
_STRESS_MODE = os.environ.get("STRESS", "0") == "1"


def _require_stress_mode(env):
    if not _STRESS_MODE:
        env.skip()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run(env, args, threads, clients, requests=None, test_time=None,
         test_dir=None, name_suffix=""):
    """Build a Benchmark from args+config and run it. Returns (ok, run_config)."""
    benchmark_specs = {
        "name": env.testName + name_suffix,
        "args": list(args),
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=threads, clients=clients,
                                        requests=requests, test_time=test_time)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    if test_dir is None:
        test_dir = tempfile.mkdtemp()
    run_config = RunConfig(test_dir, env.testName + name_suffix, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)

    benchmark = Benchmark.from_json(run_config, benchmark_specs)
    ok = benchmark.run()
    if not ok:
        debugPrintMemtierOnError(run_config, env)
    return ok, run_config


def _assert_clean_stderr(env, run_config):
    """No sanitizer/abort signatures in stderr."""
    stderr_path = os.path.join(run_config.results_dir, "mb.stderr")
    if not os.path.isfile(stderr_path):
        return
    with open(stderr_path) as f:
        content = f.read()
    # Canonical signatures that should NEVER appear under a clean sanitizer run.
    for bad in (
        "stack smashing detected",
        "Segmentation fault",
        "AddressSanitizer:",
        "LeakSanitizer:",
        "UndefinedBehaviorSanitizer:",
        # TSAN prints "WARNING: ThreadSanitizer:"; the sanitizer workflow
        # is marked continue-on-error and has a suppression file, so we
        # do not assert on TSAN here.
    ):
        if bad in content:
            env.debugPrint("STDERR contains '{}':\n{}".format(bad, content[:4000]), True)
        env.assertFalse(bad in content,
                        message="stderr contains '{}'".format(bad))


# ---------------------------------------------------------------------------
# 1. Large data values under load
# ---------------------------------------------------------------------------

def test_huge_data_size_under_load(env):
    """``--data-size=2 MiB`` with 8 threads x 16 clients.

    The default sanitizer matrix runs ``--data-size`` at most a few KiB.
    Multi-MiB values exercise the bulk write/read paths, per-request
    allocation accounting, and cluster fan-out buffering -- the same
    surface area implicated in the issue #411 gap matrix.
    """
    _require_stress_mode(env)
    # Cluster + standalone both supported (no monitor input here).
    args = [
        "--ratio=1:1",
        "--key-pattern=R:R",
        "--key-minimum=1",
        "--key-maximum=200",
        "--data-size=2097152",  # 2 MiB
        "--pipeline=2",
        "--hide-histogram",
    ]
    # Keep request count small: a 2 MiB value at 8x16=128 concurrent clients
    # is already ~256 MiB of in-flight traffic per pipeline depth.
    ok, run_config = _run(env, args, threads=8, clients=16, requests=50)
    env.assertTrue(ok, message="memtier did not complete the huge-data-size run")
    _assert_clean_stderr(env, run_config)


# ---------------------------------------------------------------------------
# 2. Huge monitor input lines on cluster (hash-tagged keys)
# ---------------------------------------------------------------------------

def test_huge_monitor_line_cluster(env):
    """Multi-MiB monitor lines fed to a cluster run.

    PR #405 fixed a VLA stack-overflow in ``split_command_to_args`` for
    long monitor lines, but the only regression test
    (``tests/soak/test_large_payloads.py``) is standalone-only. This test
    drives the same parser in ``--cluster-mode``: every monitor line
    targets keys with a shared hash tag ``{stress411}`` so they all
    resolve to one slot and the cluster routing layer does not have to
    fan out a giant blob across multiple shards.
    """
    _require_stress_mode(env)
    if not env.isCluster():
        # The standalone variant is already covered by
        # tests/soak/test_large_payloads.py; here we only assert the
        # cluster code path.
        env.skip()
        return

    test_dir = tempfile.mkdtemp()
    monitor_file = os.path.join(test_dir, "monitor_huge.txt")

    # Single shared hash tag -> single owning shard.
    big_key = "{stress411}:big"
    mid_key = "{stress411}:mid"

    big_value_bytes = _BIG_BLOB_MB * 1024 * 1024  # default 8 MiB
    mid_value_bytes = max(1, _BIG_BLOB_MB // 4) * 1024 * 1024  # 2 MiB

    # Stream-write so we don't hold both blobs in Python memory.
    line_fmt = '1764031576.604009 [0 127.0.0.1:51682] "SET" "{key}" "{val}"\n'
    with open(monitor_file, "w") as f:
        f.write(line_fmt.format(key=mid_key, val="M" * mid_value_bytes))
        f.write(line_fmt.format(key=big_key, val="B" * big_value_bytes))
        for i in range(5):
            f.write(line_fmt.format(
                key="{{stress411}}:fill_{}".format(i),
                val="F" * 1024,
            ))

    args = [
        "--monitor-input={}".format(monitor_file),
        "--command=__monitor_line@__",
        "--monitor-pattern=S",
        "--hide-histogram",
    ]
    # 1 thread / 1 client / few requests: regression is in PARSE, not volume.
    ok, run_config = _run(env, args, threads=1, clients=1, requests=10)

    try:
        env.assertTrue(ok, message="memtier did not complete huge-monitor cluster run")
        _assert_clean_stderr(env, run_config)

        # Server-side oracle: at least one giant blob made it through to
        # the owning shard. Cluster clients return MOVED from non-owners;
        # iterate to find the owner.
        saw_blob = False
        for conn in env.getOSSMasterNodesConnectionList():
            for key, expected in ((big_key, big_value_bytes),
                                  (mid_key, mid_value_bytes)):
                try:
                    length = conn.execute_command("STRLEN", key)
                except Exception:
                    continue
                if length == expected:
                    saw_blob = True
                    break
            if saw_blob:
                break
        env.assertTrue(saw_blob,
                       message="no giant SET landed on its owning shard")
    finally:
        try:
            os.remove(monitor_file)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# 3. Huge key prefix on cluster + pipelining
# ---------------------------------------------------------------------------

def test_huge_key_prefix_cluster_pipeline(env):
    """16 KiB ``--key-prefix`` + ``--pipeline=50`` (cluster + standalone).

    ``tests/test_large_key_prefix.py`` caps at 4 KiB and is
    standalone-only. The pipeline path stages multiple commands per
    connection-flush, so this test maximises the dynamic-buffer
    allocation churn introduced by PR #379.
    """
    _require_stress_mode(env)
    # Works on both standalone and cluster. Keys are not hash-tagged on
    # purpose: we want the cluster routing layer to distribute the giant
    # prefix across shards.
    prefix = "S" * 16384  # 16 KiB

    args = [
        "--ratio=1:0",
        "--key-prefix={}".format(prefix),
        "--key-minimum=1",
        "--key-maximum=200",
        "--key-pattern=P:P",
        "--data-size=8",
        "--pipeline=50",
        "--hide-histogram",
    ]
    ok, run_config = _run(env, args, threads=2, clients=2, requests=400)
    env.assertTrue(ok, message="memtier did not complete huge-key-prefix cluster pipeline run")
    _assert_clean_stderr(env, run_config)


# ---------------------------------------------------------------------------
# 4. Reconnect churn under a longer run
# ---------------------------------------------------------------------------

def test_reconnect_churn_long_run(env):
    """``--reconnect-interval`` over a longer-than-default ``--test-time``.

    The default sanitizer cells run for seconds with no reconnect churn;
    this test forces a reconnect every 10 requests for ~30s (overridable)
    so the close()/connect() codepaths see repeated traversals under
    ASAN/UBSan/TSAN.
    """
    _require_stress_mode(env)
    # --reconnect-interval is intentionally unsupported in cluster mode
    # (memtier exits with: "cluster mode dose not support reconnect-interval option").
    env.skipOnCluster()

    args = [
        "--reconnect-interval=10",
        "--ratio=1:1",
        "--key-pattern=R:R",
        "--key-minimum=1",
        "--key-maximum=10000",
        "--data-size=64",
        "--pipeline=1",
        "--hide-histogram",
    ]
    # Use --test-time, not --requests, so the duration is bounded.
    ok, run_config = _run(env, args, threads=4, clients=4, requests=None,
                          test_time=_RECONNECT_TEST_TIME)
    env.assertTrue(ok, message="memtier did not complete reconnect-churn run")
    _assert_clean_stderr(env, run_config)


# ---------------------------------------------------------------------------
# 5. --debug flag codepaths
# ---------------------------------------------------------------------------

def test_debug_flag_paths(env):
    """Tiny SET/GET workload with ``--debug``.

    ``--debug`` enables verbose logging codepaths that are otherwise
    untouched by CI -- a separate set of fprintf()/string-building paths
    which are precisely the family of latent bugs the issue #411 gap
    matrix calls out.
    """
    _require_stress_mode(env)
    args = [
        "--debug",
        "--ratio=1:1",
        "--key-pattern=R:R",
        "--key-minimum=1",
        "--key-maximum=1000",
        "--data-size=64",
        "--pipeline=1",
        "--hide-histogram",
    ]
    ok, run_config = _run(env, args, threads=2, clients=2, requests=200)
    env.assertTrue(ok, message="memtier did not complete --debug run")
    _assert_clean_stderr(env, run_config)
