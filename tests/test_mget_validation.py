"""
Validation and regression tests for the --multi-key-get feature.

Covers four classes of checks:

1. CLI rejection tests — the binary must exit non-zero with a clear error
   message for invalid flag combinations.  These tests use subprocess
   directly and exercise the argument-parsing / pre-connect validation
   paths, so they do not need an active Redis connection.

2. keylist buffer-realloc regression — a key prefix long enough to push
   the first key past the initial keylist buffer forces a realloc inside
   keylist::add_key().  Before the fix (save/restore m_buffer_ptr offset),
   m_buffer_ptr became a dangling pointer after realloc and any subsequent
   write was undefined behaviour.  Running under ASAN catches this reliably.

3. Partial-ratio-cycle regression — when ratio.b is not a multiple of
   multi_key_get, the final MGET in a cycle carries fewer than
   multi_key_get keys.  Before the fix, m_get_ratio_count was advanced by
   the *configured* maximum rather than the *actual* keys sent, making the
   denominator of the ratio stale and distorting ops/sec accounting.

4. Narrow key-range regression — when key-maximum - key-minimum + 1 is
   smaller than multi_key_get, get_keys_count() returns fewer keys than
   the configured maximum; the benchmark must still complete cleanly.

Run with:
  TEST=test_mget_validation.py OSS_STANDALONE=1 ./tests/run_tests.sh
"""
import json
import os
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
# Internal helpers
# ---------------------------------------------------------------------------

def _run_memtier(args):
    """Run memtier_benchmark with *args* and return the CompletedProcess."""
    return subprocess.run(
        [MEMTIER_BINARY] + args,
        capture_output=True,
        text=True,
    )


def _build_benchmark(env, test_dir, extra_args, threads=1, clients=1,
                     requests=100):
    """Return (Benchmark, RunConfig) for an arbitrary workload."""
    config = get_default_memtier_config(threads=threads, clients=clients,
                                        requests=requests)
    benchmark_specs = {"name": env.testName, "args": list(extra_args)}
    addTLSArgs(benchmark_specs, env)
    add_required_env_arguments(benchmark_specs, config, env,
                               env.getMasterNodesList())
    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)
    return Benchmark.from_json(run_config, benchmark_specs), run_config


def _read_json(run_config):
    """Load and return the mb.json results dict."""
    json_filename = os.path.join(run_config.results_dir, "mb.json")
    with open(json_filename) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# 1. CLI rejection tests
# ---------------------------------------------------------------------------

def test_multi_key_get_zero_rejected(env):
    """--multi-key-get=0 must be rejected before connecting to any server."""
    env.skipOnCluster()

    master = env.getMasterNodesList()[0]
    result = _run_memtier([
        "-s", "127.0.0.1",
        "-p", str(master["port"]),
        "--multi-key-get=0",
        "-n", "1",
    ])

    env.assertNotEqual(
        result.returncode, 0,
        message="--multi-key-get=0 must exit non-zero",
    )
    env.assertTrue(
        "multi-key-get must be greater than zero" in result.stderr,
        message="Expected rejection message in stderr for --multi-key-get=0",
    )


def test_multi_key_get_memcache_binary_rejected(env):
    """--multi-key-get is not supported with memcache_binary (no wire-level implementation)."""
    env.skipOnCluster()

    master = env.getMasterNodesList()[0]
    result = _run_memtier([
        "-s", "127.0.0.1",
        "-p", str(master["port"]),
        "--protocol=memcache_binary",
        "--multi-key-get=5",
        "-n", "1",
    ])

    env.assertNotEqual(
        result.returncode, 0,
        message="--multi-key-get with memcache_binary must exit non-zero",
    )
    env.assertTrue(
        "not supported with memcache_binary" in result.stderr,
        message="Expected rejection message in stderr for memcache_binary",
    )


def test_multi_key_get_command_incompatible_rejected(env):
    """--multi-key-get cannot be combined with --command (MGET applies to the
    default GET slot; arbitrary commands replace the entire command set)."""
    env.skipOnCluster()

    master = env.getMasterNodesList()[0]
    result = _run_memtier([
        "-s", "127.0.0.1",
        "-p", str(master["port"]),
        "--command=GET __key__",
        "--multi-key-get=5",
        "-n", "1",
    ])

    env.assertNotEqual(
        result.returncode, 0,
        message="--multi-key-get with --command must exit non-zero",
    )
    env.assertTrue(
        "cannot be combined with --command" in result.stderr,
        message="Expected incompatibility message in stderr",
    )


# ---------------------------------------------------------------------------
# 2. keylist buffer-realloc regression
# ---------------------------------------------------------------------------

def test_multi_key_get_keylist_buffer_realloc(env):
    """
    A key prefix long enough to overflow the initial keylist buffer forces
    a realloc() inside keylist::add_key() while previously added keys are
    already stored in m_keys[].

    keylist buffer_size = 256 * (multi_key_get + 1).  With --multi-key-get=3
    that is 1024 bytes.  A 340-character prefix produces keys of ~342 bytes
    each.  After two keys (684 bytes) the third add_key() call overflows:
    684 + 342 = 1026 >= 1024 triggering realloc with m_keys_count == 2.

    Before the full fix, only m_buffer_ptr was updated after realloc.  The
    already-stored m_keys[0].key_ptr and m_keys[1].key_ptr still pointed into
    the freed old allocation.  Any subsequent read of those pointers when
    building the MGET wire command produced a use-after-free.  ASAN detects
    this reliably; without ASAN a crash or silently garbled keys may result.
    """
    env.skipOnCluster()

    test_dir = tempfile.mkdtemp()
    # 340-char prefix: each key is ~342 bytes; initial buffer (1024 bytes)
    # overflows on the third add_key() call when two keys are already stored.
    long_prefix = "A" * 340

    benchmark, run_config = _build_benchmark(
        env, test_dir,
        extra_args=[
            "--ratio=0:3",
            "--multi-key-get=3",
            "--key-prefix={}".format(long_prefix),
            "--key-minimum=1",
            "--key-maximum=100",
        ],
        threads=1,
        clients=1,
        requests=50,
    )

    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(
        memtier_ok,
        message="Benchmark must complete without crash when keylist buffer grows",
    )

    results = _read_json(run_config)
    gets = results.get("ALL STATS", {}).get("Gets", {})
    env.assertGreater(
        gets.get("Count", 0), 0,
        message="Expected at least one MGET result recorded in JSON",
    )


# ---------------------------------------------------------------------------
# 3. Partial-ratio-cycle regression
# ---------------------------------------------------------------------------

def test_multi_key_get_partial_ratio_cycle(env):
    """
    When ratio.b is not a multiple of multi_key_get, the final MGET in each
    ratio cycle carries fewer keys than the configured maximum.

    With ratio=1:5 multi-key-get=4: the first MGET carries 4 keys, the
    second carries 1 key (ratio.b - 4 = 1 remaining slot).  The m_get_ratio_
    count fix ensures the counter is advanced by the actual keys sent (1) not
    the configured max (4).  In both old and new code the cycle reset fires at
    the same request boundary (both reach >= ratio.b on the same call), so
    this combination does not produce a measurable difference in aggregated
    stats.  The test verifies the benchmark completes and both SETs and GETs
    are recorded, which would fail if the partial cycle caused an infinite loop
    or a crash.
    """
    env.skipOnCluster()

    test_dir = tempfile.mkdtemp()

    benchmark, run_config = _build_benchmark(
        env, test_dir,
        extra_args=[
            "--ratio=1:5",
            "--multi-key-get=4",
        ],
        threads=1,
        clients=1,
        requests=200,
    )

    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(
        memtier_ok,
        message="Benchmark must complete cleanly with partial ratio cycle",
    )

    results = _read_json(run_config)
    stats = results.get("ALL STATS", {})
    env.assertGreater(
        stats.get("Sets", {}).get("Count", 0), 0,
        message="Expected non-zero Set count with ratio=1:5",
    )
    env.assertGreater(
        stats.get("Gets", {}).get("Count", 0), 0,
        message="Expected non-zero Get count with ratio=1:5 multi-key-get=4",
    )


# ---------------------------------------------------------------------------
# 4. Narrow key-range regression
# ---------------------------------------------------------------------------

def test_multi_key_get_narrow_key_range(env):
    """
    Completion test: benchmark must not crash or hang when the key range
    (key-maximum - key-minimum + 1 = 3) is smaller than multi_key_get (10).

    With ratio=0:1 the ratio slot available per cycle is 1, so keys_count =
    min(1, 10) = 1 regardless of key range.  This means old and new code
    advance m_get_ratio_count by the same amount (1) and produce identical
    observable output, so this test does not distinguish the ratio-counter
    fix.  Its value is ensuring no assert, crash, or infinite loop occurs
    when the key range is much smaller than the configured multi_key_get.
    """
    env.skipOnCluster()

    test_dir = tempfile.mkdtemp()

    benchmark, run_config = _build_benchmark(
        env, test_dir,
        extra_args=[
            "--ratio=0:1",
            "--multi-key-get=10",
            "--key-minimum=1",
            "--key-maximum=3",
        ],
        threads=1,
        clients=1,
        requests=100,
    )

    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(
        memtier_ok,
        message="Benchmark must complete with key range narrower than multi-key-get",
    )

    results = _read_json(run_config)
    env.assertGreater(
        results.get("ALL STATS", {}).get("Gets", {}).get("Count", 0), 0,
        message="Expected non-zero Get count with narrow key range",
    )
