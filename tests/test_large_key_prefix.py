"""
Regression tests for keys whose prefix exceeds the legacy 250-byte
m_key_buffer cap (see JIRA RED-105966).

Before the fix:
  * object_generator::m_key_buffer was a fixed 250-byte stack array, and
    generate_key() called snprintf(buf, sizeof(buf)-1, "%s%llu", ...).
  * With a >=250-byte prefix, snprintf truncated the prefix and never
    appended the key index, so every iteration produced the same content
    and only one unique key was written to Redis (per the ticket).
  * snprintf still returned the un-truncated length, so memtier sent
    that many bytes from the 250-byte buffer, exposing adjacent heap
    bytes as part of the key.

These tests verify the fix by running memtier against a real Redis with
a large --key-prefix and asserting on the actual key set in the server.

  TEST=test_large_key_prefix.py OSS_STANDALONE=1 ./tests/run_tests.sh
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


def _run_set_workload(env, prefix, key_maximum=200, requests=400,
                      threads=1, clients=1):
    """Run a SET-only workload with the given prefix and return run_config."""
    test_dir = tempfile.mkdtemp()
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--ratio=1:0",
            "--key-prefix={}".format(prefix),
            "--key-minimum=1",
            "--key-maximum={}".format(key_maximum),
            "--key-pattern=P:P",
            "--data-size=8",
            "--pipeline=10",
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=threads, clients=clients,
                                        requests=requests)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)

    benchmark = Benchmark.from_json(run_config, benchmark_specs)
    ok = benchmark.run()
    if not ok:
        debugPrintMemtierOnError(run_config, env)
    env.assertTrue(ok)
    return run_config


def _assert_key_set_well_formed(env, conn, prefix, expected_unique):
    """All keys in the DB start with `prefix` and decode as prefix+<index>."""
    db_keys = conn.keys("*")
    env.assertEqual(len(db_keys), expected_unique,
                    message="expected {} keys, got {}".format(
                        expected_unique, len(db_keys)))

    prefix_bytes = prefix.encode() if isinstance(prefix, str) else prefix
    prefix_len = len(prefix_bytes)

    seen_indices = set()
    for k in db_keys:
        kb = k if isinstance(k, (bytes, bytearray)) else k.encode()
        # Every key must start with the exact, un-truncated prefix.
        env.assertEqual(kb[:prefix_len], prefix_bytes,
                        message="key does not start with full prefix; "
                                "len(key)={}, expected prefix length {}".format(
                                    len(kb), prefix_len))
        suffix = kb[prefix_len:]
        env.assertTrue(len(suffix) > 0 and suffix.isdigit(),
                       message="key suffix is not a decimal index: {!r}".format(
                           suffix[:32]))
        seen_indices.add(int(suffix))

    # We populate indices 1..key_maximum sequentially. Pattern P:P cycles
    # through the range, so all of them should be present.
    env.assertEqual(len(seen_indices), expected_unique,
                    message="expected {} distinct indices, got {}".format(
                        expected_unique, len(seen_indices)))


def test_prefix_just_above_legacy_cap(env):
    """251-byte prefix: one byte over the old 250-byte buffer."""
    env.skipOnCluster()
    env.flush()
    prefix = "a" * 251
    key_maximum = 200
    _run_set_workload(env, prefix, key_maximum=key_maximum, requests=800)
    conn = env.getConnection()
    _assert_key_set_well_formed(env, conn, prefix, expected_unique=key_maximum)


def test_prefix_1kib(env):
    """Reproduces the JIRA RED-105966 case (1003-byte prefix)."""
    env.skipOnCluster()
    env.flush()
    prefix = "x" * 1003
    key_maximum = 200
    _run_set_workload(env, prefix, key_maximum=key_maximum, requests=800)
    conn = env.getConnection()
    _assert_key_set_well_formed(env, conn, prefix, expected_unique=key_maximum)


def test_prefix_4kib(env):
    """4 KiB prefix exercises a larger realloc; Redis caps keys at 512 MiB."""
    env.skipOnCluster()
    env.flush()
    prefix = "z" * 4096
    key_maximum = 100
    _run_set_workload(env, prefix, key_maximum=key_maximum, requests=400)
    conn = env.getConnection()
    _assert_key_set_well_formed(env, conn, prefix, expected_unique=key_maximum)


def test_default_short_prefix_still_works(env):
    """Sanity: the common short-prefix path still produces the expected
    number of distinct memtier-N keys after the buffer became dynamic."""
    env.skipOnCluster()
    env.flush()
    # Default --key-prefix is "memtier-".
    prefix = "memtier-"
    key_maximum = 50
    _run_set_workload(env, prefix, key_maximum=key_maximum, requests=200)
    conn = env.getConnection()
    _assert_key_set_well_formed(env, conn, prefix, expected_unique=key_maximum)
