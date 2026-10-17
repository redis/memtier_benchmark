"""
Protocol-level correctness tests for MGET RESP wire format.

Verifies that redis_protocol::write_command_multi_get() emits correctly
structured RESP arrays by capturing commands via Redis MONITOR and inspecting
the parsed command entries.  Tests 5 and 6 use JSON output to verify that
the MGET array response parser in memtier works correctly.

All tests are standalone-only: MONITOR semantics and the absence of slot
restrictions make cluster mode unsuitable for this test suite.

Run with:
  TEST=test_mget_protocol.py OSS_STANDALONE=1 ./tests/run_tests.sh
"""

import json
import os
import tempfile
import threading
import time

import redis

from include import (
    TLS_CACERT,
    TLS_CERT,
    TLS_KEY,
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

def _get_redis_conn(env):
    """Return a redis.Redis connection that matches the test environment."""
    master_nodes_list = env.getMasterNodesList()
    if env.isUnixSocket():
        return redis.Redis(unix_socket_path=master_nodes_list[0]["unix_socket_path"])
    kwargs = {"host": "127.0.0.1", "port": master_nodes_list[0]["port"]}
    if getattr(env, "useTLS", False):
        # Skip server cert verification: test certs are self-signed and the CN
        # rarely matches "127.0.0.1", which would cause hostname-check failure.
        # The server still verifies the client cert via ssl_certfile/ssl_keyfile.
        kwargs["ssl"] = True
        kwargs["ssl_cert_reqs"] = "none"
        if TLS_CERT:
            kwargs["ssl_certfile"] = TLS_CERT
        if TLS_KEY:
            kwargs["ssl_keyfile"] = TLS_KEY
    return redis.Redis(**kwargs)


def _capture_monitor(conn, results, stop_event):
    """
    Thread target: call Monitor.next_command() in a loop until stop_event is
    set, appending each parsed command dict to *results*.

    next_command() blocks on the network read; we rely on the connection
    being closed (disconnect()) after stop_event is set to unblock the last
    pending read.
    """
    try:
        with conn.monitor() as m:
            while not stop_event.is_set():
                try:
                    cmd = m.next_command()
                    results.append(cmd)
                except Exception:
                    # Connection closed by _stop_monitor() or a transient read
                    # error; either way we are done.
                    break
    except Exception:
        pass


def _start_monitor(conn):
    """Start a background MONITOR thread.  Returns (thread, results, stop_event)."""
    results = []
    stop_event = threading.Event()
    t = threading.Thread(target=_capture_monitor,
                         args=(conn, results, stop_event),
                         daemon=True)
    t.start()
    # Give the MONITOR subscription time to be acknowledged by Redis before
    # memtier starts sending traffic.
    time.sleep(0.15)
    return t, results, stop_event


def _stop_monitor(conn, thread, stop_event, drain_secs=0.3):
    """Signal the monitor thread to stop and wait for it."""
    stop_event.set()
    # Allow queued commands to drain from the socket buffer.
    time.sleep(drain_secs)
    try:
        conn.connection_pool.disconnect()
    except Exception:
        pass
    thread.join(timeout=5)


def _run_mget_workload(env, extra_args, threads=1, clients=1, requests=10):
    """Build, run, and return (ok, run_config) for an MGET workload."""
    test_dir = tempfile.mkdtemp()
    config = get_default_memtier_config(threads=threads, clients=clients,
                                        requests=requests)
    benchmark_specs = {"name": env.testName, "args": list(extra_args)}
    addTLSArgs(benchmark_specs, env)
    add_required_env_arguments(benchmark_specs, config, env,
                               env.getMasterNodesList())
    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)
    benchmark = Benchmark.from_json(run_config, benchmark_specs)
    ok = benchmark.run()
    return ok, run_config


def _filter_mget(monitor_results):
    """Return only MONITOR entries whose first token is 'MGET'."""
    out = []
    for entry in monitor_results:
        cmd_str = entry.get("command", "")
        parts = cmd_str.split()
        if parts and parts[0].upper() == "MGET":
            out.append(parts)
    return out


def _preload_keys(env, key_min, key_max):
    """Populate keys [key_min, key_max] via a SET-only memtier pass."""
    test_dir = tempfile.mkdtemp()
    config = get_default_memtier_config(threads=1, clients=1,
                                        requests="allkeys")
    benchmark_specs = {
        "name": env.testName + "_preload",
        "args": [
            "--ratio=1:0",
            "--key-pattern=P:P",
            "--key-minimum={}".format(key_min),
            "--key-maximum={}".format(key_max),
        ],
    }
    addTLSArgs(benchmark_specs, env)
    add_required_env_arguments(benchmark_specs, config, env,
                               env.getMasterNodesList())
    run_config = RunConfig(test_dir, env.testName + "_preload", config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)
    benchmark = Benchmark.from_json(run_config, benchmark_specs)
    ok = benchmark.run()
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(ok, message="preload SET pass failed")


def _reset_commandstats(env):
    for conn in env.getOSSMasterNodesConnectionList():
        conn.execute_command("CONFIG", "RESETSTAT")


def _flushall(env):
    for conn in env.getOSSMasterNodesConnectionList():
        conn.execute_command("FLUSHALL")


def _read_json(run_config):
    json_filename = os.path.join(run_config.results_dir, "mb.json")
    with open(json_filename) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Test 1: array size == N keys for N=5
# ---------------------------------------------------------------------------

def test_mget_resp_array_size(env):
    """
    Each wire MGET must carry exactly 5 keys when --multi-key-get=5.

    Approach: MONITOR captures every command seen by Redis while memtier runs
    with -t 1 -c 1 -n 3 --ratio=0:5 --multi-key-get=5.  We parse the MONITOR
    output and assert:
      - Every captured MGET has exactly 5 key arguments.
      - Exactly 3 MGET entries were captured (one per logical request).
    """
    env.skipOnCluster()

    key_count = 5
    requests = 3

    conn = _get_redis_conn(env)
    t, results, stop_event = _start_monitor(conn)

    ok, run_config = _run_mget_workload(
        env,
        extra_args=[
            "--ratio=0:{}".format(key_count),
            "--multi-key-get={}".format(key_count),
        ],
        threads=1,
        clients=1,
        requests=requests,
    )

    _stop_monitor(conn, t, stop_event)
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(ok)

    mget_entries = _filter_mget(results)

    env.debugPrint(
        "Captured {} MGET entries (expected {})".format(len(mget_entries), requests),
        True,
    )

    env.assertEqual(
        len(mget_entries),
        requests,
        message="Expected exactly {} MGET wire commands, got {}".format(
            requests, len(mget_entries)
        ),
    )

    for i, parts in enumerate(mget_entries):
        # parts[0] == 'MGET', parts[1:] are the keys
        actual_keys = len(parts) - 1
        env.assertEqual(
            actual_keys,
            key_count,
            message="MGET #{}: expected {} key arguments, got {}. Command: {}".format(
                i, key_count, actual_keys, " ".join(parts)
            ),
        )


# ---------------------------------------------------------------------------
# Test 2: array size == 72 keys
# ---------------------------------------------------------------------------

def test_mget_resp_array_size_72(env):
    """
    Each wire MGET must carry exactly 72 keys when --multi-key-get=72.

    Uses -n 2 to keep the test short while still checking more than one
    command.
    """
    env.skipOnCluster()

    key_count = 72
    requests = 2

    conn = _get_redis_conn(env)
    t, results, stop_event = _start_monitor(conn)

    ok, run_config = _run_mget_workload(
        env,
        extra_args=[
            "--ratio=0:{}".format(key_count),
            "--multi-key-get={}".format(key_count),
        ],
        threads=1,
        clients=1,
        requests=requests,
    )

    _stop_monitor(conn, t, stop_event)
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(ok)

    mget_entries = _filter_mget(results)

    env.debugPrint(
        "Captured {} MGET entries (expected {})".format(len(mget_entries), requests),
        True,
    )

    env.assertEqual(
        len(mget_entries),
        requests,
        message="Expected exactly {} MGET wire commands, got {}".format(
            requests, len(mget_entries)
        ),
    )

    for i, parts in enumerate(mget_entries):
        actual_keys = len(parts) - 1
        env.assertEqual(
            actual_keys,
            key_count,
            message="MGET #{}: expected {} key arguments, got {}".format(
                i, key_count, actual_keys
            ),
        )


# ---------------------------------------------------------------------------
# Test 3: keys within one MGET are not all identical
# ---------------------------------------------------------------------------

def test_mget_key_uniqueness_in_command(env):
    """
    Keys within a single MGET should be independently generated.

    With a key range of 1–1,000,000 the probability of all 3 keys in any
    single MGET colliding is negligible.  We run 10 requests and assert that
    at least 9 of them contain more than one distinct key.
    """
    env.skipOnCluster()

    key_count = 3
    requests = 10
    # Threshold: allow at most 1 MGET with all-identical keys (statistical
    # noise), requiring at least requests-1 MGETs to have distinct keys.
    min_unique_mgets = requests - 1

    conn = _get_redis_conn(env)
    t, results, stop_event = _start_monitor(conn)

    ok, run_config = _run_mget_workload(
        env,
        extra_args=[
            "--ratio=0:{}".format(key_count),
            "--multi-key-get={}".format(key_count),
            "--key-minimum=1",
            "--key-maximum=1000000",
        ],
        threads=1,
        clients=1,
        requests=requests,
    )

    _stop_monitor(conn, t, stop_event)
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(ok)

    mget_entries = _filter_mget(results)
    env.assertEqual(len(mget_entries), requests)

    unique_mgets = 0
    for parts in mget_entries:
        keys = parts[1:]  # strip 'MGET'
        if len(set(keys)) > 1:
            unique_mgets += 1

    env.debugPrint(
        "{}/{} MGETs had more than one distinct key".format(unique_mgets, requests),
        True,
    )
    env.assertTrue(
        unique_mgets >= min_unique_mgets,
        message="Expected at least {}/{} MGETs with distinct keys, got {}".format(
            min_unique_mgets, requests, unique_mgets
        ),
    )


# ---------------------------------------------------------------------------
# Test 4: key prefix is reflected in MGET arguments
# ---------------------------------------------------------------------------

def test_mget_key_prefix(env):
    """
    Keys sent in MGET must use the configured prefix.

    Part A: default prefix 'memtier-' — all captured key arguments start with
            'memtier-'.
    Part B: explicit '--key-prefix=bench-' — all captured key arguments start
            with 'bench-'.
    """
    env.skipOnCluster()

    requests = 5
    key_count = 3

    for prefix, extra_prefix_args in [
        ("memtier-", []),
        ("bench-", ["--key-prefix=bench-"]),
    ]:
        conn = _get_redis_conn(env)
        t, results, stop_event = _start_monitor(conn)

        ok, run_config = _run_mget_workload(
            env,
            extra_args=[
                "--ratio=0:{}".format(key_count),
                "--multi-key-get={}".format(key_count),
            ] + extra_prefix_args,
            threads=1,
            clients=1,
            requests=requests,
        )

        _stop_monitor(conn, t, stop_event)
        debugPrintMemtierOnError(run_config, env)
        env.assertTrue(ok, message="benchmark failed for prefix='{}'".format(prefix))

        mget_entries = _filter_mget(results)
        env.assertEqual(
            len(mget_entries),
            requests,
            message="prefix='{}': expected {} MGETs, got {}".format(
                prefix, requests, len(mget_entries)
            ),
        )

        for i, parts in enumerate(mget_entries):
            for key in parts[1:]:
                env.assertTrue(
                    key.startswith(prefix),
                    message="prefix='{}': MGET #{} contains key '{}' that does not "
                    "start with '{}'".format(prefix, i, key, prefix),
                )


# ---------------------------------------------------------------------------
# Test 5: response array parsing (Hits/sec > 0, Misses/sec == 0)
# ---------------------------------------------------------------------------

def test_mget_response_array_parsing(env):
    """
    Verify that memtier correctly parses the MGET array response.

    Steps:
      1. Preload keys 1–5 via a SET-only memtier pass.
      2. Run a GET-only MGET pass over the same range with --key-pattern=S:S
         (sequential) so every key that is fetched exists in Redis.
      3. Assert Gets.Hits/sec > 0 and Gets.Misses/sec == 0 in JSON output.
    """
    env.skipOnCluster()

    key_min, key_max = 1, 5
    key_count = key_max - key_min + 1  # 5

    _flushall(env)
    _preload_keys(env, key_min, key_max)
    _reset_commandstats(env)

    ok, run_config = _run_mget_workload(
        env,
        extra_args=[
            "--ratio=0:{}".format(key_count),
            "--multi-key-get={}".format(key_count),
            "--key-pattern=S:S",
            "--key-minimum={}".format(key_min),
            "--key-maximum={}".format(key_max),
        ],
        threads=1,
        clients=1,
        requests=10,
    )
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(ok)

    results = _read_json(run_config)
    gets = results["ALL STATS"]["Gets"]

    env.assertGreater(
        gets.get("Hits/sec", 0),
        0,
        message="Expected Hits/sec > 0 when all keys were preloaded",
    )
    env.assertEqual(
        gets.get("Misses/sec", -1),
        0,
        message="Expected Misses/sec == 0 when all keys were preloaded",
    )


# ---------------------------------------------------------------------------
# Test 6: MGET hit rate consistent with GET hit rate for same keyspace
# ---------------------------------------------------------------------------

def test_mget_vs_get_same_keyspace(env):
    """
    MGET and GET over the same fully-populated keyspace should both yield
    100 % hit rates (Hits/sec > 0, Misses/sec == 0).

    Benchmark A: regular GET (--ratio=0:1), 200 requests.
    Benchmark B: MGET with 10 keys (--ratio=0:10 --multi-key-get=10),
                 20 requests (= 200 individual key fetches).
    """
    env.skipOnCluster()

    key_min, key_max = 1, 1000

    _flushall(env)
    _preload_keys(env, key_min, key_max)
    _reset_commandstats(env)

    # --- Benchmark A: regular GET ---
    ok_a, run_config_a = _run_mget_workload(
        env,
        extra_args=[
            "--ratio=0:1",
            "--key-pattern=R:R",
            "--key-minimum={}".format(key_min),
            "--key-maximum={}".format(key_max),
        ],
        threads=1,
        clients=1,
        requests=200,
    )
    debugPrintMemtierOnError(run_config_a, env)
    env.assertTrue(ok_a, message="Benchmark A (GET) failed")

    results_a = _read_json(run_config_a)
    gets_a = results_a["ALL STATS"]["Gets"]

    env.assertGreater(
        gets_a.get("Hits/sec", 0),
        0,
        message="Benchmark A: expected Hits/sec > 0",
    )
    env.assertEqual(
        gets_a.get("Misses/sec", -1),
        0,
        message="Benchmark A: expected Misses/sec == 0",
    )

    # --- Benchmark B: MGET with 10 keys ---
    _reset_commandstats(env)

    ok_b, run_config_b = _run_mget_workload(
        env,
        extra_args=[
            "--ratio=0:10",
            "--multi-key-get=10",
            "--key-pattern=R:R",
            "--key-minimum={}".format(key_min),
            "--key-maximum={}".format(key_max),
        ],
        threads=1,
        clients=1,
        requests=20,
    )
    debugPrintMemtierOnError(run_config_b, env)
    env.assertTrue(ok_b, message="Benchmark B (MGET) failed")

    results_b = _read_json(run_config_b)
    gets_b = results_b["ALL STATS"]["Gets"]

    env.assertGreater(
        gets_b.get("Hits/sec", 0),
        0,
        message="Benchmark B: expected Hits/sec > 0",
    )
    env.assertEqual(
        gets_b.get("Misses/sec", -1),
        0,
        message="Benchmark B: expected Misses/sec == 0",
    )


# ---------------------------------------------------------------------------
# Test 7: single-key MGET edge case
# ---------------------------------------------------------------------------

def test_mget_single_key(env):
    """
    The minimum valid MGET is 1 key.

    --multi-key-get=1 must still emit MGET (not GET) wire commands.
    We verify this via:
      (a) MONITOR: every captured command starts with 'MGET' and has exactly
          1 key argument.
      (b) commandstats: cmdstat_mget.calls == 10, cmdstat_get.calls == 0.
    """
    env.skipOnCluster()

    key_count = 1
    requests = 10

    _reset_commandstats(env)

    conn = _get_redis_conn(env)
    t, results, stop_event = _start_monitor(conn)

    ok, run_config = _run_mget_workload(
        env,
        extra_args=[
            "--ratio=0:1",
            "--multi-key-get=1",
        ],
        threads=1,
        clients=1,
        requests=requests,
    )

    _stop_monitor(conn, t, stop_event)
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(ok)

    # (a) MONITOR check
    mget_entries = _filter_mget(results)
    env.assertEqual(
        len(mget_entries),
        requests,
        message="Expected {} MGET wire commands, got {}".format(requests, len(mget_entries)),
    )
    for i, parts in enumerate(mget_entries):
        actual_keys = len(parts) - 1
        env.assertEqual(
            actual_keys,
            key_count,
            message="MGET #{}: expected 1 key argument, got {}".format(i, actual_keys),
        )

    # (b) commandstats check
    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    mget_calls = 0
    get_calls = 0
    for conn_cs in master_nodes_connections:
        stats = conn_cs.execute_command("INFO", "COMMANDSTATS")
        mget_calls += stats.get("cmdstat_mget", {}).get("calls", 0)
        get_calls += stats.get("cmdstat_get", {}).get("calls", 0)

    env.assertEqual(
        mget_calls,
        requests,
        message="cmdstat_mget.calls: expected {}, got {}".format(requests, mget_calls),
    )
    env.assertEqual(
        get_calls,
        0,
        message="cmdstat_get.calls: expected 0 (MGET should not fall back to GET), "
        "got {}".format(get_calls),
    )
