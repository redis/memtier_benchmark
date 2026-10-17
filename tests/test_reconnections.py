import json
import math
import os
import re
import signal
import socket
import subprocess
import tempfile
import time
import threading
from include import *
from mb import Benchmark, RunConfig


def _add_auth_args(benchmark_specs, env):
    """--authenticate with the env's password (e.g. --oss_password), so
    memtier's clients get a session the kills below can target."""
    password = env.getOSSMasterNodesConnectionList()[0].connection_pool.connection_kwargs.get("password")
    if password:
        benchmark_specs["args"].append("--authenticate=" + password)


def _run_with_memtier_kills(env, benchmark, config, conns, waves, timeout=60):
    """Run memtier and kill its connections in `waves` waves.

    Each wave waits for memtier's clients to show up (the first connect, or
    the reconnect after the previous wave) and kills them straight away, so
    a short --requests run cannot end before the first wave. Only memtier's
    clients are killed and counted, never the harness's.

    Returns (exit code or None if memtier hung, kills per wave, stderr, ALL STATS
    totals from mb.json or {}).
    """
    harness = harness_client_ids(conns)
    stdout_path = "{0}/mb.stdout".format(config.results_dir)
    stderr_path = "{0}/mb.stderr".format(config.results_dir)
    kills = []
    with open(stdout_path, "w") as stdout_f, open(stderr_path, "w") as stderr_f:
        proc = subprocess.Popen(benchmark.args, stdout=stdout_f, stderr=stderr_f, cwd=config.results_dir)
        try:
            for _wave in range(waves):
                if not wait_for_memtier_clients(env, conns, harness, proc):
                    break
                kills.append(kill_memtier_clients(env, conns, harness))
                # Let the killed clients leave CLIENT LIST before looking for
                # the reconnected ones.
                time.sleep(0.05)
            try:
                rc = proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                rc = None
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=10)
    with open(stderr_path) as f:
        stderr_content = f.read()
    totals = {}
    json_path = "{0}/mb.json".format(config.results_dir)
    if os.path.exists(json_path):
        try:
            with open(json_path) as f:
                totals = json.load(f)["ALL STATS"]["Totals"]
        except (ValueError, KeyError):
            pass
    env.debugPrint("memtier exit code: {}, memtier clients killed per wave: {}, Connection Errors: {}".format(
        rc, kills, totals.get("Connection Errors")), True)
    return rc, kills, stderr_content, totals


def test_reconnect_on_connection_kill(env):
    """
    With --reconnect-on-error, memtier reconnects after its connections are
    killed and completes the run.

    Each wave kills memtier's connections as soon as they are up (the first
    connect, then each reconnect), so the run cannot end before the kills.
    """
    key_max = 10000
    key_min = 1

    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum={}".format(key_min),
            "--key-maximum={}".format(key_max),
            "--reconnect-on-error",
            "--max-reconnect-attempts=10",
            "--reconnect-backoff-factor=1.5",
            "--connection-timeout=5",
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    _add_auth_args(benchmark_specs, env)

    config = get_default_memtier_config(threads=2, clients=2, requests=20000)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    reset_commandstats(master_nodes_connections)

    rc, kills, stderr_content, totals = _run_with_memtier_kills(env, benchmark, config, master_nodes_connections,
                                                                waves=3)
    if rc != 0:
        env.debugPrint("STDERR:\n{}".format(stderr_content[-2000:]), True)

    env.assertGreaterEqual(sum(kills), 1, message="no memtier connection was killed")
    env.assertEqual(rc, 0, message="memtier exited {} after connection kills".format(rc))
    # memtier's own account of the kills and of the reconnects.
    env.assertGreaterEqual(stderr_content.count("attempting reconnection"), 1,
                           message="memtier logged no reconnection attempt")
    env.assertGreaterEqual(stderr_content.count("Reconnection successful"), 1,
                           message="memtier logged no successful reconnection")
    env.assertGreaterEqual(totals.get("Connection Errors", 0), 1, message="no connection error in mb.json")

    merged_command_stats = {
        "cmdstat_set": {"calls": 0},
        "cmdstat_get": {"calls": 0},
    }
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    env.assertTrue(overall_request_count > 0)


def test_reconnect_cluster_mode_no_assertion(env):
    """
    Regression test for https://github.com/redis/memtier_benchmark/issues/377

    When --cluster-mode and --reconnect-on-error are combined and a connection
    drops mid-run, cluster_client::connect() used to fire
    `assert(m_connections.size() == m_key_index_pools.size())` and SIGABRT the
    process. This test runs that exact combination, kills connections during
    the run, and verifies that:
      - memtier never aborts (no SIGABRT, no "Assertion" line in stderr)
      - the process exits with a normal status code (no signal death)
    """
    if not env.isCluster():
        env.skip()

    key_max = 100000
    key_min = 1

    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            "--ratio=1:0",
            "--key-pattern=P:P",
            "--key-minimum={}".format(key_min),
            "--key-maximum={}".format(key_max),
            "--data-size=512",
            "--random-data",
            "--reconnect-on-error",
            "--max-reconnect-attempts=20",
            "--reconnect-backoff-factor=1.0",
            "--connection-timeout=5",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    _add_auth_args(benchmark_specs, env)

    config = get_default_memtier_config(threads=4, clients=2, requests=None, test_time=10)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    harness = harness_client_ids(master_nodes_connections)

    import subprocess

    stdout_path = "{0}/mb.stdout".format(config.results_dir)
    stderr_path = "{0}/mb.stderr".format(config.results_dir)
    with open(stdout_path, "w") as stdout_f, open(stderr_path, "w") as stderr_f:
        proc = subprocess.Popen(
            benchmark.args,
            stdout=stdout_f,
            stderr=stderr_f,
            cwd=config.results_dir,
        )

        # Let connections fully establish before kicking them over.
        time.sleep(2)

        kill_count = 0
        # Two waves of CLIENT KILL to ensure the reconnect path is exercised
        # at least once per shard.
        # Only memtier's clients are killed and counted.
        for _wave in range(2):
            kill_count += kill_memtier_clients(env, master_nodes_connections, harness)
            time.sleep(2)

        # Generous timeout: --test-time=10 + reconnect backoff + summary flush.
        try:
            return_code = proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            # If memtier is wedged post-kill that is a separate concern;
            # the bug under test is the SIGABRT, so we kill the process and
            # still verify no assertion fired.
            proc.kill()
            try:
                return_code = proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                return_code = -9

    env.debugPrint("Total clients killed (CLIENT KILL count): {}".format(kill_count), True)
    env.debugPrint("memtier exit code: {}".format(return_code), True)

    with open(stderr_path) as f:
        stderr_content = f.read()

    # The core regression assertion: no abort from the parallel-vector invariant.
    has_assertion_abort = "Assertion `m_connections.size() == m_key_index_pools.size()'" in stderr_content
    if has_assertion_abort:
        env.debugPrint("STDERR:\n{}".format(stderr_content), True)
    env.assertFalse(has_assertion_abort)

    # We must have actually killed at least one connection or this test is meaningless.
    env.assertTrue(kill_count > 0)

    # SIGABRT == -6 (negative-signal form from Popen.wait()).
    # 134 (128+6) can also surface from shells; guard against both.
    env.assertNotEqual(return_code, -6)
    env.assertNotEqual(return_code, 134)


def test_reconnect_unlimited_no_spurious_thread_restart(env):
    """
    Regression test for https://github.com/redis/memtier_benchmark/issues/391.

    With --max-reconnect-attempts=0 (unlimited) and --reconnect-on-error,
    libevent can deliver a storm of connection-error callbacks per dead
    connection (EOF + stray read errors). Before the fix, every callback
    after the first (which set m_reconnecting=true) fell into the terminal
    else branch and called event_base_loopbreak(), killing the benchmark
    thread. This produced the misleading log line:

        Maximum reconnection attempts (0) exceeded for ..., ending this thread.

    …even though the user had explicitly requested unlimited reconnects.

    This test kills memtier's connections with --max-reconnect-attempts=0 and
    checks that it reconnects, never prints that message, and completes.

    It does not reproduce the #391 storm itself: CLIENT KILL delivers one
    error per connection here, plain or TLS, so the run passes with the
    m_reconnecting guard removed too. It covers the unlimited-reconnect path
    around that guard, not the duplicate callbacks.
    """
    key_max = 10000
    key_min = 1

    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum={}".format(key_min),
            "--key-maximum={}".format(key_max),
            "--reconnect-on-error",
            "--max-reconnect-attempts=0",   # unlimited
            "--reconnect-backoff-factor=1.0",
            "--connection-timeout=5",
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    _add_auth_args(benchmark_specs, env)

    config = get_default_memtier_config(threads=2, clients=2, requests=20000)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    master_nodes_connections = env.getOSSMasterNodesConnectionList()

    # Kill each connection again as soon as it is back, three times.
    rc, kills, stderr_content, totals = _run_with_memtier_kills(env, benchmark, config, master_nodes_connections,
                                                                waves=3)
    if rc != 0:
        env.debugPrint("STDERR:\n{}".format(stderr_content[-2000:]), True)

    env.assertGreaterEqual(sum(kills), 1, message="no memtier connection was killed")
    env.assertGreaterEqual(stderr_content.count("attempting reconnection"), 1,
                           message="memtier logged no reconnection attempt")
    env.assertGreaterEqual(totals.get("Connection Errors", 0), 1, message="no connection error in mb.json")
    # With unlimited reconnects, no kill may end a thread. (The duplicate
    # callbacks of #391 that used to print this are not produced here; see
    # the docstring.)
    env.assertFalse(
        "Maximum reconnection attempts (0) exceeded" in stderr_content,
        message="a thread ended under --max-reconnect-attempts=0 (unlimited)",
    )
    env.assertGreaterEqual(stderr_content.count("Reconnection successful"), 1,
                           message="memtier logged no successful reconnection")
    # Benchmark must have completed; if threads were killed spuriously it would
    # exit non-zero or stall.
    env.assertEqual(rc, 0)


def test_reconnect_disabled_by_default(env):
    """
    Reconnection is disabled by default: without --reconnect-on-error, a
    killed connection ends its thread and memtier exits non-zero.
    """
    key_max = 1000
    key_min = 1

    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum={}".format(key_min),
            "--key-maximum={}".format(key_max),
            "--hide-histogram",
            # Note: NO --reconnect-on-error flag
        ],
    }
    addTLSArgs(benchmark_specs, env)
    _add_auth_args(benchmark_specs, env)

    config = get_default_memtier_config(threads=1, clients=1, requests=100000)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    master_nodes_connections = env.getOSSMasterNodesConnectionList()

    rc, kills, stderr_content, totals = _run_with_memtier_kills(env, benchmark, config, master_nodes_connections,
                                                                waves=1, timeout=30)

    env.assertGreaterEqual(sum(kills), 1, message="no memtier connection was killed")
    env.assertTrue("--reconnect-on-error is not set" in stderr_content,
                   message="memtier did not end the thread of its killed connection: " + stderr_content[-2000:])
    env.assertGreaterEqual(totals.get("Connection Errors", 0), 1, message="no connection error in mb.json")
    env.assertTrue(rc is not None and rc != 0, message="memtier exited {} after its connection was killed".format(rc))


def _refusing_socket():
    """A bound socket that does not listen: connects to its port are refused.

    Holding it for the whole run keeps the port from being reused by another
    process, which closing a probe socket before the run would not.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    return sock


def _dead_server_args(env, port, json_path, extra_args):
    tls_specs = {"args": []}
    addTLSArgs(tls_specs, env)
    return [
        MEMTIER_BINARY,
        "--server=127.0.0.1",
        "--port={}".format(port),
        "--threads=1",
        "--clients=2",
        "--reconnect-on-error",
        "--connection-stage-timeout=0",
        "--connection-timeout=1",
        "--hide-histogram",
        "--json-out-file={}".format(json_path),
    ] + tls_specs["args"] + extra_args


def _logged_reconnect_errors(stderr):
    """Connection errors that attempt_reconnect() logged.

    Each call counts one connection error and logs either an "attempting
    reconnection" line or the final "Maximum reconnection attempts" line.
    Unlike the "Connection error: " line before it, which is "TLS connection
    error: " over TLS and missing when errno is 0, this is the same for every
    transport. A duplicate call while a reconnect is pending is counted but
    not logged, so the counter can be higher.
    """
    return len(
        re.findall(
            r"attempting reconnection \d+(?:/\d+| \(unlimited\))|Maximum reconnection attempts \(\d+\) exceeded",
            stderr,
        )
    )


def _assert_time_series_within(env, results, max_second):
    """Every "Time-Serie" second is within the run, none keyed from the epoch."""
    bad = []

    def walk(value, path):
        if isinstance(value, dict):
            for k, v in value.items():
                if k == "Time-Serie" and isinstance(v, dict):
                    bad.extend("{}/{}".format(path, s) for s in v if int(s) > max_second)
                else:
                    walk(v, path + "/" + k)

    walk(results, "")
    env.assertEqual(bad[:5], [])


def _assert_no_null_stats(env, results):
    """No NaN (written as null) or infinite value outside "configuration"."""
    bad = []

    def walk(value, path):
        if isinstance(value, dict):
            for k, v in value.items():
                walk(v, path + "/" + k)
        elif isinstance(value, list):
            for i, v in enumerate(value):
                walk(v, "{}/{}".format(path, i))
        elif value is None or (isinstance(value, float) and not math.isfinite(value)):
            bad.append(path)

    for section, value in results.items():
        if section != "configuration":
            walk(value, "/" + section)
    env.assertEqual(bad, [])


def _assert_zero_rates(env, stats):
    for cmd in ("Sets", "Gets", "Waits", "Totals"):
        for field in ("Ops/sec", "KB/sec", "KB/sec RX", "KB/sec TX"):
            env.assertEqual(stats[cmd][field], 0.0, message="{} {}".format(cmd, field))


def test_connection_errors_reported_when_server_never_reachable(env):
    """Clients that never connect keep their connection errors (issue #702).

    No connection ever succeeds, so no client sets a start time. The final
    report used to skip those clients and print Connection Errors 0, although
    the live line counted every failed connect. The run has no duration, so
    every rate must be 0, not NaN (JSON null).
    """
    env.skipOnCluster()
    test_dir = tempfile.mkdtemp()
    json_path = os.path.join(test_dir, "mb.json")
    refusing = _refusing_socket()
    try:
        result = subprocess.run(
            _dead_server_args(
                env, refusing.getsockname()[1], json_path, ["--max-reconnect-attempts=2", "--test-time=30"]
            ),
            capture_output=True,
            text=True,
            timeout=60,
        )
    finally:
        refusing.close()
    # One failed connect and two failed reconnects at least; the thread ends
    # when its first client runs out of attempts. Each of the 2 clients makes
    # at most 1 + 2 attempts.
    logged = _logged_reconnect_errors(result.stderr)
    env.assertGreaterEqual(logged, 3, message=result.stderr[-2000:])
    with open(json_path) as f:
        results = json.load(f)
    stats = results["ALL STATS"]
    env.assertGreaterEqual(stats["Totals"]["Connection Errors"], logged, message=repr(stats["Totals"]))
    env.assertLessEqual(stats["Totals"]["Connection Errors"], 2 * (1 + 2), message=repr(stats["Totals"]))
    _assert_zero_rates(env, stats)
    _assert_no_null_stats(env, results)
    _assert_time_series_within(env, results, 31)


def test_ctrl_c_reported_when_server_never_reachable(env):
    """A Ctrl+C before any client connects is reported (issue #702).

    The "after N secs" message used the client durations, which stay 0 until
    a client connects, and the JSON lost the interrupted flag together with
    the clients that never started.
    """
    env.skipOnCluster()
    test_dir = tempfile.mkdtemp()
    json_path = os.path.join(test_dir, "mb.json")
    refusing = _refusing_socket()
    proc = subprocess.Popen(
        _dead_server_args(env, refusing.getsockname()[1], json_path, ["--test-time=30"]),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        time.sleep(3)
        proc.send_signal(signal.SIGINT)
        try:
            _, stderr = proc.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            env.assertTrue(False, message="memtier did not exit after SIGINT")
            return
    finally:
        refusing.close()
    match = re.search(r"Interrupted by user \(Ctrl\+C\) after ([0-9.]+) secs", stderr)
    env.assertTrue(match is not None, message=stderr[-2000:])
    if match is None:
        return
    env.assertGreaterEqual(float(match.group(1)), 2.0, message=match.group(0))
    with open(json_path) as f:
        results = json.load(f)
    stats = results["ALL STATS"]
    env.assertEqual(stats["Runtime"]["Interrupted"], "true", message=repr(stats["Runtime"]))
    # Each logged attempt is counted once; a refused connect reports one
    # error, so at most one more per client may be counted without a log line.
    logged = _logged_reconnect_errors(stderr)
    env.assertGreaterEqual(logged, 2, message=stderr[-2000:])
    env.assertGreaterEqual(stats["Totals"]["Connection Errors"], logged, message=repr(stats["Totals"]))
    env.assertLessEqual(stats["Totals"]["Connection Errors"], logged + 2, message=repr(stats["Totals"]))
    _assert_zero_rates(env, stats)
    _assert_no_null_stats(env, results)
    _assert_time_series_within(env, results, 31)


def test_connection_errors_reported_when_only_a_later_client_connects(env):
    """A client that never connects, merged before one that did (issue #702).

    Client 0 is refused on every attempt; client 1, added by the staircase
    ramp after 3 s, connects through a proxy that accepts one connection.
    The final report must count client 0's connection errors, and its zero
    timestamps must not enter the merged run time. Client 1 uses the run's
    start time, and the thread ends when client 0's last dial fails at 6 s,
    so the run lasts about 6 s; averaging in client 0 would halve it.
    """
    env.skipOnCluster()
    upstream = env.getMasterNodesList()[0]
    if upstream.get("unix_socket_path"):
        env.skip()
    test_dir = tempfile.mkdtemp()
    json_path = os.path.join(test_dir, "mb.json")

    # Bound but not listening (connects are refused) until client 0's second
    # attempt has failed; it then accepts the next connection, client 1's,
    # forwards it to Redis and stops listening, so client 0 stays refused.
    proxy = _refusing_socket()
    port = proxy.getsockname()[1]
    args = _dead_server_args(
        env,
        port,
        json_path,
        [
            "--clients-start=1",
            "--clients-step=1",
            "--step-duration=3",
            "--reconnect-backoff-factor=2",
            "--max-reconnect-attempts=2",
            "--test-time=8",
        ],
    )
    # Delays are 2 s and 4 s: client 0 dials at 0 s, 2 s and 6 s, client 1 at
    # 3 s. Client 0's last dial fails at 6 s, before the 8 s deadline, so it
    # happens whether or not reconnects stop at the deadline.
    proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    stderr = bytearray()
    second_failure = threading.Event()

    def read_stderr():
        while True:
            chunk = proc.stderr.read1(4096)
            if not chunk:
                return
            stderr.extend(chunk)
            if b"attempting reconnection 2/2" in stderr:
                second_failure.set()

    reader = threading.Thread(target=read_stderr, daemon=True)
    reader.start()
    pumps = []
    try:
        env.assertTrue(second_failure.wait(20), message=stderr.decode(errors="replace")[-2000:])
        proxy.listen(1)
        proxy.settimeout(5)
        conn, _ = proxy.accept()
        proxy.close()
        up = socket.create_connection(("127.0.0.1", upstream["port"]))

        def pump(src, dst):
            try:
                while True:
                    data = src.recv(65536)
                    if not data:
                        break
                    dst.sendall(data)
            except OSError:
                pass
            finally:
                for s in (src, dst):
                    try:
                        s.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass

        for src, dst in ((conn, up), (up, conn)):
            t = threading.Thread(target=pump, args=(src, dst), daemon=True)
            t.start()
            pumps.append(t)
        proc.wait(timeout=60)
    finally:
        proxy.close()
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    reader.join(10)
    text = stderr.decode(errors="replace")
    # Client 0 makes at most 1 + 2 attempts, at least the 2 the proxy waited
    # for; client 1 keeps its connection. Compare with what memtier logged.
    logged = _logged_reconnect_errors(text)
    env.assertGreaterEqual(logged, 2, message=text[-2000:])
    with open(json_path) as f:
        results = json.load(f)
    stats = results["ALL STATS"]
    totals = stats["Totals"]
    env.assertGreaterEqual(totals["Connection Errors"], logged, message=repr(totals))
    env.assertLessEqual(totals["Connection Errors"], 1 + 2, message=repr(totals))
    env.assertGreater(totals["Count"], 0, message=repr(totals))
    duration_ms = stats["Runtime"]["Total duration"]
    env.assertGreaterEqual(duration_ms, 5000, message=repr(stats["Runtime"]))
    env.assertLessEqual(duration_ms, 12000, message=repr(stats["Runtime"]))
    env.assertAlmostEqual(totals["Ops/sec"], totals["Count"] * 1000.0 / duration_ms, delta=totals["Ops/sec"] * 0.05)
    _assert_no_null_stats(env, results)
    _assert_time_series_within(env, results, 8 + 1)


def test_reconnect_backoff_cap_60s(env):
    """
    Regression test for 2.4 review finding #13: unbounded exponential backoff.

    With --reconnect-backoff-factor=4.0 and --max-reconnect-attempts=0
    (unlimited), the delay would previously double unboundedly — after ~15
    attempts the scheduled reconnect is already >1 billion seconds away, and
    after ~30 it exceeds a 32-bit second counter entirely.  shard_connection.cpp
    now clamps the delay to MEMTIER_BACKOFF_CAP_SEC (60 s) after every
    multiplication.

    This test targets a closed port so every connect attempt fails.  It runs
    memtier for 20 s (--test-time=20) then lets it exit naturally.

    Behaviour note: memtier uses libevent non-blocking connects, so each
    call to connect() returns 0 (in-progress) even against a closed port;
    the ECONNREFUSED arrives later via the error callback which re-starts the
    backoff sequence from 1.0 s.  Because of this reset the scheduled delay
    stays at factor*1.0 = 4.0 s per cycle; a single 20 s window cannot drive
    the delay up to 60 s.  The cap is therefore validated by direct inspection
    of MEMTIER_BACKOFF_CAP_SEC in shard_connection.cpp rather than by an
    end-to-end log check.

    What this test CAN verify end-to-end:
      1. The process exits cleanly (signal-killed is acceptable; SIGABRT is not).
      2. At least two "attempting reconnection" log lines are present — the test
         is not vacuous (memtier actually ran and exercised the reconnect path).
      3. No logged backoff value in stderr exceeds 60 seconds.
    """
    import subprocess
    import re
    import socket
    from include import MEMTIER_BINARY

    # Find a closed port on localhost (bind + immediately close so the port
    # is guaranteed unused for the duration of the test).
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    closed_port = s.getsockname()[1]
    s.close()

    test_dir = tempfile.mkdtemp()

    cmd = [
        MEMTIER_BINARY,
        "--server=127.0.0.1",
        "--port={}".format(closed_port),
        "--protocol=redis",
        "--threads=1",
        "--clients=1",
        "--reconnect-on-error",
        "--reconnect-backoff-factor=4.0",
        "--max-reconnect-attempts=0",  # unlimited
        "--connection-timeout=1",
        "--test-time=20",
    ]

    stdout_path = "{}/mb.stdout".format(test_dir)
    stderr_path = "{}/mb.stderr".format(test_dir)

    with open(stdout_path, "w") as stdout_f, open(stderr_path, "w") as stderr_f:
        proc = subprocess.Popen(cmd, stdout=stdout_f, stderr=stderr_f, cwd=test_dir)

    try:
        return_code = proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            return_code = proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            return_code = proc.wait(timeout=5)

    env.debugPrint("memtier exit code: {}".format(return_code), True)

    # Process must not have crashed with SIGABRT (exit -6 or 134).
    env.assertNotEqual(return_code, -6)
    env.assertNotEqual(return_code, 134)

    with open(stderr_path) as f:
        stderr_content = f.read()

    env.debugPrint("STDERR (first 2000 chars):\n{}".format(stderr_content[:2000]), True)

    # Parse every "attempting reconnection … in X.XX seconds" line.
    # Pattern matches both the limited and unlimited variants logged by
    # shard_connection.cpp:
    #   "attempting reconnection 5 (unlimited) in 42.00 seconds..."
    #   "attempting reconnection 3/10 in 42.00 seconds..."
    # NOTE: the previous [^i]+ negated class could never match the "(unlimited)"
    # variant because "unlimited" contains 'i', causing the assertion to run
    # against an empty list (vacuous pass) under --max-reconnect-attempts=0.
    delay_pattern = re.compile(r"attempting reconnection\b.+?\bin\s+([\d.]+)\s+seconds")
    delays = [float(m.group(1)) for m in delay_pattern.finditer(stderr_content)]

    env.debugPrint("Observed backoff delays (s): {}".format(delays), True)

    # The test ran for 20 s; with a 4 s reconnect cycle at least 2 reconnect
    # attempts must have been logged (proves memtier stayed alive and tried).
    env.assertGreaterEqual(
        len(delays),
        2,
        message=(
            "Expected at least 2 reconnect delay log lines, got {}. "
            "memtier may have exited immediately (bad option or crash) rather "
            "than actually exercising the reconnect path.".format(len(delays))
        ),
    )
    # RLTest assertions are soft: assertGreaterEqual records a failure but does
    # not halt execution.  Guard here so that max(delays) is never reached on
    # an empty list (which would raise ValueError and mask the real failure).
    if not delays:
        return

    max_observed = max(delays)
    env.debugPrint("Max observed backoff: {:.2f}s".format(max_observed), True)

    # Cap must not be exceeded.
    env.assertLessEqual(
        max_observed,
        60.0,
        message="Backoff exceeded 60 s cap: {:.2f} s observed".format(max_observed),
    )


def _run_with_mid_run_client_kill(env, extra_args, test_time, kill_after):
    """Kill active workload clients once and retain their output and server counts."""
    import subprocess

    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            "--rate-limiting=200",
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=10000",
            "--hide-histogram",
        ] + extra_args,
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=2, clients=2, requests=None, test_time=test_time)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    conn = master_nodes_connections[0]  # Both callers skip cluster mode.
    initial_client_ids = {client["id"] for client in conn.client_list()}

    def command_count():
        stats = conn.info("commandstats")
        return sum(stats.get("cmdstat_" + command, {}).get("calls", 0) for command in ("get", "set"))

    initial_commands = command_count()

    stderr_path = "{0}/mb.stderr".format(config.results_dir)
    started = time.monotonic()
    with open("{0}/mb.stdout".format(config.results_dir), "w") as stdout_f, open(stderr_path, "w") as stderr_f:
        proc = subprocess.Popen(benchmark.args, stdout=stdout_f, stderr=stderr_f, cwd=config.results_dir)
        try:
            # Confirm all four clients have issued workload commands before
            # injecting the disconnect, including on slower TLS/sanitizer cells.
            ready_deadline = started + test_time / 2
            while True:
                workload_clients = [client for client in conn.client_list()
                                    if client["id"] not in initial_client_ids and client["cmd"] in ("get", "set")]
                if len(workload_clients) == 4:
                    break
                if proc.poll() is not None or time.monotonic() >= ready_deadline:
                    raise AssertionError("four active workload clients were not ready before the kill")
                time.sleep(0.02)
            time.sleep(max(0, started + kill_after - time.monotonic()))
            commands_before_kill = command_count() - initial_commands
            killed = conn.execute_command("CLIENT", "KILL", "TYPE", "normal")
            env.assertGreaterEqual(killed, 4, message="the disconnect must reach all four workload clients")
            env.assertGreater(commands_before_kill, 0, message="no workload ran before the disconnect")
            return_code = proc.wait(timeout=6 * test_time)
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=10)
    elapsed = time.monotonic() - started
    server_commands = command_count() - initial_commands

    with open(stderr_path) as f:
        stderr_content = f.read()
    with open("{0}/mb.json".format(config.results_dir)) as f:
        results = json.load(f)["ALL STATS"]
    env.debugPrint("memtier exit code: {}, elapsed {:.1f}s".format(return_code, elapsed), True)
    env.debugPrint("STDERR:\n{}".format(stderr_content[:2000]), True)
    env.assertGreater(results["Totals"]["Count"], 0)
    return return_code, elapsed, stderr_content, results, server_commands


def test_connection_kill_without_reconnect_fails_on_time(env):
    """
    Without --reconnect-on-error, connections killed mid-run must END the run
    as a failure. They must not cause the worker threads to be silently
    rebuilt: a rebuilt thread started a fresh --test-time window with fresh
    stats, so the run took (kill time + a full --test-time), up to five times
    over, while reporting the requested duration; it discarded the ops recorded
    before the kill; and it exited 0.
    """
    env.skipOnCluster()
    test_time, kill_after = 8, 2
    rc, elapsed, stderr_content, results, _ = _run_with_mid_run_client_kill(env, [], test_time, kill_after)

    env.assertEqual(rc, 1, message="a run whose connections were killed must report an incomplete run")
    env.assertFalse("Restarting thread" in stderr_content, message="a worker thread was silently restarted")
    env.assertTrue("client(s) stopped before reaching their stop condition" in stderr_content,
                   message="the failure was not reported")
    env.assertTrue("the results of this run are not valid" in stderr_content)
    # A restart re-runs the full window, so the run would take at least
    # kill_after + test_time. Ending on the failure takes about kill_after.
    env.assertLess(elapsed, test_time, message="run took {:.1f}s for --test-time={}".format(elapsed, test_time))
    env.assertLess(results["Runtime"]["Total duration"], test_time * 1000)


def test_connection_kill_with_reconnect_is_not_rerun(env):
    """
    With --reconnect-on-error, connections killed mid-run are recovered and the
    run ends at --test-time with exit 0. The recovered connection errors must
    not cause the worker threads to be re-run afterwards: that discarded the
    whole completed window, reported a second one in its place, and doubled
    the wall time.
    """
    env.skipOnCluster()
    test_time, kill_after = 8, 2
    rc, elapsed, stderr_content, results, server_commands = _run_with_mid_run_client_kill(
        env, ["--reconnect-on-error", "--max-reconnect-attempts=10"], test_time, kill_after
    )

    env.assertEqual(rc, 0)
    env.assertFalse("Restarting thread" in stderr_content, message="a worker thread was silently restarted")
    env.assertGreaterEqual(elapsed, test_time, message="recovery ended before the requested window")
    # JSON truncates timestamps to milliseconds; allow one millisecond of
    # rounding while requiring the complete original measurement window.
    env.assertGreaterEqual(results["Runtime"]["Total duration"], test_time * 1000 - 1)
    # With pipeline=1, at most one response per client can be lost at the
    # forced disconnect and one at final teardown. Match Redis's command
    # count so silently dropping the pre-outage stats cannot pass.
    reported_commands = results["Totals"]["Count"]
    env.assertGreaterEqual(reported_commands, server_commands - 8,
                           message="JSON discarded completed operations across recovery")
    env.assertLessEqual(reported_commands, server_commands)
    # A re-run takes about 2 * test_time; an honest run takes test_time plus
    # process start-up and teardown.
    env.assertLess(
        elapsed, test_time + 4, message="run took {:.1f}s for --test-time={}".format(elapsed, test_time)
    )


def test_dropped_in_flight_requests_do_not_stall_client(env):
    """
    Requests in flight on a killed connection must not stall the client after
    it reconnects.

    A killed connection discards its in-flight requests, which had already
    been counted as generated. Left counted, they kept generated ahead of
    processed for good, so with --reconnect-interval the client held its
    pipeline at the next reconnect point, its worker's event loop ran out of
    events, and the run failed with "client(s) stopped before reaching their
    stop condition".
    """
    import subprocess

    # Cluster mode rejects --reconnect-interval, and its in-flight budget is
    # covered by the same fix, but a separate pre-existing end-of-run hang
    # with reconnects in flight would make a cluster variant flaky.
    env.skipOnCluster()
    test_time = 8
    args = [
        "--pipeline=1",
        "--ratio=1:1",
        "--key-pattern=R:R",
        "--key-minimum=1",
        "--key-maximum=10000",
        "--reconnect-interval=10",
        "--reconnect-on-error",
        "--hide-histogram",
    ]
    benchmark_specs = {"name": env.testName, "args": args}
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=1, clients=1, requests=None, test_time=test_time)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    conn = env.getOSSMasterNodesConnectionList()[0]

    stderr_path = "{0}/mb.stderr".format(config.results_dir)
    kills = []
    with open("{0}/mb.stdout".format(config.results_dir), "w") as stdout_f, open(stderr_path, "w") as stderr_f:
        proc = subprocess.Popen(benchmark.args, stdout=stdout_f, stderr=stderr_f, cwd=config.results_dir)
        try:
            # The client is never rate limited, so most kills land with a
            # request in flight; one discarded request is enough. A kill can
            # still miss (e.g. a socket a planned reconnect just closed), so
            # use several waves, spaced beyond the default 1s reconnect delay.
            for _wave in range(4):
                time.sleep(1.5)
                try:
                    kills.append(conn.execute_command("CLIENT", "KILL", "TYPE", "normal"))
                except Exception as e:
                    env.debugPrint("CLIENT KILL failed: {}".format(e), True)
            rc = proc.wait(timeout=test_time + 30)
        finally:
            if proc.poll() is None:
                proc.kill()
            rc = proc.wait(timeout=10)

    with open(stderr_path) as f:
        stderr_content = f.read()
    env.debugPrint("memtier exit code: {}, CLIENT KILL counts: {}".format(rc, kills), True)
    if rc != 0:
        env.debugPrint("STDERR:\n{}".format(stderr_content[-2000:]), True)

    env.assertGreaterEqual(sum(kills), 1, message="no CLIENT KILL wave found a memtier connection")
    env.assertGreaterEqual(stderr_content.count("Reconnection successful"), 1,
                           message="no connection was killed and recovered")
    env.assertFalse("stopped before reaching their stop condition" in stderr_content,
                    message="client stopped sending after a reconnect")
    env.assertEqual(rc, 0, message="memtier exited {} after connection kills".format(rc))


def test_cluster_run_exits_with_reconnects_pending(env):
    """
    A cluster run must exit when connections are killed near --test-time.

    A dropped shard connection's reconnect reconnected the main connection
    instead of itself, and a CLUSTER SLOTS refresh could reconnect a shard
    connection whose own reconnect was still pending. Either way a live
    connection was connected again: its in-flight requests stayed counted as
    pending on a fresh socket that never sent them, so it never became idle
    and memtier never exited (#558).
    """
    import subprocess

    if not env.isCluster():
        env.skip()
        return
    # Over TLS a separate, rarer stall can still hang this scenario (#562).
    if env.useTLS:
        env.skip()
        return
    test_time = 3
    rounds = 10
    masters = env.getOSSMasterNodesConnectionList()
    for i in range(rounds):
        args = [
            "--pipeline=4",
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=10000",
            "--reconnect-on-error",
            "--hide-histogram",
        ]
        benchmark_specs = {"name": "{}_{}".format(env.testName, i), "args": args}
        addTLSArgs(benchmark_specs, env)
        config = get_default_memtier_config(threads=2, clients=2, requests=None, test_time=test_time)
        add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
        test_dir = tempfile.mkdtemp()
        config = RunConfig(test_dir, benchmark_specs["name"], config, {})
        ensure_clean_benchmark_folder(config.results_dir)
        benchmark = Benchmark.from_json(config, benchmark_specs)

        stderr_path = "{0}/mb.stderr".format(config.results_dir)
        hung = False
        with open("{0}/mb.stdout".format(config.results_dir), "w") as stdout_f, open(stderr_path, "w") as stderr_f:
            proc = subprocess.Popen(benchmark.args, stdout=stdout_f, stderr=stderr_f, cwd=config.results_dir)
            try:
                # Kill around the deadline, varying the offset per round, so
                # connections drop and reconnect (1s delay) while others still
                # have requests in flight.
                start = time.time()
                for offset in (-0.9, -0.5, -0.2, 0.1):
                    delay = start + test_time + offset + 0.1 * (i % 3) - time.time()
                    if delay > 0:
                        time.sleep(delay)
                    for m in masters:
                        try:
                            m.execute_command("CLIENT", "KILL", "TYPE", "normal")
                        except Exception:
                            pass
                try:
                    proc.wait(timeout=test_time + 20)
                except subprocess.TimeoutExpired:
                    hung = True
            finally:
                if proc.poll() is None:
                    proc.kill()
                proc.wait(timeout=10)
        if hung:
            with open(stderr_path) as f:
                env.debugPrint("round {} STDERR tail:\n{}".format(i, f.read()[-3000:]), True)
            env.assertFalse(hung, message="memtier did not exit after --test-time (round {})".format(i))
            break


def test_cluster_shard_dial_failure_honors_max_reconnect_attempts(env):
    """
    A shard connection whose dial fails before reaching the event loop must
    still be retried with backoff and count against --max-reconnect-attempts.

    One primary announces 224.0.0.1, so connecting to it fails synchronously
    (ENETUNREACH) inside the CLUSTER SLOTS handling. No connection event ever
    reported that failure and no reconnect timer was armed, so every slot map
    refresh redialed the node at once, ignoring the backoff and
    --max-reconnect-attempts (#570).
    """
    import subprocess

    if not env.isCluster():
        env.skip()
        return
    masters = env.getOSSMasterNodesConnectionList()
    master_nodes = env.getMasterNodesList()
    seed_port = int(master_nodes[0]['port'])
    victim = None
    for m in masters:
        if int(m.connection_pool.connection_kwargs['port']) != seed_port:
            victim = m
            break
    env.assertTrue(victim is not None, message="need a primary other than the seed")
    seed = [m for m in masters if m is not victim][0]
    unreachable = "224.0.0.1"
    max_attempts = 2

    def wait_announced(present):
        deadline = time.time() + 30
        while time.time() < deadline:
            if (unreachable in str(seed.execute_command("CLUSTER", "SLOTS"))) == present:
                return True
            time.sleep(0.2)
        return False

    victim.execute_command("CONFIG", "SET", "cluster-announce-ip", unreachable)
    try:
        env.assertTrue(wait_announced(True), message="announced address did not propagate")
        benchmark_specs = {
            "name": env.testName,
            "args": [
                "--ratio=1:0",
                "--key-pattern=R:R",
                "--key-minimum=1",
                "--key-maximum=10000",
                "--reconnect-on-error",
                "--max-reconnect-attempts={}".format(max_attempts),
                "--hide-histogram",
            ],
        }
        addTLSArgs(benchmark_specs, env)
        config = get_default_memtier_config(threads=1, clients=1, requests=None, test_time=10)
        add_required_env_arguments(benchmark_specs, config, env, master_nodes)
        test_dir = tempfile.mkdtemp()
        config = RunConfig(test_dir, env.testName, config, {})
        ensure_clean_benchmark_folder(config.results_dir)
        benchmark = Benchmark.from_json(config, benchmark_specs)
        stderr_path = "{0}/mb.stderr".format(config.results_dir)
        with open("{0}/mb.stdout".format(config.results_dir), "w") as stdout_f, open(stderr_path, "w") as stderr_f:
            proc = subprocess.run(benchmark.args, stdout=stdout_f, stderr=stderr_f, cwd=config.results_dir,
                                  timeout=60)
        with open(stderr_path) as f:
            stderr_content = f.read()
    finally:
        victim.execute_command("CONFIG", "SET", "cluster-announce-ip", "")
        # Fail here, not in a later cluster test, if the topology stays poisoned.
        env.assertTrue(wait_announced(False), message="announced address {} was not reverted".format(unreachable))

    # Every synchronous dial failure goes through
    # attempt_reconnect("Connection failed"), whichever step failed
    # (getaddrinfo, setup_socket() or bufferevent_socket_connect()), so count
    # those lines rather than only the "connect failed" ones.
    sync_failures = len(re.findall(r"^Connection failed, attempting reconnection", stderr_content, re.M))
    exhausted = "Maximum reconnection attempts ({}) exceeded for Connection failed".format(max_attempts)
    if exhausted in stderr_content:
        sync_failures += 1
    failed_dials = stderr_content.count("connect failed")
    if (sync_failures > max_attempts + 1 or failed_dials > max_attempts + 1 or exhausted not in stderr_content):
        env.debugPrint("STDERR tail:\n{}".format(stderr_content[-3000:]), True)
    # One initial dial plus one per reconnect attempt.
    env.assertTrue(sync_failures <= max_attempts + 1,
                   message="unreachable shard failed to dial {} times with --max-reconnect-attempts={}".format(
                       sync_failures, max_attempts))
    env.assertTrue(failed_dials <= max_attempts + 1,
                   message="unreachable shard dialed {} times with --max-reconnect-attempts={}".format(
                       failed_dials, max_attempts))
    env.assertTrue(exhausted in stderr_content,
                   message="--max-reconnect-attempts was not enforced for the unreachable shard")
    env.assertNotEqual(proc.returncode, 0)


def _clear_requirepass(conn, passwords):
    """Remove requirepass so later tests on this server can connect. The pool
    may open a new connection, which then has to authenticate first."""
    import redis

    try:
        conn.execute_command("CONFIG", "SET", "requirepass", "")
        return
    except redis.exceptions.AuthenticationError:
        pass
    kwargs = dict(conn.connection_pool.connection_kwargs)
    for password in passwords:
        kwargs["password"] = password
        client = redis.Redis(connection_class=conn.connection_pool.connection_class, **kwargs)
        try:
            client.execute_command("CONFIG", "SET", "requirepass", "")
            return
        except redis.exceptions.AuthenticationError:
            continue
        finally:
            client.close()
    raise AssertionError("could not clear requirepass")


def _run_with_mid_run_password_change(env, extra_args, test_time, rate_limit=True):
    """Run with AUTH, then change the password and drop the connections, so
    every reconnect gets -WRONGPASS. Return (rc, elapsed, stderr); rc is None
    if memtier had to be killed."""
    import subprocess

    conn = env.getOSSMasterNodesConnectionList()[0]  # Callers skip cluster mode.
    benchmark_specs = {
        "name": env.testName,
        "args": ["--authenticate=pw-625", "--reconnect-on-error", "--hide-histogram"]
        + (["--rate-limiting=200"] if rate_limit else [])
        + extra_args,
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=2, requests=None, test_time=test_time)
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)

    stderr_path = "{0}/mb.stderr".format(config.results_dir)
    rc = None
    # Set the password only now: add_required_env_arguments() opens new
    # connections under TLS, which would fail to authenticate.
    conn.execute_command("CONFIG", "SET", "requirepass", "pw-625")
    started = time.monotonic()
    try:
        with open("{0}/mb.stdout".format(config.results_dir), "w") as stdout_f, open(stderr_path, "w") as stderr_f:
            proc = subprocess.Popen(benchmark.args, stdout=stdout_f, stderr=stderr_f, cwd=config.results_dir)
            try:
                time.sleep(2)
                conn.execute_command("CONFIG", "SET", "requirepass", "other-625")
                # SKIPME defaults to yes, so this connection survives.
                killed = conn.execute_command("CLIENT", "KILL", "TYPE", "normal")
                env.assertGreaterEqual(killed, 2, message="the kill must reach both workload clients")
                rc = proc.wait(timeout=test_time + 15)
            except subprocess.TimeoutExpired:
                pass
            finally:
                if proc.poll() is None:
                    proc.kill()
                proc.wait(timeout=10)
    finally:
        _clear_requirepass(conn, ("other-625", "pw-625"))
    elapsed = time.monotonic() - started
    with open(stderr_path) as f:
        stderr_content = f.read()
    env.debugPrint("memtier exit code: {}, elapsed {:.1f}s".format(rc, elapsed), True)
    env.debugPrint("STDERR:\n{}".format(stderr_content[-2000:]), True)
    env.assertContains("authentication failed", stderr_content)
    # The reconnects connect at TCP level but never complete setup.
    env.assertFalse("Reconnection successful" in stderr_content,
                    message="reconnects whose AUTH failed were reported as successful")
    return rc, elapsed, stderr_content


def test_auth_failure_on_reconnect_honors_max_reconnect_attempts(env):
    """
    An AUTH error on a reconnect (the password changed mid-run) must count as
    a failed reconnect attempt. It used to leave the connection open with
    setup unfinished: no request was sent again, so neither
    --max-reconnect-attempts nor --test-time ended the run, and memtier hung
    after logging "Reconnection successful" (#625).
    """
    env.skipOnCluster()
    test_time = 30
    rc, elapsed, stderr_content = _run_with_mid_run_password_change(
        env, ["--max-reconnect-attempts=3"], test_time
    )
    env.assertIsNotNone(rc, message="memtier hung after AUTH failed on reconnect")
    env.assertNotEqual(rc, 0, message="a run that cannot re-authenticate must fail")
    env.assertTrue("Maximum reconnection attempts (3) exceeded" in stderr_content,
                   message="--max-reconnect-attempts was not enforced for AUTH errors")
    # 2 s before the kill, then 3 attempts at the default 1 s backoff.
    env.assertLess(elapsed, test_time / 2, message="run took {:.1f}s".format(elapsed))


def test_auth_failure_on_reconnect_honors_test_time(env):
    """
    With unlimited reconnect attempts, AUTH errors on every reconnect must not
    keep the run going past --test-time (#625).
    """
    env.skipOnCluster()
    test_time = 8
    rc, elapsed, _ = _run_with_mid_run_password_change(env, [], test_time)
    env.assertIsNotNone(rc, message="memtier hung after AUTH failed on reconnect")
    env.assertNotEqual(rc, 0, message="a run that cannot re-authenticate must fail")
    env.assertLess(elapsed, test_time + 5, message="run took {:.1f}s for --test-time={}".format(elapsed, test_time))


def test_auth_failure_on_reconnect_with_retry_on_error_honors_test_time(env):
    """
    Same as above with --retry-on-error: the requests in flight at the kill
    are held for replay until setup succeeds, which it never does. They must
    not keep the run reconnecting past --test-time (#625). No rate limit, so
    the full pipeline is in flight at the kill.
    """
    env.skipOnCluster()
    test_time = 8
    rc, elapsed, stderr_content = _run_with_mid_run_password_change(
        env, ["--retry-on-error", "--pipeline=16"], test_time, rate_limit=False
    )
    env.assertIsNotNone(rc, message="memtier hung after AUTH failed on reconnect")
    env.assertNotEqual(rc, 0, message="a run that cannot re-authenticate must fail")
    env.assertTrue("run is over, not reconnecting" in stderr_content,
                   message="the run did not stop reconnecting at --test-time")
    # The requests held for replay are accounted, not dropped silently.
    env.assertTrue("held for retry when the run ended were not resent" in stderr_content,
                   message="requests held for replay were not counted when the run ended")
    env.assertLess(elapsed, test_time + 5, message="run took {:.1f}s for --test-time={}".format(elapsed, test_time))


def _sanitized():
    return any(os.environ.get(k) for k in ("ASAN_OPTIONS", "TSAN_OPTIONS", "UBSAN_OPTIONS"))


# Extra seconds for wall-clock bounds under sanitizers, which slow memtier down.
_SLACK = 5 if _sanitized() else 0


class _StoppableProxy:
    """Proxy to the test server over TCP, or over a unix socket when
    upstream_unix is set. stop() closes the listener and every proxied
    connection (and removes the unix socket), so the server looks gone for
    good: reconnects are refused (TCP) or fail before reaching the event loop
    (unix socket). stop(keep_accepting=True) instead keeps accepting and
    closes each new connection at once, like a proxy with a dead backend.
    With forward_first_only, only the first connection is proxied: later ones
    are accepted but never answered. freeze() holds the replies on the current
    connections; drop() then closes them and forwards new ones normally."""

    def __init__(self, upstream_port=None, upstream_unix=None, forward_first_only=False):
        import socket

        self._upstream_unix = upstream_unix
        self._upstream_port = upstream_port
        self.unix_path = None
        if upstream_unix:
            self.unix_path = os.path.join(tempfile.mkdtemp(), "proxy.sock")
            self._srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._srv.bind(self.unix_path)
        else:
            self._srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._srv.bind(("127.0.0.1", 0))
            self.port = self._srv.getsockname()[1]
        self._srv.listen(64)
        self._socks = []
        self._forward_first_only = forward_first_only
        self._forwarded = 0
        self._stopped = False
        self._keep_accepting = False
        self._frozen = False
        self._lock = threading.Lock()
        threading.Thread(target=self._accept, daemon=True).start()

    def _connect_upstream(self):
        import socket

        if self._upstream_unix:
            upstream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            # A relative path dodges the 108-byte sun_path limit under deep
            # working directories.
            path = min(self._upstream_unix, os.path.relpath(self._upstream_unix), key=len)
            upstream.connect(path)
            return upstream
        return socket.create_connection(("127.0.0.1", self._upstream_port))

    def _accept(self):
        while True:
            try:
                client, _ = self._srv.accept()
            except OSError:
                return
            with self._lock:
                if self._stopped:
                    client.close()
                    if self._keep_accepting:
                        continue
                    return
                if self._forward_first_only and self._forwarded:
                    self._socks.append(client)
                    continue
                self._forwarded += 1
                upstream = self._connect_upstream()
                self._socks += [client, upstream]
            for src, dst, replies in ((client, upstream, False), (upstream, client, True)):
                threading.Thread(target=self._pump, args=(src, dst, replies), daemon=True).start()

    def _pump(self, src, dst, replies):
        try:
            while True:
                data = src.recv(65536)
                if not data:
                    break
                while replies and self._frozen and dst in self._socks:
                    time.sleep(0.01)
                dst.sendall(data)
        except OSError:
            pass

    def freeze(self):
        self._frozen = True

    def restart(self):
        """Listen again on the same address after stop(): the server is back."""
        import socket

        with self._lock:
            if self.unix_path:
                srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                srv.bind(self.unix_path)
            else:
                srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                srv.bind(("127.0.0.1", self.port))
            srv.listen(64)
            self._srv = srv
            self._stopped = False
            self._keep_accepting = False
        threading.Thread(target=self._accept, daemon=True).start()

    def drop(self):
        import socket

        with self._lock:
            socks, self._socks = self._socks, []
            self._frozen = False
            for s in socks:
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                s.close()

    def stop(self, keep_accepting=False, blackhole=False):
        """With blackhole, the port then gets a listener whose accept queue is
        full, so new connects hang (TCP only)."""
        import socket

        with self._lock:
            for s in getattr(self, "_blackhole", []):
                s.close()
            self._blackhole = []
            self._stopped = True
            self._keep_accepting = keep_accepting
            if not keep_accepting:
                try:
                    # Wakes the accept() blocked in _accept(); close() alone does not.
                    self._srv.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                self._srv.close()
                if self.unix_path and os.path.exists(self.unix_path):
                    os.unlink(self.unix_path)
            socks, self._socks = self._socks, []
            for s in socks:
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                s.close()
            if blackhole and not self.unix_path:
                # Linux queues backlog + 1 connections and then drops the
                # SYNs of new ones: one filler makes every later connect hang.
                srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                srv.bind(("127.0.0.1", self.port))
                srv.listen(0)
                filler = socket.create_connection(("127.0.0.1", self.port))
                self._blackhole = [filler, srv]


def _run_until_server_gone(env, extra_args, test_time, requests=None, stop_after=2, wait=None, threads=1, clients=2,
                           forward_first_only=False, accept_after_stop=False, script=None, before_start=None):
    """Run through a proxy that is stopped after stop_after seconds, or before
    memtier starts when stop_after is None. With accept_after_stop, the
    stopped proxy accepts and closes every new connection. script, a list of
    (seconds, function(proxy)), replaces stop_after. Return (rc, elapsed, stderr,
    stdout, json); rc is None if memtier had to be killed, json is None if
    no JSON report was written."""
    import subprocess

    benchmark_specs = {
        "name": env.testName,
        "args": ["--reconnect-on-error", "--rate-limiting=200", "--hide-histogram"] + extra_args,
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=threads, clients=clients, requests=requests, test_time=test_time)
    node = dict(env.getMasterNodesList()[0])
    if env.isUnixSocket():
        proxy = _StoppableProxy(upstream_unix=node["unix_socket_path"], forward_first_only=forward_first_only)
        node["unix_socket_path"] = proxy.unix_path
    else:
        proxy = _StoppableProxy(upstream_port=int(node["port"]), forward_first_only=forward_first_only)
        node["port"] = proxy.port
    add_required_env_arguments(benchmark_specs, config, env, [node])
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)

    stderr_path = "{0}/mb.stderr".format(config.results_dir)
    stdout_path = "{0}/mb.stdout".format(config.results_dir)
    rc = None
    if stop_after is None:
        proxy.stop()
    if before_start is not None:
        before_start()
    started = time.monotonic()
    with open(stdout_path, "w") as stdout_f, open(stderr_path, "w") as stderr_f:
        proc = subprocess.Popen(benchmark.args, stdout=stdout_f, stderr=stderr_f, cwd=config.results_dir)
        try:
            if script is not None:
                for at, action in script:
                    time.sleep(max(0, at - (time.monotonic() - started)))
                    action(proxy)
            elif stop_after is not None:
                time.sleep(stop_after)
                proxy.stop(keep_accepting=accept_after_stop)
            rc = proc.wait(timeout=wait if wait is not None else test_time + 15)
        except subprocess.TimeoutExpired:
            pass
        finally:
            proxy.stop()
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=10)
    elapsed = time.monotonic() - started
    with open(stderr_path) as f:
        stderr_content = f.read()
    with open(stdout_path) as f:
        stdout_content = f.read()
    json_path = "{0}/mb.json".format(config.results_dir)
    results = None
    if os.path.isfile(json_path):
        with open(json_path) as f:
            try:
                results = json.load(f)
            except ValueError:
                pass
    env.debugPrint("memtier exit code: {}, elapsed {:.1f}s".format(rc, elapsed), True)
    env.debugPrint("STDERR:\n{}".format(stderr_content[-2000:]), True)
    return rc, elapsed, stderr_content, stdout_content, results


def test_server_gone_unlimited_reconnects_honor_test_time(env):
    """
    With --reconnect-on-error and unlimited --max-reconnect-attempts, a server
    that goes away for good must not keep a --test-time run going: it used to
    retry once a second forever, never printing the report (#656). The run now
    stops reconnecting at the deadline and ends with its report. Under a unix
    socket the reconnects fail before reaching the event loop, which must
    advance the run's clock too. A backoff factor below 1 makes more than 10
    attempts before the deadline, so the --requests-only warning would show.
    """
    env.skipOnCluster()
    test_time = 6
    rc, elapsed, stderr_content, stdout_content, results = _run_until_server_gone(
        env, ["--reconnect-backoff-factor=0.7"], test_time)
    env.assertIsNotNone(rc, message="memtier kept reconnecting past --test-time")
    env.assertEqual(rc, 0, message="a run that reached --test-time must end normally")
    env.assertTrue("run is over, not reconnecting" in stderr_content,
                   message="the run did not stop reconnecting at --test-time")
    env.assertTrue("Totals" in stdout_content, message="the report was not printed")
    env.assertFalse("server still unreachable" in stderr_content,
                    message="the wait-for-the-server warning is only for --requests runs")
    env.assertLess(elapsed, test_time + 5 + _SLACK,
                   message="run took {:.1f}s for --test-time={}".format(elapsed, test_time))
    # Every failed reconnect counts as a connection error on top of the drop,
    # over TCP and over a unix socket alike: 2 clients reconnecting for 4 s
    # report well over one error each.
    env.assertIsNotNone(results, message="no JSON report")
    if results is not None:
        conn_errors = results["ALL STATS"]["Totals"]["Connection Errors"]
        env.assertGreaterEqual(conn_errors, 4, message="failed reconnects were not counted: {}".format(conn_errors))


def test_server_gone_long_backoff_ends_at_test_time(env):
    """
    A reconnect armed just before the --test-time deadline must not wait out
    its full backoff delay: the run used to end only when that attempt failed,
    up to 60 s late, and report that dead time in its duration and throughput
    (#656). The delay is capped at the time left, and the client's stats end
    at the deadline.
    """
    env.skipOnCluster()
    test_time = 4
    rc, elapsed, stderr_content, _, results = _run_until_server_gone(
        env, ["--reconnect-backoff-factor=20"], test_time, stop_after=1.5)
    env.assertIsNotNone(rc, message="memtier kept reconnecting past --test-time")
    env.assertEqual(rc, 0, message="a run that reached --test-time must end normally")
    env.assertTrue("run is over, not reconnecting" in stderr_content,
                   message="the run did not stop reconnecting at --test-time")
    env.assertLess(elapsed, test_time + 3 + _SLACK,
                   message="run took {:.1f}s for --test-time={}".format(elapsed, test_time))
    env.assertIsNotNone(results, message="no JSON report")
    if results is not None:
        # The stats end at the deadline itself; a 20 s backoff would overshoot
        # it by far more than this tolerance.
        duration_ms = results["ALL STATS"]["Runtime"]["Total duration"]
        env.assertGreaterEqual(duration_ms, test_time * 1000 - 100,
                               message="Total duration {} ms for --test-time={}".format(duration_ms, test_time))
        env.assertLessEqual(duration_ms, test_time * 1000 + (2000 if _SLACK else 500),
                            message="Total duration {} ms for --test-time={}".format(duration_ms, test_time))


def test_server_gone_never_started_client_ends_at_test_time(env):
    """
    A client whose connection is up but never answered when the server goes
    away, while another client's is, never starts its own clock. The
    connection-stage supervisor is already disarmed by the other client, so
    the run's deadline (from the benchmark's start) must stop its reconnects
    too, instead of retrying forever (#656). That client did no work, so the
    run fails (exit 1), as for any client that ends before its stop condition.
    """
    env.skipOnCluster()
    test_time = 4
    for threads, clients in ((2, 1), (1, 2)):
        rc, elapsed, stderr_content, _, _ = _run_until_server_gone(
            env, ["--rate-limiting=50"], test_time, threads=threads, clients=clients, forward_first_only=True)
        env.assertIsNotNone(rc, message="-t {} -c {}: memtier kept reconnecting past --test-time".format(threads, clients))
        env.assertEqual(rc, 1, message="-t {} -c {}: a client that never started must fail the run".format(
            threads, clients))
        env.assertTrue("stopped before reaching their stop condition" in stderr_content,
                       message="-t {} -c {}: the never-started client was not reported".format(threads, clients))
        env.assertTrue("run is over, not reconnecting" in stderr_content,
                       message="-t {} -c {}: the run did not stop reconnecting".format(threads, clients))
        env.assertLess(elapsed, test_time + 5 + _SLACK,
                       message="-t {} -c {}: run took {:.1f}s for --test-time={}".format(threads, clients, elapsed,
                                                                                       test_time))


def _assert_late_client_timed_from_run_start(env, results, test_time):
    """The client that only connects after the deadline is timed from the run's
    start: the JSON Total duration, the average over all clients, covers
    --test-time. Timed from its late connect instead, its own duration would
    be a fraction of a second, pulling the average well below --test-time."""
    env.assertIsNotNone(results, message="no JSON report")
    if results is not None:
        duration_ms = results["ALL STATS"]["Runtime"]["Total duration"]
        env.assertGreaterEqual(duration_ms, test_time * 1000 - 100,
                               message="Total duration {} ms for --test-time={}: the late client was timed from its "
                               "connect".format(duration_ms, test_time))


def test_never_started_client_dropped_after_deadline_does_not_restart(env):
    """
    A client whose connection is up but never answered, while another
    client's is, is dropped after the deadline by a server that then answers
    new connections. Once it reconnects it must not start its clock then and
    run a whole --test-time after the deadline: it is timed from the
    benchmark's start, so its first reply ends it, and the run ends normally
    (#656).

    Under TLS the proxy cannot hold a connection after its handshake (it
    forwards raw bytes): the unanswered connection never completes its
    handshake, so it is still connecting, and its drop is a failed connect,
    as from a server that is gone. That client fails the run (exit 1). The
    started-late path itself is transport-independent; under TLS it is
    exercised by test_json_aggregated_connection_errors_sec, where the server
    kills connections after their handshake.
    """
    env.skipOnCluster()
    test_time = 3

    def drop_and_serve(proxy):
        proxy._forward_first_only = False
        proxy.drop()

    rc, elapsed, stderr_content, _, results = _run_until_server_gone(
        env, ["--rate-limiting=50"], test_time, threads=1, clients=2, forward_first_only=True,
        script=[(test_time + 0.3, drop_and_serve)])
    if env.useTLS:
        env.assertEqual(rc, 1, message="TLS: a never-completed handshake is a failed connect and fails the run")
        env.assertTrue("Connection failed and the run is over" in stderr_content)
    else:
        env.assertEqual(rc, 0, message="the run must end normally")
        _assert_late_client_timed_from_run_start(env, results, test_time)
    env.assertLess(elapsed, test_time + 2 + _SLACK,
                   message="run took {:.1f}s: the never-started client ran after the deadline".format(elapsed))


def test_never_started_client_dropped_before_deadline_does_not_restart(env):
    """
    Same as above, but the drop comes shortly before the deadline: the
    reconnect is due at the deadline (its delay is capped there), when the run
    is over. Connected then, the client must not start its clock and run a
    whole --test-time after the deadline (#656). This is what a server that
    keeps killing connections does to a client killed before each first reply
    (test_json_aggregated_connection_errors_sec under TLS).

    Under TLS the proxy cannot hold a connection after its handshake (it
    forwards raw bytes): the unanswered connection never completes its
    handshake, so it is still connecting, and its drop is a failed connect,
    as from a server that is gone. That client fails the run (exit 1). The
    started-late path itself is transport-independent; under TLS it is
    exercised by test_json_aggregated_connection_errors_sec, where the server
    kills connections after their handshake.
    """
    env.skipOnCluster()
    test_time = 3

    def drop_and_serve(proxy):
        proxy._forward_first_only = False
        proxy.drop()

    for at in (-0.5, -0.05):
        rc, elapsed, stderr_content, _, results = _run_until_server_gone(
            env, ["--rate-limiting=50"], test_time, threads=1, clients=2, forward_first_only=True,
            script=[(test_time + at, drop_and_serve)])
        if env.useTLS:
            env.assertEqual(rc, 1, message="drop at {:+.2f}s, TLS: a never-completed handshake is a failed connect "
                            "and fails the run".format(at))
            env.assertTrue("Connection failed and the run is over" in stderr_content)
        else:
            env.assertEqual(rc, 0, message="drop at {:+.2f}s: the run must end normally".format(at))
            _assert_late_client_timed_from_run_start(env, results, test_time)
        env.assertLess(elapsed, test_time + 2 + _SLACK,
                       message="drop at {:+.2f}s: run took {:.1f}s: the never-started client ran after the "
                       "deadline".format(at, elapsed))


def test_accept_then_close_peer_with_retry_on_error_ends_at_test_time(env):
    """
    A peer that accepts each connection and closes it at once (a proxy with
    a dead backend, a TLS terminator, maxclients) drops an established
    connection, which is still reconnected after the deadline so held
    requests are resent. With --retry-on-error every reconnect resends the
    replay queue and is dropped again: past the deadline that must not turn
    into an endless zero-delay reconnect loop (#656). One reconnect after the
    run is over is allowed; a second drop ends the run.
    """
    env.skipOnCluster()
    test_time = 4
    rc, elapsed, stderr_content, stdout_content, _ = _run_until_server_gone(
        env, ["--retry-on-error"], test_time, accept_after_stop=True)
    env.assertIsNotNone(rc, message="memtier kept reconnecting past --test-time")
    env.assertEqual(rc, 0, message="a run that reached --test-time must end normally")
    env.assertTrue("run is over, not reconnecting" in stderr_content,
                   message="the run did not stop reconnecting at --test-time")
    env.assertTrue("Totals" in stdout_content, message="the report was not printed")
    env.assertLess(elapsed, test_time + 4 + _SLACK,
                   message="run took {:.1f}s for --test-time={}".format(elapsed, test_time))


def test_live_server_drop_after_deadline_resends_held_requests(env):
    """
    A live server that drops connections with replies pending just after the
    --test-time deadline: the drop is reconnected and, with --retry-on-error,
    the held requests are resent and answered, so the run ends normally with
    none abandoned. Only a failed connect (or a second drop) ends the
    reconnects once the run is over (#656). The proxy holds the replies from
    just before the deadline, then closes the connections just after it.
    """
    env.skipOnCluster()
    test_time = 3
    rc, elapsed, stderr_content, stdout_content, _ = _run_until_server_gone(
        env, ["--retry-on-error", "--pipeline=8"], test_time, clients=1,
        script=[(test_time - 0.5, lambda p: p.freeze()), (test_time + 0.7, lambda p: p.drop())])
    env.assertEqual(rc, 0, message="a run that reached --test-time must end normally")
    # Over TLS the proxy's close (no close_notify) is reported as a
    # connection error rather than an EOF; both are drops of an established
    # connection.
    dropped = "connection dropped." in stderr_content or "Connection error, attempting" in stderr_content
    env.assertTrue(dropped, message="the proxy did not drop the connection")
    env.assertFalse("run is over, not reconnecting" in stderr_content,
                    message="a drop from a live server after the deadline was not reconnected")
    env.assertFalse("held for retry when the run ended were not resent" in stderr_content,
                    message="the requests held for replay were abandoned")
    env.assertTrue("Totals" in stdout_content, message="the report was not printed")
    env.assertLess(elapsed, test_time + 5 + _SLACK,
                   message="run took {:.1f}s for --test-time={}".format(elapsed, test_time))


def test_reconnect_hanging_past_drain_grace_is_left_to_connection_timeout(env):
    """
    A client whose reconnect is still connecting when the 10 s drain grace
    after --test-time expires has nothing in flight: it is not a stalled
    client, and must be left to the reconnect logic (#669's idle-client
    guard), not ended as "got no reply". The server's port stops answering
    SYNs, so the reconnect armed for the deadline hangs until
    --connection-timeout, after the grace; that failed connect then ends the
    run normally (#656).
    """
    env.skipOnCluster()
    if env.isUnixSocket():
        env.skip()  # a full unix-socket backlog fails the connect at once
    test_time = 3
    rc, elapsed, stderr_content, _, results = _run_until_server_gone(
        env, ["--connection-timeout=12"], test_time, clients=1,
        script=[(1.5, lambda p: p.stop(blackhole=True))], wait=40 + _SLACK)
    env.assertEqual(rc, 0, message="a run that reached --test-time must end normally")
    env.assertFalse("got no reply" in stderr_content,
                    message="a client waiting for its reconnect was ended as stalled")
    env.assertTrue("Connection timeout" in stderr_content, message="the reconnect did not hang until the timeout")
    env.assertTrue("run is over, not reconnecting" in stderr_content,
                   message="the timed-out reconnect did not end the run")
    # Ended by the 12 s connect timeout, not by the 10 s grace.
    env.assertGreater(elapsed, test_time + 10, message="run took {:.1f}s".format(elapsed))
    env.assertLess(elapsed, 30 + _SLACK, message="run took {:.1f}s".format(elapsed))
    if results is not None:
        duration_ms = results["ALL STATS"]["Runtime"]["Total duration"]
        env.assertLessEqual(duration_ms, test_time * 1000 + (2000 if _SLACK else 500),
                            message="Total duration {} ms for --test-time={}".format(duration_ms, test_time))


def test_setup_rejected_then_server_gone_fails_the_run(env):
    """
    The server rejects AUTH on a reconnect, then goes away: the last failure
    is a failed connect, but the setup had been rejected since the last reply.
    That run must fail at the deadline (#630), not end normally as for a
    server that is only gone (#656). Over a unix socket the failed connect
    happens inside reconnect(), which used to lose the rejection.
    """
    env.skipOnCluster()
    test_time = 5
    conn = env.getOSSMasterNodesConnectionList()[0]

    def change_password(proxy):
        conn.execute_command("CONFIG", "SET", "requirepass", "other-656")
        proxy.drop()  # the reconnects reach the server and get -WRONGPASS

    try:
        rc, elapsed, stderr_content, _, _ = _run_until_server_gone(
            env, ["--authenticate=pw-656"], test_time,
            before_start=lambda: conn.execute_command("CONFIG", "SET", "requirepass", "pw-656"),
            # The reconnect due 1 s after the drop gets -WRONGPASS; the
            # server is gone for the next ones.
            script=[(1.5, change_password), (3.0, lambda p: p.stop())])
    finally:
        _clear_requirepass(conn, ("other-656", "pw-656"))
    env.assertTrue("authentication failed" in stderr_content, message="AUTH was not rejected on reconnect")
    env.assertEqual(rc, 1, message="a run whose setup was rejected before the server went away must fail")
    env.assertTrue("Connection setup failed earlier and the run is over" in stderr_content,
                   message="the rejected setup was not taken into account")
    env.assertTrue("client(s) failed once the run was over" in stderr_content,
                   message="the failed client was not reported as such")
    env.assertLess(elapsed, test_time + 4 + _SLACK,
                   message="run took {:.1f}s for --test-time={}".format(elapsed, test_time))


def test_setup_recovered_then_server_gone_ends_normally(env):
    """
    The server rejects AUTH on a reconnect, a later reconnect authenticates
    and gets replies, then the server goes away before the deadline. The
    rejection was recovered from, so the run ends normally at the deadline
    (#656), not as a rejected setup (#630). Any workload reply shows the
    connection serves requests again, error replies included (the SETEX
    variant only gets errors).
    """
    env.skipOnCluster()
    test_time = 7
    conn = env.getOSSMasterNodesConnectionList()[0]

    def change_password(proxy):
        conn.execute_command("CONFIG", "SET", "requirepass", "other-656")
        proxy.drop()

    for extra in ([], ["--command=SETEX __key__ -1 v"]):
        try:
            rc, elapsed, stderr_content, _, _ = _run_until_server_gone(
                env, ["--authenticate=pw-656", "--reconnect-backoff-factor=0"] + extra, test_time,
                before_start=lambda: conn.execute_command("CONFIG", "SET", "requirepass", "pw-656"),
                # Rejected at ~2.2 s, accepted at ~3.2 s, server gone at 4.5 s.
                script=[(1.2, change_password),
                        (2.5, lambda p: conn.execute_command("CONFIG", "SET", "requirepass", "pw-656")),
                        (4.5, lambda p: p.stop())])
        finally:
            _clear_requirepass(conn, ("other-656", "pw-656"))
        label = " ".join(extra) or "default workload"
        env.assertTrue("authentication failed" in stderr_content,
                       message="{}: AUTH was not rejected on reconnect".format(label))
        env.assertEqual(rc, 0, message="{}: a run that recovered from a rejected setup must end normally".format(label))
        env.assertFalse("setup failed earlier" in stderr_content,
                        message="{}: the recovered setup failure was still counted".format(label))
        env.assertTrue("Connection failed and the run is over" in stderr_content,
                       message="{}: the run did not end at the deadline".format(label))
        env.assertLess(elapsed, test_time + 4 + _SLACK,
                       message="{}: run took {:.1f}s for --test-time={}".format(label, elapsed, test_time))


def test_server_gone_bounded_reconnects_fail_as_before(env):
    """
    With a finite --max-reconnect-attempts the attempts bound the run, as on
    master, also around the --test-time deadline: a server that stays down
    fails the run (exit 1) once they run out, at their usual backoff (no
    burst of attempts at the deadline).
    """
    env.skipOnCluster()
    test_time = 6
    for attempts in (6, 1):
        rc, elapsed, stderr_content, _, results = _run_until_server_gone(
            env, ["--reconnect-backoff-factor=0", "--max-reconnect-attempts={}".format(attempts)], test_time)
        env.assertEqual(rc, 1, message="--max-reconnect-attempts={}: a server that stays down must fail the run".format(
            attempts))
        env.assertTrue("Maximum reconnection attempts ({}) exceeded".format(attempts) in stderr_content)
        env.assertFalse("run is over, not reconnecting" in stderr_content,
                        message="the deadline ended a run with bounded attempts")
        # The server goes at 2 s; one attempt a second.
        env.assertLess(elapsed, 2 + attempts + 4 + _SLACK, message="run took {:.1f}s".format(elapsed))
        if results is not None:
            conn_errors = results["ALL STATS"]["Totals"]["Connection Errors"]
            env.assertLessEqual(conn_errors, 2 * (attempts + 2),
                                message="{} connection errors for 2 clients and {} attempts".format(conn_errors, attempts))


def test_server_restarting_around_deadline_bounded_ends_normally(env):
    """
    With a finite --max-reconnect-attempts, a server that restarts around the
    --test-time deadline (down just before it, back after it) is reconnected
    once it is back, and the run ends normally (exit 0), as on master: the
    attempts were not exhausted.
    """
    env.skipOnCluster()
    test_time = 6
    rc, elapsed, stderr_content, _, _ = _run_until_server_gone(
        env, ["--reconnect-backoff-factor=0", "--max-reconnect-attempts=10"], test_time,
        script=[(5.3, lambda p: p.stop()), (6.8, lambda p: p.restart())])
    env.assertEqual(rc, 0, message="a bounded run whose server came back must end normally")
    env.assertFalse("Maximum reconnection attempts" in stderr_content)
    env.assertLess(elapsed, test_time + 5 + _SLACK,
                   message="run took {:.1f}s for --test-time={}".format(elapsed, test_time))


def test_server_gone_unlimited_reconnects_warn_with_requests(env):
    """
    With --requests and unlimited --max-reconnect-attempts the run waits for
    the server indefinitely, as documented, but must say so, once (#656).
    """
    env.skipOnCluster()
    # A backoff factor below 1 shrinks the delay (down to the 0.1 s floor),
    # so the 10 attempts that trigger the warning take about 2.5 s instead of
    # 10: it shows about 4.5 s into the run (the server goes at 2 s).
    rc, _, stderr_content, _, _ = _run_until_server_gone(env, ["--reconnect-backoff-factor=0.7"], None,
                                                         requests=1000000, wait=8 + _SLACK)
    env.assertIsNone(rc, message="a --requests run with unlimited reconnects must keep waiting for the server")
    env.assertTrue("server still unreachable after 10 reconnection attempts" in stderr_content,
                   message="no warning that the run is waiting for an unreachable server")
    # Once per run, not once per connection (2 clients here).
    env.assertEqual(stderr_content.count("server still unreachable"), 1,
                    message="the wait-for-the-server warning was repeated")


def test_server_gone_before_start_unlimited_reconnects_end_at_stage_timeout(env):
    """
    A server that is gone before the first connect never starts the run's
    clock, which --test-time is measured on. That startup phase is bounded by
    --connection-stage-timeout instead (#426): the run must abort with exit
    code 2 once it expires, not keep reconnecting (#656).
    """
    env.skipOnCluster()
    stage_timeout = 6 if _sanitized() else 3
    rc, elapsed, stderr_content, _, _ = _run_until_server_gone(
        env, ["--reconnect-backoff-factor=0", "--connection-stage-timeout={}".format(stage_timeout)], 2,
        stop_after=None, wait=stage_timeout + 15)
    env.assertIsNotNone(rc, message="memtier kept reconnecting to a server that was never up")
    # --test-time (2 s) passes first, but no connection reached steady state,
    # so it does not apply: the supervisor must end the run.
    env.assertFalse("run is over, not reconnecting" in stderr_content,
                    message="--test-time ended a run that never started")
    if env.isUnixSocket():
        # A missing unix socket fails the first connect synchronously, in
        # prepare(), so the run fails at once without reconnecting.
        env.assertEqual(rc, 1, message="a run that cannot connect its unix socket must fail")
        env.assertTrue("failed to connect" in stderr_content)
        return
    env.assertEqual(rc, 2, message="a run that never connected must fail through --connection-stage-timeout")
    env.assertTrue("memtier_benchmark: aborting after" in stderr_content,
                   message="the --connection-stage-timeout supervisor did not end the run")
    env.assertLess(elapsed, stage_timeout + 5,
                   message="run took {:.1f}s for --connection-stage-timeout={}".format(elapsed, stage_timeout))


def test_merged_duration_averages_all_threads(env):
    """
    With several threads, the JSON Start time / Finish time / Total duration
    (and so every rate) are the average over all clients. merge_run_stats()
    restarted the averaging weight for each thread, so each thread's first
    client replaced the average built so far and the result came only from
    the last thread's clients (#709).

    Four threads with one rate-limited client each take about 3 s for
    --requests=300. One client is disconnected once and, with
    --reconnect-backoff-factor=3, waits 3 s before reconnecting, so it ends
    about 3 s later: the rate limiter resets its budget on disconnect and
    refills it per interval after the reconnect, so it does not catch up on
    the missed requests. The mean over all four clients is about 3.75 s.
    The killed client is the first to connect, so it is not in the last
    thread, and the last thread alone reports about 3 s.
    """
    env.skipOnCluster()
    requests, rate, reconnect_delay_ms = 300, 100, 3000
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            "--rate-limiting={}".format(rate),
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=1000",
            "--reconnect-on-error",
            "--max-reconnect-attempts=10",
            "--reconnect-backoff-factor={}".format(reconnect_delay_ms // 1000),
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=4, clients=1, requests=requests)
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    conn = env.getOSSMasterNodesConnectionList()[0]
    initial_client_ids = {client["id"] for client in conn.client_list()}

    stderr_path = "{0}/mb.stderr".format(config.results_dir)
    with open("{0}/mb.stdout".format(config.results_dir), "w") as stdout_f, open(stderr_path, "w") as stderr_f:
        proc = subprocess.Popen(benchmark.args, stdout=stdout_f, stderr=stderr_f, cwd=config.results_dir)
        try:
            deadline = time.monotonic() + 10
            while True:
                workload = [c for c in conn.client_list()
                            if c["id"] not in initial_client_ids and c["cmd"] in ("get", "set")]
                if len(workload) == 4:
                    break
                if proc.poll() is not None or time.monotonic() >= deadline:
                    raise AssertionError("four active workload clients were not ready before the kill "
                                         "(rc={}, clients={})".format(proc.poll(), workload))
                time.sleep(0.02)
            time.sleep(1)
            killed = conn.execute_command("CLIENT", "KILL", "ID", workload[0]["id"])
            killed_alive = proc.poll() is None
            return_code = proc.wait(timeout=60)
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=10)

    with open(stderr_path) as f:
        stderr_content = f.read()
    with open("{0}/mb.json".format(config.results_dir)) as f:
        runtime = json.load(f)["ALL STATS"]["Runtime"]
    env.debugPrint("Runtime: {}".format(runtime), True)
    diag = "rc={} killed={} memtier alive at kill={} stderr tail:\n{}".format(
        return_code, killed, killed_alive, stderr_content[-2000:])
    env.assertEqual(killed, 1, message="CLIENT KILL ID did not kill a workload client; " + diag)
    env.assertEqual(return_code, 0, message=diag)
    # CLIENT KILL arrives as EOF ("Connection dropped") or, e.g. as ECONNRESET
    # or under TLS, as an error ("Connection error"); both log this.
    env.assertTrue("attempting reconnection" in stderr_content,
                   message="the killed client did not reconnect; " + diag)
    duration = runtime["Total duration"]
    env.assertEqual(duration, runtime["Finish time"] - runtime["Start time"], message=str(runtime))
    # Mean of three clients at ~3 s and one at ~6 s: ~3.75 s. Allow half the
    # delayed client's share either side. The last thread alone gives ~3 s
    # (~6 s if it held the killed client); a weight that overcounts the
    # first client (e.g. advancing by 2 per client) gives ~4.3 s.
    expected_ms = requests * 1000 // rate + reconnect_delay_ms // 4
    margin_ms = reconnect_delay_ms // 8
    env.assertGreater(duration, expected_ms - margin_ms,
                      message="Total duration {} ms ignores the delayed client; {}".format(duration, runtime))
    env.assertLess(duration, expected_ms + margin_ms,
                   message="Total duration {} ms overweights the delayed client; {}".format(duration, runtime))
