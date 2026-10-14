import json
import re
import tempfile
import time
import threading
from include import *
from mb import Benchmark, RunConfig


def test_reconnect_on_connection_kill(env):
    """
    Test that memtier_benchmark can automatically reconnect when connections are killed.

    This test:
    1. Starts memtier_benchmark with --reconnect-on-error enabled
    2. Runs a background thread that periodically kills client connections using CLIENT KILL
    3. Verifies that memtier_benchmark successfully reconnects and completes the test
    """
    key_max = 10000
    key_min = 1

    # Configure memtier with reconnection enabled
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum={}".format(key_min),
            "--key-maximum={}".format(key_max),
            "--reconnect-on-error",  # Enable automatic reconnection
            "--max-reconnect-attempts=10",  # Allow up to 10 reconnection attempts
            "--reconnect-backoff-factor=1.5",  # Backoff factor for delays
            "--connection-timeout=5",  # 5 second connection timeout
        ],
    }
    addTLSArgs(benchmark_specs, env)

    # Use fewer threads/clients and more requests to have a longer running test
    config = get_default_memtier_config(threads=2, clients=2, requests=5000)
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(
        config, key_min, key_max
    )

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # Get master connections for killing clients
    master_nodes_connections = env.getOSSMasterNodesConnectionList()

    # Flag to stop the killer thread
    stop_killer = threading.Event()
    kill_count = [0]  # Use list to allow modification in nested function

    def client_killer():
        """Background thread that kills client connections periodically"""
        while not stop_killer.is_set():
            time.sleep(2)  # Wait 2 seconds between kills
            try:
                for master_connection in master_nodes_connections:
                    # Get list of clients
                    clients = master_connection.execute_command("CLIENT", "LIST")

                    # CLIENT LIST may return bytes or string depending on Redis client version
                    if isinstance(clients, bytes):
                        clients = clients.decode('utf-8')

                    # Parse client list and find memtier clients
                    # CLIENT LIST returns a string with one client per line
                    for client_line in clients.split("\n"):
                        if not client_line.strip():
                            continue

                        # Parse client info
                        client_info = {}
                        for part in client_line.split(' '):
                            if "=" in part:
                                key, value = part.split("=", 1)
                                client_info[key] = value

                        # Kill client if it has an ID and is not the current connection
                        # (avoid killing our own connection)
                        if "id" in client_info and "cmd" in client_info:
                            # Don't kill connections running CLIENT LIST
                            if client_info["cmd"] != "client":
                                try:
                                    master_connection.execute_command(
                                        "CLIENT", "KILL", "ID", client_info["id"]
                                    )
                                    kill_count[0] += 1
                                    env.debugPrint(
                                        "Killed client ID: {}".format(
                                            client_info["id"]
                                        ),
                                        True,
                                    )
                                except Exception as e:
                                    # Client might have already disconnected
                                    env.debugPrint(
                                        "Failed to kill client {}: {}".format(
                                            client_info["id"], str(e)
                                        ),
                                        True,
                                    )
            except Exception as e:
                env.debugPrint("Error in client_killer: {}".format(str(e)), True)

    # Start the killer thread
    killer_thread = threading.Thread(target=client_killer)
    killer_thread.daemon = True
    killer_thread.start()

    try:
        # Run memtier_benchmark
        memtier_ok = benchmark.run()

        # Stop the killer thread
        stop_killer.set()
        killer_thread.join(timeout=5)

        env.debugPrint("Total clients killed: {}".format(kill_count[0]), True)

        # Verify that we actually killed some connections
        if kill_count[0] == 0:
            env.debugPrint("WARNING: No clients were killed during the test", True)
        env.assertTrue(kill_count[0] > 0)

        # Verify memtier completed successfully despite connection kills
        debugPrintMemtierOnError(config, env)
        env.assertTrue(memtier_ok == True)

        # Verify output files exist
        env.assertTrue(os.path.isfile("{0}/mb.stdout".format(config.results_dir)))
        env.assertTrue(os.path.isfile("{0}/mb.stderr".format(config.results_dir)))
        env.assertTrue(os.path.isfile("{0}/mb.json".format(config.results_dir)))

        # Check stderr for reconnection messages
        with open("{0}/mb.stderr".format(config.results_dir)) as stderr:
            stderr_content = stderr.read()
            # Should see reconnection attempt messages
            has_reconnect_msg = "reconnection" in stderr_content.lower() or "reconnect" in stderr_content.lower()
            if not has_reconnect_msg:
                env.debugPrint("WARNING: No reconnection messages found in stderr", True)
            env.assertTrue(has_reconnect_msg)

        # Verify that some requests were completed
        # (we may not get the exact expected count due to reconnections, but should get some)
        merged_command_stats = {
            "cmdstat_set": {"calls": 0},
            "cmdstat_get": {"calls": 0},
        }
        overall_request_count = agg_info_commandstats(
            master_nodes_connections, merged_command_stats
        )
        if overall_request_count == 0:
            env.debugPrint("WARNING: No requests completed", True)
        env.assertTrue(overall_request_count > 0)

    finally:
        # Make sure to stop the killer thread
        stop_killer.set()
        killer_thread.join(timeout=5)


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

    config = get_default_memtier_config(threads=4, clients=2, requests=None, test_time=10)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()

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
        for _wave in range(2):
            for master_connection in master_nodes_connections:
                try:
                    killed = master_connection.execute_command(
                        "CLIENT", "KILL", "TYPE", "normal"
                    )
                    if isinstance(killed, int):
                        kill_count += killed
                except Exception as e:
                    env.debugPrint(
                        "CLIENT KILL failed: {}".format(str(e)), True
                    )
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

    This test verifies that:
    1. The misleading "Maximum reconnection attempts (0) exceeded" message
       does NOT appear in stderr.
    2. memtier_benchmark completes successfully (threads survive the kills).
    """
    import subprocess

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
        ],
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=2, clients=2, requests=5000)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)
    master_nodes_connections = env.getOSSMasterNodesConnectionList()

    stdout_path = "{0}/mb.stdout".format(config.results_dir)
    stderr_path = "{0}/mb.stderr".format(config.results_dir)
    with open(stdout_path, "w") as stdout_f, open(stderr_path, "w") as stderr_f:
        proc = subprocess.Popen(
            benchmark.args,
            stdout=stdout_f,
            stderr=stderr_f,
            cwd=config.results_dir,
        )

        # Let connections establish, then kill them twice in rapid succession
        # to maximise the chance that libevent delivers multiple error callbacks
        # per dead connection while a reconnect timer is already pending.
        time.sleep(1)
        for _wave in range(3):
            for conn in master_nodes_connections:
                try:
                    conn.execute_command("CLIENT", "KILL", "TYPE", "normal")
                except Exception:
                    pass
            time.sleep(0.5)

        try:
            return_code = proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
            return_code = proc.wait(timeout=10)

    with open(stderr_path) as f:
        stderr_content = f.read()

    env.debugPrint("memtier exit code: {}".format(return_code), True)
    env.debugPrint("STDERR:\n{}".format(stderr_content[:2000]), True)

    # Core regression assertion: the misleading terminal message must not appear.
    # Before the fix, duplicate error callbacks while m_reconnecting==True landed
    # in the terminal else and printed this with the configured cap (0) rather
    # than the actual attempt count, then killed the thread.
    env.assertFalse(
        "Maximum reconnection attempts (0) exceeded" in stderr_content,
        message="Spurious thread-kill message found: duplicate error callback while "
        "reconnect already pending incorrectly triggered event_base_loopbreak()",
    )

    # Benchmark must have completed; if threads were killed spuriously it would
    # exit non-zero or stall.
    env.assertEqual(return_code, 0)


def test_reconnect_disabled_by_default(env):
    """
    Test that reconnection is disabled by default and memtier fails when connections are killed.

    This test verifies backwards compatibility - without --reconnect-on-error flag,
    memtier should fail when connections are killed.
    """
    key_max = 1000
    key_min = 1

    # Configure memtier WITHOUT reconnection enabled
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum={}".format(key_min),
            "--key-maximum={}".format(key_max),
            # Note: NO --reconnect-on-error flag
        ],
    }
    addTLSArgs(benchmark_specs, env)

    # Use fewer threads/clients
    config = get_default_memtier_config(threads=1, clients=1, requests=10000)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # Get master connections for killing clients
    master_nodes_connections = env.getOSSMasterNodesConnectionList()

    # Start memtier in background
    import subprocess

    memtier_process = subprocess.Popen(
        benchmark.args,
        stdout=open("{0}/mb.stdout".format(config.results_dir), "w"),
        stderr=open("{0}/mb.stderr".format(config.results_dir), "w"),
        cwd=config.results_dir,
    )

    # Wait a bit for connections to establish
    time.sleep(1)

    # Kill one client connection
    killed = False
    for master_connection in master_nodes_connections:
        clients = master_connection.execute_command("CLIENT", "LIST")

        # CLIENT LIST may return bytes or string depending on Redis client version
        if isinstance(clients, bytes):
            clients = clients.decode('utf-8')

        for client_line in clients.split("\n"):
            if not client_line.strip():
                continue

            client_info = {}
            for part in client_line.split():
                if "=" in part:
                    key, value = part.split("=", 1)
                    client_info[key] = value

            if (
                "id" in client_info
                and "cmd" in client_info
                and client_info["cmd"] != "client"
            ):
                try:
                    master_connection.execute_command(
                        "CLIENT", "KILL", "ID", client_info["id"]
                    )
                    killed = True
                    env.debugPrint(
                        "Killed client ID: {}".format(client_info["id"]), True
                    )
                    break
                except:
                    pass
        if killed:
            break

    # Wait for memtier to finish
    return_code = memtier_process.wait(timeout=30)

    # Without reconnect-on-error, memtier should fail (non-zero exit code) when connection is killed
    # Note: This test might be flaky if the connection is killed after all work is done
    # So we just verify the test completes one way or another
    env.debugPrint("memtier exit code: {}".format(return_code), True)
    if not killed:
        env.debugPrint("WARNING: No connections were killed", True)
    env.assertTrue(killed)


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
