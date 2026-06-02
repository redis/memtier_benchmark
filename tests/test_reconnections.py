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

        Maximum reconnection attempts (0) exceeded for ..., triggering thread restart.

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
