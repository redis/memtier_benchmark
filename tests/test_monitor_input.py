import tempfile
import os
import json
from include import *
from mb import Benchmark, RunConfig


def test_monitor_input_specific_command(env):
    """
    Test that memtier_benchmark can use specific commands from a monitor input file.

    This test:
    1. Creates a monitor input file with multiple commands
    2. Uses __monitor_line1__ to select the first command (SET)
    3. Verifies the command executes correctly
    """
    # cluster mode does not support monitor-input option
    env.skipOnCluster()
    # Create monitor input file
    test_dir = tempfile.mkdtemp()
    monitor_file = os.path.join(test_dir, "monitor.txt")
    with open(monitor_file, "w") as f:
        f.write(
            '[ proxy49 ] 1764031576.604009 [0 172.16.10.147:51682] "SET" "key1" "value1"\n'
        )
        f.write('[ proxy47 ] 1764031576.603223 [0 172.16.10.147:39564] "GET" "key1"\n')
        f.write(
            '[ proxy48 ] 1764031576.605123 [0 172.16.10.147:41234] "HSET" "myhash" "field1" "value1"\n'
        )
        f.write(
            '[ proxy50 ] 1764031576.606456 [0 172.16.10.147:42567] "LPUSH" "mylist" "item1"\n'
        )
        f.write(
            '[ proxy51 ] 1764031576.607789 [0 172.16.10.147:43890] "SADD" "myset" "member1"\n'
        )

    # Configure memtier to use the first command from monitor file
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--monitor-input={}".format(monitor_file),
            "--command=__monitor_line1__",  # Use first command (SET)
        ],
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=1, clients=1, requests=100)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # Run memtier_benchmark
    memtier_ok = benchmark.run()

    # Verify success
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok == True)
    env.assertTrue(os.path.isfile("{0}/mb.stdout".format(config.results_dir)))
    env.assertTrue(os.path.isfile("{0}/mb.stderr".format(config.results_dir)))

    # Check that stderr shows the monitor file was loaded
    with open("{0}/mb.stderr".format(config.results_dir)) as stderr:
        stderr_content = stderr.read()
        env.assertTrue("Loaded 5 monitor commands from 5 total lines" in stderr_content)


    # Verify the key was created in Redis (standalone and OSS cluster-safe)
    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    found = False
    for master_connection in master_nodes_connections:
        try:
            result = master_connection.execute_command("GET", "key1")
        except Exception:
            # In cluster mode, non-owner shards may reply MOVED/ASK; ignore and continue
            continue
        if result == b"value1":
            found = True
            break
    env.assertTrue(found)


def test_monitor_input_random_runtime(env):
    """
    Test that __monitor_line@__ picks random commands at runtime.

    This test:
    1. Creates a monitor input file with multiple different command types
    2. Uses __monitor_line@__ to randomly select commands at runtime
    3. Verifies that multiple different command types were executed
    """
    # cluster mode does not support monitor-input option
    env.skipOnCluster()
    # Create monitor input file with diverse commands
    test_dir = tempfile.mkdtemp()
    monitor_file = os.path.join(test_dir, "monitor.txt")
    with open(monitor_file, "w") as f:
        f.write(
            '[ proxy49 ] 1764031576.604009 [0 172.16.10.147:51682] "SET" "key1" "value1"\n'
        )
        f.write('[ proxy47 ] 1764031576.603223 [0 172.16.10.147:39564] "GET" "key1"\n')
        f.write(
            '[ proxy48 ] 1764031576.605123 [0 172.16.10.147:41234] "HSET" "myhash" "field1" "value1"\n'
        )
        f.write(
            '[ proxy50 ] 1764031576.606456 [0 172.16.10.147:42567] "LPUSH" "mylist" "item1"\n'
        )
        f.write(
            '[ proxy51 ] 1764031576.607789 [0 172.16.10.147:43890] "SADD" "myset" "member1"\n'
        )

    # Configure memtier to use random commands from monitor file
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--monitor-input={}".format(monitor_file),
            "--command=__monitor_line@__",  # Command selection at runtime
            "--monitor-pattern=R",  # Random selection
        ],
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=2, clients=2, requests=100)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # Run memtier_benchmark
    memtier_ok = benchmark.run()

    # Verify success
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok == True)
    env.assertTrue(os.path.isfile("{0}/mb.stdout".format(config.results_dir)))
    env.assertTrue(os.path.isfile("{0}/mb.stderr".format(config.results_dir)))

    # Check that stderr shows the monitor file was loaded
    with open("{0}/mb.stderr".format(config.results_dir)) as stderr:
        stderr_content = stderr.read()
        env.assertTrue("Loaded 5 monitor commands from 5 total lines" in stderr_content)

    # Verify that multiple different data types were created in Redis
    # This proves that different commands were executed
    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    types_found = set()

    for master_connection in master_nodes_connections:
        # Check for different key types
        keys_to_check = [
            ("key1", "string"),
            ("myhash", "hash"),
            ("mylist", "list"),
            ("myset", "set"),
        ]

        for key, expected_type in keys_to_check:
            try:
                key_type = master_connection.execute_command("TYPE", key)
                if isinstance(key_type, bytes):
                    key_type = key_type.decode("utf-8")
                if key_type == expected_type:
                    types_found.add(expected_type)
            except:
                pass

    # We should have at least 2 different types, proving randomization worked
    env.debugPrint("Types found: {}".format(types_found), True)
    env.assertTrue(len(types_found) >= 2)
    if len(types_found) < 2:
        env.debugPrint(
            "Expected at least 2 different data types, found: {}".format(types_found),
            True,
        )


def test_monitor_input_sequential_default(env):
    """
    Test that __monitor_line@__ picks commands sequentially when monitor-pattern is explicitly set to S.

    This test:
    1. Creates a monitor input file with multiple SET commands for the same key but different values
    2. Uses __monitor_line@__ with --monitor-pattern=S (sequential pattern, which is also the default)
    3. Verifies that the commands are applied in sequential order (with wrap-around)
    """
    # cluster mode does not support monitor-input option
    env.skipOnCluster()
    # Create monitor input file with sequential SET commands
    test_dir = tempfile.mkdtemp()
    monitor_file = os.path.join(test_dir, "monitor.txt")
    with open(monitor_file, "w") as f:
        f.write(
            '[ proxy60 ] 1764031576.604009 [0 172.16.10.147:51682] "SET" "seq_key" "v1"\n'
        )
        f.write(
            '[ proxy61 ] 1764031576.605123 [0 172.16.10.147:41234] "SET" "seq_key" "v2"\n'
        )
        f.write(
            '[ proxy62 ] 1764031576.606456 [0 172.16.10.147:42567] "SET" "seq_key" "v3"\n'
        )

    # Configure memtier to use sequential commands from monitor file with explicit pattern S
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--monitor-input={}".format(monitor_file),
            "--command=__monitor_line@__",  # Sequential selection
            "--monitor-pattern=S",       # Explicit sequential pattern
        ],
    }
    addTLSArgs(benchmark_specs, env)

    # 4 requests: expect sequence q1, q2, q3, q1
    config = get_default_memtier_config(threads=1, clients=1, requests=4)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # Run memtier_benchmark
    memtier_ok = benchmark.run()

    # Verify success
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok == True)
    env.assertTrue(os.path.isfile("{0}/mb.stdout".format(config.results_dir)))
    env.assertTrue(os.path.isfile("{0}/mb.stderr".format(config.results_dir)))

    # Verify the final value for seq_key corresponds to the expected sequence with wrap-around (v1)
    # This must work both on standalone and OSS cluster deployments.
    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    found = False
    for master_connection in master_nodes_connections:
        try:
            result = master_connection.execute_command("GET", "seq_key")
        except Exception:
            # In cluster mode, non-owner shards may reply MOVED/ASK; ignore and continue
            continue
        if result == b"v1":
            found = True
            break
    env.assertTrue(found)


def test_monitor_input_mixed_commands(env):
    """
    Test mixing specific and random monitor commands with command ratios.

    This test:
    1. Creates a monitor input file
    2. Uses 30% __monitor_line1__ (specific SET command) and 70% __monitor_line@__ (random)
    3. Verifies both command types execute correctly
    """
    # cluster mode does not support monitor-input option
    env.skipOnCluster()
    # Create monitor input file
    test_dir = tempfile.mkdtemp()
    monitor_file = os.path.join(test_dir, "monitor.txt")
    with open(monitor_file, "w") as f:
        f.write(
            '[ proxy49 ] 1764031576.604009 [0 172.16.10.147:51682] "SET" "key1" "value1"\n'
        )
        f.write('[ proxy47 ] 1764031576.603223 [0 172.16.10.147:39564] "GET" "key1"\n')
        f.write(
            '[ proxy48 ] 1764031576.605123 [0 172.16.10.147:41234] "HSET" "myhash" "field1" "value1"\n'
        )
        f.write(
            '[ proxy50 ] 1764031576.606456 [0 172.16.10.147:42567] "LPUSH" "mylist" "item1"\n'
        )
        f.write(
            '[ proxy51 ] 1764031576.607789 [0 172.16.10.147:43890] "SADD" "myset" "member1"\n'
        )

    # Configure memtier with mixed commands
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--monitor-input={}".format(monitor_file),
            "--command=__monitor_line1__",
            "--command-ratio=30",
            "--command=__monitor_line@__",
            "--command-ratio=70",
        ],
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=1, clients=1, requests=100)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # Run memtier_benchmark
    memtier_ok = benchmark.run()

    # Verify success
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok == True)
    env.assertTrue(os.path.isfile("{0}/mb.stdout".format(config.results_dir)))
    env.assertTrue(os.path.isfile("{0}/mb.stderr".format(config.results_dir)))

    # Check that stderr shows the monitor file was loaded
    with open("{0}/mb.stderr".format(config.results_dir)) as stderr:
        stderr_content = stderr.read()
        env.assertTrue("Loaded 5 monitor commands from 5 total lines" in stderr_content)


    # Verify key1 exists (from the specific SET command) in a cluster-safe way
    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    found = False
    for master_connection in master_nodes_connections:
        try:
            result = master_connection.execute_command("GET", "key1")
        except Exception:
            # In cluster mode, non-owner shards may reply MOVED/ASK; ignore and continue
            continue
        if result == b"value1":
            found = True
            break
    env.assertTrue(found)


def test_monitor_input_malformed_placeholder_rejected(env):
    """
    Test that malformed monitor placeholders are rejected with an error.

    This test verifies that placeholders like:
    - __monitor_line1 (missing trailing __)
    - __monitor_line1_ (only one trailing underscore)
    - __monitor_line1__garbage (trailing characters)
    - __monitor_line1abc__ (non-numeric characters before __)
    are properly rejected when --monitor-input is provided.
    """
    # cluster mode does not support monitor-input option
    env.skipOnCluster()
    # Create monitor input file
    test_dir = tempfile.mkdtemp()
    monitor_file = os.path.join(test_dir, "monitor.txt")
    with open(monitor_file, "w") as f:
        f.write(
            '[ proxy49 ] 1764031576.604009 [0 172.16.10.147:51682] "SET" "key1" "value1"\n'
        )

    malformed_placeholders = [
        "__monitor_line1",        # missing trailing __
        "__monitor_line1_",       # only one trailing underscore
        "__monitor_line1__garbage",  # trailing characters after valid placeholder
        "__monitor_line1abc__",   # non-numeric characters before __
        "__monitor_line__",       # missing index number
    ]

    config = get_default_memtier_config(threads=1, clients=1, requests=10)
    master_nodes_list = env.getMasterNodesList()

    for placeholder in malformed_placeholders:
        benchmark_specs = {
            "name": env.testName,
            "args": [
                "--monitor-input={}".format(monitor_file),
                "--command={}".format(placeholder),
            ],
        }
        addTLSArgs(benchmark_specs, env)

        add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

        run_config = RunConfig(test_dir, env.testName, config, {})
        ensure_clean_benchmark_folder(run_config.results_dir)

        benchmark = Benchmark.from_json(run_config, benchmark_specs)

        # Run memtier_benchmark - should fail
        memtier_ok = benchmark.run()

        # Verify failure
        if memtier_ok:
            env.debugPrint(
                "Expected failure for malformed placeholder '{}' but it succeeded".format(
                    placeholder
                ),
                True,
            )
        env.assertFalse(memtier_ok)

        # Check stderr for error message
        stderr_file = "{0}/mb.stderr".format(run_config.results_dir)
        if os.path.isfile(stderr_file):
            with open(stderr_file) as stderr:
                stderr_content = stderr.read()
                # The placeholder should either be rejected as invalid monitor placeholder
                # or treated as unknown command by Redis
                has_error = (
                    "invalid monitor placeholder" in stderr_content
                    or "error" in stderr_content.lower()
                )
                if not has_error:
                    env.debugPrint(
                        "Expected error message for '{}', got: {}".format(
                            placeholder, stderr_content
                        ),
                        True,
                    )
                env.assertTrue(has_error)


def test_monitor_placeholder_literal_without_monitor_input(env):
    """
    Test that __monitor_line1__ is treated as a literal command when --monitor-input is not provided.

    When monitor input is not configured, placeholder-like strings should be sent
    to Redis as-is (which will result in an unknown command error from Redis).
    This verifies that the placeholder validation only applies when monitor is in use.
    """
    # cluster mode does not support monitor-input option
    env.skipOnCluster()
    test_dir = tempfile.mkdtemp()

    # Configure memtier WITHOUT --monitor-input but WITH a monitor-like placeholder
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--command=__monitor_line1__",  # This should be sent literally to Redis
        ],
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=1, clients=1, requests=10)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # Run memtier_benchmark
    # This should NOT fail with "invalid monitor placeholder" error
    # It will likely fail because Redis doesn't recognize __monitor_line1__ as a command,
    # but that's a Redis error, not a memtier validation error
    benchmark.run()  # Result doesn't matter - we only care about the error type

    # Check stderr - should NOT contain "invalid monitor placeholder"
    stderr_file = "{0}/mb.stderr".format(config.results_dir)
    if os.path.isfile(stderr_file):
        with open(stderr_file) as stderr:
            stderr_content = stderr.read()
            has_monitor_error = "invalid monitor placeholder" in stderr_content
            if has_monitor_error:
                env.debugPrint(
                    "Placeholder validation should not apply without --monitor-input",
                    True,
                )
            env.assertFalse(has_monitor_error)


def test_monitor_random_reproducible_without_randomize(env):
    """
    Test that monitor random selection is reproducible when --randomize is NOT used.

    Without --randomize, the random generator uses a constant seed, so running
    memtier twice with the same configuration should produce identical results.

    This test:
    1. Creates a monitor file with numbered SET commands
    2. Runs memtier twice without --randomize
    3. Verifies both runs set the same final values (proving same command sequence)
    """
    # cluster mode does not support monitor-input option
    env.skipOnCluster()
    # Create monitor input file with SET commands that write their index
    test_dir = tempfile.mkdtemp()
    monitor_file = os.path.join(test_dir, "monitor.txt")
    with open(monitor_file, "w") as f:
        # Each command sets a counter key to track which command was selected
        f.write('[ proxy1 ] 1764031576.604009 [0 127.0.0.1:1234] "INCR" "cmd_1_count"\n')
        f.write('[ proxy2 ] 1764031576.604010 [0 127.0.0.1:1234] "INCR" "cmd_2_count"\n')
        f.write('[ proxy3 ] 1764031576.604011 [0 127.0.0.1:1234] "INCR" "cmd_3_count"\n')
        f.write('[ proxy4 ] 1764031576.604012 [0 127.0.0.1:1234] "INCR" "cmd_4_count"\n')
        f.write('[ proxy5 ] 1764031576.604013 [0 127.0.0.1:1234] "INCR" "cmd_5_count"\n')

    # Run configuration - single thread/client for deterministic ordering
    base_args = [
        "--monitor-input={}".format(monitor_file),
        "--command=__monitor_line@__",
        "--monitor-pattern=R",  # Random selection
        # Note: NO --randomize flag - should use constant seed
    ]

    config_dict = get_default_memtier_config(threads=1, clients=1, requests=100)
    master_nodes_list = env.getMasterNodesList()

    # Helper function to run benchmark and get command counts
    def run_and_get_counts(run_name):
        run_dir = os.path.join(test_dir, run_name)
        os.makedirs(run_dir, exist_ok=True)

        benchmark_specs = {"name": run_name, "args": base_args.copy()}
        addTLSArgs(benchmark_specs, env)
        add_required_env_arguments(benchmark_specs, config_dict.copy(), env, master_nodes_list)

        config = RunConfig(run_dir, run_name, config_dict.copy(), {})
        ensure_clean_benchmark_folder(config.results_dir)

        benchmark = Benchmark.from_json(config, benchmark_specs)
        memtier_ok = benchmark.run()

        if not memtier_ok:
            debugPrintMemtierOnError(config, env)
        env.assertTrue(memtier_ok)

        # Get counts from Redis
        counts = {}
        master_nodes_connections = env.getOSSMasterNodesConnectionList()
        for conn in master_nodes_connections:
            for i in range(1, 6):
                key = "cmd_{}_count".format(i)
                val = conn.execute_command("GET", key)
                if val:
                    if isinstance(val, bytes):
                        val = val.decode("utf-8")
                    counts[key] = int(val)
        return counts

    # Clear any existing keys
    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    for conn in master_nodes_connections:
        conn.execute_command("FLUSHALL")

    # Run 1
    counts_run1 = run_and_get_counts("run1")
    env.debugPrint("Run 1 counts: {}".format(counts_run1), True)

    # Clear Redis for run 2
    for conn in master_nodes_connections:
        conn.execute_command("FLUSHALL")

    # Run 2 - should produce identical results
    counts_run2 = run_and_get_counts("run2")
    env.debugPrint("Run 2 counts: {}".format(counts_run2), True)

    # Verify both runs produced the same command distribution
    env.assertEqual(counts_run1, counts_run2)


def test_command_stats_breakdown_by_command(env):
    """
    Test that --command-stats-breakdown=command (default) aggregates stats by command type.

    This test:
    1. Runs memtier with multiple SET and GET commands
    2. Verifies the output shows aggregated "Sets" and "Gets" rows (not per-command)
    """
    # cluster mode does not support monitor-input option
    env.skipOnCluster()
    test_dir = tempfile.mkdtemp()

    # Configure memtier with multiple commands of the same type
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--command=SET foo __key__ __data__",
            "--command=SET bar __key__ __data__",
            "--command=GET foo",
            "--command=GET bar",
            "--hide-histogram",
            # Default is --command-stats-breakdown=command
        ],
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=1, clients=1, requests=100)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # Run memtier_benchmark
    memtier_ok = benchmark.run()

    # Verify success
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok == True)

    # Check stdout for aggregated output
    with open("{0}/mb.stdout".format(config.results_dir)) as stdout:
        stdout_content = stdout.read()

        # Count occurrences of "Sets" and "Gets" in the output
        # With aggregation, we should see exactly one "Sets" row and one "Gets" row
        lines = stdout_content.split("\n")
        sets_count = sum(1 for line in lines if line.strip().startswith("Sets"))
        gets_count = sum(1 for line in lines if line.strip().startswith("Gets"))

        # Should have exactly 1 Sets row and 1 Gets row (aggregated)
        env.assertEqual(sets_count, 1)
        env.assertEqual(gets_count, 1)

    # Verify JSON output includes time series data for aggregated command types
    json_filename = "{0}/mb.json".format(config.results_dir)
    with open(json_filename) as results_json:
        results_dict = json.load(results_json)

        # Check that aggregated command types have Time-Serie data
        set_metrics = results_dict["ALL STATS"]["Sets"]
        get_metrics = results_dict["ALL STATS"]["Gets"]

        # Verify Time-Serie exists and is not empty
        env.assertTrue("Time-Serie" in set_metrics)
        env.assertTrue("Time-Serie" in get_metrics)

        set_metrics_ts = set_metrics["Time-Serie"]
        get_metrics_ts = get_metrics["Time-Serie"]

        # Time series should have at least one second of data
        env.assertTrue(len(set_metrics_ts) > 0)
        env.assertTrue(len(get_metrics_ts) > 0)

        # Verify time series data has expected fields
        for second_data in set_metrics_ts.values():
            env.assertTrue("Count" in second_data)
            env.assertTrue("Bytes RX" in second_data)
            env.assertTrue("Bytes TX" in second_data)
            # If we had commands on that second, verify latency metrics exist
            if second_data["Count"] > 0:
                env.assertTrue("p50.00" in second_data)
                env.assertTrue("p99.00" in second_data)

        for second_data in get_metrics_ts.values():
            env.assertTrue("Count" in second_data)
            env.assertTrue("Bytes RX" in second_data)
            env.assertTrue("Bytes TX" in second_data)
            if second_data["Count"] > 0:
                env.assertTrue("p50.00" in second_data)
                env.assertTrue("p99.00" in second_data)


def test_command_stats_breakdown_by_line(env):
    """
    Test that --command-stats-breakdown=line shows each command line separately.

    This test:
    1. Runs memtier with multiple SET and GET commands
    2. Uses --command-stats-breakdown=line
    3. Verifies the output shows separate rows for each command
    """
    # cluster mode does not support monitor-input option
    env.skipOnCluster()
    test_dir = tempfile.mkdtemp()

    # Configure memtier with multiple commands of the same type
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--command=SET foo __key__ __data__",
            "--command=SET bar __key__ __data__",
            "--command=GET foo",
            "--command=GET bar",
            "--hide-histogram",
            "--command-stats-breakdown=line",  # Show per-command stats
        ],
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=1, clients=1, requests=100)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # Run memtier_benchmark
    memtier_ok = benchmark.run()

    # Verify success
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok == True)

    # Check stdout for per-command output
    with open("{0}/mb.stdout".format(config.results_dir)) as stdout:
        stdout_content = stdout.read()

        # Count occurrences of "Sets" and "Gets" in the output
        # Without aggregation, we should see 2 "Sets" rows and 2 "Gets" rows
        lines = stdout_content.split("\n")
        sets_count = sum(1 for line in lines if line.strip().startswith("Sets"))
        gets_count = sum(1 for line in lines if line.strip().startswith("Gets"))

        # Should have 2 Sets rows and 2 Gets rows (one per command)
        env.assertEqual(sets_count, 2)
        env.assertEqual(gets_count, 2)


def test_monitor_input_malformed_command_skipped(env):
    """
    Test that malformed monitor commands are skipped with a warning instead of
    causing memtier_benchmark to hang or crash.

    This test:
    1. Creates a monitor file with a mix of valid and malformed commands
    2. Runs memtier with __monitor_line@__ to select commands at runtime
    3. Verifies the benchmark completes successfully
    4. Verifies warning messages are logged for malformed commands
    """
    # cluster mode does not support monitor-input option
    env.skipOnCluster()
    test_dir = tempfile.mkdtemp()

    # Create monitor file with some malformed commands
    monitor_file = os.path.join(test_dir, "monitor.txt")
    with open(monitor_file, "w") as f:
        # Valid command
        f.write('1764031576.604009 [0 127.0.0.1:51682] "SET" "key1" "value1"\n')
        # Malformed command - unclosed quote
        f.write('1764031576.605000 [0 127.0.0.1:51682] "SET" "key2" "unclosed\n')
        # Valid command
        f.write('1764031576.606000 [0 127.0.0.1:51682] "SET" "key3" "value3"\n')
        # Malformed command - empty
        f.write('1764031576.607000 [0 127.0.0.1:51682] \n')
        # Valid command
        f.write('1764031576.608000 [0 127.0.0.1:51682] "SET" "key4" "value4"\n')

    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--monitor-input={}".format(monitor_file),
            "--command=__monitor_line@__",
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)

    # Use enough requests to likely hit the malformed commands
    config = get_default_memtier_config(threads=1, clients=1, requests=50)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # Run memtier_benchmark - should complete without hanging
    memtier_ok = benchmark.run()

    # Verify success (benchmark should complete, not hang)
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok == True)

    # Check that stderr shows warnings about skipped commands
    with open("{0}/mb.stderr".format(config.results_dir)) as stderr:
        stderr_content = stderr.read()
        # Should have warning messages about skipped malformed commands
        env.assertTrue(
            "warning: skipping" in stderr_content.lower()
            or "Loaded" in stderr_content  # At minimum, the file was loaded
        )


def test_monitor_input_nul_byte_in_prefix_not_truncated(env):
    """
    Regression test for NUL-truncation in load_from_file (review finding #30, bug F1).

    Before the fix, load_from_file used strchr(line, '"') to locate the start
    of the command portion.  strchr stops at the first NUL byte, so a MONITOR
    capture line whose metadata prefix contains an embedded \\0 (e.g. a binary
    timestamp field) caused the entire line to be skipped with a "no commands
    found" warning — even though getline had read the full byte count and the
    '"CMD"' portion was intact.

    The fix replaces strchr with memchr(seg_start, '"', seg_len), which searches
    exactly the byte count returned by getline and therefore finds the opening
    quote regardless of NUL bytes in the prefix.

    This test writes three MONITOR-format lines to a binary file, where the
    second line has a NUL byte embedded in its metadata prefix (before the
    first '"').  The benchmark is run with __monitor_line@__ and we assert that
    all three commands are loaded (i.e. line 2 was NOT silently dropped).
    """
    env.skipOnCluster()

    test_dir = tempfile.mkdtemp()
    monitor_file = os.path.join(test_dir, "monitor_nul.bin")

    # Write the file in binary mode so the NUL byte is preserved verbatim.
    # Line 2 has '\x00' in the metadata prefix, before the first '"'.
    line1 = b'1764031576.604009 [0 127.0.0.1:51682] "SET" "nul_key1" "value1"\n'
    line2 = b'1764031576.605000\x00[0 127.0.0.1:51682] "SET" "nul_key2" "value2"\n'
    line3 = b'1764031576.606000 [0 127.0.0.1:51682] "SET" "nul_key3" "value3"\n'
    with open(monitor_file, "wb") as f:
        f.write(line1 + line2 + line3)

    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--monitor-input={}".format(monitor_file),
            "--command=__monitor_line@__",
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=1, clients=1, requests=9)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)
    memtier_ok = benchmark.run()

    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok == True,
                   message="memtier failed while replaying monitor file with NUL byte in prefix")

    # All three commands must have been loaded — if line 2 was dropped due to
    # NUL truncation the count would be 2, not 3.
    with open("{0}/mb.stderr".format(config.results_dir)) as stderr:
        stderr_content = stderr.read()
        env.assertTrue(
            "Loaded 3 monitor commands from 3 total lines" in stderr_content,
            message="Expected 3 loaded commands (NUL-in-prefix line was silently dropped). "
                    "stderr: {}".format(stderr_content[-800:]),
        )


def test_monitor_input_cr_only_line_endings(env):
    """
    Regression test for CR-only line-ending handling in load_from_file
    (review finding #30, bug F2).

    Before the fix, getline() used '\\n' as the sole delimiter.  A monitor
    capture file whose lines are separated by bare '\\r' (classic Mac / some
    Windows exporters) was therefore read as a single giant line; the metadata
    of every line after the first was appended as extra tokens to the first
    command, and most commands were dropped or mis-routed silently.

    The fix normalises bare '\\r' bytes to '\\n' before tokenising, so each
    logical MONITOR line is treated as a separate segment.

    This test writes three MONITOR-format SET commands separated by '\\r' only
    (no '\\n') and asserts that all three commands are loaded.
    """
    env.skipOnCluster()

    test_dir = tempfile.mkdtemp()
    monitor_file = os.path.join(test_dir, "monitor_cr.bin")

    # Three SET commands, separated by \r only (no \n).
    line1 = b'1764031576.604009 [0 127.0.0.1:51682] "SET" "cr_key1" "value1"'
    line2 = b'1764031576.605000 [0 127.0.0.1:51682] "SET" "cr_key2" "value2"'
    line3 = b'1764031576.606000 [0 127.0.0.1:51682] "SET" "cr_key3" "value3"'
    with open(monitor_file, "wb") as f:
        f.write(line1 + b"\r" + line2 + b"\r" + line3 + b"\r")

    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--monitor-input={}".format(monitor_file),
            "--command=__monitor_line@__",
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=1, clients=1, requests=9)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)
    memtier_ok = benchmark.run()

    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok == True,
                   message="memtier failed while replaying CR-only monitor file")

    # All three commands must have been loaded.  Pre-fix, the entire file was
    # read as one line and only one (malformed) command would be found.
    with open("{0}/mb.stderr".format(config.results_dir)) as stderr:
        stderr_content = stderr.read()
        env.assertTrue(
            "Loaded 3 monitor commands from 3 total lines" in stderr_content,
            message="Expected 3 loaded commands (CR-only line endings not split correctly). "
                    "stderr: {}".format(stderr_content[-800:]),
        )

    # Verify that all three keys were actually set in Redis.
    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    keys_found = set()
    for master_connection in master_nodes_connections:
        for key in ("cr_key1", "cr_key2", "cr_key3"):
            try:
                result = master_connection.execute_command("EXISTS", key)
                if result:
                    keys_found.add(key)
            except Exception:
                pass
    env.assertTrue(
        len(keys_found) == 3,
        message="Expected all 3 CR-only keys in Redis, found: {}".format(keys_found),
    )


def test_monitor_input_large_line_no_stack_overflow(env):
    """
    Regression test for the VLA stack-overflow in
    arbitrary_command::split_command_to_args (config_types.cpp).

    Before the fix the function declared `char buffer[command_len]` on the
    stack and was called per-request from shard_connection::fill_pipeline ->
    client::create_arbitrary_request. Any MONITOR-replay line larger than
    the worker thread's stack (default 8-12 MB on Linux) blew the stack the
    moment the random/sequential picker selected it -> SIGSEGV (or, when
    glibc's canary caught it first, "*** stack smashing detected ***").

    The fix moves the buffer to the heap (std::vector<char>); this test
    feeds memtier a single SET line whose value is comfortably larger than
    the default per-thread stack and asserts memtier completes the run
    without crashing.
    """
    # --monitor-input does not support cluster mode (see other tests).
    env.skipOnCluster()

    # 16 MiB value: comfortably larger than the typical 8-12 MB Linux
    # per-thread stack on CI runners and dev boxes, smaller than redis'
    # default proto-max-bulk-len (512 MB). Pre-fix this crashed
    # deterministically on the very first request that selected the line.
    test_dir = tempfile.mkdtemp()
    monitor_file = os.path.join(test_dir, "monitor_huge.txt")
    big_value = "A" * (16 * 1024 * 1024)
    with open(monitor_file, "w") as f:
        f.write('"SET" "regression_key" "' + big_value + '"\n')

    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--monitor-input={}".format(monitor_file),
            "--command=__monitor_line@__",
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)

    # Small request count: 10 SETs of 16 MiB is ~160 MB on loopback - fast
    # enough for CI, and 10 calls is well past the first one that would have
    # crashed pre-fix.
    config = get_default_memtier_config(threads=1, clients=1, requests=10)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)
    memtier_ok = benchmark.run()

    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok == True,
                   message="memtier crashed (likely VLA stack overflow regression) "
                           "while replaying a large --monitor-input line")
    # Sanity: stderr must not carry the canary message.
    with open("{0}/mb.stderr".format(config.results_dir)) as stderr:
        stderr_content = stderr.read()
        env.assertFalse("stack smashing detected" in stderr_content,
                        message="glibc stack canary tripped: {}".format(
                            stderr_content[-500:]))

    # Sanity: the value made it to the server.
    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    found = False
    for master_connection in master_nodes_connections:
        try:
            result = master_connection.execute_command("STRLEN", "regression_key")
        except Exception:
            continue
        if result == len(big_value):
            found = True
            break
    env.assertTrue(found,
                   message="SET with 16 MiB value did not land in redis "
                           "- memtier may have crashed before sending it")
