from collections import Counter
import math
import tempfile
from itertools import pairwise

from zipfian_benchmark_runner import ZipfianBenchmarkRunner, MonitorThread
from include import (
    addTLSArgs,
    get_default_memtier_config,
    get_expected_request_count,
    add_required_env_arguments,
    ensure_clean_benchmark_folder,
    agg_info_commandstats,
    assert_minimum_memtier_outcomes
)
from mb import Benchmark, RunConfig


def correlation_coeficient(x: list[float], y: list[float]) -> float:
    """Calculate Pearson correlation coefficient between two lists of numbers"""
    n = len(x)
    mean_x = sum(x) / n
    mean_y = sum(y) / n

    numerator = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))

    sum_sq_x = sum((x[i] - mean_x) ** 2 for i in range(n))
    sum_sq_y = sum((y[i] - mean_y) ** 2 for i in range(n))
    denominator = math.sqrt(sum_sq_x * sum_sq_y)

    return numerator / denominator if denominator != 0 else 0


def analyze_zipfian_correlation(key_counts: Counter) -> float:
    """Analyze key distribution and return correlation coefficient for Zipf's law validation"""

    # Sort keys by frequency (descending)
    sorted_keys = sorted(key_counts.items(), key=lambda x: x[1], reverse=True)

    # Verify that frequency follows Zipf's law
    # in Zipfian, frequency of kth element is proportional to 1/k^s
    # so log(frequency) should be roughly linear with log(rank)
    ranks = list(range(1, len(sorted_keys) + 1))
    _, frequencies = zip(*sorted_keys)

    # Calculate correlation between log(rank) and log(frequency)
    log_ranks = [math.log(r) for r in ranks]
    log_freqs = [math.log(f) if f > 0 else 0 for f in frequencies]

    return correlation_coeficient(log_ranks, log_freqs)


def calculate_concentration_ratio(distribution: Counter, top_n: int = 10) -> float:
    """Calculate the concentration ratio of top N keys in the distribution"""
    top_n_sum = sum(sorted(distribution.values(), reverse=True)[:top_n])
    total_sum = sum(distribution.values())

    return top_n_sum / total_sum if total_sum > 0 else 0.0


def test_zipfian_key_distribution(env):
    """Test that the Zipfian key-pattern follows Zipf's law"""
    key_min = 1
    key_max = 10000

    # Use the helper class to run benchmark and collect data
    runner = ZipfianBenchmarkRunner(env, key_min, key_max)
    combined_key_counts = runner.run_benchmark_and_collect_key_counting(env.testName)

    # Verify Zipfian properties using helper function
    correlation = analyze_zipfian_correlation(combined_key_counts)

    # should be close to -1 for a perfect law relationship
    # should be negative since log(rank) is increasing and log(freq) is decreasing
    env.assertTrue(correlation < -0.8)


def test_zipfian_exponent_effect(env):
    """Test different Zipfian exponents and verify they affect the distribution"""
    key_min = 1
    key_max = 10000

    # Test with different exponents
    zipf_exponents = {0.5, 1.0, 2.0}
    distributions = {}

    # Run benchmarks for each exponent
    runner = ZipfianBenchmarkRunner(env, key_min, key_max)

    for zipf_exp in zipf_exponents:
        test_name = f"{env.testName}_exp_{zipf_exp}"
        combined_key_counts = runner.run_benchmark_and_collect_key_counting(
            test_name, zipf_exp=zipf_exp
        )
        distributions[zipf_exp] = combined_key_counts

    # Verify that higher exponents lead to more skewed distributions
    sorted_exponents = sorted(zipf_exponents)
    for exp1, exp2 in pairwise(sorted_exponents):
        dist1 = distributions[exp1]
        dist2 = distributions[exp2]

        concentration1 = calculate_concentration_ratio(dist1)
        concentration2 = calculate_concentration_ratio(dist2)

        # Higher exponent should have higher concentration in top keys
        env.assertTrue(concentration2 > concentration1)


def test_zipfian_arbitrary_command_set(env):
    """Test that arbitrary SET command with zipfian key pattern follows Zipf's law"""
    key_min = 1
    key_max = 10000

    # Use the helper class to run benchmark with arbitrary commands
    runner = ZipfianBenchmarkRunner(env, key_min, key_max)
    commands = ["SET __key__ __data__"]
    combined_key_counts = runner.run_arbitrary_command_benchmark_and_collect_key_counting(
        env.testName, commands
    )

    # Verify Zipfian properties using helper function
    correlation = analyze_zipfian_correlation(combined_key_counts)

    # should be close to -1 for a perfect law relationship
    # should be negative since log(rank) is increasing and log(freq) is decreasing
    env.assertTrue(correlation < -0.8)


def test_zipfian_arbitrary_command_hset(env):
    """Test that arbitrary HSET command with zipfian key pattern follows Zipf's law"""
    key_min = 1
    key_max = 10000

    # Use the helper class to run benchmark with arbitrary commands
    runner = ZipfianBenchmarkRunner(env, key_min, key_max)
    commands = ["HSET __key__ field1 __data__"]
    combined_key_counts = runner.run_arbitrary_command_benchmark_and_collect_key_counting(
        env.testName, commands
    )

    # Verify Zipfian properties using helper function
    correlation = analyze_zipfian_correlation(combined_key_counts)

    # should be close to -1 for a perfect law relationship
    env.assertTrue(correlation < -0.8)


def test_zipfian_arbitrary_command_mixed_operations(env):
    """Test multiple arbitrary commands with zipfian key pattern"""
    key_min = 1
    key_max = 10000

    # Use the helper class to run benchmark with multiple arbitrary commands
    runner = ZipfianBenchmarkRunner(env, key_min, key_max)
    commands = [
        "SET __key__ __data__",
        "GET __key__",
        "HSET __key__ field1 __data__"
    ]
    combined_key_counts = runner.run_arbitrary_command_benchmark_and_collect_key_counting(
        env.testName, commands
    )

    # Verify Zipfian properties using helper function
    correlation = analyze_zipfian_correlation(combined_key_counts)

    # should be close to -1 for a perfect law relationship
    env.assertTrue(correlation < -0.8)

    # Verify that we have a reasonable distribution
    env.assertTrue(len(combined_key_counts) > 100)  # Should access many different keys
    env.assertTrue(sum(combined_key_counts.values()) > 1000)  # Should have many operations


def test_zipfian_arbitrary_command_exponent_effect(env):
    """Test different Zipfian exponents with arbitrary commands"""
    key_min = 1
    key_max = 10000

    # Test with different exponents
    zipf_exponents = [0.5, 1.0, 2.0]
    distributions = {}

    # Run benchmarks for each exponent
    runner = ZipfianBenchmarkRunner(env, key_min, key_max)
    commands = ["SET __key__ __data__"]

    for zipf_exp in zipf_exponents:
        test_name = f"{env.testName}_exp_{zipf_exp}"
        combined_key_counts = runner.run_arbitrary_command_benchmark_and_collect_key_counting(
            test_name, commands, zipf_exp=zipf_exp
        )
        distributions[zipf_exp] = combined_key_counts

    # Verify that higher exponents lead to more skewed distributions
    sorted_exponents = sorted(zipf_exponents)
    for i in range(len(sorted_exponents) - 1):
        exp1 = sorted_exponents[i]
        exp2 = sorted_exponents[i + 1]

        dist1 = distributions[exp1]
        dist2 = distributions[exp2]

        concentration1 = calculate_concentration_ratio(dist1)
        concentration2 = calculate_concentration_ratio(dist2)

        # Higher exponent should have higher concentration in top keys
        env.assertTrue(concentration2 > concentration1)


def test_sequential_zipfian_mixed_pattern(env):
    """Test mixed sequential and zipfian key patterns (S:Z and Z:S)"""
    key_min = 1
    key_max = 1000  # Smaller range for clearer pattern analysis

    # Test S:Z pattern (Sequential SET, Zipfian GET)
    runner = ZipfianBenchmarkRunner(env, key_min, key_max)

    # Create benchmark specs with mixed pattern
    args = [
        "--ratio=1:1",  # Equal SET and GET operations
        "--key-pattern=S:Z",  # Sequential SET, Zipfian GET
        f"--key-minimum={key_min}",
        f"--key-maximum={key_max}",
        "--key-zipf-exp=1.0"
    ]

    benchmark_specs = {"name": f"{env.testName}_S_Z", "args": args}

    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=runner.threads, clients=runner.clients)
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config, key_min, key_max)
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create temporary directory and run config
    test_dir = tempfile.mkdtemp()
    run_config = RunConfig(test_dir, f"{env.testName}_S_Z", config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)

    benchmark = Benchmark.from_json(run_config, benchmark_specs)

    # Setup monitoring
    master_nodes_connections = env.getOSSMasterNodesConnectionList()

    monitor_threads = []
    for conn in master_nodes_connections:
        # prevent accumulating stats from previous runs
        conn.execute_command("CONFIG", "RESETSTAT")

        # start monitoring connection
        monitor_thread = MonitorThread(conn)
        monitor_thread.start()
        monitor_threads.append(monitor_thread)

    # Run the benchmark
    memtier_ok = benchmark.run()

    # Verify the benchmark ran successfully
    merged_command_stats = {
        "cmdstat_set": {"calls": 0},
        "cmdstat_get": {"calls": 0},
    }
    overall_request_count = agg_info_commandstats(
        master_nodes_connections, merged_command_stats
    )
    assert_minimum_memtier_outcomes(
        run_config,
        env,
        memtier_ok,
        overall_expected_request_count,
        overall_request_count,
    )

    # Collect and combine results from all monitor threads
    combined_key_counts = Counter()
    for thread in monitor_threads:
        thread.join()  # waits for monitor thread to finish
        combined_key_counts.update(thread.key_counts)

    # For S:Z pattern, we expect:
    # - Some keys accessed more frequently (from zipfian GET operations)
    # - But also some sequential pattern influence (from SET operations)
    # The distribution should be less perfectly zipfian than pure Z:Z

    # Verify we have reasonable key distribution
    env.assertTrue(len(combined_key_counts) > 50)  # Should access many different keys
    env.assertTrue(sum(combined_key_counts.values()) > 100)  # Should have many operations

    # The correlation should be negative but may be weaker than pure zipfian
    correlation = analyze_zipfian_correlation(combined_key_counts)
    env.assertTrue(correlation < -0.3)  # Weaker requirement due to mixed pattern

def test_zipfian_and_sequential_simultaneous_arbitrary_commands(env):
    """Test simultaneous zipfian HSET and sequential HGETALL commands in high keyspace"""
    key_min = 950000  # Start near end of typical keyspace
    key_max = 1000000  # End at 1M

    runner = ZipfianBenchmarkRunner(env, key_min, key_max)

    # Run both commands simultaneously:
    # - HSET with zipfian key pattern (will create/update hash keys following zipfian distribution)
    # - HGETALL with sequential key pattern (will read hash keys sequentially)
    args = [
        f"--key-minimum={key_min}",
        f"--key-maximum={key_max}",
        "--command=HSET __key__ field1 __data__",
        "--command-key-pattern=Z",  # Zipfian pattern for HSET
        "--command=HGETALL __key__",
        "--command-key-pattern=S",  # Sequential pattern for HGETALL
        "--key-zipf-exp=1.0"
    ]

    benchmark_specs = {"name": env.testName, "args": args}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=runner.threads, clients=runner.clients)
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config, key_min, key_max)
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)

    benchmark = Benchmark.from_json(run_config, benchmark_specs)

    # Setup monitoring
    master_nodes_connections = env.getOSSMasterNodesConnectionList()

    monitor_threads = []
    for conn in master_nodes_connections:
        conn.execute_command("CONFIG", "RESETSTAT")
        monitor_thread = MonitorThread(conn)
        monitor_thread.start()
        monitor_threads.append(monitor_thread)

    # Run the benchmark with both commands
    memtier_ok = benchmark.run()

    # Verify the benchmark ran successfully
    merged_command_stats = {
        "cmdstat_hset": {"calls": 0},
        "cmdstat_hgetall": {"calls": 0},
    }
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(run_config, env, memtier_ok, overall_expected_request_count, overall_request_count)

    # Collect and combine results from all monitor threads
    combined_key_counts = Counter()
    for thread in monitor_threads:
        thread.join()
        combined_key_counts.update(thread.key_counts)

    # Verify we have reasonable key distribution
    env.assertTrue(len(combined_key_counts) > 100)  # Should access many different keys
    env.assertTrue(sum(combined_key_counts.values()) > 100)  # Should have many operations

    # Extract key numbers and analyze patterns
    high_range_hits = 0
    total_hits = 0
    key_numbers = []

    for key_name, count in combined_key_counts.items():
        total_hits += count
        if key_name.startswith("memtier-"):
            try:
                key_num = int(key_name.split("-")[1])
                key_numbers.append(key_num)
                if key_num >= 950000:
                    high_range_hits += count
            except (IndexError, ValueError):
                pass

    # Verify that we got significant hits in the high keyspace range
    high_range_percentage = high_range_hits / total_hits if total_hits > 0 else 0
    env.assertTrue(high_range_percentage > 0.6)  # Should hit high range with mixed patterns

    # Verify that keys are in the expected range
    if key_numbers:
        min_key_accessed = min(key_numbers)
        max_key_accessed = max(key_numbers)
        env.assertTrue(min_key_accessed >= key_min)
        env.assertTrue(max_key_accessed <= key_max)

        # Check range coverage - should have good spread due to sequential HGETALL
        key_range_covered = max_key_accessed - min_key_accessed
        expected_range = key_max - key_min
        coverage_ratio = key_range_covered / expected_range
        env.assertTrue(coverage_ratio > 0.1)  # Should cover at least 10% of range

    # The combined pattern should show mixed characteristics:
    # - Some zipfian influence from HSET operations
    # - Some sequential influence from HGETALL operations
    # - Overall less extreme than pure zipfian but not completely uniform
    correlation = analyze_zipfian_correlation(combined_key_counts)
    env.assertTrue(-0.6 < correlation < -0.2)  # Mixed pattern: moderate negative correlation

    # Verify both command types were executed
    hset_calls = merged_command_stats.get("cmdstat_hset", {}).get("calls", 0)
    hgetall_calls = merged_command_stats.get("cmdstat_hgetall", {}).get("calls", 0)

    env.assertTrue(hset_calls > 0)  # Should have HSET operations
    env.assertTrue(hgetall_calls > 0)  # Should have HGETALL operations

    # Check that we have a reasonable mix of both operations
    total_commands = hset_calls + hgetall_calls
    if total_commands > 0:
        hset_ratio = hset_calls / total_commands
        hgetall_ratio = hgetall_calls / total_commands

        # Both commands should contribute significantly (at least 20% each)
        env.assertTrue(hset_ratio > 0.2)
        env.assertTrue(hgetall_ratio > 0.2)
