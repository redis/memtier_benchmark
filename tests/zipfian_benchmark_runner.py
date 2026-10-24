import tempfile
import threading
import redis
from collections import Counter

from include import (
    get_default_memtier_config,
    add_required_env_arguments,
    ensure_clean_benchmark_folder,
    addTLSArgs,
    agg_info_commandstats,
    assert_minimum_memtier_outcomes,
    get_expected_request_count,
)
from mb import Benchmark, RunConfig


class MonitorThread(threading.Thread):
    """Monitor Redis commands and count key accesses by command type"""

    def __init__(self, connection, stop_commands: set[str] = None):
        threading.Thread.__init__(self)
        self.monitor: redis.client.Monitor = connection.monitor()
        self.ready = threading.Event()
        self.stop_commands: set[str] = stop_commands or {
            "INFO COMMANDSTATS",
            "FLUSHALL",
        }
        self.key_counts: Counter = Counter()  # Overall key counts (for backward compatibility)
        self.key_counts_by_command: dict[str, Counter] = {}  # Key counts per command type

    def run(self):
        try:
            with self.monitor as m:
                # The context manager has received the MONITOR acknowledgement.
                self.ready.set()
                for command_info in m.listen():
                    command = command_info.get("command")

                    if command.upper() in self.stop_commands:
                        break

                    parts = command.split()
                    if len(parts) >= 2:
                        cmd_name = parts[0].upper()
                        key = parts[1]

                        # Track overall key counts (for backward compatibility)
                        self.key_counts[key] += 1

                        # Track key counts by command type
                        if cmd_name not in self.key_counts_by_command:
                            self.key_counts_by_command[cmd_name] = Counter()
                        self.key_counts_by_command[cmd_name][key] += 1

                        # Handle common Redis commands that use multiple keys
                        if cmd_name == "MSET" and len(parts) >= 3:
                            # MSET key1 value1 key2 value2 ...
                            for i in range(1, len(parts), 2):
                                if i < len(parts):
                                    key = parts[i]
                                    self.key_counts[key] += 1
                                    self.key_counts_by_command[cmd_name][key] += 1

        except redis.ConnectionError:
            # stop monitoring: server connection was closed
            pass

def get_combined_key_counts_by_command(monitor_threads):
    """Combine key counts by command from multiple monitor threads"""
    combined_by_command = {}
    combined_overall = Counter()

    for thread in monitor_threads:
        thread.join()  # Wait for thread to finish

        # Combine overall counts
        combined_overall.update(thread.key_counts)

        # Combine counts by command
        for cmd_name, key_counts in thread.key_counts_by_command.items():
            if cmd_name not in combined_by_command:
                combined_by_command[cmd_name] = Counter()
            combined_by_command[cmd_name].update(key_counts)

    return combined_overall, combined_by_command


class ZipfianBenchmarkRunner:
    """Helper class to run Zipfian distribution benchmarks and collect key access data"""

    def __init__(
        self, env, key_min: int, key_max: int, threads: int = 2, clients: int = 10
    ):
        self.env = env
        self.key_min = key_min
        self.key_max = key_max
        self.threads = threads
        self.clients = clients

    def run_benchmark_and_collect_key_counting(
        self, test_name: str, zipf_exp: float = None
    ) -> Counter:
        """Run a complete benchmark and return key access distribution data"""
        # Create benchmark specs
        args = [
            "--ratio=1:1",  # Both SET and GET operations
            "--key-pattern=Z:Z",  # Zipfian for both SET and GET
            f"--key-minimum={self.key_min}",
            f"--key-maximum={self.key_max}",
        ]

        if zipf_exp is not None:
            args.append(f"--key-zipf-exp={zipf_exp}")

        benchmark_specs = {"name": test_name, "args": args}

        addTLSArgs(benchmark_specs, self.env)

        config = get_default_memtier_config(threads=self.threads, clients=self.clients)
        master_nodes_list = self.env.getMasterNodesList()
        overall_expected_request_count = get_expected_request_count(
            config, self.key_min, self.key_max
        )

        add_required_env_arguments(benchmark_specs, config, self.env, master_nodes_list)

        # Create temporary directory and run config
        test_dir = tempfile.mkdtemp()
        run_config = RunConfig(test_dir, test_name, config, {})
        ensure_clean_benchmark_folder(run_config.results_dir)

        benchmark = Benchmark.from_json(run_config, benchmark_specs)

        # Setup monitoring
        master_nodes_connections = self.env.getOSSMasterNodesConnectionList()

        monitor_threads = []
        for conn in master_nodes_connections:
            # prevent accumulating stats from previous runs
            conn.execute_command("CONFIG", "RESETSTAT")

            # start monitoring connection
            monitor_thread = MonitorThread(conn)
            monitor_thread.start()
            monitor_threads.append(monitor_thread)
            self.env.assertTrue(monitor_thread.ready.wait(timeout=10),
                                message="MONITOR must be active before starting the benchmark")

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
            self.env,
            memtier_ok,
            overall_expected_request_count,
            overall_request_count,
        )

        """Collect and combine results from all monitor threads"""
        combined_key_counts = Counter()
        for thread in monitor_threads:
            thread.join()  # waits for monitor thread to finish
            combined_key_counts.update(thread.key_counts)

        return combined_key_counts

    def run_arbitrary_command_benchmark_and_collect_key_counting(
        self, test_name: str, commands: list[str], zipf_exp: float = None
    ) -> Counter:
        """Run a benchmark with arbitrary commands and return key access distribution data"""
        # Create benchmark specs with arbitrary commands
        args = [
            f"--key-minimum={self.key_min}",
            f"--key-maximum={self.key_max}",
        ]

        # Add arbitrary commands with zipfian key pattern
        for command in commands:
            args.append(f"--command={command}")
            args.append("--command-key-pattern=Z")

        if zipf_exp is not None:
            args.append(f"--key-zipf-exp={zipf_exp}")

        benchmark_specs = {"name": test_name, "args": args}

        addTLSArgs(benchmark_specs, self.env)

        config = get_default_memtier_config(threads=self.threads, clients=self.clients)
        master_nodes_list = self.env.getMasterNodesList()
        overall_expected_request_count = get_expected_request_count(
            config, self.key_min, self.key_max
        )

        add_required_env_arguments(benchmark_specs, config, self.env, master_nodes_list)

        # Create temporary directory and run config
        test_dir = tempfile.mkdtemp()
        run_config = RunConfig(test_dir, test_name, config, {})
        ensure_clean_benchmark_folder(run_config.results_dir)

        benchmark = Benchmark.from_json(run_config, benchmark_specs)

        # Setup monitoring
        master_nodes_connections = self.env.getOSSMasterNodesConnectionList()

        monitor_threads = []
        for conn in master_nodes_connections:
            # prevent accumulating stats from previous runs
            conn.execute_command("CONFIG", "RESETSTAT")

            # start monitoring connection
            monitor_thread = MonitorThread(conn)
            monitor_thread.start()
            monitor_threads.append(monitor_thread)
            self.env.assertTrue(monitor_thread.ready.wait(timeout=10),
                                message="MONITOR must be active before starting the benchmark")

        # Run the benchmark
        memtier_ok = benchmark.run()

        # For arbitrary commands, we need to check the actual commands used
        # Default to checking common commands, but this could be made more flexible
        merged_command_stats = {
            "cmdstat_set": {"calls": 0},
            "cmdstat_get": {"calls": 0},
            "cmdstat_hset": {"calls": 0},
        }
        overall_request_count = agg_info_commandstats(
            master_nodes_connections, merged_command_stats
        )
        assert_minimum_memtier_outcomes(
            run_config,
            self.env,
            memtier_ok,
            overall_expected_request_count,
            overall_request_count,
        )

        """Collect and combine results from all monitor threads"""
        combined_key_counts = Counter()
        for thread in monitor_threads:
            thread.join()  # waits for monitor thread to finish
            combined_key_counts.update(thread.key_counts)

        return combined_key_counts
