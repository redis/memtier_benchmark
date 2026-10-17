import tempfile
import re
import json
import time
import signal
import subprocess
import shutil
import os
import fcntl
import socket
import struct
import resource
from include import *
from mb import Benchmark, RunConfig


def test_preload_and_set_get(env):
    key_max = 500000
    key_min = 1
    benchmark_specs = {"name": env.testName, "args": ['--pipeline=10','--ratio=1:0','--key-pattern=P:P','--key-minimum={}'.format(key_min),'--key-maximum={}'.format(key_max)]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=2, clients=10, requests='allkeys')
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config,key_min, key_max)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    assert_keyspace_range(env, key_max, key_min, master_nodes_connections)

    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count,
                                    overall_request_count)
    json_filename = '{0}/mb.json'.format(config.results_dir)

    for master_connection in master_nodes_connections:
        master_connection.execute_command("CONFIG", "RESETSTAT")

    benchmark_specs = {"name": env.testName, "args": ['--pipeline=10','--ratio=1:1','--key-pattern=R:R','--key-minimum={}'.format(key_min),'--key-maximum={}'.format(key_max)]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=2, clients=10, requests=200000)
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config,key_min, key_max)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)

    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    assert_keyspace_range(env, key_max, key_min, master_nodes_connections)

    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count,
                                    overall_request_count)


def test_default_set(env):
    key_max = 500000
    key_min = 1
    benchmark_specs = {"name": env.testName, "args": ['--pipeline=10','--ratio=1:0','--key-pattern=P:P','--key-minimum={}'.format(key_min),'--key-maximum={}'.format(key_max)]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=2, clients=10, requests='allkeys')
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config,key_min, key_max)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()
    master_nodes_connections = env.getOSSMasterNodesConnectionList()

    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    assert_keyspace_range(env, key_max, key_min, master_nodes_connections)

    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count,
                                    overall_request_count)

    # ensure if we run again on a different key pattern the dataset doesn't grow
    for master_connection in master_nodes_connections:
        master_connection.execute_command("CONFIG", "RESETSTAT")

    benchmark_specs = {"name": env.testName, "args": ['--client-stats',f'{test_dir}/set_client_stats','--pipeline=10','--ratio=1:0','--key-pattern=R:R','--key-minimum={}'.format(key_min),'--key-maximum={}'.format(key_max)]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=2, clients=10, requests=200000)
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config,key_min, key_max)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    results_dir = config.results_dir
    ensure_clean_benchmark_folder(results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}}
    assert_keyspace_range(env, key_max, key_min, master_nodes_connections)

    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count,
                                    overall_request_count)

    # Assert that all CSV BW metrics are properly stored and calculated
    first_client_csv_stats = '{0}/set_client_stats-1-0-0.csv'.format(test_dir)
    found, set_tx_column_data = get_column_csv(first_client_csv_stats,"SET Total Bytes TX")
    env.assertTrue(found)
    found, set_rx_column_data = get_column_csv(first_client_csv_stats,"SET Total Bytes RX")
    env.assertTrue(found)
    found, set_tx_rx_column_data = get_column_csv(first_client_csv_stats,"SET Total Bytes")
    env.assertTrue(found)
    found, set_reqs_column_data = get_column_csv(first_client_csv_stats,"SET Requests")
    env.assertTrue(found)
    for col_pos, ops_sec in enumerate(set_reqs_column_data):
        if int(ops_sec) > 0:
            set_tx = int(set_tx_column_data[col_pos])
            set_rx = int(set_rx_column_data[col_pos])
            set_tx_rx = int(set_tx_rx_column_data[col_pos])
            env.assertTrue(set_tx >= 0)
            env.assertTrue(set_rx >= 0)
            env.assertTrue(set_tx_rx >= 0)
            env.assertAlmostEqual(set_tx_rx,set_tx+set_rx,1)

    # the GET bw should be 0
    found, get_tx_column_data = get_column_csv(first_client_csv_stats,"GET Total Bytes TX")
    env.assertTrue(found)
    found, get_rx_column_data = get_column_csv(first_client_csv_stats,"GET Total Bytes RX")
    env.assertTrue(found)
    found, get_tx_rx_column_data = get_column_csv(first_client_csv_stats,"GET Total Bytes")
    env.assertTrue(found)
    for col_pos, ops_sec in enumerate(set_reqs_column_data):
        if int(ops_sec) > 0:
            get_tx = int(get_tx_column_data[col_pos])
            get_rx = int(get_rx_column_data[col_pos])
            get_tx_rx = int(get_tx_rx_column_data[col_pos])
            env.assertTrue(get_tx == 0)
            env.assertTrue(get_rx == 0)
            env.assertTrue(get_tx_rx == 0)
            env.assertAlmostEqual(set_tx_rx,set_tx+set_rx,1)

    ## Assert that all JSON BW metrics are properly stored and calculated
    json_filename = '{0}/mb.json'.format(config.results_dir)
    with open(json_filename) as results_json:
        results_dict = json.load(results_json)
        set_metrics = results_dict['ALL STATS']['Sets']
        get_metrics = results_dict['ALL STATS']['Gets']
        set_metrics_ts = results_dict['ALL STATS']['Sets']["Time-Serie"]
        get_metrics_ts = results_dict['ALL STATS']['Gets']["Time-Serie"]
        for metric_name in ["KB/sec RX/TX","KB/sec RX","KB/sec TX","KB/sec"]:
            # assert the metric exists
            env.assertTrue(metric_name in set_metrics)
            env.assertTrue(metric_name in get_metrics)
            # assert the metric value is non zero on writes and zero on reads
            set_metric_value_kbs = set_metrics[metric_name]
            get_metric_value_kbs = get_metrics[metric_name]
            env.assertTrue(set_metric_value_kbs >= 0)
            env.assertTrue(get_metric_value_kbs == 0)

        for second_data in set_metrics_ts.values():
            bytes_rx = second_data["Bytes RX"]
            bytes_tx = second_data["Bytes TX"]
            count = second_data["Count"]
            # if we had commands on that second the BW needs to be > 0
            if count > 0:
                env.assertTrue(bytes_rx >= 0)
                env.assertTrue(bytes_tx >= 0)

        for second_data in get_metrics_ts.values():
            bytes_rx = second_data["Bytes RX"]
            bytes_tx = second_data["Bytes TX"]
            # This test is write only so there should be no reads RX/TX and count
            count = second_data["Count"]
            env.assertTrue(count == 0)
            env.assertTrue(bytes_rx == 0)
            env.assertTrue(bytes_tx == 0)

def test_default_set_get(env):
    benchmark_specs = {"name": env.testName, "args": []}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count)

    json_filename = '{0}/mb.json'.format(config.results_dir)
    ## Assert that all BW metrics are properly stored and calculated
    with open(json_filename) as results_json:
        results_dict = json.load(results_json)
        set_metrics = results_dict['ALL STATS']['Sets']
        get_metrics = results_dict['ALL STATS']['Gets']
        set_metrics_ts = results_dict['ALL STATS']['Sets']["Time-Serie"]
        get_metrics_ts = results_dict['ALL STATS']['Gets']["Time-Serie"]
        for metric_name in ["KB/sec RX/TX","KB/sec RX","KB/sec TX","KB/sec"]:
            # assert the metric exists
            env.assertTrue(metric_name in set_metrics)
            env.assertTrue(metric_name in get_metrics)
            # assert the metric value is non zero given we've had write and read
            set_metric_value_kbs = set_metrics[metric_name]
            get_metric_value_kbs = get_metrics[metric_name]
            env.assertTrue(set_metric_value_kbs >= 0)
            env.assertTrue(get_metric_value_kbs >= 0)

        for second_data in set_metrics_ts.values():
            bytes_rx = second_data["Bytes RX"]
            bytes_tx = second_data["Bytes TX"]
            count = second_data["Count"]
            # if we had commands on that second the BW needs to be > 0
            if count > 0:
                p50 = second_data["p50.00"]
                p99 = second_data["p99.00"]
                p999 = second_data["p99.90"]
                env.assertTrue(bytes_rx >= 0)
                env.assertTrue(bytes_tx >= 0)
                env.assertTrue(p50 >= 0.0)
                env.assertTrue(p99 >= 0.0)
                env.assertTrue(p999 >= 0.0)

        for second_data in get_metrics_ts.values():
            bytes_rx = second_data["Bytes RX"]
            bytes_tx = second_data["Bytes TX"]
            count = second_data["Count"]
            # if we had commands on that second the BW needs to be > 0
            if count > 0:
                p50 = second_data["p50.00"]
                p99 = second_data["p99.00"]
                p999 = second_data["p99.90"]
                env.assertTrue(bytes_rx >= 0)
                env.assertTrue(bytes_tx >= 0)
                env.assertTrue(p50 >= 0.0)
                env.assertTrue(p99 >= 0.0)
                env.assertTrue(p999 >= 0.0)

def test_default_set_get_with_print_percentiles(env):
    p_str = '0,10,20,30,40,50,60,70,80,90,95,100'
    histogram_prefix = 'percentiles-test'
    benchmark_specs = {"name": env.testName, "args": ['--print-percentiles={}'.format(p_str),'--hdr-file-prefix={}'.format(histogram_prefix)]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count)
    json_filename = '{0}/mb.json'.format(config.results_dir)

    hdr_files_sufix = ["_FULL_RUN_1","_SET_command_run_1","_GET_command_run_1"]
    histogram_files = []
    for sufix in hdr_files_sufix:
        for ftype in ["txt","hgrm"]:
            histogram_files.append("{0}{1}.{2}".format(histogram_prefix,sufix,ftype))

    ## Assert that all requested percentiles are stored at the json file
    with open(json_filename) as results_json:
        results_dict = json.load(results_json)
        set_percentiles = results_dict['ALL STATS']['Sets']['Percentile Latencies']
        cleaned_keys = [ x.split(".")[0] for x in  set_percentiles.keys() ]
        for p in p_str.split(","):
            env.assertTrue("p{}".format(p) in cleaned_keys )
    
    # Assert that histogram output files are present
    for fname in histogram_files:
        env.assertTrue(os.path.isfile('{0}'.format(fname)))

    

# run each test on different env
def test_default_set_get_1_1(env):
    benchmark_specs = {"name": env.testName, "args": ['--ratio=1:1']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count)

    # assert same number of gets and sets
    env.assertEqual(merged_command_stats['cmdstat_set']['calls'], merged_command_stats['cmdstat_get']['calls'])

# run each test on different env
def test_short_reconnect_interval(env):
    # cluster mode dose not support reconnect-interval option
    env.skipOnCluster()
    benchmark_specs = {"name": env.testName, "args": ['--reconnect-interval=1']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count)


# run each test on different env
def test_reconnect_interval_with_rate_limiting(env):
    # cluster mode does not support reconnect-interval option
    env.skipOnCluster()
    # Reproduces the hang when combining --reconnect-interval=1 with --rate-limiting=1.
    # With rate-limiting=1, each connection sends 1 req/sec, so use short test-time.
    benchmark_specs = {"name": env.testName, "args": ['--reconnect-interval=1', '--rate-limiting=1', '--test-time=10']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=None, test_time=10)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()
    env.assertTrue(memtier_ok)

    # Validate JSON output exists and has results
    json_filename = '{0}/mb.json'.format(config.results_dir)
    with open(json_filename) as json_file:
        results = json.load(json_file)
    sets_count = results['ALL STATS']['Sets']['Count']
    gets_count = results['ALL STATS']['Gets']['Count']
    env.assertGreater(sets_count + gets_count, 0)


# run each test on different env
def test_default_set_get_3_runs(env):
    run_count = 3
    benchmark_specs = {"name": env.testName, "args": ['--run-count={}'.format(run_count)]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config) * run_count

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count)



# run each test on different env
def test_print_all_runs(env):
    run_count = 5
    benchmark_specs = {"name": env.testName, "args": ['--print-all-runs','--run-count={}'.format(run_count)]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config) * run_count

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count)

    json_filename = '{0}/mb.json'.format(config.results_dir)
    ## Assert that all BW metrics are properly stored and calculated
    with open(json_filename) as results_json:
        results_dict = json.load(results_json)
        print_all_runs = results_dict["configuration"]["print-all-runs"]
        env.assertTrue(print_all_runs)
        for run_count in range(1, run_count+1):
            # assert the run infomation exists
            env.assertTrue(f"RUN #{run_count} RESULTS" in results_dict)
        
        # ensure best, worst, and aggregate results are present
        env.assertTrue("BEST RUN RESULTS" in results_dict)
        env.assertTrue("WORST RUN RESULTS" in results_dict)
        env.assertTrue(f"AGGREGATED AVERAGE RESULTS ({run_count} runs)" in results_dict)
        # all stats should only exist on a single run json
        env.assertTrue("ALL STATS" not in results_dict)

def test_default_arbitrary_command_pubsub(env):
    benchmark_specs = {"name": env.testName, "args": []}
    addTLSArgs(benchmark_specs, env)
    # on arbitrary command args should be the last one
    benchmark_specs["args"].append('--command=publish \"__key__\" \"__data__\"')
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    if not benchmark.run():
        debugPrintMemtierOnError(config, env)


def test_default_arbitrary_command_keyless(env):
    benchmark_specs = {"name": env.testName, "args": []}
    addTLSArgs(benchmark_specs, env)
    # on arbitrary command args should be the last one
    benchmark_specs["args"].append('--command=PING')
    config = get_default_memtier_config(10,5,10000)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    if not benchmark.run():
        debugPrintMemtierOnError(config, env)

    json_filename = '{0}/mb.json'.format(config.results_dir)
    ## Assert that all BW metrics are properly stored and calculated
    with open(json_filename) as results_json:
        results_dict = json.load(results_json)
        metrics = results_dict['ALL STATS']['Pings']
        metrics_ts = results_dict['ALL STATS']['Pings']["Time-Serie"]
        totals_metrics_ts = results_dict['ALL STATS']['Totals']["Time-Serie"]
        for metric_name in ["KB/sec RX/TX","KB/sec RX","KB/sec TX","KB/sec"]:
            # assert the metric exists
            env.assertTrue(metric_name in metrics)
            # assert the metric value is non zero given we've had write and read
            metric_value_kbs = metrics[metric_name]
            env.assertTrue(metric_value_kbs > 0)

        totals_metrics_ts_v = list(totals_metrics_ts.values())
        for pos, second_data in enumerate(metrics_ts.values()):
            bytes_rx = second_data["Bytes RX"]
            bytes_tx = second_data["Bytes TX"]
            count = second_data["Count"]
            second_data_total = totals_metrics_ts_v[pos]
            for metric_name in ["p50.00","p99.00","p99.90"]:
                if count > 0:
                    metric_value_second_data = second_data[metric_name]
                    metric_value_totals_second_data = second_data_total[metric_name]
                    env.assertTrue(metric_value_totals_second_data == metric_value_second_data)
                    env.assertTrue(metric_value_second_data > 0.0)
            # if we had commands on that second the BW needs to be > 0
            if count > 0:
                env.assertTrue(bytes_rx >= 0)
                env.assertTrue(bytes_tx >= 0)


def test_default_arbitrary_command_set(env):
    benchmark_specs = {"name": env.testName, "args": []}
    addTLSArgs(benchmark_specs, env)
    # on arbitrary command args should be the last one
    benchmark_specs["args"].append('--command=SET __key__ __data__')
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count)


def test_default_arbitrary_command_hset(env):
    benchmark_specs = {"name": env.testName, "args": []}
    addTLSArgs(benchmark_specs, env)
    # on arbitrary command args should be the last one
    benchmark_specs["args"].append('--command=HSET __key__ field1 __data__')
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_hset': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count)


def test_default_arbitrary_command_hset_multi_data_placeholders(env):
    benchmark_specs = {"name": env.testName, "args": []}
    addTLSArgs(benchmark_specs, env)
    # on arbitrary command args should be the last one
    benchmark_specs["args"].append('--command=HSET __key__ field1 __data__ field2 __data__ field3 __data__')
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_hset': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count,
                                    overall_request_count)


def test_key_placeholder(env):
    env.skipOnCluster()
    run_count = 1
    benchmark_specs = {"name": env.testName, "args": ['--command=HSET __key__ f __data__']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config) * run_count

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_hset': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count,
                                    overall_request_count)


# key placeholder combined with other data
def test_key_placeholder_togetherwithdata(env):
    env.skipOnCluster()
    run_count = 1
    benchmark_specs = {"name": env.testName, "args": ['--command=SET \"prefix:__key__:suffix\" \"__data__\"']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=4, clients=1,requests=50)
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config) * run_count

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count,
                                    overall_request_count)

    # Ensure all keys have the correct prefix and suffix
    for conn in master_nodes_connections:
        for key in conn.scan_iter("*"):
            decoded_key = key.decode().split(":")
            env.assertEqual(decoded_key[0], "prefix")
            env.assertEqual(decoded_key[1].split("-")[0], "memtier")
            env.assertEqual(decoded_key[2], "suffix")


def test_default_set_get_rate_limited(env):
    env.skipOnCluster()
    master_nodes_list = env.getMasterNodesList()
    for client_count in [1,2,4]:
        for thread_count in [1,2]:
            rps_per_client = 100
            test_time_secs = 5
            overall_expected_rps = rps_per_client * client_count * thread_count * len(master_nodes_list)
            overall_expected_request_count = test_time_secs * overall_expected_rps
            # we give a 1 sec margin
            request_delta = overall_expected_rps
            # we will specify rate limit and the test time, which should help us get an approximate request count
            benchmark_specs = {"name": env.testName, "args": ['--rate-limiting={}'.format(rps_per_client)]}
            addTLSArgs(benchmark_specs, env)
            config = get_default_memtier_config(thread_count,client_count,None,test_time_secs)

            master_nodes_connections = env.getOSSMasterNodesConnectionList()

            # reset the commandstats
            for master_connection in master_nodes_connections:
                master_connection.execute_command("CONFIG", "RESETSTAT")

            add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

            # Create a temporary directory
            test_dir = tempfile.mkdtemp()

            config = RunConfig(test_dir, env.testName, config, {})
            ensure_clean_benchmark_folder(config.results_dir)

            benchmark = Benchmark.from_json(config, benchmark_specs)

            # benchmark.run() returns True if the return code of memtier_benchmark was 0
            memtier_ok = benchmark.run()

            master_nodes_connections = env.getOSSMasterNodesConnectionList()
            merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
            overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
            assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count, request_delta)


def test_rate_limited_completion_no_hang(env):
    """
    Verify rate-limited benchmarks complete without hanging.
    Runs multiple iterations to catch race conditions in cleanup logic.
    """
    master_nodes_list = env.getMasterNodesList()

    rps_per_client = 20
    test_time_secs = 5
    thread_count = 4
    client_count = 10
    iterations = 3
    timeout_per_iteration = test_time_secs + 20

    for iteration in range(1, iterations + 1):
        env.debugPrint(f"Running iteration {iteration}/{iterations}",True)
        benchmark_specs = {"name": f"{env.testName}_iter{iteration}", "args": ['--rate-limiting={}'.format(rps_per_client)]}
        addTLSArgs(benchmark_specs, env)
        config = get_default_memtier_config(thread_count, client_count, None, test_time_secs)

        add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

        test_dir = tempfile.mkdtemp()

        config = RunConfig(test_dir, f"{env.testName}_iter{iteration}", config, {})
        ensure_clean_benchmark_folder(config.results_dir)

        benchmark = Benchmark.from_json(config, benchmark_specs)

        start_time = time.time()
        try:
            process = subprocess.Popen(
                stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                executable=benchmark.binary, args=benchmark.args)
            _stdout, _stderr = process.communicate(timeout=timeout_per_iteration)
            exit_code = process.wait()
            elapsed_time = time.time() - start_time
            memtier_ok = (exit_code == 0)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            env.assertTrue(False, message=f"Benchmark hung on iteration {iteration} (timeout {timeout_per_iteration}s)")
            return
        env.debugPrint(f"Iteration {iteration} memtier exit code: {exit_code}",True)
        env.assertTrue(memtier_ok, message=f"Iteration {iteration} memtier exit code: {exit_code}")

        max_expected_time = test_time_secs + 10
        env.debugPrint(f"Iteration {iteration} took: {elapsed_time:.1f}s (pass < {max_expected_time}s)",True)
        env.assertTrue(elapsed_time < max_expected_time,
            message=f"Iteration {iteration} took: {elapsed_time:.1f}s (pass < {max_expected_time}s)")

        try:
            shutil.rmtree(test_dir)
        except:
            pass


def test_data_import(env):
    env.skipOnCluster()
    benchmark_specs = {"name": env.testName, "args": [f"--data-import={ROOT_FOLDER}/tests/data-import-2-keys.txt",'--ratio=1:1']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config,1, 2)
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    # reset the commandstats
    for master_connection in master_nodes_connections:
        master_connection.execute_command("CONFIG", "RESETSTAT")

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    assert_keyspace_range(env, 2, 1, master_nodes_connections)

    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count)


def test_data_import_setex(env):
    env.skipOnCluster()
    benchmark_specs = {"name": env.testName, "args": [f"--data-import={ROOT_FOLDER}/tests/data-import-2-keys-expiration.txt",'--ratio=1:1']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config,1, 2)
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    # reset the commandstats
    for master_connection in master_nodes_connections:
        master_connection.execute_command("CONFIG", "RESETSTAT")

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    assert_keyspace_range(env, 2, 1, master_nodes_connections)

    merged_command_stats = {'cmdstat_setex': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count)


def test_valid_json_using_debug_command(env):
    benchmark_specs = {"name": env.testName, "args": []}
    addTLSArgs(benchmark_specs, env)
    # on arbitrary command args should be the last one
    benchmark_specs["args"].append('--command=DEBUG SLEEP 2')
    total_requests = 3
    config = get_default_memtier_config(1,1,total_requests)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    if not benchmark.run():
        debugPrintMemtierOnError(config, env)

    ## Assert that all JSON BW metrics are properly stored and calculated
    json_filename = '{0}/mb.json'.format(config.results_dir)
    with open(json_filename) as results_json:
        # ensure it's a valid json
        results_dict = json.load(results_json)
        debug_metrics = results_dict['ALL STATS']['Debugs']
        debug_count = debug_metrics["Count"]
        total_metrics = results_dict['ALL STATS']['Totals']
        total_count = total_metrics["Count"]
        env.assertEqual(debug_count, total_count)
        env.assertEqual(debug_count, total_requests)
        debug_metrics_ts = debug_metrics["Time-Serie"]
      

        for second_data in debug_metrics_ts.values():
            count = second_data["Count"]
            # if we had commands on that second the BW needs to be > 0
            if count > 0:
                for latency_metric_name in ["Accumulated Latency","Min Latency","Max Latency","p50.00","p99.00","p99.90"]:
                    metric_value = second_data[latency_metric_name]
                    env.assertTrue(metric_value >= 0.0)


def test_uri_basic_connection(env):
    """Test basic URI connection functionality"""
    master_nodes_list = env.getMasterNodesList()
    master_node = master_nodes_list[0]

    # Build URI based on environment
    if env.isUnixSocket():
        # Skip URI test for Unix sockets as they don't use host:port
        return

    host = master_node.get('host', 'localhost')
    port = str(master_node.get('port', 6379))

    # Test basic redis:// URI
    uri = f"redis://{host}:{port}"
    benchmark_specs = {"name": env.testName, "args": [f'--uri={uri}', '--requests=100']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=100)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)

    # Verify the benchmark ran successfully
    env.assertTrue(memtier_ok)


def test_uri_with_database_selection(env):
    """Test URI with database selection functionality"""
    master_nodes_list = env.getMasterNodesList()
    master_node = master_nodes_list[0]

    # Build URI based on environment
    if env.isUnixSocket():
        # Skip URI test for Unix sockets as they don't use host:port
        return

    # Skip database selection test in cluster mode (only supports DB 0)
    if env.isCluster():
        return

    host = master_node.get('host', 'localhost')
    port = str(master_node.get('port', 6379))

    # Test URI with database selection
    uri_with_db = f"redis://{host}:{port}/1"
    benchmark_specs = {"name": env.testName, "args": [f'--uri={uri_with_db}', '--requests=50']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=50)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)

    # Verify the benchmark ran successfully
    env.assertTrue(memtier_ok)


def test_uri_ipv6_bracketed_host(env):
    """--uri accepts a bracketed IPv6 host, like redis-cli -u (issue #701)."""
    env.skipOnUnixSocket()
    master_nodes_list = env.getMasterNodesList()
    port = master_nodes_list[0]['port']
    if master_nodes_list[0].get('host') not in (None, 'localhost', '127.0.0.1'):
        env.skip()
    import socket
    try:
        socket.create_connection(('::1', port), timeout=2).close()
    except OSError:
        env.debugPrint('server not reachable on [::1]:{}, skipping'.format(port), force=True)
        env.skip()

    # Decoy --server/--port that the URI must override: if the bracketed host
    # or its port were dropped, memtier would try the decoy and fail.
    benchmark_specs = {"name": env.testName,
                       "args": ['--server=host-must-be-overridden.invalid', '--port=1',
                                '--uri=redis://[::1]:{}'.format(port), '--ratio=1:0', '--hide-histogram',
                                '--connection-stage-timeout=5']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=50)
    config['memtier_benchmark']['explicit_connect_args'] = True
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok)


def test_uri_authentication_ownership(env):
    """URI fields own storage; omitted fields retain CLI credentials and CLI/default hosts."""
    env.skipOnUnixSocket()
    env.skipOnVersionSmaller('6.0.0')  # Named users require Redis ACL support.
    nodes = env.getMasterNodesList()
    connections = env.getOSSMasterNodesConnectionList()
    user = 'memtier-uri-auth-ownership'
    password = 'uri-auth-password:with-colon'
    credentials = '{}:{}'.format(user, password)
    endpoint = '{}:{}'.format(nodes[0].get('host') or '127.0.0.1', nodes[0]['port'])
    uri = '--uri=redis://{}'.format(endpoint)
    authenticated_uri = '--uri=redis://{}@{}'.format(credentials, endpoint)
    auth = '--authenticate={}'.format(credentials)
    bad_auth = '--authenticate={}:incorrect'.format(user)
    empty_host_uri = '--uri=redis://:{}'.format(nodes[0]['port'])
    # Name each warning that should be emitted; omitted URI fields never override CLI values.
    cases = [
        ('wrong-named-user-password', [bad_auth], set()),
        ('cli-before-uri', [auth, uri], {'host', 'port'}),
        ('cli-after-uri', [uri, auth], {'host', 'port'}),
        ('uri-before-cli', [authenticated_uri, bad_auth], {'auth', 'host', 'port'}),
        ('uri-after-cli', [bad_auth, authenticated_uri], {'auth', 'host', 'port'}),
        ('auth-only', [auth], set()),
        ('repeated-auth', [bad_auth, '-a', credentials, uri], {'host', 'port'}),
        ('repeated-uri', [authenticated_uri, uri, auth], {'host', 'port'}),
        ('uri-only', [authenticated_uri], {'host', 'port'}),
        ('empty-host-cli-server', [auth, empty_host_uri], {'port'}),
        ('uri-no-cli-server', [auth, uri], set()),
        ('uri-localhost-cli-server', [auth, uri, '--server=localhost'], {'host', 'port'}),
        ('uri-omitted-port', [auth, '--uri=redis://' + endpoint.rsplit(':', 1)[0]], {'host'}),
        ('uri-empty-fields', [auth, '--uri=redis://'], set()),
        ('uri-db-overrides-cli', [auth, uri + '/0', '--select-db=1'], {'host', 'port', 'db'}),
        ('uri-db-only', [auth, uri + '/0'], {'host', 'port'}),
        ('uri-db-zero-cli', [auth, uri + '/0', '--select-db=0'], {'host', 'port'}),
    ]
    if nodes[0].get('host') in (None, 'localhost', '127.0.0.1'):
        cases.append(('empty-host-default-server', [auth, empty_host_uri, '--ipv4'], set()))
    if not env.isCluster():
        cases.extend([
            ('uri-omitted-db', [auth, uri, '--select-db=1'], {'host', 'port'}),
            ('uri-empty-db', [auth, uri + '/', '--select-db=1'], {'host', 'port'}),
        ])
    if env.useTLS:
        rediss_uri = uri.replace('redis://', 'rediss://', 1)
        cases.extend([
            ('rediss-with-cli-tls', [auth, rediss_uri], {'host', 'port', 'tls'}),
            ('rediss-without-cli-tls', [auth, rediss_uri], {'host', 'port'}),
        ])
    warning_options = {'auth': '--authenticate', 'host': '--host/--server',
                       'port': '--port', 'db': '--select-db', 'tls': '--tls'}
    created = []
    try:
        for connection in connections:
            connection.execute_command('ACL', 'SETUSER', user, 'reset', 'on',
                                       '>' + password, '~*', '+@all')
            created.append(connection)
        for name, args, expected_warnings in cases:
            specs = {'name': env.testName + '-' + name,
                     'args': args + ['--ratio=1:0', '--hide-histogram',
                                     '--connection-stage-timeout=5']}
            addTLSArgs(specs, env)
            if name == 'rediss-without-cli-tls':
                specs['args'].remove('--tls')
            config = get_default_memtier_config(threads=1, clients=1, requests=10)
            add_required_env_arguments(specs, config, env, nodes)
            if name in ('empty-host-default-server', 'uri-no-cli-server'):
                # The URI supplies the private port; keep Benchmark from
                # inserting an explicit --server argument.
                config['memtier_benchmark']['explicit_connect_args'] = True
            with tempfile.TemporaryDirectory() as directory:
                config = RunConfig(directory, specs['name'], config, {})
                ensure_clean_benchmark_folder(config.results_dir)
                benchmark = Benchmark.from_json(config, specs)
                ok = benchmark.run(timeout=20)
                with open(os.path.join(config.results_dir, 'mb.stderr')) as result:
                    stderr = result.read()
                for field, option in warning_options.items():
                    env.assertEqual('both URI and {} specified'.format(option) in stderr,
                                    field in expected_warnings, message=name + ':' + field)
                if name == 'wrong-named-user-password':
                    env.assertFalse(ok, message=name)
                    env.assertFalse(os.path.exists(os.path.join(config.results_dir, 'mb.timeout')))
                    env.assertContains('authentication failed', stderr)
                    continue
                if not ok:
                    debugPrintMemtierOnError(config, env)
                env.assertTrue(ok, message=name)
                if not ok:
                    continue
                with open(os.path.join(config.results_dir, 'mb.json')) as result:
                    stats = json.load(result)['ALL STATS']
                env.assertEqual(stats['Sets']['Count'], 10, message=name)
                env.assertEqual(stats['Totals']['Count'], 10, message=name)
    finally:
        for connection in created:
            connection.execute_command('ACL', 'DELUSER', user)


def test_uri_authentication_parse_errors(env):
    """Reject invalid URI fields even after URI credentials replace CLI auth."""
    for uri, error in [
        ('redis://user:password@127.0.0.1:invalid', 'invalid port number'),
        ('redis://password@127.0.0.1:1/invalid', 'invalid database number'),
        ('invalid://user:password@127.0.0.1:1', 'unsupported URI scheme'),
    ]:
        result = subprocess.run(
            [MEMTIER_BINARY, '--authenticate=cli-password', '--uri=' + uri],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        env.assertEqual(result.returncode, 1)
        env.assertContains(error, result.stderr.decode('utf-8', 'replace'))

    # An explicit default port is still a CLI value. Cluster DB validation
    # stops this parse-only check before any connection can be attempted.
    result = subprocess.run(
        [MEMTIER_BINARY, '--port=6379', '--cluster-mode', '--uri=redis://127.0.0.1:1/1'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
    env.assertEqual(result.returncode, 1)
    stderr = result.stderr.decode('utf-8', 'replace')
    env.assertContains('database selection not supported in cluster mode', stderr)
    env.assertContains('both URI and --port specified', stderr)


def test_uri_invalid_scheme(env):
    """Test URI with invalid scheme"""
    # Test invalid scheme
    benchmark_specs = {"name": env.testName, "args": ['--uri=invalid://localhost:6379', '--requests=1']}
    config = get_default_memtier_config(threads=1, clients=1, requests=1)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() should return False for invalid URI
    memtier_ok = benchmark.run()
    env.assertFalse(memtier_ok)


def test_uri_malformed(env):
    """Test malformed URI (missing scheme)"""
    # Test malformed URI (missing scheme)
    benchmark_specs = {"name": env.testName, "args": ['--uri=not-a-uri', '--requests=1']}
    config = get_default_memtier_config(threads=1, clients=1, requests=1)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() should return False for malformed URI
    memtier_ok = benchmark.run()
    env.assertFalse(memtier_ok)


def test_uri_invalid_port(env):
    """Test URI with invalid port number"""
    # Test invalid port number
    benchmark_specs = {"name": env.testName, "args": ['--uri=redis://localhost:99999', '--requests=1']}
    config = get_default_memtier_config(threads=1, clients=1, requests=1)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() should return False for invalid port
    memtier_ok = benchmark.run()
    env.assertFalse(memtier_ok)


def test_uri_invalid_database(env):
    """Test URI with invalid database number"""
    # Test invalid database number
    benchmark_specs = {"name": env.testName, "args": ['--uri=redis://localhost:6379/invalid', '--requests=1']}
    config = get_default_memtier_config(threads=1, clients=1, requests=1)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() should return False for invalid database number
    memtier_ok = benchmark.run()
    env.assertFalse(memtier_ok)


def test_interrupt_signal_handling(env):
    """Test that Ctrl+C (SIGINT) properly stops the benchmark and outputs correct statistics"""
    # Use a large number of requests so the test doesn't finish before we interrupt it
    benchmark_specs = {"name": env.testName, "args": ['--requests=1000000', '--hide-histogram']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=4, clients=50, requests=1000000)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # Start the benchmark process manually so we can send SIGINT
    import logging
    logging.debug('  Command: %s', ' '.join(benchmark.args))

    stderr_file = open(os.path.join(config.results_dir, 'mb.stderr'), 'wb')
    process = subprocess.Popen(
        stdin=None, stdout=subprocess.PIPE, stderr=stderr_file,
        executable=benchmark.binary, args=benchmark.args)

    # Wait 3 seconds then send SIGINT
    time.sleep(3)
    process.send_signal(signal.SIGINT)

    # Wait for process to finish
    _stdout, _ = process.communicate()
    stderr_file.close()

    # Write stdout to file
    benchmark.write_file('mb.stdout', _stdout)

    # Read stderr to check for interrupt message
    with open(os.path.join(config.results_dir, 'mb.stderr'), 'r') as stderr:
        stderr_content = stderr.read()
        # Check that the interrupt message is present and shows elapsed time
        env.assertTrue("Interrupted by user (Ctrl+C) after" in stderr_content)
        env.assertTrue("secs, stopping threads..." in stderr_content)

    # Check JSON output
    json_filename = '{0}/mb.json'.format(config.results_dir)
    env.assertTrue(os.path.isfile(json_filename))

    with open(json_filename) as results_json:
        results_dict = json.load(results_json)

        # Check that Runtime section exists and has Interrupted flag
        env.assertTrue("ALL STATS" in results_dict)
        env.assertTrue("Runtime" in results_dict["ALL STATS"])
        runtime = results_dict["ALL STATS"]["Runtime"]

        # Verify interrupted flag is set to "true"
        env.assertTrue("Interrupted" in runtime)
        env.assertEqual(runtime["Interrupted"], "true")

        # Verify duration is reasonable (should be around 3 seconds, give or take)
        env.assertTrue("Total duration" in runtime)
        duration_ms = runtime["Total duration"]
        env.assertTrue(duration_ms >= 2000)  # At least 2 seconds
        env.assertTrue(duration_ms <= 5000)  # At most 5 seconds

        # Verify that throughput metrics are NOT zero
        totals_metrics = results_dict["ALL STATS"]["Totals"]

        # Check ops/sec is not zero
        env.assertTrue("Ops/sec" in totals_metrics)
        total_ops_sec = totals_metrics["Ops/sec"]
        env.assertTrue(total_ops_sec > 0)

        # Check latency metrics are not zero
        env.assertTrue("Latency" in totals_metrics)
        total_latency = totals_metrics["Latency"]
        env.assertTrue(total_latency > 0)

        # Check that we actually processed some operations
        env.assertTrue("Count" in totals_metrics)
        total_count = totals_metrics["Count"]
        env.assertTrue(total_count > 0)


def _run_count_interrupted(env, test_time, signal_when, run_count=3, sig=signal.SIGINT, threads=2, clients=5,
                           extra_args=()):
    """Run --run-count=run_count with --print-all-runs and send it sig (SIGINT
    by default) once signal_when(stderr_text) is true.

    Returns (returncode, stderr text, seconds from the signal to exit, mb.json dict).
    """
    benchmark_specs = {"name": env.testName,
                       "args": ['--run-count={}'.format(run_count), '--print-all-runs', '--hide-histogram'] +
                       list(extra_args)}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=threads, clients=clients, requests=None, test_time=test_time)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)

    stderr_path = os.path.join(config.results_dir, 'mb.stderr')

    def read_stderr():
        with open(stderr_path, 'r', errors='replace') as fh:
            return fh.read()

    with open(stderr_path, 'wb') as stderr_file:
        process = subprocess.Popen(
            stdin=None, stdout=subprocess.PIPE, stderr=stderr_file,
            executable=benchmark.binary, args=benchmark.args)
        deadline = time.time() + 60
        while process.poll() is None and time.time() < deadline and not signal_when(read_stderr()):
            time.sleep(0.01)
        # The trigger must fire while memtier still runs; otherwise the test
        # never interrupted anything and its other checks are meaningless.
        env.assertTrue(process.poll() is None,
                       message="signal trigger did not fire before memtier exited: {}".format(read_stderr()))
        process.send_signal(sig)
        t_signal = time.time()
        try:
            _stdout, _ = process.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            _stdout, _ = process.communicate()
        elapsed = time.time() - t_signal
    benchmark.write_file('mb.stdout', _stdout)
    with open('{0}/mb.json'.format(config.results_dir)) as results_json:
        results_dict = json.load(results_json)
    return process.returncode, read_stderr(), elapsed, results_dict


def _assert_series_cut_after_run_1(env, rc, stderr_content, results_dict, run_1_interrupted):
    """run_1_interrupted: run 1's own flag ("true" if it saw the signal). BEST
    and WORST are run 1 and carry its flag; the cut series marks AGG "true"."""
    env.assertEqual(rc, 0, message=stderr_content)
    # No later run was launched ("[RUN #2]" and the "[RUN #2 ...%" progress).
    env.assertFalse("[RUN #2" in stderr_content, message=stderr_content)
    env.assertTrue("skipping the remaining runs (1 of 3 done)" in stderr_content, message=stderr_content)
    # The section keys follow --run-count; "Runs completed" says what they hold.
    env.assertEqual(results_dict.get("run information", {}).get("Runs completed"), 1,
                    message=results_dict.get("run information"))
    agg = "AGGREGATED AVERAGE RESULTS (3 runs)"
    for section, interrupted in (("RUN #1 RESULTS", run_1_interrupted), ("BEST RUN RESULTS", run_1_interrupted),
                                 ("WORST RUN RESULTS", run_1_interrupted), (agg, "true")):
        env.assertTrue(section in results_dict, message=list(results_dict))
        if section in results_dict:
            env.assertEqual(results_dict[section]["Runtime"]["Interrupted"], interrupted, message=section)
    if agg in results_dict and "BEST RUN RESULTS" in results_dict:
        # One run: the average is that run.
        best_ops = results_dict["BEST RUN RESULTS"]["Totals"]["Ops/sec"]
        agg_ops = results_dict[agg]["Totals"]["Ops/sec"]
        env.assertTrue(best_ops > 0 and abs(agg_ops - best_ops) <= 0.01 * best_ops,
                       message="AGG Ops/sec {} vs BEST {}".format(agg_ops, best_ops))
    if agg in results_dict and "RUN #1 RESULTS" in results_dict:
        # #703: the AGGREGATED counts are per-run means, integers like the
        # run's own section, also when only one run completed.
        run_1 = results_dict["RUN #1 RESULTS"]
        for path in (("Totals", "Count"), ("Totals", "Accumulated Latency"),
                     ("Totals", "Connection Errors"), ("Runtime", "Unanswered requests")):
            value = results_dict[agg][path[0]][path[1]]
            own = run_1[path[0]][path[1]]
            env.assertTrue(isinstance(value, int), message="{} {}: {!r}".format(agg, path, value))
            # Totals Accumulated Latency is the sum of the rows, each of
            # them truncated to the ms, so it can be a little below RUN #1's.
            slack = 3 if path[1] == "Accumulated Latency" else 0
            env.assertTrue(abs(value - own) <= slack, message="{} {}: {} vs RUN #1 {}".format(agg, path, value, own))
        env.assertEqual(results_dict[agg]["Totals"]["Count"], run_1["Totals"]["Count"])


def test_interrupt_stops_run_count(env):
    """Ctrl+C stops the whole --run-count series, not only the current run (#690).

    Before the fix every remaining run started anyway, sent about 1 s of load,
    and was counted in BEST/WORST/AGGREGATED. Now memtier exits after the
    interrupted run, and the summary covers only that run.
    """
    started = time.time()
    rc, stderr_content, elapsed, results_dict = _run_count_interrupted(
        env, 30, lambda _err: time.time() - started >= 1.5)
    _assert_series_cut_after_run_1(env, rc, stderr_content, results_dict, "true")
    env.assertEqual(stderr_content.count("Interrupted by user (Ctrl+C) after"), 1, message=stderr_content)
    # Sanity bound: each skipped run used to add a 1 s pause plus 1 s of load
    # (>= 4 s here); with the fix the exit takes about 1 s at most.
    env.assertTrue(elapsed < 3.5, message="exit took {:.1f}s after SIGINT".format(elapsed))


def test_interrupt_between_runs_stops_run_count(env):
    """Ctrl+C in the settle pause after run 1 starts no further run (#690).

    Run 1 (--test-time=1) completes on its own, so no run sees the signal:
    RUN #1 (and BEST/WORST, which are run 1) stay Interrupted "false", but the
    series is cut short, so the AGGREGATED section says Interrupted "true".
    """
    seen = []

    def run_1_done(err):
        # The last progress line of run 1 reports 0 threads; main() then
        # sleeps 1 s before run 2. Wait 0.4 s more so the signal lands inside
        # that pause, not in the check just before it. Progress can exceed
        # 100% on a slow (e.g. TSAN) build, so match any percentage.
        if not seen and re.search(r"\[RUN #1 \d+%[^\r\n]*\b0 threads", err):
            seen.append(time.time())
        return bool(seen) and time.time() - seen[0] >= 0.4

    rc, stderr_content, _elapsed, results_dict = _run_count_interrupted(env, 1, run_1_done)
    _assert_series_cut_after_run_1(env, rc, stderr_content, results_dict, "false")


def test_interrupt_last_run_marks_aggregate(env):
    """Ctrl+C during the last run marks the aggregate Interrupted (#690).

    Every requested run happened, so only the interrupted run's own flag can
    carry Interrupted into the AGGREGATED section (aggregate_average()).
    """
    seen = []

    def run_2_started(err):
        if not seen and "[RUN #2" in err:
            seen.append(time.time())
        return bool(seen) and time.time() - seen[0] >= 0.5

    rc, stderr_content, _elapsed, results_dict = _run_count_interrupted(env, 2, run_2_started, run_count=2)
    env.assertEqual(rc, 0, message=stderr_content)
    env.assertEqual(stderr_content.count("Interrupted by user (Ctrl+C) after"), 1, message=stderr_content)
    env.assertFalse("skipping the remaining runs" in stderr_content, message=stderr_content)
    env.assertEqual(results_dict.get("run information", {}).get("Runs completed"), 2,
                    message=results_dict.get("run information"))
    for section, interrupted in (("RUN #1 RESULTS", "false"), ("RUN #2 RESULTS", "true"),
                                 ("AGGREGATED AVERAGE RESULTS (2 runs)", "true")):
        env.assertTrue(section in results_dict, message=list(results_dict))
        if section in results_dict:
            env.assertEqual(results_dict[section]["Runtime"]["Interrupted"], interrupted, message=section)


def test_interrupt_does_not_race_worker_stats(env):
    """Ctrl+C does not touch the clients' stats while the workers run (#710).

    client_group::interrupt() used to finalize every client's run_stats on the
    main thread right after asking the worker loops to stop, while the
    workers could still be recording replies into the same objects (a data
    race; a double free under ASan). Now the end times are stamped after the
    workers are joined. A TSAN build reports the race (and exits 66) in
    about half the interrupts, hence several runs; any build checks the
    interrupted results.
    """
    for attempt in range(3):
        started = time.time()
        rc, stderr_content, _elapsed, results_dict = _run_count_interrupted(
            env, 30, lambda _err: time.time() - started >= 2, run_count=1,
            threads=4, clients=10, extra_args=['--pipeline=16'])
        env.assertFalse("ThreadSanitizer" in stderr_content, message=stderr_content)
        env.assertFalse("AddressSanitizer" in stderr_content, message=stderr_content)
        env.assertEqual(rc, 0, message=stderr_content)
        env.assertEqual(stderr_content.count("Interrupted by user (Ctrl+C) after"), 1, message=stderr_content)
        runtime = results_dict["ALL STATS"]["Runtime"]
        env.assertEqual(runtime["Interrupted"], "true", message=attempt)
        # The end time is the interrupt time, not the (later) join.
        env.assertTrue(1000 <= runtime["Total duration"] <= 6000, message=runtime)
        env.assertTrue(results_dict["ALL STATS"]["Totals"]["Count"] > 0, message=attempt)
        # One failed attempt is enough; the others would only repeat the
        # same TSAN report in the log.
        if rc != 0:
            break


def test_cluster_seed_owns_no_keys(env):
    """Reads must reach their shard when the -p seed owns none of the keys (#610).

    The seed connects first. Its GETs used to find the owning shard still
    connecting, get no route, and retry inside fill_pipeline without ever
    returning to the event loop, so the other shards never finished
    connecting: 0 ops at 100% CPU.
    """
    if not env.isCluster() or env.isUnixSocket():
        env.skip()
        return

    master_nodes_list = env.getMasterNodesList()
    any_master = env.getOSSMasterNodesConnectionList()[0]
    key = 'memtier-4'
    slot = int(any_master.execute_command('CLUSTER', 'KEYSLOT', key))
    owner_port = None
    for entry in any_master.execute_command('CLUSTER', 'SLOTS'):
        if int(entry[0]) <= slot <= int(entry[1]):
            owner_port = int(entry[2][1])
    env.assertNotEqual(owner_port, None)
    seed_ports = [node['port'] for node in master_nodes_list if node['port'] != owner_port]
    if not seed_ports:
        env.skip()
        return

    requests = 200
    cases = [
        ('get', ['--ratio=0:1'], requests),
        ('set_get', ['--ratio=1:1'], requests),
        ('command_get', ['--command=GET __key__'], requests),
        # Without --requests only the in-flight budget in hold_pipeline()
        # bounds what the seed queues for the owner while it connects.
        ('get_test_time', ['--ratio=0:1'], None),
    ]
    for name, args, case_requests in cases:
        benchmark_specs = {'name': env.testName + '-' + name,
                           'args': args + ['--key-minimum=4', '--key-maximum=4', '--hide-histogram']}
        addTLSArgs(benchmark_specs, env)
        if case_requests is None:
            config = get_default_memtier_config(threads=None, clients=None, requests=None, test_time=3)
        else:
            config = get_default_memtier_config(threads=1, clients=1, requests=case_requests)
        add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
        config['redis_process_port'] = seed_ports[0]

        test_dir = tempfile.mkdtemp()
        config = RunConfig(test_dir, benchmark_specs['name'], config, {})
        ensure_clean_benchmark_folder(config.results_dir)
        benchmark = Benchmark.from_json(config, benchmark_specs)

        ok = benchmark.run(timeout=20)
        if not ok:
            debugPrintMemtierOnError(config, env)
        env.assertTrue(ok, message=name)
        env.assertFalse(os.path.exists(os.path.join(config.results_dir, 'mb.timeout')), message=name)
        if not ok:
            continue
        with open(os.path.join(config.results_dir, 'mb.json')) as results_json:
            stats = json.load(results_json)['ALL STATS']
        if case_requests is None:
            env.assertGreater(stats['Totals']['Count'], 0, message=name)
        else:
            env.assertEqual(stats['Totals']['Count'], case_requests, message=name)
        env.assertEqual(stats['Totals']['MOVED/sec'], 0, message=name)


def test_sigterm_closes_json(env):
    """SIGTERM stops the run like Ctrl+C and leaves a valid --json-out-file (#704).

    SIGTERM (kill, timeout, docker stop, CI runners) used to have no handler:
    the process died with only the configuration section written, so the
    JSON file was truncated and could not be parsed.
    """
    benchmark_specs = {"name": env.testName, "args": ['--hide-histogram']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=2, requests=None, test_time=60)
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)

    stderr_path = os.path.join(config.results_dir, 'mb.stderr')
    with open(stderr_path, 'wb') as stderr_file:
        process = subprocess.Popen(stdin=None, stdout=subprocess.PIPE, stderr=stderr_file,
                                   executable=benchmark.binary, args=benchmark.args)
        time.sleep(2)
        process.send_signal(signal.SIGTERM)
        try:
            _stdout, _ = process.communicate(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            _stdout, _ = process.communicate()
            env.assertTrue(False, message="memtier did not exit within 20s of SIGTERM")
    benchmark.write_file('mb.stdout', _stdout)

    # The default action would end the process by the signal (-SIGTERM).
    env.assertEqual(process.returncode, 0)
    with open(stderr_path) as stderr:
        env.assertContains("Interrupted by SIGTERM after", stderr.read())

    with open(os.path.join(config.results_dir, 'mb.json')) as results_json:
        runtime = json.load(results_json)["ALL STATS"]["Runtime"]
    env.assertEqual(runtime["Interrupted"], "true")
    env.assertLess(runtime["Total duration"], 10000)


def test_sigterm_stops_run_count(env):
    """SIGTERM during run 1 stops the --run-count series like Ctrl+C (#704)."""
    started = time.time()
    rc, stderr_content, _elapsed, results_dict = _run_count_interrupted(
        env, 30, lambda _err: time.time() - started >= 1.5, sig=signal.SIGTERM)
    _assert_series_cut_after_run_1(env, rc, stderr_content, results_dict, "true")
    env.assertEqual(stderr_content.count("Interrupted by SIGTERM after"), 1, message=stderr_content)
    env.assertContains("Interrupted by SIGTERM, skipping the remaining runs (1 of 3 done)", stderr_content)


def test_sigterm_between_runs_stops_run_count(env):
    """SIGTERM in the settle pause after run 1 starts no further run (#704)."""
    seen = []

    def run_1_done(err):
        # Same trigger as test_interrupt_between_runs_stops_run_count.
        if not seen and re.search(r"\[RUN #1 \d+%[^\r\n]*\b0 threads", err):
            seen.append(time.time())
        return bool(seen) and time.time() - seen[0] >= 0.4

    rc, stderr_content, _elapsed, results_dict = _run_count_interrupted(env, 1, run_1_done, sig=signal.SIGTERM)
    _assert_series_cut_after_run_1(env, rc, stderr_content, results_dict, "false")
    env.assertFalse("Interrupted by SIGTERM after" in stderr_content, message=stderr_content)
    env.assertContains("Interrupted by SIGTERM, skipping the remaining runs (1 of 3 done)", stderr_content)


def test_second_signal_terminates(env):
    """A second SIGTERM ends memtier at once when the graceful stop is stuck (#704).

    memtier opens --json-out-file at startup, so a FIFO with no reader holds
    it in open(). The first SIGTERM only sets the interrupt flag (open() is
    restarted), so memtier stays up; the second one, sent 1.5 s later (past
    the 1 s window that coalesces duplicates), must kill it.
    """
    benchmark_specs = {"name": env.testName, "args": ['--hide-histogram']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=None, test_time=30)
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    fifo = os.path.join(config.results_dir, 'mb.json')
    if os.path.exists(fifo):
        os.remove(fifo)
    os.mkfifo(fifo)

    process = subprocess.Popen(stdin=None, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               executable=benchmark.binary, args=benchmark.args)
    try:
        time.sleep(1)
        process.send_signal(signal.SIGTERM)
        time.sleep(1.5)
        env.assertTrue(process.poll() is None, message="first SIGTERM killed memtier (rc {})".format(process.poll()))
        process.send_signal(signal.SIGTERM)
        try:
            rc = process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            rc = None
        env.assertEqual(rc, -signal.SIGTERM)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        os.remove(fifo)


def test_duplicate_signal_counts_once(env):
    """Signals within 1 s of the first are one interrupt, not an escalation (#704).

    Ctrl+C on `timeout ... memtier` delivers SIGINT twice (terminal and
    timeout), and a process-group kill does the same. memtier must still stop
    gracefully: rc 0 and a valid --json-out-file marked Interrupted.
    """
    cases = [("direct-2x-SIGTERM", False, signal.SIGTERM)]
    if shutil.which("timeout"):
        cases += [("timeout-group-SIGINT", True, signal.SIGINT), ("timeout-group-SIGTERM", True, signal.SIGTERM)]
    for name, under_timeout, sig in cases:
        benchmark_specs = {"name": env.testName + '-' + name, "args": ['--hide-histogram']}
        addTLSArgs(benchmark_specs, env)
        config = get_default_memtier_config(threads=1, clients=2, requests=None, test_time=60)
        add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
        test_dir = tempfile.mkdtemp()
        config = RunConfig(test_dir, benchmark_specs['name'], config, {})
        ensure_clean_benchmark_folder(config.results_dir)
        benchmark = Benchmark.from_json(config, benchmark_specs)

        args = list(benchmark.args)
        if under_timeout:
            args = ["timeout", "100"] + args
        process = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                   start_new_session=True)
        try:
            time.sleep(2)
            if under_timeout:
                os.killpg(process.pid, sig)
            else:
                process.send_signal(sig)
                process.send_signal(sig)
            try:
                rc = process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                rc = None
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        env.assertEqual(rc, 0, message=name)
        try:
            with open(os.path.join(config.results_dir, 'mb.json')) as results_json:
                interrupted = json.load(results_json)["ALL STATS"]["Runtime"]["Interrupted"]
        except ValueError as e:
            interrupted = "invalid JSON: {}".format(e)
        env.assertEqual(interrupted, "true", message=name)


def test_json_out_file_unwritable(env):
    """An --json-out-file that cannot be opened is a clean startup error.

    json_handler used to print the fopen() error and go on writing to a NULL
    FILE *, so memtier crashed with SIGSEGV. Under TSAN a signal during a
    blocked fopen() (EINTR) took the same path.
    """
    benchmark_specs = {"name": env.testName,
                       "args": ['--json-out-file=/nonexistent-memtier-dir/mb.json', '--hide-histogram']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=10)
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)

    process = subprocess.Popen(benchmark.args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        _, stderr = process.communicate(timeout=20)
    except subprocess.TimeoutExpired:
        process.kill()
        _, stderr = process.communicate()
    stderr = stderr.decode(errors='replace')
    env.assertEqual(process.returncode, 1, message=stderr)
    env.assertContains("error: cannot open --json-out-file /nonexistent-memtier-dir/mb.json", stderr)


def test_json_out_file_unwritable_keeps_out_file(env):
    """An unwritable --json-out-file must not wipe an existing --out-file.

    --out-file is opened without truncation and only truncated once the
    --json-out-file has been opened as well.
    """
    benchmark_specs = {"name": env.testName,
                       "args": ['--json-out-file=/nonexistent-memtier-dir/mb.json', '--hide-histogram']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=10)
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    out_path = os.path.join(config.results_dir, 'mb.stdout')
    out_before = 'previous results\n'
    with open(out_path, 'w') as f:
        f.write(out_before)
    benchmark.args += ['--out-file', out_path]

    process = subprocess.Popen(benchmark.args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        _, stderr = process.communicate(timeout=20)
    except subprocess.TimeoutExpired:
        process.kill()
        _, stderr = process.communicate()
    stderr = stderr.decode(errors='replace')
    env.assertEqual(process.returncode, 1, message=stderr)
    env.assertContains("error: cannot open --json-out-file /nonexistent-memtier-dir/mb.json", stderr)
    with open(out_path) as f:
        env.assertEqual(f.read(), out_before)


def test_out_file_replaces_existing(env):
    """A successful run replaces a longer existing --out-file completely."""
    benchmark_specs = {"name": env.testName, "args": ['--hide-histogram']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=10)
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    out_path = os.path.join(config.results_dir, 'mb.stdout')
    with open(out_path, 'w') as f:
        f.write('stale\n' * 100000)
    benchmark.args += ['--out-file', out_path]

    process = subprocess.run(benchmark.args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=60)
    stderr = process.stderr.decode(errors='replace')
    env.assertEqual(process.returncode, 0, message=stderr)
    # The JSON file is opened before the results file is truncated, but the
    # stderr notes keep their old order.
    env.assertLess(stderr.index("Json file "), stderr.index("Writing results to " + out_path))
    with open(out_path) as f:
        out = f.read()
    env.assertNotContains('stale', out)
    env.assertContains('ALL STATS', out)


def test_out_file_unwritable(env):
    """An --out-file that cannot be opened is a clean startup error (#716).

    main() used to print the fopen() error, run the whole benchmark and then
    write the results to a NULL FILE *, so memtier crashed with SIGSEGV. The
    run is time-bounded (60 s) so a regression shows up as a crash or timeout
    rather than a quick exit; no benchmark thread may start. The check runs
    before the --json-out-file is opened, so an existing JSON file is left
    untouched.
    """
    benchmark_specs = {"name": env.testName,
                       "args": ['--out-file=/nonexistent-memtier-dir/mb.stdout', '--hide-histogram']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=None, test_time=60)
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    json_path = os.path.join(config.results_dir, 'mb.json')
    json_before = '{"previous": "run"}\n'
    with open(json_path, 'w') as f:
        f.write(json_before)
    benchmark.args += ['--json-out-file', json_path]

    start = time.monotonic()
    process = subprocess.Popen(benchmark.args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        _, stderr = process.communicate(timeout=20)
    except subprocess.TimeoutExpired:
        process.kill()
        _, stderr = process.communicate()
    elapsed = time.monotonic() - start
    stderr = stderr.decode(errors='replace')
    env.assertEqual(process.returncode, 1, message=stderr)
    env.assertContains("error: cannot open --out-file /nonexistent-memtier-dir/mb.stdout: "
                       "No such file or directory", stderr)
    env.assertNotContains("Launching threads", stderr)
    env.assertLess(elapsed, 20)
    with open(json_path) as f:
        env.assertEqual(f.read(), json_before)


def _startup_error_keeps_output_files(env, extra_args, expected_rc, expected_error):
    """A startup validation error must leave existing output files untouched.

    The --out-file and --json-out-file are opened only after all option
    validation, so a run that never starts does not truncate them or leave
    an unterminated JSON file behind (#718).
    """
    benchmark_specs = {"name": env.testName, "args": ['--hide-histogram'] + extra_args}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=10)
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    out_path = os.path.join(config.results_dir, 'mb.stdout')
    json_path = os.path.join(config.results_dir, 'mb.json')
    out_before = 'previous results\n'
    json_before = '{"previous": "run"}\n'
    with open(out_path, 'w') as f:
        f.write(out_before)
    with open(json_path, 'w') as f:
        f.write(json_before)
    benchmark.args += ['--out-file', out_path, '--json-out-file', json_path]

    process = subprocess.Popen(benchmark.args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        _, stderr = process.communicate(timeout=20)
    except subprocess.TimeoutExpired:
        process.kill()
        _, stderr = process.communicate()
    stderr = stderr.decode(errors='replace')
    env.assertEqual(process.returncode, expected_rc, message=stderr)
    env.assertContains(expected_error, stderr)
    env.assertNotContains("Launching threads", stderr)
    with open(out_path) as f:
        env.assertEqual(f.read(), out_before)
    with open(json_path) as f:
        env.assertEqual(f.read(), json_before)


def test_startup_error_keeps_output_files_data_size(env):
    _startup_error_keeps_output_files(env, ['--data-size', '10', '--data-size-list', '10:1'], 2,
                                      "error: data-size cannot be used with data-size-list or data-size-range.")


def test_startup_error_keeps_output_files_select_db(env):
    # --cluster-mode (added for cluster envs) rejects the protocol first.
    expected = ("error: cluster mode supported only in redis protocol." if env.isCluster()
                else "error: select-db can only be used with redis protocol.")
    _startup_error_keeps_output_files(env, ['-P', 'memcache_text', '--select-db', '1'], 2, expected)


def test_startup_error_keeps_output_files_data_import(env):
    # --cluster-mode (added for cluster envs) is rejected with --data-import first.
    expected = ("error: Cluster mode cannot be specified when importing." if env.isCluster()
                else "error: failed to read keys.")
    _startup_error_keeps_output_files(env, ['--data-import', '/nonexistent-memtier-dir/keys.imp'], 1, expected)


def _start_memtier(env, work_dir, extra_args, stdout, drop_default_out_file=False, prefix=None, requests=10,
                   test_time=None, preexec_fn=None):
    """Start a short benchmark whose output goes to extra_args/stdout; return the Popen.

    The results and memtier's stderr (work_dir/memtier.stderr) go under
    work_dir. prefix (e.g. ['stdbuf', '-o0']) is put in front of the memtier
    command.
    """
    benchmark_specs = {"name": env.testName, "args": ['--hide-histogram'] + extra_args}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=requests, test_time=test_time)
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
    config = RunConfig(work_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    args = list(benchmark.args)
    if drop_default_out_file:
        # mb.py always passes --out-file; without it the results go to stdout.
        i = args.index('--out-file')
        del args[i:i + 2]
    proc_env = None
    if prefix:
        args = prefix + args
        # stdbuf works by LD_PRELOAD; let an ASan build accept that.
        proc_env = dict(os.environ)
        proc_env['ASAN_OPTIONS'] = (proc_env.get('ASAN_OPTIONS', '') + ':verify_asan_link_order=0').lstrip(':')
    with open(_memtier_stderr_path(work_dir), 'wb') as stderr:
        return subprocess.Popen(args, stdout=stdout, stderr=stderr, env=proc_env, preexec_fn=preexec_fn)


def _memtier_stderr_path(work_dir):
    return os.path.join(work_dir, 'memtier.stderr')


def _read_memtier_stderr(work_dir):
    with open(_memtier_stderr_path(work_dir), 'rb') as f:
        return f.read().decode(errors='replace')


def _finish_memtier(process, work_dir, timeout=30):
    """Wait for a _start_memtier() process; return (rc, stderr)."""
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    return process.returncode, _read_memtier_stderr(work_dir)


def _run_with_unwritable_output(env, extra_args, stdout, drop_default_out_file=False, prefix=None, **kwargs):
    """Run a short benchmark whose output goes to extra_args/stdout; return (rc, stderr)."""
    with tempfile.TemporaryDirectory() as work_dir:
        process = _start_memtier(env, work_dir, extra_args, stdout, drop_default_out_file, prefix, **kwargs)
        return _finish_memtier(process, work_dir)


def _closed_pipe_run(env, extra_args, prefix=None):
    """Run with stdout on a pipe whose read end is already closed; return (rc, stderr)."""
    rfd, wfd = os.pipe()
    os.close(rfd)
    try:
        return _run_with_unwritable_output(env, extra_args, wfd, drop_default_out_file=True, prefix=prefix)
    finally:
        os.close(wfd)


def test_out_file_write_error(env):
    """A --out-file that cannot be written fails the run (#719).

    /dev/full accepts the open and fails every write with ENOSPC, like a full
    disk. memtier used to ignore the failed writes and fclose() and exit 0.
    """
    if not os.path.exists('/dev/full'):
        env.skip()
        return
    rc, stderr = _run_with_unwritable_output(env, ['--out-file', '/dev/full'], subprocess.DEVNULL)
    env.assertEqual(rc, 1, message=stderr)
    env.assertContains("error: failed writing --out-file /dev/full: No space left on device", stderr)


def test_json_out_file_write_error(env):
    """A --json-out-file that cannot be written fails the run (#719)."""
    if not os.path.exists('/dev/full'):
        env.skip()
        return
    rc, stderr = _run_with_unwritable_output(env, ['--json-out-file', '/dev/full'], subprocess.DEVNULL)
    env.assertEqual(rc, 1, message=stderr)
    env.assertContains("error: failed writing --json-out-file /dev/full: No space left on device", stderr)
    env.assertNotContains("Json file closed.", stderr)


def test_stdout_write_error(env):
    """Results on stdout that cannot be written fail the run (#719)."""
    if not os.path.exists('/dev/full'):
        env.skip()
        return
    with open('/dev/full', 'w') as full:
        rc, stderr = _run_with_unwritable_output(env, [], full, drop_default_out_file=True)
    env.assertEqual(rc, 1, message=stderr)
    env.assertContains("error: failed writing results to stdout: No space left on device", stderr)


def test_connection_stage_abort_keeps_rc_2_on_write_error(env):
    """A connection-stage abort exits 2 even when the output write fails (#719).

    The output error is still reported. A wrong password makes every
    connection fail AUTH, so --connection-stage-timeout=1 aborts the run.
    """
    if not os.path.exists('/dev/full'):
        env.skip()
        return
    rc, stderr = _run_with_unwritable_output(
        env, ['--authenticate', 'memtier-719-wrong-password', '--connection-stage-timeout=1',
              '--out-file', '/dev/full'],
        subprocess.DEVNULL, requests=None, test_time=20)
    env.assertEqual(rc, 2, message=stderr)
    env.assertContains("aborting after", stderr)
    env.assertContains("error: failed writing --out-file /dev/full: No space left on device", stderr)


def _symlink_to_dev_full(directory, names):
    """Make each of names in directory a symlink to /dev/full; return their paths."""
    paths = [os.path.join(directory, name) for name in names]
    for path in paths:
        os.symlink('/dev/full', path)
    return paths


def test_hdr_file_write_error(env):
    """--hdr-file-prefix files that cannot be written fail the run (#744).

    The names are built from the prefix, so a symlink to /dev/full stands in
    for each one. This covers the .txt and .hgrm files of the full run and
    of a command; the other files are still written.
    """
    if not os.path.exists('/dev/full'):
        env.skip()
        return
    with tempfile.TemporaryDirectory() as hdr_dir:
        prefix = os.path.join(hdr_dir, 'h')
        full = _symlink_to_dev_full(hdr_dir, ['h_FULL_RUN_1.txt', 'h_FULL_RUN_1.hgrm',
                                              'h_SET_command_run_1.txt', 'h_GET_command_run_1.hgrm'])
        rc, stderr = _run_with_unwritable_output(env, ['--hdr-file-prefix', prefix], subprocess.DEVNULL)
        env.assertEqual(rc, 1, message=stderr)
        for path in full:
            env.assertContains("error: failed writing --hdr-file-prefix file {}: ".format(path), stderr)
        for name in ['h_SET_command_run_1.hgrm', 'h_GET_command_run_1.txt']:
            env.assertGreater(os.path.getsize(os.path.join(hdr_dir, name)), 0)


def _check_one_hdr_file_fails(env, extra_args, name):
    """Only the HDR file name fails (a /dev/full symlink); the run must still exit 1."""
    with tempfile.TemporaryDirectory() as hdr_dir:
        prefix = os.path.join(hdr_dir, 'h')
        path, = _symlink_to_dev_full(hdr_dir, [name])
        rc, stderr = _run_with_unwritable_output(env, ['--hdr-file-prefix', prefix] + extra_args,
                                                 subprocess.DEVNULL)
        env.assertEqual(rc, 1, message=name + ': ' + stderr)
        env.assertEqual(stderr.count("error: failed writing --hdr-file-prefix file"), 1, message=stderr)
        env.assertContains("error: failed writing --hdr-file-prefix file {}: ".format(path), stderr)


# One test per HDR writer, failing exactly one file at a time: the .txt
# (first) and the .hgrm (second) file of each writer must both fail the run.

def test_hdr_full_run_file_write_error(env):
    """Only one full-run HDR file fails: the run exits 1 (#744)."""
    if not os.path.exists('/dev/full'):
        env.skip()
        return
    for name in ['h_FULL_RUN_1.txt', 'h_FULL_RUN_1.hgrm']:
        _check_one_hdr_file_fails(env, [], name)


def test_hdr_set_command_file_write_error(env):
    """Only one SET HDR file fails: the run exits 1 (#744)."""
    if not os.path.exists('/dev/full'):
        env.skip()
        return
    for name in ['h_SET_command_run_1.txt', 'h_SET_command_run_1.hgrm']:
        _check_one_hdr_file_fails(env, [], name)


def test_hdr_get_command_file_write_error(env):
    """Only one GET HDR file fails: the run exits 1 (#744)."""
    if not os.path.exists('/dev/full'):
        env.skip()
        return
    for name in ['h_GET_command_run_1.txt', 'h_GET_command_run_1.hgrm']:
        _check_one_hdr_file_fails(env, [], name)


def test_hdr_arbitrary_command_file_write_error(env):
    """Only one HDR file of a --command fails: the run exits 1 (#744).

    The second command's files are used, so a failure that a later
    command's success would hide is caught too.
    """
    if not os.path.exists('/dev/full'):
        env.skip()
        return
    commands = ['--command', 'SET __key__ __data__', '--command', 'GET __key__']
    for name in ['h_SET_command_run_1.txt', 'h_GET_command_run_1.txt', 'h_GET_command_run_1.hgrm']:
        _check_one_hdr_file_fails(env, commands, name)


def test_client_stats_write_error(env):
    """A --client-stats CSV that cannot be written fails the run (#744)."""
    if not os.path.exists('/dev/full'):
        env.skip()
        return
    with tempfile.TemporaryDirectory() as stats_dir:
        csv, = _symlink_to_dev_full(stats_dir, ['cs-1-0-0.csv'])
        rc, stderr = _run_with_unwritable_output(env, ['--client-stats', os.path.join(stats_dir, 'cs')],
                                                 subprocess.DEVNULL)
        env.assertEqual(rc, 1, message=stderr)
        env.assertContains("error: failed writing --client-stats file {}: No space left on device".format(csv),
                           stderr)


def test_hdr_file_prefix_missing_directory(env):
    """An --hdr-file-prefix in a missing directory fails at startup (#744)."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        missing = os.path.join(tmp_dir, 'missing')
        rc, stderr = _run_with_unwritable_output(env, ['--hdr-file-prefix', os.path.join(missing, 'h')],
                                                 subprocess.DEVNULL)
        env.assertEqual(rc, 1, message=stderr)
        env.assertContains("error: cannot write --hdr-file-prefix directory {}: No such file or directory"
                           .format(missing), stderr)
        env.assertNotContains("Preparing benchmark client", stderr)


def test_client_stats_missing_directory(env):
    """A --client-stats prefix in a missing directory fails at startup (#744)."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        missing = os.path.join(tmp_dir, 'missing')
        rc, stderr = _run_with_unwritable_output(env, ['--client-stats', os.path.join(missing, 'cs')],
                                                 subprocess.DEVNULL)
        env.assertEqual(rc, 1, message=stderr)
        env.assertContains("error: cannot write --client-stats directory {}: No such file or directory"
                           .format(missing), stderr)
        env.assertNotContains("Preparing benchmark client", stderr)


def test_verify_only_skips_output_prefix_dir_check(env):
    """--verify-only writes no HDR or --client-stats files, so their directories are not checked (#744)."""
    if env.isCluster():  # --data-import (needed by --verify-only) is standalone only
        env.skip()
        return
    with tempfile.TemporaryDirectory() as tmp_dir:
        missing = os.path.join(tmp_dir, 'missing')
        data_import = os.path.join(tmp_dir, 'import.csv')
        with open(data_import, 'w') as f:
            f.write('dumpflags, time, exptime, nbytes, nsuffix, it_flags, clsid, nkey, key, data\n'
                    '0, 0, 0, 7, 1, 0, 0, 3, "key", "value"\n')
        rc, stderr = _run_with_unwritable_output(
            env, ['--data-import', data_import, '--data-verify', '--verify-only',
                  '--hdr-file-prefix', os.path.join(missing, 'h'), '--client-stats', os.path.join(missing, 'cs')],
            subprocess.DEVNULL)
        # Past the point where the directories would have been checked.
        env.assertContains("Writing results to", stderr)
        env.assertNotContains("directory {}".format(missing), stderr)
        env.assertNotContains("cannot open", stderr)


def test_output_prefix_dir_is_a_file(env):
    """A regular file where the prefix directory should be fails at startup with ENOTDIR (#744).

    Both with and without the execute bit, for both prefix options.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        not_dir = os.path.join(tmp_dir, 'not-a-dir')
        with open(not_dir, 'w'):
            pass
        for mode in (0o755, 0o644):
            os.chmod(not_dir, mode)
            for option in ('--hdr-file-prefix', '--client-stats'):
                rc, stderr = _run_with_unwritable_output(env, [option, os.path.join(not_dir, 'p')],
                                                         subprocess.DEVNULL)
                env.assertEqual(rc, 1, message=stderr)
                env.assertContains("error: cannot write {} directory {}: Not a directory".format(option, not_dir),
                                   stderr)
                env.assertNotContains("Preparing benchmark client", stderr)


def test_hdr_file_open_error(env):
    """An HDR file that cannot be opened when the run ends fails the run (#744).

    A directory in the file's place makes fopen() fail with EISDIR.
    """
    with tempfile.TemporaryDirectory() as hdr_dir:
        prefix = os.path.join(hdr_dir, 'h')
        os.mkdir(prefix + '_FULL_RUN_1.txt')
        rc, stderr = _run_with_unwritable_output(env, ['--hdr-file-prefix', prefix], subprocess.DEVNULL)
        env.assertEqual(rc, 1, message=stderr)
        env.assertContains("error: cannot open --hdr-file-prefix file {}_FULL_RUN_1.txt: Is a directory"
                           .format(prefix), stderr)


def test_client_stats_open_error_reported_once(env):
    """--client-stats CSVs that cannot be opened fail the run, reported once (#744).

    Only the first file is named, then the count of the others.
    """
    with tempfile.TemporaryDirectory() as stats_dir:
        prefix = os.path.join(stats_dir, 'cs')
        for i in range(3):
            os.mkdir('{}-1-0-{}.csv'.format(prefix, i))
        rc, stderr = _run_with_unwritable_output(env, ['--client-stats', prefix, '--clients', '3'],
                                                 subprocess.DEVNULL)
        env.assertEqual(rc, 1, message=stderr)
        env.assertContains("error: cannot open --client-stats file {}-1-0-0.csv: Is a directory".format(prefix),
                           stderr)
        env.assertEqual(stderr.count("error: cannot open --client-stats file"), 1, message=stderr)
        env.assertContains("error: 2 more --client-stats files could not be written", stderr)


def _run_with_oom_sets(env, extra_args, closed_stdout_pipe=False, **kwargs):
    """Run SETs that all fail with -OOM, so each one is logged to --failed-keys-file."""
    conns = env.getOSSMasterNodesConnectionList()
    saved = [conn.execute_command('CONFIG', 'GET', 'maxmemory')[1] for conn in conns]
    for conn in conns:
        conn.execute_command('CONFIG', 'SET', 'maxmemory', '1')
    try:
        if closed_stdout_pipe:
            return _closed_pipe_run(env, ['--ratio', '1:0'] + extra_args)
        return _run_with_unwritable_output(env, ['--ratio', '1:0'] + extra_args, subprocess.DEVNULL, **kwargs)
    finally:
        for conn, value in zip(conns, saved):
            conn.execute_command('CONFIG', 'SET', 'maxmemory', value)


def test_failed_keys_file_missing_directory(env):
    """A --failed-keys-file that cannot be opened fails at startup (#744), like --out-file."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = os.path.join(tmp_dir, 'missing', 'failed-keys.csv')
        rc, stderr = _run_with_unwritable_output(env, ['--failed-keys-file', path], subprocess.DEVNULL)
        env.assertEqual(rc, 1, message=stderr)
        env.assertContains("error: cannot open --failed-keys-file {}: No such file or directory".format(path),
                           stderr)
        env.assertNotContains("Preparing benchmark client", stderr)


def test_failed_keys_file_write_error(env):
    """A --failed-keys-file whose header cannot be written fails at startup (#744).

    The flush was not checked, so nothing reached the file and memtier
    exited 0 without a message.
    """
    if not os.path.exists('/dev/full'):
        env.skip()
        return
    rc, stderr = _run_with_oom_sets(env, ['--failed-keys-file', '/dev/full'])
    env.assertEqual(rc, 1, message=stderr)
    env.assertContains("error: failed writing --failed-keys-file /dev/full: No space left on device", stderr)
    env.assertNotContains("Preparing benchmark client", stderr)


def _file_size_limit_allowed(limit):
    """True if this process may raise its own RLIMIT_FSIZE soft limit to limit."""
    hard = resource.getrlimit(resource.RLIMIT_FSIZE)[1]
    return hard == resource.RLIM_INFINITY or hard >= limit


def _limit_file_size(limit):
    """preexec_fn: cap the size of regular files the child writes; a write past it fails with EFBIG."""
    def apply():
        signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
        resource.setrlimit(resource.RLIMIT_FSIZE, (limit, limit))
    return apply


def _check_failed_keys_record_write_error(env, extra_args):
    limit = 1 << 20
    if not _file_size_limit_allowed(limit):
        env.skip()
        return
    with tempfile.TemporaryDirectory() as log_dir:
        path = os.path.join(log_dir, 'failed-keys.csv')
        with open(path, 'w') as f:
            f.write('timestamp,command,key,status,retries\n')
            f.write('#' * (limit - f.tell() - 1) + '\n')
        rc, stderr = _run_with_oom_sets(env, ['--failed-keys-file', path] + extra_args,
                                        preexec_fn=_limit_file_size(limit))
        env.assertEqual(rc, 1, message=stderr)
        env.assertContains("Preparing benchmark client", stderr)
        env.assertContains("error: failed writing --failed-keys-file {}: File too large; logging disabled."
                           .format(path), stderr)
        env.assertEqual(stderr.count("error: failed writing --failed-keys-file"), 1, message=stderr)
        env.assertEqual(os.path.getsize(path), limit)


def test_failed_keys_file_record_write_error(env):
    """A failed-keys record that cannot be written fails the run at the end (#744).

    The log already holds its header and is at RLIMIT_FSIZE, so opening it
    works and every record fails with EFBIG. The other files stay far below
    the limit. The run goes on without the log.
    """
    _check_failed_keys_record_write_error(env, [])


def test_failed_keys_file_record_write_error_threads(env):
    """Same, with 16 connections over 4 threads logging through the shared logger (#744)."""
    _check_failed_keys_record_write_error(env, ['--threads', '4', '--clients', '4'])


def test_failed_keys_file_stdout_reader_gone(env):
    """A --failed-keys-file on stdout whose reader went away is not an error (#744).

    Same rule as for the results on stdout (#719): only logging stops.
    """
    rc, stderr = _run_with_oom_sets(env, ['--failed-keys-file', '/dev/stdout'], closed_stdout_pipe=True)
    env.assertEqual(rc, 0, message=stderr)
    env.assertNotContains("--failed-keys-file", stderr)


def test_failed_keys_file_written(env):
    """The same -OOM run with a writable --failed-keys-file logs every key and exits 0."""
    with tempfile.TemporaryDirectory() as log_dir:
        path = os.path.join(log_dir, 'failed-keys.csv')
        rc, stderr = _run_with_oom_sets(env, ['--failed-keys-file', path])
        env.assertEqual(rc, 0, message=stderr)
        with open(path) as f:
            lines = f.read().splitlines()
        env.assertEqual(lines[0], 'timestamp,command,key,status,retries')
        env.assertEqual(len(lines), 11, message=stderr)


def test_out_file_fifo_reader_exits_is_an_error(env):
    """An --out-file FIFO whose reader exits fails the run (#719).

    Only stdout, or an --out-file that is the same file as stdout, gets the
    gone-reader exemption; for any other --out-file the results are lost.
    """
    with tempfile.TemporaryDirectory() as work_dir:
        fifo = os.path.join(work_dir, 'results.fifo')
        os.mkfifo(fifo)
        # Open the read end first (non-blocking), so memtier's open() does not block.
        rfd = os.open(fifo, os.O_RDONLY | os.O_NONBLOCK)
        try:
            process = _start_memtier(env, work_dir, ['--out-file', fifo], subprocess.DEVNULL, requests=None,
                                     test_time=2)
            # Close the reader only once memtier has the FIFO open (it prints
            # this line after the open). Closed earlier, memtier's open() would
            # wait for a new reader forever.
            opened = "Writing results to {}...".format(fifo)
            deadline = time.time() + 30
            while (opened not in _read_memtier_stderr(work_dir) and process.poll() is None
                   and time.time() < deadline):
                time.sleep(0.05)
        finally:
            os.close(rfd)
        rc, stderr = _finish_memtier(process, work_dir)
    env.assertContains(opened, stderr)
    env.assertEqual(rc, 1, message=stderr)
    env.assertContains("error: failed writing --out-file {}: Broken pipe".format(fifo), stderr)


def test_stdout_closed_pipe_is_not_an_error(env):
    """A stdout reader that went away (e.g. `| head`) is not a failed run (#719).

    SIGPIPE is ignored, so the writes fail with EPIPE; that is the reader's
    choice, not a lost result, and the exit status stays 0.
    """
    rc, stderr = _closed_pipe_run(env, [])
    env.assertEqual(rc, 0, message=stderr)
    env.assertNotContains("error: failed writing", stderr)


def test_stdout_closed_pipe_unbuffered_is_not_an_error(env):
    """A gone stdout reader is not an error with any stdout buffering (#719).

    stdio may drop the data of a failed write (glibc does line-buffered and
    unbuffered, musl always), so the final fflush() succeeds with only ferror()
    set and the EPIPE is lost. The exemption must not depend on that errno.
    """
    if shutil.which('stdbuf') is None:
        env.skip()
        return
    for mode in ['-o0', '-oL']:
        rc, stderr = _closed_pipe_run(env, [], prefix=['stdbuf', mode])
        env.assertEqual(rc, 0, message='stdbuf {}: {}'.format(mode, stderr))
        env.assertNotContains("error: failed writing", stderr)


def test_stdout_reader_exits_early(env):
    """`memtier | head -n 1` exits 0, the reader quitting mid-output (#719).

    A best-effort smoke test: whether head has exited before memtier writes
    depends on timing (it caught the lost-EPIPE bug in 27 of 30 runs). The
    closed-pipe tests above are the deterministic guard.
    """
    if shutil.which('stdbuf') is None or shutil.which('head') is None:
        env.skip()
        return
    head = subprocess.Popen(['head', '-n', '1'], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL)
    try:
        rc, stderr = _run_with_unwritable_output(env, [], head.stdin, drop_default_out_file=True,
                                                 prefix=['stdbuf', '-oL'])
    finally:
        head.stdin.close()
        head.wait(timeout=10)
    env.assertEqual(rc, 0, message=stderr)
    env.assertNotContains("error: failed writing", stderr)


def test_out_file_dev_stdout_closed_pipe_is_not_an_error(env):
    """--out-file /dev/stdout gets the stdout rule for a gone reader (#719)."""
    if not os.path.exists('/dev/stdout'):
        env.skip()
        return
    rc, stderr = _closed_pipe_run(env, ['--out-file', '/dev/stdout'])
    env.assertEqual(rc, 0, message=stderr)
    env.assertNotContains("error: failed writing", stderr)


def test_stdout_closed_socket_is_not_an_error(env):
    """A stdout socket whose peer went away is not a failed run (#719).

    Covers a closed unix socketpair peer (EPIPE) and a TCP peer that reset
    the connection (ECONNRESET), with default buffering and, where stdbuf
    exists, unbuffered (the errno is then lost and poll() decides).
    """
    prefixes = [None]
    if shutil.which('stdbuf') is not None:
        prefixes.append(['stdbuf', '-o0'])

    def closed_socketpair():
        a, b = socket.socketpair()
        b.close()
        return a

    def reset_tcp():
        # Bound to an ephemeral port; nothing else connects to it.
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(('127.0.0.1', 0))
        listener.listen(1)
        client = socket.create_connection(listener.getsockname(), timeout=5)
        peer, _ = listener.accept()
        listener.close()
        # SO_LINGER 0: close() sends RST instead of FIN.
        peer.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        peer.close()
        time.sleep(0.1)
        return client

    for name, make in [('socketpair', closed_socketpair), ('tcp-rst', reset_tcp)]:
        for prefix in prefixes:
            sock = make()
            try:
                rc, stderr = _run_with_unwritable_output(env, [], sock.fileno(), drop_default_out_file=True,
                                                         prefix=prefix)
            finally:
                sock.close()
            label = '{} {}: {}'.format(name, prefix, stderr)
            env.assertEqual(rc, 0, message=label)
            env.assertNotContains("error: failed writing", stderr)


def _full_nonblocking_pipe():
    """Return (rfd, wfd): a pipe filled to the last byte, its write end O_NONBLOCK."""
    rfd, wfd = os.pipe()
    try:
        fcntl.fcntl(wfd, getattr(fcntl, "F_SETPIPE_SZ", 1031), 4096)
    except OSError:
        pass  # a larger pipe is just filled with more data
    os.set_blocking(wfd, False)
    # Fill it to the last byte: a write <= PIPE_BUF is all-or-nothing.
    for chunk in (b'x' * 4096, b'x'):
        try:
            while True:
                os.write(wfd, chunk)
        except BlockingIOError:
            pass
    return rfd, wfd


def test_stdout_full_nonblocking_pipe_is_an_error(env):
    """Results lost on a live stdout pipe fail the run (#719).

    stdout is a full non-blocking pipe whose reader is still open (as when it
    is shared with a parent that set O_NONBLOCK), so the writes fail with
    EAGAIN. With default buffering the EAGAIN is reported; line-buffered,
    glibc drops that data and only a synthetic EIO is left. Either way the
    reader is still there, so this must not pass as a gone reader.
    """
    prefixes = [None]
    if shutil.which('stdbuf') is not None:
        prefixes.append(['stdbuf', '-oL'])
    for prefix in prefixes:
        rfd, wfd = _full_nonblocking_pipe()
        try:
            rc, stderr = _run_with_unwritable_output(env, [], wfd, drop_default_out_file=True, prefix=prefix)
        finally:
            os.close(wfd)
            os.close(rfd)
        env.assertEqual(rc, 1, message='{}: {}'.format(prefix, stderr))
        env.assertContains("error: failed writing results to stdout: ", stderr)
