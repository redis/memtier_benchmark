import glob
import os
import logging

MEMTIER_BINARY = os.environ.get("MEMTIER_BINARY", "memtier_benchmark")
TLS_CERT = os.environ.get("TLS_CERT", "")
ROOT_FOLDER = os.environ.get("ROOT_FOLDER", "")
TLS_KEY = os.environ.get("TLS_KEY", "")
TLS_CACERT = os.environ.get("TLS_CACERT", "")
TLS_PROTOCOLS = os.environ.get("TLS_PROTOCOLS", "")
VERBOSE = bool(int(os.environ.get("VERBOSE","0")))


def ensure_tls_protocols(master_nodes_connections):
    if TLS_PROTOCOLS != "":
        # if we've specified the TLS_PROTOCOLS env variable ensure the server enforces thos protocol versions
        for master_connection in master_nodes_connections:
            master_connection.execute_command("CONFIG", "SET", "tls-protocols", TLS_PROTOCOLS)


def assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count,
                                    overall_request_count, overall_request_delta=None):
    failed_asserts = env.getNumberOfFailedAssertion()
    try:
        # assert correct exit code
        env.assertTrue(memtier_ok == True)
        # assert we have all outputs
        env.assertTrue(os.path.isfile('{0}/mb.stdout'.format(config.results_dir)))
        env.assertTrue(os.path.isfile('{0}/mb.stderr'.format(config.results_dir)))
        env.assertTrue(os.path.isfile('{0}/mb.json'.format(config.results_dir)))
        if overall_request_delta is None:
            # assert we have the expected request count
            logging.debug(f"Checking if expected value {overall_expected_request_count} matches the actual value {overall_request_count}")
            env.assertEqual(overall_expected_request_count, overall_request_count)
        else:
            env.assertAlmostEqual(overall_expected_request_count, overall_request_count,overall_request_delta)
    finally:
        if env.getNumberOfFailedAssertion() > failed_asserts:
            debugPrintMemtierOnError(config, env)

def add_required_env_arguments(benchmark_specs, config, env, master_nodes_list):
    if VERBOSE:
        logging.basicConfig(level=logging.DEBUG)

    # if we've specified TLS_PROTOCOLS ensure we configure it on redis
    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    ensure_tls_protocols(master_nodes_connections)

    # check if environment is cluster
    if env.isCluster():
        benchmark_specs["args"].append("--cluster-mode")
    # check if environment uses Unix Socket connections
    if env.isUnixSocket():
        benchmark_specs["args"].append("--unix-socket")
        benchmark_specs["args"].append(master_nodes_list[0]['unix_socket_path'])
        config["memtier_benchmark"]['explicit_connect_args'] = True
    else:
        config['redis_process_port'] = master_nodes_list[0]['port']


def debugPrintMemtierOnError(config, env):
    for fname, label in [('mb.stderr', 'STDERR'), ('mb.stdout', 'STDOUT')]:
        path = '{0}/{1}'.format(config.results_dir, fname)
        if not os.path.isfile(path):
            env.debugPrint("### {0} not found (memtier may have exited before writing output): {1}".format(label, path), True)
            continue
        with open(path) as f:
            env.debugPrint("### PRINTING {0} OUTPUT OF MEMTIER ON FAILURE ###".format(label), True)
            env.debugPrint("### {0} file location: {1}".format(fname, path), True)
            for line in f:
                env.debugPrint(line.rstrip(), True)

    if not env.isCluster():
        if env.envRunner is not None:
            log_file = os.path.join(env.envRunner.dbDirPath, env.envRunner._getFileName('master', '.log'))
            with open(log_file) as redislog:
                env.debugPrint("### REDIS LOG ###", True)
                env.debugPrint(
                    "### log_file file location: {0}".format(log_file), True)
                for line in redislog:
                    env.debugPrint(line.rstrip(), True)


def get_expected_request_count(config, key_minimum=0, key_maximum=1000000):
    result = -1
    if 'memtier_benchmark' in config:
        mt = config['memtier_benchmark']
        if 'threads' in mt and 'clients' in mt and 'requests' in mt:
            if mt['requests'] != 'allkeys':
                result = mt['threads'] * mt['clients'] * mt['requests']
            else:
                result = key_maximum - key_minimum + 1
    return result


def agg_info_commandstats(master_nodes_connections, merged_command_stats):
    overall_request_count = 0
    for master_connection in master_nodes_connections:
        shard_stats = master_connection.execute_command("INFO", "COMMANDSTATS")
        for cmd_name, cmd_stat in shard_stats.items():
            if cmd_name in merged_command_stats:
                overall_request_count += cmd_stat['calls']
                merged_command_stats[cmd_name]['calls'] = merged_command_stats[cmd_name]['calls'] + cmd_stat['calls']
    return overall_request_count


def addTLSArgs(benchmark_specs, env):
    if env.useTLS:
        benchmark_specs['args'].append('--tls')
        benchmark_specs['args'].append('--cert={}'.format(TLS_CERT))
        benchmark_specs['args'].append('--cacert={}'.format(TLS_CACERT))
        if TLS_KEY != "":
            benchmark_specs['args'].append('--key={}'.format(TLS_KEY))
        else:
            benchmark_specs['args'].append('--tls-skip-verify')
        if TLS_PROTOCOLS != "":
            benchmark_specs['args'].append('--tls-protocols={}'.format(TLS_PROTOCOLS))
            


def get_default_memtier_config(threads=10, clients=5, requests=1000, test_time=None):
    config = {
        "memtier_benchmark": {
            "binary": MEMTIER_BINARY,
            "threads": threads,
            "clients": clients,
            "requests": requests,
            "test_time": test_time
        },
    }
    return config


def ensure_clean_benchmark_folder(dirname):
    files = glob.glob('{}/*'.format(dirname))
    for f in files:
        os.remove(f)
    if os.path.exists(dirname):
        os.removedirs(dirname)
    os.makedirs(dirname)


def assert_keyspace_range(env, key_max, key_min, master_nodes_connections):
    expected_keyspace_range = key_max - key_min + 1
    overall_keyspace_range = agg_keyspace_range(master_nodes_connections)
    # assert we have the expected keyspace range
    logging.debug(f"Checking if expected keyspace value {expected_keyspace_range} matches the actual value {overall_keyspace_range}")
    env.assertEqual(expected_keyspace_range, overall_keyspace_range)


def agg_keyspace_range(master_nodes_connections):
    overall_keyspace_range = 0
    for master_connection in master_nodes_connections:
        shard_reply = master_connection.execute_command("INFO", "KEYSPACE")
        shard_count = 0
        if 'db0' in shard_reply:
            if 'keys' in shard_reply['db0']:
                shard_count = int(shard_reply['db0']['keys'])
        overall_keyspace_range = overall_keyspace_range + shard_count
    return overall_keyspace_range


def get_column_csv(filename,column_name):
    found = False
    with open(filename,"r") as fd:
        stop_line = 0
        lines = fd.readlines()
        for line in lines:
            # CSV is the first part of file
            if "Full-Test GET Latency" in line or len(line) == 0:
                break
            stop_line = stop_line + 1
        print(stop_line)
        csv_lines = lines[1:stop_line-1]
        header_line = csv_lines[0].strip().split(",")
        col_pos = -1
        for col_index,col in enumerate(header_line):
            if column_name == col:
                col_pos = col_index
                found = True
        data_lines = []
        for line in csv_lines[1:]:
            data_lines.append(line.strip().split(","))
        column_data = []
        if found is True:
            for line in data_lines:
                column_data.append(line[col_pos])
    return found, column_data