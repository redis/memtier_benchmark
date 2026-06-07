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
    """Build a default memtier_benchmark config dict.

    Pass requests=None to omit --requests entirely; this is required when the
    caller wants to bound the run by --test-time only (memtier rejects
    --requests and --test-time as mutually exclusive). mb.py skips the
    --requests emission when this value is None.
    """
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


def get_cluster_replica_connections(env):
    """Return List[redis.Redis] for every replica advertised by CLUSTER NODES.

    Cluster-mode only.  Requires the env was started with useSlaves=True.
    Returns an empty list when not in cluster mode or when no replicas are
    found (so callers can gracefully skip rather than crash).

    When the env was started with ``--use-slaves`` (RLTest's useSlaves=True)
    but CLUSTER NODES advertises no replicas, this helper emits a loud
    stderr warning before returning an empty list.  This is the known
    RLTest harness gap: ``--use-slaves`` starts replicas via ``--slaveof``
    *without* ``--cluster-enabled yes``, so the slave processes are
    standalone and never join cluster gossip.  See the
    ``Read Preference -> Testing limitations`` section in README.md for
    the full background.
    """
    import sys
    import redis as _redis

    if not env.isCluster():
        return []
    try:
        any_conn = env.getOSSMasterNodesConnectionList()[0]
        raw = any_conn.execute_command("CLUSTER", "NODES")
    except Exception:
        return []

    # raw may be a bytes string or a plain str depending on the redis-py version
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")

    conns = []
    for line in raw.strip().split("\n"):
        parts = line.split()
        if len(parts) < 3:
            continue
        flags = parts[2]
        if "slave" not in flags and "replica" not in flags:
            continue
        host_part, _, _ = parts[1].partition("@")
        host, _, port_str = host_part.rpartition(":")
        if not port_str:
            continue
        try:
            port = int(port_str)
        except ValueError:
            continue
        conns.append(
            _redis.Redis(
                host=host,
                port=port,
                decode_responses=True,
                socket_connect_timeout=5,
            )
        )

    # If RLTest was launched with useSlaves=True but CLUSTER NODES
    # advertises zero replicas, emit a loud warning so the silent skip
    # is at least visible in test output.  Use getattr() chains because
    # RLTest's env.envRunner shape varies across versions.
    if not conns:
        use_slaves = False
        runner = getattr(env, "envRunner", None)
        if runner is not None:
            use_slaves = bool(
                getattr(runner, "useSlaves", False)
                or getattr(runner, "use_slaves", False)
            )
        if use_slaves:
            sys.stderr.write(
                "warning: OSS_CLUSTER_REPLICAS=1 is set and RLTest started "
                "slave nodes,\nbut CLUSTER NODES shows zero replicas "
                "(slaves were started with --slaveof\nand not "
                "--cluster-enabled yes, so they are not in cluster "
                "gossip).\nThe read-preference tests will skip. See README "
                "\"Read Preference -\ntesting limitations\" for the known "
                "harness gap.\n"
            )
            sys.stderr.flush()
    return conns


def reset_commandstats(connections):
    """CONFIG RESETSTAT on each connection.  Use to baseline before a run."""
    for c in connections:
        try:
            c.execute_command("CONFIG", "RESETSTAT")
        except Exception:
            pass


def server_supports_resp3(env):
    """Detect whether the test cluster's Redis version supports RESP3.

    Capability probe used by tests that pass --protocol=resp3. RESP3 was
    introduced in Redis 6.0, so checking the server's major version is
    sufficient.

    HELLO 3 cannot be used as a probe over a RESP2 connection: the server
    switches wire format to RESP3 mid-reply (the response is a %7\\r\\n map),
    redis-py's RESP2 parser fails with a protocol error, and the broad
    ``except`` would silently classify a fully RESP3-capable Redis 6+ server
    as "not supported" (R5 round-18 caused 3 RESP3 read-preference tests to
    silent-skip on Redis 6+). Parse ``redis_version`` from ``INFO server``
    instead — that reply stays RESP2 and tells us exactly what we need.
    """
    try:
        conn = env.getConnection()
        info = conn.execute_command("INFO", "server")
        version = None
        if isinstance(info, dict):
            version = info.get("redis_version")
        else:
            # Raw bulk string fallback (older redis-py / decode_responses=True).
            if isinstance(info, bytes):
                info = info.decode("utf-8", errors="replace")
            for line in info.splitlines():
                line = line.strip()
                if line.startswith("redis_version:"):
                    version = line.split(":", 1)[1].strip()
                    break
        if not version:
            return False
        major = int(version.split(".")[0])
        return major >= 6
    except Exception:
        return False


def get_get_call_count(conn):
    """Read 'cmdstat_get' from INFO COMMANDSTATS.  Returns 0 if absent."""
    try:
        info = conn.execute_command("INFO", "COMMANDSTATS")
    except Exception:
        return 0

    # INFO COMMANDSTATS may be returned as a dict (redis-py >= 4) or a raw str.
    if isinstance(info, dict):
        stat = info.get("cmdstat_get", {})
        return int(stat.get("calls", 0))

    # Raw string fallback (older redis-py or decode_responses=True).
    for line in info.split("\n"):
        line = line.strip()
        if not line.startswith("cmdstat_get:"):
            continue
        # format: cmdstat_get:calls=N,usec=M,...
        for kv in line.split(":", 1)[1].split(","):
            kv = kv.strip()
            if kv.startswith("calls="):
                try:
                    return int(kv.split("=", 1)[1])
                except ValueError:
                    return 0
    return 0


def get_column_csv(filename, column_name):
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