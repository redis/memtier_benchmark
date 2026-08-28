import glob
import os
import re
import logging
import time
import urllib.error
import urllib.request

MEMTIER_BINARY = os.environ.get("MEMTIER_BINARY", "memtier_benchmark")
TLS_CERT = os.environ.get("TLS_CERT", "")
ROOT_FOLDER = os.environ.get("ROOT_FOLDER", "")
TLS_KEY = os.environ.get("TLS_KEY", "")
TLS_CACERT = os.environ.get("TLS_CACERT", "")
TLS_PROTOCOLS = os.environ.get("TLS_PROTOCOLS", "")
VERBOSE = bool(int(os.environ.get("VERBOSE","0")))


def get_redis_conn_for_node(env, node, **extra_kwargs):
    """Return a redis.Redis connection to a specific cluster/master node dict
    (as returned by env.getMasterNodesList(), or a replica dict with a
    'port'/'host' key), TLS-aware.

    Skips server cert verification (self-signed test certs, CN rarely
    matches "127.0.0.1") and presents the client cert/key when the env is
    TLS. extra_kwargs are passed through to redis.Redis() as-is (e.g.
    decode_responses, socket_connect_timeout) and take precedence over the
    TLS/unix-socket defaults if they overlap.
    """
    import redis as _redis

    if env.isUnixSocket():
        kwargs = {"unix_socket_path": node["unix_socket_path"]}
    else:
        kwargs = {"host": node.get("host") or "127.0.0.1", "port": node["port"]}
        if getattr(env, "useTLS", False):
            kwargs["ssl"] = True
            kwargs["ssl_cert_reqs"] = "none"
            if TLS_CERT:
                kwargs["ssl_certfile"] = TLS_CERT
            if TLS_KEY:
                kwargs["ssl_keyfile"] = TLS_KEY
    kwargs.update(extra_kwargs)
    return _redis.Redis(**kwargs)


def capture_monitor(conn, results, stop_event, errors=None):
    """Thread target: append each MONITOR entry from conn to results until
    stop_event is set.

    If errors is given (a list), any exception that ends the loop -- most
    usefully a TLS handshake failure against a connection built without the
    right TLS kwargs -- is appended to it instead of being silently
    swallowed. A caller whose assertion then finds zero captured commands
    can report errors[0] instead of a bare "observed: []" that gives no clue
    why.
    """
    try:
        with conn.monitor() as m:
            while not stop_event.is_set():
                try:
                    results.append(m.next_command())
                except Exception as e:
                    if errors is not None:
                        errors.append(e)
                    break
    except Exception as e:
        if errors is not None:
            errors.append(e)


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
    stderr warning before returning an empty list, as a defensive fallback.
    README.md's ``Read Preference -> Testing limitations`` section and
    issue #462 describe this as expected under plain RLTest's
    ``--use-slaves`` (starts replicas via ``--slaveof`` *without*
    ``--cluster-enabled yes``, so they never join cluster gossip) -- but
    this repo's pinned RLTest fork (tests/test_requirements.txt) already
    fixes that (real ``CLUSTER MEET``/``CLUSTER REPLICATE``), confirmed by
    tests/test_client_no_touch_cluster.py genuinely running (not skipping)
    against a replica in this repo's own CI. The warning path here is not
    expected to fire in this repo's current test dependencies.
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
    # is at least visible in test output. MEMTIER_CLUSTER_REPLICAS_EXPECTED
    # is set by run_tests.sh only inside the specific subshell that passes
    # --use-slaves to RLTest -- the same signal
    # test_client_no_touch_cluster.py's hard-fail gate uses, so there's one
    # way to answer "did this invocation ask for replicas", not two.
    if not conns:
        if os.environ.get("MEMTIER_CLUSTER_REPLICAS_EXPECTED") == "1":
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


# ---------------------------------------------------------------------------
# Prometheus exporter scrape helpers (shared by tests/test_prometheus.py).
#
# The exporter prints exactly one stdout announce line once its socket is
# bound (PLAN.md §3.2, Decisions #9 / #26):
#
#     Prometheus exporter listening on http://127.0.0.1:46127/metrics
#     Prometheus exporter listening on http://[::1]:46127/metrics   (IPv6)
#
# PROM_LISTEN_RE captures the URL (IPv6 host kept bracketed, RFC 3986).
# ---------------------------------------------------------------------------
PROM_LISTEN_RE = re.compile(
    r"Prometheus exporter listening on (http://(?:\[[0-9A-Fa-f:]+\]|[^:/\s]+):\d+/metrics)"
)

# The byte-exact OpenMetrics/Prometheus 0.0.4 content type the exporter emits
# (PLAN.md §3.3, prom::CONTENT_TYPE).
PROM_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def wait_for_prometheus_url(stdout_path, timeout=30.0, interval=0.1):
    """Poll *stdout_path* for the exporter announce line and return the URL.

    Returns the captured /metrics URL (IPv6 host stays bracketed) or None if
    the announce line never appears within *timeout* seconds.  The file is
    re-read each poll because the producer flushes the line asynchronously.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with open(stdout_path) as fh:
                m = PROM_LISTEN_RE.search(fh.read())
        except FileNotFoundError:
            m = None
        if m:
            return m.group(1)
        time.sleep(interval)
    return None


class PromScrape(object):
    """Result of a single /metrics HTTP GET.

    Attributes:
      status   -- HTTP status code (int), or None on a connection-level failure
      body     -- decoded response body (str), or "" on a connection failure
      headers  -- email.message.Message of response headers, or None
      error    -- the exception instance on a connection-level failure, else None

    A connection-level failure (refused / reset / timeout) is *not* an
    exception out of prom_scrape: it is reported as status=None so callers
    with a "200-or-connection-failure" closed outcome set (F15) can branch
    cleanly.  HTTP error statuses (404/501/503) are returned as a normal
    result with the real status code and body (urllib raises HTTPError for
    >=400, which we catch and unwrap).
    """

    def __init__(self, status=None, body="", headers=None, error=None):
        self.status = status
        self.body = body
        self.headers = headers
        self.error = error

    def header(self, name):
        """Case-insensitive single-header lookup; returns None if absent."""
        if self.headers is None:
            return None
        return self.headers.get(name)


def prom_scrape(url, timeout=2.0, method="GET"):
    """HTTP-scrape *url*, returning a PromScrape.

    * 2xx                       -> status/body/headers populated, error=None
    * HTTP >= 400 (404/501/503) -> real status + body via HTTPError unwrap
    * connection-level failure  -> status=None, error set (the process beat
                                   the scrape to teardown, etc.)
    """
    req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return PromScrape(status=resp.status, body=body, headers=resp.headers)
    except urllib.error.HTTPError as e:
        # >= 400: a real HTTP reply (404 / 501 / 503).  Unwrap to a result.
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        return PromScrape(status=e.code, body=body, headers=e.headers)
    except (urllib.error.URLError, OSError) as e:
        # Connection refused / reset / timeout: the closed-outcome "the
        # process beat us to teardown" branch.  Report, don't raise.
        return PromScrape(status=None, body="", headers=None, error=e)


def prom_parse(body):
    """Parse a Prometheus exposition body into {sample_name: [Sample, ...]}.

    Uses prometheus_client's official text parser (test_requirements.txt pins
    prometheus_client>=0.20).  The return value is a dict keyed by the *sample*
    name (so memtier_latency_seconds yields memtier_latency_seconds_bucket,
    _count, _sum entries) mapping to a list of prometheus_client Sample
    namedtuples (name, labels, value, timestamp, exemplar).

    Raises on a malformed body so tests that "hard-assert prom_parse" (F2)
    fail loudly on a corrupt scrape.
    """
    from prometheus_client.parser import text_string_to_metric_families

    out = {}
    for family in text_string_to_metric_families(body):
        for sample in family.samples:
            out.setdefault(sample.name, []).append(sample)
    return out


def prom_sample_value(parsed, name, labels=None):
    """Return the value of the first sample named *name* matching *labels*.

    *parsed* is the dict from prom_parse.  *labels* is an optional dict of
    label key/value pairs the sample must contain (subset match).  Returns
    None if no matching sample exists.
    """
    for s in parsed.get(name, []):
        if labels is None or all(s.labels.get(k) == v for k, v in labels.items()):
            return s.value
    return None


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