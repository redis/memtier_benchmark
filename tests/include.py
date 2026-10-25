import functools
import glob
import inspect
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

def _option_values(args, short, long):
    """Values of a memtier option passed as -x v, -xv, --long v or --long=v."""
    values = []
    for i, arg in enumerate(args):
        if arg in (short, long):
            values.append(args[i + 1] if i + 1 < len(args) else "")
        elif arg.startswith(long + "="):
            values.append(arg[len(long) + 1:])
        elif arg.startswith(short) and not arg.startswith("--"):
            values.append(arg[len(short):])
    return values


def env_password(env):
    """
    The password the env's servers require (run_tests.sh REDIS_PASSWORD,
    RLTest --oss_password), or None. Read it here rather than from a redis-py
    connection's kwargs, so every test agrees on where it comes from.
    """
    return env.getMasterNodesList()[0].get("password") or None


def memtier_auth_args(env, args=()):
    """
    --authenticate with the env's password (run_tests.sh REDIS_PASSWORD), for
    tests that build the memtier command line themselves. Empty when the env
    has no password, when args already carry -a/--authenticate or a -u/--uri
    with userinfo, or when args select a memcache protocol (which cannot AUTH
    as a Redis client). As in memtier, the last --uri and --protocol win.
    """
    password = env_password(env)
    args = [str(arg) for arg in args]
    if not password or _option_values(args, "-a", "--authenticate"):
        return []
    uris = _option_values(args, "-u", "--uri")
    if uris and "@" in uris[-1].split("://", 1)[-1].split("/", 1)[0]:
        return []
    protocols = _option_values(args, "-P", "--protocol")
    if protocols and protocols[-1].startswith("memcache"):
        return []
    return ["--authenticate", password]


# NOAUTH guard (#842). memtier counts error replies as completed requests and
# exits 0, so a test whose memtier command line forgets --authenticate still
# passes on a password-protected env while every request is answered -NOAUTH.
# When the env has a password, every RLTest function test that takes an env is
# wrapped: it snapshots errorstat_NOAUTH on each of the env's servers (all
# shards, and replicas) before the test and fails the test if any count grew.
# Checking the servers catches every client (Benchmark, hand-built Popen
# command lines, redis-py), not only the runs that go through mb.py.
NOAUTH_GUARD_ATTR = "allow_noauth"


def allow_noauth(reason):
    """
    Opt a test out of the NOAUTH guard because it is unauthenticated against
    the env's servers on purpose (memcache protocols, missing or wrong
    credentials under test). The reason is required and shows in the code.
    """
    if not reason:
        raise ValueError("allow_noauth needs a reason")

    def mark(test):
        setattr(test, NOAUTH_GUARD_ATTR, reason)
        return test
    return mark


def _noauth_nodes(env):
    """(label, connect) for each server of the env: every shard's master and replicas."""
    runner = getattr(env, "envRunner", None)
    shards = getattr(runner, "shards", None) or [runner]
    nodes = []
    for index, shard in enumerate(shards, 1):
        nodes.append(("shard {} master".format(index), shard.getConnection))
        if getattr(shard, "useSlaves", False):
            for replica in range(len(getattr(shard, "slavePorts", None) or [])):
                nodes.append(("shard {} replica {}".format(index, replica + 1),
                              functools.partial(shard.getSlaveConnection, replica)))
    return nodes


def bounded_client(conn, timeout=5.0):
    """
    Make a redis-py client fail fast: no retries and a socket timeout, so a
    stopped server costs one refused connect rather than redis-py's retry
    backoff, and a hung one cannot block for longer than `timeout`. Changes
    the client's own pool settings and drops connections it already opened
    with the old ones, so use it on a client of your own (RLTest returns a new
    one from every getConnection()).
    """
    from redis.backoff import NoBackoff
    from redis.retry import Retry

    pool = getattr(conn, "connection_pool", None)
    if pool is not None:
        # set_retry also reaches the pool's existing connections, which keep
        # their own retry. Those connections keep the socket timeout they were
        # made with (redis-py's default is 5 s, as is ours); disconnecting
        # them still drops a socket a command may have left mid-reply.
        pool.set_retry(Retry(NoBackoff(), 0))
        pool.connection_kwargs["socket_timeout"] = timeout
        if "socket_connect_timeout" in pool.connection_kwargs:
            pool.connection_kwargs["socket_connect_timeout"] = timeout
        pool.disconnect()
    return conn


def _read_noauth(env):
    """({label: errorstat_NOAUTH}, {label: error}) for each server of the env."""
    counts, errors = {}, {}
    for label, connect in _noauth_nodes(env):
        conn = None
        try:
            conn = bounded_client(connect())
            stats = conn.info("errorstats").get("errorstat_NOAUTH", {})
            counts[label] = int(stats.get("count", 0))
        except Exception as error:
            errors[label] = error
        finally:
            if conn is not None:
                conn.close()
    return counts, errors


def noauth_counts(env):
    """
    errorstat_NOAUTH per server of the env. A server the env's password
    cannot reach (stopped, or requirepass changed by the test) is left out.
    """
    return _read_noauth(env)[0]


def _check_noauth(env, before):
    after, errors = _read_noauth(env)
    for label in sorted(set(before) - set(after)):
        # Stopped during the test (a replica shut down on purpose, say): its
        # NOAUTH count is gone, so it cannot be checked.
        env.debugPrint("NOAUTH guard: {} not checked after the test: {}".format(
            label, errors.get(label, "not found")), force=True)
    for label, count in sorted(after.items()):
        # Unreadable before the test but readable after it: all of its NOAUTH
        # replies count as new, though some may predate this test.
        start = before.get(label, 0)
        # A server restarted during the test starts counting from zero.
        new = count - start if count >= start else count
        if new:
            env.assertEqual(new, 0, message=(
                "{} answered {} request(s) -NOAUTH, but the env has a password "
                "(REDIS_PASSWORD): authenticate memtier with memtier_auth_args(env) "
                "or add_required_env_arguments(), or mark the test "
                "@allow_noauth(reason) if it is unauthenticated on purpose (#842)"
            ).format(label, new))


def noauth_guard(test):
    """
    Wrap an RLTest function test that takes the env in the NOAUTH check. As
    RLTest does, any test with a positional parameter gets the env as its
    first argument, whatever the name; extra parameters keep their defaults.
    """
    if getattr(test, NOAUTH_GUARD_ATTR, None) or getattr(test, "_noauth_guarded", False):
        return test
    try:
        args = inspect.getfullargspec(test).args
    except TypeError:
        return test
    if not args:
        return test

    @functools.wraps(test)
    def guarded(env):
        if not env_password(env):
            return test(env)
        before, errors = _read_noauth(env)
        # Never pass without checking: no readable server means the guard
        # cannot see NOAUTH at all (RLTest drift, or a password mismatch).
        if not before:
            env.assertTrue(False, message=(
                "NOAUTH guard: could not read INFO errorstats from any of the env's "
                "servers before the test, so NOAUTH replies would go unseen (#842): "
                "{}".format("; ".join("{}: {}".format(label, error)
                                      for label, error in sorted(errors.items())) or "no servers")))
        try:
            return test(env)
        finally:
            if before:
                _check_noauth(env, before)
    guarded._noauth_guarded = True
    return guarded


def _install_noauth_guard():
    # RLTest has no per-test hook for function tests, so wrap the target that
    # its loader resolves just before each test runs. include.py is imported
    # while RLTest discovers the test files, before any test starts, and
    # RLTest forks its --parallelism workers after that.
    try:
        from RLTest import loader
    except ImportError:
        return
    test_function = loader.TestFunction
    if getattr(test_function, "_noauth_guard", False):
        return
    initialize = test_function.initialize

    def guarded_initialize(self):
        initialize(self)
        self.target = noauth_guard(self.target)
    test_function.initialize = guarded_initialize
    test_function._noauth_guard = True


_install_noauth_guard()


def add_required_env_arguments(benchmark_specs, config, env, master_nodes_list):
    if VERBOSE:
        logging.basicConfig(level=logging.DEBUG)

    # if we've specified TLS_PROTOCOLS ensure we configure it on redis
    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    ensure_tls_protocols(master_nodes_connections)

    # The env's password (run_tests.sh REDIS_PASSWORD, RLTest --oss_password)
    benchmark_specs["args"].extend(memtier_auth_args(env, benchmark_specs["args"]))
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
        # errors="replace": the output can echo non-UTF-8 user input.
        with open(path, errors="replace") as f:
            env.debugPrint("### PRINTING {0} OUTPUT OF MEMTIER ON FAILURE ###".format(label), True)
            env.debugPrint("### {0} file location: {1}".format(fname, path), True)
            for line in f:
                env.debugPrint(line.rstrip(), True)

    if env.isCluster():
        return
    runner = getattr(env, 'envRunner', None)
    db_dir = getattr(runner, 'dbDirPath', None)
    get_file_name = getattr(runner, '_getFileName', None)
    if db_dir is None or not callable(get_file_name):
        env.debugPrint("### Redis log unavailable (environment has no captured log) ###", True)
        return
    log_file = os.path.join(db_dir, get_file_name('master', '.log'))
    try:
        with open(log_file, errors="replace") as redislog:
            env.debugPrint("### REDIS LOG ###", True)
            env.debugPrint("### log_file file location: {0}".format(log_file), True)
            for line in redislog:
                env.debugPrint(line.rstrip(), True)
    except OSError as error:
        # Existing environments and -s may not provide a log. Diagnostic IO
        # must not replace the assertion failure that brought us here.
        env.debugPrint("### Redis log unavailable: {0} ###".format(error), True)


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
                password=env_password(env),
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


def shutdown_nosave(conn, timeout=5.0):
    """
    SHUTDOWN NOSAVE the server behind a redis-py connection. True only once
    the server stopped answering: a refused SHUTDOWN (NOAUTH, for example)
    leaves it running, and redis-py raises NOAUTH as a ConnectionError, so
    the closed connection alone does not prove the shutdown. The client is
    made fail-fast (bounded_client), so redis-py does not retry, and so
    resend, the SHUTDOWN.
    """
    import redis as _redis

    bounded_client(conn)
    try:
        conn.execute_command("SHUTDOWN", "NOSAVE")
    except _redis.exceptions.AuthenticationError:
        return False
    except _redis.exceptions.ConnectionError:
        pass  # the server closes the connection as it exits
    except _redis.exceptions.RedisError:
        return False
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            conn.ping()
        except _redis.exceptions.AuthenticationError:
            pass  # still running
        except _redis.exceptions.ConnectionError:
            return True
        except _redis.exceptions.RedisError:
            pass
        time.sleep(0.1)
    return False


def harness_client_ids(connections):
    """Per connection, the IDs of the normal clients already on that server.

    Take it before starting memtier: every normal client that shows up later
    and is not one of these is memtier's, so the kills below leave the
    harness's own connections (RLTest's, redis-py's pool) alone and count only
    memtier's.

    This assumes no harness connection opens after the snapshot. It holds
    while the kills run on the calling thread, which reuses the one pooled
    connection per server. Polling from a second thread through the same
    pool can open a new pooled connection, which would be taken for
    memtier's and killed.
    """
    harness = []
    for conn in connections:
        ids = set(int(c["id"]) for c in conn.client_list(_type="normal"))
        ids.add(int(conn.client_id()))
        harness.append(ids)
    return harness


_client_list_errors = set()


def memtier_client_ids(env, connections, harness):
    """Per connection, the IDs of memtier's clients (see harness_client_ids).

    Only normal clients: a replica link that connects later is not memtier's.
    """
    found = []
    for conn, ids in zip(connections, harness):
        try:
            found.append([int(c["id"]) for c in conn.client_list(_type="normal") if int(c["id"]) not in ids])
        except Exception as e:
            # Log each distinct failure once, not on every 1ms poll.
            if str(e) not in _client_list_errors:
                _client_list_errors.add(str(e))
                env.debugPrint("CLIENT LIST failed: {}".format(e), True)
            found.append([])
    return found


def wait_for_memtier_clients(env, connections, harness, proc, timeout=30.0):
    """Poll until memtier has a client on some server; False if proc exits first."""
    deadline = time.time() + timeout
    while time.time() < deadline and proc.poll() is None:
        if any(memtier_client_ids(env, connections, harness)):
            return True
        time.sleep(0.001)
    return False


def kill_memtier_clients(env, connections, harness):
    """CLIENT KILL memtier's clients; returns how many were killed."""
    killed = 0
    for conn, ids in zip(connections, memtier_client_ids(env, connections, harness)):
        for client_id in ids:
            try:
                killed += int(conn.execute_command("CLIENT", "KILL", "ID", client_id, "SKIPME", "yes"))
            except Exception:
                pass  # already gone
    return killed


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
