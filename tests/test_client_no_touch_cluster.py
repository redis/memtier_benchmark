"""
Cluster-mode regression test: CLIENT NO-TOUCH must land on a shard
connection discovered via a live CLUSTER SLOTS reply, not just on the
bootstrap seed connection.

Why this matters: send_conn_setup_commands() sends CLIENT NO-TOUCH via
bufferevent_write() rather than a plain protocol-level evbuffer_add call,
specifically because a connection whose FD was attached via
bufferevent_socket_new() + a later bufferevent_socket_connect() (the path
cluster_client::connect_shard_connection() uses for every shard connection
except the single bootstrap seed connection -- every replica, and every
primary beyond the first, discovered from a CLUSTER SLOTS reply) does not
get its first user-level send's EPOLLOUT armed by the evbuffer notify
callback on its own. tests/test_client_no_touch.py (standalone-only) can't
exercise this: every connection in a standalone run IS the bootstrap seed
connection.

Two tests, two ways to land a connection on the "discovered" path. Only
the dedicated "OSS-CLUSTER + replicas: client-no-touch" CI cell (which
sets OSS_CLUSTER_REPLICAS=1, so RLTest is started with --use-slaves) runs
both for real; this file is also collected -- with no TEST: pin -- by the
plain "OSS-CLUSTER API: TCP Plaintext"/"TCP TLS" cells in ci.yml,
asan.yml, tsan.yml and ubsan.yml, none of which start replicas at all.

1. test_client_no_touch_lands_on_replica_discovered_via_cluster_slots
   targets a replica connection, via get_cluster_replica_connections()
   (tests/include.py). Whether an empty result is a hard failure or a
   skip depends on env_started_with_slaves(env) (tests/include.py): in
   the dedicated cell above, zero replica connections is a hard failure
   (env.assertTrue) -- this repo's pinned RLTest fork fixes the
   gossip-visibility gap issue #462 describes (real CLUSTER
   MEET/REPLICATE, not bare --slaveof), so a silent skip there would let
   the cell go green while only exercising the non-seed-primary case
   below, exactly the coverage gap this cell exists to close. In the
   plain cluster cells, which never asked RLTest for replicas, an empty
   result is expected and the test skips.

2. test_client_no_touch_lands_on_non_seed_primary_discovered_via_cluster_slots
   needs no replicas: any shard beyond memtier's single bootstrap seed
   connection (master_nodes_list[0]) is connected via the identical
   "discovered" path, so the default SHARDS=3 matrix cell already exercises
   it on its own.

Run with:
  TEST=test_client_no_touch_cluster.py OSS_CLUSTER=1 ./tests/run_tests.sh
  TEST=test_client_no_touch_cluster.py OSS_CLUSTER=1 OSS_CLUSTER_REPLICAS=1 ./tests/run_tests.sh
"""

import tempfile
import threading
import time

from include import (
    add_required_env_arguments,
    addTLSArgs,
    capture_monitor,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    env_started_with_slaves,
    get_cluster_replica_connections,
    get_default_memtier_config,
    get_redis_conn_for_node,
)
from mb import Benchmark, RunConfig


def _run_and_check_no_touch(env, conn, extra_args):
    """Attach MONITOR to conn, run a --client-no-touch cluster-mode
    benchmark, and assert CLIENT NO-TOUCH ON was observed. `conn` is
    disconnected as part of cleanup."""
    stop_event = threading.Event()
    results = []
    errors = []
    monitor_thread = threading.Thread(
        target=capture_monitor, args=(conn, results, stop_event, errors), daemon=True
    )
    monitor_thread.start()
    # Give the MONITOR subscription time to be acknowledged before memtier
    # connects, mirroring tests/test_mget_protocol.py's established pattern.
    time.sleep(0.15)

    try:
        config = get_default_memtier_config(threads=1, clients=1, test_time=3)
        config['memtier_benchmark']['requests'] = None

        args = [
            # add_required_env_arguments() below appends --cluster-mode
            # itself once env.isCluster() is true; no need to pass it here.
            '--client-no-touch',
            '--key-pattern', 'R:R',
            '--key-minimum', '1',
            '--key-maximum', '1000',
        ] + extra_args
        benchmark_specs = {"name": env.testName, "args": args}
        addTLSArgs(benchmark_specs, env)
        add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())

        test_dir = tempfile.mkdtemp()
        run_config = RunConfig(test_dir, env.testName, config, {})
        ensure_clean_benchmark_folder(run_config.results_dir)
        benchmark = Benchmark.from_json(run_config, benchmark_specs)

        memtier_ok = benchmark.run()
        debugPrintMemtierOnError(run_config, env)
        env.assertTrue(memtier_ok == True)
    finally:
        stop_event.set()
        time.sleep(0.3)
        try:
            conn.connection_pool.disconnect()
        except Exception:
            pass
        monitor_thread.join(timeout=5)

    no_touch_seen = any(
        entry.get("command", "").upper().startswith("CLIENT NO-TOUCH")
        for entry in results
    )
    env.assertTrue(
        no_touch_seen,
        message="expected CLIENT NO-TOUCH ON on the monitored connection; "
               "monitor observed: {}{}".format(
                   [e.get("command") for e in results][:20],
                   "; MONITOR thread error: {!r}".format(errors[0]) if errors else "",
               ),
    )


def test_client_no_touch_lands_on_replica_discovered_via_cluster_slots(env):
    if not env.isCluster():
        env.skip()
        return
    env.skipOnVersionSmaller("7.2")

    replica_conns = get_cluster_replica_connections(env)
    if not replica_conns:
        if env_started_with_slaves(env):
            # RLTest was asked for replicas (OSS_CLUSTER_REPLICAS=1's
            # --use-slaves) but produced none -- see module docstring for
            # why this is a hard failure rather than a skip here.
            env.assertTrue(
                False,
                message="expected at least one replica connection: RLTest was "
                        "started with --use-slaves but get_cluster_replica_connections() "
                        "returned none",
            )
        else:
            # Plain cluster cell (no TEST: pin collects this file too) --
            # never asked for replicas, so finding none is expected.
            env.skip()
        return

    _run_and_check_no_touch(
        env, replica_conns[0], ['--read-preference', 'secondaryPreferred', '--ratio', '0:1']
    )


def test_client_no_touch_lands_on_non_seed_primary_discovered_via_cluster_slots(env):
    """Same assertion, but on a second PRIMARY shard instead of a replica --
    this one actually runs in the standard CI matrix (no --use-slaves, no
    #462 dependency), since any shard beyond memtier's single bootstrap seed
    connection is connected via the same "discovered from CLUSTER SLOTS"
    path a replica is."""
    if not env.isCluster():
        env.skip()
        return
    env.skipOnVersionSmaller("7.2")

    master_nodes_list = env.getMasterNodesList()
    if len(master_nodes_list) < 2:
        # Single-shard cluster: every connection IS the bootstrap seed, same
        # as standalone. Nothing to exercise here.
        env.skip()
        return

    # master_nodes_list[0] is memtier's -s/-p bootstrap seed (see
    # add_required_env_arguments); any other entry is only ever reached via
    # the CLUSTER SLOTS reply loop.
    non_seed_conn = get_redis_conn_for_node(env, master_nodes_list[1])
    _run_and_check_no_touch(env, non_seed_conn, ['--ratio', '1:1'])
