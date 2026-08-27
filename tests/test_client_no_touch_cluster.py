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

Two tests, two ways to land a connection on the "discovered" path:

1. test_client_no_touch_lands_on_replica_discovered_via_cluster_slots
   targets a replica connection. Known harness limitation:
   get_cluster_replica_connections() returns an empty list under RLTest's
   --use-slaves (the resulting slaves are not cluster-gossip members -- see
   README.md "Testing limitations" and
   https://github.com/redis/memtier_benchmark/issues/462, filed for the
   identical gap affecting the --read-preference test suite). This test
   skips gracefully in that case, matching
   tests/test_read_preference_failover.py's established pattern, and will
   start actually verifying once #462 lands. In the meantime this was
   verified manually against a real `redis-cli --cluster create`-equivalent
   cluster with cluster-aware replicas via `redis-cli MONITOR` (see PR
   #526's description).

2. test_client_no_touch_lands_on_non_seed_primary_discovered_via_cluster_slots
   sidesteps that gap entirely and DOES run in the standard CI matrix (no
   --use-slaves needed): cluster_client's CLUSTER-SLOTS-reply loop connects
   every primary shard beyond memtier's single bootstrap seed connection
   (master_nodes_list[0], the -s/-p argument) via connect_shard_connection()
   -- the exact same "discovered" path replicas use -- so any additional
   shard in a multi-shard cluster (the default SHARDS=3 OSS-CLUSTER matrix
   cell already provides this) already exercises it, with zero dependency
   on replica gossip visibility.

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
        # Known harness gap (README "Testing limitations", issue #462) --
        # get_cluster_replica_connections() already emitted its own stderr
        # warning if --use-slaves was set but produced no gossip-visible
        # replicas.
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
