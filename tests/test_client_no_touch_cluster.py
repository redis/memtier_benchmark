"""
Cluster-mode regression test: CLIENT NO-TOUCH must land on a shard
connection discovered via a live CLUSTER SLOTS reply, not just on the
bootstrap seed connection.

Why this matters: send_conn_setup_commands() sends CLIENT NO-TOUCH via
bufferevent_write() rather than a plain protocol-level evbuffer_add call,
following the same precedent set by READONLY for connections discovered
via a live CLUSTER SLOTS reply (every shard beyond the single bootstrap
seed connection) -- cluster_client::connect_shard_connection() attaches
those via bufferevent_socket_new() + a later bufferevent_socket_connect().
Whether that precedent's original EPOLLOUT-arming diagnosis actually holds
is an open question (see #532); bufferevent_write is safe either way, and
this test exists to prove the actual observable behavior -- CLIENT
NO-TOUCH reaching the server on a discovered connection -- regardless of
which write path turns out to be necessary. tests/test_client_no_touch.py
(standalone-only) can't exercise this: every connection in a standalone
run IS the bootstrap seed connection.

Two tests, two ways to land a connection on the "discovered" path. Only
the dedicated "OSS-CLUSTER + replicas: client-no-touch" CI cell (which
sets OSS_CLUSTER_REPLICAS=1, so RLTest is started with --use-slaves) runs
both for real; this file is also collected -- with no TEST: pin -- by the
plain "OSS-CLUSTER API: TCP Plaintext"/"TCP TLS" cells in ci.yml,
asan.yml, tsan.yml and ubsan.yml, none of which start replicas at all.

1. test_client_no_touch_lands_on_replica_discovered_via_cluster_slots
   targets a replica connection, via get_cluster_replica_connections()
   (tests/include.py). Whether an empty result is a hard failure or a skip
   depends on replicas_expected() (tests/include.py): a hard failure in
   the dedicated cell above
   (where a silent skip would let the cell go green while only exercising
   the non-seed-primary case below -- exactly the coverage gap this cell
   exists to close), a skip in the plain cluster cells, which never asked
   RLTest for replicas. This makes it the first test in the repo to
   hard-require the pinned RLTest fork's cluster-aware --use-slaves
   behavior (see tests/test_requirements.txt) rather than skip on its
   absence -- if that fork branch moves, or upstream RLTest#253 lands with
   different semantics, this is the test that goes red.

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
    get_cluster_replica_connections,
    get_default_memtier_config,
    get_redis_conn_for_node,
    replicas_expected,
    wait_for_monitor_registered,
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
    # Poll (not a fixed sleep) until the MONITOR subscription actually shows
    # up in CLIENT LIST -- this file is collected by the asan/tsan/ubsan
    # cluster cells too, the slowest runners, where a missed MONITOR attach
    # costs a red build rather than a retry. Needs its own connection to the
    # *same* node `conn` is monitoring (CLIENT LIST is per-server, and
    # `conn` itself is busy in capture_monitor's thread) -- pull host/port
    # from conn's own connection kwargs specifically, not a blind ** of the
    # whole dict (redis-py's ConnectionPool carries internal bookkeeping
    # keys there that aren't valid Redis() constructor arguments), and
    # rebuild via get_redis_conn_for_node() for the same TLS handling `conn`
    # itself was built with.
    conn_kwargs = conn.connection_pool.connection_kwargs
    control_conn = get_redis_conn_for_node(
        env, {"host": conn_kwargs.get("host"), "port": conn_kwargs["port"]}
    )
    env.assertTrue(
        wait_for_monitor_registered(control_conn),
        message="MONITOR subscription never showed up in CLIENT LIST",
    )

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
        try:
            control_conn.connection_pool.disconnect()
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
        if replicas_expected():
            # See module docstring for why an empty result here is a hard
            # failure rather than a skip.
            env.assertTrue(
                False,
                message="test-harness problem, not a memtier bug: RLTest was started "
                        "with --use-slaves (OSS_CLUSTER_REPLICAS=1) but CLUSTER NODES "
                        "shows zero replicas -- the pinned RLTest fork "
                        "(tests/test_requirements.txt) isn't producing cluster-visible "
                        "replicas the way it did when this test was written. This test "
                        "never got as far as sending CLIENT NO-TOUCH.",
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
