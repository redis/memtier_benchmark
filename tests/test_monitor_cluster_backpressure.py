"""
Regression test for end-to-end backpressure in --monitor-input cluster replay.

Background
----------
In cluster mode, --monitor-input + `--command __monitor_line@__` uses a
route-then-stage design: each shard connection reads a monitor line, computes
the key's slot, and (when the key belongs to a different shard) pushes the
command into that shard's staged queue (m_staged_monitor_commands) and wakes it
via schedule_fill(). Staging does not grow the routing connection's own
in-flight pipeline, so fill_pipeline's `m_pipeline->size() < pipeline` gate
never throttles the producer. Without an end-to-end limit the routing side
fans out commands far faster than the targets drain them (each target drains
~pipeline-per-RTT), the staged queues grow without bound, and the reported
latency (measured from selection) climbs monotonically as queue-residence time
dominates the tail — p99 climbs into the tens of ms locally and to tens of
seconds over a real network, while p50 stays at true server latency.

The fix caps the global in-flight count (staged + sent ==
m_reqs_generated - m_reqs_processed) at pipeline * connection_count in
cluster_client::hold_pipeline, coupling production to drain.

What this test pins down
------------------------
With the fix, p99 latency stays bounded (it reflects real server RTT, not
queue residence). Without it, p99 blows past tens of ms for this workload and
grows with the request count. The threshold below has large margin in both
directions: the fixed client reports sub-millisecond p99 against a loopback
cluster, while the unbounded-staging regression reports ~80ms+ at this request
count and worse as it scales.

Run:
    TEST=test_monitor_cluster_backpressure.py OSS_CLUSTER=1 SHARDS=3 ./tests/run_tests.sh
"""

import json
import os
import random
import tempfile

from include import (
    addTLSArgs,
    add_required_env_arguments,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig


# p99 latency ceiling, in milliseconds. The fixed client reports sub-ms p99 on
# a loopback cluster; the unbounded-staging regression reports ~80ms+ at this
# request count (and grows with it). 50ms leaves >25x margin for the fix while
# still failing the regression decisively.
_P99_CEILING_MS = 50.0

# Per-client request count. Large enough that, without backpressure, the staged
# queues build a tail of ~requests/shards commands per client (the source of the
# latency climb); small enough to keep the run to ~1-2s.
_REQUESTS = 4000


def _make_monitor_file(test_dir, n=200):
    """Write a monitor capture with many distinct keys so commands fan out
    across every shard (forcing the cross-shard route-then-stage path)."""
    monitor_file = os.path.join(test_dir, "monitor.txt")
    rng = random.Random(1)
    lines = []
    for i in range(n):
        key = "key:{}".format(rng.randint(1, 100000))
        ts = 1764031576.0 + i * 0.001
        if i % 3 == 0:
            lines.append('[ proxy ] {:.6f} [0 127.0.0.1:5000] "SET" "{}" "v{}"'.format(ts, key, i))
        else:
            lines.append('[ proxy ] {:.6f} [0 127.0.0.1:5000] "GET" "{}"'.format(ts, key))
    with open(monitor_file, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    return monitor_file


def _run_monitor_cluster(env, monitor_file, threads=1, clients=4, requests=_REQUESTS, pipeline=1):
    """Run a monitor-replay workload in cluster mode; return (ok, json_dict, run_config)."""
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--monitor-input={}".format(monitor_file),
            "--command=__monitor_line@__",
            "--monitor-pattern=R",
            "--pipeline={}".format(pipeline),
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=threads, clients=clients, requests=requests)
    master_nodes_list = env.getMasterNodesList()
    # Appends --cluster-mode automatically when env.isCluster().
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)

    benchmark = Benchmark.from_json(run_config, benchmark_specs)
    ok = benchmark.run()
    debugPrintMemtierOnError(run_config, env)

    js = {}
    json_path = "{}/mb.json".format(run_config.results_dir)
    if os.path.isfile(json_path):
        with open(json_path) as fh:
            js = json.load(fh)
    return ok, js, run_config


def test_monitor_cluster_latency_bounded(env):
    """Replaying a monitor capture against an OSS cluster must keep p99 latency
    bounded — i.e. production is coupled to drain and the staged queues do not
    grow without bound. A regression in the backpressure cap reintroduces the
    monotonic latency climb and trips the ceiling below."""
    if not env.isCluster():
        env.skip()
        return

    test_dir = tempfile.mkdtemp()
    monitor_file = _make_monitor_file(test_dir)

    ok, js, _run_config = _run_monitor_cluster(env, monitor_file)
    env.assertTrue(ok, message="memtier did not complete the monitor-cluster run")

    totals = js.get("ALL STATS", {}).get("Totals", {})
    env.assertContains("Percentile Latencies", totals)
    p99 = totals["Percentile Latencies"].get("p99.00")
    env.assertTrue(p99 is not None, message="p99.00 missing from JSON Totals")

    env.assertTrue(
        p99 < _P99_CEILING_MS,
        message="monitor-cluster p99 latency {:.3f}ms exceeds ceiling {:.1f}ms — staged-queue "
                "backpressure likely regressed (latency dominated by queue residence)".format(
                    p99, _P99_CEILING_MS),
    )


def test_monitor_cluster_completes_and_routes(env):
    """Sanity: the monitor-replay run completes and actually drives commands
    across the cluster (non-zero ops, no hang)."""
    if not env.isCluster():
        env.skip()
        return

    test_dir = tempfile.mkdtemp()
    monitor_file = _make_monitor_file(test_dir)

    # Smaller, quick run.
    ok, js, _run_config = _run_monitor_cluster(env, monitor_file, requests=1000)
    env.assertTrue(ok, message="memtier did not complete the monitor-cluster run")

    totals = js.get("ALL STATS", {}).get("Totals", {})
    env.assertTrue(totals.get("Ops/sec", 0) > 0, message="expected non-zero Ops/sec")
    env.assertTrue(totals.get("Count", 0) > 0, message="expected non-zero request Count")
