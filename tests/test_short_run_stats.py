"""Short-run throughput smoke for #463; exact rounding is covered by the C++ unit."""

import json
import os
import tempfile

from include import (
    addTLSArgs,
    add_required_env_arguments,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig


def test_short_run_reports_positive_throughput(env):
    # A real-server smoke cannot force a wall-clock second boundary. The
    # run_stats_merge unit supplies that deterministic regression gate.
    specs = {"name": env.testName, "args": ["--ratio=1:0", "--hide-histogram"]}
    addTLSArgs(specs, env)
    password = env.getConnection().connection_pool.connection_kwargs.get("password")
    if password:
        specs["args"].append("--authenticate=" + password)
    config = get_default_memtier_config(threads=1, clients=1, requests=200)
    add_required_env_arguments(specs, config, env, env.getMasterNodesList())
    connections = env.getOSSMasterNodesConnectionList()
    before = sum(
        c.info("commandstats").get("cmdstat_set", {}).get("calls", 0)
        for c in connections
    )
    with tempfile.TemporaryDirectory() as test_dir:
        run_config = RunConfig(test_dir, env.testName, config, {})
        ensure_clean_benchmark_folder(run_config.results_dir)
        ok = Benchmark.from_json(run_config, specs).run()
        debugPrintMemtierOnError(run_config, env)
        env.assertTrue(ok, message="short #463 regression run did not complete")
        with open(os.path.join(run_config.results_dir, "mb.json")) as source:
            totals = json.load(source)["ALL STATS"]["Totals"]
        env.assertEqual(totals["Count"], 200)
        env.assertEqual(totals["Connection Errors"], 0)
        after = sum(
            c.info("commandstats").get("cmdstat_set", {}).get("calls", 0)
            for c in connections
        )
        env.assertEqual(after - before, 200)
        env.assertGreater(totals["Ops/sec"], 0)
