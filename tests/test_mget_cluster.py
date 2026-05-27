"""
Cluster-mode tests for the MGET feature.

Background
----------
`redis_protocol::write_command_multi_get()` is used by the `--multi-key-get N`
option to batch multiple GETs into a single MGET command.  In cluster mode the
keys are selected from a pre-built per-slot cache: memtier scans `[key_min,
key_max]` once at startup, groups key indices by their exact CRC16 hash slot,
and at request time picks N keys from the same slot as the target shard.  This
guarantees every MGET is served by a single shard without using hash-tag
prefixes, while still distributing load across all shards.  MGET is also
available via the `--command='MGET ...'` arbitrary-command path: the caller is
then responsible for ensuring all keys belong to the same slot.

Test matrix
-----------
1. test_mget_cluster_mode_allowed
   --multi-key-get with --cluster-mode must exit 0 and register MGET calls.

2. test_mget_arbitrary_single_slot
   MGET via --command with hash-tagged keys (all in the same slot) must
   complete successfully and register calls in INFO COMMANDSTATS.

3. test_mget_arbitrary_single_slot_each_shard
   Same-slot MGET must route to the CORRECT shard for every shard in the
   cluster, not only shard 0.

4. test_mget_arbitrary_crossslot_handled_gracefully
   MGET with plain (non-tagged) keys will likely span multiple slots; Redis
   returns CROSSSLOT errors.  The benchmark must not crash (no SIGSEGV /
   SIGABRT), and the CROSSSLOT error must surface in memtier's stderr.

5. test_mget_arbitrary_cluster_hits_land_on_shard
   After pre-loading keys onto a single shard, MGET reads from that shard
   must report hits (Hits/sec > 0) in the JSON output and all MGET calls must
   be counted on the owning shard only.

6. test_mget_standalone_not_affected_by_cluster_restriction
   In standalone mode --multi-key-get must work fine; the cluster-mode guard
   must not accidentally block standalone users.
"""

import json
import os
import subprocess
import tempfile

from redis.cluster import key_slot

from include import (
    MEMTIER_BINARY,
    add_required_env_arguments,
    addTLSArgs,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig


# ---------------------------------------------------------------------------
# Shared cluster helpers (copied from test_cluster_transaction.py pattern)
# ---------------------------------------------------------------------------

def _master_conns_by_port(env):
    """Return {port:int -> Redis connection} for every master in the cluster."""
    return {
        int(conn.connection_pool.connection_kwargs["port"]): conn
        for conn in env.getOSSMasterNodesConnectionList()
    }


def _flush_cluster(env):
    for conn in env.getOSSMasterNodesConnectionList():
        conn.execute_command("FLUSHALL")


def _dbsize_per_shard(env):
    """Map of port:int -> integer DBSIZE."""
    return {port: int(conn.execute_command("DBSIZE"))
            for port, conn in _master_conns_by_port(env).items()}


def _owning_port(env, hash_tag):
    """Return the master port that owns the slot for *hash_tag*.

    Uses CLUSTER SLOTS as the source of truth so the test remains correct
    under any shard count or non-default slot layout.
    """
    slot = key_slot(hash_tag.encode())
    any_master = next(iter(env.getOSSMasterNodesConnectionList()))
    slots_view = any_master.execute_command("CLUSTER", "SLOTS")
    for entry in slots_view:
        slot_start, slot_end = int(entry[0]), int(entry[1])
        if slot_start <= slot <= slot_end:
            owner_port = int(entry[2][1])
            return owner_port
    raise AssertionError(
        "no master found for slot {} of hash tag {!r}".format(slot, hash_tag))


def _pick_hash_tags_one_per_shard(env, candidates=None):
    """Return [(hash_tag, owner_port)] with one hash tag per master shard.

    The candidate pool is intentionally larger than the typical 3-shard split
    so the helper still finds a unique tag per shard under alternate layouts.
    """
    if candidates is None:
        candidates = ['b', 'c', 'a', 'f', 'g', 'd', 'j', 'k', 'h']
    master_count = len(env.getOSSMasterNodesConnectionList())
    seen = set()
    chosen = []
    for tag in candidates:
        port = _owning_port(env, tag)
        if port in seen:
            continue
        seen.add(port)
        chosen.append((tag, port))
        if len(seen) == master_count:
            break
    env.assertEqual(
        len(chosen), master_count,
        message="couldn't find one hash-tag per shard (got {} for {} shards). "
                "Extend the candidate list.".format(len(chosen), master_count))
    return chosen


def _read_stderr(run_config):
    path = "{0}/mb.stderr".format(run_config.results_dir)
    if not os.path.isfile(path):
        return ""
    with open(path) as f:
        return f.read()


def _cmdstat_mget_calls_per_shard(env):
    """Return {port:int -> int} mapping each master's MGET call count."""
    result = {}
    for port, conn in _master_conns_by_port(env).items():
        stats = conn.execute_command("INFO", "COMMANDSTATS")
        calls = 0
        if "cmdstat_mget" in stats:
            calls = int(stats["cmdstat_mget"]["calls"])
        result[port] = calls
    return result


def _reset_stats(env):
    for conn in env.getOSSMasterNodesConnectionList():
        conn.execute_command("CONFIG", "RESETSTAT")


def _run_mget_workload(env, extra_args, threads=2, clients=4, requests=100):
    """Run a short MGET workload and return (memtier_ok, run_config)."""
    benchmark_specs = {"name": env.testName, "args": list(extra_args)}
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=threads, clients=clients,
                                        requests=requests)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)

    benchmark = Benchmark.from_json(run_config, benchmark_specs)
    ok = benchmark.run()
    return ok, run_config


# ---------------------------------------------------------------------------
# Test 1 – --multi-key-get is allowed (and works) in cluster mode
# ---------------------------------------------------------------------------

def test_mget_cluster_mode_allowed(env):
    """--multi-key-get with --cluster-mode must now WORK: memtier exits 0 and
    MGET calls are registered across the cluster.  In cluster mode the keys for
    each MGET are hash-tagged so all N keys land in the same slot.
    """
    if not env.isCluster():
        env.skip()
        return

    _reset_stats(env)

    extra_args = [
        "--ratio=0:10",
        "--multi-key-get=10",
        "--key-minimum=1",
        "--key-maximum=100",
    ]
    ok, run_config = _run_mget_workload(env, extra_args,
                                        threads=1, clients=1, requests=20)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="--multi-key-get in cluster mode should exit 0")

        total_mget_calls = sum(_cmdstat_mget_calls_per_shard(env).values())
        env.assertGreater(
            total_mget_calls, 0,
            message="expected MGET calls in INFO COMMANDSTATS but got 0 "
                    "across all shards")
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


def test_mget_cluster_72_keys(env):
    """A large --multi-key-get=72 workload must complete in cluster mode and
    the total number of MGET commands across all shards must match the expected
    request count (threads * clients * requests = 1 * 2 * 30 = 60)."""
    if not env.isCluster():
        env.skip()
        return

    _reset_stats(env)

    threads, clients, requests = 1, 2, 30
    extra_args = [
        "--ratio=0:72",
        "--multi-key-get=72",
        "--key-minimum=1",
        "--key-maximum=1000",
    ]
    ok, run_config = _run_mget_workload(
        env, extra_args,
        threads=threads, clients=clients, requests=requests)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="--multi-key-get=72 in cluster mode should exit 0")

        total_mget_calls = sum(_cmdstat_mget_calls_per_shard(env).values())
        expected_calls = threads * clients * requests
        env.assertEqual(
            total_mget_calls, expected_calls,
            message="expected {} MGET calls across all shards (threads={} * "
                    "clients={} * requests={}), got {}".format(
                        expected_calls, threads, clients, requests,
                        total_mget_calls))
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


def test_mget_cluster_distributed_across_shards(env):
    """The rotating group-tag mechanism must distribute MGET load across ALL
    shards, not pin everything to a single shard.  Run a larger workload and
    assert every shard recorded at least one MGET call."""
    if not env.isCluster():
        env.skip()
        return

    _reset_stats(env)

    extra_args = [
        "--ratio=0:10",
        "--multi-key-get=10",
        "--key-minimum=1",
        "--key-maximum=1000",
    ]
    ok, run_config = _run_mget_workload(env, extra_args,
                                        threads=2, clients=5, requests=200)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="--multi-key-get in cluster mode should exit 0")

        calls_per_shard = _cmdstat_mget_calls_per_shard(env)
        for port, calls in calls_per_shard.items():
            env.assertGreater(
                calls, 0,
                message="shard on port {} recorded 0 MGET calls — load is not "
                        "distributed across all shards (per-shard counts: "
                        "{})".format(port, calls_per_shard))
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


def test_mget_cluster_no_crossslot_errors(env):
    """Hash-tagged keys must keep every key of a single MGET in the same slot,
    so no CROSSSLOT errors should ever surface and memtier must exit 0."""
    if not env.isCluster():
        env.skip()
        return

    _reset_stats(env)

    extra_args = [
        "--ratio=0:10",
        "--multi-key-get=10",
        "--key-minimum=1",
        "--key-maximum=100",
    ]
    ok, run_config = _run_mget_workload(env, extra_args,
                                        threads=1, clients=1, requests=50)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="--multi-key-get in cluster mode should exit 0")

        stderr_text = _read_stderr(run_config)
        env.assertTrue(
            "CROSSSLOT" not in stderr_text,
            message="unexpected CROSSSLOT error in memtier stderr — hash "
                    "tagging should keep all MGET keys in the same slot; "
                    "stderr excerpt: {!r}".format(stderr_text[:400]))
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


# ---------------------------------------------------------------------------
# Test 2 – same-slot MGET via arbitrary --command works end-to-end
# ---------------------------------------------------------------------------

def test_mget_arbitrary_single_slot(env):
    """MGET via --command with a hash-tagged key must complete successfully
    and show calls in INFO COMMANDSTATS.  Cluster mode restricts arbitrary
    commands to a single __key__ token, so we use MGET {mg}-__key__."""
    if not env.isCluster():
        env.skip()
        return

    _reset_stats(env)

    extra_args = [
        "--command=MGET {mg}-__key__",
        "--key-minimum=1",
        "--key-maximum=100",
    ]
    ok, run_config = _run_mget_workload(env, extra_args,
                                        threads=2, clients=4, requests=100)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")

        total_mget_calls = sum(_cmdstat_mget_calls_per_shard(env).values())
        env.assertGreater(
            total_mget_calls, 0,
            message="expected MGET calls in INFO COMMANDSTATS but got 0 "
                    "across all shards")
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


# ---------------------------------------------------------------------------
# Test 3 – same-slot MGET routes to the correct shard for EVERY shard
# ---------------------------------------------------------------------------

def test_mget_arbitrary_single_slot_each_shard(env):
    """For each shard in the cluster, pick a hash tag whose slot is owned by
    that shard, run a small MGET workload, and verify:
      - memtier exits 0
      - cmdstat_mget calls on the owning shard > 0
      - cmdstat_mget calls on ALL OTHER shards == 0
    This confirms that MGET routing is correct for every shard, not just
    whichever shard happens to be shard 0.
    """
    if not env.isCluster():
        env.skip()
        return

    tags_per_shard = _pick_hash_tags_one_per_shard(env)

    for hash_tag, expected_port in tags_per_shard:
        _reset_stats(env)

        extra_args = [
            "--command=MGET {" + hash_tag + "}-__key__",
            "--key-minimum=1",
            "--key-maximum=50",
        ]
        ok, run_config = _run_mget_workload(env, extra_args,
                                            threads=1, clients=2, requests=60)

        failed = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(
                ok,
                message="memtier exited non-zero for hash tag '{}' "
                        "(expected owner port {})".format(
                            hash_tag, expected_port))

            calls_per_shard = _cmdstat_mget_calls_per_shard(env)

            env.assertGreater(
                calls_per_shard.get(expected_port, 0), 0,
                message="hash tag '{}' produced no MGET calls on its "
                        "owning port {} — routing is broken".format(
                            hash_tag, expected_port))

            for port, calls in calls_per_shard.items():
                if port == expected_port:
                    continue
                env.assertEqual(
                    calls, 0,
                    message="hash tag '{}' (owner port {}) leaked {} MGET "
                            "calls onto port {} — cross-shard leakage".format(
                                hash_tag, expected_port, calls, port))
        finally:
            if env.getNumberOfFailedAssertion() > failed:
                debugPrintMemtierOnError(run_config, env)


# ---------------------------------------------------------------------------
# Test 4 – cross-slot MGET is handled gracefully (no crash)
# ---------------------------------------------------------------------------

def test_mget_arbitrary_crossslot_handled_gracefully(env):
    """MGET with plain (non-tagged) keys will likely span multiple hash slots.
    Redis replies with CROSSSLOT errors.  The benchmark must:
      - not crash (no SIGSEGV / SIGABRT — Python subprocess.run returns)
      - surface "CROSSSLOT" in its stderr (benchmark_error_log echoes Redis
        errors there) or exit non-zero
    """
    if not env.isCluster():
        env.skip()
        return

    # Use a small run so errors accumulate quickly without taking long.
    extra_args = [
        "--command=MGET __key__ __key__",
        "--key-minimum=1",
        "--key-maximum=1000",
    ]
    # Run via subprocess directly so we can capture raw stderr without the
    # Benchmark helper hiding the return code from us.
    master_nodes_list = env.getMasterNodesList()
    port = master_nodes_list[0]["port"]

    test_dir = tempfile.mkdtemp()
    results_dir = os.path.join(test_dir, "crossslot_run")
    os.makedirs(results_dir)

    args = [
        MEMTIER_BINARY,
        "-s", "127.0.0.1", "-p", str(port),
        "--cluster-mode",
        "-t", "1", "-c", "1", "--requests", "50",
        "--command=MGET __key__ __key__",
        "--key-minimum=1",
        "--key-maximum=1000",
        "--out-file", os.path.join(results_dir, "mb.stdout"),
        "--json-out-file", os.path.join(results_dir, "mb.json"),
    ]
    if hasattr(env, "envRunner") and env.envRunner is not None:
        if getattr(env.envRunner, "useTLS", False):
            args.append("--tls")

    proc = subprocess.run(args, capture_output=True, timeout=30)

    # The process must have exited cleanly (i.e., not been killed by a
    # signal like SIGSEGV=-11 or SIGABRT=-6).
    env.assertTrue(
        proc.returncode >= 0,
        message="memtier was killed by a signal (returncode={}), "
                "indicating a crash".format(proc.returncode))

    # CROSSSLOT errors should appear somewhere: either memtier stderr or the
    # process may have exited non-zero.  Accept either form.
    stderr_text = proc.stderr.decode(errors="replace")
    crossslot_visible = (
        "CROSSSLOT" in stderr_text
        or proc.returncode != 0
    )
    env.assertTrue(
        crossslot_visible,
        message="expected CROSSSLOT errors to surface in stderr or a "
                "non-zero exit; returncode={}, stderr excerpt: {!r}".format(
                    proc.returncode, stderr_text[:400]))


# ---------------------------------------------------------------------------
# Test 5 – hits land on the correct shard after key pre-load
# ---------------------------------------------------------------------------

def test_mget_arbitrary_cluster_hits_land_on_shard(env):
    """Phase 1: pre-load 500 keys tagged with {ht} using sequential SET so
    they all reside on the shard that owns the {ht} slot.
    Phase 2: run MGET with 3 tagged keys per call and verify:
      - Hits/sec > 0 in the JSON output (keys were found)
      - All MGET calls are counted on the owning shard only (no leakage)
    """
    if not env.isCluster():
        env.skip()
        return

    _flush_cluster(env)
    hash_tag = "ht"
    key_min = 1
    key_max = 500

    # --- Phase 1: pre-load -------------------------------------------------
    set_extra_args = [
        "--command=SET {" + hash_tag + "}-__key__ __data__",
        "--command-key-pattern=S",
        "--key-minimum={}".format(key_min),
        "--key-maximum={}".format(key_max),
    ]
    ok_set, run_config_set = _run_mget_workload(
        env, set_extra_args,
        threads=1, clients=1,
        requests=key_max - key_min + 1,
    )

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok_set, message="pre-load SET phase exited non-zero")
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config_set, env)

    # --- Phase 2: MGET read ------------------------------------------------
    _reset_stats(env)

    mget_extra_args = [
        "--command=MGET {" + hash_tag + "}-__key__",
        "--command-key-pattern=R",
        "--key-minimum={}".format(key_min),
        "--key-maximum={}".format(key_max),
    ]
    ok_mget, run_config_mget = _run_mget_workload(
        env, mget_extra_args,
        threads=2, clients=4,
        requests=200,
    )

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok_mget, message="MGET read phase exited non-zero")

        # All MGET calls must be on the owning shard.
        owner_port = _owning_port(env, hash_tag)
        calls_per_shard = _cmdstat_mget_calls_per_shard(env)

        env.assertGreater(
            calls_per_shard.get(owner_port, 0), 0,
            message="no MGET calls recorded on the owning shard (port {}) "
                    "for hash tag '{{{}}}'".format(owner_port, hash_tag))

        for port, calls in calls_per_shard.items():
            if port == owner_port:
                continue
            env.assertEqual(
                calls, 0,
                message="MGET calls leaked onto non-owner shard port {} "
                        "({} calls) — routing incorrect for tag '{{{}}}'".format(
                            port, calls, hash_tag))

        # Verify hits via the JSON output.  The arbitrary-command path emits
        # stats under "ALL STATS" -> command-name (here: "MGET").  Fall back
        # to checking "Gets" or "Totals" Hits/sec if the command-specific key
        # is not present (depends on memtier version).
        json_path = os.path.join(run_config_mget.results_dir, "mb.json")
        if os.path.isfile(json_path):
            with open(json_path) as f:
                doc = json.load(f)
            all_stats = doc.get("ALL STATS", {})
            # Arbitrary-command workloads report under the command name
            # ("MGET").  "Totals" is intentionally excluded: it always carries
            # Hits/sec=0 for arbitrary commands (hit tracking is not wired
            # up for the --command path), which would produce a false failure.
            hits_sec = None
            for section_key in ("MGET", "Gets"):
                section = all_stats.get(section_key, {})
                if "Hits/sec" in section:
                    hits_sec = float(section["Hits/sec"])
                    break
            if hits_sec is not None:
                env.assertGreater(
                    hits_sec, 0.0,
                    message="Hits/sec is 0 — pre-loaded keys were not found "
                            "by MGET (section looked up: {})".format(section_key))
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config_mget, env)


# ---------------------------------------------------------------------------
# Test 6 – standalone: --multi-key-get is NOT affected by the cluster guard
# ---------------------------------------------------------------------------

def test_mget_standalone_not_affected_by_cluster_restriction(env):
    """In standalone mode --multi-key-get must work correctly.  The cluster-
    mode validation check must not accidentally block standalone users.

    Runs: --ratio=0:10 --multi-key-get=10 -t 1 -c 1 -n 20
    Expects cmdstat_mget.calls == 20 across the (single) master node.
    """
    if env.isCluster():
        env.skip()
        return

    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--ratio=0:10",
            "--multi-key-get=10",
            "--key-minimum=1",
            "--key-maximum=100",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=20)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)

    benchmark = Benchmark.from_json(run_config, benchmark_specs)
    ok = benchmark.run()

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="--multi-key-get in standalone should exit 0")

        master_nodes_connections = env.getOSSMasterNodesConnectionList()
        merged = {"cmdstat_mget": {"calls": 0}}
        for conn in master_nodes_connections:
            stats = conn.execute_command("INFO", "COMMANDSTATS")
            if "cmdstat_mget" in stats:
                merged["cmdstat_mget"]["calls"] += int(
                    stats["cmdstat_mget"]["calls"])

        env.assertEqual(
            merged["cmdstat_mget"]["calls"], 20,
            message="expected 20 MGET calls in standalone (1 per request with "
                    "--ratio=0:10 and --requests=20), got {}".format(
                        merged["cmdstat_mget"]["calls"]))
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)
