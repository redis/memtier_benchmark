"""
Regression tests for issue #389 / `--transaction`.

Background: in `--cluster-mode`, memtier routes each `--command` by hashing
the first key argument, so the keyless transaction-lifecycle commands
(`MULTI`, `EXEC`, `UNWATCH`) end up on a different shard connection than the
keyed commands they wrap, breaking transaction state. The `--transaction`
flag pins one full rotation of `--command` entries to a single shard
connection (the slot owner of the first keyed command in the rotation) so
that a `WATCH`/`MULTI`/.../`EXEC` block stays together on one connection.

These tests run only against the OSS-CLUSTER environment; they assert that
memtier exits cleanly and that memtier's own stderr (where Redis error
responses are echoed via benchmark_error_log) never contains a
broken-transaction error (`unwatch inside MULTI`, `EXEC without MULTI`,
`MULTI calls can not be nested`, `EXECABORT`).
"""

import json
import os
import tempfile

from include import (
    add_required_env_arguments,
    addTLSArgs,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig
from redis.cluster import key_slot

# Server-side error fragments that indicate the transaction state machine
# has been torn between two connections — the exact symptoms from #389.
TRANSACTION_BREAKAGE_PATTERNS = [
    "unwatch inside MULTI",
    "EXEC without MULTI",
    "MULTI calls can not be nested",
    "EXECABORT",
    "DISCARD without MULTI",
]


def _read_stderr(run_config):
    path = "{0}/mb.stderr".format(run_config.results_dir)
    if not os.path.isfile(path):
        return ""
    with open(path) as f:
        return f.read()


def _assert_no_transaction_breakage(env, stderr_text):
    for needle in TRANSACTION_BREAKAGE_PATTERNS:
        env.assertTrue(
            needle not in stderr_text,
            message="server-side transaction error '{}' present — keyless "
                    "commands appear to have been routed to a different "
                    "shard connection than the keyed ones".format(needle),
        )


def _run_transaction_workload(env, extra_command_args, threads=2, clients=4,
                              requests=500):
    """Common helper: run a short --transaction workload and return
    (memtier_ok, run_config, stderr_text)."""
    benchmark_specs = {"name": env.testName, "args": ["--transaction"]}
    addTLSArgs(benchmark_specs, env)
    benchmark_specs["args"].extend(extra_command_args)

    config = get_default_memtier_config(threads=threads, clients=clients,
                                        requests=requests)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)

    benchmark = Benchmark.from_json(run_config, benchmark_specs)
    ok = benchmark.run()
    return ok, run_config, _read_stderr(run_config)


def test_transaction_watch_multi_exec_unwatch(env):
    """The exact failure mode from #389: WATCH/GET/MULTI/SET/EXEC/UNWATCH
    with hash-tagged keys. Must succeed end-to-end with zero server-side
    transaction errors."""
    if not env.isCluster():
        env.skip()
        return

    # Hash-tag forces every key to the same slot. With --transaction we also
    # pin all the keyless commands (MULTI/EXEC/UNWATCH) to that slot's shard.
    cmds = [
        '--command=WATCH {tx}-__key__',
        '--command=GET   {tx}-__key__',
        '--command=MULTI',
        '--command=SET   {tx}-__key__ __data__',
        '--command=EXEC',
        '--command=UNWATCH',
    ]
    ok, run_config, stderr = _run_transaction_workload(env, cmds)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")
        _assert_no_transaction_breakage(env, stderr)
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


def test_transaction_minimal_multi_exec(env):
    """Smaller surface: just MULTI / SET / EXEC. Validates the pin-on-first-
    keyed-command path with no preceding WATCH."""
    if not env.isCluster():
        env.skip()
        return

    cmds = [
        '--command=MULTI',
        '--command=SET   {mx}-__key__ __data__',
        '--command=INCR  {mx}-counter',
        '--command=EXEC',
    ]
    ok, run_config, stderr = _run_transaction_workload(env, cmds)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok)
        _assert_no_transaction_breakage(env, stderr)
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


def test_transaction_with_discard(env):
    """DISCARD is also a keyless transaction terminator and must follow the
    same pinning as EXEC."""
    if not env.isCluster():
        env.skip()
        return

    cmds = [
        '--command=MULTI',
        '--command=SET   {dx}-__key__ __data__',
        '--command=DISCARD',
    ]
    ok, run_config, stderr = _run_transaction_workload(env, cmds)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok)
        _assert_no_transaction_breakage(env, stderr)
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


def test_transaction_in_standalone_is_noop(env):
    """In standalone, --transaction is accepted but does nothing: each client
    already runs on a single connection, so the rotation order is naturally
    preserved. The benchmark must complete cleanly with no server-side
    transaction breakage."""
    if env.isCluster():
        env.skip()
        return

    # No hash tag needed here — there's only one shard.
    cmds = [
        '--command=MULTI',
        '--command=SET   __key__ __data__',
        '--command=INCR  counter',
        '--command=EXEC',
    ]
    ok, run_config, stderr = _run_transaction_workload(env, cmds)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")
        _assert_no_transaction_breakage(env, stderr)
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


def test_transaction_requires_command(env):
    """--transaction with no --command must be rejected (any env)."""
    import subprocess
    from include import MEMTIER_BINARY

    master_nodes_list = env.getMasterNodesList()
    port = master_nodes_list[0]["port"]
    args = [
        MEMTIER_BINARY,
        "-s", "127.0.0.1", "-p", str(port),
        "-t", "1", "-c", "1", "--requests", "1",
        "--transaction",
    ]
    if env.isCluster():
        args.append("--cluster-mode")
    if hasattr(env, "envRunner") and env.envRunner is not None:
        if getattr(env.envRunner, "useTLS", False):
            args.append("--tls")

    proc = subprocess.run(args, capture_output=True, timeout=15)
    env.assertNotEqual(
        proc.returncode, 0,
        message="--transaction without --command should fail validation")
    env.assertTrue(
        b"--transaction requires" in proc.stderr,
        message="expected stderr to mention requirement on --command; "
                "got: {!r}".format(proc.stderr[:400]))


# ---------------------------------------------------------------------------
# Ingestion + slot-coverage tests
#
# The five tests above only check the absence of server-side transaction
# errors. The tests below take it further: they flush the cluster, run a
# write workload through MULTI/EXEC, then inspect the cluster to confirm
# that
#   1. the actual write side-effects landed in Redis (ingestion happened),
#   2. they landed on the SLOT OWNER of the chosen hash tag and nowhere
#      else (pin correctness),
#   3. multiple distinct keys were touched within that slot (not just one
#      key getting overwritten),
#   4. memtier can correctly target hash tags whose slots span every shard
#      of the cluster (general routing, not hardcoded to shard 0).
# ---------------------------------------------------------------------------

def _master_conns_by_port(env):
    """Return {port:int -> Redis connection} for every master in the
    cluster. Keying by port avoids the pitfall that two distinct Redis
    client instances pointing at the same server don't compare/hash as
    equal."""
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
    """Return the port of the master that owns the slot for the given hash
    tag, using CLUSTER SLOTS as the source of truth (so the test stays
    correct under any shard count or non-default slot layout)."""
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
    """Return [(hash_tag, owner_port)] with one short hash tag per master
    shard, each tag chosen so its slot is owned by a distinct master."""
    if candidates is None:
        # Compact, distinct hash tags that span the typical 3-shard split
        # 0-5461 / 5462-10923 / 10924-16383. The pool is intentionally
        # larger so the helper still finds one per shard under alternate
        # cluster layouts.
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
        message="couldn't find a hash-tag tag per shard (got {} for {} "
                "shards). Extend the candidate list.".format(
                    len(chosen), master_count))
    return chosen


def _run_ingestion(env, hash_tag, requests, threads=1, clients=1,
                   key_max=None):
    """Run a --transaction MULTI/SET/EXEC workload that writes
    `{tag}-key-<n>` keys. Returns (ok, run_config, stderr_text)."""
    if key_max is None:
        # Default: cover the request count so every request can land on a
        # distinct key value (--key-pattern=R picks randomly, so duplicates
        # are fine — the cluster-side count check uses set semantics).
        key_max = requests
    cmds = [
        '--command=MULTI',
        '--command=SET {' + hash_tag + '}-key-__key__ memtier-ingest-data',
        '--command=EXEC',
        '--key-minimum=1',
        '--key-maximum={}'.format(key_max),
    ]
    return _run_transaction_workload(env, cmds, threads=threads,
                                     clients=clients, requests=requests)


def test_transaction_ingestion_lands_on_pinned_shard(env):
    """Ingestion through MULTI/EXEC must (a) actually write keys to Redis,
    (b) put every key on the slot owner of the hash tag, (c) leave the
    other shards empty, and (d) touch more than one distinct key value
    inside the slot."""
    if not env.isCluster():
        env.skip()
        return

    _flush_cluster(env)
    hash_tag = 'ing'
    requests = 200
    ok, run_config, stderr = _run_ingestion(env, hash_tag, requests=requests,
                                            key_max=50)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")
        _assert_no_transaction_breakage(env, stderr)

        sizes = _dbsize_per_shard(env)
        owner_port = _owning_port(env, hash_tag)
        owner_size = sizes[owner_port]

        # (a) ingestion happened: at least one key landed.
        env.assertGreater(
            owner_size, 0,
            message="no keys landed on the slot owner of {{{}}} (port {}); "
                    "the transaction pin is broken".format(
                        hash_tag, owner_port))

        # (d) multiple distinct keys touched — we expect close to key_max
        # unique keys for requests=200, key_max=50, --key-pattern=R.
        # Allow some slack for unlikely RNG collisions; require at least
        # half the range to be exercised.
        env.assertGreater(
            owner_size, 25,
            message="only {} distinct keys present on owner — expected the "
                    "rotation to touch most of key_max=50 unique keys".format(
                        owner_size))

        # (b)+(c) every other shard is empty: no MOVED leakage, no
        # cross-shard writes from misrouted SETs.
        for port, sz in sizes.items():
            if port == owner_port:
                continue
            env.assertEqual(
                sz, 0,
                message="non-owner shard on port {} unexpectedly has {} "
                        "keys — keyed commands escaped the pin".format(
                            port, sz))
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


def test_transaction_routes_correctly_for_every_shard(env):
    """--transaction must work for *any* slot, not just the slot that
    happens to hash to shard 0. For each shard in the cluster, pick a hash
    tag whose slot Redis assigns to that shard, run a small ingestion,
    and verify the keys land exclusively on the expected shard."""
    if not env.isCluster():
        env.skip()
        return

    tags_per_shard = _pick_hash_tags_one_per_shard(env)

    for hash_tag, expected_port in tags_per_shard:
        _flush_cluster(env)
        ok, run_config, stderr = _run_ingestion(env, hash_tag, requests=60,
                                                key_max=20)

        failed = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(
                ok,
                message="memtier exited non-zero for hash tag '{}' (expected "
                        "owner on port {})".format(hash_tag, expected_port))
            _assert_no_transaction_breakage(env, stderr)

            sizes = _dbsize_per_shard(env)
            env.assertGreater(
                sizes[expected_port], 0,
                message="hash tag '{}' produced no keys on its owner port "
                        "{} — slot routing broken for this shard".format(
                            hash_tag, expected_port))
            for port, sz in sizes.items():
                if port == expected_port:
                    continue
                env.assertEqual(
                    sz, 0,
                    message="hash tag '{}' (owner port {}) leaked {} keys "
                            "onto port {}".format(hash_tag, expected_port,
                                                  sz, port))
        finally:
            if env.getNumberOfFailedAssertion() > failed:
                debugPrintMemtierOnError(run_config, env)


def test_transaction_sequential_ingestion_full_population(env):
    """Sequential key pattern over a large range must land every key in
    the cluster exactly once. With --transaction MULTI/SET/EXEC,
    --key-pattern=S, --key-minimum=1 --key-maximum=N and --requests=N on
    a single client/single thread, memtier walks the key range in order:
    keys 1..N are each written exactly once, so the cluster ends up with
    DBSIZE == N keys all on the slot owner of the hash tag.

    This is the strict regression check the issue #389 repro really
    needs — the looser ingestion test above only verifies that ~half
    of a small range was touched; this one pins down the exact count
    against a large range, catching off-by-one and silently-dropped-
    transactions regressions.
    """
    if not env.isCluster():
        env.skip()
        return

    _flush_cluster(env)
    hash_tag = 'seq'
    # Keep this large enough to exercise the pin across many rotations
    # but small enough that CI doesn't time out (memtier on a local
    # 3-shard cluster does roughly ~20-40k req/sec for a 3-command
    # MULTI/SET/EXEC rotation, so 100k unique keys completes well under
    # a minute even under sanitizers).
    #
    # --requests counts every individual command, not rotations. The
    # rotation is 3 commands (MULTI / SET / EXEC), so to actually emit N
    # SET commands and hit every value in --key-pattern=S range 1..N, we
    # need --requests = 3*N.
    n = 100000

    cmds = [
        '--command=MULTI',
        '--command=SET {' + hash_tag + '}-key-__key__ memtier-seq-data',
        '--command-key-pattern=S',  # must follow --command=SET so it applies to SET, not EXEC
        '--command=EXEC',
        '--key-prefix=',            # empty prefix so keys are bare integers: {seq}-key-1, {seq}-key-2, …
        '--key-minimum=1',
        '--key-maximum={}'.format(n),
    ]
    ok, run_config, stderr = _run_transaction_workload(
        env, cmds, threads=1, clients=1, requests=3 * n)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")
        _assert_no_transaction_breakage(env, stderr)

        sizes = _dbsize_per_shard(env)
        owner_port = _owning_port(env, hash_tag)

        # Exact match: every requested key value 1..n was written once.
        env.assertEqual(
            sizes[owner_port], n,
            message="expected DBSIZE={} on owner port {} after sequential "
                    "ingestion, got {}".format(n, owner_port,
                                               sizes[owner_port]))

        # And every other shard is still empty.
        for port, sz in sizes.items():
            if port == owner_port:
                continue
            env.assertEqual(
                sz, 0,
                message="non-owner shard on port {} has {} keys after "
                        "single-slot sequential ingestion".format(port, sz))

        # Sanity-check the actual key/value of a few entries to make sure
        # we wrote the expected payload (not just empty placeholders or
        # MULTI-discarded keys). Use direct GETs against the owning
        # master.
        owner_conn = next(c for c in env.getOSSMasterNodesConnectionList()
                          if int(c.connection_pool.connection_kwargs[
                              "port"]) == owner_port)
        for key_idx in (1, n // 2, n):
            key = "{{{}}}-key-{}".format(hash_tag, key_idx)
            value = owner_conn.execute_command("GET", key)
            env.assertEqual(
                value, b"memtier-seq-data",
                message="GET {!r} on port {} returned {!r}, expected "
                        "'memtier-seq-data' — transaction commit may have "
                        "been silently dropped".format(
                            key, owner_port, value))
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


def test_transaction_appears_in_config_and_json_output(env):
    """--transaction must be reflected in both the printed config (stderr)
    and the JSON output file so tooling that parses memtier output can
    detect the mode without re-parsing the command line."""
    if not env.isCluster():
        env.skip()
        return

    cmds = [
        '--show-config',
        '--command=MULTI',
        '--command=SET {cfg}-__key__ __data__',
        '--command=EXEC',
        '--command-key-pattern=R',
    ]
    ok, run_config, stderr = _run_transaction_workload(env, cmds, requests=60)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero")

        # Printed config block (emitted to stderr via --show-config) must
        # contain "transaction = yes".
        env.assertTrue(
            "transaction = yes" in stderr,
            message="'transaction = yes' not found in memtier stderr config block; "
                    "got stderr excerpt: {!r}".format(stderr[:600]))

        # JSON output file must have configuration.transaction == "true".
        json_path = os.path.join(run_config.results_dir, "mb.json")
        env.assertTrue(
            os.path.isfile(json_path),
            message="mb.json not found at {}".format(json_path))
        with open(json_path) as f:
            doc = json.load(f)
        cfg_section = doc.get("configuration", {})
        env.assertEqual(
            cfg_section.get("transaction"), "true",
            message="JSON configuration.transaction expected 'true', got {!r}".format(
                cfg_section.get("transaction")))
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)


def test_transaction_keyless_only_rotation(env):
    """A rotation whose only commands are keyless (MULTI + EXEC with no
    keyed command inside) must complete without hanging or asserting.

    Without a keyed command the lookahead falls back to the current
    connection for the pin and never generates a staged key. All non-pin
    connections get schedule_fill() wakeups on every rotation boundary but
    are immediately re-blocked by hold_pipeline(). This exercises the
    liveness of that fast-block/wake path."""
    if not env.isCluster():
        env.skip()
        return

    cmds = [
        '--command=MULTI',
        '--command=EXEC',
    ]
    ok, run_config, stderr = _run_transaction_workload(env, cmds,
                                                       threads=2, clients=2,
                                                       requests=200)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier_benchmark exited non-zero (possible hang or crash)")
        _assert_no_transaction_breakage(env, stderr)
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)
