"""Collection cursor iterators: real Redis replies, wire arguments, and JSON stats.

The MONITOR oracle replays the captured read-only commands after the run against
unchanged collections. This verifies actual cursor progression, not merely that
requests were counted under the continuation label. All network helpers inherit
RLTest's TLS configuration.
"""
import ast
import json
import os
import re
import tempfile
from collections import defaultdict

from redis.exceptions import ResponseError

from include import (
    add_required_env_arguments,
    addTLSArgs,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig


COMMANDS = ("HSCAN", "SSCAN", "ZSCAN")
PROTOCOLS = ("redis", "resp3")
_QUOTED_ARGUMENT = re.compile(r'"(?:[^"\\]|\\.)*"')


def _quote(value):
    """Encode a byte argument for memtier's Redis-style command tokenizer."""
    return '"' + ''.join(
        chr(byte) if 32 <= byte < 127 and byte not in (34, 92)
        else '\\x{:02x}'.format(byte) for byte in value
    ) + '"'


def _preload(conn, command, keys, count=640):
    # Long noninteger members force dictionary/skiplist encoding on old and new
    # Redis releases; compact encodings may return everything despite COUNT 1.
    members = ["member:{:04d}:{}".format(i, "x" * 80) for i in range(count)]
    pipe = conn.pipeline()
    for key in keys:
        pipe.delete(key)
        if command == "HSCAN":
            pipe.hset(key, mapping={member: "value" for member in members})
        elif command == "SSCAN":
            pipe.sadd(key, *members)
        else:
            pipe.zadd(key, {member: i for i, member in enumerate(members)})
    pipe.execute()


def _build(env, directory, args, requests, threads, clients, pipeline=1):
    config = get_default_memtier_config(threads=threads, clients=clients,
                                        requests=requests)
    config["memtier_benchmark"]["pipeline"] = pipeline
    specs = {"name": env.testName, "args": list(args)}
    addTLSArgs(specs, env)
    add_required_env_arguments(specs, config, env, env.getMasterNodesList())
    run_config = RunConfig(directory, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)
    return Benchmark.from_json(run_config, specs), run_config


def _read_file(run_config, name):
    path = os.path.join(run_config.results_dir, name)
    if not os.path.isfile(path):
        return ""
    with open(path) as stream:
        return stream.read()


def _capture(conn, benchmark):
    """Capture RESP arguments without redis-py's lossy joined MONITOR string."""
    records = []
    marker = b"collection-scan-test-finished"
    with conn.monitor() as monitor:
        ok = benchmark.run()
        conn.echo(marker)
        while True:
            raw = monitor.connection.read_response()
            if isinstance(raw, bytes):
                raw = raw.decode("latin1")
            # Redis MONITOR uses C-escaped quoted strings, which are also valid
            # Python byte literals. latin1 preserves bytes before unescaping.
            location, command = raw.split("] ", 1)
            args = tuple(ast.literal_eval('b' + match.group(0))
                         for match in _QUOTED_ARGUMENT.finditer(command))
            if args == (b"ECHO", marker):
                break
            if args and args[0].upper() in (b"SCAN", b"HSCAN", b"SSCAN", b"ZSCAN"):
                records.append((location.split(" ", 1)[1], args))
    return ok, records


def _run(env, command, args=(), requests=80, threads=1, clients=1,
         protocol="redis", incremental=True, initial_cursor=b"0"):
    conn = env.getConnection()
    options = ["--command", command, "--protocol", protocol] + list(args)
    if incremental:
        options.append("--scan-incremental-iteration")
    with tempfile.TemporaryDirectory() as directory:
        benchmark, run_config = _build(env, directory, options, requests,
                                       threads, clients)
        failed = env.getNumberOfFailedAssertion()
        try:
            ok, records = _capture(conn, benchmark)
            env.assertTrue(ok, message="Benchmark failed: {}".format(command))
            results = json.loads(_read_file(run_config, "mb.json"))
            stats = results["ALL STATS"]
            env.assertEqual(stats["Totals"]["Count"], requests * threads * clients)
            env.assertEqual(len(records), requests * threads * clients)
            if incremental:
                name = command.split()[0].capitalize()
                stdout = _read_file(run_config, "mb.stdout")
                env.assertContains(name.upper() + " 0", stdout)
                env.assertContains(name.upper() + " <cursor>", stdout)
                initial = stats[name + " 0s"]["Count"]
                continuation = stats[name + " <cursor>s"]["Count"]
                cursor_pos = 1 if name == "Scan" else 2
                env.assertEqual(initial, sum(args[cursor_pos] == initial_cursor
                                             for _, args in records))
                env.assertEqual(initial + continuation, requests * threads * clients)
            return stats, records, _read_file(run_config, "mb.stderr")
        finally:
            if env.getNumberOfFailedAssertion() > failed:
                debugPrintMemtierOnError(run_config, env)


def _assert_walk(env, conn, records, cap=0, sequential_keys=None, initial_cursor=b"0"):
    """Compare sent cursors to Redis replies and check cycle argument stability."""
    states = {}
    initials = defaultdict(list)
    continuations = 0
    for client, args in records:
        pos = 1 if args[0].upper() == b"SCAN" else 2
        expected, previous, iterations = states.get(client, (initial_cursor, None, 0))
        env.assertEqual(args[pos], expected, message="Wrong cursor: {!r}".format(args))
        if args[pos] == initial_cursor:
            initials[client].append(args[1] if pos == 2 else b"")
            iterations = 0
        else:
            continuations += 1
            env.assertEqual(args[:pos], previous[:pos], message="Collection changed mid-walk")
            env.assertEqual(args[pos + 1:], previous[pos + 1:],
                            message="Options changed mid-walk")
            iterations += 1
        try:
            reply = conn.execute_command(*args)
            expected = str(int(reply[0])).encode()
        except ResponseError:
            expected = initial_cursor
        if expected == b"0" or (cap and iterations >= cap):
            expected = initial_cursor
        states[client] = expected, args, iterations
    if sequential_keys:
        for keys in initials.values():
            env.assertGreater(len(keys), 1)
            env.assertEqual(keys, [sequential_keys[i % len(sequential_keys)]
                                   for i in range(len(keys))])
    return initials, continuations


def test_collection_scan_literal_cursor_walks(env):
    """RESP2/3 walks preserve spaces, quotes, backslashes and embedded NULs."""
    env.skipOnCluster()
    conn = env.getConnection()
    key = b'collection key:"\\\x00'
    for command in COMMANDS:
        _preload(conn, command, [key])
        for protocol in PROTOCOLS:
            _, records, _ = _run(env, '{} {} 0 MATCH "member:*" COUNT 100'.format(
                command.lower(), _quote(key)), protocol=protocol)
            initials, continuations = _assert_walk(env, conn, records)
            env.assertGreater(continuations, 0)
            env.assertGreater(len(next(iter(initials.values()))), 1)
            env.assertTrue(all(args[1] == key for _, args in records))


def test_collection_scan_generated_keys_and_caps(env):
    """Independent clients pin their generated key and advance once per cycle."""
    env.skipOnCluster()
    conn = env.getConnection()
    # Exercise both the stack and heap paths for placeholder affixes.
    for command in COMMANDS:
        for prefix in ("{", "p" * 1100):
            keys = [(prefix + "iter:" + str(i) + "}:tail").encode() for i in range(1, 4)]
            _preload(conn, command, keys)
            _, records, _ = _run(env, command + " " + prefix + "__key__}:tail 0 COUNT 1", [
                "--command-key-pattern", "S", "--key-prefix", "iter:",
                "--key-minimum", "1", "--key-maximum", "3",
                "--scan-incremental-max-iterations", "3",
            ], requests=24, threads=2, clients=2)
            initials, continuations = _assert_walk(env, conn, records, cap=3,
                                                  sequential_keys=keys)
            env.assertEqual(len(initials), 4)
            env.assertEqual(sum(map(len, initials.values())), 24)
            env.assertEqual(continuations, 72)


def test_collection_scan_generated_keys_complete_cycles(env):
    """Natural cursor exhaustion and --transaction both advance generated keys."""
    env.skipOnCluster()
    conn = env.getConnection()
    keys = [b"iter:1", b"iter:2", b"iter:3"]
    for command in COMMANDS:
        _preload(conn, command, keys)
        for extra in ([], ["--transaction"]):
            _, records, _ = _run(env, command + " __key__ 0 COUNT 100", [
                "--command-key-pattern", "S", "--key-prefix", "iter:",
                "--key-minimum", "1", "--key-maximum", "3",
            ] + extra)
            _, continuations = _assert_walk(env, conn, records, sequential_keys=keys)
            env.assertGreater(continuations, 0)


def test_collection_scan_generated_match_and_data(env):
    """Generated MATCH arguments and variable-size __data__ keys stay pinned."""
    env.skipOnCluster()
    conn = env.getConnection()
    for command in COMMANDS:
        _preload(conn, command, [b"data", b"xxxx", b"xxxxx", b"xxxxxx"])
        for expression, extra in (
            (command + " data 0 MATCH __key__ COUNT 100", [
                "--command-key-pattern", "S", "--key-prefix", "member:",
                "--key-minimum", "1", "--key-maximum", "3"]),
            (command + " __data__ 0 COUNT 100", ["--data-size-range", "4-6"]),
            (command + " data 0 MATCH __data__ COUNT 100", [
                "--data-size", "16", "--random-data"]),
        ):
            _, records, _ = _run(env, expression, extra)
            _, continuations = _assert_walk(env, conn, records)
            env.assertGreater(continuations, 0)


def test_collection_scan_options_and_empty_pages(env):
    """Bare scans and empty MATCH results still consume the returned cursor."""
    env.skipOnCluster()
    conn = env.getConnection()
    for command in COMMANDS:
        _preload(conn, command, [b"options"])
        for options in ("", 'MATCH ""', 'MATCH "absent:*" COUNT 100',
                        'COUNT 100 MATCH "absent:*\\x00"'):
            _, records, _ = _run(env, command + " options 0 " + options,
                                 requests=80)
            _, continuations = _assert_walk(env, conn, records)
            env.assertGreater(continuations, 0)
            # Check the binary and empty MATCH arguments on every request.
            expected = b"absent:*\x00" if "\\x00" in options else b""
            if options == 'MATCH ""' or "\\x00" in options:
                for _, args in records:
                    env.assertEqual(args[args.index(b"MATCH") + 1], expected)


def test_collection_scan_missing_small_and_wrongtype(env):
    """One-page/missing collections and server errors always restart at zero."""
    env.skipOnCluster()
    conn = env.getConnection()
    for command in COMMANDS:
        for mode in ("missing", "small", "wrongtype", "invalid-option"):
            key = b"edge"
            conn.delete(key)
            if mode in ("small", "invalid-option"):
                _preload(conn, command, [key], count=1)
            elif mode == "wrongtype":
                conn.set(key, "string")
            options = " COUNT invalid" if mode == "invalid-option" else ""
            stats, records, stderr = _run(env, command + " edge 0" + options,
                                          requests=12)
            _assert_walk(env, conn, records)
            env.assertEqual(stats[command.capitalize() + " 0s"]["Count"], 12)
            env.assertEqual(stats[command.capitalize() + " <cursor>s"]["Count"], 0)
            if mode == "wrongtype":
                env.assertContains("WRONGTYPE", stderr)
            elif mode == "invalid-option":
                env.assertContains("ERR", stderr)


def test_collection_scan_without_incremental_mode(env):
    """Opt-in behavior: ordinary arbitrary collection scans keep cursor zero."""
    env.skipOnCluster()
    conn = env.getConnection()
    for command in COMMANDS:
        _preload(conn, command, [b"ordinary"])
        stats, records, _ = _run(env, command + " ordinary 0 COUNT 1", requests=12,
                                 incremental=False)
        env.assertEqual(stats[command.capitalize() + "s"]["Count"], 12)
        env.assertTrue(all(args[2] == b"0" for _, args in records))


def test_collection_scan_invalid_configurations(env):
    """Reject missing operands, pipelining, multiple commands, and cluster mode."""
    for command in COMMANDS:
        cases = [
            ([], "SCAN, SSCAN, HSCAN or ZSCAN"),
            (["--command", command], "cursor"),
            (["--command", command + " key"], "cursor"),
            (["--command", command + " key 0", "--pipeline", "2"], "pipeline"),
            (["--command", command + " key 0", "--command", "PING"],
             "exactly one --command (SCAN, SSCAN, HSCAN or ZSCAN)"),
            (["--command", command + " key 0", "--cluster-mode"], "cluster"),
        ]
        # Cluster rejection is deliberately checked in the cluster matrix too;
        # all other cases require standalone validation ordering.
        if env.isCluster():
            cases = cases[-1:]
        for args, expected in cases:
            with tempfile.TemporaryDirectory() as directory:
                benchmark, run_config = _build(env, directory,
                    args + ["--scan-incremental-iteration"], 1, 1, 1)
                env.assertFalse(benchmark.run())
                env.assertContains(expected, _read_file(run_config, "mb.stderr"))


def test_scan_quoted_match_continuations(env):
    """The shared continuation builder preserves SCAN's quoted MATCH options."""
    env.skipOnCluster()
    conn = env.getConnection()
    pipe = conn.pipeline()
    for i in range(200):
        pipe.set(b"quoted key:\x00:" + str(i).encode(), "value")
    pipe.execute()
    stats, records, _ = _run(env, 'SCAN 0 MATCH "quoted key:\\x00:*" COUNT 20')
    # Keyspace dictionaries may rehash in cron even without writes, so a later
    # replay can legitimately return different cursors. Collection dictionaries
    # in the tests above are not subject to this background keyspace rehash.
    env.assertGreater(stats["Scan <cursor>s"]["Count"], 0)
    env.assertTrue(all(args[3] == b"quoted key:\x00:*" for _, args in records))


def test_collection_scan_empty_literal_key(env):
    """An empty collection name remains an empty RESP argument on every page."""
    env.skipOnCluster()
    conn = env.getConnection()
    for command in COMMANDS:
        _preload(conn, command, [b""])
        _, records, _ = _run(env, command + ' "" 0 COUNT 100', requests=24)
        _, continuations = _assert_walk(env, conn, records)
        env.assertGreater(continuations, 0)
        env.assertTrue(all(args[1] == b"" for _, args in records))


def test_collection_scan_nonzero_initial_cursor(env):
    """A caller-supplied start cursor is retained when starting each new walk."""
    env.skipOnCluster()
    conn = env.getConnection()
    for command in COMMANDS:
        _preload(conn, command, [b"resume"])
        cursor = int(conn.execute_command(command, "resume", 0, "COUNT", 100)[0])
        env.assertGreater(cursor, 0)
        start = str(cursor).encode()
        _, records, _ = _run(env, command + " resume " + str(cursor) + " COUNT 100",
                             requests=24, initial_cursor=start)
        initials, continuations = _assert_walk(env, conn, records, initial_cursor=start)
        env.assertGreater(continuations, 0)
        env.assertGreater(len(next(iter(initials.values()))), 1)


def test_hscan_novalues(env):
    """Pass HSCAN NOVALUES through unchanged when the Redis version supports it."""
    env.skipOnCluster()
    conn = env.getConnection()
    _preload(conn, "HSCAN", [b"fields"])
    try:
        conn.execute_command("HSCAN", "fields", 0, "NOVALUES")
    except ResponseError:
        env.skip()
        return
    for protocol in PROTOCOLS:
        _, records, _ = _run(env, "HSCAN fields 0 COUNT 100 NOVALUES",
                             requests=24, protocol=protocol)
        _, continuations = _assert_walk(env, conn, records)
        env.assertGreater(continuations, 0)
        env.assertTrue(all(args[-1] == b"NOVALUES" for _, args in records))


def test_collection_scan_reconnect_preserves_walk(env):
    """Planned reconnects preserve a logical client's cursor and generated key."""
    env.skipOnCluster()
    conn = env.getConnection()
    keys = [b"reconnect:1", b"reconnect:2"]
    for command in COMMANDS:
        _preload(conn, command, keys)
        _, records, _ = _run(env, command + " __key__ 0 COUNT 100", [
            "--reconnect-interval", "2", "--command-key-pattern", "S",
            "--key-prefix", "reconnect:", "--key-minimum", "1", "--key-maximum", "2",
        ], requests=24)
        env.assertGreater(len({client for client, _ in records}), 1)
        logical_records = [("single-client", args) for _, args in records]
        _, continuations = _assert_walk(env, conn, logical_records, sequential_keys=keys)
        env.assertGreater(continuations, 0)
