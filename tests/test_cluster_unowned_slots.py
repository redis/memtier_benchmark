"""A valid partial CLUSTER SLOTS map must not lose keys or spin (#685)."""

import binascii
import csv
import json
import os
import socketserver
import subprocess
import tempfile
import threading
import time
from include import MEMTIER_BINARY


def _slot(key):
    start = key.find(b"{")
    end = key.find(b"}", start + 1) if start >= 0 else -1
    if end > start + 1:
        key = key[start + 1 : end]
    return binascii.crc_hqx(key, 0) % 16384


class _Node(socketserver.ThreadingTCPServer):
    daemon_threads = True

    def __init__(
        self, ranges, replica=False, heal=False, drop_after=0, disconnect_on_drop=True, auth_delay=0
    ):
        super().__init__(("127.0.0.1", 0), _Handler)
        self.root = self
        self.extra = []
        self.ranges = ranges
        self.replica = _Node(ranges) if replica else None
        if self.replica:
            self.replica.root = self
        self.auth_delay = auth_delay
        self.auth_closes = 0
        self.auth_attempts = 0
        self.slots_delay = 0
        self.workload = []
        self.readonly_delay = 0
        self.heal = heal
        self.drop_after = drop_after
        self.dropped = False
        self.disconnect_on_drop = disconnect_on_drop
        self.slots_calls = 0
        self.requests = []
        self.error_keys = []
        self.errors = []
        self.lock = threading.Lock()

    def topology(self):
        root = self.root
        if root.slots_calls:
            time.sleep(root.slots_delay)
        with root.lock:
            root.slots_calls += 1
            if root.heal and root.error_keys:
                root.ranges = [(0, 16383)]
            ranges = list(root.ranges)
        nodes = [root] + ([root.replica] if root.replica else [])
        reply = b"*" + str(len(ranges)).encode() + b"\r\n"
        for low, high in ranges:
            reply += (
                b"*"
                + str(2 + len(nodes)).encode()
                + b"\r\n:"
                + str(low).encode()
                + b"\r\n:"
                + str(high).encode()
                + b"\r\n"
            )
            for node in nodes:
                reply += b"*2\r\n$9\r\n127.0.0.1\r\n:" + str(node.server_address[1]).encode() + b"\r\n"
        return reply


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        node = self.server
        root = node.root
        authenticated = False
        multi = False
        dirty = False
        queued = []
        self.connection.settimeout(15)
        try:
            while True:
                line = self.rfile.readline()
                if not line:
                    return
                args = []
                for _ in range(int(line[1:])):
                    size = int(self.rfile.readline()[1:])
                    args.append(self.rfile.read(size))
                    assert self.rfile.read(2) == b"\r\n"
                verb = args[0].upper()
                if verb == b"AUTH":
                    assert args[-1] == b"dummy-685"
                    time.sleep(node.auth_delay)
                    with root.lock:
                        node.auth_attempts += 1
                        if node.auth_closes:
                            node.auth_closes -= 1
                            return
                    authenticated = True
                    reply = b"+OK\r\n"
                else:
                    assert authenticated
                    if verb in (b"PING", b"SET", b"GET", b"MGET"):
                        with root.lock:
                            node.workload.append(tuple(args))
                    if verb == b"CLUSTER":
                        assert not multi, "CLUSTER SLOTS injected into MULTI"
                        reply = node.topology()
                    elif verb == b"PING":
                        reply = b"+PONG\r\n"
                    elif verb in (b"READONLY", b"WATCH", b"DISCARD"):
                        if verb == b"READONLY":
                            time.sleep(node.readonly_delay)
                        reply = b"+OK\r\n"
                        if verb == b"DISCARD":
                            multi = dirty = False
                            queued = []
                    elif verb == b"MULTI":
                        multi = True
                        reply = b"+OK\r\n"
                    elif verb == b"EXEC":
                        reply = (
                            b"-EXECABORT Transaction discarded\r\n"
                            if dirty
                            else b"*" + str(len(queued)).encode() + b"\r\n" + b"".join(queued)
                        )
                        multi = dirty = False
                        queued = []
                    else:
                        assert verb in (b"SET", b"GET", b"MGET"), args
                        keys = args[1:] if verb == b"MGET" else args[1:2]
                        with root.lock:
                            node.requests.append((verb, tuple(keys)))
                            if root.drop_after and not root.dropped and len(root.requests) == root.drop_after:
                                root.dropped = True
                                root.ranges = [(0, 3497), (3499, 16383)]
                                if root.disconnect_on_drop:
                                    return
                            missing = any(
                                not any(low <= _slot(key) <= high for low, high in root.ranges)
                                for key in keys
                            )
                            if missing:
                                root.error_keys.extend(keys)
                        reply = (
                            b"-CLUSTERDOWN Hash slot not served\r\n"
                            if missing
                            else (
                                b"+OK\r\n"
                                if verb == b"SET"
                                else (
                                    b"$-1\r\n"
                                    if verb == b"GET"
                                    else b"*" + str(len(keys)).encode() + b"\r\n" + b"$-1\r\n" * len(keys)
                                )
                            )
                        )
                        if multi:
                            if missing:
                                dirty = True
                            else:
                                queued.append(reply)
                                reply = b"+QUEUED\r\n"
                self.wfile.write(reply)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as error:
            node.errors.append(repr(error))


def _run(env, node, args, expected_exit=0, timeout=8, seed_replica=False):
    if env.isCluster() or env.useTLS:
        node.server_close()
        if node.replica:
            node.replica.server_close()
        for extra in node.extra:
            extra.server_close()
        env.skip()  # The fixture owns its complete topology and plaintext sockets.
    nodes = [node] + ([node.replica] if node.replica else []) + node.extra
    threads = [threading.Thread(target=n.serve_forever) for n in nodes]
    for t in threads:
        t.start()
    try:
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "stats.json")
            failures = os.path.join(directory, "failed.csv")
            command = [
                MEMTIER_BINARY,
                "-s",
                "127.0.0.1",
                "-p",
                str((node.replica if seed_replica else node).server_address[1]),
                "--authenticate=dummy-685",
                "--cluster-mode",
                "-t",
                "1",
                "-c",
                "1",
                "--pipeline=1",
                "--key-minimum=1",
                "--key-maximum=1",
                "--connection-stage-timeout=2",
                "--hide-histogram",
                "--json-out-file=" + path,
                "--failed-keys-file=" + failures,
            ]
            if not any(arg.startswith("--command=") for arg in args):
                command.extend(["--ratio=1:0", "--key-pattern=S:S"])
            command += args
            started = time.monotonic()
            result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
            node.run_elapsed = time.monotonic() - started
            env.assertEqual(result.returncode, expected_exit, message=result.stderr[-3000:])
            with open(path) as source:
                stats = json.load(source)["ALL STATS"]
            rows = []
            if os.path.exists(failures):
                with open(failures) as source:
                    rows = list(csv.DictReader(source))
            env.assertEqual([error for n in nodes for error in n.errors], [])
            return stats, rows, result.stderr
    finally:
        for n in nodes:
            n.shutdown()
            n.server_close()
        for t in threads:
            t.join(timeout=3)


def _hole(**kwargs):
    return _Node([(0, 3497), (3499, 16383)], **kwargs)


def test_unowned_requests_are_sent_and_counted(env):
    node = _hole()
    stats, failures, _ = _run(env, node, ["--requests=100"])
    env.assertEqual(stats["Totals"]["Count"], 100)
    env.assertEqual(len(node.requests), 100)
    env.assertEqual(len(failures), 100)
    env.assertTrue(all(keys == (b"memtier-1",) for _, keys in node.requests))
    env.assertGreaterEqual(node.slots_calls, 2)
    # Bootstrap, one immediate refresh, then at most one per elapsed second.
    env.assertLessEqual(node.slots_calls, 2 + int(node.run_elapsed))


def test_unowned_timed_run_ends_and_refresh_is_bounded(env):
    node = _hole()
    stats, failures, _ = _run(env, node, ["--test-time=2", "--rate-limiting=100"])
    env.assertGreater(stats["Totals"]["Count"], 0)
    env.assertEqual(stats["Totals"]["Count"], len(node.requests))
    env.assertEqual(len(failures), len(node.requests))
    env.assertGreaterEqual(node.slots_calls, 2)
    env.assertLessEqual(node.slots_calls, 2 + int(node.run_elapsed))


def test_partial_map_preserves_sequential_key_identity(env):
    for pattern in ("S:S", "P:P"):
        node = _Node([(0, 8191)])
        stats, failures, _ = _run(
            env, node, ["--requests=1000", "--key-maximum=1000", "--key-pattern=" + pattern]
        )
        env.assertEqual(stats["Totals"]["Count"], 1000)
        env.assertEqual(
            [keys[0] for _, keys in node.requests], [b"memtier-" + str(i).encode() for i in range(1, 1001)]
        )
        env.assertEqual(len(failures), len(node.error_keys))
        env.assertGreater(len(failures), 0)


def test_partial_map_with_owned_keys_is_unchanged(env):
    node = _Node([(0, 8191)])
    stats, failures, _ = _run(env, node, ["--requests=100"])
    env.assertEqual(stats["Totals"]["Count"], 100)
    env.assertEqual(failures, [])
    env.assertEqual(node.slots_calls, 1)


def test_unowned_retry_recovers_after_refresh(env):
    node = _hole(heal=True)
    stats, failures, _ = _run(
        env, node, ["--requests=20", "--retry-on-error", "--max-retries=3", "--retry-backoff-ms=10"]
    )
    env.assertEqual(stats["Totals"]["Count"], 20)
    env.assertEqual(len(node.requests), 21)
    env.assertEqual(len(node.error_keys), 1)
    env.assertEqual(failures, [])


def test_permanent_hole_retries_are_bounded(env):
    node = _hole()
    stats, failures, _ = _run(
        env, node, ["--requests=20", "--retry-on-error", "--max-retries=2", "--retry-backoff-ms=10"]
    )
    env.assertEqual(stats["Totals"]["Count"], 20)
    env.assertEqual(len(node.requests), 60)
    env.assertEqual(len(failures), 20)


def test_unowned_read_preference_contract(env):
    for mode, fallback, code in (
        ("primary", "error", 0),
        ("secondaryPreferred", "error", 0),
        ("nearest", "error", 0),
        ("secondary", "primary", 0),
        ("secondary", "error", 1),
        ("secondary", "queue", 1),
    ):
        node = _hole(replica=True)
        stats, failures, stderr = _run(
            env,
            node,
            [
                "--requests=20",
                "--ratio=0:1",
                "--read-preference=" + mode,
                "--read-preference-fallback=" + fallback,
            ],
            expected_exit=code,
        )
        env.assertEqual(node.replica.requests, [])
        env.assertEqual(stats["Totals"]["Count"], 0 if code else 20)
        if code:
            env.assertContains("cannot route a strict-secondary read", stderr)
        else:
            env.assertEqual(len(failures), 20)


def test_unowned_mget_keys_are_not_omitted(env):
    node = _hole()
    stats, failures, _ = _run(env, node, ["--requests=20", "--ratio=0:2", "--multi-key-get=2"])
    env.assertEqual(stats["Totals"]["Count"], 20)
    env.assertEqual(len(node.requests), 20)
    env.assertTrue(
        all(verb == b"MGET" and keys == (b"memtier-1", b"memtier-1") for verb, keys in node.requests)
    )
    env.assertEqual(len(failures), 20)


def test_unowned_arbitrary_key_is_preserved(env):
    node = _hole()
    stats, failures, _ = _run(env, node, ["--requests=20", "--command=SET __key__ __data__"])
    env.assertEqual(stats["Totals"]["Count"], 20)
    env.assertEqual(len(failures), 20)
    env.assertTrue(all(keys == (b"memtier-1",) for _, keys in node.requests))


def test_unowned_transaction_does_not_queue_topology_refresh(env):
    node = _hole()
    stats, _, _ = _run(
        env,
        node,
        [
            "--requests=24",
            "--transaction",
            "--command=MULTI",
            "--command=SET __key__ __data__",
            "--command=EXEC",
        ],
    )
    env.assertEqual(stats["Totals"]["Count"], 24)
    env.assertEqual(len(node.requests), 8)
    env.assertGreaterEqual(node.slots_calls, 2)


def test_midrun_slot_removal_after_reconnect(env):
    for retry in ([], ["--retry-on-error", "--max-retries=1", "--retry-backoff-ms=10"]):
        node = _Node([(0, 16383)], drop_after=5)
        stats, failures, _ = _run(
            env,
            node,
            [
                "--requests=20",
                "--reconnect-on-error",
                "--max-reconnect-attempts=3",
                "--reconnect-backoff-factor=0",
            ]
            + retry,
        )
        env.assertTrue(node.dropped)
        env.assertEqual(stats["Totals"]["Count"], 20)
        env.assertGreaterEqual(len(node.error_keys), 16)
        env.assertGreaterEqual(len(node.requests), 21)
        env.assertGreaterEqual(len(failures), 16)
        env.assertTrue(all(keys == (b"memtier-1",) for _, keys in node.requests))


def test_unowned_monitor_key_is_sent_to_primary(env):
    for seed_replica in (False, True):
        node = _hole(replica=True, auth_delay=0.1 if seed_replica else 0)
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "monitor.txt")
            with open(path, "w") as output:
                output.write('1.000000 [0 127.0.0.1:12345] "SET" "memtier-1" "dummy"\n')
            stats, failures, _ = _run(
                env,
                node,
                ["--requests=20", "--monitor-input=" + path, "--command=__monitor_line@__"],
                seed_replica=seed_replica,
            )
        env.assertEqual(stats["Totals"]["Count"], 20)
        env.assertEqual(len(node.requests), 20)
        env.assertEqual(node.replica.requests, [])
        env.assertEqual(len(failures), 20)


def test_unowned_write_from_replica_seed_uses_primary(env):
    node = _hole(replica=True)
    stats, failures, _ = _run(env, node, ["--requests=20"], seed_replica=True)
    env.assertEqual(stats["Totals"]["Count"], 20)
    env.assertEqual(len(node.requests), 20)
    env.assertEqual(node.replica.requests, [])
    env.assertEqual(len(failures), 20)


def test_unowned_strict_secondary_mget_and_arbitrary_fail(env):
    for args in (["--ratio=0:2", "--multi-key-get=2"], ["--command=GET __key__", "--command-is-read"]):
        node = _hole(replica=True)
        stats, _, stderr = _run(
            env, node, ["--requests=20", "--read-preference=secondary"] + args, expected_exit=1
        )
        env.assertEqual(stats["Totals"]["Count"], 0)
        env.assertEqual(node.requests, [])
        env.assertEqual(node.replica.requests, [])
        env.assertContains("cannot route a strict-secondary read", stderr)


def test_full_map_discovers_slot_removal_without_disconnect(env):
    for retry in ([], ["--retry-on-error", "--max-retries=1", "--retry-backoff-ms=10"]):
        node = _Node([(0, 16383)], drop_after=5, disconnect_on_drop=False, heal=True)
        stats, failures, _ = _run(env, node, ["--requests=20"] + retry)
        env.assertTrue(node.dropped)
        env.assertEqual(stats["Totals"]["Count"], 20)
        env.assertEqual(len(node.requests), 21 if retry else 20)
        env.assertEqual(len(node.error_keys), 1)
        env.assertEqual(len(failures), 0 if retry else 1)
        env.assertEqual(node.slots_calls, 2)
        env.assertTrue(all(keys == (b"memtier-1",) for _, keys in node.requests))


def test_unowned_slot_warning_is_once_per_process(env):
    node = _hole()
    stats, failures, stderr = _run(env, node, ["--requests=20", "--threads=2", "--clients=3"])
    env.assertEqual(stats["Totals"]["Count"], 120)
    env.assertEqual(len(node.requests), 120)
    env.assertEqual(len(failures), 120)
    env.assertEqual(stderr.count("has no owner; sending requests to a primary"), 1)


class _TwoPrimaries(_Node):
    """Seed B is connected while advertised-first primary A is still setting up.

    The missing slot accepts data after advertising a hole, modelling recovery
    between SLOTS and the data command. Successful replies must not depend on
    CLUSTERDOWN forcing another cache build to keep the producer progressing.
    """

    def __init__(self, unowned=True):
        super().__init__([(0, 16383)])
        self.first_primary = _Node([(0, 16383)], auth_delay=0.1)
        self.first_primary.root = self
        self.first_primary.topology = self.topology
        self.extra = [self.first_primary]
        self.unowned = unowned

    def topology(self):
        with self.lock:
            self.slots_calls += 1
        ranges = [(0, 3497), (3499, 8000)] if self.unowned else [(0, 8000)]
        owners = [(low, high, self.first_primary) for low, high in ranges] + [(8001, 16383, self)]
        reply = b"*" + str(len(owners)).encode() + b"\r\n"
        for low, high, owner in owners:
            reply += (
                b"*3\r\n:"
                + str(low).encode()
                + b"\r\n:"
                + str(high).encode()
                + b"\r\n*2\r\n$9\r\n127.0.0.1\r\n:"
                + str(owner.server_address[1]).encode()
                + b"\r\n"
            )
        return reply


def test_unowned_mget_keeps_its_primary_producer_during_setup(env):
    for limits in (
        ["--requests=20"],
        ["--requests=20", "--pipeline=8"],
        ["--test-time=1", "--rate-limiting=20"],
    ):
        node = _TwoPrimaries()
        stats, failures, _ = _run(env, node, ["--ratio=0:2", "--multi-key-get=2"] + limits)
        count = stats["Totals"]["Count"]
        if limits[0].startswith("--requests"):
            env.assertEqual(count, 20)
        else:
            env.assertGreater(count, 0)
        env.assertEqual(len(node.requests), count)
        env.assertEqual(node.first_primary.requests, [])
        env.assertTrue(
            all(verb == b"MGET" and keys == (b"memtier-1", b"memtier-1") for verb, keys in node.requests)
        )
        env.assertEqual(failures, [])
        env.assertEqual(node.slots_calls, 1)


def test_two_primary_owned_mget_and_unowned_get_controls(env):
    for unowned, args in ((False, ["--ratio=0:2", "--multi-key-get=2"]), (True, ["--ratio=0:1"])):
        node = _TwoPrimaries(unowned=unowned)
        stats, failures, _ = _run(env, node, ["--requests=20"] + args)
        requests = node.requests + node.first_primary.requests
        env.assertEqual(stats["Totals"]["Count"], 20)
        env.assertEqual(len(requests), 20)
        env.assertTrue(
            all(
                keys == ((b"memtier-1",) if unowned else (b"memtier-1", b"memtier-1")) for _, keys in requests
            )
        )
        if not unowned:
            env.assertEqual(node.requests, [])
        env.assertEqual(failures, [])
        env.assertEqual(node.slots_calls, 1)


def test_unowned_transaction_disconnected_primary(env):
    for reconnect in (False, True):
        node = _hole(replica=True, auth_delay=0.02)
        node.auth_closes = 1
        node.replica.readonly_delay = 0.2
        node.slots_delay = 0.1
        args = [
            "--requests=24",
            "--transaction",
            "--command=MULTI",
            "--command=SET __key__ value",
            "--command=EXEC",
        ]
        if reconnect:
            args += ["--reconnect-on-error", "--max-reconnect-attempts=2", "--reconnect-backoff-factor=0.5"]
        stats, rows, stderr = _run(env, node, args, seed_replica=True, expected_exit=0 if reconnect else 1)
        env.assertEqual(len(node.replica.requests), 0)
        env.assertEqual(node.auth_attempts, 2 if reconnect else 1)
        env.assertEqual(stats["Totals"]["Count"], 24 if reconnect else 0)
        env.assertEqual(len(node.requests), 8 if reconnect else 0)
        if reconnect:
            env.assertEqual([keys for verb, keys in node.requests], [(b"memtier-1",)] * 8)
        else:
            env.assertContains("--reconnect-on-error is not set", stderr)


def test_unowned_monitor_disconnected_primary(env):
    for reconnect, unowned, drops, pipeline in (
        (False, True, 1, 1),
        (True, True, 1, 1),
        (True, False, 1, 1),
        (True, True, 2, 1),
        (True, True, 2, 4),
        (True, False, 2, 4),
    ):
        node = (
            _hole(replica=True, auth_delay=0.02)
            if unowned
            else _Node([(0, 16383)], replica=True, auth_delay=0.02)
        )
        node.auth_closes = drops
        node.replica.readonly_delay = 0.2
        node.slots_delay = 0.4 if drops > 1 else 0.1
        with tempfile.TemporaryDirectory() as directory:
            monitor = os.path.join(directory, "monitor.txt")
            with open(monitor, "w") as target:
                for index in range(10):
                    target.write(f'1.000000 [0 127.0.0.1:12345] "SET" "memtier-1" "value-{index}"\n')
            # PING reaches steady state before replay: the startup watchdog
            # must not conceal a counted command stranded on the dead primary.
            args = [
                "--requests=20",
                "--pipeline=" + str(pipeline),
                "--read-preference=secondaryPreferred",
                "--monitor-input=" + monitor,
                "--monitor-pattern=S",
                "--command=PING",
                "--command=__monitor_line@__",
            ]
            if reconnect:
                args += [
                    "--reconnect-on-error",
                    "--max-reconnect-attempts=3",
                    "--reconnect-backoff-factor=0.5",
                ]
            stats, rows, stderr = _run(
                env, node, args, seed_replica=True, expected_exit=0 if reconnect else 1
            )
        env.assertEqual(len(node.replica.requests), 0)
        if reconnect:
            env.assertEqual(stats["Totals"]["Count"], 20)
            env.assertEqual(node.auth_attempts, drops + 1)
            env.assertEqual(len(node.requests), 10)
            env.assertEqual(
                sum(command == (b"PING",) for command in node.workload + node.replica.workload), 10
            )
            env.assertEqual(
                [command for command in node.workload if command[0] == b"SET"],
                [(b"SET", b"memtier-1", f"value-{index}".encode()) for index in range(10)],
            )
            env.assertContains("requested a cluster slot map refresh to reconnect", stderr)
            env.assertEqual([keys for verb, keys in node.requests], [(b"memtier-1",)] * 10)
        else:
            env.assertEqual(stats["Totals"]["Count"], 1)
            env.assertEqual(len(node.requests), 0)
            env.assertContains("no reachable primary", stderr)


def test_unowned_keys_survive_repeated_redial(env):
    # The same queue-reset bug affects ordinary routed key indices: dropping
    # them and decrementing the count silently substitutes later keys.
    for unowned, drops, pipeline in ((True, 1, 1), (True, 2, 4), (False, 2, 4)):
        node = (
            _hole(replica=True, auth_delay=0.02)
            if unowned
            else _Node([(0, 16383)], replica=True, auth_delay=0.02)
        )
        node.auth_closes = drops
        node.replica.readonly_delay = 0.2
        node.slots_delay = 0.4
        stats, _, _ = _run(
            env,
            node,
            [
                "--requests=20",
                "--pipeline=" + str(pipeline),
                "--key-prefix={memtier-1}-",
                "--key-maximum=100",
                "--read-preference=secondaryPreferred",
                "--reconnect-on-error",
                "--max-reconnect-attempts=3",
                "--reconnect-backoff-factor=0.5",
            ],
            seed_replica=True,
        )
        env.assertEqual(stats["Totals"]["Count"], 20)
        env.assertEqual(node.auth_attempts, drops + 1)
        env.assertEqual(node.replica.requests, [])
        env.assertEqual(
            [keys for _, keys in node.requests],
            [(f"{{memtier-1}}-{index}".encode(),) for index in range(1, 21)],
        )
