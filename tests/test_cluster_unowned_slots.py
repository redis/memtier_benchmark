"""A valid partial CLUSTER SLOTS map must not lose keys or spin (#685)."""

import binascii
import csv
import errno
import json
import os
import re
import socket
import socketserver
import subprocess
import tempfile
import threading
import time
from include import MEMTIER_BINARY

def _clusterdown_refresh_interval():
    """How often memtier at most asks for CLUSTER SLOTS after -CLUSTERDOWN, in seconds."""
    header = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "cluster_client.h")
    with open(header) as source:
        match = re.search(r"#define\s+CLUSTERDOWN_REFRESH_INTERVAL_USEC\s+(\d+)", source.read())
    assert match, "CLUSTERDOWN_REFRESH_INTERVAL_USEC is not defined in %s" % header
    return int(match.group(1)) / 1e6


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
        self.drop_request = None
        self.error_keys = []
        self.errors = []
        self.lock = threading.Lock()

    # Reply hooks, overridden by _Failover; set_reply=None takes the default path.
    set_reply = None
    # Called with each SET/GET/MGET before the lock is taken, so it may wait.
    before_request = None

    def ping_reply(self, server):
        return b"+PONG\r\n"

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
                        if reply is None:
                            return
                    elif verb == b"PING":
                        reply = root.ping_reply(node)
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
                    elif verb == b"SET" and root.set_reply is not None:
                        reply = root.set_reply(node, args)
                    else:
                        assert verb in (b"SET", b"GET", b"MGET"), args
                        if node.before_request is not None:
                            node.before_request(args)
                        keys = args[1:] if verb == b"MGET" else args[1:2]
                        with root.lock:
                            node.requests.append((verb, tuple(keys)))
                            if node.drop_request is not None and node.drop_request(args):
                                return
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


def _run(env, node, args, expected_exit=0, timeout=8, seed_replica=False, watch=None):
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
            if watch is None:
                result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
            else:
                # Hand each stderr line to watch while memtier runs.
                with subprocess.Popen(
                    command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True
                ) as process:
                    lines = []
                    deadline = threading.Timer(timeout, process.kill)
                    deadline.start()
                    try:
                        for line in process.stderr:
                            lines.append(line)
                            watch(line)
                        process.wait()
                    finally:
                        deadline.cancel()
                result = subprocess.CompletedProcess(command, process.returncode, "", "".join(lines))
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


def test_unusable_slots_refresh_is_bounded_by_reconnect_attempts(env):
    """A seed that keeps answering CLUSTER SLOTS unusably doesn't strand a --requests run.

    The primary drops its third SET, so its redial is left to the seed's
    refresh, and from then on the seed answers every CLUSTER SLOTS with an
    empty or an all-malformed map. Each such reply reconnects the seed, so
    --max-reconnect-attempts ends the run, as on master.
    """
    for unusable in (b"*0\r\n", b"*1\r\n*1\r\n:0\r\n"):
        node = _Node([(0, 16383)], replica=True)
        node.unusable = 0
        node.drop_request = lambda args, node=node: len(node.requests) == 3
        healthy = node.replica.topology

        def topology(node=node, unusable=unusable, healthy=healthy):
            with node.lock:
                dropped = len(node.requests) >= 3
                node.unusable += dropped
            return unusable if dropped else healthy()

        node.replica.topology = topology
        with tempfile.TemporaryDirectory() as directory:
            monitor = os.path.join(directory, "monitor.txt")
            with open(monitor, "w") as target:
                for index in range(10):
                    target.write(f'1.000000 [0 127.0.0.1:12345] "SET" "memtier-1" "value-{index}"\n')
            stats, _, stderr = _run(
                env,
                node,
                [
                    "--requests=20",
                    "--read-preference=secondaryPreferred",
                    "--monitor-input=" + monitor,
                    "--monitor-pattern=S",
                    "--command=PING",
                    "--command=__monitor_line@__",
                    "--reconnect-on-error",
                    "--max-reconnect-attempts=3",
                    "--reconnect-backoff-factor=0.5",
                    # The seed fails setup on every reconnect. With the
                    # harness's 2 s, a loaded host can let the stage
                    # supervisor end the run before --max-reconnect-attempts
                    # does, and this test checks the cap.
                    "--connection-stage-timeout=30",
                ],
                seed_replica=True,
                expected_exit=1,
                # Without the cap the run outlives this timeout.
                timeout=20,
            )
        env.assertContains("requested a cluster slot map refresh to reconnect", stderr)
        env.assertContains("Maximum reconnection attempts (3) exceeded for CLUSTER SLOTS error", stderr)
        # The refresh, then one per reconnect of the seed.
        env.assertEqual(node.unusable, 4)
        env.assertLess(stats["Totals"]["Count"], 20)


def test_given_up_primary_monitor_drops_are_reported(env):
    """Lines dropped with a given-up primary are errors in the JSON and in --failed-keys-file.

    A primary A with replicas R (the seed) and X. A drops a SET and refuses
    its redial. X is dropped once A's redial is accepted, and it redials
    slowly. X is still in setup when A runs out of attempts, so A is given up
    on while the thread goes on, and the monitor lines staged on A are dropped
    as slot-owner-down-for-monitor-line.

    A line is staged on A only while A is in setup, and only if a peer is
    woken then: a producer selects no line while A is disconnected, and
    nothing wakes it when A moves into setup. So X's first PONG is held, which
    makes X see its drop at once (an idle connection only sees a drop on its
    next send). Dropping X after A's redial is accepted makes X's SLOTS
    refresh land while A is in setup, and it wakes R, whose next line is
    staged on A. A's AUTH is held two seconds before it is refused.

    The run is tried up to 3 times, until memtier reports the drop ("dropped N
    staged monitor line(s)"), as a margin for a host that stalls the client
    past A's two seconds. The PR base never reports it.
    """
    for tries in range(1, 4):
        stats, rows, stderr = _given_up_primary_with_replica_in_setup(env)
        if re.search(r"dropped \d+ staged monitor line", stderr):
            break
    env.debugPrint("runs: %d" % tries, force=True)
    env.assertContains("leaving conn 1 dead and continuing while peers are still mid-setup", stderr)
    dropped = [row for row in rows if row["status"] == "slot-owner-down-for-monitor-line"]
    env.assertGreater(len(dropped), 0, message=str(rows))
    env.assertEqual([(row["command"], row["key"]) for row in dropped], [("ARBITRARY", "memtier-1")] * len(dropped))
    # Every error in ALL STATS has its failed-keys record: the drops and the
    # requests held for resend when the thread ends.
    env.assertEqual(stats["Totals"]["Errors"], len(rows), message=str(rows))


def _given_up_primary_with_replica_in_setup(env):
    node = _Node([(0, 16383)], replica=True)
    x = _Node([])
    x.root = node
    node.extra = [x]
    x.sockets = []
    accept = x.process_request

    def process_request(request, address):
        x.sockets.append(request)
        accept(request, address)

    x.process_request = process_request
    accept_a = node.process_request
    holding, redialing, released = threading.Event(), threading.Event(), threading.Event()

    def ping_reply(server):
        # X's first PONG is held, so X has a request in flight and sees its
        # drop at once (an idle connection only sees it on its next send).
        if server is x and not released.is_set():
            holding.set()
            released.wait(10)
        return b"+PONG\r\n"

    node.ping_reply = ping_reply

    def process_a(request, address):
        # A's redial is accepted, so A is in setup: drop X now. X's redial
        # asks for a SLOTS refresh after its 0.5s backoff, and the refresh
        # reply wakes R, whose next line is staged on A (A's AUTH is held two
        # seconds, then refused). X redials slowly, so it's still in setup
        # when A runs out of attempts.
        if redialing.is_set() and not released.is_set():
            x.auth_delay = 5
            for sock in x.sockets:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            released.set()
        accept_a(request, address)

    node.process_request = process_a
    nodes = [node, node.replica, x]

    def topology():
        with node.lock:
            node.slots_calls += 1
        reply = b"*1\r\n*%d\r\n:0\r\n:16383\r\n" % (2 + len(nodes))
        for server in nodes:
            reply += b"*2\r\n$9\r\n127.0.0.1\r\n:%d\r\n" % server.server_address[1]
        return reply

    for server in nodes:
        server.topology = topology

    def drop(args):
        # A drops a SET once it has served two and X holds its PING.
        if len(node.requests) < 3 or not holding.is_set() or redialing.is_set():
            return False
        node.auth_delay = 2
        node.auth_closes = 100
        redialing.set()
        return True

    def before_request(args):
        # X's PING went out before A's first SETs; on a starved host its
        # handler may not have run yet.
        if len(node.requests) == 2:
            holding.wait(5)

    node.before_request = before_request
    node.drop_request = drop
    with tempfile.TemporaryDirectory() as directory:
        monitor = os.path.join(directory, "monitor.txt")
        with open(monitor, "w") as target:
            for index in range(10):
                target.write(f'1.000000 [0 127.0.0.1:12345] "SET" "memtier-1" "value-{index}"\n')
        stats, rows, stderr = _run(
            env,
            node,
            [
                "--requests=20",
                "--read-preference=secondaryPreferred",
                "--monitor-input=" + monitor,
                "--monitor-pattern=S",
                "--command=PING",
                "--command=__monitor_line@__",
                "--reconnect-on-error",
                "--max-reconnect-attempts=1",
                "--reconnect-backoff-factor=0.5",
                # The JSON reports Errors only with it.
                "--retry-on-error",
                # X's slow redial must not end the run as a stage failure.
                "--connection-stage-timeout=30",
            ],
            seed_replica=True,
            # A is given up on, so the run can't reach --requests.
            expected_exit=1,
            timeout=20,
        )
    return stats, rows, stderr


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


def test_replica_promotion_does_not_drain_secondary_reads(env):
    """A former replica's staged GETs must not bypass routing after promotion."""
    for pipeline in (4, 16, 64):
        node = _Node([(0, 16383)], replica=True)
        other = _Node([(0, 16383)])
        other.root = node
        node.extra = [other]
        target = node.replica
        promoted = False
        primary_reads = []

        def drop_first_read(args):
            nonlocal promoted
            if not promoted:
                promoted = True
                return True
            primary_reads.append(tuple(args))
            return False

        def topology():
            with node.lock:
                node.slots_calls += 1
                nodes = [target, node, other] if promoted else [node, target, other]
            reply = b"*1\r\n*5\r\n:0\r\n:16383\r\n"
            for server in nodes:
                reply += b"*2\r\n$9\r\n127.0.0.1\r\n:" + str(server.server_address[1]).encode() + b"\r\n"
            return reply

        target.drop_request = drop_first_read
        node.topology = target.topology = other.topology = topology
        stats, _, _ = _run(
            env,
            node,
            [
                "--requests=1000",
                "--pipeline=" + str(pipeline),
                "--ratio=0:1",
                "--read-preference=secondary",
                "--read-preference-fallback=error",
                "--reconnect-on-error",
                "--max-reconnect-attempts=3",
                "--reconnect-backoff-factor=0.5",
            ],
            timeout=15,
        )
        env.assertEqual(promoted, True, message="fixture must close and promote the selected replica")
        env.assertEqual(stats["Totals"]["Count"], 1000)
        env.assertEqual(primary_reads, [], message="staged strict-secondary reads reached a promoted primary")


def test_owned_monitor_waits_for_primary_refresh(env):
    """Owned replay must stay on its primary while that primary refreshes SLOTS."""
    for pipeline in (1, 4):
        node = _Node([(0, 16383)], replica=True, drop_after=1, disconnect_on_drop=False)
        node.slots_delay = 0.2
        with tempfile.TemporaryDirectory() as directory:
            monitor = os.path.join(directory, "monitor.txt")
            expected = [(b"SET", f"memtier-{1 + index % 2}".encode(), f"value-{index}".encode())
                        for index in range(20)]
            with open(monitor, "w") as target:
                for _, key, value in expected:
                    target.write(f'1.000000 [0 127.0.0.1:12345] "SET" "{key.decode()}" "{value.decode()}"\n')
            stats, _, _ = _run(
                env, node,
                ["--requests=40", "--pipeline=" + str(pipeline),
                 "--read-preference=secondaryPreferred", "--monitor-input=" + monitor,
                 "--monitor-pattern=S", "--command=PING", "--command=__monitor_line@__"],
                seed_replica=True,
            )
        env.assertEqual(node.replica.requests, [])
        env.assertEqual(stats["Totals"]["Count"], 40)
        env.assertEqual([command for command in node.workload if command[0] == b"SET"], expected)


class _HealingPrimaries(_Node):
    """Primary A advertises only 0-8191. Once allow_heal is set and a request
    has hit an unowned slot, the next CLUSTER SLOTS adds primary B for
    8192-16383 (#825)."""

    def __init__(self):
        super().__init__([(0, 8191)])
        self.second = _Node([(0, 16383)])
        self.second.root = self
        self.second.topology = self.topology
        self.extra = [self.second]
        self.allow_heal = threading.Event()

    def topology(self):
        with self.lock:
            self.slots_calls += 1
            if self.error_keys and self.allow_heal.is_set():
                self.ranges = [(0, 16383)]
            healed = len(self.ranges) == 1 and self.ranges[0] == (0, 16383)
        owners = [(0, 8191, self)] + ([(8192, 16383, self.second)] if healed else [])
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


def test_percentile_bound_follows_healed_topology(env):
    """The percentile warning's bound must cover connections opened when a
    partial map heals (#825). At 100 req/s (burst 2) for 6s a connection
    sends at most 100 x 6 + 2 x 2 = 604 requests, and at most 100 x 2 + 2 =
    202 in a 1s window. The run starts on one primary (604, window 202) and,
    once the first warnings are printed, heals to two (1208, window 404):
    p99.7 (needs 334) loses its window warning, p99.9 (needs 1000) trades
    its run warning for a window one, and p99.95 (needs 2000) stays
    unresolved with the raised bound. Each is updated exactly once, every
    win1s row still marks the percentiles warned about, and p50 never
    warns."""
    node = _HealingPrimaries()

    def watch(line):
        if line.startswith("warning: p99.95 needs"):
            node.allow_heal.set()

    stats, _, stderr = _run(
        env,
        node,
        [
            "--test-time=6",
            "--rate-limiting=100",
            "--key-maximum=1000",
            "--realtime-latencies",
            "--latency-window=1",
            "--print-percentiles=50,99.7,99.9,99.95",
        ],
        timeout=30,
        watch=watch,
    )
    # B served traffic, and the run stays within the last stated bound (it
    # usually exceeds the first one, but how far depends on when it heals).
    env.assertGreater(len(node.second.requests), 0, message=stderr[-3000:])
    env.assertLessEqual(stats["Totals"]["Count"], 1208, message=stderr[-3000:])
    first = [
        "warning: p99.7 needs at least 334 latency samples, but the 1s latency window holds at most about 202; "
        "the win1s row marks it with '*'.",
        "warning: p99.9 needs at least 1000 latency samples, but this run records at most about 604, "
        "so it will not be resolved.",
        "warning: p99.95 needs at least 2000 latency samples, but this run records at most about 604, "
        "so it will not be resolved.",
    ]
    update = [
        "warning: the cluster topology grew to 2 rate-limited connections, so the percentile sample bounds "
        "above are raised:",
        "note: p99.7 needs at least 334 latency samples and the 1s latency window may now hold up to about 404, "
        "so the win1s row may resolve it.",
        "note: p99.9 needs at least 1000 latency samples and this run may now record up to about 1208, "
        "so it may be resolved.",
        "warning: p99.9 needs at least 1000 latency samples, but the 1s latency window holds at most about 404; "
        "the win1s row marks it with '*'.",
        "warning: p99.95 needs at least 2000 latency samples, but this run records at most about 1208, "
        "so it will not be resolved.",
    ]
    lines = [line for line in stderr.splitlines() if line.startswith(("warning: p", "warning: the cluster", "note: p"))]
    env.assertEqual(lines, first + update, message=stderr[-3000:])
    # Rows wrap into continuation lines; a value of "-" means no samples.
    marks = [
        (pct, mark)
        for line in stderr.splitlines()
        if "latency [win1s" in line
        for pct, _, mark in re.findall(r"p([\d.]+)\s+(\d+\.\d+)(\*?)", line)
    ]
    env.assertGreater(len(marks), 0, message=stderr[-3000:])
    for pct, mark in marks:
        if pct in ("99.9", "99.95"):
            env.assertEqual(mark, "*", message="win1s p%s unmarked" % pct)


def test_percentile_requests_bound_is_not_repeated_on_growth(env):
    """--requests=500 is an exact run bound that a healed map does not raise,
    so the p99.9 warning (needs 1000) is not repeated; the 1s window bound of
    p99.8 (needs 500) rises from 202 to 404 and is updated (#825)."""
    node = _HealingPrimaries()

    def watch(line):
        if line.startswith("warning: p99.9 needs"):
            node.allow_heal.set()

    stats, _, stderr = _run(
        env,
        node,
        [
            "--requests=500",
            "--rate-limiting=100",
            "--key-maximum=1000",
            "--realtime-latencies",
            "--latency-window=1",
            "--print-percentiles=50,99.8,99.9",
        ],
        timeout=30,
        watch=watch,
    )
    env.assertEqual(stats["Totals"]["Count"], 500)
    env.assertGreater(len(node.second.requests), 0, message=stderr[-3000:])
    lines = [line for line in stderr.splitlines() if line.startswith(("warning: p", "warning: the cluster", "note: p"))]
    env.assertEqual(
        lines,
        [
            "warning: p99.8 needs at least 500 latency samples, but the 1s latency window holds at most about 202; "
            "the win1s row marks it with '*'.",
            "warning: p99.9 needs at least 1000 latency samples, but this run records at most 500, "
            "so it will not be resolved.",
            "warning: the cluster topology grew to 2 rate-limited connections, so the percentile sample bounds "
            "above are raised:",
            "warning: p99.8 needs at least 500 latency samples, but the 1s latency window holds at most about 404; "
            "the win1s row marks it with '*'.",
        ],
        message=stderr[-3000:],
    )


class _Failover(_Node):
    """Primary A (self) with replica R, the seed, plus spare primary B.

    A answers its first SET with CLUSTERDOWN, so it refreshes CLUSTER SLOTS.
    The server map becomes `change` when that refresh reaches A, and A's
    SLOTS replies are delayed from then on. Meanwhile R stages monitor lines
    on A (#799), and they are still queued when the new map is committed
    (#835, #836). With silent=True the map changes on A's first SET instead,
    which succeeds, and memtier learns of it from a -MOVED. With redial=True
    A closes its first two connections during AUTH instead, and the refresh
    after the second one returns the new map, so A is still redialing when
    it changes role. With pending=True A closes instead of answering that
    refresh once R's PING is held, and that PING gets CLUSTERDOWN, so the map
    is committed while A's reconnect timer is still pending (see ping_reply).
    A SET sent to a node that does not own its slot
    gets -MOVED, or with moved=False is accepted, as by a demoted primary.
    """

    def __init__(self, change, moved=True, silent=False, redial=False, initial=None, pending=False):
        super().__init__([(0, 16383)])
        self.replica = _Node([])
        self.b = _Node([])
        self.extra = [self.b]
        self.moved = moved
        self.silent = silent
        self.redial = redial
        self.refreshed = False
        self.triggered = redial
        self.pending = pending
        self.held = False
        self.r_pings = 0
        self.refreshed_at = None
        self.sent = set()
        # Shares the lock, so every change below can wake a waiting handler.
        self.cond = threading.Condition(self.lock)
        self.closed = threading.Event()
        if redial:
            self.auth_closes = 2
            self.auth_delay = 0.02
        self.log = []
        self.change = initial or [(0, 16383, ["A", "R"])]
        self.apply_change()
        self.changed = False
        self.change = change
        for name, server in (("A", self), ("R", self.replica), ("B", self.b)):
            server.root = self
            server.name = name
            server.topology = lambda server=server: self.slots_reply(server)

    def slots_reply(self, server):
        with self.lock:
            if self.redial and self.auth_attempts >= 2 and not self.changed:
                self.apply_change()
                self.refreshed = True
            if server is self and self.triggered and not self.changed:
                self.apply_change()
            if server is self and self.changed:
                if not self.refreshed:
                    self.refreshed_at = time.monotonic()
                self.refreshed = True
                self.cond.notify_all()
        if self.pending and server is self and self.refreshed and not self.closed.is_set():
            # Close only once R's PING is held, so the commit comes from it.
            with self.cond:
                self.cond.wait_for(lambda: self.held, timeout=10)
            self.closed.set()
            return None
        if (server is self and self.changed) or (self.redial and self.auth_attempts and self.slots_calls):
            time.sleep(0.3)
        with self.lock:
            self.slots_calls += 1
            shards = list(self.map)
        reply = b"*" + str(len(shards)).encode() + b"\r\n"
        for low, high, servers in shards:
            reply += b"*%d\r\n:%d\r\n:%d\r\n" % (2 + len(servers), low, high)
            for s in servers:
                reply += b"*2\r\n$9\r\n127.0.0.1\r\n:%d\r\n" % s.server_address[1]
        return reply

    def apply_change(self):
        self.changed = True
        servers = {"A": self, "R": self.replica, "B": self.b}
        self.map = [(low, high, [servers[n] for n in names]) for low, high, names in self.change]

    def set_reply(self, server, args):
        slot = _slot(args[1])
        with self.lock:
            owner = next((servers[0] for low, high, servers in self.map if low <= slot <= high), None)
            # An unowned slot belongs on any primary, for its routing error.
            owned = owner is server or (owner is None and any(server is servers[0] for _, _, servers in self.map))
            # Whether A was asked for the new map before this SET arrived:
            # one sent earlier can only be stale.
            self.log.append((server.name, args[1], int(args[2].split(b"-")[1]), owned, self.refreshed))
            self.sent.add(self.log[-1][2])
            self.cond.notify_all()
            if server is self and not self.triggered:
                self.triggered = True
                if self.silent:
                    self.apply_change()
                else:
                    return b"-CLUSTERDOWN Hash slot not served\r\n"
        if owner is None:
            return b"-CLUSTERDOWN Hash slot not served\r\n"
        if owner is server or not self.moved:
            return b"+OK\r\n"
        return b"-MOVED %d 127.0.0.1:%d\r\n" % (slot, owner.server_address[1])

    def ping_reply(self, server):
        if not (self.pending and server is self.replica):
            return b"+PONG\r\n"
        # The client alternates PING and monitor line, so lines 0..n-2 were
        # selected before R's n-th PING. While A refreshes, A sends nothing,
        # so a selected line that no node has received is staged on A.
        with self.cond:
            self.r_pings += 1
            n = self.r_pings
            if self.held:
                return b"+PONG\r\n"
            # Before the refresh, answer only once A has those lines, so R
            # cannot fill the in-flight budget with lines A has not answered
            # and stall with nothing on R left to hold.
            self.cond.wait_for(lambda: self.refreshed or self.sent.issuperset(range(n - 1)), timeout=10)
            # Hold the first PING behind which an unowned line (memtier-1,
            # even index) is staged on A; answer the others so R stages more.
            if not (self.refreshed and any(i not in self.sent for i in range(0, n - 1, 2))):
                return b"+PONG\r\n"
            self.held = True
            self.cond.notify_all()
        self.closed.wait(10)
        # memtier acts on CLUSTERDOWN at most once per
        # CLUSTERDOWN_REFRESH_INTERVAL_USEC (cluster_client::handle_response()).
        # Its last poll was A's, before A sent the SLOTS request seen at
        # refreshed_at.
        time.sleep(max(0.0, self.refreshed_at + _clusterdown_refresh_interval() + 0.05 - time.monotonic()))
        return b"-CLUSTERDOWN The cluster is down\r\n"


# New server map per scenario, as (low, high, [primary, replicas...]).
_FAILOVERS = {
    # #835: memtier-2 (slot 15817) moves from A to B.
    "move": [(0, 8191, ["A", "R"]), (8192, 16383, ["B"])],
    # #836: A leaves the map; B takes over its slots and R.
    "retire": [(0, 16383, ["B", "R"])],
    # #836: R is promoted and A demoted to its replica.
    "demote": [(0, 16383, ["R", "A"])],
    # The same, with memtier-1 (slot 3498) left without an owner.
    "demote-hole": [(0, 3497, ["R", "A"]), (3499, 16383, ["R", "A"])],
}

# Server map before the change, where it is not A owning every slot with R.
_INITIAL = {
    # B is connected, and idle, before it takes memtier-2 over.
    "move-live": [(0, 16000, ["A", "R"]), (16001, 16383, ["B"])],
}


def _check_failover_replay(env, scenario, pipeline, moved=True, silent=False, redial=False, pending=False):
    node = _Failover(
        _FAILOVERS[scenario.replace("-live", "")],
        moved=moved,
        silent=silent,
        redial=redial,
        initial=_INITIAL.get(scenario),
        pending=pending,
    )
    with tempfile.TemporaryDirectory() as directory:
        monitor = os.path.join(directory, "monitor.txt")
        with open(monitor, "w") as target:
            for index in range(20):
                target.write(f'1.000000 [0 127.0.0.1:12345] "SET" "memtier-{1 + index % 2}" "value-{index}"\n')
        stats, _, stderr = _run(
            env,
            node,
            [
                "--requests=40",
                "--pipeline=" + str(pipeline),
                "--read-preference=secondaryPreferred",
                "--monitor-input=" + monitor,
                "--monitor-pattern=S",
                "--command=PING",
                "--command=__monitor_line@__",
            ]
            # With pending=True, A's 4 s reconnect timer outlasts the refresh R triggers.
            + (
                ["--reconnect-on-error", "--max-reconnect-attempts=3", "--reconnect-backoff-factor=" + ("4" if pending else "0.5")]
                if redial or pending
                else []
            ),
            seed_replica=True,
            timeout=15,
        )
    label = "%s pipeline=%d moved=%s silent=%s redial=%s pending=%s" % (scenario, pipeline, moved, silent, redial, pending)
    values = [entry[2] for entry in node.log]
    counts = {value: values.count(value) for value in values}
    result = {
        "missing": sorted(set(range(20)) - set(values)),
        "duplicated": sorted(value for value, count in counts.items() if count > 1),
        # Sent to a node that did not own the slot after A was asked for the
        # new map; with silent=True, the SETs A sent before learning of the
        # change are in-flight casualties and are listed apart.
        "misrouted": [(name, value) for name, _, value, owned, refreshed in node.log if not owned and refreshed],
    }
    in_flight = [(name, value) for name, _, value, owned, refreshed in node.log if not owned and not refreshed]
    env.debugPrint("%s: %s in-flight=%s sequence=%s" % (label, result, in_flight, values), force=True)
    env.assertTrue(node.changed, message=label)
    if pending:
        env.assertTrue(node.held, message=label)
    env.assertEqual(stats["Totals"]["Count"], 40, message=label)
    env.assertEqual(result, {"missing": [], "duplicated": [], "misrouted": []}, message=label + stderr[-2000:])
    if silent:
        env.assertLessEqual(len(in_flight), pipeline, message=label)
        env.assertTrue(all(name == "A" for name, _ in in_flight), message=label)
    else:
        env.assertEqual(in_flight, [], message=label)
    if scenario.startswith("move"):
        # Two primaries drain concurrently, so only each key's order is fixed.
        for key in (b"memtier-1", b"memtier-2"):
            sent = [entry[2] for entry in node.log if entry[1] == key]
            env.assertEqual(sent, sorted(sent), message=label)
        env.assertEqual(sorted(values), list(range(20)), message=label)
    else:
        # One primary at a time: the file order, each line once.
        env.assertEqual(values, list(range(20)), message=label)


def test_staged_monitor_follows_moved_slot(env):
    """A slot moved by the committed map takes its staged lines along (#835)."""
    for silent in (False, True):
        for pipeline in (1, 4):
            _check_failover_replay(env, "move", pipeline, silent=silent)
    for pipeline in (1, 4):
        _check_failover_replay(env, "move-live", pipeline)


def test_staged_monitor_survives_retired_primary(env):
    """A retired primary hands its staged lines to the slot's new owner (#836)."""
    for pipeline in (1, 4):
        _check_failover_replay(env, "retire", pipeline)


def test_staged_monitor_leaves_demoted_primary(env):
    """A demoted primary's staged lines go to the promoted one (#836)."""
    for moved, silent, redial in ((True, False, False), (False, False, False), (True, True, False), (True, False, True)):
        for pipeline in (1, 4):
            _check_failover_replay(env, "demote", pipeline, moved=moved, silent=silent, redial=redial)
    for pipeline in (1, 4):
        _check_failover_replay(env, "demote-hole", pipeline)
    # Unowned lines leave A while its reconnect timer is pending too. At
    # pipeline=1 no unowned line is staged on A before it closes.
    _check_failover_replay(env, "demote-hole", 4, pending=True)


def test_cluster_startup_dial_failing_at_once_aborts(env):
    """A --cluster-mode seed whose dial fails at once aborts the run at startup.

    connect() to the broadcast address without SO_BROADCAST fails before the
    event loop (ENETUNREACH or EACCES on Linux), and libevent returns -1 from
    bufferevent_socket_connect(). cluster_client::connect() now passes that
    on, so prepare aborts. Master returned 0, launched the thread and ended
    it with no request sent.
    """
    if env.isCluster() or env.useTLS:
        env.skip()  # The seed is a fixed address, not the env's servers.
    seed = "255.255.255.255"
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.setblocking(False)
    try:
        code = probe.connect_ex((seed, 29971))
    finally:
        probe.close()
    # libevent treats EINPROGRESS (and, in 2.1, ECONNREFUSED) as a dial in
    # progress, reported through the event loop.
    if code in (0, errno.EINPROGRESS, errno.ECONNREFUSED, errno.EWOULDBLOCK):
        env.debugPrint(
            "skip: connect() to %s does not fail at once here (%s)" % (seed, errno.errorcode.get(code, code)),
            force=True,
        )
        env.skip()
    started = time.monotonic()
    try:
        result = subprocess.run(
            [
                MEMTIER_BINARY,
                "--cluster-mode",
                "-s",
                seed,
                "-p",
                "29971",
                "-t",
                "1",
                "-c",
                "1",
                "--requests=10",
                "--hide-histogram",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
        )
    except subprocess.TimeoutExpired:
        env.assertTrue(False, message="memtier did not exit within 10s")
        return
    elapsed = time.monotonic() - started
    env.assertEqual(result.returncode, 1, message=result.stderr)
    env.assertContains("prepare: failed to connect, test aborted.", result.stderr)
    env.assertLess(elapsed, 10)
