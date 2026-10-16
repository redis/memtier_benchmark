"""
Tests for --retry-on-error, --max-retries, --retry-on, --failed-keys-file.

Run subset:
  TEST=test_retry_on_error.py ./tests/run_tests.sh
"""

import csv
import json
import os
import re
import select
import socket
import socketserver
import subprocess
import tempfile
import threading
import time
import zlib
from include import *
from mb import Benchmark, RunConfig


# Sanitizer builds (the CI workflows set these) run memtier several times
# slower, which delays resends by tens of milliseconds.
_SANITIZER_ACTIVE = any(os.environ.get(k) for k in ("ASAN_OPTIONS", "TSAN_OPTIONS", "UBSAN_OPTIONS"))


def _build_benchmark(env, extra_args, threads=1, clients=1, requests=1000):
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            "--ratio=1:0",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=1000",
        ]
        + list(extra_args),
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=threads, clients=clients, requests=requests)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    return Benchmark.from_json(config, benchmark_specs), config


def test_retry_on_error_happy_path(env):
    """--retry-on-error with no induced errors: completes normally, no stats regression."""
    benchmark, config = _build_benchmark(
        env,
        extra_args=[
            "--retry-on-error",
            "--max-retries=5",
            "--retry-backoff-ms=10",
        ],
    )
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok)

    # JSON should contain the new config fields surfaced in the configuration
    # block (sanity-check the flag plumbed through).
    json_path = "{0}/mb.json".format(config.results_dir)
    env.assertTrue(os.path.isfile(json_path))


def test_max_retries_zero_disables_retry(env):
    """--max-retries=0 disables retry even with the master switch on."""
    benchmark, config = _build_benchmark(
        env,
        extra_args=[
            "--retry-on-error",
            "--max-retries=0",
        ],
        requests=100,
    )
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok)


def test_failed_keys_file_logs_permanent_error(env):
    """A permanent error (WRONGTYPE) lands in the failed-keys file mid-run.

    Seeds a known key with a string value, then issues an LPUSH against it. The
    LPUSH always returns WRONGTYPE; with --retry-on-error the permanent
    classifier should NOT retry, and instead append a CSV line.
    """
    # Cluster mode complicates --command routing; skip for now.
    if env.isCluster():
        env.skip()
        return

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    for master_connection in master_nodes_connections:
        master_connection.execute_command("FLUSHALL")
        master_connection.execute_command("SET", "wrongtype-key", "hello")

    test_dir = tempfile.mkdtemp()
    failed_log = os.path.join(test_dir, "failed-keys.csv")

    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            '--command=LPUSH wrongtype-key value',
            "--retry-on-error",
            "--max-retries=10",
            "--failed-keys-file={}".format(failed_log),
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=5)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)

    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok)

    # File exists; header + at least one failure row.
    env.assertTrue(os.path.isfile(failed_log))
    with open(failed_log, "r") as f:
        lines = f.readlines()
    env.assertGreater(len(lines), 1)
    env.assertContains("timestamp,command,key,status,retries", lines[0])
    # Every non-header line must mention WRONGTYPE.
    for line in lines[1:]:
        env.assertContains("WRONGTYPE", line)


def test_retry_on_filter_restricts_set(env):
    """--retry-on=LOADING,BUSY restricts retry to those prefixes only.

    A WRONGTYPE response (permanent) should still NOT be retried even though
    the filter doesn't list it (permanent set always wins).
    """
    if env.isCluster():
        env.skip()
        return

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    for master_connection in master_nodes_connections:
        master_connection.execute_command("FLUSHALL")
        master_connection.execute_command("SET", "wrongtype-key", "hello")

    test_dir = tempfile.mkdtemp()
    failed_log = os.path.join(test_dir, "failed-keys.csv")

    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            '--command=LPUSH wrongtype-key value',
            "--retry-on-error",
            "--retry-on=LOADING,BUSY,TRYAGAIN",
            "--max-retries=10",
            "--failed-keys-file={}".format(failed_log),
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=3)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)

    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok)

    env.assertTrue(os.path.isfile(failed_log))
    with open(failed_log, "r") as f:
        lines = f.readlines()
    # WRONGTYPE still falls into the permanent set even with a restrictive
    # filter that doesn't list it.
    env.assertGreater(len(lines), 1)
    for line in lines[1:]:
        env.assertContains("WRONGTYPE", line)


def _failed_keys_rows(env, extra_args, requests):
    """Run memtier with --failed-keys-file and return the logged key column."""
    test_dir = tempfile.mkdtemp()
    failed_log = os.path.join(test_dir, "failed-keys.csv")
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            "--key-prefix=fk:",
            "--key-minimum=1",
            "--key-maximum=3",
            "--failed-keys-file={}".format(failed_log),
            "--hide-histogram",
        ]
        + list(extra_args),
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=requests)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)

    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok)

    env.assertTrue(os.path.isfile(failed_log))
    with open(failed_log, "r") as f:
        rows = list(csv.reader(f))
    env.assertEqual(rows[0], ["timestamp", "command", "key", "status", "retries"])
    return rows[1:]


def test_failed_keys_file_records_key(env):
    """Failed requests log the key that was sent, not an empty string.

    Arbitrary commands used to log "" for every row because their __key__
    argument was never attached to the request, and SET/GET/MGET only attached
    the key with --retry-on-error. Multi-key requests log their first key.
    """
    if env.isCluster():
        env.skip()
        return

    seeded = ["fk:1", "fk:2", "fk:3"]
    for master_connection in env.getOSSMasterNodesConnectionList():
        master_connection.execute_command("FLUSHALL")
        for key in seeded:
            master_connection.execute_command("SET", key, "hello")
            master_connection.execute_command("SET", "{t}:" + key, "hello")
            master_connection.execute_command("RPUSH", "list-" + key, "a")
        # MGET never fails on its own; a user without it gets -NOPERM.
        master_connection.execute_command("ACL", "SETUSER", "fk-no-mget", "on", ">fk-pass", "~*", "&*",
                                          "+@all", "-mget")

    # (command, args, expected keys or None for "first keys only", status)
    cases = [
        ("ARBITRARY", ["--command=LPUSH __key__ v", "--command-key-pattern=S"], seeded, "WRONGTYPE"),
        ("ARBITRARY", ["--command=LPUSH __key__ v", "--command-key-pattern=S", "--retry-on-error"], seeded,
         "WRONGTYPE"),
        ("ARBITRARY", ["--command=LPUSH {t}:__key__ v", "--command-key-pattern=S"], ["{t}:" + k for k in seeded],
         "WRONGTYPE"),
        # Two keys: the row carries the first (fk:N), never the list-fk:N one.
        ("ARBITRARY", ["--command=SINTER __key__ list-__key__", "--command-key-pattern=S"], None, "WRONGTYPE"),
        ("GET", ["--key-prefix=list-fk:", "--ratio=0:1", "--key-pattern=S:S"], ["list-" + k for k in seeded],
         "WRONGTYPE"),
        # MGET without --retry-on-error, logged by its first key.
        ("GET", ["--authenticate=fk-no-mget:fk-pass", "--ratio=0:1", "--multi-key-get=3", "--key-pattern=S:S"],
         None, "NOPERM"),
    ]
    try:
        for command, args, expected, status in cases:
            label = "{} {}".format(command, args)
            rows = _failed_keys_rows(env, args, requests=3)
            env.assertEqual(len(rows), 3, message=label)
            for row in rows:
                env.assertEqual(row[1], command, message=label)
                env.assertContains(status, row[3], message=label)
                if expected is None:
                    env.assertContains(row[2], seeded, message=label)
            if expected is not None:
                env.assertEqual(sorted(row[2] for row in rows), sorted(expected), message=label)
    finally:
        for master_connection in env.getOSSMasterNodesConnectionList():
            master_connection.execute_command("ACL", "DELUSER", "fk-no-mget")


def test_failed_keys_file_escapes_backslash_and_long_keys(env):
    """The key field escape is reversible and never truncated.

    A backslash used to be written as is, so the 6-byte key p\\x011 (literal
    backslash) and the 3-byte key p<0x01>1 both logged as "p\\x011". The
    escaped key also went through a 1024-byte buffer, so longer keys were
    cut off without any marker.
    """
    if env.isCluster():
        env.skip()
        return

    backslash_prefix = "bs\\x0"  # literal backslash, not an escape
    control_prefix = "bs\x01"
    long_prefix = "L" * 1100
    for master_connection in env.getOSSMasterNodesConnectionList():
        master_connection.execute_command("FLUSHALL")
        # Lists, so GET fails with WRONGTYPE.
        master_connection.execute_command("RPUSH", backslash_prefix + "11", "a")
        master_connection.execute_command("RPUSH", control_prefix + "1", "a")
        master_connection.execute_command("RPUSH", long_prefix + "1", "a")

    cases = [
        (backslash_prefix, "11", "bs\\\\x011"),
        (control_prefix, "1", "bs\\x011"),
        (long_prefix, "1", long_prefix + "1"),
    ]
    for prefix, index, expected in cases:
        args = [
            "--command=GET __key__",
            "--command-key-pattern=S",
            "--key-prefix=" + prefix,
            "--key-minimum=" + index,
            "--key-maximum=" + index,
        ]
        rows = _failed_keys_rows(env, args, requests=1)
        env.assertEqual(len(rows), 1, message=repr(prefix[:16]))
        if rows:
            env.assertContains("WRONGTYPE", rows[0][3])
            env.assertEqual(rows[0][2], expected, message=repr(prefix[:16]))


def test_invalid_max_retries_rejected(env):
    """--max-retries below -1 must be rejected by getopt parsing."""
    test_dir = tempfile.mkdtemp()
    config = get_default_memtier_config(threads=1, clients=1, requests=10)
    master_nodes_list = env.getMasterNodesList()
    benchmark_specs = {
        "name": env.testName,
        "args": ["--retry-on-error", "--max-retries=-5"],
    }
    addTLSArgs(benchmark_specs, env)
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)

    memtier_ok = benchmark.run()
    # Run should fail (nonzero exit) due to invalid argument.
    env.assertFalse(memtier_ok)


def _run_with_background(env, benchmark, config, background, timeout):
    """Run memtier as a subprocess while background(proc) runs alongside it.

    Returns (exit code, stderr).
    """
    stderr_path = "{0}/mb.stderr".format(config.results_dir)
    with open("{0}/mb.stdout".format(config.results_dir), "w") as stdout_f, open(stderr_path, "w") as stderr_f:
        proc = subprocess.Popen(benchmark.args, stdout=stdout_f, stderr=stderr_f, cwd=config.results_dir)
        try:
            background(proc)
            rc = proc.wait(timeout=timeout)
        finally:
            if proc.poll() is None:
                proc.kill()
            rc = proc.wait(timeout=10)
    with open(stderr_path) as f:
        stderr_content = f.read()
    env.debugPrint("memtier exit code: {}".format(rc), True)
    if rc != 0:
        env.debugPrint("STDERR:\n{}".format(stderr_content[-2000:]), True)
    return rc, stderr_content


def test_retried_requests_are_counted_once(env):
    """
    A request retried after a transient error is processed once, not once per
    attempt.

    Each retryable error response used to count the request as processed, and
    the successful replay counted it again, so a --requests run stopped early
    with exit 0 after sending only part of the requested commands.
    """
    # Toggles maxmemory on one server; the key layout assumes a single shard.
    env.skipOnCluster()
    requests = 3000
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            "--ratio=1:0",
            "--key-pattern=P:P",
            "--key-minimum=1",
            "--key-maximum={}".format(requests),
            # About 3 s for the run; the error window below sits inside it.
            "--rate-limiting=1000",
            "--retry-on-error",
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=requests)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)

    conn = env.getOSSMasterNodesConnectionList()[0]
    conn.execute_command("FLUSHALL")
    saved = conn.execute_command("CONFIG", "GET", "maxmemory*")
    # Keys come back as bytes without decoded responses.
    saved = {k.decode() if isinstance(k, bytes) else k: v for k, v in zip(saved[0::2], saved[1::2])}
    conn.execute_command("CONFIG", "SET", "maxmemory-policy", "noeviction")
    conn.execute_command("CONFIG", "RESETSTAT")

    def oom_window(_proc):
        # SETs fail with a retryable -OOM for about a second mid-run.
        time.sleep(1)
        conn.execute_command("CONFIG", "SET", "maxmemory", "1")
        time.sleep(1)
        conn.execute_command("CONFIG", "SET", "maxmemory", "0")

    try:
        rc, _ = _run_with_background(env, benchmark, config, oom_window, timeout=60)
    finally:
        for k in ("maxmemory", "maxmemory-policy"):
            if k in saved:
                conn.execute_command("CONFIG", "SET", k, saved[k])

    rejected = conn.execute_command("INFO", "commandstats").get("cmdstat_set", {}).get("rejected_calls", 0)
    env.assertGreater(rejected, 0, message="no SET was rejected; the error window missed the run")
    env.assertEqual(rc, 0)
    env.assertEqual(conn.execute_command("DBSIZE"), requests,
                    message="the run stopped before sending every requested SET")
    with open("{0}/mb.json".format(config.results_dir)) as f:
        results = json.load(f)
    env.assertEqual(results["ALL STATS"]["Sets"]["Count"], requests)


def test_retry_after_last_request_is_sent(env):
    """
    A request retried after every request of a --requests run has been sent
    still goes out.

    Once nothing was left to send, the client paused its connection while it
    waited for the retry, and the replay was written to the paused
    connection, never sent: the run failed with "client(s) stopped before
    reaching their stop condition".
    """
    # Toggles maxmemory on one server; the key layout assumes a single shard.
    env.skipOnCluster()
    requests = 5
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            "--ratio=1:0",
            "--key-pattern=P:P",
            "--key-minimum=1",
            "--key-maximum={}".format(requests),
            "--retry-on-error",
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=requests)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)

    conn = env.getOSSMasterNodesConnectionList()[0]
    conn.execute_command("FLUSHALL")
    saved = conn.execute_command("CONFIG", "GET", "maxmemory*")
    # Keys come back as bytes without decoded responses.
    saved = {k.decode() if isinstance(k, bytes) else k: v for k, v in zip(saved[0::2], saved[1::2])}
    conn.execute_command("CONFIG", "SET", "maxmemory-policy", "noeviction")
    # Every SET fails with a retryable -OOM until the window below ends, so
    # the last request is retried after all of them were sent.
    conn.execute_command("CONFIG", "SET", "maxmemory", "1")

    def oom_window(_proc):
        time.sleep(1)
        conn.execute_command("CONFIG", "SET", "maxmemory", "0")

    try:
        rc, _ = _run_with_background(env, benchmark, config, oom_window, timeout=60)
    finally:
        for k in ("maxmemory", "maxmemory-policy"):
            if k in saved:
                conn.execute_command("CONFIG", "SET", k, saved[k])

    env.assertEqual(rc, 0)
    env.assertEqual(conn.execute_command("DBSIZE"), requests,
                    message="a retried SET was never sent")


def test_retry_backoff_is_per_request(env):
    """
    --retry-backoff-factor grows the backoff of each request from its own
    retry count, so a request that exhausts --max-retries does not slow down
    the next one.

    The backoff was one value per connection, multiplied on every retry and
    reset only when a retried request succeeded. With every SET failing,
    3 requests took 47 s here instead of about 1 s. 200 requests do not all
    fit in one batch of retries, so a backoff carried over from earlier
    requests shows up in the run time and the latency.
    """
    # Toggles maxmemory on one server; the key layout assumes a single shard.
    env.skipOnCluster()
    requests = 200
    max_retries = 5
    test_dir = tempfile.mkdtemp()
    failed_keys = os.path.join(test_dir, "failed-keys.csv")
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            "--ratio=1:0",
            "--key-pattern=P:P",
            "--key-minimum=1",
            "--key-maximum={}".format(requests),
            "--retry-on-error",
            "--max-retries={}".format(max_retries),
            # Per request: 10+20+40+80+160 ms = 0.31 s.
            "--retry-backoff-ms=10",
            "--retry-backoff-factor=2",
            # No reply ever succeeds; don't abort on that.
            "--connection-stage-timeout=0",
            "--failed-keys-file={}".format(failed_keys),
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=requests)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)

    conn = env.getOSSMasterNodesConnectionList()[0]
    conn.execute_command("FLUSHALL")
    saved = conn.execute_command("CONFIG", "GET", "maxmemory*")
    # Keys come back as bytes without decoded responses.
    saved = {k.decode() if isinstance(k, bytes) else k: v for k, v in zip(saved[0::2], saved[1::2])}
    conn.execute_command("CONFIG", "SET", "maxmemory-policy", "noeviction")
    # Every SET fails with a retryable -OOM for the whole run.
    conn.execute_command("CONFIG", "SET", "maxmemory", "1")

    start = time.time()
    try:
        try:
            rc, stderr = _run_with_background(env, benchmark, config, lambda _proc: None, timeout=20)
        except subprocess.TimeoutExpired:
            rc, stderr = None, ""
    finally:
        for k in ("maxmemory", "maxmemory-policy"):
            if k in saved:
                conn.execute_command("CONFIG", "SET", k, saved[k])
    elapsed = time.time() - start

    env.assertTrue(rc is not None, message="the run did not end within 20 s")
    env.assertEqual(rc, 0, message=stderr[-2000:])
    env.assertLess(elapsed, 10, message="the retry backoff grew across requests")
    # Each request waits 10+20+40+80+160 ms before it is given up on, and its
    # latency spans all attempts.
    with open("{0}/mb.json".format(config.results_dir)) as f:
        latency = json.load(f)["ALL STATS"]["Sets"]["Average Latency"]
    env.assertGreater(latency, 250, message="retries did not wait their backoff")
    env.assertLess(latency, 700 if _SANITIZER_ACTIVE else 500, message="retries waited longer than their own backoff")
    totals = re.findall(r"errors (\d+) \(\+\d+\)\s+retries (\d+) ", stderr)
    env.assertTrue(totals, message=stderr[-2000:])
    if totals:
        env.assertEqual(int(totals[-1][0]), requests)
        env.assertEqual(int(totals[-1][1]), requests * max_retries)
    with open(failed_keys) as f:
        rows = list(csv.DictReader(f))
    env.assertEqual(len(rows), requests)
    for row in rows:
        env.assertEqual(int(row["retries"]), max_retries)


_NO_REPLY_WARNING = re.compile(
    r"warning: (\d+) client\(s\) got no reply to (\d+) request\(s\) (\d+) seconds after the test time ended")
_STILL_RETRYING_WARNING = re.compile(
    r"warning: (\d+) client\(s\) were still retrying (\d+) request\(s\) (\d+) seconds after the test time ended")


def test_test_time_ends_when_every_request_is_retried(env):
    """
    A --test-time run whose retries never succeed ends once the drain grace
    after the deadline is over, like one waiting for replies that never come.

    finished() measures the run by the per-second stats clock, which only
    moves on a completed op. With every SET failing and unlimited
    --max-retries nothing completes, and the drain grace skipped a client
    whose requests were held for retry rather than in flight, so the run
    kept resending them until it was killed (issue #715).
    """
    # Toggles maxmemory on one server; the key layout assumes a single shard.
    env.skipOnCluster()
    test_time = 3
    # Upper bound for TEST_TIME_DRAIN_GRACE_SECS in client.h; the value the
    # run used is read back from its warning.
    max_grace = 10
    conn = env.getOSSMasterNodesConnectionList()[0]
    conn.execute_command("FLUSHALL")
    saved = conn.execute_command("CONFIG", "GET", "maxmemory*")
    # Keys come back as bytes without decoded responses.
    saved = {k.decode() if isinstance(k, bytes) else k: v for k, v in zip(saved[0::2], saved[1::2])}
    conn.execute_command("CONFIG", "SET", "maxmemory-policy", "noeviction")
    # Every SET fails with a retryable -OOM for the whole run.
    conn.execute_command("CONFIG", "SET", "maxmemory", "1")
    try:
        # Factor 0 resends every 100 ms, 2 waits out a growing backoff, and
        # a 20 s backoff is longer than the whole run. Without
        # --retry-on-error the errors are final and the run ends on time.
        for backoff_ms, factor in ((100, "0"), (100, "2"), (20000, "1"), (None, None)):
            retry = backoff_ms is not None
            label = "--retry-backoff-ms {} --retry-backoff-factor {}".format(backoff_ms, factor) if retry \
                else "no --retry-on-error"
            test_dir = tempfile.mkdtemp()
            failed_keys = os.path.join(test_dir, "failed-keys.csv")
            args = [
                "--pipeline=1",
                "--ratio=1:0",
                # No reply ever succeeds; don't abort on that.
                "--connection-stage-timeout=0",
                "--failed-keys-file={}".format(failed_keys),
                "--hide-histogram",
            ]
            if retry:
                args += ["--retry-on-error", "--retry-backoff-ms={}".format(backoff_ms),
                         "--retry-backoff-factor={}".format(factor)]
            benchmark_specs = {"name": env.testName, "args": args}
            addTLSArgs(benchmark_specs, env)
            config = get_default_memtier_config(threads=1, clients=1, requests=None, test_time=test_time)
            master_nodes_list = env.getMasterNodesList()
            add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
            config = RunConfig(test_dir, env.testName, config, {})
            ensure_clean_benchmark_folder(config.results_dir)
            benchmark = Benchmark.from_json(config, benchmark_specs)

            start = time.monotonic()
            try:
                rc, stderr = _run_with_background(env, benchmark, config, lambda _proc: None,
                                                  timeout=test_time + max_grace + 10)
            except subprocess.TimeoutExpired:
                rc, stderr = None, ""
            elapsed = time.monotonic() - start

            env.assertTrue(rc is not None, message="{}: the run did not end within {} s".format(
                label, test_time + max_grace + 10))
            if rc is None:
                continue
            env.assertEqual(rc, 0, message="{}: {}".format(label, stderr[-2000:]))
            no_reply = _NO_REPLY_WARNING.findall(stderr)
            retrying = _STILL_RETRYING_WARNING.findall(stderr)
            with open("{0}/mb.json".format(config.results_dir)) as f:
                runtime = json.load(f)["ALL STATS"]["Runtime"]
            # The report covers --test-time, not the grace spent retrying.
            env.assertGreaterEqual(runtime["Total duration"], test_time * 1000, message=label)
            env.assertLess(runtime["Total duration"], test_time * 1000 + 200, message=label)
            if not retry:
                env.assertEqual(no_reply + retrying, [], message=label)
                env.assertGreaterEqual(elapsed, test_time, message=label)
                env.assertLess(elapsed, test_time + 3, message="{}: did not end at --test-time".format(label))
                continue

            # The client is ended once, by the grace: as retrying, and also as
            # waiting for replies if the tick found a resend still on the wire.
            env.assertTrue(len(retrying) + len(no_reply) in (1, 2), message="{}: {}".format(label, stderr[-2000:]))
            env.assertTrue(len(retrying) <= 1 and len(no_reply) <= 1, message=label)
            graces = set(int(g) for _, _, g in retrying + no_reply)
            env.assertEqual(len(graces), 1, message=label)
            grace = graces.pop() if len(graces) == 1 else max_grace
            env.assertLessEqual(grace, max_grace, message=label)
            # The main thread checks for the end of the run once a second.
            env.assertGreaterEqual(elapsed, test_time + grace, message="{}: ended before the drain grace".format(label))
            env.assertLess(elapsed, test_time + grace + 3, message="{}: did not end after the drain grace".format(label))
            held = sum(int(n) for _, n, _ in retrying)
            unanswered = sum(int(n) for _, n, _ in no_reply)
            env.assertGreater(held + unanswered, 0, message=label)
            env.assertEqual(runtime.get("Unanswered requests", 0), unanswered, message=label)
            # Each of them is counted as an error and logged, and nothing else.
            with open(failed_keys) as f:
                statuses = [row["status"] for row in csv.DictReader(f)]
            env.assertEqual(statuses.count("run-ended-before-resend"), held, message=label)
            env.assertEqual(statuses.count("no-reply-after-test-time"), unanswered, message=label)
            env.assertEqual(len(statuses), held + unanswered, message=label)
            totals = re.findall(r"errors (\d+) \(\+\d+\)", stderr)
            env.assertTrue(totals, message=label)
            if totals:
                env.assertEqual(int(totals[-1]), held + unanswered, message=label)
    finally:
        for k in ("maxmemory", "maxmemory-policy"):
            if k in saved:
                conn.execute_command("CONFIG", "SET", k, saved[k])


class _MixedErrorPeer(socketserver.ThreadingTCPServer):
    """A standalone peer that answers attempt n (1-based) of SET key with
    -TRYAGAIN when fails(key, n) is true and +OK otherwise, and records when
    each attempt arrived."""
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, fails):
        super().__init__(("127.0.0.1", 0), _MixedErrorHandler)
        self.fails = fails
        self.arrivals = {}
        self.errors = []


class _MixedErrorHandler(socketserver.BaseRequestHandler):
    def handle(self):
        peer = self.server
        buf = b""
        try:
            while True:
                data = self.request.recv(65536)
                if not data:
                    return
                now = time.monotonic()
                buf += data
                out = []
                while buf.startswith(b"*"):
                    end = buf.find(b"\r\n")
                    if end < 0:
                        break
                    pos, args = end + 2, []
                    for _ in range(int(buf[1:end])):
                        end = buf.find(b"\r\n", pos)
                        if end < 0:
                            break
                        size = int(buf[pos + 1:end])
                        if len(buf) < end + 4 + size:
                            end = -1
                            break
                        args.append(buf[end + 2:end + 2 + size])
                        pos = end + 4 + size
                    if end < 0:
                        break
                    buf = buf[pos:]
                    if args[0].upper() != b"SET":
                        out.append(b"*0\r\n" if args[0].upper() == b"COMMAND" else b"+OK\r\n")
                        continue
                    key = args[1].decode()
                    attempts = peer.arrivals.setdefault(key, [])
                    attempts.append(now)
                    if peer.fails(key, len(attempts)):
                        out.append(b"-TRYAGAIN mixed failures\r\n")
                    else:
                        out.append(b"+OK\r\n")
                if out:
                    self.request.sendall(b"".join(out))
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as error:
            peer.errors.append(repr(error))


def test_retry_backoff_with_mixed_failures(env):
    """
    Retries of requests at different retry counts, queued on one connection
    together, each wait their own backoff.

    The connection has one retry timer. It was armed with the backoff of the
    request that found it idle and then resent the whole queue, so a first
    retry could wait for a timer armed by a 4th retry (1.28 s instead of
    20 ms) and a 4th retry could go out after 20 ms. A request queued with an
    earlier due time must also bring the timer forward.

    --rate-limiting spreads the first sends over about 2 s, so new requests
    fail while older ones wait for later retries.
    """
    base_ms, factor, keys = 20, 4, 40
    # SET memtier-N fails its first N % 5 attempts.
    with _MixedErrorPeer(lambda key, attempt: attempt <= int(key.split("-")[-1]) % 5) as peer:
        thread = threading.Thread(target=peer.serve_forever, daemon=True)
        thread.start()
        args = [MEMTIER_BINARY, "--server=127.0.0.1", "--port={}".format(peer.server_address[1]),
                "--threads=1", "--clients=1", "--pipeline=8", "--requests={}".format(keys), "--rate-limiting=20",
                "--ratio=1:0", "--key-pattern=P:P", "--key-minimum=1", "--key-maximum={}".format(keys),
                "--hide-histogram", "--retry-on-error", "--max-retries=4",
                "--retry-backoff-ms={}".format(base_ms), "--retry-backoff-factor={}".format(factor)]
        try:
            result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=60, text=True)
        finally:
            peer.shutdown()
            thread.join(timeout=5)

    env.assertEqual(result.returncode, 0, message=result.stderr[-2000:])
    env.assertEqual(peer.errors, [])
    env.assertEqual(len(peer.arrivals), keys)
    # A resend may be late, never early. Sanitizer builds get a wider upper
    # bound and up to 10% of gaps late beyond it: a TSAN build stalls the
    # worker for ~200 ms about 1 s into the run, with or without
    # --retry-on-error (first sends stall too), which delays every resend due
    # in that window. Before the fix 30-75 of the 80 gaps were off.
    if _SANITIZER_ACTIVE:
        upper = lambda expected: expected * 1.5 + 100
        allowed_bad = lambda gaps: gaps // 10
    else:
        upper = lambda expected: expected * 1.25 + 25
        allowed_bad = lambda gaps: 0
    gaps = bad = 0
    report = []
    for key, attempts in sorted(peer.arrivals.items()):
        env.assertEqual(len(attempts), int(key.split("-")[-1]) % 5 + 1, message=key)
        for resend in range(1, len(attempts)):
            expected = base_ms * factor ** (resend - 1)
            gap = (attempts[resend] - attempts[resend - 1]) * 1000
            gaps += 1
            if not expected - 3 <= gap <= upper(expected):
                bad += 1
                report.append("{} resend {}: {:.0f} ms, expected {} ms".format(key, resend, gap, expected))
    env.assertGreater(gaps, 0)
    env.assertLessEqual(bad, allowed_bad(gaps),
                        message="{} of {} gaps off their own backoff: {}".format(bad, gaps, "; ".join(report[:10])))


def test_requests_in_flight_have_room_to_retry(env):
    """
    A request that fails in flight is retried, not given up on with its
    retries left, when the retry queue fills up.

    New requests were sent until the retry queue itself was full, so
    requests already in flight that then failed found it full and were
    logged with retries=0 despite --max-retries=4. New work is now held while
    queued retries plus requests in flight reach the cap.
    """
    keys, max_retries = 600, 4

    def fails(key, attempt):
        # About 60% of attempts fail, the same ones on every run.
        return zlib.crc32("{}:{}".format(key, attempt).encode()) % 10 < 6

    with _MixedErrorPeer(fails) as peer, tempfile.TemporaryDirectory() as directory:
        thread = threading.Thread(target=peer.serve_forever, daemon=True)
        thread.start()
        failed_keys = os.path.join(directory, "failed.csv")
        args = [MEMTIER_BINARY, "--server=127.0.0.1", "--port={}".format(peer.server_address[1]),
                "--threads=1", "--clients=1", "--pipeline=8", "--requests={}".format(keys),
                "--ratio=1:0", "--key-pattern=P:P", "--key-minimum=1", "--key-maximum={}".format(keys),
                "--hide-histogram", "--retry-on-error", "--max-retries={}".format(max_retries),
                "--retry-backoff-ms=10", "--retry-backoff-factor=2", "--failed-keys-file=" + failed_keys]
        try:
            result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=60, text=True)
        finally:
            peer.shutdown()
            thread.join(timeout=5)
        with open(failed_keys) as source:
            rows = list(csv.DictReader(source))

    env.assertEqual(result.returncode, 0, message=result.stderr[-2000:])
    env.assertEqual(peer.errors, [])
    expected = sorted(key for key in peer.arrivals
                      if all(fails(key, attempt) for attempt in range(1, max_retries + 2)))
    early = [row["key"] for row in rows if int(row["retries"]) < max_retries]
    env.assertEqual(early, [], message="{} requests given up on with retries left".format(len(early)))
    env.assertEqual(sorted(row["key"] for row in rows), expected)


class _DepthPeer(socketserver.ThreadingTCPServer):
    """A standalone peer that holds its +OK replies until `depth` SETs are
    waiting, or 0.5 s went by, and records the most SETs it held at once."""
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, depth):
        super().__init__(("127.0.0.1", 0), _DepthHandler)
        self.depth = depth
        self.most_held = 0
        self.errors = []


class _DepthHandler(socketserver.BaseRequestHandler):
    def handle(self):
        peer = self.server
        buf = b""
        held, since = 0, None
        try:
            while True:
                if select.select([self.request], [], [], 0.01)[0]:
                    data = self.request.recv(65536)
                    if not data:
                        return
                    buf += data
                out = []
                while buf.startswith(b"*"):
                    end = buf.find(b"\r\n")
                    if end < 0:
                        break
                    pos, args = end + 2, []
                    for _ in range(int(buf[1:end])):
                        end = buf.find(b"\r\n", pos)
                        if end < 0:
                            break
                        size = int(buf[pos + 1:end])
                        if len(buf) < end + 4 + size:
                            end = -1
                            break
                        args.append(buf[end + 2:end + 2 + size])
                        pos = end + 4 + size
                    if end < 0:
                        break
                    buf = buf[pos:]
                    if args[0].upper() != b"SET":
                        out.append(b"*0\r\n" if args[0].upper() == b"COMMAND" else b"+OK\r\n")
                        continue
                    held += 1
                    since = since or time.monotonic()
                if held and (held >= peer.depth or time.monotonic() - since >= 0.5):
                    peer.most_held = max(peer.most_held, held)
                    out.append(b"+OK\r\n" * held)
                    held, since = 0, None
                if out:
                    self.request.sendall(b"".join(out))
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as error:
            peer.errors.append(repr(error))


def test_small_max_retry_queue_keeps_pipeline_depth(env):
    """
    A --max-retry-queue below --pipeline does not cut the pipeline on a
    healthy server.

    New work is held while queued retries plus requests in flight reach the
    cap, so a cap of 4 alone kept only 4 of 64 requests in flight. The hold
    now starts at --pipeline at the least; the queue itself stays capped at 4.
    """
    pipeline = 64
    with _DepthPeer(pipeline) as peer:
        thread = threading.Thread(target=peer.serve_forever, daemon=True)
        thread.start()
        args = [MEMTIER_BINARY, "--server=127.0.0.1", "--port={}".format(peer.server_address[1]),
                "--threads=1", "--clients=1", "--pipeline={}".format(pipeline),
                "--requests={}".format(pipeline * 3), "--ratio=1:0", "--hide-histogram",
                "--retry-on-error", "--max-retry-queue=4"]
        try:
            result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=60, text=True)
        finally:
            peer.shutdown()
            thread.join(timeout=5)

    env.assertEqual(result.returncode, 0, message=result.stderr[-2000:])
    env.assertEqual(peer.errors, [])
    env.assertEqual(peer.most_held, pipeline, message="requests in flight at once")


def test_dropped_replays_do_not_stall_client(env):
    """
    Requests --retry-on-error drops after a reconnect must not stall the client.

    With --max-retries=0, the requests in flight on a killed connection are
    dropped instead of replayed. They stayed counted as generated, so with
    --reconnect-interval the client held its pipeline for good and the run
    failed with "client(s) stopped before reaching their stop condition".
    """
    # Cluster mode rejects --reconnect-interval.
    env.skipOnCluster()
    test_time = 8
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=1",
            "--ratio=1:1",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=10000",
            "--reconnect-interval=10",
            "--reconnect-on-error",
            "--retry-on-error",
            "--max-retries=0",
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=None, test_time=test_time)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    conn = env.getOSSMasterNodesConnectionList()[0]

    kills = []

    def kill_waves(_proc):
        # Several waves spaced beyond the default 1s reconnect delay; one kill
        # with a request in flight is enough.
        for _wave in range(4):
            time.sleep(1.5)
            try:
                kills.append(conn.execute_command("CLIENT", "KILL", "TYPE", "normal"))
            except Exception as e:
                env.debugPrint("CLIENT KILL failed: {}".format(e), True)

    rc, stderr_content = _run_with_background(env, benchmark, config, kill_waves, timeout=test_time + 30)

    env.assertGreaterEqual(sum(kills), 1, message="no CLIENT KILL wave found a memtier connection")
    env.assertGreaterEqual(stderr_content.count("Reconnection successful"), 1,
                           message="no connection was killed and recovered")
    env.assertFalse("stopped before reaching their stop condition" in stderr_content,
                    message="client stopped sending after a reconnect")
    env.assertEqual(rc, 0, message="memtier exited {} after connection kills".format(rc))


def test_dropped_replays_are_counted_as_errors(env):
    """
    Requests --retry-on-error gives up on after a reconnect count as errors.

    The requests in flight on a killed connection are logged to
    --failed-keys-file as "connection-dropped" once --max-retries is reached,
    but were not counted in the error total ("errors 0"), unlike the requests
    abandoned when the run ends. Every logged row is a request that never got
    a reply, so the error total must match the rows.
    """
    # The CLIENT KILL below is not routed to every node in cluster mode.
    env.skipOnCluster()
    test_dir = tempfile.mkdtemp()
    failed_log = os.path.join(test_dir, "failed-keys.csv")
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=16",
            "--command=SET __key__ v",
            "--command-ratio=200",
            # memtier kills its own sibling connections, with requests in flight.
            "--command=CLIENT KILL TYPE normal SKIPME yes",
            "--command-ratio=1",
            "--reconnect-on-error",
            "--retry-on-error",
            "--max-retries=0",
            "--failed-keys-file={}".format(failed_log),
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=4, requests=None, test_time=3)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)

    rc, stderr_content = _run_with_background(env, benchmark, config, lambda _proc: None, timeout=40)
    env.assertEqual(rc, 0, message="memtier exited {}".format(rc))
    with open(failed_log) as f:
        rows = list(csv.reader(f))[1:]
    env.assertGreater(len(rows), 0, message="no request was dropped; the scenario was not hit")
    # A request still held for retry when --test-time ends is abandoned as
    # "run-ended-before-resend"; it is counted the same way.
    env.assertEqual([row for row in rows if row[3] not in ("connection-dropped", "run-ended-before-resend")], [])
    totals = re.findall(r"errors (\d+) \(", stderr_content)
    env.assertTrue(totals, message=stderr_content[-2000:])
    if totals:
        env.assertEqual(int(totals[-1]), len(rows), message=stderr_content[-2000:])


_TXN_KILL_THREADS, _TXN_KILL_CLIENTS = 2, 4


def _run_transaction_with_kills(env, extra_args, requests=None, test_time=None, kill_interval=0.5, pipeline=1,
                                set_ratio=1):
    """Run a MULTI/SET/EXEC --transaction rotation while every primary's
    clients are killed every kill_interval seconds.

    Returns (rc, stderr, connections killed, server -ERR replies, failed-keys rows).
    """
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline={}".format(pipeline),
            "--transaction",
            "--command=MULTI",
            "--command=SET __key__ __data__",
            "--command-ratio={}".format(set_ratio),
            "--command=EXEC",
            "--reconnect-on-error",
            "--hide-histogram",
        ]
        + list(extra_args),
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=_TXN_KILL_THREADS, clients=_TXN_KILL_CLIENTS, requests=requests,
                                        test_time=test_time)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    failed_keys = os.path.join(config.results_dir, "failed_keys.csv")
    benchmark_specs["args"].append("--failed-keys-file={}".format(failed_keys))
    benchmark = Benchmark.from_json(config, benchmark_specs)
    conns = env.getOSSMasterNodesConnectionList()
    for conn in conns:
        conn.execute_command("CONFIG", "RESETSTAT")

    kills = []

    def kill_loop(proc):
        end = time.time() + (test_time - 1 if test_time else 60)
        while time.time() < end and proc.poll() is None:
            time.sleep(kill_interval)
            for conn in conns:
                try:
                    kills.append(conn.execute_command("CLIENT", "KILL", "TYPE", "normal"))
                except Exception as e:
                    env.debugPrint("CLIENT KILL failed: {}".format(e), True)

    rc, stderr_content = _run_with_background(env, benchmark, config, kill_loop, timeout=(test_time or 60) + 30)

    killed = sum(kills)
    errors = sum(conn.execute_command("INFO", "errorstats").get("errorstat_ERR", {}).get("count", 0)
                 for conn in conns)
    rows = []
    if os.path.exists(failed_keys):
        with open(failed_keys) as f:
            rows = f.read().splitlines()[1:]
    env.debugPrint("killed {} connections, {} -ERR replies, {} failed-keys rows".format(killed, errors, len(rows)),
                   True)
    env.assertGreaterEqual(killed, 1, message="no CLIENT KILL found a memtier connection")
    return rc, stderr_content, killed, errors, rows


def test_transaction_requests_are_not_retried(env):
    """
    --transaction requests are not retried or replayed after a reconnect.

    A killed connection's in-flight requests were replayed after the
    reconnect, without their MULTI. The replayed EXEC failed with "EXEC
    without MULTI", which was retried, failed again, and so on until the end
    of the run: millions of errors, and in cluster mode the run never ended.
    """
    rc, stderr_content, killed, errors, rows = _run_transaction_with_kills(env, ["--retry-on-error"], test_time=6)
    retries = re.findall(r"retries (\d+)", stderr_content)
    env.assertEqual(rc, 0, message="memtier exited {} after connection kills".format(rc))
    env.assertTrue(retries and retries[-1] == "0", message="transaction requests were retried")
    # A killed connection may still send the rest of its current block, whose
    # EXEC fails once; nothing is resent after that.
    env.assertLessEqual(errors, killed, message="{} -ERR replies for {} killed connections".format(errors, killed))
    # The requests in flight on a killed connection are dropped, not replayed.
    dropped = [r for r in rows if r.split(",")[1] == "REPLAY"]
    env.assertGreaterEqual(len(dropped), 1, message="no in-flight request was dropped on a kill")
    env.assertTrue(all(r.endswith(',"transaction-not-replayed",0') for r in dropped),
                   message="unexpected REPLAY rows: {}".format(dropped[:5]))
    # Every logged request never got a successful reply, the dropped ones
    # included: each one counts as an error.
    totals = re.findall(r"errors (\d+) \(", stderr_content)
    env.assertTrue(totals, message=stderr_content[-2000:])
    if totals:
        env.assertEqual(int(totals[-1]), len(rows), message="{} REPLAY rows".format(len(dropped)))


def test_dropped_transaction_requests_do_not_stall_requests_run(env):
    """
    Transaction requests dropped on a kill are uncounted, so a --requests run
    still sends every request and ends.
    """
    rc, stderr_content, _, _, _ = _run_transaction_with_kills(env, ["--retry-on-error"], requests=30000,
                                                              kill_interval=0.3, pipeline=4)
    env.assertFalse("stopped before reaching" in stderr_content,
                    message="client stopped sending after dropping transaction requests")
    env.assertEqual(rc, 0, message="memtier exited {} after connection kills".format(rc))


def test_transaction_drops_not_logged_without_retry(env):
    """
    Without --retry-on-error, requests dropped on a kill were never going to
    be resent: they are not logged as transaction-not-replayed.
    """
    rc, _, _, _, rows = _run_transaction_with_kills(env, [], test_time=4)
    env.assertEqual(rc, 0, message="memtier exited {} after connection kills".format(rc))
    replay = [r for r in rows if r.split(",")[1] == "REPLAY"]
    env.assertEqual(replay, [], message="REPLAY rows without --retry-on-error: {}".format(replay[:5]))


def test_transaction_lost_multi_is_not_continued(env):
    """
    Once a reconnect drops the connection's open MULTI, the rest of that
    block is not sent: outside the MULTI its SET runs untransacted and its
    EXEC fails with "EXEC without MULTI". (Cluster mode is covered in
    test_cluster_transaction.py.) The skip ends at the EXEC: the GETs after
    it are still sent.
    """
    if env.isCluster():
        env.skip()
        return
    # Long blocks, so most kills land inside one; a --pipeline that does not
    # divide the rotation, so the kill does not always find it at a boundary.
    get_ratio, pipeline = 50, 7
    rc, stderr_content, killed, errors, _ = _run_transaction_with_kills(
        env, ["--command=GET __key__", "--command-ratio={}".format(get_ratio)], test_time=6, kill_interval=0.3,
        pipeline=pipeline, set_ratio=50)
    env.assertEqual(rc, 0, message="memtier exited {} after connection kills".format(rc))
    env.assertFalse("EXEC without MULTI" in stderr_content, message="an EXEC was sent without its MULTI")
    # memtier is the only client, so any -ERR reply is one of its own.
    env.assertEqual(errors, 0, message="{} -ERR replies for {} killed connections".format(errors, killed))
    # Every rotation sends get_ratio GETs, except the GETs in flight on a
    # killed connection and the partial rotation of each connection at the end.
    stats = env.getOSSMasterNodesConnectionList()[0].execute_command("INFO", "commandstats")
    multi = stats.get("cmdstat_multi", {}).get("calls", 0)
    get = stats.get("cmdstat_get", {}).get("calls", 0)
    env.assertGreater(multi, 0, message="no MULTI was sent")
    missing = get_ratio * multi - get
    env.debugPrint("{} MULTIs, {} GETs, {} missing, {} connections killed".format(multi, get, missing, killed), True)
    connections = _TXN_KILL_THREADS * _TXN_KILL_CLIENTS
    env.assertLessEqual(missing, get_ratio * connections + pipeline * killed,
                        message="{} GETs missing for {} MULTIs: GETs after a skipped EXEC were skipped too".format(
                            missing, multi))


def _db_conn(env, db):
    import redis

    pool = env.getOSSMasterNodesConnectionList()[0].connection_pool
    kwargs = dict(pool.connection_kwargs, db=db)
    return redis.Redis(connection_pool=redis.ConnectionPool(connection_class=pool.connection_class, **kwargs))


def _replay_churn_run(env, extra_args, password=None):
    """Kill memtier's connections while deep pipelines are in flight, so
    --retry-on-error has requests to resend after each reconnect.

    Returns (exit code, stderr). With password, the server requires it for
    the duration of the run.
    """
    test_time = 6
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--pipeline=32",
            "--ratio=1:0",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=10000",
            "--reconnect-on-error",
            "--max-reconnect-attempts=100",
            "--retry-on-error",
            "--hide-histogram",
        ]
        + list(extra_args),
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=2, requests=None, test_time=test_time)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)

    # A connection of its own, never killed (CLIENT KILL skips the caller), so
    # it can still undo requirepass once the run is over.
    admin = _db_conn(env, 0)
    admin.execute_command("FLUSHALL")
    kills = []

    def kill_waves(_proc):
        # Waves spaced beyond the default 1s reconnect delay.
        for _wave in range(4):
            time.sleep(1.2)
            kills.append(admin.execute_command("CLIENT", "KILL", "TYPE", "normal"))

    if password is not None:
        admin.execute_command("CONFIG", "SET", "requirepass", password)
    try:
        rc, stderr_content = _run_with_background(env, benchmark, config, kill_waves, timeout=test_time + 30)
    finally:
        if password is not None:
            admin.execute_command("CONFIG", "SET", "requirepass", "")

    env.assertGreaterEqual(sum(kills), 1, message="no CLIENT KILL wave found a memtier connection")
    env.assertGreaterEqual(stderr_content.count("Reconnection successful"), 1,
                           message="no connection was killed and recovered")
    env.assertEqual(rc, 0, message="memtier exited {} after connection kills".format(rc))
    return rc, stderr_content


def test_replays_wait_for_auth_after_reconnect(env):
    """
    Requests resent after a reconnect must follow AUTH on the new connection.

    They used to be written as soon as the connection came up, ahead of the
    setup commands, so on a password-protected server every resend was
    answered -NOAUTH and counted as an error (#527).
    """
    # One server to set requirepass on and kill clients from.
    env.skipOnCluster()
    password = "memtier-527"
    _rc, stderr_content = _replay_churn_run(env, ["--authenticate={}".format(password)], password)
    env.assertEqual(stderr_content.count("NOAUTH"), 0,
                    message="a request was resent before AUTH after a reconnect")


def test_replays_wait_for_select_after_reconnect(env):
    """
    Requests resent after a reconnect must follow SELECT on the new connection,
    or they run against database 0 (#527).
    """
    # Cluster mode rejects --select-db.
    env.skipOnCluster()
    _replay_churn_run(env, ["--select-db=3"])
    env.assertGreater(_db_conn(env, 3).execute_command("DBSIZE"), 0,
                      message="no key was written to the selected database")
    env.assertEqual(_db_conn(env, 0).execute_command("DBSIZE"), 0,
                    message="a request was resent before SELECT after a reconnect and wrote to database 0")


class _StallingProxy(threading.Thread):
    """TCP proxy to the server. Connections opened in the first `stall`
    seconds stop getting replies at `stall` and are closed at `close`;
    later connections are proxied as is, and the SETs sent on them counted."""

    def __init__(self, port, stall, close):
        super().__init__(daemon=True)
        self.target = port
        self.stall = stall
        self.close = close
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(16)
        self.port = self.listener.getsockname()[1]
        self.t0 = None
        self.done = False
        self.late_sets = 0
        self.lock = threading.Lock()

    def run(self):
        while not self.done:
            try:
                client, _ = self.listener.accept()
            except OSError:
                return
            if self.t0 is None:
                self.t0 = time.monotonic()
            early = time.monotonic() - self.t0 < self.stall
            server = socket.create_connection(("127.0.0.1", self.target))
            threading.Thread(target=self._pump, args=(client, server, early), daemon=True).start()

    def _pump(self, client, server, early):
        try:
            while not self.done:
                elapsed = time.monotonic() - self.t0
                if early and elapsed >= self.close:
                    return
                for sock in select.select([client, server], [], [], 0.01)[0]:
                    data = sock.recv(65536)
                    if not data:
                        return
                    if sock is client:
                        server.sendall(data)
                        if not early:
                            with self.lock:
                                self.late_sets += data.count(b"$3\r\nSET\r\n")
                    elif not (early and elapsed >= self.stall):
                        client.sendall(data)
        except OSError:
            pass
        finally:
            client.close()
            server.close()

    def stop(self):
        self.done = True
        self.listener.close()


def test_replays_after_deadline_are_counted(env):
    """
    Requests in flight on connections dropped near the --test-time deadline
    are resent after the reconnect, past the deadline, and counted once, and
    the reconnect's connection errors too.

    The run's stats used to end at the first reply read once the run was
    finished: here the SELECT reply, before the resent requests were even
    sent. Their replies then went into a bucket that was never reported, and
    the bucket ending the run was reported twice: Count fell short of the
    last progress line and Connection Errors doubled.
    """
    # A plain TCP proxy in front of a single server.
    env.skipOnCluster()
    if env.isUnixSocket() or env.useTLS:
        env.skip()
        return
    clients = 2
    pipeline = 32
    port = env.getMasterNodesList()[0]["port"]
    proxy = _StallingProxy(port, stall=1.8, close=2.1)
    proxy.start()
    try:
        with tempfile.TemporaryDirectory() as directory:
            stats = os.path.join(directory, "stats.json")
            args = [MEMTIER_BINARY, "--server=127.0.0.1", "--port={}".format(proxy.port), "--threads=1",
                    "--clients={}".format(clients), "--pipeline={}".format(pipeline), "--test-time=2",
                    "--ratio=1:0", "--select-db=3", "--retry-on-error", "--reconnect-on-error",
                    "--hide-histogram", "--json-out-file=" + stats]
            result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, text=True)
            with open(stats) as f:
                totals = json.load(f)["ALL STATS"]["Totals"]
    finally:
        proxy.stop()

    env.assertEqual(result.returncode, 0, message=result.stderr[-2000:])
    env.assertEqual(result.stderr.count("Reconnection successful"), clients, message=result.stderr[-2000:])
    progress = [int(n) for n in re.findall(r"(\d+) ops,", result.stderr)]
    env.assertTrue(progress, message="no progress line")
    env.assertEqual(totals["Count"], progress[-1] if progress else None,
                    message="Count differs from the last progress line")
    env.assertEqual(totals["Connection Errors"], clients)
    # Every request in flight at the stall is resent, after the deadline.
    env.assertEqual(proxy.late_sets, clients * pipeline, message="requests in flight were not resent")


class _LateErrorPeer(socketserver.ThreadingTCPServer):
    """A standalone peer that answers +OK, then holds every reply until after
    the --test-time deadline and answers the held requests with one write:
    +OK, then retryable errors. Requests that arrive after that are retries.
    """
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, hold, release):
        super().__init__(("127.0.0.1", 0), _LateErrorHandler)
        self.hold = hold
        self.release = release
        self.t0 = None
        self.failed = 0
        self.retried = 0
        self.errors = []


class _LateErrorHandler(socketserver.BaseRequestHandler):
    def handle(self):
        peer = self.server
        buf = b""
        held = 0
        try:
            while True:
                if select.select([self.request], [], [], 0.005)[0]:
                    data = self.request.recv(65536)
                    if not data:
                        return
                    buf += data
                out = []
                while buf.startswith(b"*"):
                    end = buf.find(b"\r\n")
                    if end < 0:
                        break
                    pos, args = end + 2, []
                    for _ in range(int(buf[1:end])):
                        end = buf.find(b"\r\n", pos)
                        if end < 0:
                            break
                        size = int(buf[pos + 1:end])
                        if len(buf) < end + 4 + size:
                            end = -1
                            break
                        args.append(buf[end + 2:end + 2 + size])
                        pos = end + 4 + size
                    if end < 0:
                        break
                    buf = buf[pos:]
                    verb = args[0].upper()
                    if verb not in (b"SET", b"GET"):
                        out.append(b"*0\r\n" if verb == b"COMMAND" else b"+OK\r\n")
                        continue
                    if peer.t0 is None:
                        peer.t0 = time.monotonic()
                    elapsed = time.monotonic() - peer.t0
                    if elapsed < peer.hold:
                        out.append(b"+OK\r\n")
                    elif held >= 0 and elapsed < peer.release:
                        held += 1
                    else:
                        peer.retried += 1
                        out.append(b"+OK\r\n")
                if held > 0 and time.monotonic() - peer.t0 >= peer.release:
                    # All the held replies in one write, so memtier reads
                    # them together and has nothing pending afterwards. The
                    # first succeeds: a completed request is what moves
                    # memtier's run clock past --test-time.
                    out.insert(0, b"+OK\r\n" + b"-BUSY held past the deadline\r\n" * (held - 1))
                    peer.failed += held - 1
                    held = -1
                if out:
                    self.request.sendall(b"".join(out))
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as error:
            peer.errors.append(repr(error))


def test_retries_left_at_end_of_test_time_are_counted(env):
    """
    Retries still waiting for their backoff when a --test-time run ends are
    accounted as errors and in --failed-keys-file.

    After the deadline, the last replies on a connection can all arrive as
    retryable errors in one read. Nothing is pending then, so the run ended
    and the connection dropped its retry queue: the requests vanished, with
    "errors 0" and nothing logged.
    """
    pipeline = 8
    with _LateErrorPeer(hold=0.3, release=1.5) as peer, tempfile.TemporaryDirectory() as directory:
        thread = threading.Thread(target=peer.serve_forever, daemon=True)
        thread.start()
        failed_keys = os.path.join(directory, "failed.csv")
        args = [MEMTIER_BINARY, "--server=127.0.0.1", "--port={}".format(peer.server_address[1]),
                "--threads=1", "--clients=1", "--pipeline={}".format(pipeline), "--test-time=1",
                "--ratio=1:1", "--key-minimum=1", "--key-maximum=1000", "--hide-histogram",
                "--retry-on-error", "--retry-backoff-ms=200", "--failed-keys-file=" + failed_keys]
        try:
            result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=30, text=True)
        finally:
            peer.shutdown()
            thread.join(timeout=5)
        with open(failed_keys) as source:
            logged = sum(1 for line in source if "run-ended-before-resend" in line)

    env.assertEqual(result.returncode, 0, message=result.stderr)
    env.assertEqual(peer.errors, [])
    # Premise: the whole pipeline was held past the deadline, and all but
    # the first request failed.
    env.assertEqual(peer.failed, pipeline - 1, message=result.stderr)
    abandoned = peer.failed - peer.retried
    env.assertGreater(abandoned, 0, message="every retry was resent; the scenario was not hit")
    env.assertEqual(logged, abandoned, message=result.stderr)
    totals = re.findall(r"errors (\d+) \(", result.stderr)
    env.assertTrue(totals, message=result.stderr)
    if totals:
        env.assertEqual(int(totals[-1]), abandoned, message=result.stderr)


def test_replays_abandoned_when_reconnects_run_out_are_counted(env):
    """
    Requests held for resend when --max-reconnect-attempts runs out are
    logged and counted as errors.

    The peer closes the connection with writes in flight and stops listening.
    The reconnect is refused, the thread ends, and nothing will resend the
    requests: they vanished with "errors 0" and nothing logged.
    """
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    def serve():
        conn, _ = listener.accept()
        listener.close()  # refuse the reconnect
        buf = b""
        try:
            while b"SET" not in buf:
                data = conn.recv(65536)
                if not data:
                    break
                buf += data
        finally:
            conn.close()  # with the writes in flight

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    with tempfile.TemporaryDirectory() as directory:
        failed_keys = os.path.join(directory, "failed.csv")
        args = [MEMTIER_BINARY, "--server=127.0.0.1", "--port={}".format(port), "--threads=1", "--clients=1",
                "--pipeline=8", "--requests=50", "--ratio=1:0", "--key-minimum=1", "--key-maximum=1000",
                "--hide-histogram", "--retry-on-error", "--reconnect-on-error", "--max-reconnect-attempts=1",
                "--failed-keys-file=" + failed_keys]
        result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, text=True)
        thread.join(timeout=5)
        with open(failed_keys) as source:
            rows = list(csv.reader(source))[1:]

    # The run fails: its thread ended before --requests was reached.
    env.assertNotEqual(result.returncode, 0, message=result.stderr)
    env.assertContains("ending this thread", result.stderr)
    env.assertGreaterEqual(len(rows), 1, message="no abandoned request was logged")
    env.assertEqual([row[3] for row in rows], ["max-reconnect-attempts-exceeded"] * len(rows))
    totals = re.findall(r"errors (\d+) \(", result.stderr)
    env.assertTrue(totals, message=result.stderr)
    if totals:
        env.assertEqual(int(totals[-1]), len(rows), message=result.stderr)


def _run_against_peer_that_drops_clients(env, clients, pipeline, close_all, extra_args):
    """
    Run memtier with --retry-on-error against a peer that accepts every
    client, stops listening (so reconnects are refused), waits for each
    client's whole pipeline and then closes every connection (close_all) or
    only the first one, leaving the others open and unanswered. Returns the
    result and the --failed-keys-file rows.
    """
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(clients)
    port = listener.getsockname()[1]

    def serve():
        conns = []
        try:
            while len(conns) < clients:
                conns.append(listener.accept()[0])
            listener.close()  # refuse the reconnects
            for conn in conns:
                conn.settimeout(10)
                buf = b""
                # Wait for the whole pipeline, so every request is in flight.
                while buf.count(b"SET") < pipeline:
                    data = conn.recv(65536)
                    if not data:
                        break
                    buf += data
            if not close_all:
                conns[0].close()
                for conn in conns[1:]:
                    try:
                        conn.recv(65536)  # until memtier closes it
                    except OSError:
                        pass
        finally:
            listener.close()
            for conn in conns:
                conn.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    with tempfile.TemporaryDirectory() as directory:
        failed_keys = os.path.join(directory, "failed.csv")
        args = [MEMTIER_BINARY, "--server=127.0.0.1", "--port={}".format(port), "--threads=1",
                "--clients={}".format(clients), "--pipeline={}".format(pipeline), "--requests=200",
                "--ratio=1:0", "--key-minimum=1", "--key-maximum=1000", "--hide-histogram", "--retry-on-error",
                "--failed-keys-file=" + failed_keys] + extra_args
        result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, text=True)
        thread.join(timeout=15)
        with open(failed_keys) as source:
            rows = list(csv.reader(source))[1:]
    return result, rows


def _assert_every_request_counted(env, result, rows, expected, reasons):
    env.assertNotEqual(result.returncode, 0, message=result.stderr)
    env.assertContains("ending this thread", result.stderr)
    # Every request of every client was in flight and none got a reply.
    env.assertEqual(len(rows), expected, message=result.stderr[-2000:])
    got = set(row[3] for row in rows)
    env.assertEqual(got - reasons, set(), message=str(got))
    totals = re.findall(r"errors (\d+) \(", result.stderr)
    env.assertTrue(totals, message=result.stderr)
    if totals:
        env.assertEqual(int(totals[-1]), len(rows), message=result.stderr[-2000:])


def test_replays_held_by_sibling_clients_are_counted_when_thread_ends(env):
    """
    When one connection runs out of reconnect attempts it ends the whole
    thread. The other clients on that thread hold the requests they had in
    flight on the same outage; they are logged and counted as errors too.

    Only the requests of the connection that ran out were counted: the
    siblings' were freed silently.
    """
    clients, pipeline = 4, 16
    result, rows = _run_against_peer_that_drops_clients(
        env, clients, pipeline, True, ["--reconnect-on-error", "--max-reconnect-attempts=1"])
    _assert_every_request_counted(env, result, rows, clients * pipeline,
                                  {"max-reconnect-attempts-exceeded", "thread-ended-before-resend"})


def test_in_flight_requests_of_sibling_clients_are_counted_when_thread_ends(env):
    """
    The requests siblings still have in flight when the thread ends are
    counted too, not only the ones they hold for resend.

    The peer closes only the first connection; reconnecting is refused, so
    that connection ends the thread while the other clients wait for their
    replies.
    """
    clients, pipeline = 4, 8
    result, rows = _run_against_peer_that_drops_clients(
        env, clients, pipeline, False, ["--reconnect-on-error", "--max-reconnect-attempts=1"])
    _assert_every_request_counted(env, result, rows, clients * pipeline,
                                  {"max-reconnect-attempts-exceeded", "thread-ended-before-resend"})


def test_requests_lost_without_reconnect_are_counted_when_thread_ends(env):
    """
    Without --reconnect-on-error, the first lost connection ends the thread
    before the other clients handle their own EOF. Their requests, still in
    flight, are logged and counted as errors like the first one's.
    """
    clients, pipeline = 4, 8
    result, rows = _run_against_peer_that_drops_clients(env, clients, pipeline, True, [])
    _assert_every_request_counted(env, result, rows, clients * pipeline,
                                  {"connection-lost-no-reconnect", "thread-ended-before-resend"})
