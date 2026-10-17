"""Regression tests for a server that comes up after memtier starts (issue #708).

With --reconnect-on-error, a client whose first connect fails records a
connection error before its start time is set. The per-second stats used to be
rolled to the second "now - 0", an epoch-sized number, so as soon as the
client connected its --test-time looked over: the run sent 0 requests, and the
JSON time series was keyed by epoch seconds.

Run with:
  TEST=test_reconnect_late_server_start.py ./tests/run_tests.sh
"""

import json
import os
import shutil
import socket
import subprocess
import tempfile
import time

import redis

from include import MEMTIER_BINARY

CLIENTS = 2
SERVER_DELAY_SECS = 2


def _free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _run_with_late_server(env, stop_args):
    """Run memtier against a port redis only listens on SERVER_DELAY_SECS later.

    Returns (returncode, stderr, ALL STATS, commandstats calls), or None after
    reporting the failure if the server could not be started.
    """
    redis_server = os.environ.get("REDIS_SERVER") or shutil.which("redis-server")
    port = _free_port()
    test_dir = tempfile.mkdtemp()
    json_path = os.path.join(test_dir, "mb.json")
    server_log = os.path.join(test_dir, "redis.log")
    memtier = None
    server = None
    try:
        memtier = subprocess.Popen(
            [
                MEMTIER_BINARY,
                "--server=127.0.0.1",
                "--port={}".format(port),
                "--threads=1",
                "--clients={}".format(CLIENTS),
                "--reconnect-on-error",
                "--connection-stage-timeout=0",
                "--hide-histogram",
                "--json-out-file={}".format(json_path),
            ]
            + stop_args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        # Let the first connects and a reconnect fail before the server is up.
        time.sleep(SERVER_DELAY_SECS)
        server = subprocess.Popen(
            [redis_server, "--port", str(port), "--save", "", "--appendonly", "no", "--dir", test_dir,
             "--logfile", server_log],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        conn = redis.Redis(host="127.0.0.1", port=port, socket_timeout=5)
        deadline = time.monotonic() + 10
        while True:
            # Another process may have taken the port between _free_port()
            # and now; redis then exits instead of listening.
            if server.poll() is not None:
                with open(server_log) as f:
                    log = f.read()
                env.assertTrue(False, message="redis-server on port {} exited: {}".format(port, log[-1000:]))
                return None
            try:
                pid = conn.info("server")["process_id"]
                if pid != server.pid:
                    env.assertTrue(False, message="port {} was taken by another server (pid {})".format(port, pid))
                    return None
                break
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
                if time.monotonic() > deadline:
                    env.assertTrue(False, message="redis-server on port {} did not come up".format(port))
                    return None
                time.sleep(0.05)
        _, stderr = memtier.communicate(timeout=60)
        with open(json_path) as f:
            stats = json.load(f)["ALL STATS"]
        info = conn.info("commandstats")
        calls = sum(v["calls"] for k, v in info.items() if k in ("cmdstat_get", "cmdstat_set"))
        return memtier.returncode, stderr, stats, calls
    finally:
        for proc in (memtier, server):
            if proc is not None and proc.poll() is None:
                proc.kill()
            if proc is not None:
                proc.wait(timeout=10)
        shutil.rmtree(test_dir, ignore_errors=True)


def _check_common(env, result):
    returncode, stderr, stats, calls = result
    env.assertEqual(returncode, 0, message=stderr[-2000:])
    totals = stats["Totals"]
    # Every client fails at least twice before redis is up, and each of those
    # errors must survive the late start (#708). Don't tie the count to the
    # "Connection error:" log line: it is only printed when errno is set, so an
    # error can be counted without one.
    env.assertGreaterEqual(totals["Connection Errors"], 2 * CLIENTS, message=repr(totals) + stderr[-2000:])
    env.assertGreater(totals["Count"], 0, message=stderr[-2000:])
    env.assertEqual(totals["Count"], calls, message="server saw {} GET/SET calls".format(calls))
    return totals


def test_server_started_after_memtier(env):
    env.skipOnCluster()
    if not (os.environ.get("REDIS_SERVER") or shutil.which("redis-server")):
        env.skip()
        return
    test_time = 5
    result = _run_with_late_server(env, ["--test-time={}".format(test_time)])
    if result is None:
        return
    totals = _check_common(env, result)
    # The run lasts --test-time from the first connect, not 0 s.
    runtime = result[2]["Runtime"]
    env.assertGreaterEqual(runtime["Total duration"], (test_time - 1) * 1000, message=repr(runtime))
    env.assertLessEqual(runtime["Total duration"], (test_time + 2) * 1000, message=repr(runtime))
    # Time-series keys are seconds into the run.
    seconds = [int(s) for s in totals["Time-Serie"].keys()]
    env.assertLessEqual(max(seconds), test_time + 2, message=repr(sorted(seconds)))


def test_server_started_after_memtier_requests(env):
    env.skipOnCluster()
    if not (os.environ.get("REDIS_SERVER") or shutil.which("redis-server")):
        env.skip()
        return
    requests = 1000
    result = _run_with_late_server(env, ["--requests={}".format(requests)])
    if result is None:
        return
    totals = _check_common(env, result)
    env.assertEqual(totals["Count"], requests * CLIENTS, message=repr(totals))
    seconds = [int(s) for s in totals["Time-Serie"].keys()]
    env.assertLessEqual(max(seconds), 10, message=repr(sorted(seconds)))
