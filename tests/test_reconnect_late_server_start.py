"""Regression test for a server that comes up after memtier starts (issue #708).

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

from include import MEMTIER_BINARY


def _free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def test_server_started_after_memtier(env):
    env.skipOnCluster()
    redis_server = os.environ.get("REDIS_SERVER") or shutil.which("redis-server")
    if not redis_server:
        env.skip()
        return

    test_time = 5
    port = _free_port()
    test_dir = tempfile.mkdtemp()
    json_path = os.path.join(test_dir, "mb.json")
    memtier = None
    server = None
    try:
        memtier = subprocess.Popen(
            [
                MEMTIER_BINARY,
                "--server=127.0.0.1",
                "--port={}".format(port),
                "--threads=1",
                "--clients=2",
                "--reconnect-on-error",
                "--connection-stage-timeout=0",
                "--test-time={}".format(test_time),
                "--hide-histogram",
                "--json-out-file={}".format(json_path),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        # Let the first connects and a reconnect fail before the server is up.
        time.sleep(2)
        server = subprocess.Popen(
            [redis_server, "--port", str(port), "--save", "", "--appendonly", "no", "--dir", test_dir],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        _, stderr = memtier.communicate(timeout=60)
        env.assertEqual(memtier.returncode, 0, message=stderr[-2000:])

        with open(json_path) as f:
            stats = json.load(f)["ALL STATS"]
        totals = stats["Totals"]
        env.assertGreater(totals["Connection Errors"], 0, message=repr(totals))
        env.assertGreater(totals["Count"], 0, message=stderr[-2000:])
        # The run lasts --test-time from the first connect, not 0 s.
        duration_ms = stats["Runtime"]["Total duration"]
        env.assertGreaterEqual(duration_ms, (test_time - 1) * 1000, message=repr(stats["Runtime"]))
        env.assertLessEqual(duration_ms, (test_time + 2) * 1000, message=repr(stats["Runtime"]))
        # Time-series keys are seconds into the run.
        seconds = [int(s) for s in totals["Time-Serie"].keys()]
        env.assertLessEqual(max(seconds), test_time + 2, message=repr(sorted(seconds)))
    finally:
        for proc in (memtier, server):
            if proc is not None and proc.poll() is None:
                proc.kill()
            if proc is not None:
                proc.wait(timeout=10)
        shutil.rmtree(test_dir, ignore_errors=True)
