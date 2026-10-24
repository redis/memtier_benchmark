"""
Regression test for issue #712: a --retry-backoff-factor below 1 with
unlimited --max-retries must not shrink the retry delay to 0 ms.

Run subset:
  TEST=test_retry_backoff_floor.py ./tests/run_tests.sh
"""

import re
import subprocess
import tempfile
import time
from include import *
from mb import Benchmark, RunConfig


def _set_rejected_calls(connections):
    # cmdstat_set is absent until the first SET.
    return sum(int(conn.info("commandstats").get("cmdstat_set", {}).get("rejected_calls", 0)) for conn in connections)


def _retried_ops(stderr_path):
    # Last "retried_ops N" of the live progress line, None before the first.
    with open(stderr_path) as f:
        found = re.findall(r"retried_ops (\d+)", f.read())
    return int(found[-1]) if found else None


def test_retry_backoff_factor_below_one_keeps_a_floor(env):
    """
    With --retry-backoff-factor 0.5 the delay of a request the server keeps
    rejecting settles at 100 ms instead of decaying to 0.

    Every SET fails with -OOM (maxmemory 1, noeviction), so every request is
    resent forever. The 400 ms base delay decays to 200 ms, then 100 ms, and
    stays there: each stuck request is resent ~10 times per second. Before the
    fix it went on decaying to 0 and each request was resent thousands of
    times per second (100k-400k resends/s per connection).

    The rate is measured per stuck request (memtier's retried_ops, which stays
    constant once no new request can be sent), since how many requests get
    stuck depends on the topology: up to --max-retry-queue on a standalone
    connection, about one per shard connection in cluster mode. The run is
    killed after the measured window rather than left to end at --test-time.
    """
    masters = env.getOSSMasterNodesConnectionList()
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--ratio=1:0",
            "--key-pattern=R:R",
            "--key-minimum=1",
            "--key-maximum=1000",
            "--hide-histogram",
            "--retry-on-error",
            "--max-retries=-1",
            "--retry-backoff-ms=400",
            "--retry-backoff-factor=0.5",
            "--max-retry-queue=16",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=None, test_time=60)
    config["memtier_benchmark"]["pipeline"] = 1
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    stderr_path = "{}/mb.stderr".format(config.results_dir)

    # rejected_calls is Redis 6.2+.
    version = tuple(int(x) for x in masters[0].info("server")["redis_version"].split(".")[:2])
    if version < (6, 2):
        env.skip()
        return

    saved = []
    for conn in masters:
        values = conn.execute_command("CONFIG", "GET", "maxmemory*")
        # Keys come back as bytes without decoded responses.
        saved.append({k.decode() if isinstance(k, bytes) else k: v for k, v in zip(values[0::2], values[1::2])})
    proc = None
    changed = []
    try:
        for conn in masters:
            changed.append(conn)
            conn.execute_command("CONFIG", "SET", "maxmemory-policy", "noeviction")
            conn.execute_command("CONFIG", "SET", "maxmemory", "1")
        with open(stderr_path, "w") as stderr_file:
            proc = subprocess.Popen(benchmark.args, stdout=subprocess.DEVNULL, stderr=stderr_file)
            # Let the queue fill and every request decay to the floor
            # (400 + 200 + 100 ms), with a live line printed since.
            time.sleep(2)
            env.assertIsNone(proc.poll())
            stuck_before = _retried_ops(stderr_path)
            before = _set_rejected_calls(masters)
            start = time.monotonic()
            time.sleep(3)
            after = _set_rejected_calls(masters)
            elapsed = time.monotonic() - start
            stuck_after = _retried_ops(stderr_path)
    finally:
        if proc is not None and proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        for conn, values in zip(changed, saved):
            for k in ("maxmemory", "maxmemory-policy"):
                if k in values:
                    conn.execute_command("CONFIG", "SET", k, values[k])

    env.assertIsNotNone(stuck_after)
    env.assertGreater(stuck_after or 0, 0)
    # No request completes, so the set of stuck requests must not change.
    env.assertEqual(stuck_before, stuck_after)
    rate = (after - before) / elapsed / max(stuck_after or 0, 1)
    env.debugPrint("{} stuck requests, {:.1f} resends/s each".format(stuck_after, rate), True)
    # 10/s at the 100 ms floor; 2.5/s would mean the floor is the 400 ms base,
    # and an unfloored delay gives thousands.
    env.assertGreaterEqual(rate, 5, message="{:.1f} resends/s per request; expected ~10".format(rate))
    env.assertLessEqual(rate, 20,
                        message="{:.1f} resends/s per request; the retry delay decayed below its floor".format(rate))
