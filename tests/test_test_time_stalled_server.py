import json
import subprocess
import tempfile
import time
from include import *
from mb import Benchmark, RunConfig

# Must match TEST_TIME_DRAIN_GRACE_SECS in client.h.
DRAIN_GRACE_SECS = 10


def test_test_time_ends_when_server_stops_replying(env):
    """
    Regression test for https://github.com/redis/memtier_benchmark/issues/657.

    A --test-time run whose server stops replying while requests are in
    flight used to wait for those replies forever: no final report, and the
    process had to be killed. CLIENT PAUSE ALL keeps the connections open but
    never answers them. The run must end TEST_TIME_DRAIN_GRACE_SECS after its
    deadline, count the unanswered requests as errors, and still report the
    --test-time window.
    """
    test_time = 3
    benchmark_specs = {
        "name": env.testName,
        "args": ["--ratio=1:1"],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=2, clients=2, requests=None, test_time=test_time)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    master_nodes_connections = env.getOSSMasterNodesConnectionList()

    stdout_path = "{0}/mb.stdout".format(config.results_dir)
    stderr_path = "{0}/mb.stderr".format(config.results_dir)
    # Wait well past the grace period, but not until the pause ends: a run
    # that only ends then waited for the server, not for the grace period.
    # CLIENT UNPAUSE is itself held by CLIENT PAUSE ALL, so the pause is
    # bounded instead.
    wait_secs = test_time + DRAIN_GRACE_SECS + 5
    pause_ms = (wait_secs + 8) * 1000
    hung = False
    with open(stdout_path, "w") as stdout_f, open(stderr_path, "w") as stderr_f:
        proc = subprocess.Popen(benchmark.args, stdout=stdout_f, stderr=stderr_f, cwd=config.results_dir)
        time.sleep(test_time / 2.0)
        for conn in master_nodes_connections:
            conn.execute_command("CLIENT", "PAUSE", str(pause_ms), "ALL")
        try:
            proc.wait(timeout=wait_secs)
        except subprocess.TimeoutExpired:
            hung = True
            proc.kill()
            proc.wait(timeout=10)

    with open(stderr_path) as f:
        stderr_content = f.read()
    if hung or proc.returncode != 0:
        debugPrintMemtierOnError(config, env)

    env.assertFalse(hung, message="memtier did not end after --test-time while the server was paused")
    env.assertEqual(proc.returncode, 0)
    env.assertTrue("seconds after the test time ended; counted as errors" in stderr_content)

    # The report is still written, and covers --test-time, not the grace period.
    json_path = "{0}/mb.json".format(config.results_dir)
    env.assertTrue(os.path.isfile(json_path))
    if os.path.isfile(json_path):
        with open(json_path) as f:
            results = json.load(f)
        runtime = results["ALL STATS"]["Runtime"]
        env.assertTrue(runtime["Total duration"] < (test_time + 2) * 1000)
