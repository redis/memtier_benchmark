import json
import re
import subprocess
import tempfile
import time
from include import *
from mb import Benchmark, RunConfig

# Upper bound for TEST_TIME_DRAIN_GRACE_SECS in client.h; the value the run
# actually used is read back from its warning.
MAX_DRAIN_GRACE_SECS = 10
STALL_WARNING = re.compile(
    r"warning: (\d+) client\(s\) got no reply to (\d+) request\(s\) (\d+) seconds after the test time ended"
)


def run_with_stalled_server(env, extra_args):
    """
    Run --test-time and stop the server replying half way: CLIENT PAUSE WRITE
    keeps the connections open but never answers a SET (the workload is all
    SETs). Returns (hung, returncode, stderr, config).
    """
    test_time = 3
    benchmark_specs = {"name": env.testName, "args": ["--ratio=1:0"] + extra_args}
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
    hung = False
    with open(stdout_path, "w") as stdout_f, open(stderr_path, "w") as stderr_f:
        proc = subprocess.Popen(benchmark.args, stdout=stdout_f, stderr=stderr_f, cwd=config.results_dir)
        try:
            time.sleep(test_time / 2.0)
            for conn in master_nodes_connections:
                conn.execute_command("CLIENT", "PAUSE", "600000", "WRITE")
            try:
                proc.wait(timeout=test_time + MAX_DRAIN_GRACE_SECS + 10)
            except subprocess.TimeoutExpired:
                hung = True
                proc.kill()
                proc.wait(timeout=10)
        finally:
            for conn in master_nodes_connections:
                try:
                    conn.execute_command("CLIENT", "UNPAUSE")
                except Exception:
                    pass

    with open(stderr_path) as f:
        stderr_content = f.read()
    if hung or proc.returncode != 0:
        debugPrintMemtierOnError(config, env)
    return hung, proc.returncode, stderr_content, config, test_time


def check_stalled_run(env, extra_args):
    hung, returncode, stderr_content, config, test_time = run_with_stalled_server(env, extra_args)

    env.assertFalse(hung, message="memtier did not end after --test-time while the server was paused")
    env.assertEqual(returncode, 0)
    # Every client (2 threads x 2) had a SET in flight and is ended. A thread
    # warns once per tick that ends some of its clients: clients whose own
    # deadline falls on either side of a tick are reported on two lines.
    warnings = STALL_WARNING.findall(stderr_content)
    env.assertTrue(len(warnings) >= 2)
    env.assertEqual(sum(int(clients) for clients, _, _ in warnings), 4)
    for clients, requests, grace in warnings:
        env.assertTrue(int(requests) >= int(clients))
        env.assertTrue(int(grace) <= MAX_DRAIN_GRACE_SECS)

    # The report is still written, and covers --test-time, not the grace period.
    json_path = "{0}/mb.json".format(config.results_dir)
    env.assertTrue(os.path.isfile(json_path))
    if os.path.isfile(json_path):
        with open(json_path) as f:
            results = json.load(f)
        runtime = results["ALL STATS"]["Runtime"]
        env.assertTrue(runtime["Total duration"] < (test_time + 2) * 1000)
    return stderr_content


def test_test_time_ends_when_server_stops_replying(env):
    """
    Regression test for https://github.com/redis/memtier_benchmark/issues/657.

    A --test-time run whose server stops replying while requests are in
    flight used to wait for those replies forever: no final report, and the
    process had to be killed. The run must end TEST_TIME_DRAIN_GRACE_SECS
    after its deadline, count the unanswered requests as errors, and still
    report the --test-time window.
    """
    check_stalled_run(env, [])


def test_test_time_stalled_server_retry_on_error(env):
    """
    Same, with --retry-on-error: the abandoned requests must not also be held
    for a resend, which would count and report them a second time.
    """
    stderr_content = check_stalled_run(env, ["--retry-on-error"])
    env.assertFalse("held for retry when the run ended were not resent" in stderr_content)
