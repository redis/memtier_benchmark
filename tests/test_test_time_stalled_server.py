import csv
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
TEST_TIME = 3
STALL_WARNING = re.compile(
    r"warning: (\d+) client\(s\) got no reply to (\d+) request\(s\) (\d+) seconds after the test time ended"
)
RETRYING_WARNING = re.compile(
    r"warning: (\d+) client\(s\) were still retrying (\d+) request\(s\) (\d+) seconds after the test time ended"
)
PROGRESS_ERRORS = re.compile(r"   errors (\d+) \(\+\d+\)")


def pause_writes(conns):
    # Keeps the connections open but never answers a SET (the workloads
    # below are all SETs).
    for conn in conns:
        conn.execute_command("CLIENT", "PAUSE", "600000", "WRITE")


def run_memtier(env, extra_args, action, clients=2, wait=TEST_TIME + MAX_DRAIN_GRACE_SECS + 10):
    """
    Run an all-SET --test-time run with 2 threads and call action(masters)
    half way through it. Returns a dict with the outcome.
    """
    benchmark_specs = {"name": env.testName, "args": ["--ratio=1:0"] + extra_args}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=2, clients=clients, requests=None, test_time=TEST_TIME)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    failed_keys_path = "{0}/failed_keys.csv".format(config.results_dir)
    args = benchmark.args + ["--failed-keys-file={0}".format(failed_keys_path)]
    master_nodes_connections = env.getOSSMasterNodesConnectionList()

    stdout_path = "{0}/mb.stdout".format(config.results_dir)
    stderr_path = "{0}/mb.stderr".format(config.results_dir)
    hung = False
    start = time.time()
    with open(stdout_path, "w") as stdout_f, open(stderr_path, "w") as stderr_f:
        proc = subprocess.Popen(args, stdout=stdout_f, stderr=stderr_f, cwd=config.results_dir)
        try:
            time.sleep(TEST_TIME / 2.0)
            action(master_nodes_connections)
            try:
                proc.wait(timeout=wait)
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
    wall = time.time() - start

    with open(stderr_path) as f:
        stderr_content = f.read()
    if hung or proc.returncode != 0:
        debugPrintMemtierOnError(config, env)

    results = None
    json_path = "{0}/mb.json".format(config.results_dir)
    if os.path.isfile(json_path):
        # A killed run leaves a truncated file.
        with open(json_path) as f:
            try:
                results = json.load(f)
            except ValueError:
                pass
    failed_rows = []
    if os.path.isfile(failed_keys_path):
        with open(failed_keys_path, newline="") as f:
            failed_rows = list(csv.DictReader(f))
    return {
        "hung": hung,
        "returncode": proc.returncode,
        "stderr": stderr_content,
        "results": results,
        "failed_rows": failed_rows,
        "wall": wall,
    }


def check_stalled_run(env, extra_args, expected_clients):
    out = run_memtier(env, extra_args, pause_writes)

    env.assertFalse(out["hung"], message="memtier did not end after --test-time while the server was paused")
    env.assertEqual(out["returncode"], 0)
    # Every active client had a SET in flight and is ended. A thread warns
    # once per tick that ends some of its clients: clients whose own deadline
    # falls on either side of a tick are reported on separate lines.
    warnings = STALL_WARNING.findall(out["stderr"])
    env.assertTrue(len(warnings) >= 2)
    env.assertEqual(sum(int(clients) for clients, _, _ in warnings), expected_clients)
    abandoned = sum(int(requests) for _, requests, _ in warnings)
    for clients, requests, grace in warnings:
        env.assertTrue(int(requests) >= int(clients))
        env.assertTrue(int(grace) <= MAX_DRAIN_GRACE_SECS)

    # Each abandoned request is logged to --failed-keys-file, and nothing else.
    statuses = [row["status"] for row in out["failed_rows"]]
    env.assertEqual(statuses.count("no-reply-after-test-time"), abandoned)
    env.assertEqual(len(statuses), abandoned)

    # The report is still written, covers --test-time (not the grace period)
    # and says how many requests were abandoned.
    env.assertTrue(out["results"] is not None)
    if out["results"] is not None:
        runtime = out["results"]["ALL STATS"]["Runtime"]
        env.assertTrue(runtime["Total duration"] < (TEST_TIME + 2) * 1000)
        env.assertEqual(runtime.get("Unanswered requests"), abandoned)
    return out, abandoned


def test_test_time_ends_when_server_stops_replying(env):
    """
    Regression test for https://github.com/redis/memtier_benchmark/issues/657.

    A --test-time run whose server stops replying while requests are in
    flight used to wait for those replies forever: no final report, and the
    process had to be killed. The run must end TEST_TIME_DRAIN_GRACE_SECS
    after its deadline, count the unanswered requests as errors, log them to
    --failed-keys-file and still report the --test-time window.
    """
    check_stalled_run(env, [], expected_clients=4)


def test_test_time_stalled_server_retry_on_error(env):
    """
    Same, with --retry-on-error: the abandoned requests must be counted as
    errors exactly once (the progress line shows the error count), and not
    also held for a resend, which would count and report them a second time.
    """
    out, abandoned = check_stalled_run(env, ["--retry-on-error"], expected_clients=4)
    env.assertFalse("held for retry when the run ended were not resent" in out["stderr"])
    # The last progress line has the final error count (the "errors" field is
    # only printed once it is non-zero).
    errors = PROGRESS_ERRORS.findall(out["stderr"])
    env.assertEqual(int(errors[-1]) if errors else 0, abandoned)


def test_test_time_stalled_server_staircase(env):
    """
    Staircase mode: the step timer must not keep the run alive once the
    stalled clients are ended. The next step is due long after the deadline,
    so only the first client of each thread is active.
    """
    out, _ = check_stalled_run(env, ["--clients-start=1", "--clients-step=1", "--step-duration=60"], expected_clients=2)
    env.assertTrue(out["wall"] < TEST_TIME + MAX_DRAIN_GRACE_SECS + 10)


def check_reconnecting_client_left_alone(env, extra_args):
    def kill_clients(conns):
        for conn in conns:
            conn.execute_command("CLIENT", "KILL", "TYPE", "normal")

    # The first reconnect is due 15 s after the kill, i.e. after the grace
    # period (TEST_TIME + 10 s) has passed. The run then ends on its own.
    out = run_memtier(
        env,
        ["--reconnect-on-error", "--max-reconnect-attempts=3", "--reconnect-backoff-factor=15"] + extra_args,
        kill_clients,
        clients=1,
        # In cluster mode a reconnect can need a second backoff round.
        wait=60,
    )
    env.assertFalse(out["hung"])
    env.assertEqual(STALL_WARNING.findall(out["stderr"]), [])
    env.assertEqual(RETRYING_WARNING.findall(out["stderr"]), [])
    env.assertTrue(out["wall"] > TEST_TIME + MAX_DRAIN_GRACE_SECS)


def test_test_time_reconnecting_client_left_alone(env):
    """
    A client with nothing in flight is not a stalled client: here it waits
    for a reconnect scheduled past the grace period. It must be left to the
    reconnect logic, not ended as "no reply" or as still retrying.
    """
    check_reconnecting_client_left_alone(env, [])


def test_test_time_reconnecting_client_left_alone_retry_on_error(env):
    """
    Same with --retry-on-error: the requests that were in flight wait for
    the reconnect in the replay queue, not in the retry queue, so the client
    is not retrying either.
    """
    check_reconnecting_client_left_alone(env, ["--retry-on-error"])
