import tempfile
import json
import subprocess
import time
from include import *
from mb import Benchmark, RunConfig


# Upper bound for a single worker thread's per-second "% of a core" sample. A
# thread runs on at most one core, so ~100% is the ceiling; allow a small margin
# for the slight mismatch between the wall window and the CPU-clock sampling.
PER_THREAD_PCT_CEILING = 110.0


def _run_and_load(env, benchmark_specs, config):
    """Run memtier and return (config, results_dict) after basic assertions."""
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)
    memtier_ok = benchmark.run()
    if not memtier_ok:
        debugPrintMemtierOnError(config, env)

    env.assertTrue(memtier_ok == True)
    json_filename = '{0}/mb.json'.format(config.results_dir)
    env.assertTrue(os.path.isfile(json_filename))
    with open(json_filename) as results_json:
        return config, json.load(results_json)


def _read_stderr(config):
    stderr_filename = '{0}/mb.stderr'.format(config.results_dir)
    if os.path.isfile(stderr_filename):
        with open(stderr_filename) as stderr_file:
            return stderr_file.read()
    return ""


def test_cpu_stats_in_json(env):
    """Verify both the aggregate CPU block and per-second CPU Stats are present
    and self-consistent."""
    benchmark_specs = {"name": env.testName, "args": []}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=2, clients=5, requests=1000)

    config, results_dict = _run_and_load(env, benchmark_specs, config)

    env.assertTrue('ALL STATS' in results_dict)
    all_stats = results_dict['ALL STATS']

    # --- Aggregate "CPU" block (authoritative, getrusage-based) ---
    # Only emitted on platforms with per-thread accounting (Linux RUSAGE_THREAD).
    if 'CPU' in all_stats:
        cpu = all_stats['CPU']
        for key in ('cpu_user_seconds', 'cpu_sys_seconds', 'cpu_total_seconds',
                    'cpu_wall_seconds', 'cpu_cores_used', 'avg_cpu_utilization_pct',
                    'peak_cpu_utilization_pct', 'threads_counted'):
            env.assertTrue(key in cpu)
        # total == user + sys (within rounding)
        env.assertTrue(abs(cpu['cpu_total_seconds']
                           - (cpu['cpu_user_seconds'] + cpu['cpu_sys_seconds'])) < 0.01)
        env.assertTrue(cpu['threads_counted'] == 2)
        env.assertTrue(cpu['cpu_cores_used'] >= 0)
        env.assertTrue(cpu['avg_cpu_utilization_pct'] >= 0)
        # Per-thread breakdown
        env.assertTrue('Per Thread' in cpu)
        for t in range(2):
            tk = 'Thread {}'.format(t)
            env.assertTrue(tk in cpu['Per Thread'])
            pt = cpu['Per Thread'][tk]
            for key in ('user_seconds', 'sys_seconds', 'total_seconds', 'wall_seconds', 'cores_used'):
                env.assertTrue(key in pt)
            env.assertTrue(abs(pt['total_seconds'] - (pt['user_seconds'] + pt['sys_seconds'])) < 0.01)

    # --- Per-second advisory CPU Stats ---
    env.assertTrue('CPU Stats' in all_stats)
    cpu_stats = all_stats['CPU Stats']
    env.assertTrue(len(cpu_stats) > 0)
    for second_key, second_data in cpu_stats.items():
        env.assertTrue('Main Thread' in second_data)
        env.assertTrue(second_data['Main Thread'] >= 0)
        for t in range(2):
            thread_key = 'Thread {}'.format(t)
            env.assertTrue(thread_key in second_data)
            thread_cpu = second_data[thread_key]
            env.assertTrue(thread_cpu >= 0)
            env.assertTrue(thread_cpu < PER_THREAD_PCT_CEILING)


def test_cpu_stats_high_load(env):
    """Stress a single thread with many clients to drive high CPU and verify the
    de-duped live warning + end-of-run warning."""
    env.skipOnCluster()

    benchmark_specs = {"name": env.testName, "args": [
        '--pipeline=100',
        '--data-size=1',
        '--ratio=1:1',
        '--key-pattern=R:R',
        '--key-maximum=100',
        '--cpu-warn-threshold=50',
    ]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=500, requests=None, test_time=5)

    config, results_dict = _run_and_load(env, benchmark_specs, config)
    all_stats = results_dict['ALL STATS']
    env.assertTrue('CPU Stats' in all_stats)

    cpu_stats = all_stats['CPU Stats']
    env.assertTrue(len(cpu_stats) >= 2)

    max_thread_cpu = 0
    for second_key, second_data in cpu_stats.items():
        env.assertTrue('Thread 0' in second_data)
        thread_cpu = second_data['Thread 0']
        env.assertTrue(thread_cpu >= 0)
        env.assertTrue(thread_cpu < PER_THREAD_PCT_CEILING)
        if thread_cpu > max_thread_cpu:
            max_thread_cpu = thread_cpu

    env.debugPrint("Max worker thread CPU observed: {:.1f}%".format(max_thread_cpu), True)
    env.assertTrue(max_thread_cpu > 10.0)

    stderr_content = _read_stderr(config)
    # Live warning string (lowercase, new format).
    live_warnings = stderr_content.count('high CPU on thread')
    env.debugPrint("Live high-CPU warnings: {}".format(live_warnings), True)
    # De-dup: at most one live warning per worker thread (here: 1 thread).
    env.assertTrue(live_warnings <= 1)
    if max_thread_cpu > 50.0:
        env.assertTrue(live_warnings >= 1)
        # End-of-run summary warning (only when the aggregate block is available).
        if 'CPU' in all_stats:
            env.assertTrue('averaged' in stderr_content and 'of a core' in stderr_content)


def test_cpu_warn_threshold_flag(env):
    """--cpu-warn-threshold=0 must warn; =100 must stay silent."""
    env.skipOnCluster()

    base_args = ['--pipeline=50', '--data-size=1', '--ratio=1:1', '--key-maximum=100']

    # threshold=0 -> warns on any CPU usage
    specs0 = {"name": env.testName, "args": base_args + ['--cpu-warn-threshold=0']}
    addTLSArgs(specs0, env)
    cfg0 = get_default_memtier_config(threads=1, clients=50, requests=None, test_time=3)
    cfg0, _ = _run_and_load(env, specs0, cfg0)
    stderr0 = _read_stderr(cfg0)
    env.assertTrue('high CPU on thread' in stderr0)

    # threshold=100 -> effectively silent (a single thread cannot exceed 100% of a core)
    specs100 = {"name": env.testName, "args": base_args + ['--cpu-warn-threshold=100']}
    addTLSArgs(specs100, env)
    cfg100 = get_default_memtier_config(threads=1, clients=50, requests=None, test_time=3)
    cfg100, _ = _run_and_load(env, specs100, cfg100)
    stderr100 = _read_stderr(cfg100)
    env.assertTrue('high CPU on thread' not in stderr100)
    env.assertTrue('of a core' not in stderr100)


def test_cpu_aggregate_cores(env):
    """The aggregate cores_used must be plausible and roughly match the per-thread
    sum."""
    env.skipOnCluster()

    benchmark_specs = {"name": env.testName, "args": [
        '--pipeline=50', '--data-size=1', '--ratio=1:1', '--key-maximum=100',
    ]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=2, clients=50, requests=None, test_time=4)

    config, results_dict = _run_and_load(env, benchmark_specs, config)
    all_stats = results_dict['ALL STATS']

    if 'CPU' not in all_stats:
        env.debugPrint("Aggregate CPU block not present (non-Linux platform), skipping", True)
        return

    cpu = all_stats['CPU']
    # 2 worker threads + main thread: cores_used must be > 0 and well under threads+2.
    env.assertTrue(cpu['cpu_cores_used'] > 0.0)
    env.assertTrue(cpu['cpu_cores_used'] < 4.0)

    # Sum of per-thread cores_used should be <= aggregate (aggregate also includes main).
    per_thread_sum = sum(pt['cores_used'] for pt in cpu['Per Thread'].values())
    env.assertTrue(per_thread_sum <= cpu['cpu_cores_used'] + 0.05)
    # avg% normalized to worker count
    env.assertTrue(abs(cpu['avg_cpu_utilization_pct']
                       - 100.0 * cpu['cpu_cores_used'] / cpu['threads_counted']) < 0.5)


def test_cpu_stats_external_validation(env):
    """Cross-validate memtier's authoritative cpu_total_seconds against psutil."""
    try:
        import psutil
    except ImportError:
        env.debugPrint("psutil not available, skipping external CPU validation", True)
        return

    env.skipOnCluster()

    benchmark_specs = {"name": env.testName, "args": [
        '--pipeline=100', '--data-size=1', '--ratio=1:1', '--key-pattern=R:R', '--key-maximum=100',
    ]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=500, requests=None, test_time=5)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)
    process = subprocess.Popen(benchmark.args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Accumulate total process CPU-seconds from psutil over the run.
    access_denied = False
    start_wall = time.time()
    last_total = 0.0
    try:
        p = psutil.Process(process.pid)
        while process.poll() is None:
            try:
                ct = p.cpu_times()
                last_total = ct.user + ct.system
            except psutil.NoSuchProcess:
                break
            time.sleep(0.5)
    except psutil.AccessDenied:
        access_denied = True
    except psutil.NoSuchProcess:
        pass

    stdout, stderr = process.communicate()
    if stderr:
        benchmark.write_file('mb.stderr', stderr)
    env.assertTrue(process.returncode == 0)

    if access_denied:
        env.debugPrint("psutil.AccessDenied (macOS task_for_pid restriction), skipping", True)
        return

    json_filename = os.path.join(config.results_dir, 'mb.json')
    env.assertTrue(os.path.isfile(json_filename))
    with open(json_filename) as f:
        results_dict = json.load(f)

    all_stats = results_dict['ALL STATS']
    if 'CPU' not in all_stats:
        env.debugPrint("Aggregate CPU block not present, skipping external validation", True)
        return

    internal_total = all_stats['CPU']['cpu_total_seconds']
    env.debugPrint("memtier cpu_total_seconds: {:.2f}s".format(internal_total), True)
    env.debugPrint("psutil total CPU-seconds:  {:.2f}s".format(last_total), True)

    # Both should show meaningful CPU and agree within a generous tolerance
    # (psutil also counts short-lived helper threads not in memtier's worker set).
    env.assertTrue(internal_total > 0.5)
    env.assertTrue(last_total > 0.5)
    env.assertTrue(abs(internal_total - last_total) < max(2.0, 0.5 * last_total))
