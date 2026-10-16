import tempfile
import time
import signal
import subprocess
import os
import shlex
import shutil
import sys
from pathlib import Path
from include import *
from mb import Benchmark, RunConfig


def test_crash_handler_with_active_connections(env):
    """
    Test that crash handler prints CLIENT LIST OUTPUT when crashing with active connections.
    This test starts a benchmark, waits 5 seconds, sends SEGV signal, and verifies the crash report.
    """
    # Setup benchmark configuration
    benchmark_specs = {
        "name": env.testName,
        "args": ['--pipeline=10']
    }
    addTLSArgs(benchmark_specs, env)

    # Use test_time instead of requests (they are mutually exclusive)
    config = get_default_memtier_config(threads=2, clients=2, requests=None, test_time=60)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # Build the command that will be run
    print(f"Starting memtier_benchmark: {' '.join(benchmark.args)}")

    # Create log file for crash output
    log_file = os.path.join(config.results_dir, "crash_output.log")

    # Start memtier_benchmark process in background
    with open(log_file, 'w') as f:
        proc = subprocess.Popen(benchmark.args, stdout=f, stderr=subprocess.STDOUT)
    
    try:
        # Wait 5 seconds for connections to be established and benchmark to be running
        print("Waiting 5 seconds for benchmark to start... and then sending SEGV signal to PID")
        time.sleep(5)
        
        # Check if process is still running
        if proc.poll() is not None:
            with open(log_file, 'r') as f:
                log_content = f.read()
                print(f"ERROR: Process exited early. Log:\n{log_content}")
            env.assertTrue(False, message="memtier_benchmark exited before we could send SEGV signal")

        # Send SEGV signal to trigger crash handler
        print(f"Sending SEGV signal to PID {proc.pid}")
        os.kill(proc.pid, signal.SIGSEGV)

        # Wait for process to crash
        proc.wait(timeout=10)

    except subprocess.TimeoutExpired:
        print("ERROR: Process did not exit after SEGV signal")
        proc.kill()
        proc.wait()
        env.assertTrue(False, message="Process did not exit after SEGV signal")
    
    # Read the crash log
    with open(log_file, 'r') as f:
        crash_output = f.read()

    print(f"\n{'='*80}")
    print("Crash output:")
    print('='*80)
    print(crash_output)
    print('='*80)

    # Verify crash report contains expected sections
    errors = []

    if "=== MEMTIER_BENCHMARK BUG REPORT START" not in crash_output:
        errors.append("Missing bug report start marker")
    if "memtier_benchmark crashed by signal" not in crash_output:
        errors.append("Missing crash signal message")
    if "STACK TRACE" not in crash_output:
        errors.append("Missing stack trace section")
    if "INFO OUTPUT" not in crash_output:
        errors.append("Missing info output section")
    if "CLIENT LIST OUTPUT" not in crash_output:
        errors.append("Missing client list output section")
    if "=== MEMTIER_BENCHMARK BUG REPORT END" not in crash_output:
        errors.append("Missing bug report end marker")

    # Verify CLIENT LIST OUTPUT contains expected information
    # Should have 2 threads with 2 clients each = 4 total client entries
    client_lines = [line for line in crash_output.split('\n') if 'thread=' in line and 'client=' in line]
    if len(client_lines) < 4:
        errors.append(f"Expected at least 4 client connection entries, found {len(client_lines)}")

    # Verify each client line has required fields
    for line in client_lines:
        if 'thread=' not in line:
            errors.append(f"Missing thread field in: {line}")
        if 'client=' not in line:
            errors.append(f"Missing client field in: {line}")
        if 'conn=' not in line:
            errors.append(f"Missing conn field in: {line}")
        if 'addr=' not in line:
            errors.append(f"Missing addr field in: {line}")
        if 'state=' not in line:
            errors.append(f"Missing state field in: {line}")
        if 'pending=' not in line:
            errors.append(f"Missing pending field in: {line}")

    # Verify at least some connections are in connected state
    connected_lines = [line for line in client_lines if 'state=connected' in line]
    if len(connected_lines) == 0:
        errors.append("Expected at least some connections to be in 'connected' state")

    if errors:
        print("\n❌ ERRORS:")
        for error in errors:
            print(f"  - {error}")
        env.assertTrue(False, message="Crash handler test failed: " + "; ".join(errors))

    print(f"\n✅ SUCCESS!")
    print(f"✓ Crash handler test passed! Found {len(client_lines)} client connections in crash report")
    print(f"✓ {len(connected_lines)} connections were in 'connected' state")

    # Cleanup
    try:
        os.remove(log_file)
    except:
        pass

    # For RLTest compatibility - mark test as passed
    env.assertTrue(True)


def test_crash_handler_worker_thread(env):
    """
    Test that crash handler works when a worker thread crashes (not the main thread).
    This test starts a benchmark, waits 5 seconds, sends SEGV signal to a worker thread,
    and verifies the crash report shows the correct crashing thread.
    """
    # Setup benchmark configuration
    benchmark_specs = {
        "name": env.testName,
        "args": ['--pipeline=10']
    }
    addTLSArgs(benchmark_specs, env)

    # Use test_time instead of requests (they are mutually exclusive)
    config = get_default_memtier_config(threads=2, clients=2, requests=None, test_time=60)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # Build the command that will be run
    print(f"Starting memtier_benchmark: {' '.join(benchmark.args)}")

    # Create log file for crash output
    log_file = os.path.join(config.results_dir, "crash_output.log")

    # Start memtier_benchmark process in background
    with open(log_file, 'w') as f:
        proc = subprocess.Popen(benchmark.args, stdout=f, stderr=subprocess.STDOUT)

    try:
        # Wait 5 seconds for connections to be established and benchmark to be running
        print("Waiting 5 seconds for benchmark to start...")
        time.sleep(5)

        # Check if process is still running
        if proc.poll() is not None:
            with open(log_file, 'r') as f:
                log_content = f.read()
                print(f"ERROR: Process exited early. Log:\n{log_content}")
            env.assertTrue(False, message="memtier_benchmark exited before we could send SEGV signal")

        # Find worker threads by enumerating /proc/<pid>/task/
        # Retry a few times in case threads are still being created
        task_dir = f"/proc/{proc.pid}/task"
        thread_ids = []
        max_retries = 10
        for retry in range(max_retries):
            thread_ids = []
            try:
                for tid in os.listdir(task_dir):
                    tid_int = int(tid)
                    # Skip the main thread (same as process ID)
                    if tid_int != proc.pid:
                        thread_ids.append(tid_int)
            except (OSError, ValueError) as e:
                print(f"ERROR: Could not enumerate threads: {e}")
                proc.kill()
                proc.wait()
                env.assertTrue(False, message=f"Could not enumerate threads: {e}")

            if len(thread_ids) > 0:
                print(f"Found {len(thread_ids)} worker threads: {thread_ids}")
                break

            # Wait a bit and retry
            print(f"No worker threads found yet, retry {retry+1}/{max_retries}...")
            time.sleep(0.5)

        if len(thread_ids) == 0:
            print("ERROR: No worker threads found after retries")
            proc.kill()
            proc.wait()
            env.assertTrue(False, message="No worker threads found after retries")

        # Send SEGV signal to the first worker thread
        target_tid = thread_ids[0]
        print(f"Sending SEGV signal to worker thread TID {target_tid} (main PID: {proc.pid})")
        os.kill(target_tid, signal.SIGSEGV)

        # Wait for process to crash
        proc.wait(timeout=10)

    except subprocess.TimeoutExpired:
        print("ERROR: Process did not exit after SEGV signal")
        proc.kill()
        proc.wait()
        env.assertTrue(False, message="Process did not exit after SEGV signal")

    # Read the crash log
    with open(log_file, 'r') as f:
        crash_output = f.read()

    print(f"\n{'='*80}")
    print("Crash output:")
    print('='*80)
    print(crash_output)
    print('='*80)

    # Verify crash report contains expected sections
    errors = []

    if "=== MEMTIER_BENCHMARK BUG REPORT START" not in crash_output:
        errors.append("Missing bug report start marker")
    if "memtier_benchmark crashed by signal" not in crash_output:
        errors.append("Missing crash signal message")
    if "STACK TRACE" not in crash_output:
        errors.append("Missing stack trace section")
    if "current/crashing thread" not in crash_output:
        errors.append("Missing current/crashing thread marker in stack trace")
    if "worker thread" not in crash_output:
        errors.append("Missing worker thread information in stack trace")
    if "INFO OUTPUT" not in crash_output:
        errors.append("Missing info output section")
    if "CLIENT LIST OUTPUT" not in crash_output:
        errors.append("Missing client list output section")
    if "=== MEMTIER_BENCHMARK BUG REPORT END" not in crash_output:
        errors.append("Missing bug report end marker")

    # Verify CLIENT LIST OUTPUT contains expected information
    client_lines = [line for line in crash_output.split('\n') if 'thread=' in line and 'client=' in line]
    if len(client_lines) < 4:
        errors.append(f"Expected at least 4 client connection entries, found {len(client_lines)}")

    if errors:
        print("\n❌ ERRORS:")
        for error in errors:
            print(f"  - {error}")
        env.assertTrue(False, message="Worker thread crash handler test failed: " + "; ".join(errors))

    print(f"\n✅ SUCCESS!")
    print(f"✓ Worker thread crash handler test passed!")
    print(f"✓ Crash report correctly identified crashing worker thread")
    print(f"✓ Found {len(client_lines)} client connections in crash report")

    # Cleanup
    try:
        os.remove(log_file)
    except:
        pass

    # For RLTest compatibility - mark test as passed
    env.assertTrue(True)



def test_crash_handler_time_zone_lock_held(env):
    """
    Issue #681: the crash handler called localtime(), which takes glibc's time
    zone lock and on first use mallocs and opens /etc/localtime. A crash that
    interrupted a thread holding one of those locks deadlocked the handler
    before it printed anything, and alarm(5) then killed the process with no
    report. An LD_PRELOAD shim makes the time zone functions block when they
    are called from a signal handler, which reproduces that deadlock on every
    run. The report must still be complete.
    """
    compiler = shlex.split(os.environ.get("CC", "cc"))
    if (sys.platform != "linux" or not compiler or not shutil.which(compiler[0])
            or not shutil.which("ldd")):
        env.skip()
        return
    benchmark_specs = {"name": env.testName, "args": ['--pipeline=10']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=2, clients=2, requests=None, test_time=60)
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
    with tempfile.TemporaryDirectory() as directory:
        config = RunConfig(directory, env.testName, config, {})
        ensure_clean_benchmark_folder(config.results_dir)
        benchmark = Benchmark.from_json(config, benchmark_specs)
        linked = subprocess.run(["ldd", benchmark.args[0]], capture_output=True, text=True, timeout=10)
        if linked.returncode != 0:
            env.skip()  # Static executables cannot use this interposition test.
            return
        library = Path(directory) / "tz_lock.so"
        subprocess.run(compiler + ["-shared", "-fPIC",
                                   str(Path(__file__).with_name("crash_handler_tz_lock_injector.c")),
                                   "-o", str(library), "-ldl"],
                       check=True, capture_output=True, timeout=30)
        # Keep the sanitizer runtime first; its interceptors forward to the shim.
        runtimes = [line.split()[2] for line in linked.stdout.splitlines()
                    if line.strip().startswith(("libasan.so", "libtsan.so", "libubsan.so"))
                    and "=>" in line and len(line.split()) >= 3]
        preloads = runtimes + [str(library)]
        if os.environ.get("LD_PRELOAD"):
            preloads.append(os.environ["LD_PRELOAD"])
        child_env = dict(os.environ, LD_PRELOAD=":".join(preloads))

        log_file = os.path.join(config.results_dir, "crash_output.log")
        with open(log_file, 'w') as f:
            proc = subprocess.Popen(benchmark.args, stdout=f, stderr=subprocess.STDOUT, env=child_env)
        try:
            # Wait until the workers exist and have started sending traffic.
            task_dir = f"/proc/{proc.pid}/task"
            worker_tids = []
            for _ in range(50):
                if proc.poll() is not None:
                    break
                worker_tids = [int(t) for t in os.listdir(task_dir) if int(t) != proc.pid]
                if worker_tids:
                    break
                time.sleep(0.1)
            time.sleep(2)
            env.assertIsNone(proc.poll(), message="memtier_benchmark exited before the SEGV")
            env.assertTrue(len(worker_tids) > 0, message="no worker threads found")
            print(f"Sending SEGV to worker thread TID {worker_tids[0]} (main PID: {proc.pid})")
            os.kill(worker_tids[0], signal.SIGSEGV)
            # A deadlocked handler is killed by its own alarm(5) watchdog.
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            env.assertTrue(False, message="Process did not exit after SEGV signal")
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()

        with open(log_file, 'r') as f:
            crash_output = f.read()
        print(crash_output[crash_output.find("=== MEMTIER_BENCHMARK BUG REPORT"):])

        # The process must end by the crash signal, not by the SIGALRM watchdog.
        env.assertEqual(proc.returncode, -signal.SIGSEGV,
                        message=f"exit status {proc.returncode}; -{int(signal.SIGALRM)} means the watchdog killed a stuck handler")
        markers = ("=== MEMTIER_BENCHMARK BUG REPORT START",
                   "memtier_benchmark crashed by signal: 11",
                   "Crashed running signal <SIGSEGV>",
                   "current/crashing thread",
                   "INFO OUTPUT",
                   "CLIENT LIST OUTPUT",
                   "=== MEMTIER_BENCHMARK BUG REPORT END")
        missing = [m for m in markers if m not in crash_output]
        env.assertEqual(missing, [], message="crash report incomplete")
        env.assertNotContains("BUG REPORT INCOMPLETE", crash_output)
