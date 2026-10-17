import tempfile
import time
import signal
import subprocess
import os
import ctypes
import datetime
import fcntl
import platform
import re
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
        thread_ids = []
        max_retries = 10
        for retry in range(max_retries):
            thread_ids = _worker_tids(proc.pid)

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
        _signal_thread(proc.pid, target_tid, signal.SIGSEGV)

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



def _crash_shim_env(env, directory, binary):
    """
    Build tests/crash_handler_lock_injector.c and return a child environment
    that preloads it, or skip the test when that cannot be done here.
    """
    compiler = shlex.split(os.environ.get("CC", "cc"))
    if (sys.platform != "linux" or not compiler or not shutil.which(compiler[0])
            or not shutil.which("ldd")):
        print("SKIP: needs Linux, a C compiler and ldd")
        env.skip()
    try:
        linked = subprocess.run(["ldd", binary], capture_output=True, text=True, timeout=60)
    except subprocess.TimeoutExpired:
        print("SKIP: ldd timed out")
        env.skip()
    if linked.returncode != 0:
        print(f"SKIP: ldd failed (static executable?): {linked.stderr.strip()}")
        env.skip()
    library = Path(directory) / "crash_lock.so"
    try:
        subprocess.run(compiler + ["-shared", "-fPIC",
                                   str(Path(__file__).with_name("crash_handler_lock_injector.c")),
                                   "-o", str(library), "-ldl", "-lpthread"],
                       check=True, capture_output=True, text=True, timeout=120)
    except subprocess.TimeoutExpired:
        print("SKIP: compiling the preload shim timed out")
        env.skip()
    except subprocess.CalledProcessError as e:
        print(f"SKIP: compiling the preload shim failed: {e.stderr.strip()}")
        env.skip()
    # Keep the sanitizer runtime first; its interceptors forward to the shim.
    runtimes = [line.split()[2] for line in linked.stdout.splitlines()
                if line.strip().startswith(("libasan.so", "libtsan.so", "libubsan.so"))
                and "=>" in line and len(line.split()) >= 3]
    preloads = runtimes + [str(library)]
    if os.environ.get("LD_PRELOAD"):
        preloads.append(os.environ["LD_PRELOAD"])
    child_env = dict(os.environ, LD_PRELOAD=":".join(preloads))
    for name in ("MEMTIER_TEST_BLOCK_BACKTRACE", "MEMTIER_TEST_LOCK_STDERR"):
        child_env.pop(name, None)
    # The shim records here what it saw; see _shim_marker().
    child_env["MEMTIER_TEST_SHIM_MARKER"] = str(Path(directory) / "shim_marker.txt")
    return child_env


def _shim_marker(child_env):
    """Lines the preload shim wrote to its marker file."""
    try:
        with open(child_env["MEMTIER_TEST_SHIM_MARKER"]) as f:
            return f.read().split("\n")
    except OSError:
        return []


_TSAN_BUILD = {}


def _tsan_build():
    """
    True for a ThreadSanitizer build. Those keep SIG_DFL for the watchdog
    (TSan would defer its handler forever), so they print no INCOMPLETE line.
    Look for the runtime's __tsan_init symbol name in the binary: it is there
    whether the runtime is a shared library (GCC) or linked in (clang).
    """
    binary = shutil.which(MEMTIER_BINARY) or MEMTIER_BINARY
    if binary not in _TSAN_BUILD:
        try:
            with open(binary, "rb") as f:
                _TSAN_BUILD[binary] = b"__tsan_init" in f.read()
        except OSError:
            _TSAN_BUILD[binary] = False
    return _TSAN_BUILD[binary]


def _worker_tids(pid):
    """
    Threads of pid other than the main thread that can take SIGSEGV. This
    leaves out helper threads that block every signal, such as the TSan
    runtime's background thread and the preload shim's crash-shim thread;
    a thread-directed SIGSEGV to one of those would stay pending forever.
    """
    tids = []
    try:
        names = os.listdir(f"/proc/{pid}/task")
    except OSError:
        return tids
    for name in names:
        tid = int(name)
        if tid == pid:
            continue
        try:
            with open(f"/proc/{pid}/task/{name}/status") as status:
                blocked = next((int(line.split()[1], 16) for line in status
                                if line.startswith("SigBlk:")), 0)
        except OSError:
            continue
        if not blocked & (1 << (signal.SIGSEGV - 1)):
            tids.append(tid)
    return sorted(tids)


_SYS_TGKILL = {"x86_64": 234, "aarch64": 131, "i686": 270, "armv7l": 268}


def _signal_thread(pid, tid, sig):
    """
    Send sig to thread tid of process pid. os.kill(tid) is kill(2): it sends a
    process-directed signal that any thread may take, so use tgkill(2) when
    libc has it or this architecture's syscall number is known.

    TSan builds get kill(2) to the process instead. TSan runs the handler for
    a signal sent to a busy thread later, from an intercepted call and not on
    the alternate signal stack, and in 1 of about 20 local runs that handler
    printed nothing before the watchdog fired. kill(2) is what these tests
    always used; the kernel then picks a thread, normally the main thread
    sleeping between progress lines, where TSan delivers it at once.
    """
    if _tsan_build():
        os.kill(pid, sig)
        return
    libc = ctypes.CDLL(None, use_errno=True)
    if hasattr(libc, "tgkill"):  # glibc 2.30 and later
        result = libc.tgkill(pid, tid, sig)
    elif platform.machine() in _SYS_TGKILL:
        result = libc.syscall(_SYS_TGKILL[platform.machine()], pid, tid, sig)
    else:
        result = None
    if result == 0:
        return
    if result is not None:
        print(f"tgkill failed (errno {ctypes.get_errno()}), falling back to kill")
    os.kill(tid, sig)


def _crash_worker(env, child_env, stderr=None, wait_timeout=15, before_crash=None):
    """
    Run memtier with child_env, send SIGSEGV to a worker thread once it is
    running, and return (exit status, output, seconds from SEGV to exit).
    Output is stdout, plus stderr unless a stderr file object is given.
    """
    benchmark_specs = {"name": env.testName, "args": ['--pipeline=10']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=2, clients=2, requests=None, test_time=60)
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
    directory = tempfile.mkdtemp()
    config = RunConfig(directory, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    log_file = os.path.join(config.results_dir, "crash_output.log")
    with open(log_file, 'w') as f:
        proc = subprocess.Popen(benchmark.args, stdout=f,
                                stderr=stderr if stderr is not None else subprocess.STDOUT,
                                env=child_env)
    elapsed = None
    sent_at = None
    try:
        # Wait until the workers exist and have started sending traffic.
        workers = lambda: _worker_tids(proc.pid)
        for _ in range(50):
            if proc.poll() is not None or workers():
                break
            time.sleep(0.1)
        time.sleep(2)
        worker_tids = workers() if proc.poll() is None else []
        env.assertIsNone(proc.poll(), message="memtier_benchmark exited before the SEGV")
        env.assertTrue(len(worker_tids) > 0, message="no worker threads found")
        if before_crash is not None:
            before_crash()
        print(f"Sending SEGV to worker thread TID {worker_tids[0]} (main PID: {proc.pid})")
        start = time.monotonic()
        sent_at = datetime.datetime.now(datetime.timezone.utc)
        _signal_thread(proc.pid, worker_tids[0], signal.SIGSEGV)
        proc.wait(timeout=wait_timeout)
        elapsed = time.monotonic() - start
        print(f"Exit status {proc.returncode}, {elapsed:.1f} s after the SEGV")
    except subprocess.TimeoutExpired:
        env.assertTrue(False, message=f"Process did not exit within {wait_timeout} s of the SEGV")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    with open(log_file, 'r') as f:
        output = f.read()
    shutil.rmtree(directory, ignore_errors=True)
    print(output[output.find("=== MEMTIER_BENCHMARK BUG REPORT"):])
    return proc.returncode, output, elapsed, sent_at


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
    with tempfile.TemporaryDirectory() as directory:
        child_env = _crash_shim_env(env, directory, shutil.which(MEMTIER_BINARY) or MEMTIER_BINARY)
        # A fixed zone with a half-hour offset, as a POSIX TZ string so no
        # zone data is needed: the header must show this local time.
        child_env["TZ"] = "<+0530>-5:30"
        returncode, output, _, sent_at = _crash_worker(env, child_env)
        marker = _shim_marker(child_env)

    # Without this the test would pass trivially if the shim was not loaded
    # or the handler did not run on a signal stack, where the shim blocks.
    env.assertContains("signal-stack", marker, message="the shim never saw the crash handler")

    # The process must end by the crash signal, not by the SIGALRM watchdog.
    env.assertEqual(returncode, -signal.SIGSEGV,
                    message=f"exit status {returncode}; -{int(signal.SIGALRM)} means the watchdog killed a stuck handler")
    markers = ("=== MEMTIER_BENCHMARK BUG REPORT START",
               "memtier_benchmark crashed by signal: 11",
               "Crashed running signal <SIGSEGV>",
               "current/crashing thread",
               "INFO OUTPUT",
               "CLIENT LIST OUTPUT",
               "=== MEMTIER_BENCHMARK BUG REPORT END")
    missing = [m for m in markers if m not in output]
    env.assertEqual(missing, [], message="crash report incomplete")
    env.assertNotContains("BUG REPORT INCOMPLETE", output)

    stamp = re.search(r"^\[\d+\] (\d\d \w{3} \d{4} \d\d:\d\d:\d\d) # memtier_benchmark crashed by signal", output, re.M)
    env.assertIsNotNone(stamp, message="no timestamp on the crash header")
    if stamp is not None and sent_at is not None:
        expected = (sent_at + datetime.timedelta(hours=5, minutes=30)).replace(tzinfo=None)
        # Parse by hand: strptime's %b follows the locale, the report is English.
        day, month, rest = stamp.group(1).split(" ", 2)
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        shown = datetime.datetime.strptime(f"{day} {months.index(month) + 1} {rest}", "%d %m %Y %H:%M:%S")
        env.assertLess(abs((shown - expected).total_seconds()), 5,
                       message=f"header time {shown}, expected about {expected} (UTC+05:30)")


def test_crash_handler_watchdog_marks_incomplete_report(env):
    """
    When the best-effort part of the report hangs (here backtrace() blocks on
    the signal stack), the SIGALRM watchdog must say the report is incomplete
    and end the process about 5 s after the crash. The lines written before
    the hang must be there. TSan builds end the same way without the line.
    """
    with tempfile.TemporaryDirectory() as directory:
        child_env = _crash_shim_env(env, directory, shutil.which(MEMTIER_BINARY) or MEMTIER_BINARY)
        child_env["MEMTIER_TEST_BLOCK_BACKTRACE"] = "1"
        returncode, output, elapsed, _ = _crash_worker(env, child_env)
        marker = _shim_marker(child_env)

    env.assertContains("blocked backtrace", marker, message="the shim did not block backtrace()")

    env.assertEqual(returncode, -signal.SIGALRM)
    env.assertIsNotNone(elapsed)
    if elapsed is not None:
        env.assertLess(elapsed, 8, message=f"watchdog took {elapsed:.1f} s")
    markers = ["=== MEMTIER_BENCHMARK BUG REPORT START", "memtier_benchmark crashed by signal: 11"]
    if not _tsan_build():
        markers.append("=== MEMTIER_BENCHMARK BUG REPORT INCOMPLETE")
    for marker in markers:
        env.assertContains(marker, output)
    env.assertNotContains("=== MEMTIER_BENCHMARK BUG REPORT END", output)


def test_crash_handler_header_survives_stderr_lock(env):
    """
    When another thread holds stderr's stdio lock, every fprintf(stderr) in
    the crash handler blocks. The header lines are written with write(2) and
    must still appear, and the watchdog must add the INCOMPLETE line and end
    the process. The shim takes the lock from a helper thread when the crash
    handler arms its alarm.

    Skipped on TSan builds. There the crash signal reaches memtier's handler
    only through TSan's deferred delivery, which waits for the thread to pass
    an intercepted call, and the scenario parks a helper thread in a stdio
    lock while the handler spins waiting for it. That is not deterministic:
    in CI (job 117257659823) nothing after the SEGV was printed and the
    process did not exit, while local runs passed. On TSan the
    write(2) header is still checked by test_crash_handler_time_zone_lock_held
    and the watchdog exit by the other two watchdog tests.
    """
    if _tsan_build():
        print("SKIP: the stderr lock scenario is not deterministic under TSan's deferred signal delivery")
        env.skip()
    with tempfile.TemporaryDirectory() as directory:
        child_env = _crash_shim_env(env, directory, shutil.which(MEMTIER_BINARY) or MEMTIER_BINARY)
        child_env["MEMTIER_TEST_LOCK_STDERR"] = "1"
        returncode, output, elapsed, _ = _crash_worker(env, child_env)
        marker = _shim_marker(child_env)

    env.assertContains("signal-stack", marker, message="the shim never saw the crash handler")
    env.assertContains("stderr-locked", marker, message="the helper thread did not take the stderr lock")

    env.assertEqual(returncode, -signal.SIGALRM)
    env.assertIsNotNone(elapsed)
    if elapsed is not None:
        env.assertLess(elapsed, 10, message=f"watchdog took {elapsed:.1f} s")
    markers = ["=== MEMTIER_BENCHMARK BUG REPORT START",
               "memtier_benchmark crashed by signal: 11",
               "Crashed running signal <SIGSEGV>",
               "Signal code:",
               "Fault address:",
               "=== MEMTIER_BENCHMARK BUG REPORT INCOMPLETE"]
    missing = [m for m in markers if m not in output]
    env.assertEqual(missing, [], message="lines written with write(2) are missing")
    # The stack trace header is the first fprintf; it must not get through.
    env.assertNotContains("--- STACK TRACE", output)


def test_crash_handler_watchdog_blocked_stderr(env):
    """
    When stderr is a pipe that nobody drains, every write in the crash handler
    blocks, including the watchdog's own INCOMPLETE line. The watchdog must
    still end the process: it re-arms alarm(1) before writing, and the second
    SIGALRM takes the default action.
    """
    if sys.platform != "linux" or not hasattr(fcntl, "F_SETPIPE_SZ"):
        env.skip()
    read_fd, write_fd = os.pipe()
    fill_fd = None
    try:
        try:
            fcntl.fcntl(write_fd, fcntl.F_SETPIPE_SZ, 4096)
        except OSError:
            pass  # A larger pipe only means more bytes to fill below.
        # A second open of the pipe gets its own file description, so making
        # it non-blocking does not affect memtier's blocking stderr.
        fill_fd = os.open(f"/proc/self/fd/{write_fd}", os.O_WRONLY | os.O_NONBLOCK)

        def fill_pipe():
            try:
                while True:
                    os.write(fill_fd, b"x" * 512)
            except BlockingIOError:
                pass

        with os.fdopen(os.dup(write_fd), "wb") as stderr:
            os.close(write_fd)
            write_fd = None
            returncode, output, elapsed, _ = _crash_worker(env, None, stderr=stderr, before_crash=fill_pipe)
    finally:
        for fd in (read_fd, write_fd, fill_fd):
            if fd is not None:
                os.close(fd)

    env.assertEqual(returncode, -signal.SIGALRM,
                    message=f"exit status {returncode}; the watchdog did not end a handler blocked on stderr")
    env.assertIsNotNone(elapsed)
    if elapsed is not None:
        env.assertLess(elapsed, 9, message=f"watchdog took {elapsed:.1f} s")
