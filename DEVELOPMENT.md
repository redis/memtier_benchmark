# Development Guide

This document provides information for developers working on memtier_benchmark.

## Building from Source

### Prerequisites

The following libraries are required for building:

* libevent 2.0.10 or newer.
* OpenSSL (unless TLS support is disabled by `./configure --disable-tls`).

The following tools are required:
* autoconf
* automake
* pkg-config
* GNU make
* GCC C++ compiler
* clang-format (for code formatting)

### CentOS/Red Hat Linux 7 or newer

Use the following to install prerequisites:
```
$ sudo yum install autoconf automake make gcc-c++ \
    zlib-devel libmemcached-devel libevent-devel openssl-devel \
    clang-tools-extra
```

### Ubuntu/Debian

Use the following to install prerequisites:

```
$ sudo apt-get install build-essential autoconf automake \
    libevent-dev pkg-config zlib1g-dev libssl-dev clang-format
```

### macOS

To build natively on macOS, use Homebrew to install the required dependencies:

```
$ brew install autoconf automake libtool libevent pkg-config openssl@3.0 clang-format
```

When running `./configure`, if it fails to find libssl it may be necessary to
tweak the `PKG_CONFIG_PATH` environment variable:

```
PKG_CONFIG_PATH=`brew --prefix openssl@3.0`/lib/pkgconfig ./configure
```

### Building and Installing

After downloading the source tree, use standard autoconf/automake commands:

```
$ autoreconf -ivf
$ ./configure
$ make
$ sudo make install
```

**Note**: Debug symbols (`-g`) are included by default in all builds for crash analysis and debugging. The default build flags are `-O2 -g -Wall`, which provides:
- Full optimizations (`-O2`) for production performance
- Debug symbols (`-g`) for meaningful stack traces and core dump analysis
- All warnings enabled (`-Wall`)

For development/debugging builds without optimizations:

```
$ ./configure CXXFLAGS="-g -O0 -Wall"
```

This disables optimizations (`-O0`), making it easier to step through code in a debugger, but should not be used for performance testing or production.

### Code Style

This project uses `clang-format` for consistent C++ code formatting. The style configuration is defined in `.clang-format`.

To format all C++ files:
```
$ make format
```

To check formatting without modifying files:
```
$ make format-check
```

CI will automatically check that all C++ files are properly formatted on every push and pull request.

#### Pre-commit hook

The repository ships a `pre-commit` hook under `.githooks/` that runs `make
format-check-staged` before each commit. That target inspects the *staged
blobs* in the git index (not the working tree), so style violations are
caught locally instead of in CI and the check is immune to working-tree
vs. index drift (e.g. running `make format` without `git add -u`). Enable
it once per clone:

```
$ make install-hooks
```

This sets `core.hooksPath` to `.githooks` for this repository. To bypass the
hook for a single commit (avoid making a habit of it):

```
$ git commit --no-verify
```

## Testing

The project includes a basic set of integration tests.

### Integration Tests

Integration tests are based on [RLTest](https://github.com/RedisLabsModules/RLTest), and specific setup parameters can be provided
to configure tests and topologies (OSS standalone and OSS cluster). By default the tests will be ran for all common commands, and with OSS standalone setup.

To run all integration tests in a Python virtualenv, follow these steps:

    $ mkdir -p .env
    $ virtualenv .env
    $ source .env/bin/activate
    $ pip install -r tests/test_requirements.txt
    $ ./tests/run_tests.sh

To understand what test options are available simply run:

    $ ./tests/run_tests.sh --help

### Memory Leak Detection with Sanitizers

memtier_benchmark supports building with AddressSanitizer (ASAN) and LeakSanitizer (LSAN) to detect memory errors and leaks during testing.

To build with sanitizers enabled:

    $ ./configure --enable-sanitizers
    $ make

To run tests with leak detection:

    $ ASAN_OPTIONS=detect_leaks=1 ./tests/run_tests.sh

If memory leaks or errors are detected, tests will fail with detailed error messages showing the location of the issue.

To verify ASAN is enabled:

    $ ldd ./memtier_benchmark | grep asan

### Undefined Behavior Detection with UBSan

memtier_benchmark supports building with UndefinedBehaviorSanitizer (UBSan) to detect undefined behavior such as integer overflows, null pointer dereferences, and alignment issues.

To build with UBSan enabled:

    $ ./configure --enable-ubsan
    $ make

To run tests with undefined behavior detection:

    $ UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1 ./tests/run_tests.sh

UBSan can be combined with ASAN for comprehensive testing:

    $ ./configure --enable-sanitizers --enable-ubsan
    $ make

**Note:** UBSan can be used together with ASAN/LSAN, but not with ThreadSanitizer (TSAN).

### Data Race Detection with Thread Sanitizer

memtier_benchmark supports building with ThreadSanitizer (TSAN) to detect data races and threading issues.

To build with Thread Sanitizer enabled:

    $ ./configure --enable-thread-sanitizer
    $ make

To run tests with race detection (requires disabling ASLR on kernel 6.6+):

    $ TSAN_OPTIONS="suppressions=$(pwd)/tsan_suppressions.txt" setarch `uname -m` -R ./tests/run_tests.sh

To verify TSAN is enabled:

    $ ldd ./memtier_benchmark | grep tsan

**Note:** TSAN and ASAN are mutually exclusive and cannot be used together. A suppression file (`tsan_suppressions.txt`) is provided to ignore known benign data races that do not affect correctness.

### On-demand fuzzers and extra suites (PR labels)

A few longer-running test suites do not run on every PR by default — their wall-clock cost outweighs the per-PR signal — but you can opt a PR into running them by attaching a label:

| Label | Workflow | What it runs | When to use |
|---|---|---|---|
| `run-fuzz` | `.github/workflows/monitor-fuzz.yml` | Black-box fuzz driver (`tests/fuzz/fuzz_monitor_input.py`) against `--monitor-input` / `arbitrary_command::split_command_to_args`, ASAN+UBSan build, `FUZZ_ITER=300` (~3-4 min). | Any PR that touches the monitor-input parser, the arbitrary-command splitter, or the RESP-encoded request path. |
| `run-differential` | `.github/workflows/differential.yml` | Differential harness (`tests/differential_redis_benchmark.py`) — memtier vs `redis-benchmark` / `redis-cli` against the same redis, asserts total ops / hits / p99 latency / throughput / server-killed diagnostics agree within explicit tolerances. | Any PR that plausibly affects the protocol formatter, the RESP parser, MGET pipelining, latency accounting, or hit/miss bookkeeping. |
| `run-soak` | `.github/workflows/soak-nightly.yml` | Seven-scenario soak / stress / chaos matrix (`tests/soak/`): 30 min memory soak (RSS slope), giant MONITOR-input payloads, 32t/256c high concurrency + FD hygiene, `CLIENT KILL` churn, cluster failover + reshard mid-run, `-ERR`-only retry storm, `tc netem` slow network. ~30-35 min total under a 60-min matrix cap. | Any PR that plausibly affects memory growth, retry / reconnect, cluster handling, or large-payload paths. |

Attach a label via the CLI:

    $ gh pr edit <num> --add-label run-fuzz
    $ gh pr edit <num> --add-label run-differential
    $ gh pr edit <num> --add-label run-soak

Removing the label and pushing a new commit skips the workflow again. All three also run on `workflow_dispatch`; `run-fuzz` runs nightly with `FUZZ_ITER=2000` and `run-soak` runs nightly via cron. Failures upload reproducers / logs / `tests/logs/` as workflow artifacts.

Local runs:

    $ MEMTIER=$(pwd)/memtier_benchmark FUZZ_ITER=200 \
          python3 tests/fuzz/fuzz_monitor_input.py

    $ RUN_DIFFERENTIAL=1 MEMTIER_BINARY=$(pwd)/memtier_benchmark \
          pytest tests/differential_redis_benchmark.py -v

    $ MEMTIER_BINARY=$(pwd)/memtier_benchmark \
          python3 -m RLTest --test tests/soak/test_large_payloads.py -v

## Crash Handling and Debugging

memtier_benchmark includes built-in crash handling that automatically prints a detailed bug report when the program crashes due to signals like SIGSEGV, SIGBUS, SIGFPE, SIGILL, or SIGABRT.

### Crash Report Features

When a crash occurs, memtier_benchmark will automatically:

1. **Print a detailed bug report** including:
   - Timestamp and process ID
   - Signal type and code
   - Fault address
   - Stack trace for all threads:
     - Detailed stack trace for the crashing thread showing the call chain
     - Thread IDs for all worker threads (stack traces for non-crashing threads not available on most platforms)
   - System information:
     - `os`: Operating system name, kernel version, and architecture
     - `memtier_version`: Version number from configure.ac
     - `memtier_git_sha1`: Git commit SHA (8 characters) at build time
     - `memtier_git_dirty`: Whether working directory had uncommitted changes (0=clean, 1=dirty)
     - `arch_bits`: CPU architecture (32 or 64 bit)
     - `gcc_version`: GCC compiler version used to build
     - `libevent_version`: libevent library version
     - `openssl_version`: OpenSSL library version (if TLS enabled)
   - Client connection information (if benchmark is running):
     - For each thread, client, and connection:
       - Connection address and port (remote endpoint)
       - Local port (client-side port number)
       - Connection state (disconnected, connecting, connected)
       - Number of pending requests
       - Last command type sent on this connection (GET, SET, etc.)
   - Instructions for enabling core dumps

2. **Attempt to enable core dumps** by setting `RLIMIT_CORE` to unlimited at startup
   - This allows the OS to generate a core dump file for post-mortem debugging
   - Note: Core dump generation also depends on system configuration (e.g., `/proc/sys/kernel/core_pattern`)

3. **Re-raise the signal** after printing the bug report to allow the OS to generate a core dump

### Example Crash Report

```
=== MEMTIER_BENCHMARK BUG REPORT START: Cut & paste starting from here ===
[12345] 16 Dec 2025 11:46:18 # memtier_benchmark crashed by signal: 11
[12345] 16 Dec 2025 11:46:18 # Crashed running signal <SIGSEGV>
[12345] 16 Dec 2025 11:46:18 # Signal code: 1
[12345] 16 Dec 2025 11:46:18 # Fault address: (nil)

[12345] 16 Dec 2025 11:46:18 # --- STACK TRACE (all threads)
[12345] 16 Dec 2025 11:46:18 # Thread 130098597599040 (current/crashing thread):
[12345] 16 Dec 2025 11:46:18 #   /lib/x86_64-linux-gnu/libc.so.6(+0x45330) [0x...]
[12345] 16 Dec 2025 11:46:18 #   /lib/x86_64-linux-gnu/libc.so.6(clock_nanosleep+0xbf) [0x...]
[12345] 16 Dec 2025 11:46:18 #   ./memtier_benchmark(+0x18c6) [0x...]
...
[12345] 16 Dec 2025 11:46:18 # Thread 130098583828160 (worker thread 0):
[12345] 16 Dec 2025 11:46:18 #   (Note: Stack trace for non-crashing threads not available on this platform)
[12345] 16 Dec 2025 11:46:18 # Thread 130098575435456 (worker thread 1):
[12345] 16 Dec 2025 11:46:18 #   (Note: Stack trace for non-crashing threads not available on this platform)

[12345] 16 Dec 2025 11:46:18 # --- INFO OUTPUT
[12345] 16 Dec 2025 11:46:18 # os:Linux 6.14.0-37-generic x86_64
[12345] 16 Dec 2025 11:46:18 # memtier_version:2.2.1
[12345] 16 Dec 2025 11:46:18 # memtier_git_sha1:8985eb5a
[12345] 16 Dec 2025 11:46:18 # memtier_git_dirty:1
[12345] 16 Dec 2025 11:46:18 # arch_bits:64
[12345] 16 Dec 2025 11:46:18 # gcc_version:13.3.0
[12345] 16 Dec 2025 11:46:18 # libevent_version:2.1.12-stable
[12345] 16 Dec 2025 11:46:18 # openssl_version:OpenSSL 3.0.13 30 Jan 2024

[12345] 16 Dec 2025 11:46:18 # --- CLIENT LIST OUTPUT
[12345] 16 Dec 2025 11:46:18 # thread=0 client=0 conn=0 addr=127.0.0.1:6379 local_port=37934 state=connected pending=10 last_cmd=GET
[12345] 16 Dec 2025 11:46:18 # thread=0 client=1 conn=0 addr=127.0.0.1:6379 local_port=37948 state=connected pending=7 last_cmd=GET
[12345] 16 Dec 2025 11:46:18 # thread=1 client=0 conn=0 addr=127.0.0.1:6379 local_port=37964 state=connected pending=10 last_cmd=GET
[12345] 16 Dec 2025 11:46:18 # thread=1 client=1 conn=0 addr=127.0.0.1:6379 local_port=37978 state=connected pending=0 last_cmd=none
[12345] 16 Dec 2025 11:46:18 # For more information, please check the core dump if available.
[12345] 16 Dec 2025 11:46:18 # To enable core dumps: ulimit -c unlimited
[12345] 16 Dec 2025 11:46:18 # Core pattern: /proc/sys/kernel/core_pattern

=== MEMTIER_BENCHMARK BUG REPORT END. Make sure to include from START to END. ===

       Please report this bug by opening an issue on github.com/redis/memtier_benchmark
```

### Testing the Crash Handler

You can test the crash handler functionality by sending a signal to a running memtier_benchmark process:

```bash
# Start memtier_benchmark in one terminal
$ ./memtier_benchmark --server localhost --port 6379 --test-time 60

# In another terminal, send SEGV signal to trigger crash handler
$ kill -SEGV `pgrep memtier`
```

This will trigger the crash handler and display the bug report. This is useful for:
- Verifying crash handling works on your system
- Testing in CI/CD pipelines
- Ensuring core dumps are properly configured

Test scripts are also available:

```bash
# Integration tests with active connections - using RLTest framework
$ TEST=test_crash_handler_integration.py ./tests/run_tests.sh
```

The integration tests include:

1. **test_crash_handler_with_active_connections**: Tests crash handler when main thread crashes
   - Starts a benchmark with 2 threads and 2 clients
   - Waits 5 seconds for connections to be established
   - Sends SEGV signal to the main process
   - Verifies crash report includes CLIENT LIST OUTPUT with connection states

2. **test_crash_handler_worker_thread**: Tests crash handler when a worker thread crashes
   - Starts a benchmark with 2 threads and 2 clients
   - Waits 5 seconds for connections to be established
   - Enumerates worker threads using `/proc/<pid>/task/`
   - Sends SEGV signal to a worker thread (not the main thread)
   - Verifies crash report correctly identifies the crashing worker thread
   - Verifies stack trace shows the worker thread's call stack

Both tests follow the standard RLTest pattern used by other memtier_benchmark tests and integrate seamlessly with the existing test infrastructure.

### Enabling Core Dumps

memtier_benchmark automatically attempts to enable core dumps at startup. However, system settings may prevent core dump generation.

To manually enable core dumps:

```bash
# Enable unlimited core dump size
$ ulimit -c unlimited

# Run memtier_benchmark
$ ./memtier_benchmark [options]
```

To check where core dumps are written:

```bash
$ cat /proc/sys/kernel/core_pattern
```

Common patterns:
- `core` - Writes to current directory as `core` or `core.<pid>`
- `|/usr/share/apport/apport %p %s %c %d %P` - Ubuntu/Debian using Apport
- `|/usr/lib/systemd/systemd-coredump %P %u %g %s %t %c %h %e` - systemd-coredump

### Testing Core Dump Generation

To verify that core dumps are being generated:

```bash
# 1. Enable core dumps
$ ulimit -c unlimited

# 2. Check current limit
$ ulimit -c
unlimited

# 3. Start memtier_benchmark and trigger a test crash
$ ./memtier_benchmark --server localhost --port 6379 --test-time 60 &
$ sleep 2
$ kill -SEGV `pgrep memtier`

# 4. Check for core dump in current directory
$ ls -lh core* 2>/dev/null

# 5. If using systemd-coredump, check with coredumpctl or journalctl
$ coredumpctl list memtier  # List all memtier crashes
$ journalctl -xe | grep -i memtier  # Check system journal for crash info
$ coredumpctl info  # Show details of most recent crash
$ coredumpctl gdb  # Open most recent core dump in gdb directly
$ coredumpctl dump -o core.dump  # Extract core file to current directory

# 6. If using Apport (Ubuntu/Debian), check /var/crash
$ ls -lh /var/crash/*memtier*
```

**If no core dump appears** (common on Ubuntu with Apport):

The message "Segmentation fault (core dumped)" may appear even when no core file is created. This happens when Apport intercepts the crash but doesn't save it (often for non-packaged binaries).

To get actual core files for debugging:

```bash
# Temporarily disable Apport and configure direct core dumps
$ sudo systemctl stop apport
$ sudo sysctl -w kernel.core_pattern=core.%p

# Enable core dumps in your shell
$ ulimit -c unlimited

# Trigger test crash
$ ./memtier_benchmark --server localhost --port 6379 --test-time 60 &
$ sleep 2
$ kill -SEGV `pgrep memtier`

# Core file should now appear
$ ls -lh core.*

# Analyze with gdb
$ gdb ./memtier_benchmark core.83396
(gdb) bt
(gdb) frame 0
(gdb) info locals

# Re-enable Apport when done
$ sudo sysctl -w kernel.core_pattern='|/usr/share/apport/apport -p%p -s%s -c%c -d%d -P%P -u%u -g%g -F%F -- %E'
$ sudo systemctl start apport
```

**Note**: The crash handler's stack trace is often sufficient for debugging without needing the core dump. Core dumps are mainly useful for examining variable values and memory state at the time of the crash.

### Analyzing Core Dumps

If a core dump is generated, you can analyze it using gdb.

**With systemd-coredump** (easiest method):

```bash
# List all core dumps
$ coredumpctl list

# Show details of most recent crash
$ coredumpctl info

# Open most recent core dump directly in gdb
$ coredumpctl gdb

# Or extract the core file
$ coredumpctl dump -o core.dump
$ gdb ./memtier_benchmark core.dump
```

**With direct core files**:

```bash
# Load the core dump
$ gdb ./memtier_benchmark core.12345

# In gdb, examine the backtrace
(gdb) bt

# Examine specific frames
(gdb) frame 0
(gdb) info locals

# Print variables
(gdb) print variable_name

# Quit gdb
(gdb) quit
```

**Example session**:

```bash
$ ./memtier_benchmark --server localhost --port 6379 --test-time 60 &
$ sleep 2
$ kill -SEGV `pgrep memtier`
[crash report appears]
Segmentation fault (core dumped)

$ coredumpctl list
TIME                          PID  UID  GID SIG     COREFILE EXE
Tue 2025-12-16 12:08:17 WET 83396 1000 1000 SIGSEGV present  /home/fco/.../memtier_benchmark

$ coredumpctl gdb
(gdb) bt
#0  0x00007483d6a9eb2c in __pthread_kill_implementation
#1  0x00007483d6a4527e in __GI_raise
#2  0x00005c8f4b42a5d2 in crash_handler
...
(gdb) quit
```



This will include full debugging information in the binary, making stack traces more readable and allowing detailed inspection with gdb.

### Reporting Crashes

If you encounter a crash, please report it by opening an issue on [GitHub](https://github.com/redis/memtier_benchmark/issues) and include:

1. The complete bug report output (from START to END markers)
2. The command line used to run memtier_benchmark
3. System information (OS, architecture, library versions)
4. Steps to reproduce the crash (if known)
5. Core dump or gdb backtrace (if available)

