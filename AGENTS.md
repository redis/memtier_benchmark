# AI Agent Guidelines for memtier_benchmark

This document provides guidance for AI agents working with the memtier_benchmark codebase.

## Project Overview

memtier_benchmark is a high-performance load generation and benchmarking tool for NoSQL databases (Redis and Memcached) developed by Redis. It is written in C++ and uses libevent for async I/O.

## Repository Structure

```
├── memtier_benchmark.cpp  # Main entry point
├── client.cpp/h           # Client implementation
├── cluster_client.cpp/h   # Redis Cluster support
├── shard_connection.cpp/h # Connection handling
├── protocol.cpp/h         # Redis/Memcached protocol handling
├── obj_gen.cpp/h          # Object/key generation
├── run_stats.cpp/h        # Statistics collection and reporting
├── JSON_handler.cpp/h     # JSON output support
├── file_io.cpp/h          # File I/O operations
├── config_types.cpp/h     # Configuration types
├── deps/hdr_histogram/    # HdrHistogram library (vendored)
├── tests/                 # Integration tests (Python/RLTest)
└── bash-completion/       # Shell completion scripts
```

## Build System

- **Build tool**: GNU Autotools (autoconf/automake)
- **Language**: C++11
- **Dependencies**: libevent (≥2.0.10), OpenSSL (optional), zlib, pthread

### Build Commands

```bash
# Initial setup
autoreconf -ivf
./configure
make

# Debug build (no optimizations)
./configure CXXFLAGS="-g -O0 -Wall"

# With sanitizers
./configure --enable-sanitizers        # ASAN/LSAN
./configure --enable-thread-sanitizer  # TSAN
./configure --enable-ubsan             # UBSan

# Disable TLS
./configure --disable-tls
```

## Code Style

- Uses `clang-format` for formatting (config in `.clang-format`)
- Run `make format` to format code
- Run `make format-check` to verify formatting
- CI enforces formatting on all PRs

## Testing

Integration tests use RLTest framework (Python-based):

```bash
# Setup virtual environment
mkdir -p .env && virtualenv .env
source .env/bin/activate
pip install -r tests/test_requirements.txt

# Run all tests (takes several minutes)
./tests/run_tests.sh

# Run specific test file
TEST=test_crash_handler_integration.py ./tests/run_tests.sh

# Run with standalone mode (default)
OSS_STANDALONE=1 ./tests/run_tests.sh

# Run with cluster mode
OSS_CLUSTER=1 ./tests/run_tests.sh

# Run with cluster mode and custom shard count (default: 3)
OSS_CLUSTER=1 SHARDS=5 ./tests/run_tests.sh

# Run with TLS
TLS=1 ./tests/run_tests.sh

# Run with sanitizers
ASAN_OPTIONS=detect_leaks=1 ./tests/run_tests.sh
```

## Key Technical Details

1. **Multi-threaded architecture**: Uses pthreads with libevent for async I/O per thread
2. **Protocol support**: Redis (RESP) and Memcached (text and binary)
3. **Statistics**: Uses HdrHistogram for latency percentiles
4. **Cluster support**: Handles Redis Cluster topology and slot-based routing

## Common Development Tasks

### Adding a new command-line option
1. Add option definition in `memtier_benchmark.cpp` (options array and getopt_long)
2. Add field to benchmark_config struct
3. Handle the option in the main parsing loop
4. Update man page (`memtier_benchmark.1`) and bash completion
5. **Add tests** for the new option (see below)

### Adding a new test
1. Create Python test file in `tests/` following `tests_oss_simple_flow.py` pattern
2. Use RLTest decorators and `mb.py` helper for running memtier_benchmark
3. Tests run against actual Redis server (started by RLTest)

**All new features and bug fixes should include corresponding tests.**

### Modifying protocol handling
- Protocol implementations are in `protocol.cpp`
- Each protocol (redis/memcached) has its own class hierarchy
- Connection state machine is in `shard_connection.cpp`

## Debugging

### Debug Build

```bash
# Build with debug symbols and no optimization
./configure CXXFLAGS="-g -O0 -Wall"
make
```

### Using GDB

```bash
# Run under gdb
gdb --args ./memtier_benchmark -s localhost -p 6379

# Attach to running process
gdb -p $(pgrep memtier)

# Analyze core dump
gdb ./memtier_benchmark core.<pid>
```

### Crash Handler

memtier_benchmark has a built-in crash handler that prints detailed bug reports on crashes (SIGSEGV, SIGBUS, SIGFPE, SIGILL, SIGABRT), including:
- Stack traces for all threads
- System and build information
- Active client connection states

### Core Dumps

```bash
# Enable core dumps
ulimit -c unlimited

# Check core pattern
cat /proc/sys/kernel/core_pattern

# With systemd-coredump
coredumpctl list memtier
coredumpctl gdb
```

### Memory Debugging with Sanitizers

```bash
# AddressSanitizer (memory errors, leaks)
./configure --enable-sanitizers
make
ASAN_OPTIONS=detect_leaks=1 ./memtier_benchmark ...

# ThreadSanitizer (data races)
./configure --enable-thread-sanitizer
make
TSAN_OPTIONS="suppressions=$(pwd)/tsan_suppressions.txt" ./memtier_benchmark ...

# UndefinedBehaviorSanitizer
./configure --enable-ubsan
make
UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1 ./memtier_benchmark ...
```

**Note**: TSAN and ASAN are mutually exclusive; UBSan can be combined with ASAN.

## License Header (Required)

All C++ source files must include this header:

```cpp
/*
 * Copyright (C) 2011-2026 Redis Labs Ltd.
 *
 * This file is part of memtier_benchmark.
 *
 * memtier_benchmark is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2.
 *
 * memtier_benchmark is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with memtier_benchmark.  If not, see <http://www.gnu.org/licenses/>.
 */
```

## Important Notes

- Default build includes debug symbols (`-g`) for crash analysis
- The crash handler prints detailed reports on SIGSEGV/SIGBUS/etc.
- Core dumps require `ulimit -c unlimited` and proper kernel config
- TSAN and ASAN are mutually exclusive
