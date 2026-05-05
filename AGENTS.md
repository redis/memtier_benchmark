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
- **Always run `make format` after modifying C++ files and before committing.** Verify with `make format-check` that no formatting issues remain.

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

Every new CLI option **must** be added in **all** of these locations:

1. Add option to the `extended_options` enum in `memtier_benchmark.cpp`
2. Add entry to the `long_options[]` array in `memtier_benchmark.cpp`
3. Add case handler in the `getopt_long` switch in `memtier_benchmark.cpp`
4. Add field to `benchmark_config` struct in `memtier_benchmark.h`
5. Initialize default value in `config_init_defaults()` in `memtier_benchmark.cpp`
6. Add help text in `usage()` function in `memtier_benchmark.cpp`
7. Update man page (`memtier_benchmark.1`)
8. Update bash completion (`bash-completion/memtier_benchmark`) — add to `options_no_comp` (takes a value) or `options_no_args` (flag)
9. **Add tests** for the new option (see below)

**Verification**: After adding a new flag, always confirm it appears in both `--help` output and `memtier_benchmark.1`. Run `./memtier_benchmark --help | grep <flag>` and `grep <flag> memtier_benchmark.1` to verify.

### Adding a new test
1. Create Python test file in `tests/` following `tests_oss_simple_flow.py` pattern
2. Use RLTest decorators and `mb.py` helper for running memtier_benchmark
3. Tests run against actual Redis server (started by RLTest)

**All new features and bug fixes should include corresponding tests.**

### Test output validation

- Always validate structured JSON output (`mb.json`) for result correctness, not just stdout text. The JSON file under `ALL STATS` contains per-command entries (e.g., `"Sets"`, `"Gets"`, `"Scan 0s"`) with `"Count"`, `"Ops/sec"`, latency metrics, etc.
- Use `json.load()` to parse `mb.json` and assert on the expected keys and values.
- See `tests_oss_simple_flow.py` for examples of JSON output validation patterns: `results_dict['ALL STATS']['Sets']`.

### Data preloading in tests

- When possible, prefer preloading data using memtier itself (`--ratio=1:0 --key-pattern=P:P --requests=allkeys`) to match real usage patterns. See `tests_oss_simple_flow.py` `test_preload_and_set_get` for the standard pattern.
- Direct Redis client calls (Python `redis` pipeline) are acceptable when simpler — e.g., loading a small number of keys with specific prefixes or non-string data types.

### Modifying protocol handling
- Protocol implementations are in `protocol.cpp`
- Each protocol (redis/memcached) has its own class hierarchy
- Connection state machine is in `shard_connection.cpp`

## Releasing

### Release-note style

GitHub release notes are curated, not auto-generated. Follow the pattern established
from 2.2.1 onward:

```markdown
# What's Changed

## New Features
- **Short title**: One-sentence description ending with the PR number (#123).

## Bug Fixes
- **Short title**: Description with `inline code` for flag/identifier names (#124).

## Packaging & Distribution
- ...

## Developer Tooling & CI Improvements
- ...

## Maintenance
- ...

## New Contributors
- @handle made their first contribution (#125)

**Full Changelog**: https://github.com/redis/memtier_benchmark/compare/<prev>...<this>
```

Conventions:
- `# What's Changed` heading, then `##` sections (only the sections that have
  content — drop empty ones).
- Section order, when present: New Features → Bug Fixes → Packaging & Distribution
  → Developer Tooling & CI Improvements → Build & Configuration Improvements →
  Maintenance → AI → New Contributors.
- Bullet form: `- **Bold lead phrase**: Sentence-cased description ending with a
  period, with the PR number in parens before the period (#NNN).`
- Use inline backticks for CLI flags, file names, and identifiers.
- Close with the `**Full Changelog**` compare link.
- Do *not* paste the GitHub auto-generated `* by @user in <PR-link>` style — it
  was used up to 2.2.0 but has been replaced by the curated form.

### Patch releases (cherry-picks to a release branch)

Patch releases live on a release branch (e.g. `2.3` for the 2.3.x line). The flow:

1. **List the candidates**: `git log --oneline origin/master ^origin/<release-branch>`.
   For each commit, decide whether it belongs in a patch release. Skip duplicates
   that were applied independently to the release branch (compare diffs, not just
   subjects — see `0cb8e9a` vs `d92b163` in the 2.3.1 history).
2. **Branch from the release branch**: `git switch -c release.<X.Y.Z> origin/<release-branch>`.
3. **Cherry-pick in chronological order** so the release-branch history reads
   forward. Resolve conflicts as they arise.
4. **Verify CI workflows fire on the release branch.** Workflow files
   (`asan.yml`, `ci.yml`, `code-style.yml`, `tsan.yml`, `ubsan.yml`,
   `release-rpm.yml`) historically restricted `pull_request: branches:` to
   `[master, main]` only. A PR targeting the release branch will run **zero**
   checks until the branch list includes the release branch — extend each
   workflow's `branches:` filter as part of the release PR if it is missing.
5. **Run `utils/prepare_release.sh <X.Y.Z>`** as the last commit. See gotchas
   below.
6. **Open a single PR base `<release-branch>` ← `release.<X.Y.Z>`.** Don't split
   the cherry-picks and the version bump into separate PRs for a patch release;
   the atomic shape leaves the release branch self-consistent if the PR is
   delayed.
7. **After merge, tag and publish a Release** on the merged release-branch HEAD.
   The `release: published` event fires `dockers.yml`, `release-rpm.yml`, and
   `release.yml` (APT) automatically.
8. **Mirror any review-driven follow-ups back to master** as a separate PR.
   Cursor Bugbot frequently catches latent issues on release-prep PRs (e.g. the
   `get_total_latency()` narrowing flagged on the 2.3.1 backport); do not let
   those fixes live only on the release branch.

### `utils/prepare_release.sh` gotchas

`utils/prepare_release.sh <version>` bumps `configure.ac`, runs
`autoreconf -ivf && ./configure && make && make rebuild-man`, and commits
`configure.ac + memtier_benchmark.1` as a single `Release <version>` commit.
Watch out for:

- **Stale `version.h` corrupts the man page.** The Makefile rule
  `version.h: $(SHELL) $(srcdir)/version.sh` has **no dependencies**, so once
  `version.h` exists `make` won't re-run `version.sh`. The script then bakes an
  outdated git SHA into `memtier_benchmark.1` via `help2man`. **Always
  `rm -f version.h` before running the script** so the SHA matches HEAD.
- **No branch sanity check.** The script doesn't verify the current branch.
  Confirm `git branch --show-current` matches the intended release branch
  before running.
- **No version-string validation.** A typo like `2.3.1.` writes a broken
  `AC_INIT` line and the failure only surfaces on the next build. Sanity-check
  with `grep AC_INIT configure.ac` after the bump commit lands.
- **No tag is created.** The script commits but doesn't tag. After merge you
  still need `git tag <X.Y.Z>` on the merged release-branch tip, push the tag,
  and publish a GitHub Release for the workflows to fire.
- **Untracked-file check is dirty-state-only.** The script blocks on tracked
  modifications but ignores untracked files. Editor backups like
  `configure.ac~` won't trigger the guard.

### Release-note generation

Use `gh release create <tag> --generate-notes --draft` as a starting point,
then rewrite the body in the curated style above before publishing. Don't
publish the auto-generated draft as-is — it doesn't match the project's
release-note conventions.

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
