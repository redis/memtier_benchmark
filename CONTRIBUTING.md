# Contributing

We treat this repo as "Open Source" within Redis: anyone who clears the bar below is welcome to contribute.

## Local setup

memtier_benchmark is a C++ project built with GNU Autotools. The following steps work on Ubuntu/Debian; see [DEVELOPMENT.md](DEVELOPMENT.md) for CentOS/Red Hat and macOS instructions.

**Install build dependencies (Ubuntu/Debian):**

```bash
sudo apt-get install build-essential autoconf automake \
    libevent-dev pkg-config zlib1g-dev libssl-dev clang-format
```

**Clone and build:**

```bash
git clone git@github.com:redis/memtier_benchmark.git
cd memtier_benchmark
autoreconf -ivf
./configure
make
```

The binary is produced at `./memtier_benchmark`. Run `sudo make install` to install system-wide.

**Enable the pre-commit formatting hook (once per clone):**

```bash
make install-hooks
```

This runs `make format-check-staged` before each commit to catch clang-format violations early.

## Branch naming

```
<type>/<short-description>
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`

Example: `feat/add-pipeline-mode`

## Coding standards

- Keep changes focused; one logical change per PR.
- Follow the conventions already present in the codebase (formatting, naming, error handling).
- No dead code, no commented-out blocks.
- All C++ files must pass `clang-format` checks. Run `make format` to auto-format, then `make format-check` to verify.

## Submitting changes

1. Fork the repository or create a branch from `master`.
2. Make your changes with clear, atomic commits.
3. Open a pull request against `master` with a descriptive title and summary.
4. Address review comments promptly; force-push to the same branch to update.

## Testing

- All new behaviour must be covered by tests.
- Existing tests must pass: run the test suite locally before opening a PR.
- Coverage should not decrease.

The integration tests use [RLTest](https://github.com/RedisLabsModules/RLTest) and require a Redis server on `$PATH`. Set up a Python virtualenv and run the suite:

```bash
mkdir -p .env
virtualenv .env
source .env/bin/activate
pip install -r tests/test_requirements.txt
./tests/run_tests.sh
```

To see all available test options:

```bash
./tests/run_tests.sh --help
```

## Review process

- At least one maintainer approval is required before merge.
- CI must be green.
- Maintainers may request changes or close PRs that do not meet the bar — this is normal and not personal.
