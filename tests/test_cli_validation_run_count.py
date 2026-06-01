"""
CLI validation regression tests for --run-count.

Closes items 4 and 5 from issue #426 (phase 2a):

  4. ``memtier_benchmark --run-count -1`` previously triggered SIGABRT with
     ``std::bad_alloc`` because the negative value silently wrapped through
     the unsigned cast (-1 -> ~4.3B), which the per-run vectors then tried
     to reserve.

  5. ``memtier_benchmark --run-count 2147483647`` (INT_MAX) previously
     triggered SIGABRT with ``std::bad_alloc`` because no upper bound was
     enforced and the allocator OOM'd while reserving the per-run
     bookkeeping vectors.

After the fix, both inputs must be rejected at parse time with a non-zero
exit code and a readable error on stderr.  A reasonable valid value (2)
must still run normally end-to-end.

The CLI rejection cases (1-3 below) only exercise the argument-parsing
path and do not need a live Redis connection.  The happy-path case (4)
runs against the RLTest-provided server.

Run with:
  TEST=test_cli_validation_run_count.py OSS_STANDALONE=1 ./tests/run_tests.sh
"""
import json
import os
import subprocess
import tempfile

from include import (
    MEMTIER_BINARY,
    add_required_env_arguments,
    addTLSArgs,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_memtier(args):
    """Run memtier_benchmark with *args* and return the CompletedProcess.

    No --server is supplied: validation must reject the bad --run-count
    value before any connection attempt is made.
    """
    return subprocess.run(
        [MEMTIER_BINARY] + args,
        capture_output=True,
        text=True,
        timeout=30,
    )


# ---------------------------------------------------------------------------
# 1. --run-count=-1  (issue #426 item 4)
# ---------------------------------------------------------------------------

def test_run_count_negative_rejected(env):
    """``--run-count=-1`` must be rejected (was SIGABRT std::bad_alloc)."""
    result = _run_memtier(["--run-count=-1"])

    env.assertNotEqual(
        result.returncode, 0,
        message="--run-count=-1 must exit non-zero (was SIGABRT std::bad_alloc)",
    )
    env.assertTrue(
        "run count must be greater than zero" in result.stderr,
        message=(
            "Expected 'run count must be greater than zero' in stderr, "
            "got: {!r}".format(result.stderr)
        ),
    )


# ---------------------------------------------------------------------------
# 2. --run-count=0
# ---------------------------------------------------------------------------

def test_run_count_zero_rejected(env):
    """``--run-count=0`` must be rejected with a clear error."""
    result = _run_memtier(["--run-count=0"])

    env.assertNotEqual(
        result.returncode, 0,
        message="--run-count=0 must exit non-zero",
    )
    env.assertTrue(
        "run count must be greater than zero" in result.stderr,
        message=(
            "Expected 'run count must be greater than zero' in stderr, "
            "got: {!r}".format(result.stderr)
        ),
    )


# ---------------------------------------------------------------------------
# 3. --run-count=INT_MAX  (issue #426 item 5)
# ---------------------------------------------------------------------------

def test_run_count_int_max_rejected(env):
    """``--run-count=2147483647`` must be rejected (was SIGABRT std::bad_alloc)."""
    result = _run_memtier(["--run-count=2147483647"])

    env.assertNotEqual(
        result.returncode, 0,
        message="--run-count=2147483647 must exit non-zero (was SIGABRT std::bad_alloc)",
    )
    env.assertTrue(
        "run count must be <=" in result.stderr,
        message=(
            "Expected upper-bound rejection ('run count must be <= ...') in stderr, "
            "got: {!r}".format(result.stderr)
        ),
    )


# ---------------------------------------------------------------------------
# 4. --run-count=2  happy path
# ---------------------------------------------------------------------------

def test_run_count_valid_runs_normally(env):
    """``--run-count=2`` must run end-to-end without error."""
    env.skipOnCluster()

    test_dir = tempfile.mkdtemp()
    config = get_default_memtier_config(threads=1, clients=1, requests=20)
    benchmark_specs = {
        "name": env.testName,
        "args": ["--run-count=2"],
    }
    addTLSArgs(benchmark_specs, env)
    add_required_env_arguments(
        benchmark_specs, config, env, env.getMasterNodesList()
    )

    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)
    benchmark = Benchmark.from_json(run_config, benchmark_specs)

    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(
        memtier_ok,
        message="--run-count=2 (a valid value) must complete successfully",
    )

    json_filename = os.path.join(run_config.results_dir, "mb.json")
    env.assertTrue(
        os.path.isfile(json_filename),
        message="Expected mb.json to be produced for valid --run-count run",
    )

    with open(json_filename) as f:
        results = json.load(f)

    # With run_count > 1 the report emits BEST/WORST/AGGREGATED sections.
    # Their presence confirms run_count was honored (not silently coerced
    # to 1 by the fix).
    env.assertTrue(
        "BEST RUN RESULTS" in results,
        message=(
            "Expected 'BEST RUN RESULTS' section in mb.json with --run-count=2; "
            "top-level keys: {}".format(sorted(results.keys()))
        ),
    )
    env.assertTrue(
        "WORST RUN RESULTS" in results,
        message=(
            "Expected 'WORST RUN RESULTS' section in mb.json with --run-count=2; "
            "top-level keys: {}".format(sorted(results.keys()))
        ),
    )
