"""
Regression tests for `--command` and `--command-ratio` parse-time validation.

Covers items 12, 13 and 14 from issue #426 (CLI-fuzz follow-ups, Phase 2c):

  12. `--command __key__` (bare placeholder, no command name) used to trip the
      runtime assert at protocol.cpp:774 ("first arg is not command name?")
      and SIGABRT. We now reject at parse time with a readable error.

  13. `--command ''` parsed as a zero-arg command, then the worker loop spun
      forever picking nothing. We now reject the empty / whitespace-only
      command at parse time.

  14. `--command-ratio 0` (also empty / whitespace / negative) made strtoul()
      return 0 (or wrap to UINT_MAX for negatives), the ratio sampler then
      never picked the command and the run hung. We now require a positive
      integer at parse time.

These tests exercise the argument-parsing path only and don't need a live
Redis - we invoke the binary with `subprocess`, expect a non-zero exit and a
clear error message in stderr. The valid case (`--command-ratio 1`) is run
against the test fixture's Redis to confirm the happy path still completes.

Run with:
  TEST=test_cli_validation_command.py OSS_STANDALONE=1 ./tests/run_tests.sh
"""
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
# Internal helpers
# ---------------------------------------------------------------------------

def _run_memtier(args, timeout=10):
    """Run memtier_benchmark with *args* and return the CompletedProcess.

    A timeout guards against regressions - if the parse-time validation is
    ever removed the binary would re-enter the hang/spin path and we want
    the test to fail loudly rather than block the suite.
    """
    return subprocess.run(
        [MEMTIER_BINARY] + args,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _build_benchmark(env, test_dir, extra_args, threads=1, clients=1,
                     requests=100):
    """Return (Benchmark, RunConfig) for an arbitrary workload."""
    config = get_default_memtier_config(threads=threads, clients=clients,
                                        requests=requests)
    benchmark_specs = {"name": env.testName, "args": list(extra_args)}
    addTLSArgs(benchmark_specs, env)
    add_required_env_arguments(benchmark_specs, config, env,
                               env.getMasterNodesList())
    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)
    return Benchmark.from_json(run_config, benchmark_specs), run_config


# ---------------------------------------------------------------------------
# CLI rejection tests
# ---------------------------------------------------------------------------

def test_command_first_arg_placeholder_rejected(env):
    """`--command __key__` must be rejected before reaching the assert path.

    Item 12 from #426: a bare placeholder as the command name used to SIGABRT
    inside protocol.cpp::format_arbitrary_command at the
    "first arg is not command name?" assert.
    """
    master = env.getMasterNodesList()[0]
    result = _run_memtier([
        "-s", "127.0.0.1",
        "-p", str(master["port"]),
        "--command", "__key__",
        "--test-time=1",
    ])

    env.assertNotEqual(
        result.returncode, 0,
        message="--command __key__ must exit non-zero",
    )
    env.assertTrue(
        "first token must be a literal command name" in result.stderr,
        message="Expected literal-command-name rejection in stderr; "
                "got: {!r}".format(result.stderr),
    )
    env.assertTrue(
        "__key__" in result.stderr,
        message="Expected offending placeholder in stderr; "
                "got: {!r}".format(result.stderr),
    )


def test_command_empty_string_rejected(env):
    """`--command ''` must be rejected before entering the worker loop.

    Item 13 from #426: an empty command parsed as zero argv tokens and the
    benchmark hung forever ignoring --test-time.
    """
    master = env.getMasterNodesList()[0]
    result = _run_memtier([
        "-s", "127.0.0.1",
        "-p", str(master["port"]),
        "--command", "",
        "--test-time=1",
    ])

    env.assertNotEqual(
        result.returncode, 0,
        message="--command '' must exit non-zero",
    )
    env.assertTrue(
        "--command requires a non-empty command string" in result.stderr,
        message="Expected non-empty-command rejection in stderr; "
                "got: {!r}".format(result.stderr),
    )


def test_command_ratio_zero_rejected(env):
    """`--command-ratio 0` must be rejected at parse time.

    Item 14 from #426: a zero ratio made the worker pick nothing and the run
    hung; the previous error message ("failed to set ratio") also didn't make
    the bound clear.
    """
    master = env.getMasterNodesList()[0]
    result = _run_memtier([
        "-s", "127.0.0.1",
        "-p", str(master["port"]),
        "--command", "SET __key__ __data__",
        "--command-ratio", "0",
        "--test-time=1",
    ])

    env.assertNotEqual(
        result.returncode, 0,
        message="--command-ratio 0 must exit non-zero",
    )
    env.assertTrue(
        "--command-ratio must be a positive integer" in result.stderr,
        message="Expected positive-integer message in stderr; "
                "got: {!r}".format(result.stderr),
    )


# ---------------------------------------------------------------------------
# Happy-path regression
# ---------------------------------------------------------------------------

def test_command_ratio_one_succeeds(env):
    """`--command-ratio 1` must still parse and run end-to-end.

    Sanity check that the tightened set_ratio() (now rejects 0 / empty /
    negative) doesn't break the documented default-equivalent value.
    """
    test_dir = tempfile.mkdtemp()

    benchmark, run_config = _build_benchmark(
        env, test_dir,
        extra_args=[
            "--command", "SET __key__ __data__",
            "--command-ratio", "1",
            "--command-key-pattern", "R",
        ],
        threads=1,
        clients=1,
        requests=50,
    )

    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(
        memtier_ok,
        message="Benchmark must complete cleanly with --command-ratio 1",
    )


# ---------------------------------------------------------------------------
# Issue #515 - commands whose reply arity is not 1:1 with the request
# ---------------------------------------------------------------------------

def test_command_multi_reply_family_rejected(env):
    """The pub/sub and streaming families must be rejected at parse time.

    Issue #515: Redis answers SUBSCRIBE/PSUBSCRIBE/UNSUBSCRIBE once per
    channel, so `--command="SUBSCRIBE a b"` produced two replies for one
    queued request. The surplus reply popped a request that was never queued
    and drove m_pending_resp negative, tripping the assert in
    shard_connection::pop_req() -- SIGABRT, in release builds too (the default
    CXXFLAGS carry no -DNDEBUG).

    Single-channel SUBSCRIBE is rejected as well: it is equally fatal the
    moment anyone publishes to that channel, because the message arrives with
    no request outstanding. MONITOR / PSYNC stream indefinitely and wedge the
    run past --test-time instead.
    """
    master = env.getMasterNodesList()[0]

    for command in (
        "SUBSCRIBE chan1 chan2",
        "SUBSCRIBE chan1",
        "PSUBSCRIBE ch1* ch2*",
        "UNSUBSCRIBE chan1 chan2",
        "PUNSUBSCRIBE ch1*",
        "SSUBSCRIBE schan",
        "SUNSUBSCRIBE schan",
        "MONITOR",
        "SYNC",
        "PSYNC",
        # case-insensitive: the predicate uppercases before comparing
        "subscribe chan1 chan2",
    ):
        result = _run_memtier([
            "-s", "127.0.0.1",
            "-p", str(master["port"]),
            "--command", command,
            "--test-time=1",
        ])

        env.assertNotEqual(
            result.returncode, 0,
            message="--command {!r} must exit non-zero".format(command),
        )
        # -6/-11 would mean we reached the crash instead of the rejection.
        env.assertTrue(
            result.returncode > 0,
            message="--command {!r} must be rejected, not crash "
                    "(returncode {})".format(command, result.returncode),
        )
        env.assertTrue(
            "is not supported" in result.stderr,
            message="Expected multi-reply rejection for {!r} in stderr; "
                    "got: {!r}".format(command, result.stderr),
        )
        env.assertTrue(
            "Assertion" not in result.stderr,
            message="--command {!r} must not trip an assert; "
                    "got: {!r}".format(command, result.stderr),
        )


def test_command_publish_still_allowed(env):
    """PUBLISH is 1:1 (integer reply) and must keep working.

    Guards against over-broad denylisting of the pub/sub family: only the
    subscribe side is multi-reply.
    """
    test_dir = tempfile.mkdtemp()

    benchmark, run_config = _build_benchmark(
        env, test_dir,
        extra_args=["--command", "PUBLISH chan1 __data__"],
        threads=1,
        clients=1,
        requests=50,
    )

    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(
        memtier_ok,
        message="PUBLISH must remain benchmarkable",
    )
