"""
Regression tests for issue #733: memtier started with stdin, stdout or
stderr closed.

The next file memtier opened (--json-out-file) used to get fd 1 or 2, so the
text results or the stderr log lines were written into the JSON file, and
the run exited 0. Whether the JSON ended up invalid depends on when the
stdout buffer was flushed (on master it was valid in about 1 of 3
closed-stdout runs), so the closed-stdout tests catch the bug through the
exit code and the error message, not only the JSON. memtier now reopens a
closed fd 0-2 on /dev/null first. If stdout was closed and there is no
--out-file, the text results would go nowhere, so memtier exits 1 at
startup, before connecting or opening any output file. An output path that
names a closed fd (/dev/stdout, /dev/fd/1, a symlink to them) is rejected
the same way; any spelling of /dev/null is accepted.

Run subset:
  TEST=test_closed_std_fds.py ./tests/run_tests.sh
"""

import contextlib
import json
import os
import subprocess
import tempfile
from include import *
from mb import Benchmark, RunConfig

STDOUT_CLOSED_ERROR = "error: stdout was closed at startup, so the results would not be written; use --out-file"


def _assert_stopped_at_startup(env, json_path, stderr):
    """memtier exited before it opened the output files or connected: no
    JSON file was created and no run was started."""
    env.assertFalse(os.path.exists(json_path))
    env.assertFalse("Json file" in stderr, message=stderr)
    env.assertFalse("Preparing benchmark client" in stderr, message=stderr)


@contextlib.contextmanager
def _benchmark(env, extra_args, run_count=1):
    """Yield (benchmark, config, json_path) in a temporary directory that is
    removed afterwards."""
    with tempfile.TemporaryDirectory() as test_dir:
        yield _make_benchmark(env, extra_args, run_count, test_dir)


def _make_benchmark(env, extra_args, run_count, test_dir):
    benchmark_specs = {"name": env.testName, "args": list(extra_args)}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=100)
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    # Benchmark adds --out-file mb.stdout; drop it so the text results go to
    # stdout. It also adds --json-out-file mb.json.
    i = benchmark.args.index('--out-file')
    del benchmark.args[i:i + 2]
    json_path = os.path.join(config.results_dir, 'mb.json')
    env.assertContains(json_path, benchmark.args)
    benchmark.args += ['--run-count', str(run_count)]
    return benchmark, config, json_path


# Close the fd in a shell rather than in preexec_fn, which is not safe when
# the parent process has threads.
_CLOSE_REDIRECT = {0: '<&-', 1: '>&-', 2: '2>&-'}


def _run_with_fd_closed(benchmark, closed_fd):
    """Run memtier with closed_fd (0, 1 or 2) closed; capture stdout/stderr."""
    script = 'exec "$@" %s' % _CLOSE_REDIRECT[closed_fd]
    process = subprocess.Popen(['sh', '-c', script, 'sh'] + benchmark.args,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        stdout, stderr = process.communicate(timeout=60)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()
    return process.returncode, stdout.decode(errors='replace'), stderr.decode(errors='replace')


def _load_json(env, json_path):
    with open(json_path) as f:
        text = f.read()
    try:
        return json.loads(text)
    except ValueError as e:
        env.assertTrue(False, message="invalid JSON (%s): %r" % (e, text[:300]))
        return None


def _check_stdout_closed(env, extra_args, run_count=1):
    with _benchmark(env, extra_args, run_count) as (benchmark, _, json_path):
        rc, _, stderr = _run_with_fd_closed(benchmark, 1)
        env.assertEqual(rc, 1, message=stderr)
        env.assertContains(STDOUT_CLOSED_ERROR, stderr)
        _assert_stopped_at_startup(env, json_path, stderr)


def test_stdout_closed_json_out_file(env):
    """Closed stdout, full text output: the text used to go into the JSON."""
    _check_stdout_closed(env, [])


def test_stdout_closed_json_out_file_hide_histogram(env):
    """Closed stdout, --hide-histogram: the JSON stayed valid, but the results
    were lost (write() EBADF) and the run exited 0."""
    _check_stdout_closed(env, ['--hide-histogram'])


def test_stdout_closed_json_out_file_run_count_2(env):
    """Closed stdout, --run-count 2: more text, written before the JSON closes."""
    _check_stdout_closed(env, [], run_count=2)


def test_stdout_closed_with_out_file_succeeds(env):
    """With --out-file the results are kept, so a closed stdout is fine."""
    with _benchmark(env, []) as (benchmark, config, json_path):
        out_path = os.path.join(config.results_dir, 'mb.stdout')
        benchmark.args += ['--out-file', out_path]
        rc, _, stderr = _run_with_fd_closed(benchmark, 1)
        env.assertEqual(rc, 0, message=stderr)
        _load_json(env, json_path)
        with open(out_path) as f:
            env.assertContains("ALL STATS", f.read())


def test_stderr_closed_json_out_file(env):
    """Closed stderr: the log lines used to go into the JSON file."""
    with _benchmark(env, ['--hide-histogram']) as (benchmark, _, json_path):
        rc, stdout, _ = _run_with_fd_closed(benchmark, 2)
        env.assertEqual(rc, 0, message=stdout[-500:])
        data = _load_json(env, json_path)
        if data is not None:
            env.assertTrue("ALL STATS" in data, message=list(data.keys()))
        env.assertContains("ALL STATS", stdout)


def test_stdin_closed(env):
    """Closed stdin: reopened on /dev/null, the run is not affected."""
    with _benchmark(env, ['--hide-histogram']) as (benchmark, _, json_path):
        rc, stdout, stderr = _run_with_fd_closed(benchmark, 0)
        env.assertEqual(rc, 0, message=stderr[-500:])
        data = _load_json(env, json_path)
        if data is not None:
            env.assertTrue("ALL STATS" in data, message=list(data.keys()))
        env.assertContains("ALL STATS", stdout)


def _check_stdout_path_rejected(env, option, path=None):
    """With stdout closed, a path that names stdout resolves to the reopened
    /dev/null; memtier must reject it before it opens a file or connects.
    path=None uses a symlink (in the results dir) to /dev/stdout."""
    if not os.path.exists('/dev/stdout'):
        env.skip()
    with _benchmark(env, ['--hide-histogram']) as (benchmark, config, json_path):
        if path is None:
            path = os.path.join(config.results_dir, 'stdout-link')
            os.symlink('/dev/stdout', path)
        if option == '--json-out-file':
            i = benchmark.args.index('--json-out-file')
            benchmark.args[i + 1] = path
        else:
            benchmark.args += [option, path]
        rc, _, stderr = _run_with_fd_closed(benchmark, 1)
        env.assertEqual(rc, 1, message=stderr)
        env.assertContains("error: %s %s resolves to stdout, which was closed at startup" % (option, path), stderr)
        _assert_stopped_at_startup(env, json_path, stderr)


def test_stdout_closed_out_file_dev_stdout(env):
    _check_stdout_path_rejected(env, '--out-file', '/dev/stdout')


def test_stdout_closed_json_out_file_dev_stdout(env):
    _check_stdout_path_rejected(env, '--json-out-file', '/dev/stdout')


def test_stdout_closed_out_file_dev_fd_1(env):
    if not os.path.exists('/dev/fd'):
        env.skip()
    _check_stdout_path_rejected(env, '--out-file', '/dev/fd/1')


def test_stdout_closed_out_file_proc_thread_self_fd_1(env):
    if not os.path.exists('/proc/thread-self/fd'):
        env.skip()
    _check_stdout_path_rejected(env, '--out-file', '/proc/thread-self/fd/1')


def test_stdout_closed_out_file_symlink_to_dev_stdout(env):
    _check_stdout_path_rejected(env, '--out-file')


def _check_dev_null_allowed(env, out_path):
    """An --out-file that is /dev/null discards the text results on purpose,
    however the path is spelled."""
    with _benchmark(env, ['--hide-histogram']) as (benchmark, config, json_path):
        if out_path is None:
            out_path = os.path.join(config.results_dir, 'null-link')
            os.symlink('/dev/null', out_path)
        benchmark.args += ['--out-file', out_path]
        rc, _, stderr = _run_with_fd_closed(benchmark, 1)
        env.assertEqual(rc, 0, message=stderr)
        data = _load_json(env, json_path)
        if data is not None:
            env.assertTrue("ALL STATS" in data, message=list(data.keys()))


def test_stdout_closed_out_file_dev_null_is_allowed(env):
    _check_dev_null_allowed(env, '/dev/null')


def test_stdout_closed_out_file_dev_dot_null_is_allowed(env):
    _check_dev_null_allowed(env, '/dev/./null')


def test_stdout_closed_out_file_symlink_to_dev_null_is_allowed(env):
    _check_dev_null_allowed(env, None)


def _check_closed_fd_path_rejected(env, closed_fd, option, path):
    """Same as _check_stdout_path_rejected for stdin and stderr. With stderr
    closed the error message goes to /dev/null, so only the exit code, the
    missing JSON file and the missing results show that memtier stopped."""
    if not os.path.exists(path):
        env.skip()
    with _benchmark(env, ['--hide-histogram']) as (benchmark, config, json_path):
        if option == '--json-out-file':
            i = benchmark.args.index('--json-out-file')
            benchmark.args[i + 1] = path
        else:
            benchmark.args += [option, path]
        rc, stdout, stderr = _run_with_fd_closed(benchmark, closed_fd)
        env.assertEqual(rc, 1, message=stdout[-500:] + stderr[-500:])
        env.assertFalse(os.path.exists(json_path))
        env.assertFalse("ALL STATS" in stdout, message=stdout[-500:])
        if closed_fd != 2:
            env.assertContains("error: %s %s resolves to %s, which was closed at startup" %
                               (option, path, ['stdin', 'stdout', 'stderr'][closed_fd]), stderr)


def test_stderr_closed_json_out_file_dev_stderr(env):
    _check_closed_fd_path_rejected(env, 2, '--json-out-file', '/dev/stderr')


def test_stderr_closed_out_file_dev_fd_2(env):
    _check_closed_fd_path_rejected(env, 2, '--out-file', '/dev/fd/2')


def test_stderr_closed_failed_keys_file_dev_stderr(env):
    _check_closed_fd_path_rejected(env, 2, '--failed-keys-file', '/dev/stderr')


def test_stdin_closed_data_import_dev_stdin(env):
    """Input paths too: with stdin closed, --data-import /dev/stdin used to
    fail with "cannot open"; after the reopen it would read an empty
    /dev/null."""
    _check_closed_fd_path_rejected(env, 0, '--data-import', '/dev/stdin')


def test_stdin_closed_monitor_input_dev_stdin(env):
    _check_closed_fd_path_rejected(env, 0, '--monitor-input', '/dev/stdin')


def test_stdin_closed_cacert_dev_stdin(env):
    help_out = subprocess.run([MEMTIER_BINARY, '--help'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout
    if b'--cacert' not in help_out:
        env.skip()  # built without TLS
    _check_closed_fd_path_rejected(env, 0, '--cacert', '/dev/stdin')


def test_stdin_closed_out_file_dev_stdin(env):
    _check_closed_fd_path_rejected(env, 0, '--out-file', '/dev/stdin')


def test_stdout_closed_failed_keys_file_does_not_take_fd_1(env):
    """The reopen must run before any file is opened. If the failed-keys log
    were opened first, it would get fd 1: memtier would not see a closed
    stdout, the text results would go into the log and the run would exit 0."""
    with _benchmark(env, ['--hide-histogram']) as (benchmark, config, json_path):
        fk_path = os.path.join(config.results_dir, 'failed-keys.txt')
        benchmark.args += ['--failed-keys-file', fk_path]
        rc, _, stderr = _run_with_fd_closed(benchmark, 1)
        env.assertEqual(rc, 1, message=stderr)
        env.assertContains(STDOUT_CLOSED_ERROR, stderr)
        _assert_stopped_at_startup(env, json_path, stderr)
        if os.path.exists(fk_path):
            with open(fk_path) as f:
                env.assertFalse("ALL STATS" in f.read())


def test_stdout_closed_bad_option_reports_its_own_error(env):
    """The closed-stdout check comes after the option checks in main(), so a
    bad option still reports its own error."""
    args = ['--hide-histogram', '--thread-conn-start-min-jitter-micros', '10',
            '--thread-conn-start-max-jitter-micros', '5']
    with _benchmark(env, args) as (benchmark, _, json_path):
        rc, _, stderr = _run_with_fd_closed(benchmark, 1)
        env.assertEqual(rc, 1, message=stderr)
        env.assertContains("error: thread-conn-start-min-jitter-micros (10) cannot be greater than", stderr)
        env.assertFalse(STDOUT_CLOSED_ERROR in stderr, message=stderr)
        _assert_stopped_at_startup(env, json_path, stderr)
