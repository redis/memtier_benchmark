"""
--data-import must fail fast on a malformed file (issue #766).

A line that read_item() could not parse used to make the key-loading loop
spin forever at 100% CPU, printing the same error hundreds of MB at a time.
Other bad lines crashed instead: SIGSEGV on a truncated key, a heap overflow
on nbytes < 2, and SIGABRT in read_next_item() after connecting for files
that loaded "0 keys" or were only read at run time (--generate-keys).

Each case must now print its error once and exit 1 at startup, before any
connection. The bad-input cases need no server: memtier is pointed at a
closed port and must stop before it connects. Every run has a short timeout,
so a regression shows up as a failed test and not as a stuck CI job.

Run with:
  TEST=test_data_import_parse_errors.py OSS_STANDALONE=1 ./tests/run_tests.sh
"""
import os
import shutil
import subprocess
import tempfile
import threading

from include import *
from mb import Benchmark, RunConfig

HEADER = "dumpflags, time, exptime, nbytes, nsuffix, it_flags, clsid, nkey, key, data\n"
RUN_TIMEOUT = 10
# Keep only the start of stderr: a hung build writes ~20 MB/s.
STDERR_CAP = 64 * 1024


def _write_import_file(content):
    fd, path = tempfile.mkstemp(prefix="memtier-import-", suffix=".txt")
    with os.fdopen(fd, "w", newline="") as f:
        f.write(content)
    return path


def _run_memtier(args):
    """Run memtier with a hard timeout and a bounded stderr capture.

    Returns (returncode, stderr, timed_out). returncode is None on timeout.
    """
    proc = subprocess.Popen([MEMTIER_BINARY] + list(args), stdin=subprocess.DEVNULL,
                            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    captured = bytearray()

    def drain():
        while True:
            chunk = proc.stderr.read(65536)
            if not chunk:
                break
            if len(captured) < STDERR_CAP:
                captured.extend(chunk[:STDERR_CAP - len(captured)])

    reader = threading.Thread(target=drain)
    reader.start()
    timed_out = False
    try:
        proc.wait(timeout=RUN_TIMEOUT)
    except subprocess.TimeoutExpired:
        timed_out = True
        # A single SIGTERM is not enough for the old hang (the first signal
        # only requests a graceful stop), so kill outright.
        proc.kill()
        proc.wait()
    reader.join()
    proc.stderr.close()
    return (None if timed_out else proc.returncode), captured.decode(errors="replace"), timed_out


def _assert_fails_fast(env, label, body, needle, extra_args=()):
    path = _write_import_file(HEADER + body)
    try:
        # Port 1: nothing listens there. A build that got past startup would
        # print "Launching threads" (and then fail to connect).
        args = ["--server=127.0.0.1", "--port=1", "--data-import=" + path,
                "-t", "1", "-c", "1", "--requests=10"] + list(extra_args)
        rc, stderr, timed_out = _run_memtier(args)
    finally:
        os.unlink(path)
    env.assertFalse(timed_out,
                    message="{}: memtier hung (killed after {}s), stderr starts {!r}".format(
                        label, RUN_TIMEOUT, stderr[:500]))
    if timed_out:
        return
    env.assertEqual(rc, 1, message="{}: expected exit 1, got {} (stderr={!r})".format(label, rc, stderr[:2000]))
    env.assertContains(needle, stderr, message="{}: stderr={!r}".format(label, stderr[:2000]))
    env.assertEqual(stderr.count(needle), 1,
                    message="{}: error printed {} times".format(label, stderr.count(needle)))
    env.assertNotContains("Launching threads", stderr,
                          message="{}: did not fail at startup: {!r}".format(label, stderr[:2000]))


# (label, file body after the header, expected error text). The line number
# is part of the text so the report points at the bad line.
BAD_FILES = [
    # The reported repro: nbytes 3 for a 3-byte value desynchronizes the
    # line, and the next fscanf() fails without consuming anything.
    ("desync", '0, 0, 0, 3, 0, 0, 0, 3, "k01", "abc"\n', ":3: error parsing item values."),
    ("missing field", '0, 0, 0, 5, 0, 0, 3, "k01", "abc"\n', ":2: error parsing item values."),
    ("junk line", '0, 0, 0, 5, 0, 0, 0, 3, "k01", "abc"\nxx\n', ":3: error parsing item values."),
    ("truncated numeric fields", '0, 0, 0, 5, 0, 0, 0, 3, "k01", "abc"\n0, 0, 0', ":3: error parsing item values."),
    ("truncated key", '0, 0, 0, 5, 0, 0, 0, 3, "k0', ":2: premature end of file."),
    ("truncated value", '0, 0, 0, 5, 0, 0, 0, 3, "k01", "ab', ":2: premature end of file."),
    ("no data column", '0, 0, 0, 5, 0, 0, 0, 3, "k01"\n', ":2: error parsing csv file"),
    ("short key", '0, 0, 0, 5, 0, 0, 0, 3, k1, "abc"\n', ":2: error: key column is 2 bytes, expected 3 bytes."),
    ("short data", '0, 0, 0, 9, 0, 0, 0, 3, "k01", "abc"\n0, 0, 0, 5, 0, 0, 0, 3, "k02", "def"\n',
     ":2: error: data column is"),
    ("nbytes 1", '0, 0, 0, 1, 0, 0, 0, 3, "k01", "abc"\n', ":2: error: invalid nbytes 1, must be at least 2."),
    ("nbytes 0", '0, 0, 0, 0, 0, 0, 0, 3, "k01", "abc"\n', ":2: error: invalid nbytes 0, must be at least 2."),
    ("CRLF then junk", '0, 0, 0, 5, 0, 0, 0, 3, "k01", "abc"\r\nxx\r\n', ":3: error parsing item values."),
    ("header only", "", ": no items found."),
]


def test_data_import_malformed_fails_fast(env):
    for label, body, needle in BAD_FILES:
        _assert_fails_fast(env, label, body, needle)


def test_data_import_malformed_verify_only_fails_fast(env):
    for label, body, needle in BAD_FILES:
        _assert_fails_fast(env, label + " (--verify-only)", body, needle,
                           ["--data-verify", "--verify-only"])


def test_data_import_malformed_generate_keys_fails_fast(env):
    """With --generate-keys the file was only read by the worker threads,
    so a bad item aborted the run after it had connected."""
    for label, body, needle in BAD_FILES:
        _assert_fails_fast(env, label + " (--generate-keys)", body, needle, ["--generate-keys"])


def test_data_import_valid_crlf_quoted_no_final_newline(env):
    """Valid files still load: CRLF and LF endings, quoted values with
    embedded quotes, commas and line breaks, and no newline after the
    last item."""
    env.skipOnCluster()
    body = ('0, 0, 0, 5, 0, 0, 0, 4, "key1", "abc"\r\n'
            '0, 0, 0, 9, 0, 0, 0, 4, "key2", "a""b,c\nd"\n'
            '0, 0, 0, 7, 0, 0, 0, 4, key3, xxxxx')
    path = _write_import_file(HEADER + body)
    test_dir = tempfile.mkdtemp()
    try:
        benchmark_specs = {"name": env.testName, "args": ["--data-import=" + path, "--ratio=1:0"]}
        config = get_default_memtier_config(threads=1, clients=1, requests=3)
        master_nodes_list = env.getMasterNodesList()
        add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
        config = RunConfig(test_dir, env.testName, config, {})
        ensure_clean_benchmark_folder(config.results_dir)
        benchmark = Benchmark.from_json(config, benchmark_specs)

        conn = env.getConnection()
        conn.flushall()
        memtier_ok = benchmark.run()
        if not memtier_ok:
            debugPrintMemtierOnError(config, env)
        env.assertTrue(memtier_ok)
        for k, expected in (("key1", "abc"), ("key2", 'a"b,c\nd'), ("key3", "xxxxx")):
            value = conn.get(k)
            if isinstance(value, bytes):
                value = value.decode()
            env.assertEqual(value, expected, message=k)
    finally:
        os.unlink(path)
        shutil.rmtree(test_dir, ignore_errors=True)
