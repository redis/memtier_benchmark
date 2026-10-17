"""
--data-import must fail fast on a malformed file (issue #766).

A line that read_item() could not parse used to make the key-loading loop
spin forever at 100% CPU, printing the same error hundreds of MB at a time.
Other bad lines crashed instead: SIGSEGV on a truncated key, heap overflows
on nbytes < 2 or a huge nkey, and SIGABRT after connecting for files that
loaded only part of their items, or that were only read at run time
(--generate-keys).

Each bad item must now print its error once and exit 1 at startup, before
any connection, whether it is the first item or sits between valid ones.
A truncated LAST item after complete ones is still treated as the end of the
file, as before (e.g. a size-capped dump). The bad-input cases need no
server: memtier is pointed at a closed port and must stop before it
connects. Every run has a short timeout, so a regression fails the test
instead of stalling CI.

Run with:
  TEST=test_data_import_parse_errors.py OSS_STANDALONE=1 ./tests/run_tests.sh
"""
import json
import os
import shutil
import subprocess
import tempfile
import threading

from include import *
from mb import Benchmark, RunConfig

HEADER = "dumpflags, time, exptime, nbytes, nsuffix, it_flags, clsid, nkey, key, data\n"
GOOD_FIRST = '0, 0, 0, 5, 0, 0, 0, 3, "g01", "abc"\n'
GOOD_LAST = '0, 0, 0, 5, 0, 0, 0, 3, "g99", "xyz"\n'
RUN_TIMEOUT = 5
# Keep only the start of stderr: a hung build writes ~20 MB/s.
STDERR_CAP = 64 * 1024
# Sanitizers exit 1 by default, like a clean input error. Give them a
# distinct code so a sanitizer report cannot pass as "exit 1".
SANITIZER_EXIT = 86
SANITIZER_MARKERS = ("AddressSanitizer", "LeakSanitizer", "runtime error:", "MEMTIER_BENCHMARK BUG REPORT")


def _write_import_file(content):
    fd, path = tempfile.mkstemp(prefix="memtier-import-", suffix=".txt")
    with os.fdopen(fd, "w", newline="") as f:
        f.write(content)
    return path


def _run_memtier(args):
    """Run memtier with a hard timeout and a bounded stderr capture.

    Returns (returncode, stderr, timed_out). returncode is None on timeout.
    """
    env = dict(os.environ)
    for var in ("ASAN_OPTIONS", "UBSAN_OPTIONS", "LSAN_OPTIONS"):
        env[var] = (env.get(var, "") + ":exitcode={}".format(SANITIZER_EXIT)).lstrip(":")
    proc = subprocess.Popen([MEMTIER_BINARY] + list(args), stdin=subprocess.DEVNULL,
                            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=env)
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


def _import_args(env, path, extra_args=()):
    """memtier args for an import run that must stop before connecting.

    The arguments go through the same TLS helper as the other tests, but the
    target is port 15299, where nothing listens: a build that got past startup
    prints "Launching threads" and then fails to connect. --cluster-mode is
    left out on purpose, since memtier rejects it with --data-import before
    reading the file.
    """
    specs = {"args": ["--server=127.0.0.1", "--port=15299", "--data-import=" + path,
                      "-t", "1", "-c", "1", "--requests=10"] + list(extra_args)}
    addTLSArgs(specs, env)
    return specs["args"]


def _assert_no_sanitizer_report(env, label, rc, stderr):
    env.assertNotEqual(rc, SANITIZER_EXIT, message="{}: sanitizer exit: {!r}".format(label, stderr[:600]))
    for marker in SANITIZER_MARKERS:
        env.assertNotContains(marker, stderr, message="{}: {!r}".format(label, stderr[:600]))


def _run_case(env, label, content, extra_args):
    path = _write_import_file(content)
    try:
        rc, stderr, timed_out = _run_memtier(_import_args(env, path, extra_args))
    finally:
        os.unlink(path)
    env.assertFalse(timed_out,
                    message="{}: memtier hung (killed after {}s), stderr starts {!r}".format(
                        label, RUN_TIMEOUT, stderr[:500]))
    return rc, stderr, timed_out


def _assert_fails_fast(env, label, content, needle, extra_args=()):
    rc, stderr, timed_out = _run_case(env, label, content, extra_args)
    if timed_out:
        return
    _assert_no_sanitizer_report(env, label, rc, stderr)
    env.assertEqual(rc, 1, message="{}: expected exit 1, got {} (stderr={!r})".format(label, rc, stderr[:600]))
    env.assertContains(needle, stderr, message="{}: stderr={!r}".format(label, stderr[:600]))
    env.assertEqual(stderr.count(needle), 1,
                    message="{}: error printed {} times".format(label, stderr.count(needle)))
    env.assertNotContains("Launching threads", stderr,
                          message="{}: did not fail at startup: {!r}".format(label, stderr[:600]))
    env.assertNotContains("keys read", stderr,
                          message="{}: loaded keys despite the error: {!r}".format(label, stderr[:600]))


# (label, bad item, expected error text). Each item is tested alone (line 2)
# and between two valid items (line 3), so the error flag itself is checked
# and not only the "no items found" fallback. "{line}" is the item's line.
BAD_ITEMS = [
    # The reported repro: nbytes 3 for a 3-byte value desynchronizes the
    # line, and the next fscanf() fails without consuming anything.
    ("desync", '0, 0, 0, 3, 0, 0, 0, 3, "k01", "abc"\n', None),
    ("missing field", '0, 0, 0, 5, 0, 0, 3, "k01", "abc"\n', ":{line}: error parsing item values."),
    ("junk line", 'xx\n', ":{line}: error parsing item values."),
    ("junk line CRLF", 'xx\r\n', ":{line}: error parsing item values."),
    ("no data column", '0, 0, 0, 5, 0, 0, 0, 3, "k01"\n', ":{line}: error parsing csv file"),
    ("short key", '0, 0, 0, 5, 0, 0, 0, 3, k1, "abc"\n', ":{line}: error: key column is 2 bytes, expected 3 bytes."),
    ("short data", '0, 0, 0, 9, 0, 0, 0, 3, "k01", "abc"\n', ":{line}: error: data column is"),
    ("nbytes 0", '0, 0, 0, 0, 0, 0, 0, 3, "k01", "abc"\n', ":{line}: error: empty keys/values are not supported"),
    ("nbytes 1", '0, 0, 0, 1, 0, 0, 0, 3, "k01", "abc"\n', ":{line}: error: empty keys/values are not supported"),
    ("empty value", '0, 0, 0, 2, 0, 0, 0, 3, "k01", ""\n', ":{line}: error: empty keys/values are not supported"),
    ("empty key", '0, 0, 0, 5, 0, 0, 0, 0, "", "abc"\n', ":{line}: error: empty keys/values are not supported"),
    ("huge nkey", '0, 0, 0, 5, 0, 0, 0, 4294967295, "k01", "abc"\n', ":{line}: error: key or value too large"),
    ("nkey -1", '0, 0, 0, 5, 0, 0, 0, -1, "k01", "abc"\n', ":{line}: error: key or value too large"),
    ("huge nbytes", '0, 0, 0, 4000000000, 0, 0, 0, 3, "k01", "abc"\n', ":{line}: error: key or value too large"),
    ("wrapped nkey", '0, 0, 0, 5, 0, 0, 0, 4294967299, "k01", "abc"\n', ":{line}: error: key or value too large"),
    ("wrapped nbytes", '0, 0, 0, 4294967301, 0, 0, 0, 3, "k01", "abc"\n', ":{line}: error: key or value too large"),
    ("overflow nbytes", '0, 0, 0, 18446744073709551621, 0, 0, 0, 3, "k01", "abc"\n', ":{line}: error: key or value too large"),

]

# A truncated item at the end of the file: an error when it is the only
# item, the end of the file after complete items (master's behaviour).
TRUNCATED_ITEMS = [
    ("truncated numeric fields", '0, 0, 0', ":{line}: error parsing item values."),
    ("truncated key", '0, 0, 0, 5, 0, 0, 0, 3, "k0', ":{line}: premature end of file."),
    ("truncated value", '0, 0, 0, 5, 0, 0, 0, 3, "k01", "ab', ":{line}: premature end of file."),
]

MODES = [
    ("", []),
    (" (--verify-only)", ["--data-verify", "--verify-only"]),
    (" (--generate-keys)", ["--generate-keys"]),
]


def _bad_item_cases():
    for label, item, needle in BAD_ITEMS:
        # The desync item fails while parsing the line after it.
        alone = needle or ":3: error parsing item values."
        between = needle or ":4: error parsing item values."
        yield label, HEADER + item, alone.format(line=2)
        yield label + " between valid items", HEADER + GOOD_FIRST + item + GOOD_LAST, between.format(line=3)


def test_data_import_malformed_fails_fast(env):
    for suffix, extra in MODES:
        for label, content, needle in _bad_item_cases():
            _assert_fails_fast(env, label + suffix, content, needle, extra)


def test_data_import_truncated_only_item_fails_fast(env):
    for suffix, extra in MODES:
        for label, item, needle in TRUNCATED_ITEMS:
            _assert_fails_fast(env, label + suffix, HEADER + item, needle.format(line=2), extra)
        _assert_fails_fast(env, "header only" + suffix, HEADER, ": no items found.", extra)


def test_data_import_truncated_last_item_is_end_of_file(env):
    """A truncated last item is ignored on both startup and wrap-around."""
    env.skipOnCluster()
    for label, item, _ in TRUNCATED_ITEMS:
        for extra in ([], ["--generate-keys"]):
            env.getConnection().flushall()
            ok, stderr = _run_valid_import(env, GOOD_FIRST + item, requests=10, extra_args=extra)
            env.assertTrue(ok, message="{} {}: {!r}".format(label, extra, stderr[:600]))
            env.assertContains(":3: warning: premature end of file, ignoring the truncated last item.", stderr)
            if extra:
                env.assertTrue(env.getConnection().dbsize() > 0)
            else:
                _assert_values(env, [("g01", "abc")])


def _run_valid_import(env, body, requests, extra_args=()):
    """Load *body* through the shared benchmark helpers and return
    (memtier_ok, stderr)."""
    path = _write_import_file(HEADER + body)
    test_dir = tempfile.mkdtemp()
    try:
        benchmark_specs = {"name": env.testName, "args": ["--data-import=" + path, "--ratio=1:0"] + list(extra_args)}
        addTLSArgs(benchmark_specs, env)
        config = get_default_memtier_config(threads=1, clients=1, requests=requests)
        master_nodes_list = env.getMasterNodesList()
        add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
        config = RunConfig(test_dir, env.testName, config, {})
        ensure_clean_benchmark_folder(config.results_dir)
        benchmark = Benchmark.from_json(config, benchmark_specs)
        memtier_ok = benchmark.run(timeout=15)
        if memtier_ok:
            with open(os.path.join(config.results_dir, "mb.json")) as f:
                stats = json.load(f)["ALL STATS"]
            env.assertEqual(stats["Sets"]["Count"], requests)
            env.assertEqual(stats["Totals"]["Count"], requests)
        if not memtier_ok:
            debugPrintMemtierOnError(config, env)
        with open(os.path.join(config.results_dir, "mb.stderr"), errors="replace") as f:
            # Only the reader's own messages: they start with the file name.
            stderr = "\n".join(line for line in f.read().splitlines() if os.path.basename(path) + ":" in line)
        return memtier_ok, stderr
    finally:
        os.unlink(path)
        shutil.rmtree(test_dir, ignore_errors=True)


def _assert_values(env, expected):
    conn = env.getConnection()
    for k, want in expected:
        value = conn.get(k)
        if isinstance(value, bytes):
            value = value.decode()
        env.assertEqual(value, want, message=k)


VALID_ITEMS = [("key1", "abc"), ("key2", 'a"b,c\nd'), ("key3", "xxxxx")]


def test_data_import_valid_crlf_quoted(env):
    """Valid files still load: CRLF and LF endings, quoted values with
    embedded quotes, commas and line breaks. requests > items, so the reader
    wraps around and reopens the file. No warnings are printed."""
    env.skipOnCluster()
    body = ('0, 0, 0, 5, 0, 0, 0, 4, "key1", "abc"\r\n'
            '0, 0, 0, 9, 0, 0, 0, 4, "key2", "a""b,c\nd"\n'
            '0, 0, 0, 7, 0, 0, 0, 4, key3, xxxxx\r\n')
    for extra in ([], ["--generate-keys"]):
        env.getConnection().flushall()
        memtier_ok, stderr = _run_valid_import(env, body, requests=10, extra_args=extra)
        env.assertTrue(memtier_ok, message="{}: {!r}".format(extra, stderr[:600]))
        env.assertEqual(stderr, "", message="{}: reader messages for a valid file".format(extra))
        if extra:
            env.assertTrue(env.getConnection().dbsize() > 0)
        else:
            _assert_values(env, VALID_ITEMS)


def test_data_import_valid_no_final_newline(env):
    """No newline after the last item: loads, with the existing
    "end of line expected" warning and nothing else."""
    env.skipOnCluster()
    body = ('0, 0, 0, 5, 0, 0, 0, 4, "key1", "abc"\n'
            '0, 0, 0, 9, 0, 0, 0, 4, "key2", "a""b,c\nd"\n'
            '0, 0, 0, 7, 0, 0, 0, 4, key3, xxxxx')
    env.getConnection().flushall()
    memtier_ok, stderr = _run_valid_import(env, body, requests=10)
    env.assertTrue(memtier_ok, message=stderr[:600])
    warnings = stderr.splitlines()
    env.assertTrue(len(warnings) > 0, message=stderr[:600])
    for line in warnings:
        # m_line restarts on every reopen, so the line number stays at 4.
        env.assertTrue(line.endswith(":4: warning: end of line expected but not found."), message=line)
    _assert_values(env, VALID_ITEMS)
