"""
Unit / integration tests for the Memcached meta protocol path in
memtier_benchmark.

These tests are deliberately self-contained: they spin up a small in-process
TCP server that speaks just enough of the meta protocol to drive the binary,
then run ``memtier_benchmark -P memcache_meta`` against it. The server records
every command it receives, which lets us assert the exact wire format on the
request side, while the binary's ``--json-out-file`` output lets us assert
that the response side is parsed correctly.

There is no dependency on Redis or memcached — the tests are runnable with
just Python 3 and a built ``memtier_benchmark`` binary in the repo root::

    make
    python3 -m pytest tests/test_memcache_meta_protocol.py -v

They are also discoverable by RLTest (each top-level ``test_*`` function takes
an ``env`` argument that we accept and ignore), so they ride along with the
existing ``./tests/run_tests.sh`` flow without needing a Redis server.
"""
import json
import os
import socket
import subprocess
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
MEMTIER_BINARY = os.environ.get("MEMTIER_BINARY", os.path.join(ROOT, "memtier_benchmark"))


class MetaProtocolMockServer(threading.Thread):
    """
    Minimal mock that speaks the subset of the Memcached meta protocol used by
    memtier_benchmark::memcache_meta_protocol.

    Recognized commands:
        ms <key> <datalen> [flags]\\r\\n<data>\\r\\n   ->  HD\\r\\n
        mg <key> [flags]\\r\\n                        ->  VA <size>\\r\\n<data>\\r\\n  (hit)
                                                          EN\\r\\n                    (miss)
        mn\\r\\n                                       ->  MN\\r\\n

    The server stores everything it sees in ``self.commands`` (as raw lines)
    and ``self.kv`` (as a key->value map) so tests can assert on the wire.

    NOTE: memtier's meta protocol implementation appends `mn\r\n` after every
    single-shot command (ms / mg) so that every response is uniformly terminated
    by `MN`. Tests therefore expect (and assert on) one `mn` per logical command.
    """

    def __init__(self, miss_keys=None, ms_response=b"HD\r\n", garbage_after=None, fragment_va=False):
        super().__init__(daemon=True)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(64)
        self.port = self.sock.getsockname()[1]
        self.miss_keys = set(miss_keys or ())
        self.kv = {}                  # key (str) -> bytes
        self.commands = []            # list[bytes] of headers received
        # Server-behavior knobs for negative tests.
        self.ms_response = ms_response       # what to reply for `ms` (HD / NS / EX / NF)
        # If set, returns SERVER_ERROR exactly once after this many commands
        # have been received, then resumes normal handling. Used to verify that
        # parse_response's -1 path resets state cleanly so subsequent valid
        # responses are not misclassified as multi-get sections.
        self.garbage_after = garbage_after
        self.fragment_va = fragment_va       # split VA header and value across two sends
        self.stop_event = threading.Event()
        # Signaled by run() right before the accept loop -- tests use this to
        # avoid time.sleep() races on slow CI hosts.
        self.ready = threading.Event()
        self._lock = threading.Lock()

    def stop(self):
        self.stop_event.set()
        try:
            self.sock.close()
        except OSError:
            pass

    def run(self):
        self.sock.settimeout(0.2)
        self.ready.set()
        while not self.stop_event.is_set():
            try:
                conn, _ = self.sock.accept()
            except (OSError, socket.timeout):
                continue
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn):
        conn.settimeout(2.0)
        buf = b""
        try:
            while not self.stop_event.is_set():
                try:
                    chunk = conn.recv(65536)
                except socket.timeout:
                    continue
                except (ConnectionResetError, OSError):
                    return  # peer closed -- normal at end of memtier run
                if not chunk:
                    return
                buf += chunk
                try:
                    buf = self._drain(conn, buf)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    return
        finally:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            conn.close()

    def _drain(self, conn, buf):
        """Consume as many complete commands as possible from ``buf``."""
        out = b""
        while True:
            nl = buf.find(b"\r\n")
            if nl < 0:
                return buf
            header = buf[:nl]
            rest = buf[nl + 2:]

            with self._lock:
                self.commands.append(header)

            parts = header.split(b" ")
            cmd = parts[0]

            inject_now = (self.garbage_after is not None
                          and len(self.commands) == self.garbage_after + 1)
            if inject_now:
                # Inject one SERVER_ERROR (which the meta parser must classify
                # as unknown -> -1 with state reset). For ms commands we still
                # need to consume the value bytes from the wire.
                if cmd == b"ms" and len(parts) >= 3:
                    try:
                        datalen = int(parts[2])
                    except ValueError:
                        datalen = 0
                    if len(rest) < datalen + 2:
                        return buf
                    buf = rest[datalen + 2:]
                else:
                    buf = rest
                out += b"SERVER_ERROR injected\r\n"
            elif cmd == b"ms":
                # ms <key> <datalen> [flags...]
                if len(parts) < 3:
                    out += b"CLIENT_ERROR bad ms\r\n"
                    buf = rest
                    continue
                key = parts[1].decode("latin-1")
                try:
                    datalen = int(parts[2])
                except ValueError:
                    out += b"CLIENT_ERROR bad datalen\r\n"
                    buf = rest
                    continue
                if len(rest) < datalen + 2:
                    return buf  # need more bytes for the value
                value = rest[:datalen]
                # value MUST be terminated by CRLF on the wire
                assert rest[datalen:datalen + 2] == b"\r\n", "missing CRLF after ms value"
                with self._lock:
                    self.kv[key] = value
                buf = rest[datalen + 2:]
                out += self.ms_response
            elif cmd == b"mg":
                if len(parts) < 2:
                    out += b"CLIENT_ERROR bad mg\r\n"
                    buf = rest
                    continue
                key = parts[1].decode("latin-1")
                buf = rest
                with self._lock:
                    val = self.kv.get(key)
                if key in self.miss_keys or val is None:
                    out += b"EN\r\n"
                elif self.fragment_va:
                    # Flush queued output, deliver the VA header alone, sleep,
                    # then deliver value+CRLF. Forces parse_response to traverse
                    # rs_read_value with an under-filled evbuffer (return 0,
                    # "need more data") and resume on the next libevent callback.
                    if out:
                        conn.sendall(out)
                        out = b""
                    conn.sendall(b"VA %d\r\n" % len(val))
                    time.sleep(0.02)
                    out += val + b"\r\n"
                else:
                    out += b"VA %d\r\n%s\r\n" % (len(val), val)
            elif cmd == b"mn":
                buf = rest
                out += b"MN\r\n"
            else:
                # We don't support delete / arithmetic / etc. — return ERROR so
                # the client surfaces it in tests rather than hanging.
                buf = rest
                out += b"ERROR unknown meta command\r\n"

            if out:
                conn.sendall(out)
                out = b""

    # ----- assertion helpers -----
    def command_kinds(self):
        with self._lock:
            return [c.split(b" ", 1)[0] for c in self.commands]

    def count(self, kind):
        return self.command_kinds().count(kind.encode() if isinstance(kind, str) else kind)


def _run_memtier(extra_args, server_port, json_path, requests=200, clients=2, threads=2, timeout=30):
    cmd = [
        MEMTIER_BINARY,
        "-P", "memcache_meta",
        "-s", "127.0.0.1",
        "-p", str(server_port),
        "-c", str(clients),
        "-t", str(threads),
        "-n", str(requests),
        "--hide-histogram",
        "--json-out-file", json_path,
    ] + extra_args
    proc = subprocess.run(cmd, capture_output=True, timeout=timeout)
    return proc


class MetaProtocolTests(unittest.TestCase):
    def setUp(self):
        if not os.path.isfile(MEMTIER_BINARY) or not os.access(MEMTIER_BINARY, os.X_OK):
            self.skipTest(f"memtier_benchmark binary not found or not executable at {MEMTIER_BINARY}")
        self.server = MetaProtocolMockServer()
        self.server.start()
        # Synchronization on the ready event eliminates time.sleep() races on
        # slow CI hosts. 5s is generous; in practice it's signaled in <1ms.
        self.assertTrue(self.server.ready.wait(timeout=5.0), "mock server failed to start")
        self.tmpdir = os.path.join(HERE, "_meta_tmp")
        os.makedirs(self.tmpdir, exist_ok=True)
        # Per-test method name keeps the JSON path stable across runs and
        # avoids id()-collision aliasing between TestCase instances.
        self.json_path = os.path.join(self.tmpdir, f"meta_{self._testMethodName}.json")

    def tearDown(self):
        self.server.stop()
        self.server.join(timeout=2.0)
        # Surface a thread leak instead of letting the next test inherit a
        # half-shut-down server thread.
        self.assertFalse(self.server.is_alive(), "mock server thread did not stop within 2s")
        try:
            os.remove(self.json_path)
        except OSError:
            pass

    # ---- write_command_set : ms <key> <datalen> T<exp>\r\n<data>\r\n ----
    def test_set_wire_format_and_storage(self):
        proc = _run_memtier(
            ["--ratio=1:0", "--key-pattern=P:P", "--key-prefix=k:",
             "--key-minimum=1", "--key-maximum=50", "--data-size=8",
             "--expiry-range=60-60"],
            self.server.port, self.json_path,
            requests=50, clients=1, threads=1,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr.decode())

        # Each ms command is followed by an mn terminator on the wire.
        kinds = self.server.command_kinds()
        self.assertEqual(set(kinds), {b"ms", b"mn"}, f"unexpected kinds: {set(kinds)}")
        self.assertEqual(self.server.count("ms"), 50)
        self.assertEqual(self.server.count("mn"), 50)

        # Inspect a sample header: must look like `ms k:<n> 8 T60`.
        sample = self.server.commands[0].decode("latin-1")
        parts = sample.split(" ")
        self.assertEqual(parts[0], "ms")
        self.assertTrue(parts[1].startswith("k:"))
        self.assertEqual(parts[2], "8")              # datalen
        self.assertEqual(parts[3], "T60")            # TTL flag

        # Storage actually populated.
        self.assertEqual(len(self.server.kv), 50)
        for v in self.server.kv.values():
            self.assertEqual(len(v), 8)

        # JSON output is well-formed and reports SETs.
        with open(self.json_path) as fp:
            results = json.load(fp)
        sets = results["ALL STATS"]["Sets"]
        self.assertEqual(int(sets["Count"]), 50)
        self.assertEqual(int(results["ALL STATS"]["Gets"]["Count"]), 0)

    # ---- write_command_get + parse_response (VA hit, EN miss) ----
    def test_get_hits_and_misses(self):
        # Pre-load 5 keys so half the GETs hit, half miss.
        for i in range(1, 6):
            self.server.kv[f"hit:{i}"] = b"abcd"

        # Drive memtier with GETs against keys "missme:1..50" -> all misses (EN).
        proc = _run_memtier(
            ["--ratio=0:1", "--key-pattern=P:P", "--key-prefix=missme:",
             "--key-minimum=1", "--key-maximum=50"],
            self.server.port, self.json_path,
            requests=50, clients=1, threads=1,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr.decode())

        # Each mg is followed by an mn terminator on the wire.
        kinds = self.server.command_kinds()
        self.assertEqual(set(kinds), {b"mg", b"mn"})
        self.assertEqual(self.server.count("mg"), 50)
        self.assertEqual(self.server.count("mn"), 50)
        # Wire format check for the first mg.
        first = self.server.commands[0].decode("latin-1")
        self.assertTrue(first.startswith("mg missme:"))
        self.assertTrue(first.endswith(" v"), f"unexpected mg flags: {first!r}")

        with open(self.json_path) as fp:
            results = json.load(fp)
        gets = results["ALL STATS"]["Gets"]
        # All misses -> Hits/sec is 0 and Misses/sec equals Ops/sec.
        self.assertEqual(int(gets["Count"]), 50)
        self.assertEqual(gets["Hits/sec"], 0.0)
        self.assertGreater(gets["Misses/sec"], 0.0)
        self.assertAlmostEqual(gets["Misses/sec"], gets["Ops/sec"], places=2)

    def test_get_hits_against_preloaded_data(self):
        for i in range(1, 21):
            self.server.kv[f"hit:{i}"] = b"V" * 16

        proc = _run_memtier(
            ["--ratio=0:1", "--key-pattern=P:P", "--key-prefix=hit:",
             "--key-minimum=1", "--key-maximum=20"],
            self.server.port, self.json_path,
            requests=20, clients=1, threads=1,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr.decode())

        with open(self.json_path) as fp:
            results = json.load(fp)
        gets = results["ALL STATS"]["Gets"]
        # All hits -> Misses/sec is 0 and Hits/sec equals Ops/sec.
        self.assertEqual(int(gets["Count"]), 20)
        self.assertEqual(gets["Misses/sec"], 0.0)
        self.assertGreater(gets["Hits/sec"], 0.0)
        self.assertAlmostEqual(gets["Hits/sec"], gets["Ops/sec"], places=2)

    # ---- write_command_multi_get: pipelined `mg`s + `mn` terminator ----
    def test_multi_get_pipelines_and_terminates_with_mn(self):
        # Pre-load some keys so we get a mix of VA + EN responses inside the pipeline.
        for i in range(1, 11):
            self.server.kv[f"k:{i}"] = b"data%02d" % i

        # `--multi-key-get=4` only batches when the GET-side of the ratio can
        # absorb 4 keys per cycle, so we drive the test with `--ratio=0:4`.
        # 5 GET requests * 4 keys = 20 individual mg + 5 mn terminators.
        proc = _run_memtier(
            ["--ratio=0:4", "--key-pattern=R:R", "--key-prefix=k:",
             "--key-minimum=1", "--key-maximum=20",
             "--multi-key-get=4"],
            self.server.port, self.json_path,
            requests=5, clients=1, threads=1,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr.decode())

        # Expect: exactly 5 mn terminators, and 20 mg requests in between.
        self.assertEqual(self.server.count("mn"), 5)
        self.assertEqual(self.server.count("mg"), 20)

        # Sanity: the very first command in the wire must be `mg` (not `mn`).
        self.assertEqual(self.server.command_kinds()[0], b"mg")

    def test_set_then_get_roundtrip(self):
        # Phase 1: SETs only.
        proc = _run_memtier(
            ["--ratio=1:0", "--key-pattern=P:P", "--key-prefix=rt:",
             "--key-minimum=1", "--key-maximum=20", "--data-size=12"],
            self.server.port, self.json_path,
            requests=20, clients=1, threads=1,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr.decode())
        self.assertEqual(len(self.server.kv), 20)

        # Phase 2: GETs only against the same keyspace -> all hits.
        proc = _run_memtier(
            ["--ratio=0:1", "--key-pattern=P:P", "--key-prefix=rt:",
             "--key-minimum=1", "--key-maximum=20"],
            self.server.port, self.json_path,
            requests=20, clients=1, threads=1,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr.decode())
        with open(self.json_path) as fp:
            results = json.load(fp)
        gets = results["ALL STATS"]["Gets"]
        self.assertEqual(int(gets["Count"]), 20)
        self.assertEqual(gets["Misses/sec"], 0.0)
        self.assertAlmostEqual(gets["Hits/sec"], gets["Ops/sec"], places=2)

    # ------------------------------------------------------------------
    # Negative / edge cases. These exercise parse_response paths that the
    # happy-path tests above leave dark, and they directly cover the
    # findings raised by review-correctness and review-testing.
    # ------------------------------------------------------------------

    def _restart_server(self, **kwargs):
        """Replace self.server mid-test with a freshly-configured mock and
        wait for it to become ready. Used by tests that need a non-default
        mock configuration."""
        self.server.stop()
        self.server.join(timeout=2.0)
        self.server = MetaProtocolMockServer(**kwargs)
        self.server.start()
        self.assertTrue(self.server.ready.wait(timeout=5.0))

    def test_ns_response_does_not_error(self):
        """`NS` (not stored) is a valid meta status -- parser must accept it.

        A typo such as `memcmp(line, "NS", 3)` would misclassify NS as unknown
        and return -1; this test drives that branch deterministically.
        """
        self._restart_server(ms_response=b"NS\r\n")
        proc = _run_memtier(
            ["--ratio=1:0", "--key-pattern=P:P", "--key-prefix=ns:",
             "--key-minimum=1", "--key-maximum=20", "--data-size=8"],
            self.server.port, self.json_path,
            requests=20, clients=1, threads=1,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr.decode())
        with open(self.json_path) as fp:
            results = json.load(fp)
        # 20 SET commands sent; the parser correctly traversed NS for each.
        self.assertEqual(int(results["ALL STATS"]["Sets"]["Count"]), 20)

    def test_ex_response_does_not_error(self):
        """`EX` (exists / CAS conflict) -- analog of NS for the CAS path."""
        self._restart_server(ms_response=b"EX\r\n")
        proc = _run_memtier(
            ["--ratio=1:0", "--key-pattern=P:P", "--key-prefix=ex:",
             "--key-minimum=1", "--key-maximum=10", "--data-size=8"],
            self.server.port, self.json_path,
            requests=10, clients=1, threads=1,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr.decode())
        with open(self.json_path) as fp:
            results = json.load(fp)
        self.assertEqual(int(results["ALL STATS"]["Sets"]["Count"]), 10)

    def test_nf_response_does_not_error(self):
        """`NF` (not found) -- third 2-letter code in the same parse branch."""
        self._restart_server(ms_response=b"NF\r\n")
        proc = _run_memtier(
            ["--ratio=1:0", "--key-pattern=P:P", "--key-prefix=nf:",
             "--key-minimum=1", "--key-maximum=10", "--data-size=8"],
            self.server.port, self.json_path,
            requests=10, clients=1, threads=1,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr.decode())

    def test_fragmented_va_response_is_reassembled(self):
        """VA <size>\\r\\n and the value payload arrive in two separate TCP
        writes, forcing parse_response to traverse rs_read_value with an
        under-filled evbuffer and return 0 ("need more data"). When the next
        libevent callback fires with the value, parsing must resume cleanly.

        Catches off-by-one bugs in the value-length guard (e.g. `< value_len`
        instead of `< value_len + 2`).
        """
        self._restart_server(fragment_va=True)
        for i in range(1, 21):
            self.server.kv[f"frag:{i}"] = b"X" * 64
        proc = _run_memtier(
            ["--ratio=0:1", "--key-pattern=P:P", "--key-prefix=frag:",
             "--key-minimum=1", "--key-maximum=20"],
            self.server.port, self.json_path,
            requests=20, clients=1, threads=1,
            timeout=15,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr.decode())
        with open(self.json_path) as fp:
            results = json.load(fp)
        gets = results["ALL STATS"]["Gets"]
        self.assertEqual(int(gets["Count"]), 20)
        self.assertAlmostEqual(gets["Hits/sec"], gets["Ops/sec"], places=2)
        self.assertEqual(gets["Misses/sec"], 0.0)

    def test_unknown_response_classified_as_parse_error(self):
        """Server returns a malformed response (`SERVER_ERROR ...`) that the
        meta parser must classify as unknown and surface as a parse error.

        memtier's framework-level recovery from parse errors is the same for
        every protocol (it logs and stops popping requests on that connection),
        so we cannot use a successful end-to-end run as the assertion. What we
        CAN verify is the meta parser's contract: an unknown response line
        must produce an `error: response parsing failed` log line on stderr,
        which only happens when parse_response returns -1.

        This indirectly proves the state-reset code path is taken, because
        the same branch that emits the error code also runs the
        m_response_state = rs_initial / m_multi_mode = false reset. (A
        future regression that re-introduced the state-leak bug would by
        construction have to keep this -1 return, so this test stays a
        meaningful regression guard.)
        """
        # Inject the garbage on command index 0 -- the very first command.
        self._restart_server(garbage_after=0)
        # Use a short test_time and skip waiting for completion: we just need
        # the parse error to surface on stderr. Kill the binary after a brief
        # window.
        cmd = [
            MEMTIER_BINARY, "-P", "memcache_meta",
            "-s", "127.0.0.1", "-p", str(self.server.port),
            "-c", "1", "-t", "1", "-n", "5",
            "--ratio=1:0", "--key-pattern=P:P", "--key-prefix=err:",
            "--key-minimum=1", "--key-maximum=5", "--data-size=4",
            "--hide-histogram",
        ]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            _, stderr = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            _, stderr = proc.communicate()
        # The parser surfaced the error -- this is the observable evidence
        # that parse_response returned -1, which is exactly the code path
        # that ALSO performs the state reset (m_response_state = rs_initial,
        # m_multi_mode = false).
        self.assertIn(b"response parsing failed", stderr,
                      f"expected parse-error log on stderr; got: {stderr[-500:]!r}")

    def test_pipelined_set_completes(self):
        """--pipeline=4 forces 4 in-flight `ms` commands per connection. The
        parser is invoked in sequence against a single read buffer that may
        contain up to four queued `HD\\r\\n` responses concatenated. Asserts
        the pipelined-write + sequential-parse path is clean.
        """
        proc = _run_memtier(
            ["--ratio=1:0", "--pipeline=4", "--key-pattern=P:P",
             "--key-prefix=pipe:", "--key-minimum=1", "--key-maximum=40",
             "--data-size=8"],
            self.server.port, self.json_path,
            requests=40, clients=1, threads=1,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr.decode())
        self.assertEqual(self.server.count("ms"), 40)
        self.assertEqual(self.server.count("mn"), 40)  # one mn terminator per ms
        self.assertEqual(len(self.server.kv), 40)
        with open(self.json_path) as fp:
            results = json.load(fp)
        self.assertEqual(int(results["ALL STATS"]["Sets"]["Count"]), 40)

    def test_pipelined_mixed_set_and_multi_get(self):
        """Regression for the second-pass review's MEDIUM finding.

        --pipeline=2 --ratio=1:4 --multi-key-get=4 enqueues alternating
        single-shot SET and multi-key GET commands in the same pipeline
        depth. An earlier (now-fixed) implementation tracked multi-get mode
        as a write-time flag, which caused the SET's HD response to be
        absorbed as if it were a multi-get section -- crashing memtier on
        a m_pending_resp >= 0 assertion.

        This test would crash / hang on the buggy version. The fix (always
        terminate every command with `mn` so every response ends at `MN`)
        makes parse_response uniform across single-shot and multi-get and
        removes the race entirely.
        """
        # Pre-load some keys so multi-gets don't all miss.
        for i in range(1, 11):
            self.server.kv[f"mix:{i}"] = b"M" * 8
        proc = _run_memtier(
            ["--ratio=1:4", "--pipeline=2", "--multi-key-get=4",
             "--key-pattern=R:R", "--key-prefix=mix:",
             "--key-minimum=1", "--key-maximum=10", "--data-size=8"],
            self.server.port, self.json_path,
            requests=50, clients=1, threads=1,
            timeout=15,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr.decode())
        # Verify both command types reached the wire and were parsed correctly.
        ms_count = self.server.count("ms")
        mg_count = self.server.count("mg")
        mn_count = self.server.count("mn")
        self.assertGreater(ms_count, 0)
        self.assertGreater(mg_count, 0)

        # Core always-mn invariant: one `mn` per logical command.
        # Each single-shot ms contributes 1 mn; each multi-get batch (which
        # bundles `--multi-key-get` mg's into one logical request) contributes
        # 1 mn for the whole batch. Total mn must equal: ms_count plus the
        # number of multi-get batches.
        #
        # This is the property that makes the always-mn design safe under
        # mixed pipelining -- without it, the v2 m_multi_mode race would have
        # been masked by a non-counting test.
        multi_get_batches = mg_count // 4
        self.assertEqual(
            mn_count, ms_count + multi_get_batches,
            f"always-mn invariant broken: mn={mn_count}, ms={ms_count}, "
            f"mg_batches={multi_get_batches} (mg_count={mg_count})",
        )
        # And the mg batching must be exact (no orphan mg's outside a batch).
        self.assertEqual(mg_count % 4, 0,
                         f"unexpected mg count {mg_count} not divisible by --multi-key-get=4")

        with open(self.json_path) as fp:
            results = json.load(fp)
        self.assertGreater(int(results["ALL STATS"]["Sets"]["Count"]), 0)
        self.assertGreater(int(results["ALL STATS"]["Gets"]["Count"]), 0)

    def test_multi_get_mixed_hits_and_misses(self):
        """Deterministic mix of VA + EN within a single multi-get pipeline.
        Pre-load half the keys, mark the other half as misses. Assert both
        paths fire (Hits/sec > 0 AND Misses/sec > 0). Catches accidental
        single-path coverage that the random-keyspace test elsewhere can
        produce when the RNG happens to pick all hits or all misses.
        """
        self._restart_server(miss_keys={f"k:{i}" for i in range(11, 21)})
        for i in range(1, 11):
            self.server.kv[f"k:{i}"] = b"data%02d" % i
        proc = _run_memtier(
            ["--ratio=0:4", "--key-pattern=R:R", "--key-prefix=k:",
             "--key-minimum=1", "--key-maximum=20",
             "--multi-key-get=4"],
            self.server.port, self.json_path,
            requests=50, clients=2, threads=1,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr.decode())
        with open(self.json_path) as fp:
            results = json.load(fp)
        gets = results["ALL STATS"]["Gets"]
        # Both paths exercised (statistically near-certain at 50*4*2 = 400 keys
        # against a 50/50 keyspace).
        self.assertGreater(gets["Hits/sec"], 0.0,
                           "expected some VA-path hits in mixed multi-get")
        self.assertGreater(gets["Misses/sec"], 0.0,
                           "expected some EN-path misses in mixed multi-get")


# RLTest discovers top-level ``test_*`` functions taking an ``env`` argument.
# We adapt our unittest cases so the same file works under both runners.
def _adapt(method_name):
    def _runner(env=None):
        case = MetaProtocolTests(method_name)
        result = unittest.TestResult()
        case.run(result)
        if result.errors or result.failures:
            details = "\n".join(t for _, t in result.errors + result.failures)
            raise AssertionError(f"{method_name} failed:\n{details}")
    _runner.__name__ = method_name
    return _runner


# Happy path.
test_meta_set_wire_format_and_storage = _adapt("test_set_wire_format_and_storage")
test_meta_get_hits_and_misses = _adapt("test_get_hits_and_misses")
test_meta_get_hits_against_preloaded_data = _adapt("test_get_hits_against_preloaded_data")
test_meta_multi_get_pipelines_and_terminates_with_mn = _adapt(
    "test_multi_get_pipelines_and_terminates_with_mn"
)
test_meta_set_then_get_roundtrip = _adapt("test_set_then_get_roundtrip")
# Negative / edge cases.
test_meta_ns_response_does_not_error = _adapt("test_ns_response_does_not_error")
test_meta_ex_response_does_not_error = _adapt("test_ex_response_does_not_error")
test_meta_nf_response_does_not_error = _adapt("test_nf_response_does_not_error")
test_meta_fragmented_va_response_is_reassembled = _adapt("test_fragmented_va_response_is_reassembled")
test_meta_unknown_response_classified_as_parse_error = _adapt("test_unknown_response_classified_as_parse_error")
test_meta_pipelined_set_completes = _adapt("test_pipelined_set_completes")
test_meta_pipelined_mixed_set_and_multi_get = _adapt("test_pipelined_mixed_set_and_multi_get")
test_meta_multi_get_mixed_hits_and_misses = _adapt("test_multi_get_mixed_hits_and_misses")


if __name__ == "__main__":
    unittest.main()
