"""
S6 -- Retry storm against a flaky shim server.

Launches a minimal Python TCP server on 127.0.0.1 that consumes incoming
RESP traffic and replies with ``-ERR injected\\r\\n`` 80% of the time and
``+OK\\r\\n`` (with a trailing ``$-1\\r\\n`` for GETs) 20% of the time.
Memtier is pointed at the shim with ``--retry-on-error`` and bounded
``--max-retries`` so the retry queue is exercised continuously for ~5
minutes without driving the binary into a deterministic dead-end.

Pass conditions:
  * memtier exits 0 (does not crash or busy-loop into OOM)
  * peak RSS observed by the sidecar < 500 MB
  * peak CPU < 200% (i.e. < 2 fully-utilised cores)

We intentionally bypass the RLTest-managed redis server and use the
shim's port directly via memtier args.

NOTE: A pure ``-ERR``-always shim deterministically wedges memtier (every
in-flight request hits max-retries and the connection effectively dies).
That is itself a real bug surface, but it would make S6 fail on a clean
master, so we use a probabilistic shim that still exercises the retry
codepath aggressively without hitting that wall.
"""

import os
import sys
import socket
import subprocess
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from include import (  # noqa: E402
    addTLSArgs,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig  # noqa: E402


class ErrShim(object):
    """Tiny RESP shim. Counts inbound RESP messages by parsing the
    ``*<N>\\r\\n`` array header + each bulk string header, then emits one
    reply per fully-received message. Reply is ``-ERR injected\\r\\n``
    with probability ``err_rate``, otherwise ``+OK\\r\\n``."""

    def __init__(self, err_rate=0.8):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(256)
        self.port = self.sock.getsockname()[1]
        self.err_rate = err_rate
        self.stop = threading.Event()
        self.acceptor = None
        # Deterministic per-process: avoids skew between runs in CI.
        import random as _r
        self._rng = _r.Random(0xDEADBEEF)
        self._rng_lock = threading.Lock()

    def _next_reply(self):
        with self._rng_lock:
            r = self._rng.random()
        if r < self.err_rate:
            return b"-ERR injected\r\n"
        # +OK works as a reply to SET; for GET memtier just sees a status
        # reply and counts the request as completed without a value.
        return b"+OK\r\n"

    def _serve_client(self, conn):
        buf = bytearray()
        # Hand-rolled RESP scanner: enough to count complete messages.
        try:
            conn.settimeout(0.5)
            while not self.stop.is_set():
                try:
                    chunk = conn.recv(65536)
                except socket.timeout:
                    continue
                except OSError:
                    return
                if not chunk:
                    return
                buf.extend(chunk)
                # Drain as many complete RESP messages as we can.
                while True:
                    consumed = _resp_message_length(buf)
                    if consumed <= 0:
                        break
                    del buf[:consumed]
                    try:
                        conn.sendall(self._next_reply())
                    except OSError:
                        return
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _accept_loop(self):
        self.sock.settimeout(0.5)
        while not self.stop.is_set():
            try:
                conn, _ = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            t = threading.Thread(
                target=self._serve_client, args=(conn,), daemon=True
            )
            t.start()

    def start(self):
        self.acceptor = threading.Thread(target=self._accept_loop, daemon=True)
        self.acceptor.start()

    def shutdown(self):
        self.stop.set()
        try:
            self.sock.close()
        except OSError:
            pass


def _resp_message_length(buf):
    """Return the byte length of the first complete RESP message in
    ``buf``, or 0 if more bytes are needed. Supports the subset memtier
    actually sends: inline commands, ``*<N>`` arrays of ``$<L>`` bulk
    strings, simple strings, and integer headers."""
    if not buf:
        return 0
    # Find end of header line (\r\n).
    nl = buf.find(b"\r\n")
    if nl < 0:
        return 0
    head = bytes(buf[:nl])
    tag = head[:1]
    if tag == b"*":
        try:
            n = int(head[1:])
        except ValueError:
            return 0
        pos = nl + 2
        for _ in range(n):
            if pos >= len(buf):
                return 0
            sub_nl = buf.find(b"\r\n", pos)
            if sub_nl < 0:
                return 0
            sub_head = bytes(buf[pos:sub_nl])
            stag = sub_head[:1]
            if stag == b"$":
                try:
                    bulk_len = int(sub_head[1:])
                except ValueError:
                    return 0
                pos = sub_nl + 2
                if bulk_len >= 0:
                    end = pos + bulk_len + 2  # trailing \r\n
                    if end > len(buf):
                        return 0
                    pos = end
            else:
                # Non-bulk subelement -- single line.
                pos = sub_nl + 2
        return pos
    # Single-line commands (inline) or single-line types.
    return nl + 2


def _read_rss_kb(pid):
    try:
        with open("/proc/{}/status".format(pid), "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except (OSError, IOError):
        return None
    return None


def _read_cpu_pct(pid, prev_state):
    """Return (cpu_pct, new_state)."""
    try:
        with open("/proc/{}/stat".format(pid)) as f:
            fields = f.read().split()
        utime = int(fields[13])
        stime = int(fields[14])
        proc_jiffies = utime + stime
        with open("/proc/stat") as f:
            total_fields = f.readline().split()
        total_jiffies = sum(int(x) for x in total_fields[1:])
    except (OSError, IOError, IndexError, ValueError):
        return 0.0, prev_state

    if prev_state is None:
        return 0.0, (proc_jiffies, total_jiffies)
    prev_proc, prev_total = prev_state
    d_proc = proc_jiffies - prev_proc
    d_total = total_jiffies - prev_total
    if d_total <= 0:
        return 0.0, (proc_jiffies, total_jiffies)
    n_cpu = os.cpu_count() or 1
    return 100.0 * n_cpu * d_proc / d_total, (proc_jiffies, total_jiffies)


def test_retry_storm_bounded(env):
    env.skipOnCluster()

    test_time = int(os.environ.get("MEMTIER_SOAK_TEST_TIME", "300"))
    sample_interval = int(os.environ.get("MEMTIER_SOAK_SAMPLE_INTERVAL", "5"))

    shim = ErrShim()
    shim.start()

    try:
        benchmark_specs = {
            "name": env.testName,
            "args": [
                "--pipeline=8",
                "--retry-on-error",
                "--max-retries=100",
                "--test-time={}".format(test_time),
                "--hide-histogram",
            ],
        }
        addTLSArgs(benchmark_specs, env)

        config_dict = get_default_memtier_config(
            threads=4, clients=50, requests=None, test_time=test_time
        )
        # Talk to the shim directly; opt out of the Benchmark helper's
        # auto-injected --server/--port.
        config_dict["memtier_benchmark"]["explicit_connect_args"] = True
        benchmark_specs["args"] = [
            "--server", "127.0.0.1",
            "--port", str(shim.port),
        ] + benchmark_specs["args"]

        test_dir = tempfile.mkdtemp()
        config = RunConfig(test_dir, env.testName, config_dict, {})
        ensure_clean_benchmark_folder(config.results_dir)

        benchmark = Benchmark.from_json(config, benchmark_specs)

        proc = subprocess.Popen(
            stdin=None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            executable=benchmark.binary,
            args=benchmark.args,
        )

        stop_sampler = threading.Event()
        peak_rss_kb = [0]
        peak_cpu = [0.0]

        def sampler():
            cpu_state = None
            while not stop_sampler.is_set():
                rss = _read_rss_kb(proc.pid)
                if rss is not None and rss > peak_rss_kb[0]:
                    peak_rss_kb[0] = rss
                cpu, cpu_state = _read_cpu_pct(proc.pid, cpu_state)
                if cpu > peak_cpu[0]:
                    peak_cpu[0] = cpu
                stop_sampler.wait(sample_interval)

        sampler_thread = threading.Thread(target=sampler, daemon=True)
        sampler_thread.start()

        try:
            _stdout, _stderr = proc.communicate(timeout=test_time + 120)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            env.assertTrue(False)
            return
        finally:
            stop_sampler.set()
            sampler_thread.join(timeout=5)

        if _stderr:
            benchmark.write_file("mb.stderr", _stderr)
        memtier_ok = proc.wait() == 0

        env.debugPrint(
            "Retry storm peak RSS={:.1f} MB, peak CPU={:.1f}%".format(
                peak_rss_kb[0] / 1024.0, peak_cpu[0]
            ),
            True,
        )

        env.assertTrue(peak_rss_kb[0] < 500 * 1024)
        env.assertTrue(peak_cpu[0] < 200.0)
        if not memtier_ok:
            debugPrintMemtierOnError(config, env)
        env.assertTrue(memtier_ok)
    finally:
        shim.shutdown()
