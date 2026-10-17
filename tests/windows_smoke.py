"""Smoke test for the native Windows (MinGW-w64) build of memtier_benchmark.

Needs only the Python standard library and no Redis: the workload runs against
the stdlib RESP sink in tests/fuzz/resp_command_sink.py, which answers +OK to
every command.

Usage (from the repository root, with the MinGW runtime DLLs on PATH, e.g. in
an MSYS2 UCRT64 shell):

    python tests/windows_smoke.py [path/to/memtier_benchmark.exe]

The executable defaults to $MEMTIER_BENCHMARK, then ./memtier_benchmark.exe.
"""

import ctypes
import json
import os
import socket
import ssl
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "fuzz"))

from resp_command_sink import RESPCommandSink  # noqa: E402

RUN_TIMEOUT = 120
# NTSTATUS failure codes (access violation 0xC0000005, ...) start at 0xC0000000.
NTSTATUS_ERROR_MIN = 0xC0000000

if sys.platform == "win32":
    # Make a crashing child exit silently instead of blocking on an error dialog;
    # the mode is inherited by child processes.
    ctypes.windll.kernel32.SetErrorMode(0x8003)


def find_exe():
    for index, arg in enumerate(sys.argv[1:], 1):
        if not arg.startswith("-"):
            return os.path.abspath(sys.argv.pop(index))
    candidate = os.environ.get("MEMTIER_BENCHMARK")
    if candidate:
        return os.path.abspath(candidate)
    return os.path.abspath(os.path.join(HERE, "..", "memtier_benchmark.exe"))


EXE = find_exe()


def run_memtier(*args):
    return subprocess.run([EXE] + list(args), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          timeout=RUN_TIMEOUT, universal_newlines=True)


def unused_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class TLSCommandSink(RESPCommandSink):
    def __init__(self, context):
        super().__init__()
        self.context = context

    def _serve(self, connection):
        original = connection
        try:
            connection.settimeout(5)
            connection = self.context.wrap_socket(connection, server_side=True)
            connection.settimeout(None)
            with self._condition:
                self._connections.discard(original)
                self._connections.add(connection)
            super()._serve(connection)
        except Exception as error:
            self._record_error("TLS connection failed: {}".format(error))
            connection.close()
            with self._condition:
                self._connections.discard(original)
                self._connections.discard(connection)
                self._condition.notify_all()


class WindowsSmokeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.json_path = os.path.join(self.tmp.name, "mb.json")

    def run_against_sink(self, sink, *args):
        result = run_memtier("-s", sink.host, "-p", str(sink.port), "--hide-histogram",
                             "--json-out-file=" + self.json_path, *args)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(sink.wait_idle())
        self.assertEqual(sink.errors, ())
        with open(self.json_path) as json_file:
            return json.load(json_file)["ALL STATS"]

    def test_fixed_request_count(self):
        with RESPCommandSink() as sink:
            stats = self.run_against_sink(sink, "-t", "2", "-c", "2", "-n", "1000", "--ratio=1:0")
            self.assertEqual(stats["Sets"]["Count"], 4000)
            self.assertEqual(stats["Gets"]["Count"], 0)
            self.assertEqual(sink.request_count, 4000)

    def test_monitor_input_long_final_line(self):
        # Cross the Windows getline shim's initial 128-byte allocation and
        # several reallocations; its final line must also work without a LF.
        monitor_path = os.path.join(self.tmp.name, "monitor.txt")
        with open(monitor_path, "wb") as monitor_file:
            monitor_file.write(b'"SET" "long-line" "' + b"x" * 1024 + b'"')
        with RESPCommandSink() as sink:
            stats = self.run_against_sink(sink, "-t", "1", "-c", "1", "-n", "3",
                                          "--monitor-input=" + monitor_path,
                                          "--command=__monitor_line1__")
            self.assertEqual(stats["Sets"]["Count"], 3)
            self.assertEqual(sink.request_count, 3)

    def test_pipeline_reconnect(self):
        with RESPCommandSink() as sink:
            stats = self.run_against_sink(sink, "-t", "2", "-c", "2", "-n", "100",
                                          "--ratio=1:0", "--pipeline=4", "--reconnect-interval=20")
            self.assertEqual(stats["Sets"]["Count"], 400)
            self.assertEqual(sink.request_count, 400)

    def test_tls_reconnect(self):
        cert = os.path.join(self.tmp.name, "cert.pem")
        key = os.path.join(self.tmp.name, "key.pem")
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                        "-subj", "/CN=localhost", "-keyout", key, "-out", cert],
                       check=True, capture_output=True, timeout=30)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert, key)
        with TLSCommandSink(context) as sink:
            stats = self.run_against_sink(sink, "-t", "1", "-c", "2", "-n", "100",
                                          "--ratio=1:0", "--pipeline=4", "--reconnect-interval=20",
                                          "--tls", "--tls-skip-verify")
            self.assertEqual(stats["Sets"]["Count"], 200)
            self.assertEqual(sink.request_count, 200)

    def test_output_prefix_directory(self):
        prefix = os.path.join(self.tmp.name, "histogram")
        with RESPCommandSink() as sink:
            stats = self.run_against_sink(sink, "-t", "1", "-c", "1", "-n", "20",
                                          "--ratio=1:0", "--hdr-file-prefix=" + prefix)
            self.assertEqual(stats["Sets"]["Count"], 20)
        self.assertTrue(any(name.startswith("histogram") for name in os.listdir(self.tmp.name)))

    def test_invalid_output_directory_fails_before_requests(self):
        with RESPCommandSink() as sink:
            result = run_memtier("-s", sink.host, "-p", str(sink.port), "-t", "1", "-c", "1", "-n", "1",
                                 "--hdr-file-prefix=" + os.path.join(self.tmp.name, "missing", "histogram"))
            self.assertEqual(result.returncode, 1, result.stderr)
            self.assertIn("cannot write --hdr-file-prefix directory", result.stderr)
            self.assertEqual(sink.request_count, 0)

    def test_timed_run_reports_cpu_usage(self):
        with RESPCommandSink() as sink:
            stats = self.run_against_sink(sink, "-t", "1", "-c", "1", "--test-time=3", "--ratio=1:0")
        self.assertGreater(stats["Sets"]["Count"], 0)
        cpu = stats["CPU"]
        for key in ("cpu_user_seconds", "cpu_sys_seconds", "cpu_total_seconds", "cpu_wall_seconds",
                    "cpu_cores_used", "avg_cpu_utilization_pct", "peak_cpu_utilization_pct",
                    "threads_counted"):
            self.assertIn(key, cpu)
            self.assertGreaterEqual(cpu[key], 0, key)
        self.assertEqual(cpu["threads_counted"], 1)
        self.assertAlmostEqual(cpu["cpu_total_seconds"], cpu["cpu_user_seconds"] + cpu["cpu_sys_seconds"],
                               delta=0.01)
        self.assertGreater(cpu["cpu_total_seconds"], 0)
        self.assertGreater(cpu["cpu_wall_seconds"], 2)
        self.assertIn("Thread 0", cpu["Per Thread"])
        self.assertIn("CPU Stats", stats)

    def test_closed_port_fails_without_crashing(self):
        result = run_memtier("-s", "127.0.0.1", "-p", str(unused_port()), "-t", "1", "-c", "1", "-n", "10",
                             "--connection-stage-timeout=3", "--hide-histogram")
        code = result.returncode & 0xFFFFFFFF
        self.assertNotEqual(code, 0, result.stderr)
        self.assertLess(code, NTSTATUS_ERROR_MIN, "crashed: 0x%08x" % code)
        self.assertNotIn("BUG REPORT", result.stderr)
        self.assertIn("Connection error:", result.stderr)

    @unittest.skipUnless(sys.platform == "win32", "detached CRT handles are Windows-specific")
    def test_detached_process_writes_requested_output(self):
        startup = subprocess.STARTUPINFO()
        startup.dwFlags |= subprocess.STARTF_USESTDHANDLES
        startup.hStdInput = startup.hStdOutput = startup.hStdError = 0
        output_path = os.path.join(self.tmp.name, "detached.txt")
        with RESPCommandSink() as sink:
            child = subprocess.Popen([EXE, "-s", sink.host, "-p", str(sink.port), "-t", "1", "-c", "1",
                                      "-n", "20", "--ratio=1:0", "--out-file=" + output_path,
                                      "--json-out-file=" + self.json_path], startupinfo=startup,
                                     creationflags=subprocess.DETACHED_PROCESS)
            try:
                self.assertEqual(child.wait(timeout=20), 0)
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait()
            self.assertTrue(sink.wait_idle())
            self.assertEqual(sink.request_count, 20)
        with open(self.json_path) as output:
            self.assertEqual(json.load(output)["ALL STATS"]["Sets"]["Count"], 20)
        with open(output_path) as output:
            self.assertIn("Totals", output.read())

    @unittest.skipUnless(sys.platform == "win32", "AF_UNIX sockets are only rejected on Windows")
    def test_unix_socket_is_rejected(self):
        result = run_memtier("-S", "foo")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--unix-socket is not supported on Windows", result.stderr)


if __name__ == "__main__":
    unittest.main()
