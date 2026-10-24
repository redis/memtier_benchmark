"""Structured fatal summaries preserve results and debug-only lifecycle notices."""
import json
import os
import socket
import socketserver
import subprocess
import tempfile
import threading
import unittest

MEMTIER_BINARY = os.environ.get("MEMTIER_BINARY", os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "memtier_benchmark")))


class Peer(socketserver.StreamRequestHandler):
    def handle(self):
        self.connection.settimeout(10)
        try:
            while not self.server.stop.is_set():
                line = self.rfile.readline()
                if not line:
                    return
                count = int(line[1:])
                command = []
                for _ in range(count):
                    length = int(self.rfile.readline()[1:])
                    command.append(self.rfile.read(length))
                    assert self.rfile.read(2) == b"\r\n"
                if self.server.silent:
                    self.server.stop.wait(10)
                    return
                if command[0].upper() == b"GET":
                    self.wfile.write(b"$3\r\nzzz\r\n")
                else:
                    self.wfile.write(b"+OK\r\n")
        except (OSError, ValueError):
            return


class JSONFatalErrorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Peer)
        self.server.daemon_threads = True
        self.server.stop = threading.Event()
        self.server.silent = False
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.stop.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)
        self.temp.cleanup()

    def run_client(self, *extra):
        path = os.path.join(self.temp.name, "result.json")
        args = [MEMTIER_BINARY, "-s", "127.0.0.1", "-p", str(self.server.server_address[1]),
                "-t", "1", "-c", "1", "-n", "4", "--ratio=1:0", "--hide-histogram",
                "--json-out-file", path] + list(extra)
        result = subprocess.run(args, capture_output=True, text=True, timeout=15)
        with open(path) as source:
            document = json.load(source)
        return result, document

    def test_success_has_no_fatal_summary_or_lifecycle_noise(self):
        result, document = self.run_client()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("FATAL_ERROR", document)
        self.assertEqual(document["ALL STATS"]["Totals"]["Count"], 4)
        self.assertNotIn("Json file ", result.stderr)
        self.assertNotIn("Saving JSON output file:", result.stderr)

    def test_debug_retains_one_close_notice(self):
        result, document = self.run_client("--debug")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("FATAL_ERROR", document)
        self.assertIn("created...", result.stderr)
        self.assertEqual(result.stderr.count("Json file closed."), 1)

    def test_setup_deadline_records_fatal_summary(self):
        self.server.silent = True
        result, document = self.run_client("--authenticate=dummy-json-test",
                                          "--connection-timeout=0", "--connection-stage-timeout=1")
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(document["FATAL_ERROR"], "Connection setup failed")
        self.assertIn("ALL STATS", document)
        self.assertNotIn("dummy-json-test", json.dumps(document))

    def test_verification_failure_retains_results(self):
        source = os.path.join(self.temp.name, "import.csv")
        with open(source, "w") as output:
            output.write("dumpflags, time, exptime, nbytes, nsuffix, it_flags, clsid, nkey, key, data\n")
            output.write("0, 0, 0, 5, 0, 0, 0, 8, json-key, abc\n")
        result, document = self.run_client("--data-import", source, "--data-verify")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(document["FATAL_ERROR"], "Data verification failed or did not complete")
        self.assertEqual(document["ALL STATS"]["Totals"]["Count"], 4)
        self.assertGreater(document["client verifications results"]["keys failed"], 0)

    def test_output_failure_retains_successful_workload(self):
        if not os.path.exists("/dev/full"):
            self.skipTest("requires /dev/full")
        result, document = self.run_client("--out-file", "/dev/full")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(document["FATAL_ERROR"], "A requested output could not be written")
        self.assertEqual(document["ALL STATS"]["Totals"]["Count"], 4)


def _adapt(method):
    def run(env):
        case = JSONFatalErrorTests(method)
        result = unittest.TestResult()
        case.run(result)
        if result.skipped:
            env.skip()
        if result.errors or result.failures:
            raise AssertionError("\n".join(detail for _, detail in result.errors + result.failures))
    return run


test_json_success_without_fatal_summary = _adapt("test_success_has_no_fatal_summary_or_lifecycle_noise")
test_json_debug_lifecycle = _adapt("test_debug_retains_one_close_notice")
test_json_setup_fatal_summary = _adapt("test_setup_deadline_records_fatal_summary")
test_json_verification_fatal_summary = _adapt("test_verification_failure_retains_results")
test_json_output_fatal_summary = _adapt("test_output_failure_retains_successful_workload")


if __name__ == "__main__":
    unittest.main()
