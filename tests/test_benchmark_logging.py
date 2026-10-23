"""Keep benchmark diagnostics complete without oversized CI log lines."""

import logging
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace

from mb import Benchmark


class _Capture(logging.Handler):
    def __init__(self, path, payload):
        super().__init__()
        self.path = path
        self.payload = payload
        self.messages = []
        self.saved_before_logging = False

    def emit(self, record):
        message = record.getMessage()
        self.messages.append(message)
        if '>>> stderr <<<' in message:
            self.saved_before_logging = (
                self.path.exists() and self.path.read_bytes() == self.payload)


def _run_child(payload, exit_code, level):
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / 'mb.stderr'
        source = Path(directory) / 'payload'
        source.write_bytes(payload)
        # Use the real subprocess/pipe/file path; no Redis connection is needed.
        benchmark = Benchmark.__new__(Benchmark)
        benchmark.config = SimpleNamespace(results_dir=directory)
        benchmark.binary = sys.executable
        benchmark.args = [
            sys.executable, '-c',
            'import pathlib,sys; '
            'sys.stderr.buffer.write(pathlib.Path(sys.argv[1]).read_bytes()); '
            'sys.exit(int(sys.argv[2]))', str(source), str(exit_code)]
        logger = logging.getLogger()
        old_handlers, old_level = logger.handlers[:], logger.level
        capture = _Capture(path, payload)
        logger.handlers = [capture]
        logger.setLevel(level)
        try:
            success = benchmark.run(timeout=10)
        finally:
            logger.handlers = old_handlers
            logger.setLevel(old_level)
        return success, path.read_bytes() if path.exists() else None, capture


def test_benchmark_stderr_logs_bounded_lines_and_preserves_bytes(env):
    line = b'WRONGTYPE operation against a key holding the wrong kind of value'
    payload = ((line + b'\n') * 4000 + b'\n' + b'x' * 5000 +
               b'\r\ninvalid-\xff\nutf8-\xe2\x82\xac\n')
    success, saved, capture = _run_child(payload, 0, logging.DEBUG)
    env.assertTrue(success)
    # RLTest's verbose assertEqual prints both operands even on success. Keep
    # the large fixture out of its log too, including on a regression failure.
    env.assertTrue(saved == payload, message='raw stderr must remain byte-exact')
    env.assertTrue(capture.saved_before_logging)
    header = capture.messages.index('  >>> stderr <<<')
    messages = capture.messages[header + 1:]
    env.assertTrue(messages[:4000] == [line.decode()] * 4000,
                   message='all repeated error lines must remain present')
    env.assertTrue(messages[4000:] == [
        '', 'x' * 2048, 'x' * 2048, 'x' * 904, 'invalid-\ufffd', 'utf8-\u20ac'],
        message='chunks, blank lines and replacement decoding must match')
    env.assertTrue(all(len(message) <= 2048 for message in messages))


def test_benchmark_stderr_exit_failure_and_disabled_debug(env):
    payload = b'child failed\xff\n'
    success, saved, capture = _run_child(payload, 7, logging.WARNING)
    env.assertFalse(success)
    env.assertEqual(saved, payload)
    env.assertEqual(capture.messages, [])


def test_benchmark_empty_stderr_keeps_exit_status(env):
    for exit_code in (0, 7):
        success, saved, capture = _run_child(b'', exit_code, logging.DEBUG)
        env.assertEqual(success, exit_code == 0)
        env.assertEqual(saved, None)
        env.assertFalse(any('>>> stderr <<<' in text for text in capture.messages))
