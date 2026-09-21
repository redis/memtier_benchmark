"""Regression checks for the MONITOR parser-fuzz driver and its failure oracle."""
import contextlib
import io
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import fuzz_monitor_input as driver
from resp_command_sink import RESPCommandSink


class DriverOracleTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        patch = mock.patch.object(driver.tempfile, 'tempdir', self.directory.name)
        patch.start()
        self.addCleanup(patch.stop)
        self.sink = mock.Mock(host='127.0.0.1', port=1, request_count=0, errors=())
        self.sink.wait_idle.return_value = True

    def run_result(self, code=0, stderr=b'', **kwargs):
        result = subprocess.CompletedProcess([], code, b'', stderr)
        with mock.patch.object(driver.subprocess, 'run', return_value=result), \
                contextlib.redirect_stderr(io.StringIO()):
            return driver.run_one('oracle-test', b'invalid monitor input', self.sink, **kwargs)

    def test_clean_parse_error_is_allowed(self):
        self.assertTrue(self.run_result(code=2))
        self.assertEqual(list(Path(self.directory.name).iterdir()), [])

    def test_crash_and_sanitizer_reports_remain_failures(self):
        for code, stderr in [(-11, b''), (-6, b''), (42, b''),
                             (0, b'AddressSanitizer'), (0, b'runtime error:')]:
            with self.subTest(code=code, stderr=stderr):
                self.assertFalse(self.run_result(code, stderr))
        self.assertEqual(len(list(Path(self.directory.name).iterdir())), 5)

    def test_timeout_remains_a_failure(self):
        with mock.patch.object(driver.subprocess, 'run',
                               side_effect=subprocess.TimeoutExpired([], 1, stderr=b'last output')), \
                contextlib.redirect_stderr(io.StringIO()):
            self.assertFalse(driver.run_one('timeout', b'input', self.sink))
        self.assertEqual(len(list(Path(self.directory.name).iterdir())), 1)

    def test_sink_failure_cannot_be_accepted_as_parse_error(self):
        self.sink.errors = ('invalid RESP',)
        self.assertFalse(self.run_result(code=2))

    def test_sink_drain_failure_is_reported(self):
        self.sink.wait_idle.return_value = False
        self.assertFalse(self.run_result())

    def test_preflight_requires_a_complete_request(self):
        self.assertFalse(self.run_result(require_requests=True))

    def test_known_assertion_cannot_hide_a_crash(self):
        assertion = driver.KNOWN_NONFATAL_ASSERTIONS[0]
        self.assertTrue(self.run_result(stderr=assertion))
        self.assertFalse(self.run_result(-11, assertion))
        self.assertFalse(self.run_result(stderr=assertion + b'AddressSanitizer'))

    def test_unexpected_launch_failure_preserves_reproducer(self):
        with mock.patch.object(driver.subprocess, 'run', side_effect=OSError('launch failed')), \
                contextlib.redirect_stderr(io.StringIO()), self.assertRaises(OSError):
            driver.run_one('launch', b'input', self.sink)
        self.assertEqual(len(list(Path(self.directory.name).iterdir())), 1)


class SweepTests(unittest.TestCase):
    def setUp(self):
        stack = contextlib.ExitStack()
        self.addCleanup(stack.close)
        self.directory = Path(stack.enter_context(tempfile.TemporaryDirectory()))
        self.binary = self.directory / 'memtier'
        self.binary.touch()
        self.corpus = self.directory / 'corpus'
        self.corpus.mkdir()
        (self.corpus / 'seed.txt').write_bytes(b'corpus input')
        regressions = self.directory / 'monitor_input_regressions'
        regressions.mkdir()
        (regressions / 'saved.txt').write_bytes(b'unchanged regression')
        for name, value in [('MEMTIER', self.binary), ('HERE', self.directory),
                            ('CORPUS_DIR', self.corpus), ('INCLUDE_HUGE_SYNTHETIC', False),
                            ('FUZZ_ITER', 2)]:
            stack.enter_context(mock.patch.object(driver, name, value))
        stack.enter_context(mock.patch.dict(driver.os.environ,
                                           FUZZ_MAX_SECONDS='0', FUZZ_PROGRESS_EVERY='1'))
        stack.enter_context(contextlib.redirect_stderr(io.StringIO()))
        self.run_one = stack.enter_context(mock.patch.object(driver, 'run_one', return_value=True))
        self.sink = object()

    def test_preflight_and_saved_inputs_precede_mutations(self):
        self.assertEqual(driver.fuzz(self.sink), 0)
        calls = self.run_one.call_args_list
        self.assertEqual(len(calls), 4)
        self.assertEqual(calls[0].args[0], 'sink-preflight')
        self.assertTrue(calls[0].kwargs['require_requests'])
        self.assertEqual(calls[1].args, ('saved.txt', b'unchanged regression', self.sink))
        self.assertTrue(calls[1].kwargs['require_requests'])
        self.assertTrue(all(call.args[0] == 'seed.txt' for call in calls[2:]))

    def test_preflight_or_saved_regression_failure_stops_sweep(self):
        for results in ([False], [True, False]):
            with self.subTest(results=results):
                self.run_one.reset_mock()
                self.run_one.side_effect = results
                self.assertEqual(driver.fuzz(self.sink), 1)
                self.assertEqual(self.run_one.call_count, len(results))

    def test_mutation_failure_is_not_lost_after_later_success(self):
        self.run_one.side_effect = [True, True, False, True]
        self.assertEqual(driver.fuzz(self.sink), 1)

    def test_main_closes_sink_after_unexpected_failure(self):
        sink = RESPCommandSink()
        with mock.patch.object(driver, 'fuzz', side_effect=RuntimeError('failure')), \
                mock.patch.object(driver, 'RESPCommandSink', return_value=sink):
            with self.assertRaises(RuntimeError):
                driver.main()
        self.assertFalse(sink._accept_thread.is_alive())


@unittest.skipUnless(driver.MEMTIER.is_file(), 'build memtier or set MEMTIER to run replay regressions')
class MonitorReplayTests(unittest.TestCase):
    def test_saved_nightly_inputs_finish_without_server_side_effects(self):
        paths = sorted((driver.HERE / 'monitor_input_regressions').glob('*.txt'))
        self.assertEqual({path.name for path in paths},
                         {'wait_forever.txt', 'hello_protocol_switch.txt'})
        with RESPCommandSink() as sink:
            for path in paths:
                with self.subTest(path=path.name):
                    self.assertTrue(driver.run_one(path.name, path.read_bytes(), sink,
                                                   require_requests=True))
            self.assertFalse(sink.errors)

    def test_wait_and_protocol_switch_are_acknowledged(self):
        payload = (b'1.0 [0 127.0.0.1:1] "WAIT" "8" "0"\n'
                   b'1.0 [0 127.0.0.1:1] "HELLO" "3"\n'
                   b'1.0 [0 127.0.0.1:1] "PING"\n')
        with RESPCommandSink() as sink:
            self.assertTrue(driver.run_one('blocking-and-protocol', payload, sink,
                                           require_requests=True))
            self.assertGreaterEqual(sink.request_count, 3)
            self.assertFalse(sink.errors)


if __name__ == '__main__':
    unittest.main()
