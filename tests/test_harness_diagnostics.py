"""Failure diagnostics must preserve the original assertion in every runner mode."""
import os
import tempfile
from types import SimpleNamespace

from include import assert_minimum_memtier_outcomes


class _FailureRecorder:
    def __init__(self, runner):
        self.envRunner = runner
        self.failures = []
        self.output = []

    def getNumberOfFailedAssertion(self):
        return len(self.failures)

    def assertTrue(self, value):
        if not value:
            self.failures.append(value)

    def assertEqual(self, expected, actual):
        if expected != actual:
            self.failures.append((expected, actual))

    def isCluster(self):
        return False

    def debugPrint(self, message, force):
        self.output.append(message)


def _check_failure(env, directory, runner, expected_log):
    # Exercise the real finally/diagnostic path after a wrong operation count.
    for name in ("mb.stdout", "mb.stderr", "mb.json"):
        with open(os.path.join(directory, name), "w") as output:
            output.write("memtier-output-marker\n")
    recorder = _FailureRecorder(runner)
    assert_minimum_memtier_outcomes(
        SimpleNamespace(results_dir=directory), recorder, True, 999999, 10)
    env.assertEqual(recorder.failures, [(999999, 10)])
    env.assertTrue(any("memtier-output-marker" in line for line in recorder.output))
    env.assertTrue(any(expected_log in line for line in recorder.output))


def test_failure_diagnostics_without_managed_log(env):
    with tempfile.TemporaryDirectory() as directory:
        # Existing-env has no managed log attributes; an absent runner is valid too.
        for runner in (SimpleNamespace(), None):
            _check_failure(env, directory, runner, "Redis log unavailable")


def test_failure_diagnostics_missing_or_unreadable_log(env):
    with tempfile.TemporaryDirectory() as directory:
        # -s does not create the log. Opening a directory exercises an actual
        # filesystem read error even when the tests run with elevated privileges.
        for name in ("uncaptured.log", "unreadable.log"):
            if name == "unreadable.log":
                os.mkdir(os.path.join(directory, name))
            runner = SimpleNamespace(dbDirPath=directory, _getFileName=lambda *args: name)
            _check_failure(env, directory, runner, "Redis log unavailable")


def test_failure_diagnostics_captured_log_decoding(env):
    with tempfile.TemporaryDirectory() as directory:
        with open(os.path.join(directory, "redis.log"), "wb") as output:
            output.write(b"redis-output-marker\xff\n")
        runner = SimpleNamespace(dbDirPath=directory, _getFileName=lambda *args: "redis.log")
        _check_failure(env, directory, runner, "redis-output-marker\ufffd")
