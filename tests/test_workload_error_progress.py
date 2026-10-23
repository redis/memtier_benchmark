"""Ordinary workload errors are connection progress, not setup failures (#686)."""
import csv
import json
import os
import socketserver
import subprocess
import tempfile
import threading
import time

from include import MEMTIER_BINARY


class _Peer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, reply=b'-ERR dummy command rejection\r\n', mode='', reject=None):
        super().__init__(('127.0.0.1', 0), _Handler)
        self.reply, self.mode, self.reject = reply, mode, reject
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.connections = self.requests = self.replies = self.errors = 0
        self.first_request = None
        self.first_success = None
        self.failures = []


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        peer = self.server
        self.connection.settimeout(15)
        with peer.lock:
            peer.connections += 1
            connection = peer.connections
        local = 0
        try:
            if peer.mode == 'greeting':
                self.wfile.write(peer.reply)
                return
            while not peer.stop.is_set():
                line = self.rfile.readline()
                if not line:
                    return
                command = []
                if not line.startswith(b'*'):
                    raise ValueError('expected RESP array')
                for _ in range(int(line[1:])):
                    size = int(self.rfile.readline()[1:])
                    command.append(self.rfile.read(size))
                    if self.rfile.read(2) != b'\r\n':
                        raise ValueError('missing RESP argument terminator')
                op = command[0].upper()
                if op in (b'AUTH', b'SELECT', b'HELLO'):
                    response = b'-ERR dummy setup rejection\r\n' if op == peer.reject else (
                        b'%0\r\n' if op == b'HELLO' else b'+OK\r\n')
                    self.wfile.write(response)
                    continue
                with peer.lock:
                    peer.requests += 1
                    if peer.first_request is None:
                        peer.first_request = time.monotonic()
                    elapsed = time.monotonic() - peer.first_request
                local += 1
                if peer.mode == 'silent' or (peer.mode == 'second-silent' and connection > 1):
                    peer.stop.wait(15)
                    return
                if peer.mode == 'drop' and connection <= 2 and local == 2:
                    return
                response = peer.reply
                if peer.mode == 'recover' and elapsed >= 3:
                    response = b'+OK\r\n'
                    with peer.lock:
                        if peer.first_success is None:
                            peer.first_success = time.monotonic()
                self.wfile.write(response)
                with peer.lock:
                    peer.replies += 1
                    peer.errors += int(response.startswith(b'-'))
                if peer.mode == 'close':
                    return
        except OSError:
            pass  # the benchmark may close while a response is being written
        except Exception as error:
            peer.failures.append(repr(error))


def _run(env, peer, extra=(), duration=3, authenticate=True):
    thread = threading.Thread(target=peer.serve_forever, daemon=True)
    thread.start()
    try:
        with tempfile.TemporaryDirectory() as directory:
            stats_path = os.path.join(directory, 'stats.json')
            failed_path = os.path.join(directory, 'failed.csv')
            args = [MEMTIER_BINARY, '-s', '127.0.0.1', '-p', str(peer.server_address[1]),
                    '-t', '1', '-c', '1', '--ratio=1:0',
                    '--pipeline=1', '--rate-limiting=20', '--connection-stage-timeout=1',
                    '--hide-histogram', '--json-out-file=' + stats_path, '--failed-keys-file=' + failed_path]
            if authenticate:
                args.extend(['-a', 'dummy-workload-error-686'])
            if duration is not None:
                args.append('--test-time=' + str(duration))
            args.extend(extra)
            started = time.monotonic()
            result = subprocess.run(args, capture_output=True, text=True, timeout=20)
            elapsed = time.monotonic() - started
            with open(stats_path) as source:
                stats = json.load(source)
            with open(failed_path, newline='') as source:
                failures = list(csv.DictReader(source))
            return result, stats, failures, elapsed
    finally:
        peer.stop.set()
        peer.shutdown()
        peer.server_close()
        thread.join(timeout=3)
        env.assertEqual(peer.failures, [])


def _assert_completed(env, result, stats, peer):
    env.assertEqual(result.returncode, 0, message=result.stderr[-2000:])
    env.assertFalse('connection-stage failures' in result.stderr)
    totals = stats['ALL STATS']['Totals']
    env.assertEqual(totals['Count'], peer.replies)
    env.assertGreater(totals['Count'], 0)
    env.assertEqual(stats['ALL STATS']['Runtime']['Interrupted'], 'false')


def test_ordinary_workload_errors_reach_steady_state(env):
    for error in (b'-ERR dummy command rejection\r\n', b'-READONLY dummy replica rejection\r\n',
                  b'-NOPERM dummy command denial\r\n', b'-OOM dummy memory rejection\r\n',
                  b'-ERR command mentions Protocol error: in its text\r\n',
                  b'-DENIED dummy command denial\r\n'):
        peer = _Peer(error)
        result, stats, failures, elapsed = _run(env, peer, ['--pipeline=4'])
        _assert_completed(env, result, stats, peer)
        env.assertGreaterEqual(stats['ALL STATS']['Runtime']['Total duration'], 3000)
        env.assertEqual(len(failures), peer.errors)
        env.assertGreaterEqual(elapsed, 3)


def test_resp3_workload_errors_after_setup(env):
    peer = _Peer(b'-NOPERM dummy command denial\r\n')
    result, stats, failures, _ = _run(env, peer, ['--protocol=resp3', '--select-db=1'])
    _assert_completed(env, result, stats, peer)
    env.assertEqual(len(failures), peer.errors)


def test_workload_errors_reset_reconnect_budget(env):
    peer = _Peer(mode='drop')
    result, stats, failures, _ = _run(env, peer, ['--reconnect-on-error', '--max-reconnect-attempts=1',
                                               '--connection-stage-timeout=0'], duration=5)
    env.assertEqual(result.returncode, 0, message=result.stderr[-2000:])
    env.assertEqual(peer.connections, 3)
    env.assertGreaterEqual(result.stderr.count('Reconnection successful after 1 attempts'), 2)
    env.assertGreater(stats['ALL STATS']['Totals']['Count'], 2)
    env.assertGreater(len(failures), 2)
    env.assertFalse('maximum reconnection attempts' in result.stderr)


def test_protocol_rejections_do_not_reach_steady_state(env):
    reply = b'-ERR Protocol error: invalid bulk length\r\n'
    for mode in ('', 'close'):
        peer = _Peer(reply, mode=mode)
        result, _stats, _failures, elapsed = _run(env, peer, ['--reconnect-on-error'])
        env.assertEqual(result.returncode, 2, message=result.stderr[-2000:])
        env.assertContains('connection-stage failures', result.stderr)
        env.assertLess(elapsed, 8)
    # With the startup timeout disabled, protocol-error responses must not
    # reset the finite reconnect budget either.
    peer = _Peer(reply, mode='close')
    result, _stats, _failures, _ = _run(env, peer, ['--reconnect-on-error', '--max-reconnect-attempts=1',
                                                 '--connection-stage-timeout=0'], duration=5)
    env.assertEqual(result.returncode, 1, message=result.stderr[-2000:])
    env.assertEqual(peer.connections, 2)


def test_setup_parser_and_silent_failures_still_abort(env):
    for peer, extra in ((_Peer(reject=b'AUTH'), []),
                        (_Peer(reject=b'SELECT'), ['--select-db=1']),
                        (_Peer(reject=b'HELLO'), ['--protocol=resp3']),
                        (_Peer(b'?invalid\r\n'), []), (_Peer(mode='silent'), [])):
        result, stats, failures, elapsed = _run(env, peer, extra)
        env.assertEqual(result.returncode, 2, message=result.stderr[-2000:])
        env.assertContains('connection-stage failures', result.stderr)
        env.assertEqual(stats['ALL STATS']['Totals']['Count'], 0)
        env.assertEqual(failures, [])
        env.assertLess(elapsed, 8)


def test_retries_are_progress_but_not_logical_completions(env):
    # Unlimited retries survive the setup deadline; four logical requests
    # complete only when the peer starts accepting them after three seconds.
    peer = _Peer(b'-OOM dummy rejection\r\n', mode='recover')
    result, stats, failures, _ = _run(env, peer, ['--requests=4', '--retry-on-error',
                                               '--retry-backoff-ms=100'], duration=None)
    env.assertEqual(result.returncode, 0, message=result.stderr[-2000:])
    env.assertEqual(stats['ALL STATS']['Totals']['Count'], 4)
    env.assertEqual(failures, [])
    env.assertGreater(peer.errors, 4)
    env.assertIsNotNone(peer.first_success)
    if peer.first_success is not None:
        env.assertGreaterEqual(peer.first_success - peer.first_request, 3)
    # Finite retries exhaust once per logical request and preserve failure rows.
    peer = _Peer(b'-OOM dummy rejection\r\n')
    result, stats, failures, _ = _run(env, peer, ['--requests=4', '--retry-on-error', '--max-retries=3',
                                               '--retry-backoff-ms=100'], duration=None)
    env.assertEqual(result.returncode, 0, message=result.stderr[-2000:])
    env.assertEqual(peer.requests, 16)
    env.assertEqual(stats['ALL STATS']['Totals']['Count'], 4)
    env.assertEqual(len(failures), 4)
    env.assertTrue(all(int(row['retries']) == 3 for row in failures))


def test_workload_error_progress_is_reset_between_runs(env):
    # An ordinary error completes run one, but must not disable setup
    # supervision for run two when its first workload response is silent.
    peer = _Peer(mode='second-silent')
    result, _stats, failures, elapsed = _run(env, peer, ['--requests=4', '--run-count=2'], duration=None)
    env.assertEqual(result.returncode, 2, message=result.stderr[-2000:])
    env.assertContains('connection-stage failures', result.stderr)
    env.assertEqual(peer.connections, 2)
    env.assertEqual(peer.replies, 4)
    env.assertEqual(len(failures), 4)
    env.assertLess(elapsed, 8)


def test_accept_time_rejections_do_not_count_as_workload_progress(env):
    for reply in (b'-ERR max number of clients reached\r\n',
                  b'-ERR max number of clients + cluster connections reached\r\n',
                  b'-DENIED Redis is running in protected mode because of dummy settings\r\n'):
        # No AUTH/SELECT/HELLO: the unsolicited greeting can be read as the
        # first pipelined data reply. It must still be a startup failure.
        peer = _Peer(reply, mode='greeting')
        result, _stats, _failures, _ = _run(env, peer, ['--reconnect-on-error'], authenticate=False)
        env.assertEqual(result.returncode, 2, message=result.stderr[-2000:])
        env.assertContains('connection-stage failures', result.stderr)
        peer = _Peer(reply, mode='greeting')
        result, _stats, _failures, _ = _run(env, peer, ['--reconnect-on-error', '--max-reconnect-attempts=1',
                                                     '--connection-stage-timeout=0'],
                                          duration=5, authenticate=False)
        env.assertEqual(result.returncode, 1, message=result.stderr[-2000:])
        env.assertEqual(peer.connections, 2)
