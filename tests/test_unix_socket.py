"""Unix socket address regression tests; peers are private and need no TCP port.

The peers speak enough RESP to verify AUTH and a fixed number of SET replies.
They deliberately bind the exact abstract name length: an extra trailing NUL
in connect(2)'s address therefore fails rather than being hidden by the fixture.
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import uuid

from include import MEMTIER_BINARY


class _UnixPeer:
    def __init__(self, address):
        self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.listener.bind(address)
        self.listener.listen(1)
        self.listener.settimeout(0.1)
        self.stop = threading.Event()
        self.commands = []
        self.errors = []
        self.thread = threading.Thread(target=self._serve, daemon=True)

    def _serve(self):
        try:
            while not self.stop.is_set():
                try:
                    conn, _ = self.listener.accept()
                    break
                except socket.timeout:
                    continue
            else:
                return
            with conn:
                conn.settimeout(5)
                with conn.makefile('rb') as stream:
                    while not self.stop.is_set():
                        line = stream.readline()
                        if not line:
                            break
                        assert line.startswith(b'*'), line
                        args = []
                        for _ in range(int(line[1:])):
                            size = stream.readline()
                            assert size.startswith(b'$'), size
                            args.append(stream.read(int(size[1:])))
                            assert stream.read(2) == b'\r\n'
                        self.commands.append(args)
                        if args == [b'AUTH', b'unix-test-password']:
                            conn.sendall(b'+OK\r\n')
                        elif args[0] == b'SET' and self.commands[0][0] == b'AUTH':
                            conn.sendall(b'+OK\r\n')
                        else:
                            raise AssertionError('unexpected command: {!r}'.format(args))
        except Exception as error:
            self.errors.append(repr(error))

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop.set()
        self.thread.join(6)
        self.listener.close()
        assert not self.thread.is_alive(), 'Unix peer did not stop'


def _run(name, directory):
    return subprocess.run(
        [os.path.abspath(MEMTIER_BINARY), '-S', name, '-a', 'unix-test-password',
         '-t', '1', '-c', '1', '-n', '16', '--ratio', '1:0',
         '--json-out-file', os.path.join(directory, 'result.json')],
        cwd=directory, capture_output=True, text=True, timeout=10,
    )


def _check_connection(env, address, name, directory):
    with _UnixPeer(address) as peer:
        result = _run(name, directory)
    env.assertEqual(result.returncode, 0, message=result.stderr)
    env.assertEqual(peer.errors, [])
    env.assertEqual(peer.commands[0], [b'AUTH', b'unix-test-password'])
    env.assertEqual(sum(command[0] == b'SET' for command in peer.commands), 16)
    with open(os.path.join(directory, 'result.json')) as report:
        stats = json.load(report)['ALL STATS']
    env.assertEqual(stats['Sets']['Count'], 16)


def test_unix_filesystem_socket(env):
    # Short /tmp prefix also fits the smaller sockaddr_un used on macOS/BSD.
    with tempfile.TemporaryDirectory(prefix='mt-unix-', dir='/tmp') as directory:
        path = os.path.join(directory, 'server.sock')
        _check_connection(env, path, path, directory)


def test_unix_filesystem_at_name(env):
    # ./ escapes a filesystem name beginning with @ on Linux. Elsewhere even
    # a bare @ continues to mean an ordinary relative filesystem pathname.
    with tempfile.TemporaryDirectory(prefix='mt-unix-', dir='/tmp') as directory:
        name = './@server' if sys.platform.startswith('linux') else '@server'
        _check_connection(env, os.path.join(directory, '@server'), name, directory)


def test_unix_abstract_socket(env):
    if not sys.platform.startswith('linux'):
        env.skip()
        return
    with tempfile.TemporaryDirectory(prefix='mt-unix-', dir='/tmp') as directory:
        name = 'memtier-' + uuid.uuid4().hex
        _check_connection(env, b'\0' + name.encode(), '@' + name, directory)


def test_unix_abstract_socket_maximum_length(env):
    if not sys.platform.startswith('linux'):
        env.skip()
        return
    with tempfile.TemporaryDirectory(prefix='mt-unix-', dir='/tmp') as directory:
        # Linux sun_path has 108 bytes; the leading NUL uses one. Count encoded
        # bytes, not Unicode characters, and use the final byte of sun_path.
        name = 'memtier-' + uuid.uuid4().hex + '\N{LATIN SMALL LETTER E WITH ACUTE}'
        name += 'x' * (107 - len(name.encode()))
        _check_connection(env, b'\0' + name.encode(), '@' + name, directory)


def test_unix_abstract_socket_rejects_overlong_name(env):
    if not sys.platform.startswith('linux'):
        env.skip()
        return
    with tempfile.TemporaryDirectory(prefix='mt-unix-', dir='/tmp') as directory:
        prefix = ('memtier-' + uuid.uuid4().hex).ljust(107, 'x')
        # A listener at the truncated name proves rejection rather than silent
        # truncation to a different endpoint. Exercise both one byte too many
        # and a size large enough to expose the original heap overwrite.
        for suffix in ('x', 'x' * 4096):
            with _UnixPeer(b'\0' + prefix.encode()) as peer:
                result = _run('@' + prefix + suffix, directory)
            env.assertEqual(result.returncode, 1, message=result.stderr)
            env.assertTrue('abstract UNIX socket name is too long' in result.stderr,
                           message=result.stderr)
            env.assertEqual(peer.commands, [])
            env.assertEqual(peer.errors, [])


if __name__ == '__main__':
    # Also run on macOS build jobs without installing RLTest or starting Redis.
    import unittest

    class UnixSocketTests(unittest.TestCase):
        def assertEqual(self, first, second, message=''):
            super().assertEqual(first, second, message)

        def assertTrue(self, value, message=''):
            super().assertTrue(value, message)

        def skip(self):
            self.skipTest('abstract Unix sockets require Linux')

    for name, test in list(globals().items()):
        if name.startswith('test_'):
            setattr(UnixSocketTests, name, lambda self, test=test: test(self))
    unittest.main(defaultTest="UnixSocketTests")
