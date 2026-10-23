"""Repeated random-monitor commands share one per-type statistics mapping (#639)."""
import collections
import json
import os
import socketserver
import subprocess
import tempfile
import threading

from include import MEMTIER_BINARY
from test_hdr_histogram import _decode_v2, _hgrm_count


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        self.connection.settimeout(10)
        authenticated = False
        try:
            while True:
                line = self.rfile.readline()
                if not line:
                    return
                if not line.startswith(b'*'):
                    raise ValueError('expected RESP array')
                args = []
                for _ in range(int(line[1:])):
                    size = int(self.rfile.readline()[1:])
                    args.append(self.rfile.read(size))
                    if self.rfile.read(2) != b'\r\n':
                        raise ValueError('bad argument terminator')
                verb = args[0].upper()
                if verb == b'AUTH':
                    authenticated = args[1:] == [b'dummy-monitor-639']
                    self.wfile.write(b'+OK\r\n' if authenticated else b'-ERR auth\r\n')
                    continue
                if not authenticated:
                    raise ValueError('workload before authentication')
                if verb not in (b'SET', b'GET', b'INCR'):
                    raise ValueError('unexpected command: ' + repr(args))
                with self.server.lock:
                    self.server.counts[verb.decode()] += 1
                self.wfile.write(b'$2\r\nv1\r\n' if verb == b'GET' else
                                 b':1\r\n' if verb == b'INCR' else b'+OK\r\n')
        except OSError:
            pass
        except Exception as error:
            self.server.errors.append(repr(error))


class _Peer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def _check(env, count, mode, mixed=False):
    with _Peer(('127.0.0.1', 0), _Handler) as peer:
        peer.lock, peer.counts, peer.errors = threading.Lock(), collections.Counter(), []
        thread = threading.Thread(target=peer.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                monitor = os.path.join(directory, 'monitor.txt')
                output = os.path.join(directory, 'stats.json')
                prefix = os.path.join(directory, 'hdr')
                with open(monitor, 'w') as stream:
                    stream.write('1700000000.000000 [0 127.0.0.1:1234] "SET" "k1" "v1"\n'
                                 '1700000000.000001 [0 127.0.0.1:1234] "GET" "k1"\n')
                commands = ['__monitor_line@__'] * count
                if mixed:
                    commands.insert(1, 'INCR ordinary')
                args = [MEMTIER_BINARY, '-s', '127.0.0.1', '-p', str(peer.server_address[1]),
                        '-a', 'dummy-monitor-639', '-t', '2', '-c', '2', '-n', '120',
                        '--pipeline=4', '--hide-histogram', '--monitor-input=' + monitor,
                        '--command-stats-breakdown=' + mode, '--json-out-file=' + output,
                        '--hdr-file-prefix=' + prefix]
                for index, command in enumerate(commands):
                    args += ['--command', command]
                    if mixed:
                        args += ['--command-ratio=' + str(index + 1)]
                result = subprocess.run(args, capture_output=True, text=True, timeout=20)
                env.assertEqual(result.returncode, 0, message=result.stderr)
                env.assertEqual(peer.errors, [])
                env.assertEqual(sum(peer.counts.values()), 480)
                env.assertEqual(peer.counts['INCR'], 160 if mixed else 0)
                with open(output) as stream:
                    stats = json.load(stream)['ALL STATS']
                for name, verb in [('Sets', 'SET'), ('Gets', 'GET')]:
                    matching = [key for key in stats if key == name or key.startswith(name + ' #')]
                    env.assertEqual(matching, [name], message='one shared monitor type row')
                    env.assertTrue(peer.counts[verb] > 0)
                    env.assertEqual(stats[name]['Count'], peer.counts[verb])
                    encoded = stats[name]['Percentile Latencies']['Histogram log format']['Compressed Histogram']
                    env.assertEqual(_decode_v2(encoded)[0], peer.counts[verb])
                    env.assertEqual(_hgrm_count(prefix + '_' + verb + '_command_run_1.hgrm'), peer.counts[verb])
                    rows = [line for line in result.stdout.splitlines() if line.startswith(name + ' ')]
                    env.assertEqual(len(rows), 1)
                env.assertEqual(stats['Totals']['Count'], 480)
                env.assertEqual(_hgrm_count(prefix + '_FULL_RUN_1.hgrm'), 480)
                if mixed:
                    env.assertEqual(stats['Incrs']['Count'], 160)
                env.assertFalse(any('_SET_2_' in name or '_GET_2_' in name for name in os.listdir(directory)))
        finally:
            peer.shutdown()
            thread.join(timeout=3)


def test_random_monitor_single_and_repeated_slots(env):
    for mode in ('line', 'command'):
        _check(env, 1, mode)
        _check(env, 2, mode)


def test_random_monitor_mixed_command_ratios(env):
    for mode in ('line', 'command'):
        _check(env, 2, mode, mixed=True)
