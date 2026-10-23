"""Empty final histograms have no observed latency, including arbitrary rows (#638)."""
import json
import os
import subprocess
import tempfile
import threading

from include import MEMTIER_BINARY
from test_hdr_histogram import _decode_v2, _v2_counts, _percentile
from test_workload_error_progress import _Peer


_PERCENTILES = [100, 0, 99.999, 50]


def _check(env, extra, reject=False, monitor=False):
    peer = _Peer(reply=b'+OK\r\n', reject=b'AUTH' if reject else None)
    thread = threading.Thread(target=peer.serve_forever, daemon=True)
    thread.start()
    try:
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, 'stats.json')
            args = [MEMTIER_BINARY, '-s', '127.0.0.1', '-p', str(peer.server_address[1]),
                    '-a', 'dummy-empty-histogram', '-t', '1', '-c', '1', '-n', '1',
                    '--pipeline=1', '--hide-histogram', '--connection-stage-timeout=1',
                    '--json-out-file=' + path, '--print-percentiles=' + ','.join(map(str, _PERCENTILES))]
            if monitor:
                source = os.path.join(directory, 'monitor.txt')
                with open(source, 'w') as stream:
                    stream.write('1700000000.000000 [0 127.0.0.1:1234] "SET" "k" "v"\n')
                args += ['--monitor-input=' + source, '--command=__monitor_line@__']
            result = subprocess.run(args + extra, capture_output=True, text=True, timeout=15)
            env.assertEqual(result.returncode, 2 if reject else 0, message=result.stderr)
            with open(path) as stream:
                document = json.load(stream, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
            empty, populated = set(), set()
            for section in document.values():
                if not isinstance(section, dict):
                    continue
                for name, row in section.items():
                    if not isinstance(row, dict) or 'Percentile Latencies' not in row:
                        continue
                    quantiles = row['Percentile Latencies']
                    count, raw = _decode_v2(quantiles['Histogram log format']['Compressed Histogram'])
                    values = {float(k[1:]): v for k, v in quantiles.items() if k.startswith('p')}
                    env.assertEqual(set(values), set(_PERCENTILES))
                    if count == 0:
                        empty.add(name)
                        env.assertEqual(row['Count'], 0)
                        for key in ('Latency', 'Average Latency', 'Min Latency', 'Max Latency'):
                            env.assertEqual(row[key], 0)
                        env.assertEqual(list(values.values()), [0] * len(_PERCENTILES))
                    else:
                        populated.add(name)
                        env.assertTrue(row['Count'] > 0)
                        decoded = _v2_counts(raw)
                        env.assertEqual([values[q] for q in _PERCENTILES],
                                        [_percentile(decoded, q) / 1000 for q in _PERCENTILES])
            env.assertTrue(bool(empty), message='fixture must exercise an empty histogram')
            env.assertEqual(bool(populated), not reject)
            checked = set()
            for line in result.stdout.splitlines():
                fields = line.split()
                if fields and fields[0] in empty:
                    # Last cell is KB/sec; preceding cells are average plus configured quantiles.
                    env.assertEqual(fields[-len(_PERCENTILES)-2:-1], ['---'] * (len(_PERCENTILES) + 1))
                    checked.add(fields[0])
            env.assertEqual(checked, empty)
            env.assertEqual(peer.requests, 0 if reject else (2 if '--run-count=2' in extra else 1))
            env.assertEqual(peer.failures, [])
    finally:
        peer.stop.set()
        peer.shutdown()
        peer.server_close()
        thread.join(timeout=3)


def test_empty_builtin_and_total_histograms(env):
    _check(env, ['--ratio=1:0'])
    _check(env, ['--ratio=1:0'], reject=True)


def test_empty_arbitrary_and_average_histograms(env):
    for mode in ('line', 'command'):
        _check(env, ['--command=SET a b', '--command=GET a', '--command-stats-breakdown=' + mode])
        _check(env, ['--command=SET a b', '--command=GET a', '--command-stats-breakdown=' + mode,
                     '--run-count=2', '--print-all-runs'])


def test_empty_monitor_placeholder_histogram(env):
    _check(env, ['--command-stats-breakdown=line'], monitor=True)
