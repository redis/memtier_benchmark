"""
Simple replacement for mbdirector package.
Contains only the Benchmark and RunConfig classes needed for tests.
"""
import os
import subprocess
import logging


class RunConfig(object):
    """Configuration for a benchmark run."""
    next_id = 1

    def __init__(self, base_results_dir, name, config, benchmark_config):
        self.id = RunConfig.next_id
        RunConfig.next_id += 1

        self.redis_process_port = config.get('redis_process_port', 6379)

        mbconfig = config.get('memtier_benchmark', {})
        mbconfig.update(benchmark_config)
        self.mb_binary = mbconfig.get('binary', 'memtier_benchmark')
        self.mb_threads = mbconfig.get('threads')
        self.mb_clients = mbconfig.get('clients')
        self.mb_pipeline = mbconfig.get('pipeline')
        self.mb_requests = mbconfig.get('requests')
        self.mb_test_time = mbconfig.get('test_time')
        self.explicit_connect_args = bool(
            mbconfig.get('explicit_connect_args'))

        self.results_dir = os.path.join(base_results_dir,
                                        '{:04}_{}'.format(self.id, name))

    def __repr__(self):
        return '<RunConfig id={}>'.format(self.id)


class Benchmark(object):
    """Benchmark runner for memtier_benchmark."""
    
    def __init__(self, config, **kwargs):
        self.config = config
        self.binary = self.config.mb_binary
        self.name = kwargs['name']

        # Configure
        self.args = [self.binary]
        if not self.config.explicit_connect_args:
            self.args += ['--server', '127.0.0.1',
                          '--port', str(self.config.redis_process_port)
                          ]
        self.args += ['--out-file', os.path.join(config.results_dir,
                                                 'mb.stdout'),
                      '--json-out-file', os.path.join(config.results_dir,
                                                      'mb.json')]

        if self.config.mb_threads is not None:
            self.args += ['--threads', str(self.config.mb_threads)]
        if self.config.mb_clients is not None:
            self.args += ['--clients', str(self.config.mb_clients)]
        if self.config.mb_pipeline is not None:
            self.args += ['--pipeline', str(self.config.mb_pipeline)]
        if self.config.mb_requests is not None:
            self.args += ['--requests', str(self.config.mb_requests)]
        if self.config.mb_test_time is not None:
            self.args += ['--test-time', str(self.config.mb_test_time)]

        self.args += kwargs['args']

    @classmethod
    def from_json(cls, config, json):
        return cls(config, **json)

    def write_file(self, name, data):
        with open(os.path.join(self.config.results_dir, name), 'wb') as outfile:
            outfile.write(data)

    def run(self, timeout=240):
        """Run memtier_benchmark to completion.

        timeout: hard upper bound (seconds) on the child process. A real
        spin or hang in memtier would otherwise block communicate() until
        the CI job's 6-hour cap, which produces no diagnostic. When the
        timeout fires we kill the child, drain its pipes, write a
        truncated mb.stderr, and return False so the calling test fails
        fast rather than hanging.

        The default of 240s accommodates ASAN+TLS+reconnect-heavy workloads
        with margin. test_short_reconnect_interval, for example, runs 50,000
        ops at --reconnect-interval=1 with a full TLS handshake per op and
        sustains only ~380-450 ops/sec under ASAN+TLS (110-130s wall time),
        so a tighter default would flake on slow CI cells. Spin-guard tests
        that need a tight bound to fail fast on a real hang still override
        with a small explicit timeout (e.g. timeout=20 in
        test_read_preference_mget).
        """
        logging.debug('  Command: %s', ' '.join(self.args))
        process = subprocess.Popen(
            stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            executable=self.binary, args=self.args)
        try:
            _stdout, _stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            logging.error(
                '  memtier_benchmark exceeded %ds timeout; killing child', timeout)
            process.kill()
            try:
                _stdout, _stderr = process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                _stdout, _stderr = b'', b''
            if _stderr:
                self.write_file('mb.stderr', _stderr)
            self.write_file(
                'mb.timeout',
                'memtier_benchmark timed out after {}s\n'.format(timeout).encode())
            return False
        if _stderr:
            logging.debug('  >>> stderr <<<\n%s\n', _stderr)
            self.write_file('mb.stderr', _stderr)
        return process.wait() == 0

