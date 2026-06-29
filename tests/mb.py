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

    def run(self, timeout=None):
        """Run memtier_benchmark to completion.

        timeout: optional hard upper bound (seconds) on the child process.
        A test that passes one (e.g. soak/test_slow_network uses
        test_time + 90) gets fail-fast behaviour: on expiry we kill the
        child, drain its pipes, write a truncated mb.stderr, and return
        False instead of hanging until the CI job cap.

        Defaults to None (wait indefinitely) to preserve the prior 2.4
        behaviour for the many existing callers that don't pass a timeout
        -- adopting master's 240s default here would risk killing long
        soak/staircase runs that legitimately exceed 240s. (master uses a
        240s default; 2.4 keeps opt-in to stay regression-free.)
        """
        logging.debug('  Command: %s', ' '.join(self.args))
        process = subprocess.Popen(
            stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            executable=self.binary, args=self.args)
        try:
            _stdout, _stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired as e:
            logging.error(
                '  memtier_benchmark exceeded %ds timeout; killing child', timeout)
            process.kill()
            # Preserve whatever stderr was already captured before the timeout
            # (carried on the TimeoutExpired instance); the post-kill drain
            # frequently returns empty, which would otherwise lose the log.
            captured_err = e.stderr or b''
            try:
                _stdout, _stderr = process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                _stdout, _stderr = b'', b''
            _stderr = captured_err + (_stderr or b'')
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

