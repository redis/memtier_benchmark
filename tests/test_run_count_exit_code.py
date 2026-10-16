"""The exit code of a --run-count series reflects every run (#739).

A --connection-stage-timeout abort exits 2 and a worker failure exits 1. With
--run-count > 1, an abort in a run other than the last one used to be
forgotten, so the series exited 0 (or 1). An abort in any run must exit 2,
and 2 wins over 1, as it already does within one run.

A scripted RESP peer decides what happens to each run. With one thread, one
client and no --reconnect-on-error, memtier opens one connection per run, so
the n-th connection belongs to run n:
  "ok"    answers every command, so the run completes;
  "abort" rejects AUTH, so --connection-stage-timeout aborts the run;
  "drop"  closes the connection after a few data commands, so the worker
          ends early and the run fails (exit 1). Answering some first takes
          the run past the connection stage, so this is not an abort;
  "refuse" (--unix-socket only) removes the socket when the previous
          connection closes, so this run cannot connect and memtier exits
          from thread setup without finishing the series.
"""
import json
import os
import socketserver
import subprocess
import tempfile
import threading

from include import MEMTIER_BINARY

# A "drop" connection answers this many data commands, then closes on the
# next one; fewer than --requests, so the run cannot complete.
_DROP_AFTER = 9


class _Plan:
    def __init__(self, plan):
        self.lock = threading.Lock()
        self.plan = plan
        self.connections = 0
        self.errors = []

    def next_mode(self):
        with self.lock:
            self.connections += 1
            n = self.connections
        return self.plan[n - 1] if n <= len(self.plan) else "ok"

    def connection_closed(self):
        with self.lock:
            n = self.connections
        if n < len(self.plan) and self.plan[n] == "refuse":
            os.unlink(self.server_address)


class _TCPServer(_Plan, socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, plan):
        _Plan.__init__(self, plan)
        socketserver.ThreadingTCPServer.__init__(self, ("127.0.0.1", 0), _Handler)


class _UnixServer(_Plan, socketserver.ThreadingUnixStreamServer):
    daemon_threads = True

    def __init__(self, plan, path):
        _Plan.__init__(self, plan)
        socketserver.ThreadingUnixStreamServer.__init__(self, path, _Handler)


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        mode = self.server.next_mode()
        data_commands = 0
        try:
            while True:
                line = self.rfile.readline()
                if not line:
                    return
                args = []
                for _ in range(int(line[1:])):
                    size = self.rfile.readline()
                    args.append(self.rfile.read(int(size[1:]) + 2)[:-2])
                verb = args[0].upper()
                if verb in (b"SET", b"GET"):
                    data_commands += 1
                if verb == b"AUTH" and mode == "abort":
                    reply = b"-WRONGPASS invalid username-password pair\r\n"
                elif verb in (b"SET", b"GET") and mode == "drop" and data_commands > _DROP_AFTER:
                    return
                elif verb == b"GET":
                    reply = b"$-1\r\n"
                else:
                    reply = b"+OK\r\n"
                self.wfile.write(reply)
                self.wfile.flush()
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as error:
            self.server.errors.append(repr(error))
        finally:
            self.server.connection_closed()


def _run(env, plan, unix_socket=False):
    if env.isCluster() or env.useTLS:
        # Only the scripted peer is used; run once, in the plaintext standalone cell.
        env.skip()
    tmpdir = tempfile.mkdtemp()
    if unix_socket:
        server = _UnixServer(plan, os.path.join(tmpdir, "s"))
        target = ["--unix-socket={}".format(server.server_address)]
    else:
        server = _TCPServer(plan)
        target = ["--server=127.0.0.1", "--port={}".format(server.server_address[1])]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    json_path = os.path.join(tmpdir, "out.json")
    args = [MEMTIER_BINARY] + target + ["--threads=1", "--clients=1", "--authenticate=pw", "--requests=100",
            "--connection-stage-timeout=1", "--run-count={}".format(len(plan)),
            "--hide-histogram", "--json-out-file={}".format(json_path)]
    try:
        result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                                timeout=60, text=True)
        # A series that ends early leaves no complete JSON.
        results = None
        if "refuse" not in plan:
            with open(json_path) as f:
                results = json.load(f)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        for name in os.listdir(tmpdir):
            os.unlink(os.path.join(tmpdir, name))
        os.rmdir(tmpdir)
    env.assertEqual(server.errors, [])
    env.assertEqual(server.connections, len(plan) - plan.count("refuse"), message=result.stderr[-2000:])
    env.assertEqual(result.stderr.count("See --connection-stage-timeout."), plan.count("abort"),
                    message=result.stderr[-2000:])
    return result.returncode, result.stderr, results


def test_first_run_abort_exits_2(env):
    rc, stderr, results = _run(env, ["abort", "ok", "ok"])
    env.assertEqual(rc, 2, message=stderr[-2000:])
    # The aborted run stays in the series and marks the average.
    env.assertEqual(results["AGGREGATED AVERAGE RESULTS (3 runs)"]["Runtime"]["Interrupted"], "true")
    env.assertEqual(results["WORST RUN RESULTS"]["Runtime"]["Interrupted"], "true")
    env.assertEqual(results["BEST RUN RESULTS"]["Runtime"]["Interrupted"], "false")


def test_middle_run_abort_exits_2(env):
    rc, stderr, results = _run(env, ["ok", "abort", "ok"])
    env.assertEqual(rc, 2, message=stderr[-2000:])
    env.assertEqual(results["AGGREGATED AVERAGE RESULTS (3 runs)"]["Runtime"]["Interrupted"], "true")


def test_last_run_abort_exits_2(env):
    rc, stderr, _ = _run(env, ["ok", "ok", "abort"])
    env.assertEqual(rc, 2, message=stderr[-2000:])


def test_abort_wins_over_worker_failure_in_a_later_run(env):
    rc, stderr, _ = _run(env, ["abort", "drop"])
    env.assertEqual(rc, 2, message=stderr[-2000:])
    env.assertContains("the results of this run are not valid", stderr)


def test_abort_wins_over_worker_failure_in_an_earlier_run(env):
    rc, stderr, _ = _run(env, ["drop", "abort"])
    env.assertEqual(rc, 2, message=stderr[-2000:])
    env.assertContains("the results of this run are not valid", stderr)


def test_abort_wins_over_connect_failure_in_a_later_run(env):
    # Run 2 cannot connect, and memtier exits while preparing its threads,
    # before main() returns.
    rc, stderr, _ = _run(env, ["abort", "refuse"], unix_socket=True)
    env.assertEqual(rc, 2, message=stderr[-2000:])
    env.assertContains("failed to prepare thread", stderr)


def test_connect_failure_in_a_later_run_exits_1(env):
    rc, stderr, _ = _run(env, ["ok", "refuse"], unix_socket=True)
    env.assertEqual(rc, 1, message=stderr[-2000:])
    env.assertContains("failed to prepare thread", stderr)


def test_worker_failure_in_an_earlier_run_exits_1(env):
    rc, stderr, _ = _run(env, ["drop", "ok"])
    env.assertEqual(rc, 1, message=stderr[-2000:])


def test_all_runs_ok_exits_0(env):
    rc, stderr, _ = _run(env, ["ok", "ok"])
    env.assertEqual(rc, 0, message=stderr[-2000:])
