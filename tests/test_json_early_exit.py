"""Issue #769: exit() after opening JSON must leave a parseable document.

The Unix socket peers exercise synchronous connect/reconnect failures even in
TLS and cluster CI cells: these tests deliberately use standalone plaintext.
The allocation-failure cases stop before connection setup.
"""
import json
import os
import resource
import shutil
import sys
import socket
import subprocess
import tempfile
import threading

from include import MEMTIER_BINARY


def _run(env, directory, args, **kwargs):
    path = os.path.join(directory, "result.json")
    endpoint = [] if "-S" in args else ["-s", "127.0.0.1", "-p", str(env.getMasterNodesList()[0]["port"])]
    result = subprocess.run(
        [MEMTIER_BINARY] + endpoint + ["-t", "1", "-c", "1",
         "--requests=10", "--hide-histogram", "--json-out-file", path] + args,
        capture_output=True, text=True, timeout=20, **kwargs)
    return result, path


def _configuration_only(env, result, path, diagnostic):
    env.assertEqual(result.returncode, 1, message=result.stderr[-4000:])
    env.assertContains(diagnostic, result.stderr)
    with open(path) as output:
        document = json.load(output)
    env.assertEqual(list(document), ["configuration"])
    env.assertEqual(document["configuration"]["threads"], 1)
    env.assertEqual(result.stderr.count("Json file closed."), 1)


def test_json_closed_on_thread_prepare_failure(env):
    with tempfile.TemporaryDirectory() as directory:
        result, path = _run(env, directory, ["-S", os.path.join(directory, "missing.sock")])
        _configuration_only(env, result, path, "failed to prepare thread 0")


def test_json_closed_on_worker_reconnect_failure(env):
    with tempfile.TemporaryDirectory() as directory:
        # Keep the Unix socket below the sockaddr_un path length on macOS too.
        address = os.path.join(directory, "s")
        errors = []
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind(address)
            listener.listen(1)
            listener.settimeout(15)

            def reply_once():
                try:
                    with listener.accept()[0] as connection:
                        connection.settimeout(15)
                        # Removal before the reply guarantees that reconnect
                        # fails synchronously, rather than via a later callback.
                        os.unlink(address)
                        with connection.makefile("rb") as source:
                            count = int(source.readline()[1:])
                            command = []
                            for _ in range(count):
                                length = int(source.readline()[1:])
                                command.append(source.read(length))
                                assert source.read(2) == b"\r\n"
                            assert command[0].upper() == b"SET", command
                        connection.sendall(b"+OK\r\n")
                except Exception as error:
                    errors.append(repr(error))

            peer = threading.Thread(target=reply_once, daemon=True)
            peer.start()
            try:
                result, path = _run(env, directory, ["-S", address, "--ratio=1:0", "--reconnect-interval=1"])
            finally:
                peer.join(timeout=16)
            env.assertFalse(peer.is_alive())
            env.assertEqual(errors, [])
        _configuration_only(env, result, path, "failed to reconnect.")


def _mget_allocation_failure(env, cluster):
    # ASan/TSan reserve large shadow mappings and cannot run under RLIMIT_AS.
    # This real allocation failure needs no preload shim and uses < 300 MiB
    # physical memory: the subsequent 4 GiB key buffer is rejected by the limit.
    if sys.platform != "linux" or not shutil.which("ldd"):
        env.skip()
        return
    linked = subprocess.run(["ldd", MEMTIER_BINARY], capture_output=True, text=True, timeout=10)
    if linked.returncode or any(name in linked.stdout for name in ("libasan", "libtsan")):
        env.skip()
        return

    def limit_memory():
        resource.setrlimit(resource.RLIMIT_AS, (2_000_000_000, 2_000_000_000))

    with tempfile.TemporaryDirectory() as directory:
        args = ["--multi-key-get=16777214"] + (["--cluster-mode"] if cluster else [])
        result, path = _run(env, directory, args, preexec_fn=limit_memory)
        _configuration_only(env, result, path, "out of memory building MGET key list")


def test_json_closed_on_mget_allocation_failure(env):
    _mget_allocation_failure(env, False)


def test_json_closed_on_cluster_mget_allocation_failure(env):
    _mget_allocation_failure(env, True)


def test_json_untouched_before_startup_validation(env):
    with tempfile.TemporaryDirectory() as directory:
        path = os.path.join(directory, "result.json")
        with open(path, "w") as output:
            output.write('{"existing": true}\n')
        result, path = _run(env, directory, ["--data-size=10", "--data-size-list=10:1"])
        env.assertEqual(result.returncode, 2, message=result.stderr)
        with open(path) as output:
            env.assertEqual(output.read(), '{"existing": true}\n')
