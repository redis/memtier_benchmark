"""Verification failures must be visible to shell callers (#768)."""
import json
import os
import socket
import subprocess
import tempfile
import threading

from include import MEMTIER_BINARY, addTLSArgs


def _run(env, state, verify_only=True, extra=(), port=None):
    if env.isCluster():
        env.skip()  # --data-import is explicitly unsupported in cluster mode.
        return
    conn = env.getConnection()
    keys = ["verify-exit-01", "verify-exit-02"]
    conn.delete(*keys)
    if state != "missing":
        conn.set(keys[0], "abc")
        if state == "wrongtype":
            conn.lpush(keys[1], "def")
        else:
            conn.set(keys[1], "zzz" if state == "wrong" else "def")
    try:
        with tempfile.TemporaryDirectory() as directory:
            source = os.path.join(directory, "import.csv")
            output = os.path.join(directory, "result.json")
            with open(source, "w") as f:
                f.write("dumpflags, time, exptime, nbytes, nsuffix, it_flags, clsid, nkey, key, data\n")
                for key, value in zip(keys, ("abc", "def")):
                    f.write("0, 0, 0, 5, 0, 0, 0, {}, {}, {}\n".format(len(key), key, value))
            args = [MEMTIER_BINARY, "-s", "127.0.0.1", "-p",
                    str(port if port is not None else env.getMasterNodesList()[0]["port"]),
                    "--data-import", source, "--json-out-file", output,
                    "--ratio=1:0", "--key-pattern=P:P", "-t", "1", "-c", "1", "-n", "2",
                    "--hide-histogram", "--verify-only" if verify_only else "--data-verify"]
            password = conn.connection_pool.connection_kwargs.get("password")
            if password:
                args.extend(["-a", password])
            addTLSArgs({"args": args}, env)
            if "--unix-socket" in extra:
                del args[1:5]  # --server/--port conflict with the explicit socket.
            args.extend(extra)
            result = subprocess.run(args, capture_output=True, text=True, timeout=20)
            with open(output) as f:
                data = json.load(f)
            return result, data["client verifications results"]
    finally:
        conn.delete(*keys)


def _check(env, state, verified, failed, verify_only=True, extra=()):
    if env.isCluster():
        env.skip()
        return
    result, counts = _run(env, state, verify_only, extra)
    env.assertEqual(counts["keys verified successfuly"], verified)
    env.assertEqual(counts["keys failed"], failed)
    env.assertEqual(result.returncode, 1 if failed else 0, message=result.stderr)
    if failed:
        env.assertContains("error: {} of {} keys failed verification".format(failed, verified + failed), result.stderr)
    else:
        env.assertNotContains("keys failed verification", result.stderr)
        env.assertNotContains("no keys were verified", result.stderr)
        env.assertNotContains("data verification did not complete", result.stderr)


def test_verify_only_missing(env):
    _check(env, "missing", 0, 2)


def test_verify_only_wrong_value(env):
    _check(env, "wrong", 1, 1)


def test_verify_only_error_reply(env):
    _check(env, "wrongtype", 1, 1)


def test_verify_only_success(env):
    _check(env, "correct", 2, 0)


def test_data_verify_success(env):
    # The benchmark first writes the missing keys, then verifies them.
    _check(env, "missing", 2, 0, verify_only=False)


def test_data_verify_error_reply(env):
    # SETRANGE and GETRANGE both fail on the list; the other key verifies.
    _check(env, "wrongtype", 1, 1, verify_only=False, extra=("--data-offset=1",))


def test_verify_only_wraps_import(env):
    _check(env, "correct", 5, 0, extra=("-n", "5"))


def test_verify_only_default_ratio(env):
    # Only SET positions issue verification GETs; skipped GET positions still
    # consume the request budget. The pass must not require five replies.
    _check(env, "correct", 1, 0, extra=("--ratio=1:10", "-n", "5"))


def test_verify_only_no_replies(env):
    if env.isCluster():
        env.skip()
        return
    # Reserve a non-listening socket: connection refusal without racing another
    # process for a recently released port.
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", env.getMasterNodesList()[0]["port"] + 50))
        result, counts = _run(env, "correct", port=sock.getsockname()[1])
    env.assertEqual(counts["keys verified successfuly"], 0)
    env.assertEqual(counts["keys failed"], 0)
    env.assertEqual(result.returncode, 1, message=result.stderr)
    env.assertContains("error: no keys were verified", result.stderr)


def test_verify_only_partial_disconnect(env):
    if env.isCluster() or env.useTLS:
        env.skip()  # This scripted peer speaks plaintext RESP on a Unix socket.
        return
    with tempfile.TemporaryDirectory() as directory, socket.socket(socket.AF_UNIX) as server:
        path = os.path.join(directory, "peer.sock")
        server.bind(path)
        server.listen(1)
        server.settimeout(10)
        errors = []

        def respond():
            try:
                with server.accept()[0] as conn:
                    conn.settimeout(10)
                    with conn.makefile("rb") as stream:
                        gets = 0
                        while True:
                            line = stream.readline()
                            if not line:
                                return
                            args = []
                            for _ in range(int(line[1:])):
                                size = int(stream.readline()[1:])
                                args.append(stream.read(size + 2)[:-2])
                            if args[0].upper() == b"GET":
                                gets += 1
                                if gets == 2:
                                    return  # Drop after one successful verification.
                                conn.sendall(b"$3\r\nabc\r\n")
                            else:
                                conn.sendall(b"+OK\r\n")
            except Exception as error:
                errors.append(repr(error))

        thread = threading.Thread(target=respond, daemon=True)
        thread.start()
        try:
            result, counts = _run(env, "correct", extra=("--unix-socket", path))
        finally:
            thread.join(timeout=12)
        env.assertFalse(thread.is_alive())
        env.assertEqual(errors, [])
        env.assertEqual(counts["keys verified successfuly"], 1)
        env.assertEqual(counts["keys failed"], 0)
        env.assertEqual(result.returncode, 1, message=result.stderr)
        env.assertContains("error: data verification did not complete", result.stderr)
