"""Pipelined replies larger than one socket read must reach the parser intact.

libevent before 2.2 reads at most 4 KiB per read event, and memtier reads on
from what is queued, up to 16 KiB in all (issue #535); every byte must still
arrive once and in order. tests/unit/libevent_read_cap_test.cpp checks the
read sizes.
"""
import json
import os
import subprocess
import tempfile

from include import MEMTIER_BINARY, addTLSArgs

KEYS = 64
VALUE_SIZE = 3000


def _value(index):
    # Distinct per key and per position, so a lost, repeated or reordered
    # chunk fails verification rather than matching a neighbour's value.
    pattern = "{:04d}abcdefghijklmnopqrstuvwxyz".format(index)
    return (pattern * (VALUE_SIZE // len(pattern) + 1))[:VALUE_SIZE]


def test_pipelined_large_replies_verify(env):
    if env.isCluster():
        env.skip()  # --data-import is unsupported in cluster mode.
        return
    conn = env.getConnection()
    keys = ["large-reply-{:04d}".format(i) for i in range(KEYS)]
    try:
        for i, key in enumerate(keys):
            conn.set(key, _value(i))
        with tempfile.TemporaryDirectory() as directory:
            source = os.path.join(directory, "import.csv")
            output = os.path.join(directory, "result.json")
            with open(source, "w") as f:
                f.write("dumpflags, time, exptime, nbytes, nsuffix, it_flags, clsid, nkey, key, data\n")
                for i, key in enumerate(keys):
                    f.write("0, 0, 0, {}, 0, 0, 0, {}, {}, {}\n".format(VALUE_SIZE + 2, len(key), key, _value(i)))
            node = env.getMasterNodesList()[0]
            if env.isUnixSocket():
                target = ["--unix-socket", node["unix_socket_path"]]
            else:
                target = ["-s", "127.0.0.1", "-p", str(node["port"])]
            args = [MEMTIER_BINARY] + target + [
                    "--data-import", source, "--json-out-file", output, "--verify-only", "--ratio=1:0",
                    "--key-pattern=P:P", "-t", "1", "-c", "1", "-n", str(KEYS),
                    "--pipeline", "16", "--hide-histogram"]
            password = conn.connection_pool.connection_kwargs.get("password")
            if password:
                args.extend(["-a", password])
            addTLSArgs({"args": args}, env)
            try:
                result = subprocess.run(args, capture_output=True, text=True, timeout=30)
            except subprocess.TimeoutExpired as e:
                # A lost or extra byte leaves the parser waiting for a reply
                # that never completes.
                stderr = e.stderr.decode(errors="replace") if isinstance(e.stderr, bytes) else e.stderr
                env.assertTrue(False, message="memtier hung: {}".format(stderr))
                return
            env.assertEqual(result.returncode, 0, message=result.stderr)
            if result.returncode != 0:
                return
            env.assertNotContains("response parsing failed", result.stderr, message=result.stderr)
            with open(output) as f:
                counts = json.load(f)["client verifications results"]
            env.assertEqual(counts["keys verified successfuly"], KEYS, message=result.stderr)
            env.assertEqual(counts["keys failed"], 0, message=result.stderr)
    finally:
        conn.delete(*keys)
