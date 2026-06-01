"""Userspace TCP proxy that mirrors Redis traffic to log files plus a tiny
RESP canonicalizer for byte-equivalence assertions across clients.

Usage (programmatic):

    tap = RedisTap(upstream_host="127.0.0.1", upstream_port=6379,
                   client_log="/tmp/c2s.bin", server_log="/tmp/s2c.bin")
    tap.start()
    try:
        # point client at tap.listen_port; bytes will be mirrored to logs
        ...
    finally:
        tap.stop()

The canonicalizer is deliberately minimal: it parses RESP arrays of bulk
strings, uppercases the verb, keeps argument order, and recomputes every
length prefix. It is *not* a full RESP parser — inline commands, RESP3,
push messages and big-number / map / set frames are out of scope. Its only
job is to let two clients that issue logically-identical commands produce
byte-identical canonical forms regardless of casing or whitespace in the
verb.
"""

import os
import socket
import threading


class RedisTap:
    """Tiny TCP proxy that copies bytes in both directions and logs them.

    One worker thread accepts a single connection (one client at a time is
    enough for the differential tests) and spawns two pump threads that
    forward client->server and server->client while appending to log files.
    """

    def __init__(self, upstream_host="127.0.0.1", upstream_port=6379,
                 listen_host="127.0.0.1", listen_port=0,
                 client_log=None, server_log=None):
        self.upstream = (upstream_host, upstream_port)
        self._listen = (listen_host, listen_port)
        self.client_log = client_log
        self.server_log = server_log
        self._srv = None
        self._thread = None
        self._stop = threading.Event()
        self.listen_port = None

    def start(self):
        self._srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind(self._listen)
        self._srv.listen(8)
        self._srv.settimeout(0.5)
        self.listen_port = self._srv.getsockname()[1]
        for path in (self.client_log, self.server_log):
            if path:
                open(path, "wb").close()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        if self._srv:
            try:
                self._srv.close()
            except OSError:
                pass
        if self._thread:
            self._thread.join(timeout=2.0)

    def _serve(self):
        while not self._stop.is_set():
            try:
                cli, _ = self._srv.accept()
            except (socket.timeout, OSError):
                continue
            try:
                up = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                up.connect(self.upstream)
            except OSError:
                cli.close()
                continue
            t1 = threading.Thread(target=self._pump,
                                  args=(cli, up, self.client_log),
                                  daemon=True)
            t2 = threading.Thread(target=self._pump,
                                  args=(up, cli, self.server_log),
                                  daemon=True)
            t1.start(); t2.start()

    def _pump(self, src, dst, logpath):
        try:
            while not self._stop.is_set():
                data = src.recv(65536)
                if not data:
                    break
                if logpath:
                    with open(logpath, "ab") as f:
                        f.write(data)
                dst.sendall(data)
        except OSError:
            pass
        finally:
            for s in (src, dst):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    s.close()
                except OSError:
                    pass


def canonicalize_resp(blob):
    """Canonicalize a stream of RESP array commands.

    Returns a list of canonical command bytes. Each command is re-emitted
    as ``*N\\r\\n$L\\r\\n<arg>\\r\\n...`` with the verb (first arg)
    uppercased and every ``$L`` length recomputed from the argument bytes.
    Non-array frames (e.g. inline commands, RESP3 maps) are skipped.
    """
    out = []
    i = 0
    n = len(blob)
    while i < n:
        if blob[i:i + 1] != b"*":
            # not a multi-bulk frame — try to resync at next \r\n
            j = blob.find(b"\r\n", i)
            if j < 0:
                break
            i = j + 2
            continue
        crlf = blob.find(b"\r\n", i)
        if crlf < 0:
            break
        try:
            nargs = int(blob[i + 1:crlf])
        except ValueError:
            i = crlf + 2
            continue
        i = crlf + 2
        args = []
        ok = True
        for _ in range(nargs):
            if i >= n or blob[i:i + 1] != b"$":
                ok = False
                break
            crlf = blob.find(b"\r\n", i)
            if crlf < 0:
                ok = False
                break
            try:
                ln = int(blob[i + 1:crlf])
            except ValueError:
                ok = False
                break
            i = crlf + 2
            if ln < 0:
                args.append(None)
                continue
            if i + ln + 2 > n:
                ok = False
                break
            args.append(blob[i:i + ln])
            i += ln + 2
        if not ok or not args:
            continue
        # Uppercase the verb; recompute every length prefix.
        verb = args[0].upper() if args[0] is not None else b""
        parts = [b"*", str(len(args)).encode(), b"\r\n",
                 b"$", str(len(verb)).encode(), b"\r\n", verb, b"\r\n"]
        for a in args[1:]:
            if a is None:
                parts += [b"$-1\r\n"]
            else:
                parts += [b"$", str(len(a)).encode(), b"\r\n", a, b"\r\n"]
        out.append(b"".join(parts))
    return out


if __name__ == "__main__":
    # Smoke: start a tap on stdin-provided port, log to /tmp.
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 6400
    upstream = int(sys.argv[2]) if len(sys.argv) > 2 else 6379
    t = RedisTap(upstream_port=upstream, listen_port=port,
                 client_log="/tmp/tap_c2s.bin",
                 server_log="/tmp/tap_s2c.bin").start()
    print(f"tap listening on {t.listen_port} -> {upstream}", flush=True)
    try:
        # Block on stdin so the tap stays up; on EOF (`</dev/null`) or any
        # broken-pipe read we exit cleanly instead of spinning on a busy
        # zero-byte loop (cursor bugbot finding).
        while True:
            data = os.read(0, 4096)
            if not data:
                break
    except (KeyboardInterrupt, OSError):
        pass
    finally:
        t.stop()
