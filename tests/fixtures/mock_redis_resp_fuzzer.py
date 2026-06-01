#!/usr/bin/env python3
"""
Mock-redis server for adversarial RESP-response fuzzing of the
memtier_benchmark client-side parser.

This is a **defensive test fixture**, not an attack tool. It exists so that
the parser audit identified in issue #409 (unhandled bulk-length overflow,
RESP3 push/verbatim/map edge cases, malformed CLUSTER SLOTS, unsolicited
replies, truncated frames) is exercised on every CI run with a known set
of payloads checked into tests/fixtures/resp_payloads/.

The pass criterion is: memtier_benchmark must "report and exit cleanly,
never crash" when faced with these malformed replies - same contract as
the input/CLI side after PR #405.

Usage:
    python3 mock_redis_resp_fuzzer.py --port 12345 --fixture bulk_huge_length.bin

Behavior:
    * Listens on 127.0.0.1:<port>
    * For each accepted connection, lets the handshake commands
      (HELLO, AUTH, SELECT, CLUSTER, COMMAND, CLIENT) through with a
      benign OK / empty-array reply.
    * For every other command (typically the SET/GET workload memtier
      issues), replies with the contents of the selected fixture .bin
      and then immediately half-closes the connection. The client must
      handle that and either exit cleanly or reconnect; either is fine
      so long as it does not crash or hang.
"""
import argparse
import os
import socket
import sys
import threading


# Commands we let through with a benign reply so the memtier handshake
# completes and the workload actually starts. Order matters only for the
# match - we lowercase the inbound verb.
#
# `cluster` is NOT in the default handshake set: the `cluster_slots_*`
# fixtures specifically target the CLUSTER SLOTS parser, so we want the
# malformed bytes to be served as the CLUSTER reply. For other fixtures,
# we let CLUSTER through with `--passthrough-cluster` so memtier's
# pre-workload `CLUSTER SLOTS` (in --cluster-mode) does not consume the
# fuzz payload.
HANDSHAKE_DEFAULT = {
    "hello",
    "auth",
    "select",
    "command",
    "client",
    "ping",
    "info",
}


def _benign_reply(verb):
    """Pick a defensible reply for a handshake command."""
    if verb == "hello":
        # Minimal RESP2 +OK reply works because memtier negotiates RESP2 by
        # default. If a future test toggles --resp 3, swap to a %0 reply.
        return b"+OK\r\n"
    if verb == "cluster":
        # Empty CLUSTER SLOTS / CLUSTER SHARDS reply -- standalone topology.
        return b"*0\r\n"
    if verb == "command":
        # COMMAND / COMMAND DOCS / COMMAND COUNT -- empty array is safe.
        return b"*0\r\n"
    if verb == "info":
        return b"$0\r\n\r\n"
    # AUTH, SELECT, CLIENT, PING all accept +OK.
    return b"+OK\r\n"


def _parse_verb(buf):
    """Return the lowercased command name from a RESP request, or None.

    We only need the verb to decide handshake-vs-fuzz, so we tolerate
    incomplete buffers and bail out (returning None) rather than raising.
    """
    if not buf:
        return None
    # Inline command: "PING\r\n"
    if buf[:1] != b"*":
        line = buf.split(b"\r\n", 1)[0]
        return line.split(b" ", 1)[0].decode("ascii", errors="replace").lower()
    # Multi-bulk: "*<n>\r\n$<m>\r\n<verb>\r\n..."
    try:
        # Skip the "*<n>\r\n"
        first_nl = buf.index(b"\r\n")
        # Skip the "$<m>\r\n"
        second_nl = buf.index(b"\r\n", first_nl + 2)
        # Verb terminator
        third_nl = buf.index(b"\r\n", second_nl + 2)
        return buf[second_nl + 2 : third_nl].decode("ascii", errors="replace").lower()
    except ValueError:
        return None


def _handle(conn, payload, handshake_set, verbose):
    """Per-connection handler."""
    try:
        conn.settimeout(5.0)
        rolling = b""
        served_fuzz = False
        while True:
            try:
                chunk = conn.recv(65536)
            except (socket.timeout, OSError):
                break
            if not chunk:
                break
            rolling += chunk
            # Process as many complete-looking requests as we can without
            # being too clever; verb-sniff and reply.
            verb = _parse_verb(rolling)
            if verb is None:
                continue
            if verbose:
                sys.stderr.write("mock-redis verb={!r}\n".format(verb))
            if verb in handshake_set:
                conn.sendall(_benign_reply(verb))
                # Drain - we do not actually parse the full request length.
                # This is fine for handshake commands which are small and
                # not pipelined ahead of the workload starting.
                rolling = b""
                continue
            # Anything else: serve the adversarial fixture once and then
            # half-close. memtier will either exit (reporting a parser
            # error) or attempt reconnect; both are non-crash outcomes.
            conn.sendall(payload)
            served_fuzz = True
            try:
                conn.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            break
        if verbose and not served_fuzz:
            sys.stderr.write("mock-redis: connection closed pre-fuzz\n")
    except Exception as exc:
        if verbose:
            sys.stderr.write("mock-redis handler error: {}\n".format(exc))
    finally:
        try:
            conn.close()
        except OSError:
            pass


def serve(host, port, payload, handshake_set, stop_event, verbose):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, port))
    s.listen(64)
    s.settimeout(0.25)
    while not stop_event.is_set():
        try:
            conn, _ = s.accept()
        except socket.timeout:
            continue
        except OSError:
            break
        threading.Thread(
            target=_handle,
            args=(conn, payload, handshake_set, verbose),
            daemon=True,
        ).start()
    try:
        s.close()
    except OSError:
        pass


def main():
    parser = argparse.ArgumentParser(description="Adversarial RESP mock-redis fixture")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument(
        "--fixture",
        required=True,
        help="Path to a .bin fixture under tests/fixtures/resp_payloads/",
    )
    parser.add_argument(
        "--passthrough-cluster",
        action="store_true",
        help=(
            "Treat the CLUSTER verb as a handshake (reply with an empty "
            "*0 array) instead of serving the fixture. Use this for "
            "fixtures that target other parsers but are run against "
            "memtier in --cluster-mode."
        ),
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if not os.path.isfile(args.fixture):
        sys.stderr.write("fixture not found: {}\n".format(args.fixture))
        sys.exit(2)
    with open(args.fixture, "rb") as f:
        payload = f.read()

    handshake_set = set(HANDSHAKE_DEFAULT)
    if args.passthrough_cluster:
        handshake_set.add("cluster")

    stop_event = threading.Event()
    try:
        serve(args.host, args.port, payload, handshake_set, stop_event, args.verbose)
    except KeyboardInterrupt:
        stop_event.set()


if __name__ == "__main__":
    main()
