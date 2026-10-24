"""Standalone stdlib tests: python3 -m unittest discover -s tests/fuzz."""

import socket
import struct
import threading
import time
import tracemalloc
import unittest
from unittest.mock import patch

from resp_command_sink import RESPCommandSink, _Reader


def frame(*arguments):
    return b"*%d\r\n" % len(arguments) + b"".join(
        b"$%d\r\n" % len(argument) + argument + b"\r\n"
        for argument in arguments)


def receive(connection, size):
    data = bytearray()
    while len(data) < size:
        chunk = connection.recv(size - len(data))
        if not chunk:
            raise AssertionError("sink disconnected before its complete reply")
        data.extend(chunk)
    return bytes(data)


class RESPCommandSinkTests(unittest.TestCase):
    def connect(self, sink):
        connection = socket.create_connection((sink.host, sink.port), timeout=3)
        self.addCleanup(connection.close)
        return connection

    def test_fragmented_headers_and_binary_arguments(self):
        with RESPCommandSink() as sink:
            with self.connect(sink) as connection:
                payload = frame(b"SET", b"key\x00\r\n", b"\xff\x00\r\n$9\r\n", b"")
                for byte in payload:
                    connection.sendall(bytes((byte,)))
                self.assertEqual(receive(connection, 5), b"+OK\r\n")
            self.assertTrue(sink.wait_idle())
            self.assertEqual(sink.request_count, 1)
            self.assertEqual(sink.errors, ())

    def test_pipeline_commands_have_no_server_side_effects(self):
        commands = [
            frame(b"WAIT", b"8", b"0"),
            frame(b"HELLO", b"3"),
            frame(b"PING"),
            frame(b"SHUTDOWN"),
            frame(b"CONFIG", b"SET", b"requirepass", b"never-applied"),
            frame(b"PING"),
        ]
        with RESPCommandSink() as sink:
            with self.connect(sink) as connection:
                connection.sendall(b"".join(commands))
                self.assertEqual(receive(connection, 5 * len(commands)),
                                 b"+OK\r\n" * len(commands))
            self.assertTrue(sink.wait_idle())
            self.assertEqual(sink.request_count, len(commands))
            self.assertEqual(sink.errors, ())

    def test_large_bulk_payload_is_discarded_with_bounded_memory(self):
        chunk = b"x\x00\r\n" * 4096
        size = len(chunk) * 512
        with RESPCommandSink() as sink:
            with self.connect(sink) as connection:
                tracemalloc.start()
                try:
                    connection.sendall(b"*2\r\n$3\r\nSET\r\n$%d\r\n" % size)
                    for _ in range(512):
                        connection.sendall(chunk)
                    connection.sendall(b"\r\n" + frame(b"PING"))
                    self.assertEqual(receive(connection, 10), b"+OK\r\n" * 2)
                    _, peak = tracemalloc.get_traced_memory()
                finally:
                    tracemalloc.stop()
                self.assertLess(peak, 1024 * 1024,
                                "the sink retained a large command payload")
            self.assertTrue(sink.wait_idle())
            self.assertEqual(sink.request_count, 2)
            self.assertEqual(sink.errors, ())

    def test_rejects_malformed_lengths_and_types(self):
        malformed = [
            b"*-1\r\n", b"*+1\r\n", b"*one\r\n", b"*\r\n",
            b"*1\n", b"PING\r\n", b"*9223372036854775808\r\n",
            b"*" + b"1" * 64 + b"\r\n", b"*1\r\n$-1\r\n",
            b"*1\r\n$+1\r\n", b"*1\r\n$one\r\n",
            b"*1\r\n:1\r\n", b"*1\r\n$1\r\nxXX",
        ]
        for payload in malformed:
            with self.subTest(payload=payload), RESPCommandSink() as sink:
                with self.connect(sink) as connection:
                    connection.sendall(payload)
                    connection.shutdown(socket.SHUT_WR)
                    self.assertEqual(connection.recv(1), b"")
                self.assertTrue(sink.wait_idle())
                self.assertEqual(sink.request_count, 0)
                self.assertEqual(len(sink.errors), 1)

    def test_truncated_frame_errors_are_visible_after_wait_idle(self):
        partials = [b"*", b"*1\r\n", b"*1\r\n$5\r\nab", b"*1\r\n$1\r\nx\r"]
        for payload in partials:
            with self.subTest(payload=payload), RESPCommandSink() as sink:
                with self.connect(sink) as connection:
                    connection.sendall(frame(b"PING") + payload)
                    self.assertEqual(receive(connection, 5), b"+OK\r\n")
                    connection.shutdown(socket.SHUT_WR)
                self.assertTrue(sink.wait_idle())
                self.assertEqual(sink.request_count, 1)
                self.assertEqual(len(sink.errors), 1)
                self.assertIn("truncated", sink.errors[0])

    def test_initial_eof_and_multiple_producers(self):
        with RESPCommandSink() as sink:
            with self.connect(sink):
                pass
            connections = [self.connect(sink) for _ in range(8)]
            for connection in connections:
                connection.sendall(frame(b"PING") * 3)
            for connection in connections:
                self.assertEqual(receive(connection, 15), b"+OK\r\n" * 3)
                connection.close()
            self.assertTrue(sink.wait_idle())
            self.assertEqual(sink.request_count, 24)
            self.assertEqual(sink.errors, ())

    def test_idle_connection_survives_accept_polling(self):
        with RESPCommandSink() as sink:
            with self.connect(sink) as connection:
                self.assertFalse(sink.wait_idle(timeout=0.15))
                connection.sendall(frame(b"PING"))
                self.assertEqual(receive(connection, 5), b"+OK\r\n")
            self.assertTrue(sink.wait_idle())
            self.assertEqual(sink.errors, ())

    def test_shutdown_unblocks_idle_and_partial_connections(self):
        sink = RESPCommandSink(close_timeout=2)
        with sink:
            idle = self.connect(sink)
            partial = self.connect(sink)
            partial.sendall(b"*1\r\n$1000000\r\npartial")
            self.assertFalse(sink.wait_idle(timeout=0.1))
            started = time.monotonic()
        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual(idle.recv(1), b"")
        self.assertEqual(partial.recv(1), b"")
        self.assertTrue(sink.wait_idle())
        self.assertEqual(sink.request_count, 0)
        self.assertEqual(sink.errors, ())
        with self.assertRaises(OSError):
            socket.create_connection((sink.host, sink.port), timeout=0.1)
        sink.close()  # Repeated cleanup remains harmless.

    def test_error_snapshots_do_not_mutate(self):
        with RESPCommandSink() as sink:
            before = sink.errors
            with self.connect(sink) as connection:
                connection.sendall(b"invalid\r\n")
                connection.shutdown(socket.SHUT_WR)
            self.assertTrue(sink.wait_idle())
            self.assertEqual(before, ())
            self.assertEqual(len(sink.errors), 1)

    def test_wait_idle_drains_immediate_disconnect_before_reply(self):
        with RESPCommandSink() as sink:
            for _ in range(8):
                with self.connect(sink) as connection:
                    connection.sendall(b"*1\r\n$5\r\nab")
            self.assertTrue(sink.wait_idle())
            self.assertEqual(sink.request_count, 0)
            self.assertEqual(len(sink.errors), 8)
            self.assertTrue(all("truncated" in error for error in sink.errors))

    def test_connection_limit_rejects_excess_without_breaking_existing_client(self):
        with RESPCommandSink(max_connections=1) as sink:
            self.assertEqual(sink.host, "127.0.0.1")
            self.assertGreater(sink.port, 0)
            with self.connect(sink) as first:
                first.sendall(frame(b"PING"))
                self.assertEqual(receive(first, 5), b"+OK\r\n")
                with self.connect(sink) as excess:
                    self.assertEqual(excess.recv(1), b"")
                first.sendall(frame(b"PING"))
                self.assertEqual(receive(first, 5), b"+OK\r\n")
            self.assertTrue(sink.wait_idle())
            self.assertEqual(sink.request_count, 2)
            self.assertEqual(sink.errors, ("RESP sink connection limit exceeded",))

    def test_clean_reset_at_request_boundary(self):
        with RESPCommandSink() as sink:
            with self.connect(sink) as connection:
                connection.sendall(frame(b"PING"))
                self.assertEqual(receive(connection, 5), b"+OK\r\n")
                connection.sendall(frame(b"HELLO", b"3") + frame(b"WAIT", b"8", b"0"))
                connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                      struct.pack("ii", 1, 0))
            self.assertTrue(sink.wait_idle())
            self.assertEqual(sink.request_count, 3)
            self.assertEqual(sink.errors, ())

    def test_idle_reset_before_first_command_is_clean(self):
        with RESPCommandSink() as sink:
            with self.connect(sink) as connection:
                self.assertFalse(sink.wait_idle(timeout=0.1))
                connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                      struct.pack("ii", 1, 0))
            self.assertTrue(sink.wait_idle())
            self.assertEqual(sink.request_count, 0)
            self.assertEqual(sink.errors, ())

    def test_reset_mid_frame_is_not_a_clean_disconnect(self):
        for partial in [b"*", b"*1\r\n$", b"*1\r\n$8\r\nabc", b"*1\r\n$1\r\nx\r"]:
            with self.subTest(partial=partial), RESPCommandSink() as sink:
                with self.connect(sink) as connection:
                    connection.sendall(frame(b"PING"))
                    self.assertEqual(receive(connection, 5), b"+OK\r\n")
                    connection.sendall(partial)
                    connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                          struct.pack("ii", 1, 0))
                self.assertTrue(sink.wait_idle())
                self.assertEqual(sink.request_count, 1)
                self.assertEqual(len(sink.errors), 1)
                self.assertIn("truncated", sink.errors[0])

    def test_reply_disconnect_still_validates_buffered_trailing_frames(self):
        original_sendall = socket.socket.sendall

        def disconnected_reply(connection, data, *args, **kwargs):
            if threading.current_thread().name == "resp-command-sink-client":
                raise BrokenPipeError("injected peer disconnect")
            return original_sendall(connection, data, *args, **kwargs)

        with RESPCommandSink() as sink, patch.object(socket.socket, "sendall", disconnected_reply):
            with self.connect(sink) as connection:
                connection.sendall(frame(b"PING") + b"*1\r\n$5\r\nab")
                connection.shutdown(socket.SHUT_WR)
                self.assertEqual(connection.recv(1), b"")
            self.assertTrue(sink.wait_idle())
            self.assertEqual(sink.request_count, 1)
            self.assertEqual(len(sink.errors), 1)
            self.assertIn("truncated", sink.errors[0])

    def test_unexpected_reader_exception_is_published(self):
        with RESPCommandSink() as sink, patch.object(
                _Reader, "request", side_effect=RuntimeError("injected reader failure")):
            with self.connect(sink) as connection:
                self.assertEqual(connection.recv(1), b"")
            self.assertTrue(sink.wait_idle())
            self.assertEqual(len(sink.errors), 1)
            self.assertIn("RuntimeError: injected reader failure", sink.errors[0])

    def test_worker_start_failure_is_published_and_cleaned_up(self):
        original_start = threading.Thread.start

        def fail_client_start(thread):
            if thread.name == "resp-command-sink-client":
                raise RuntimeError("injected start failure")
            return original_start(thread)

        with RESPCommandSink() as sink, patch.object(threading.Thread, "start", fail_client_start):
            with self.connect(sink) as connection:
                self.assertEqual(connection.recv(1), b"")
            self.assertTrue(sink.wait_idle())
            self.assertEqual(len(sink.errors), 1)
            self.assertIn("RuntimeError: injected start failure", sink.errors[0])

    def test_invalid_limits_and_context_reuse_are_rejected(self):
        for options in ({"max_connections": 0}, {"close_timeout": 0}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                RESPCommandSink(**options)
        sink = RESPCommandSink()
        sink.close()  # Cleanup is also safe before startup.
        with self.assertRaises(RuntimeError):
            sink.__enter__()
        with RESPCommandSink() as active:
            with self.assertRaises(RuntimeError):
                active.__enter__()

    def test_error_storage_is_bounded_without_hiding_new_failures(self):
        with RESPCommandSink() as sink:
            self.assertEqual(sink.take_errors(), ())
            for _ in range(sink.MAX_ERRORS + 1):
                with self.connect(sink) as connection:
                    connection.sendall(b"invalid\r\n")
                    self.assertEqual(connection.recv(1), b"")
            self.assertTrue(sink.wait_idle())
            before = sink.errors
            self.assertEqual(len(before), sink.MAX_ERRORS + 1)
            self.assertEqual(before[-1], "1 additional sink errors")
            with self.connect(sink) as connection:
                connection.sendall(b"invalid\r\n")
                self.assertEqual(connection.recv(1), b"")
            self.assertTrue(sink.wait_idle())
            self.assertEqual(len(sink.errors), len(before))
            self.assertNotEqual(sink.errors, before)
            self.assertEqual(sink.errors[-1], "2 additional sink errors")
            capped = sink.take_errors()
            self.assertEqual(capped[-1], "2 additional sink errors")
            self.assertEqual(sink.errors, ())
            self.assertEqual(sink.take_errors(), ())
            with self.connect(sink) as connection:
                connection.sendall(frame(b"PING"))
                self.assertEqual(receive(connection, 5), b"+OK\r\n")
            self.assertTrue(sink.wait_idle())
            self.assertEqual(sink.take_errors(), ())
            self.assertEqual(before[-1], "1 additional sink errors")
            self.assertEqual(capped[-1], "2 additional sink errors")

    def test_later_input_keeps_its_own_diagnostic_after_earlier_errors_are_taken(self):
        with RESPCommandSink() as sink:
            for _ in range(39):
                with self.connect(sink) as connection:
                    connection.sendall(b"invalid\r\n")
                    self.assertEqual(connection.recv(1), b"")
            self.assertTrue(sink.wait_idle())
            earlier = sink.take_errors()
            self.assertEqual(earlier[-1], "7 additional sink errors")
            with self.connect(sink) as connection:
                connection.sendall(b"*9223372036854775808\r\n")
                self.assertEqual(connection.recv(1), b"")
            self.assertTrue(sink.wait_idle())
            current = sink.take_errors()
            self.assertEqual(len(current), 1)
            self.assertIn("exceeds signed 64-bit range", current[0])
            self.assertNotIn(current[0], earlier)
            self.assertEqual(sink.errors, ())

    def test_listener_bind_failure_closes_its_socket(self):
        socket_class = socket.socket
        listener = socket_class()
        self.addCleanup(listener.close)
        with patch("resp_command_sink.socket.socket", return_value=listener), patch.object(
                socket_class, "bind", side_effect=OSError("injected bind failure")):
            with self.assertRaisesRegex(OSError, "injected bind failure"):
                RESPCommandSink().__enter__()
        self.assertEqual(listener.fileno(), -1)

    def test_accept_thread_start_failure_closes_its_socket(self):
        listener = socket.socket()
        sink = RESPCommandSink()
        with patch("resp_command_sink.socket.socket", return_value=listener), patch.object(
                threading.Thread, "start", side_effect=RuntimeError("injected start failure")):
            with self.assertRaisesRegex(RuntimeError, "injected start failure"):
                sink.__enter__()
        self.assertEqual(listener.fileno(), -1)
        sink.close()

    def test_accept_failure_is_published(self):
        with patch.object(socket.socket, "accept", side_effect=OSError("injected accept failure")):
            with RESPCommandSink() as sink:
                self.assertTrue(sink.wait_idle())
                self.assertEqual(sink.errors, ("accept failed: injected accept failure",))

    def test_blocked_worker_obeys_idle_and_close_deadlines(self):
        sink = RESPCommandSink(close_timeout=0.1)
        sink.__enter__()
        self.addCleanup(sink.close)
        for _ in range(sink.MAX_ERRORS):
            with self.connect(sink) as connection:
                connection.sendall(b"invalid\r\n")
                self.assertEqual(connection.recv(1), b"")
        self.assertTrue(sink.wait_idle())
        self.assertEqual(len(sink.errors), sink.MAX_ERRORS)
        original_serve = sink._serve
        blocked = threading.Event()
        release = threading.Event()

        def delayed_cleanup(connection):
            original_serve(connection)
            blocked.set()
            release.wait(5)

        with patch.object(sink, "_serve", delayed_cleanup):
            try:
                with self.connect(sink) as connection:
                    connection.sendall(frame(b"PING"))
                    self.assertEqual(receive(connection, 5), b"+OK\r\n")
                self.assertTrue(blocked.wait(2))
                self.assertFalse(sink.wait_idle(timeout=0.1))
                started = time.monotonic()
                with self.assertRaisesRegex(
                        RuntimeError, "^RESP sink threads did not stop before close deadline$"):
                    sink.close()
                self.assertLess(time.monotonic() - started, 1)
                self.assertEqual(len(sink.errors), sink.MAX_ERRORS + 1)
                self.assertEqual(sink.errors[-1], "1 additional sink errors")
            finally:
                release.set()
                sink.close()
            self.assertTrue(sink.wait_idle())

    def test_shutdown_during_accept_closes_unregistered_connection(self):
        original_accept = socket.socket.accept
        original_shutdown = socket.socket.shutdown
        accepted = threading.Event()
        closing = threading.Event()
        release = threading.Event()
        sink = RESPCommandSink(close_timeout=2)
        close_errors = []

        def delayed_accept(listener):
            result = original_accept(listener)
            accepted.set()
            release.wait(2)
            return result

        def observed_shutdown(connection, how):
            if connection is sink._listener:
                closing.set()
            return original_shutdown(connection, how)

        def close_sink():
            try:
                sink.close()
            except Exception as error:
                close_errors.append(error)

        with patch.object(socket.socket, "accept", delayed_accept), patch.object(
                socket.socket, "shutdown", observed_shutdown):
            sink.__enter__()
            closer = None
            try:
                with self.connect(sink) as connection:
                    self.assertTrue(accepted.wait(1))
                    closer = threading.Thread(target=close_sink)
                    closer.start()
                    self.assertTrue(closing.wait(1))
                    release.set()
                    closer.join(2)
                    self.assertFalse(closer.is_alive())
                    self.assertEqual(connection.recv(1), b"")
                self.assertEqual(close_errors, [])
                self.assertEqual(sink.errors, ())
            finally:
                release.set()
                if closer is not None:
                    closer.join(2)
                sink.close()


if __name__ == "__main__":
    unittest.main()
