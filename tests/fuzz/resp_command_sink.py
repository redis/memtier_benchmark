"""Bounded RESP command sink for fuzzing input parsing without Redis effects.

Every complete array of non-null bulk strings receives a simple ``OK`` reply.
Command names and arguments are discarded, including commands such as WAIT,
HELLO, CONFIG, and SHUTDOWN. This is a transport fixture, not a Redis emulator.
"""

import socket
import threading
import time


class _FrameError(Exception):
    pass


class _Reader:
    CHUNK_SIZE = 64 * 1024
    MAX_HEADER = 64
    MAX_LENGTH = (1 << 63) - 1

    def __init__(self, stream):
        self.stream = stream

    def length(self, marker, initial=False):
        # Track header bytes explicitly: BufferedReader.readline() can consume
        # part of a header before raising on RST, losing the distinction between
        # a clean request boundary and a truncated next request.
        line = bytearray()
        while len(line) <= self.MAX_HEADER:
            try:
                byte = self.stream.read(1)
            except ConnectionResetError:
                if initial and not line:
                    return None
                raise _FrameError("truncated RESP length header (peer reset)")
            if not byte:
                break
            line.extend(byte)
            if byte == b"\n":
                break
        if not line and initial:
            return None
        if len(line) > self.MAX_HEADER:
            raise _FrameError("RESP length header is too long")
        if not line.endswith(b"\r\n"):
            raise _FrameError("truncated RESP length header")
        digits = line[1:-2]
        if line[:1] != marker or not digits or not digits.isdigit():
            raise _FrameError("invalid RESP array/bulk length")
        value = int(digits)
        if value > self.MAX_LENGTH:
            raise _FrameError("RESP length exceeds signed 64-bit range")
        return value

    def request(self):
        count = self.length(b"*", initial=True)
        if count is None:
            return False
        for _ in range(count):
            remaining = self.length(b"$")
            try:
                while remaining:
                    data = self.stream.read(min(remaining, self.CHUNK_SIZE))
                    if not data:
                        raise _FrameError("truncated RESP bulk payload")
                    remaining -= len(data)
                if self.stream.read(2) != b"\r\n":
                    raise _FrameError("invalid or truncated RESP bulk terminator")
            except ConnectionResetError:
                raise _FrameError("truncated RESP bulk payload (peer reset)")
        return True


class RESPCommandSink:
    """Serve on loopback until context exit; never execute received commands.

    ``request_count`` and ``errors`` are thread-safe snapshots. After a producer
    disconnects, call ``wait_idle`` before inspecting them: this waits for EOF
    processing and any queued accepts. No idle socket timeout is imposed because
    an instrumented fuzzer can spend a long time parsing before its first write.
    """

    host = "127.0.0.1"
    MAX_ERRORS = 32

    def __init__(self, max_connections=32, close_timeout=5):
        if max_connections < 1 or close_timeout <= 0:
            raise ValueError("connection limit and close timeout must be positive")
        self.port = None
        self._max_connections = max_connections
        self._close_timeout = close_timeout
        self._condition = threading.Condition()
        self._stopping = threading.Event()
        self._connections = set()
        self._workers = set()
        self._errors = []
        self._dropped_errors = 0
        self._request_count = 0
        self._accept_generation = 0
        self._listener = None
        self._accept_thread = None

    @property
    def request_count(self):
        with self._condition:
            return self._request_count

    @property
    def error_count(self):
        """Return all failures, including diagnostics omitted by the storage cap."""
        with self._condition:
            return len(self._errors) + self._dropped_errors

    @property
    def errors(self):
        with self._condition:
            result = tuple(self._errors)
            if self._dropped_errors:
                result += ("{} additional sink errors".format(self._dropped_errors),)
            return result

    def _record_error(self, message):
        with self._condition:
            if len(self._errors) < self.MAX_ERRORS:
                self._errors.append(message)
            else:
                self._dropped_errors += 1

    def __enter__(self):
        if self._listener is not None or self._stopping.is_set():
            raise RuntimeError("RESPCommandSink contexts cannot be reused")
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.bind((self.host, 0))
            listener.listen(self._max_connections)
            listener.settimeout(0.05)
        except BaseException:
            listener.close()
            raise
        self._listener = listener
        self.port = listener.getsockname()[1]
        self._accept_thread = threading.Thread(
            target=self._accept, name="resp-command-sink-accept", daemon=True)
        try:
            self._accept_thread.start()
        except BaseException:
            listener.close()
            self._accept_thread = None
            self._stopping.set()
            raise
        return self

    def _accept(self):
        try:
            self._accept_connections()
        except Exception as error:
            if not self._stopping.is_set():
                self._record_error("accept worker failed: {}: {}".format(
                    type(error).__name__, error))
        finally:
            with self._condition:
                self._accept_generation += 1
                self._condition.notify_all()

    def _accept_connections(self):
        while not self._stopping.is_set():
            try:
                connection, _ = self._listener.accept()
            except socket.timeout:
                with self._condition:
                    self._accept_generation += 1
                    self._condition.notify_all()
                continue
            except OSError as error:
                if not self._stopping.is_set():
                    self._record_error("accept failed: {}".format(error))
                return
            with self._condition:
                self._workers = {worker for worker in self._workers if worker.is_alive()}
                if self._stopping.is_set():
                    connection.close()
                    return
                if len(self._connections) >= self._max_connections:
                    self._record_error("RESP sink connection limit exceeded")
                    connection.close()
                    continue
                worker = None
                try:
                    connection.settimeout(None)
                    self._connections.add(connection)
                    worker = threading.Thread(target=self._serve, args=(connection,),
                                              name="resp-command-sink-client", daemon=True)
                    self._workers.add(worker)
                    # Starting under the condition prevents close() joining a
                    # registered thread that has not yet started.
                    worker.start()
                except Exception:
                    connection.close()
                    self._connections.discard(connection)
                    self._workers.discard(worker)
                    self._condition.notify_all()
                    raise

    def _serve(self, connection):
        try:
            with connection.makefile("rb", buffering=_Reader.CHUNK_SIZE) as stream:
                reader = _Reader(stream)
                can_reply = True
                while not self._stopping.is_set() and reader.request():
                    with self._condition:
                        self._request_count += 1
                    if can_reply:
                        try:
                            connection.sendall(b"+OK\r\n")
                        except (BrokenPipeError, ConnectionResetError):
                            # memtier deliberately closes using SO_LINGER(1, 0).
                            # Still validate buffered trailing frames after RST.
                            can_reply = False
        except Exception as error:
            if not self._stopping.is_set():
                self._record_error("RESP connection failed: {}: {}".format(
                    type(error).__name__, error))
        finally:
            connection.close()
            with self._condition:
                self._connections.discard(connection)
                self._condition.notify_all()

    def wait_idle(self, timeout=5):
        """Wait for accepted/queued producers to finish and publish errors."""
        deadline = time.monotonic() + timeout
        with self._condition:
            generation = self._accept_generation
            while self._connections or (
                    self._accept_thread is not None and
                    self._accept_thread.is_alive() and
                    self._accept_generation == generation):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
            workers = tuple(self._workers)
        for worker in workers:
            worker.join(max(0, deadline - time.monotonic()))
            if worker.is_alive():
                return False
        return True

    def close(self):
        """Stop blocked accepts, reads, and writes, then join all handlers."""
        self._stopping.set()
        deadline = time.monotonic() + self._close_timeout
        with self._condition:
            sockets = tuple(self._connections)
            if self._listener is not None:
                sockets += (self._listener,)
            for connection in sockets:
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                connection.close()
            self._condition.notify_all()
        threads = (() if self._accept_thread is None else (self._accept_thread,))
        with self._condition:
            threads += tuple(self._workers)
        for thread in threads:
            thread.join(max(0, deadline - time.monotonic()))
        if any(thread.is_alive() for thread in threads):
            message = "RESP sink threads did not stop before close deadline"
            self._record_error(message)
            raise RuntimeError(message)

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
