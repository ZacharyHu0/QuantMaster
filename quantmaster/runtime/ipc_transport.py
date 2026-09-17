"""Deadline-owned multiprocessing wire transport, without helper threads.

Keep the standard authenticated named-pipe / Unix-socket protocol. All bytes,
including authentication and partial messages, share one monotonic deadline.
"""

from __future__ import annotations

import math
import pickle
import socket
import struct
import sys
import time
from multiprocessing.connection import answer_challenge, deliver_challenge
from typing import Any, cast

MAX_MESSAGE_BYTES = 4 * 1024 * 1024


class DeadlineConnection:
    def __init__(self, connection: Any, deadline: float) -> None:
        self.deadline = deadline
        self.connection = connection

    def remaining(self) -> float:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("IPC deadline exceeded")
        return remaining

    @classmethod
    def connect(cls, endpoint: str, deadline: float) -> DeadlineConnection:
        if sys.platform == "win32":
            return cls(_connect_pipe(endpoint, deadline), deadline)
        channel = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        result = cls(channel, deadline)
        try:
            channel.settimeout(result.remaining())
            channel.connect(endpoint)
        except OSError:
            channel.close()
            raise
        return result

    @classmethod
    def accepted(cls, connection: Any, deadline: float) -> DeadlineConnection:
        if sys.platform == "win32":
            return cls(connection, deadline)
        try:
            channel = socket.fromfd(connection.fileno(), socket.AF_UNIX, socket.SOCK_STREAM)
        finally:
            connection.close()
        return cls(channel, deadline)

    def close(self) -> None:
        self.connection.close()

    def authenticate(self, key: bytes, *, server: bool = False) -> None:
        # The stdlib functions use only send_bytes/recv_bytes, but their stubs
        # require inheritance from the private _ConnectionBase implementation.
        channel = cast(Any, self)
        if server:
            deliver_challenge(channel, key)
            answer_challenge(channel, key)
        else:
            answer_challenge(channel, key)
            deliver_challenge(channel, key)

    def send_bytes(self, value: bytes) -> None:
        if len(value) > MAX_MESSAGE_BYTES:
            raise OSError("IPC message exceeds size limit")
        if sys.platform == "win32":
            _pipe_io(self.connection.fileno(), value, self.remaining())
        else:
            self.connection.settimeout(self.remaining())
            self.connection.sendall(struct.pack("!i", len(value)) + value)

    def _read_exact(self, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            self.connection.settimeout(self.remaining())
            chunk = self.connection.recv(size - len(chunks))
            if not chunk:
                raise EOFError
            chunks.extend(chunk)
        return bytes(chunks)

    def recv_bytes(self, maxlength: int = MAX_MESSAGE_BYTES) -> bytes:
        limit = min(maxlength, MAX_MESSAGE_BYTES)
        if sys.platform == "win32":
            return _pipe_io(self.connection.fileno(), limit, self.remaining())
        size, = struct.unpack("!i", self._read_exact(4))
        if not 0 <= size <= limit:
            raise OSError("IPC message exceeds size limit")
        return self._read_exact(size)

    def send(self, value: Any) -> None:
        self.send_bytes(pickle.dumps(value, protocol=4))

    def recv(self) -> Any:
        return pickle.loads(self.recv_bytes())


if sys.platform == "win32":
    def _connect_pipe(endpoint: str, deadline: float) -> Any:
        import _winapi
        from multiprocessing.connection import PipeConnection

        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("IPC connection deadline exceeded")
            try:
                _winapi.WaitNamedPipe(endpoint, max(1, math.ceil(remaining * 1000)))
                handle = _winapi.CreateFile(
                    endpoint, _winapi.GENERIC_READ | _winapi.GENERIC_WRITE,
                    0, _winapi.NULL, _winapi.OPEN_EXISTING,
                    _winapi.FILE_FLAG_OVERLAPPED, _winapi.NULL,
                )
            except OSError as exc:
                if exc.winerror not in (_winapi.ERROR_SEM_TIMEOUT, _winapi.ERROR_PIPE_BUSY):
                    raise
            else:
                connection = PipeConnection(handle)
                try:
                    _winapi.SetNamedPipeHandleState(handle, _winapi.PIPE_READMODE_MESSAGE, None, None)
                except OSError:
                    connection.close()
                    raise
                return connection


    def _pipe_io(handle: int, data: bytes | int, timeout: float) -> bytes:
        import _winapi

        if isinstance(data, int):
            overlapped, error = _winapi.ReadFile(handle, data, overlapped=True)
        else:
            overlapped, error = _winapi.WriteFile(handle, data, overlapped=True)
        try:
            if error == _winapi.ERROR_IO_PENDING:
                result = _winapi.WaitForMultipleObjects(
                    [overlapped.event], False, max(1, math.ceil(timeout * 1000)),
                )
                if result != _winapi.WAIT_OBJECT_0:
                    raise TimeoutError("IPC transfer deadline exceeded")
        finally:
            # Cancel and reap the owned operation before its buffer/handle can die.
            overlapped.cancel()
            _count, error = overlapped.GetOverlappedResult(True)
        if error:
            raise OSError(error, "IPC transfer failed or message exceeds size limit")
        return bytes(overlapped.getbuffer() or b"") if isinstance(data, int) else b""
