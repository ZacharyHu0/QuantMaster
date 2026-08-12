"""Cross-process serialization for news ingest and annotation pipelines."""

from __future__ import annotations

import os
import threading
import time
from contextlib import AbstractContextManager
from io import BufferedRandom
from pathlib import Path

_LOCKS_GUARD = threading.Lock()
_THREAD_LOCKS: dict[str, threading.RLock] = {}
_FILE_LOCK_STATE: dict[str, tuple[int, BufferedRandom]] = {}


def _thread_lock(path: Path) -> threading.RLock:
    key = str(path.resolve())
    with _LOCKS_GUARD:
        return _THREAD_LOCKS.setdefault(key, threading.RLock())


def _acquire_file_lock(path: Path, timeout: float) -> BufferedRandom:
    path.parent.mkdir(parents=True, exist_ok=True)
    stream = path.open("a+b")
    if stream.tell() == 0:
        stream.write(b"\0")
        stream.flush()
    deadline = time.monotonic() + max(0.1, timeout)
    while True:
        try:
            stream.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(  # type: ignore[attr-defined]
                    stream.fileno(), msvcrt.LK_NBLCK, 1,  # type: ignore[attr-defined]
                )
            else:
                import fcntl

                fcntl.flock(  # type: ignore[attr-defined]
                    stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB,  # type: ignore[attr-defined]
                )
            return stream
        except (OSError, BlockingIOError):
            if time.monotonic() >= deadline:
                stream.close()
                raise TimeoutError("等待资讯流水线锁超时") from None
            time.sleep(0.05)


def _release_file_lock(stream: BufferedRandom) -> None:
    try:
        stream.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(  # type: ignore[attr-defined]
                stream.fileno(), msvcrt.LK_UNLCK, 1,  # type: ignore[attr-defined]
            )
        else:
            import fcntl

            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)  # type: ignore[attr-defined]
    finally:
        stream.close()


class NewsPipelineLock(AbstractContextManager["NewsPipelineLock"]):
    """A reentrant process lock backed by one OS file lock per news database."""

    def __init__(self, database: Path, *, timeout: float = 900.0) -> None:
        self.path = database.resolve().with_suffix(".pipeline.lock")
        self.key = str(self.path)
        self.timeout = timeout
        self.thread_lock = _thread_lock(self.path)

    def __enter__(self) -> NewsPipelineLock:
        self.thread_lock.acquire()
        try:
            state = _FILE_LOCK_STATE.get(self.key)
            if state is None:
                _FILE_LOCK_STATE[self.key] = (
                    1,
                    _acquire_file_lock(self.path, self.timeout),
                )
            else:
                _FILE_LOCK_STATE[self.key] = (state[0] + 1, state[1])
            return self
        except Exception:
            self.thread_lock.release()
            raise

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        try:
            depth, stream = _FILE_LOCK_STATE[self.key]
            if depth == 1:
                _FILE_LOCK_STATE.pop(self.key, None)
                _release_file_lock(stream)
            else:
                _FILE_LOCK_STATE[self.key] = (depth - 1, stream)
        finally:
            self.thread_lock.release()
