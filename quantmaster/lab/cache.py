"""Small cross-process primitives for immutable Quant Lab feature caches."""

from __future__ import annotations

import hashlib
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from io import BufferedRandom
from pathlib import Path

from quantmaster.lab.errors import LabError


def _lock_stream(path: Path, timeout: float) -> BufferedRandom:
    path.parent.mkdir(parents=True, exist_ok=True)
    stream = path.open("a+b")
    if path.stat().st_size == 0:
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
                raise LabError(
                    "TASK_CONFLICT", "另一个任务正在构建同一份特征缓存",
                    action="等待当前特征任务完成后重试", retryable=True,
                    context={"cache": path.stem}, status_code=409,
                ) from None
            time.sleep(0.05)


def _unlock_stream(stream: BufferedRandom) -> None:
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


@contextmanager
def feature_cache_lock(root: Path, timeout: float = 900.0) -> Iterator[None]:
    """Serialize construction while readers continue using committed memmaps."""
    identity = hashlib.sha256(str(root.resolve()).encode("utf-8")).hexdigest()
    stream = _lock_stream(root.parent / ".locks" / f"{identity}.lock", timeout)
    try:
        yield
    finally:
        _unlock_stream(stream)
