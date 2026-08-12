"""Small, bounded local command channel for the supervisor-owned worker.

HTTP generations are disposable and must not mutate task ledgers directly.
This module gives them one local request/reply path to the long-lived runtime
worker.  Windows uses a named pipe; non-Windows test/development environments
use a Unix-domain socket under the configured data root.
"""

from __future__ import annotations

import hashlib
import logging
import os
import tempfile
import threading
from collections.abc import Callable, Mapping
from multiprocessing import AuthenticationError
from multiprocessing.connection import Client, Listener
from pathlib import Path
from typing import Any

from quantmaster.config import get_config

logger = logging.getLogger(__name__)
DEFAULT_COMMAND_TIMEOUT_SECONDS = 0.5


class WorkerCommandError(RuntimeError):
    """A worker command was received but could not be completed."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = str(code)


class WorkerCommandUnavailable(WorkerCommandError):
    """The local runtime worker cannot accept a command in the request budget."""


def _root_digest(root: Path) -> str:
    return hashlib.sha256(str(root.resolve()).encode("utf-8")).hexdigest()


def worker_command_endpoint(root: str | Path | None = None) -> str:
    """Return the stable per-data-root local endpoint without creating files."""

    base = Path(root) if root is not None else get_config().data_root
    digest = _root_digest(base)[:24]
    if os.name == "nt":
        return rf"\\.\pipe\quantmaster-runtime-{digest}"
    endpoint = base / f".quantmaster-runtime-{digest}.sock"
    if len(os.fsencode(endpoint)) <= 100:
        return str(endpoint)
    return str(Path(tempfile.gettempdir()) / f"quantmaster-runtime-{digest}.sock")


def _authkey(root: Path) -> bytes:
    return hashlib.sha256(("quantmaster-runtime:" + _root_digest(root)).encode("utf-8")).digest()


class RuntimeCommandServer:
    """One serial local command endpoint owned exclusively by runtime-worker."""

    def __init__(
        self,
        handler: Callable[[str, dict[str, Any]], Mapping[str, Any] | dict[str, Any]],
        *,
        root: str | Path | None = None,
    ) -> None:
        self.root = (Path(root) if root is not None else get_config().data_root).resolve()
        self.endpoint = worker_command_endpoint(self.root)
        self.family = "AF_PIPE" if os.name == "nt" else "AF_UNIX"
        self._handler = handler
        self._authkey = _authkey(self.root)
        self._listener: Listener | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.RLock()

    @property
    def running(self) -> bool:
        thread = self._thread
        return bool(thread is not None and thread.is_alive() and not self._stop.is_set())

    def start(self) -> None:
        with self._lock:
            if self.running:
                return
            if self.family == "AF_UNIX":
                Path(self.endpoint).unlink(missing_ok=True)
            self._stop.clear()
            self._listener = Listener(
                self.endpoint,
                family=self.family,
                authkey=self._authkey,
            )
            self._thread = threading.Thread(
                target=self._serve,
                name="runtime-worker-ipc",
                daemon=True,
            )
            self._thread.start()

    def _serve(self) -> None:
        while not self._stop.is_set():
            listener = self._listener
            if listener is None:
                return
            try:
                connection = listener.accept()
            except (AuthenticationError, OSError, EOFError):
                if not self._stop.is_set():
                    logger.debug("runtime-worker IPC accept 失败", exc_info=True)
                continue
            with connection:
                try:
                    raw = connection.recv()
                    if not isinstance(raw, dict):
                        raise WorkerCommandError("invalid_command", "本机命令格式无效")
                    operation = str(raw.get("operation") or "")
                    if operation == "__shutdown__":
                        connection.send({"ok": True, "value": {}})
                        self._stop.set()
                        continue
                    payload = raw.get("payload")
                    if not isinstance(payload, dict):
                        raise WorkerCommandError("invalid_command", "本机命令参数无效")
                    value = dict(self._handler(operation, payload))
                    connection.send({"ok": True, "value": value})
                except WorkerCommandError as exc:
                    connection.send({"ok": False, "code": exc.code, "message": str(exc)})
                except (BrokenPipeError, EOFError, OSError):
                    # A timed-out Web generation may disappear before the
                    # worker writes its answer.  The operation itself is
                    # still durable and can be observed through the ledger.
                    continue
                except BaseException:
                    logger.exception("runtime-worker IPC 命令失败")
                    try:
                        connection.send({
                            "ok": False,
                            "code": "worker_command_failed",
                            "message": "后台执行器未能完成命令",
                        })
                    except (BrokenPipeError, EOFError, OSError):
                        pass

    def stop(self) -> None:
        with self._lock:
            listener = self._listener
            thread = self._thread
        # Wake an accept() call so shutdown never inherits the historical
        # unbounded join failure.  The server sets the stop flag only after it
        # has authenticated this client and acknowledged the shutdown request;
        # setting it here first could make the accept loop exit before the
        # authentication handshake completes.
        try:
            with Client(self.endpoint, family=self.family, authkey=self._authkey) as connection:
                connection.send({"operation": "__shutdown__", "payload": {}})
                connection.recv()
        except (AuthenticationError, OSError, EOFError):
            self._stop.set()
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass
        if thread is not None:
            thread.join(timeout=1.0)
        with self._lock:
            if self._listener is listener:
                self._listener = None
            if self._thread is thread:
                self._thread = None
        if self.family == "AF_UNIX":
            Path(self.endpoint).unlink(missing_ok=True)


def call_worker_command(
    operation: str,
    payload: Mapping[str, Any] | None = None,
    *,
    timeout: float = DEFAULT_COMMAND_TIMEOUT_SECONDS,
    root: str | Path | None = None,
) -> dict[str, Any]:
    """Send one bounded command to runtime-worker and return its projection."""

    base = (Path(root) if root is not None else get_config().data_root).resolve()
    endpoint = worker_command_endpoint(base)
    family = "AF_PIPE" if os.name == "nt" else "AF_UNIX"
    deadline = max(0.05, float(timeout))
    try:
        connection = Client(endpoint, family=family, authkey=_authkey(base))
    except (AuthenticationError, OSError, EOFError) as exc:
        raise WorkerCommandUnavailable(
            "worker_unavailable", "后台 runtime-worker 未接受本机命令",
        ) from exc
    with connection:
        try:
            connection.send({"operation": str(operation), "payload": dict(payload or {})})
            if not connection.poll(deadline):
                raise WorkerCommandUnavailable(
                    "worker_unavailable", "后台 runtime-worker 在命令期限内未响应",
                )
            response = connection.recv()
        except WorkerCommandError:
            raise
        except (AuthenticationError, BrokenPipeError, EOFError, OSError) as exc:
            raise WorkerCommandUnavailable(
                "worker_unavailable", "后台 runtime-worker 的本机命令连接已中断",
            ) from exc
    if not isinstance(response, dict):
        raise WorkerCommandUnavailable("worker_unavailable", "后台 runtime-worker 返回无效响应")
    if not bool(response.get("ok")):
        raise WorkerCommandError(
            str(response.get("code") or "worker_command_failed"),
            str(response.get("message") or "后台执行器未能完成命令"),
        )
    value = response.get("value")
    if not isinstance(value, dict):
        raise WorkerCommandUnavailable("worker_unavailable", "后台 runtime-worker 返回无效结果")
    return value
