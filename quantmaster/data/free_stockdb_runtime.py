"""Manage the user-supplied free-stockdb process and its incremental updater."""

from __future__ import annotations

import hashlib
import html
import json
import logging
import os
import re
import socket
import sqlite3
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import httpx

from quantmaster.config import get_config
from quantmaster.runtime.sqlite import connect_sqlite
from quantmaster.trading_sessions import market_date

logger = logging.getLogger(__name__)
FREE_STOCKDB_MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class StockDBUpdateEvent:
    event_key: str
    kind: str
    payload: dict[str, Any]


def _monotonic() -> float:
    return time.monotonic()

_VENDOR_HOME = "https://www.app.workbuddy.link/"
_VENDOR_NOTICE_URL = f"{_VENDOR_HOME}tabs/notice.html"
_VENDOR_NOTICE_TTL = 6 * 60 * 60
_CONTROL_PATH_ENV = "QM_FREE_STOCKDB_CONTROL_PATH"
_AUTO_MAX_ATTEMPTS = 3
_AUTO_RETRY_SECONDS = 15 * 60
_UPDATER_TIMEOUT_SECONDS = 30 * 60
_TARGET_CHECK_SECONDS = 5 * 60
_SERVICE_CHECK_SECONDS = 5
_SERVICE_RESTART_BACKOFF_BASE_SECONDS = 2 * 60
_SERVICE_RESTART_BACKOFF_MAX_SECONDS = 30 * 60
_DATA_STABILITY_SECONDS = 10
_DATA_QUIESCENCE_POLL_SECONDS = 5
_OWNER_STALE_SECONDS = 120
_MIN_UPDATE_SYMBOL_COVERAGE = 0.98


class _RuntimeControl:
    """Small durable mailbox shared by the reload supervisor and Web worker."""

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS runtime_state (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    payload_json TEXT NOT NULL, updated_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS commands (
                    id TEXT PRIMARY KEY, action TEXT NOT NULL, trigger TEXT NOT NULL,
                    payload_json TEXT NOT NULL, status TEXT NOT NULL,
                    created_at REAL NOT NULL, claimed_at REAL NOT NULL DEFAULT 0,
                    completed_at REAL NOT NULL DEFAULT 0, result_json TEXT NOT NULL DEFAULT '{}');
                CREATE INDEX IF NOT EXISTS idx_freedb_commands_status
                    ON commands(status,created_at);
                CREATE TABLE IF NOT EXISTS events (
                    event_key TEXT PRIMARY KEY, kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL, status TEXT NOT NULL,
                    created_at REAL NOT NULL, claimed_at REAL NOT NULL DEFAULT 0,
                    completed_at REAL NOT NULL DEFAULT 0);
                CREATE INDEX IF NOT EXISTS idx_freedb_events_status
                    ON events(status,created_at);
            """)

    def _conn(self) -> sqlite3.Connection:
        return connect_sqlite(self.path, timeout=10.0, row_factory=True)

    @staticmethod
    def _is_locked(exc: sqlite3.OperationalError) -> bool:
        return "locked" in str(exc).lower() or "busy" in str(exc).lower()

    def _claim_with_retry(self, claim):
        """Retry only transient SQLite writer contention with a bounded delay."""
        delay = 0.02
        for attempt in range(5):
            try:
                return claim()
            except sqlite3.OperationalError as exc:
                if not self._is_locked(exc) or attempt == 4:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 0.25)

    def write_state(self, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, ensure_ascii=False, default=str)
        with self._conn() as connection:
            connection.execute(
                "INSERT INTO runtime_state(singleton,payload_json,updated_at) VALUES(1,?,?) "
                "ON CONFLICT(singleton) DO UPDATE SET payload_json=excluded.payload_json,"
                "updated_at=excluded.updated_at",
                (encoded, time.time()),
            )

    def read_state(self) -> dict[str, Any]:
        with self._conn() as connection:
            row = connection.execute(
                "SELECT payload_json,updated_at FROM runtime_state WHERE singleton=1"
            ).fetchone()
        if row is None:
            return {}
        try:
            payload = json.loads(str(row["payload_json"]))
        except (TypeError, ValueError):
            return {}
        payload["owner_heartbeat_at"] = float(row["updated_at"])
        return payload

    def enqueue(
        self, action: str, trigger: str, payload: dict[str, Any] | None = None,
    ) -> tuple[str, bool]:
        command_id = uuid.uuid4().hex
        encoded = json.dumps(payload or {}, ensure_ascii=False, default=str)
        now = time.time()
        with self._conn() as connection:
            active = connection.execute(
                "SELECT id FROM commands WHERE action=? AND status IN ('queued','running') "
                "ORDER BY created_at LIMIT 1",
                (action,),
            ).fetchone()
            if active is not None:
                return str(active["id"]), False
            connection.execute(
                "INSERT INTO commands(id,action,trigger,payload_json,status,created_at) "
                "VALUES(?,?,?,?, 'queued', ?)",
                (command_id, action, trigger, encoded, now),
            )
        return command_id, True

    def claim_command(self) -> dict[str, Any] | None:
        def claim():
            now = time.time()
            with self._conn() as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "UPDATE commands SET status='queued',claimed_at=0 "
                    "WHERE status='running' AND claimed_at<?",
                    (now - _OWNER_STALE_SECONDS,),
                )
                row = connection.execute(
                    "SELECT * FROM commands WHERE status='queued' ORDER BY created_at LIMIT 1"
                ).fetchone()
                if row is None:
                    return None
                connection.execute(
                    "UPDATE commands SET status='running',claimed_at=? WHERE id=?",
                    (now, row["id"]),
                )
                return row
        row = self._claim_with_retry(claim)
        if row is None:
            return None
        value = dict(row)
        value["payload"] = json.loads(str(value.pop("payload_json") or "{}"))
        return value

    def complete_command(self, command_id: str, result: dict[str, Any]) -> None:
        with self._conn() as connection:
            connection.execute(
                "UPDATE commands SET status='completed',completed_at=?,result_json=? WHERE id=?",
                (time.time(), json.dumps(result, ensure_ascii=False, default=str), command_id),
            )

    def command(self, command_id: str) -> dict[str, Any] | None:
        with self._conn() as connection:
            row = connection.execute("SELECT * FROM commands WHERE id=?", (command_id,)).fetchone()
        if row is None:
            return None
        value = dict(row)
        value["result"] = json.loads(str(value.pop("result_json") or "{}"))
        value.pop("payload_json", None)
        return value

    def emit(self, event_key: str, kind: str, payload: dict[str, Any]) -> None:
        with self._conn() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO events(event_key,kind,payload_json,status,created_at) "
                "VALUES(?,?,?,'pending',?)",
                (event_key, kind, json.dumps(payload, ensure_ascii=False, default=str), time.time()),
            )

    def claim_event(self) -> dict[str, Any] | None:
        def claim():
            now = time.time()
            with self._conn() as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "UPDATE events SET status='pending',claimed_at=0 "
                    "WHERE status='processing' AND claimed_at<?",
                    (now - _OWNER_STALE_SECONDS,),
                )
                row = connection.execute(
                    "SELECT * FROM events WHERE status='pending' ORDER BY created_at LIMIT 1"
                ).fetchone()
                if row is None:
                    return None
                connection.execute(
                    "UPDATE events SET status='processing',claimed_at=? WHERE event_key=?",
                    (now, row["event_key"]),
                )
                return row
        row = self._claim_with_retry(claim)
        if row is None:
            return None
        value = dict(row)
        value["payload"] = json.loads(str(value.pop("payload_json") or "{}"))
        return value

    def complete_event(self, event_key: str) -> None:
        with self._conn() as connection:
            connection.execute(
                "UPDATE events SET status='completed',completed_at=? WHERE event_key=?",
                (time.time(), event_key),
            )


class FreeStockDBRuntime:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._update_lock = threading.Lock()
        self._vendor_lock = threading.Lock()
        self._stop = threading.Event()
        self._process: subprocess.Popen[bytes] | None = None
        self._daemon_started = False
        self._updater_process: subprocess.Popen[bytes] | None = None
        self._thread: threading.Thread | None = None
        self._update_thread: threading.Thread | None = None
        self._owner = False
        self._supervised = False
        self._control: _RuntimeControl | None = None
        self._last_target_check = 0.0
        self._last_service_check = 0.0
        self._last_restart_fail = 0.0
        self._restart_failures = 0
        self._last_vendor_force = 0.0
        self._next_retry_at = 0.0
        self._retry_target = ""
        self._retry_attempt = 0
        # Automatic checks may run before the vendor publishes the next
        # trading session.  Remember that target so we do not repeatedly stop
        # a healthy local service for the same unavailable data.
        self._deferred_target = ""
        self._status: dict[str, Any] = {"state": "stopped", "message": "尚未启动"}

    @staticmethod
    def _root() -> Path:
        return get_config().free_stockdb_root

    @classmethod
    def _paths(cls) -> tuple[Path, Path, Path]:
        root = cls._root()
        return root, root / "stockdb.exe", root / "数据更新.exe"

    @classmethod
    def _owner_marker_path(cls) -> Path:
        return cls._root() / ".quantmaster-stockdb-owner.json"

    @staticmethod
    def _process_identity(pid: int) -> dict[str, Any] | None:
        if pid <= 0:
            return None
        if os.name != "nt":
            try:
                os.kill(pid, 0)
                image = str(Path(f"/proc/{pid}/exe").resolve())
                created = Path(f"/proc/{pid}").stat().st_ctime_ns
                return {"pid": pid, "image": image, "created": created}
            except (OSError, ValueError):
                return None
        import ctypes
        from ctypes import wintypes

        query_limited = 0x1000
        synchronize = 0x00100000
        wait_timeout = 0x00000102
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined]
        kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
        kernel32.WaitForSingleObject.restype = wintypes.DWORD
        kernel32.QueryFullProcessImageNameW.argtypes = (
            wintypes.HANDLE, wintypes.DWORD, wintypes.LPWSTR, ctypes.POINTER(wintypes.DWORD),
        )
        kernel32.QueryFullProcessImageNameW.restype = wintypes.BOOL
        kernel32.GetProcessTimes.argtypes = (
            wintypes.HANDLE, ctypes.POINTER(wintypes.FILETIME), ctypes.POINTER(wintypes.FILETIME),
            ctypes.POINTER(wintypes.FILETIME), ctypes.POINTER(wintypes.FILETIME),
        )
        kernel32.GetProcessTimes.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel32.CloseHandle.restype = wintypes.BOOL
        handle = kernel32.OpenProcess(query_limited | synchronize, False, pid)
        if not handle:
            return None
        try:
            if kernel32.WaitForSingleObject(handle, 0) != wait_timeout:
                return None
            size = wintypes.DWORD(32768)
            buffer = ctypes.create_unicode_buffer(size.value)
            if not kernel32.QueryFullProcessImageNameW(handle, 0, buffer, ctypes.byref(size)):
                return None
            process_created = wintypes.FILETIME()
            exited = wintypes.FILETIME()
            kernel = wintypes.FILETIME()
            user = wintypes.FILETIME()
            if not kernel32.GetProcessTimes(
                handle, ctypes.byref(process_created), ctypes.byref(exited),
                ctypes.byref(kernel), ctypes.byref(user),
            ):
                return None
            created_value = (
                (int(process_created.dwHighDateTime) << 32)
                | int(process_created.dwLowDateTime)
            )
            return {"pid": pid, "image": buffer.value, "created": created_value}
        finally:
            kernel32.CloseHandle(handle)

    @staticmethod
    def _terminate_pid(pid: int, timeout: float = 10.0) -> bool:
        if os.name != "nt":
            try:
                os.kill(pid, 15)
                return True
            except OSError:
                return False
        import ctypes
        from ctypes import wintypes

        terminate = 0x0001
        synchronize = 0x00100000
        wait_object_0 = 0
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined]
        kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.TerminateProcess.argtypes = (wintypes.HANDLE, wintypes.UINT)
        kernel32.TerminateProcess.restype = wintypes.BOOL
        kernel32.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
        kernel32.WaitForSingleObject.restype = wintypes.DWORD
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel32.CloseHandle.restype = wintypes.BOOL
        handle = kernel32.OpenProcess(terminate | synchronize, False, pid)
        if not handle:
            return False
        try:
            if not kernel32.TerminateProcess(handle, 0):
                return False
            return kernel32.WaitForSingleObject(handle, int(timeout * 1000)) == wait_object_0
        finally:
            kernel32.CloseHandle(handle)

    def _record_process_owner(self, executable: Path) -> None:
        process = self._process
        candidate_pids: list[int] = []
        try:
            candidate_pids.append(int((self._root() / "lgdb.pid").read_text().strip()))
        except (OSError, ValueError):
            pass
        launcher_pid = int(getattr(process, "pid", 0) or 0) if process is not None else 0
        if launcher_pid > 0:
            candidate_pids.append(launcher_pid)
        expected_image = str(executable.resolve()).casefold()
        process_identity = next((
            identity for pid in dict.fromkeys(candidate_pids)
            if (identity := self._process_identity(pid)) is not None
            and str(identity.get("image") or "").casefold() == expected_image
        ), None)
        owner_identity = self._process_identity(os.getpid())
        if process_identity is None or owner_identity is None:
            return
        payload = {
            "schema_version": 1,
            "root": str(self._root()),
            "executable": str(executable.resolve()),
            "process": process_identity,
            "owner": owner_identity,
            "created_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
        }
        path = self._owner_marker_path()
        temp = path.with_suffix(".tmp")
        temp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        temp.replace(path)

    def _clear_process_owner(self) -> None:
        try:
            self._owner_marker_path().unlink(missing_ok=True)
        except OSError:
            logger.warning("无法清理 free-stockdb owner 标记", exc_info=True)

    def _recover_managed_orphan(self, *executables: Path) -> bool:
        """Stop only a verified orphan previously launched by this workspace."""
        path = self._owner_marker_path()
        try:
            marker = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError):
            return False
        process = dict(marker.get("process") or {})
        owner = dict(marker.get("owner") or {})
        current_owner = self._process_identity(int(owner.get("pid") or 0))
        if current_owner and current_owner.get("created") == owner.get("created"):
            return False
        current_process = self._process_identity(int(process.get("pid") or 0))
        expected_images = {str(executable.resolve()).casefold() for executable in executables}
        verified = bool(
            current_process is not None
            and current_process.get("created") == process.get("created")
            and str(current_process.get("image") or "").casefold() in expected_images
            and str(marker.get("root") or "").casefold() == str(self._root()).casefold()
        )
        if not verified:
            self._clear_process_owner()
            return False
        assert current_process is not None
        pid = int(current_process["pid"])
        if not self._terminate_pid(pid):
            logger.warning("已识别 QuantMaster 遗留 stockdb，但无法终止 pid=%s", pid)
            return False
        self._clear_process_owner()
        deadline = time.monotonic() + 10
        while self._listening() and time.monotonic() < deadline:
            time.sleep(0.1)
        logger.info("已回收上次异常退出遗留的 stockdb 进程 pid=%s", pid)
        return not self._listening()

    @classmethod
    def _control_path(cls) -> Path:
        configured = os.environ.get(_CONTROL_PATH_ENV, "").strip()
        if configured:
            return Path(configured).expanduser().resolve()
        return cls._root() / ".quantmaster-control.sqlite"

    def _ensure_control(self) -> _RuntimeControl:
        path = self._control_path()
        if self._control is None or self._control.path != path:
            self._control = _RuntimeControl(path)
        return self._control

    @property
    def supervised(self) -> bool:
        return self._supervised

    @staticmethod
    def _endpoint() -> tuple[str, int] | None:
        parsed = urlsplit(get_config().data.free_stockdb_url)
        host = parsed.hostname or ""
        if host not in {"127.0.0.1", "localhost", "::1"}:
            return None
        try:
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
        except ValueError:
            return None
        return host, port

    def _listening(self) -> bool:
        endpoint = self._endpoint()
        if endpoint is None:
            return False
        try:
            with socket.create_connection(endpoint, timeout=0.25):
                return True
        except OSError:
            return False

    def _set_status(self, state: str, message: str, **extra: Any) -> None:
        with self._lock:
            for key in (
                "target_session",
                "actual_session",
                "validated_session",
                "attempt",
                "max_attempts",
                "next_retry_at",
                "validation",
                "service_restart_attempt",
                "service_restart_backoff_seconds",
                "service_restart_next_at",
            ):
                if key not in extra and key in self._status:
                    extra[key] = self._status[key]
            self._status = {
                "state": state, "message": message,
                "updated_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
                "owner_pid": os.getpid() if self._owner else 0,
                "supervised": self._supervised,
                **extra,
            }
            payload = dict(self._status)
        if self._owner:
            try:
                self._write_owner_state(payload)
            except (OSError, sqlite3.Error):
                logger.warning("free-stockdb 控制状态写入失败", exc_info=True)

    def _control_writer_lease(self) -> dict[str, Any]:
        identity = self._process_identity(os.getpid())
        if not identity:
            return {}
        return {
            "pid": int(identity["pid"]),
            "image": str(identity["image"]),
            "created": int(identity["created"]),
            "instance_root": str(self._root().resolve()),
            "control_path": str(self._control_path().resolve()),
        }

    def _write_owner_state(self, payload: dict[str, Any]) -> None:
        heartbeat = dict(payload)
        lease = self._control_writer_lease()
        if lease:
            heartbeat["control_writer"] = lease
        self._ensure_control().write_state(heartbeat)

    def _is_managed(self) -> bool:
        return self._daemon_started or self._process is not None

    def _service_restart_backoff_seconds(self) -> int:
        if self._restart_failures <= 0:
            return 0
        exponent = min(self._restart_failures - 1, 20)
        return min(
            _SERVICE_RESTART_BACKOFF_BASE_SECONDS * (2 ** exponent),
            _SERVICE_RESTART_BACKOFF_MAX_SECONDS,
        )

    def _service_restart_next_at(self) -> str:
        if self._last_restart_fail <= 0.0 or self._restart_failures <= 0:
            return ""
        remaining = self._last_restart_fail + self._service_restart_backoff_seconds()
        remaining -= time.monotonic()
        if remaining <= 0:
            return ""
        return datetime.fromtimestamp(
            time.time() + remaining, tz=ZoneInfo("Asia/Shanghai"),
        ).isoformat()

    def _service_restart_status(self) -> dict[str, Any]:
        return {
            "service_restart_attempt": self._restart_failures,
            "service_restart_backoff_seconds": self._service_restart_backoff_seconds(),
            "service_restart_next_at": self._service_restart_next_at(),
        }

    def _clear_service_restart_failure(self) -> None:
        self._last_restart_fail = 0.0
        self._restart_failures = 0

    def _reset_service_retry_state(self) -> None:
        self._clear_service_restart_failure()
        self._last_service_check = 0.0

    @staticmethod
    def _launch_service_process(
        executable: Path, config_path: Path, root: Path,
    ) -> subprocess.Popen[bytes]:
        # ``-d`` forks the vendor server away from QuantMaster, making it a
        # terminal/system child and preventing reliable lifetime ownership.
        # Foreground mode keeps stockdb as this Runtime Worker's actual child.
        command = [str(executable), str(config_path), "-s", "start"]
        return subprocess.Popen(
            command, cwd=root, stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

    @staticmethod
    def _executable_digest(executable: Path) -> str:
        return hashlib.sha256(executable.read_bytes()).hexdigest()

    def _start_service(self) -> bool:
        root, executable, _ = self._paths()
        config_path = root / "stockdb.conf"
        endpoint = self._endpoint()
        if endpoint is None:
            self._set_status("disabled", "服务地址不是本机回环地址，不执行进程托管")
            return False
        if self._stop.is_set():
            return False
        if self._listening():
            if self._process is None and self._recover_managed_orphan(executable):
                logger.info("free-stockdb 孤儿进程已回收，准备重新托管")
            else:
                self._set_status("running", "本地服务已运行", managed=self._is_managed())
                return True
        if not executable.is_file() or not config_path.is_file() or not (root / "data").is_dir():
            self._set_status("missing", f"等待完整发行包：{root}", managed=False)
            logger.warning("free-stockdb 未就绪，等待完整发行包：%s", root)
            return False
        code: int | None = None
        for launch_attempt in range(2):
            try:
                source_digest = self._executable_digest(executable)
                self._process = self._launch_service_process(executable, config_path, root)
            except (OSError, RuntimeError) as exc:
                logger.error("free-stockdb 托管启动失败", exc_info=True)
                self._set_status(
                    "error", f"本地服务启动失败：{type(exc).__name__}", managed=False,
                )
                return False
            launcher = self._process
            deadline = _monotonic() + 10
            while _monotonic() < deadline and not self._stop.is_set():
                if self._listening():
                    self._daemon_started = True
                    self._record_process_owner(executable)
                    self._set_status("running", "本地服务由 QuantMaster 托管", managed=True)
                    logger.info("free-stockdb 已启动 · %s:%s", *endpoint)
                    return True
                # daemon 启动器可能在后台服务开始监听前正常退出，不能据此提前失败。
                self._stop.wait(0.25)
            code = launcher.poll()
            if code is None:
                self._terminate_process(launcher, timeout=3)
            self._process = None
            try:
                replaced = self._executable_digest(executable) != source_digest
            except OSError:
                replaced = False
            if launch_attempt == 0 and replaced and not self._stop.is_set():
                logger.info("free-stockdb 引导程序已替换平台运行版，正在重新启动")
                continue
            break
        self._daemon_started = False
        self._clear_process_owner()
        self._set_status("error", "本地服务未能在受控启动窗口内就绪", exit_code=code)
        logger.warning("free-stockdb 启动后未能在受控启动窗口内就绪")
        return False

    @staticmethod
    def _terminate_process(process: subprocess.Popen[bytes], timeout: float = 15) -> None:
        try:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            try:
                process.kill()
                process.wait(timeout=5)
            except OSError:
                return
        except OSError:
            return

    @staticmethod
    def _post_windows_close(pid: int) -> bool:
        """Post WM_CLOSE only to top-level windows owned by one tracked PID."""
        if os.name != "nt":
            return False
        try:
            import ctypes
            from ctypes import wintypes

            user32 = ctypes.WinDLL(  # type: ignore[attr-defined]
                "user32", use_last_error=True,
            )
            callback_type = ctypes.WINFUNCTYPE(  # type: ignore[attr-defined]
                wintypes.BOOL, wintypes.HWND, wintypes.LPARAM,
            )
            handles = []

            @callback_type
            def visit(window, _parameter):
                owner = wintypes.DWORD()
                user32.GetWindowThreadProcessId(window, ctypes.byref(owner))
                if int(owner.value) == int(pid):
                    handles.append(window)
                return True

            user32.EnumWindows(visit, 0)
            posted = False
            for window in handles:
                posted = bool(user32.PostMessageW(window, 0x0010, 0, 0)) or posted
            return posted
        except (AttributeError, OSError, TypeError, ValueError):
            logger.warning("无法向 free-stockdb 更新器发送正常关闭请求", exc_info=True)
            return False

    @classmethod
    def _close_process_window(
        cls, process: subprocess.Popen[bytes], *, timeout: float = 3,
    ) -> bool:
        """Request a normal window close without terminating the process."""
        if process.poll() is not None:
            return True
        if not cls._post_windows_close(int(process.pid)):
            return process.poll() is not None
        try:
            process.wait(timeout=timeout)
            return True
        except subprocess.TimeoutExpired:
            return process.poll() is not None
        except OSError:
            return process.poll() is not None

    def _check_stable_updater_data(
        self,
        process: subprocess.Popen[bytes],
        *,
        target: str,
        trigger: str,
        baseline: tuple[tuple[str, int, int], ...],
        current: tuple[tuple[str, int, int], ...],
        stable_for: float,
        validated: tuple[tuple[str, int, int], ...] | None,
        accepted: tuple[tuple[str, int, int], ...] | None,
    ) -> tuple[
        tuple[tuple[str, int, int], ...] | None,
        tuple[tuple[str, int, int], ...] | None,
        bool,
    ]:
        if current == baseline or stable_for < _DATA_STABILITY_SECONDS:
            return validated, accepted, False
        if validated != current:
            from quantmaster.data.free_stockdb_source import _invalidate_sdk_clients

            self._set_status(
                "updating", f"数据已稳定，正在验收 {target}",
                phase="validating", trigger=trigger, update_result="validating",
                target_session=target,
            )
            _invalidate_sdk_clients()
            validation = self._validate_data(target)
            validated = current
            accepted = current if validation.get("accepted") else None
            if accepted is None:
                self._set_status(
                    "updating", "数据已变化，但目标日尚未通过验收，继续等待",
                    phase="syncing", trigger=trigger, update_result="running",
                    target_session=target, validation=validation,
                )
        closed = accepted == current and self._close_process_window(process)
        return validated, accepted, closed

    def _stop_service(self) -> bool:
        process = self._process
        if not self._daemon_started and process is None:
            return not self._listening()
        if process is not None and process.poll() is None:
            self._terminate_process(process)
        self._process = None
        deadline = time.monotonic() + 10
        while self._listening() and time.monotonic() < deadline:
            self._stop.wait(0.1)
        stopped = not self._listening()
        if stopped:
            self._daemon_started = False
            self._clear_process_owner()
        return stopped

    def _run_updater(
        self, updater: Path, root: Path, *, trigger: str, target: str,
    ) -> int:
        # 当前发行包没有公开、可验证的静默参数，因此保留原生窗口；只有
        # 本地目标日验收通过后才按本次 PID 请求正常关闭。
        baseline = self._data_fingerprint(root)
        process = subprocess.Popen(
            [str(updater)], cwd=root, stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        with self._lock:
            self._updater_process = process
        deadline = time.monotonic() + _UPDATER_TIMEOUT_SECONDS
        started = time.monotonic()
        previous = baseline
        stable_since = started
        next_data_check = started
        validated_fingerprint = None
        accepted_fingerprint = None
        try:
            while process.poll() is None:
                accepted = accepted_fingerprint == previous
                self._set_status(
                    "updating", (
                        "本地数据已验收，正在等待更新器窗口正常关闭"
                        if accepted else "正在同步 free-stockdb 本地数据"
                    ),
                    phase="closing" if accepted else "syncing",
                    elapsed_seconds=int(time.monotonic() - started),
                    trigger=trigger, update_result="running",
                )
                if self._stop.wait(0.5):
                    self._terminate_process(process, timeout=5)
                    return -1
                if time.monotonic() >= deadline:
                    self._terminate_process(process, timeout=5)
                    raise subprocess.TimeoutExpired(str(updater), _UPDATER_TIMEOUT_SECONDS)
                now = time.monotonic()
                if now < next_data_check:
                    continue
                next_data_check = now + _DATA_QUIESCENCE_POLL_SECONDS
                current = self._data_fingerprint(root)
                if current != previous:
                    previous = current
                    stable_since = now
                    validated_fingerprint = None
                    accepted_fingerprint = None
                    continue
                validated_fingerprint, accepted_fingerprint, closed = (
                    self._check_stable_updater_data(
                        process, target=target, trigger=trigger,
                        baseline=baseline, current=current,
                        stable_for=now - stable_since,
                        validated=validated_fingerprint,
                        accepted=accepted_fingerprint,
                    )
                )
                if closed:
                    return int(process.returncode or 0)
            return int(process.returncode or 0)
        finally:
            with self._lock:
                if self._updater_process is process:
                    self._updater_process = None

    @staticmethod
    def _data_roots(root: Path) -> tuple[Path, ...]:
        try:
            partitions = [
                path for path in root.iterdir()
                if path.is_dir() and re.fullmatch(r"data\d*", path.name, re.IGNORECASE)
            ]
        except OSError:
            return ()
        return tuple(sorted(
            partitions, key=lambda path: int(path.name[4:] or 0),
        ))

    @classmethod
    def _data_fingerprint(cls, root: Path) -> tuple[tuple[str, int, int], ...]:
        """Return a cheap fingerprint for vendor data files, including child writes."""
        bases = cls._data_roots(root)
        if not bases:
            return ()
        rows: list[tuple[str, int, int]] = []
        for base in bases:
            try:
                for path in base.rglob("*"):
                    if not path.is_file():
                        continue
                    try:
                        stat = path.stat()
                    except OSError:
                        continue
                    rows.append((
                        str(path.relative_to(root)),
                        int(stat.st_size),
                        int(stat.st_mtime_ns),
                    ))
            except OSError:
                continue
        return tuple(sorted(rows))

    def _wait_for_data_quiescent(
        self,
        root: Path,
        *,
        timeout_seconds: float = _TARGET_CHECK_SECONDS,
        stable_seconds: float = _DATA_STABILITY_SECONDS,
    ) -> bool:
        """Wait until the updater and any detached child stop changing data files."""
        deadline = time.monotonic() + max(0.0, float(timeout_seconds))
        previous = self._data_fingerprint(root)
        if not previous and not self._data_roots(root):
            return True
        stable_since = time.monotonic()
        while not self._stop.is_set():
            now = time.monotonic()
            if now - stable_since >= max(0.0, float(stable_seconds)):
                return True
            if now >= deadline:
                return False
            if self._stop.wait(min(_DATA_QUIESCENCE_POLL_SECONDS, max(0.01, deadline - now))):
                return False
            current = self._data_fingerprint(root)
            if current != previous:
                previous = current
                stable_since = time.monotonic()
        return False

    def _validate_until_ready(
        self,
        target: str,
        root: Path,
        *,
        timeout_seconds: float = _TARGET_CHECK_SECONDS,
    ) -> dict[str, Any]:
        """Recheck a just-written target every five seconds for up to five minutes."""
        validation = self._validate_data(target)
        if validation.get("accepted"):
            return validation
        # A test fixture or an installation without a vendor data directory has
        # no asynchronous writer to wait for; keep the failure immediate there.
        if not self._data_fingerprint(root) and not self._data_roots(root):
            return validation
        deadline = time.monotonic() + max(0.0, float(timeout_seconds))
        while not self._stop.is_set() and time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            if self._stop.wait(min(_DATA_QUIESCENCE_POLL_SECONDS, max(0.01, remaining))):
                break
            validation = self._validate_data(target)
            if validation.get("accepted"):
                return validation
        return validation

    def _marker_path(self) -> Path:
        return self._root() / ".quantmaster-update.json"

    @staticmethod
    def _vendor_cache_path() -> Path:
        return get_config().data_root / "free_stockdb_vendor_notice.json"

    @staticmethod
    def _parse_vendor_notice(document: str) -> dict[str, str]:
        updated_match = re.search(r"更新至\s*[:：]\s*(\d{4}-\d{2}-\d{2})", document)
        version_match = re.search(
            r"最新版本\s*v?([^\s<,，。；;]+)", document, flags=re.IGNORECASE,
        )
        announcement_match = re.search(
            r"<h3\b[^>]*class=[\"'][^\"']*\bcard-title\b[^\"']*[\"'][^>]*>"
            r"(.*?)</h3>", document,
            flags=re.IGNORECASE | re.DOTALL,
        )
        version = html.unescape(version_match.group(1)).strip() if version_match else ""
        version = re.sub(r"\s+", " ", version).rstrip("。；; ")
        announcement = announcement_match.group(1) if announcement_match else ""
        announcement = re.sub(r"<[^>]+>", " ", announcement)
        announcement = re.sub(r"\s+", " ", html.unescape(announcement)).strip()
        return {
            "notice_updated_on": updated_match.group(1) if updated_match else "",
            "version": version,
            "announcement": announcement,
        }

    def _read_vendor_cache(self) -> dict[str, Any]:
        try:
            value = json.loads(self._vendor_cache_path().read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
        except (OSError, ValueError, TypeError):
            return {}

    def cached_vendor_notice(self) -> dict[str, Any]:
        """Return only the last local vendor-notice snapshot.

        Settings-page GETs use this method so a missing or expired cache never
        promotes into an HTTP request.  The supervisor-owned runtime refreshes
        the cache through :meth:`check_vendor_notice` on its own schedule.
        """

        cached = self._read_vendor_cache()
        return cached or {
            "status": "unavailable",
            "url": _VENDOR_HOME,
            "message": "尚无本地公告快照；后台运行时会在下一次验收时更新",
        }

    def check_vendor_notice(self, *, force: bool = False) -> dict[str, Any]:
        """Read the vendor announcement without ever opening the vendor website."""
        cached = self._read_vendor_cache()
        checked_at = str(cached.get("checked_at") or "")
        if checked_at and cached.get("url") == _VENDOR_HOME and not force:
            try:
                age = datetime.now(ZoneInfo("Asia/Shanghai")) - datetime.fromisoformat(checked_at)
                if age.total_seconds() < _VENDOR_NOTICE_TTL:
                    return cached
            except ValueError:
                pass
        if not self._vendor_lock.acquire(blocking=False):
            return cached or {"status": "checking", "url": _VENDOR_HOME}
        try:
            with httpx.Client(timeout=4.0, follow_redirects=True) as client:
                response = client.get(_VENDOR_NOTICE_URL)
                response.raise_for_status()
            details = self._parse_vendor_notice(response.text)
            now = datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()
            fingerprint = (
                f"{details['notice_updated_on']}|{details['version']}|"
                f"{details['announcement']}"
            )
            notice: dict[str, Any] = {
                "status": "ok", "checked_at": now, "url": _VENDOR_HOME,
                "notice_updated_on": details["notice_updated_on"],
                "version": details["version"],
                "announcement": details["announcement"],
                "fingerprint": fingerprint,
            }
            path = self._vendor_cache_path()
            path.parent.mkdir(parents=True, exist_ok=True)
            temp = path.with_suffix(".tmp")
            temp.write_text(json.dumps(notice, ensure_ascii=False), encoding="utf-8")
            temp.replace(path)
            return notice
        except (httpx.HTTPError, OSError) as exc:
            logger.info("free-stockdb 官方动态暂不可用：%s", exc)
            return {
                **cached, "status": "stale" if cached else "unavailable",
                "url": _VENDOR_HOME, "error": type(exc).__name__,
            }
        finally:
            self._vendor_lock.release()

    def _read_marker(self) -> dict[str, Any]:
        try:
            value = json.loads(self._marker_path().read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
        except (OSError, ValueError, TypeError):
            return {}

    def _last_update_date(self) -> str:
        # Legacy markers recorded the calendar day when the updater exited.  They
        # did not prove that the database contained that trading session.
        return str(self._read_marker().get("validated_session") or "")

    def _record_update(
        self, code: int, target_session: str, validation: dict[str, Any], attempt: int,
    ) -> None:
        now = datetime.now(ZoneInfo("Asia/Shanghai"))
        validated = str(validation.get("actual_session") or target_session)
        payload = {
            "schema_version": 2,
            "date": validated,
            "validated_session": validated,
            "target_session": target_session,
            "updated_at": now.isoformat(),
            "exit_code": code,
            "attempt": attempt,
            "validation": validation,
        }
        path = self._marker_path()
        temp = path.with_suffix(".tmp")
        temp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        temp.replace(path)

    def _target_session(self, *, force_notice: bool = False) -> tuple[str, str]:
        self.check_vendor_notice(force=force_notice)
        try:
            from quantmaster.data.free_stockdb_source import register_free_stockdb_calendar
            from quantmaster.trading_sessions import resolve_session_target

            register_free_stockdb_calendar()
            expectation = resolve_session_target()
            coverage = getattr(expectation, "coverage", {}) or {}
            official_sessions: list[str] = []
            for value in coverage.get("official_dates", ()):  # update targets only
                try:
                    official_sessions.append(date.fromisoformat(str(value)[:10]).isoformat())
                except ValueError:
                    continue
            if official_sessions:
                return max(official_sessions), str(
                    coverage.get("official_source") or expectation.source
                )
            calendar_target = (
                getattr(expectation, "completion", "")
                == "current_session_closed_waiting_provider"
            )
            if expectation.session and (expectation.ready or calendar_target):
                return expectation.session, expectation.source
        except (ImportError, OSError, RuntimeError, TypeError, ValueError):
            logger.info("无法解析 free-stockdb 目标交易日", exc_info=True)
        return "", "unavailable"

    def _validate_data(self, target_session: str) -> dict[str, Any]:
        """Validate a target-date full-market slice without publishing research."""
        import numpy as np
        import pandas as pd

        from quantmaster.data.free_stockdb_source import FreeStockDBSource
        from quantmaster.data.instruments import InstrumentStore

        active = {"listed", "active", "l"}
        symbols = [
            item.symbol for item in InstrumentStore().list(market="CN", asset_type="stock")
            if item.status.casefold() in active and item.exchange in {"SH", "SZ", "BJ"}
        ]
        result: dict[str, Any] = {
            "target_session": target_session,
            "actual_session": "",
            "expected_symbols": len(symbols),
            "observed_symbols": 0,
            "symbol_ratio": 0.0,
            "required_ohlcv_ratio": 0.0,
            "accepted": False,
            "complete": False,
            "warnings": [],
            "issues": [],
        }
        if not target_session or not symbols:
            result["issues"] = ["无法确定目标交易日或 A 股证券目录为空"]
            return result
        target = date.fromisoformat(target_session)
        start = (target - timedelta(days=10)).isoformat()
        source = FreeStockDBSource()
        frames = []
        try:
            for offset in range(0, len(symbols), 300):
                frames.append(source.daily_cross_section(
                    symbols[offset:offset + 300], start, target_session,
                ))
        except Exception as exc:
            logger.warning("读取 free-stockdb 验证截面失败", exc_info=True)
            result["issues"] = [
                f"读取 free-stockdb 验证截面失败：{type(exc).__name__}: {str(exc)[:240]}"
            ]
            return result
        frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if frame.empty:
            result["issues"] = ["free-stockdb 没有返回可验证的日频截面"]
            return result
        dates = pd.to_datetime(frame["date"], errors="coerce")
        actual = dates.max()
        result["actual_session"] = "" if pd.isna(actual) else actual.date().isoformat()
        latest = frame.loc[dates.dt.date == target]
        observed_symbols = set(latest["symbol"].dropna().astype(str).str.upper())
        missing_symbols = set(symbols) - observed_symbols
        suspension_evidence: dict[str, Any] = {}
        excused_suspensions: set[str] = set()
        suspension_error = ""
        if missing_symbols and get_config().data.tushare_token:
            try:
                from quantmaster.data.instrument_snapshots import (
                    load_or_fetch_suspension_snapshot,
                )
                from quantmaster.data.tushare_source import TushareSource

                suspension_evidence = load_or_fetch_suspension_snapshot(
                    TushareSource(), target_session,
                )
                excused_suspensions = missing_symbols & {
                    str(value).upper()
                    for value in suspension_evidence.get("symbols") or ()
                }
            except (ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
                suspension_error = f"Tushare suspend_d 停牌证据不可用：{str(exc)[:240]}"
        expected_trading = set(symbols) - excused_suspensions
        observed = len(observed_symbols & expected_trading)
        unresolved_missing = expected_trading - observed_symbols
        symbol_ratio = observed / len(expected_trading) if expected_trading else 1.0
        required = ["open", "high", "low", "close", "volume"]
        valid_required = pd.Series(False, index=latest.index)
        invalid_finite = invalid_ohlc = invalid_volume = len(latest)
        if not latest.empty and all(column in latest for column in required):
            numeric = latest[required].apply(pd.to_numeric, errors="coerce")
            finite = numeric.map(np.isfinite).all(axis=1)
            prices = numeric[["open", "high", "low", "close"]]
            positive_prices = prices.gt(0).all(axis=1)
            ohlc_consistent = (
                numeric["high"].ge(prices[["open", "close"]].max(axis=1))
                & numeric["low"].le(prices[["open", "close"]].min(axis=1))
                & numeric["high"].ge(numeric["low"])
            )
            nonnegative_volume = numeric["volume"].ge(0)
            valid_required = finite & positive_prices & ohlc_consistent & nonnegative_volume
            invalid_finite = int((~finite).sum())
            invalid_ohlc = int((~(positive_prices & ohlc_consistent)).sum())
            invalid_volume = int((~nonnegative_volume).sum())
        required_ratio = float(valid_required.mean()) if len(valid_required) else 0.0
        warnings = []
        issues = []
        if symbol_ratio < 1.0:
            warnings.append(
                f"目标日 stockdb 截面覆盖 {observed}/{len(expected_trading)} 只应交易证券；"
                f"{len(unresolved_missing)} 只缺口将交由后续混合数据源补齐，"
                "未补齐部分保留缺失标记"
            )
        if suspension_error:
            warnings.append(f"{suspension_error}；不阻断本次更新验收")
        if result["actual_session"] != target_session:
            issues.append(
                f"free-stockdb 最新交易日为 {result['actual_session'] or '未知'}，"
                f"尚未到达目标日 {target_session}"
            )
        if observed <= 0:
            issues.append("目标日 stockdb 截面没有可用证券")
        if symbol_ratio < _MIN_UPDATE_SYMBOL_COVERAGE:
            issues.append(
                f"目标日证券覆盖率 {symbol_ratio:.1%} 低于更新验收线 "
                f"{_MIN_UPDATE_SYMBOL_COVERAGE:.0%}"
            )
        if required_ratio < 1.0:
            issues.append(f"目标日完整 OHLCV 比例仅 {required_ratio:.1%}")
        accepted = not issues
        result.update({
            "catalog_symbols": len(symbols),
            "expected_trading_symbols": len(expected_trading),
            "observed_symbols": observed,
            "symbol_ratio": round(symbol_ratio, 6),
            "missing_symbol_count": len(unresolved_missing),
            "missing_symbol_sample": sorted(unresolved_missing)[:50],
            "excused_suspended_symbols": sorted(excused_suspensions),
            "suspension_evidence": suspension_evidence,
            "required_ohlcv_ratio": round(required_ratio, 6),
            "invalid_ohlcv": {
                "nonfinite_rows": invalid_finite,
                "price_or_ohlc_rows": invalid_ohlc,
                "negative_volume_rows": invalid_volume,
            },
            "accepted": accepted,
            "complete": accepted and symbol_ratio == 1.0 and not suspension_error,
            "warnings": warnings,
            "issues": issues,
        })
        return result

    def _emit_update_event(self, kind: str, target: str, payload: dict[str, Any]) -> None:
        try:
            self._ensure_control().emit(f"{kind}:{target}", kind, payload)
        except (OSError, sqlite3.Error):
            logger.warning("free-stockdb 更新事件写入失败", exc_info=True)

    @staticmethod
    def _market_session_available(target: str, validation: dict[str, Any]) -> bool:
        """Return whether a strict hybrid market refresh can consume this session."""
        try:
            target_date = date.fromisoformat(target)
            actual_date = date.fromisoformat(str(validation.get("actual_session") or ""))
            observed = int(validation.get("observed_symbols") or 0)
            ohlcv_ratio = float(validation.get("required_ohlcv_ratio") or 0.0)
        except (TypeError, ValueError):
            return False
        return actual_date >= target_date and observed > 0 and ohlcv_ratio == 1.0

    def _finish_success(
        self, *, target: str, validation: dict[str, Any], code: int,
        attempt: int, trigger: str,
    ) -> bool:
        warnings = [str(value) for value in validation.get("warnings") or () if value]
        message = f"数据已验证至 {target}，本地服务已恢复"
        if warnings:
            message = f"{message}；{warnings[0]}（可继续扫描或稍后重试）"
        self._next_retry_at = 0.0
        self._retry_target = ""
        self._retry_attempt = 0
        self._deferred_target = ""
        self._record_update(code, target, validation, attempt)
        self._set_status(
            "running", message,
            phase="completed", update_result="success", exit_code=code,
            trigger=trigger, target_session=target,
            actual_session=str(validation.get("actual_session") or ""),
            validated_session=target, attempt=attempt,
            max_attempts=_AUTO_MAX_ATTEMPTS if trigger != "manual" else 1,
            next_retry_at="", validation=validation, managed=self._is_managed(),
        )
        # The SDK and native extension are replaced in place by some vendor
        # releases. Do not let this data adapter retain the old client. Domain
        # cache invalidation is delivered from the composition root event plan.
        try:
            from quantmaster.data.free_stockdb_source import _invalidate_sdk_clients

            _invalidate_sdk_clients()
        except (ImportError, RuntimeError):
            logger.warning("free-stockdb 更新后重置 SDK client 失败", exc_info=True)
        event_kind = (
            "update_succeeded"
            if bool(validation.get("complete"))
            else "market_session_partial"
        )
        self._emit_update_event(event_kind, target, {
            "target_session": target, "validation": validation, "trigger": trigger,
        })
        logger.info(
            "free-stockdb %s至 %s",
            "完整数据已验证" if event_kind == "update_succeeded" else "部分数据已接收",
            target,
        )
        return True

    def _finish_failure(
        self, *, target: str, validation: dict[str, Any], code: int,
        attempt: int, trigger: str, message: str, allow_retry: bool = True,
    ) -> bool:
        automatic = trigger in {"schedule", "retry"}
        if self._market_session_available(target, validation):
            self._emit_update_event("market_session_available", target, {
                "target_session": target, "validation": validation,
                "trigger": trigger, "message": message,
            })
        if automatic and allow_retry and attempt < _AUTO_MAX_ATTEMPTS and not self._stop.is_set():
            self._retry_target = target
            self._retry_attempt = attempt + 1
            self._next_retry_at = time.time() + _AUTO_RETRY_SECONDS
            retry_at = datetime.fromtimestamp(
                self._next_retry_at, tz=ZoneInfo("Asia/Shanghai"),
            ).isoformat()
            self._set_status(
                "running", f"{message}；将在 15 分钟后进行第 {attempt + 1} 次尝试",
                phase="retry_wait", update_result="retry_wait", exit_code=code,
                trigger=trigger, target_session=target,
                actual_session=str(validation.get("actual_session") or ""),
                validated_session=self._last_update_date(), attempt=attempt,
                max_attempts=_AUTO_MAX_ATTEMPTS, next_retry_at=retry_at,
                validation=validation, managed=self._is_managed(),
            )
            return False
        self._next_retry_at = 0.0
        self._retry_target = ""
        self._retry_attempt = 0
        result = "manual_required" if not allow_retry else "failed"
        state = "degraded" if result == "manual_required" else "running"
        self._set_status(
            state, message, phase="completed", update_result=result, exit_code=code,
            trigger=trigger, target_session=target,
            actual_session=str(validation.get("actual_session") or ""),
            validated_session=self._last_update_date(), attempt=attempt,
            max_attempts=_AUTO_MAX_ATTEMPTS if automatic else 1,
            next_retry_at="", validation=validation, managed=self._is_managed(),
        )
        if automatic:
            self._emit_update_event("update_failed", target or market_date().isoformat(), {
                "target_session": target, "validation": validation,
                "attempt": attempt, "message": message,
            })
        return False

    def update_now(
        self, trigger: str = "manual", *, target_session: str = "", attempt: int = 1,
    ) -> bool:
        if not self._update_lock.acquire(blocking=False):
            return False
        try:
            return self._update_now_locked(
                trigger=trigger, target_session=target_session, attempt=attempt,
            )
        finally:
            self._update_lock.release()

    def _update_now_locked(self, *, trigger: str, target_session: str, attempt: int) -> bool:
        root, _, updater = self._paths()
        if self._stop.is_set():
            return False
        target, target_source = (
            (target_session, "retry") if target_session else self._target_session(force_notice=True)
        )
        blank_validation: dict[str, Any] = {
            "target_session": target, "actual_session": "", "accepted": False,
            "complete": False, "warnings": [], "issues": [],
        }
        if not target:
            blank_validation["issues"] = ["无法从官方动态或可信交易日历确定目标交易日"]
            return self._finish_failure(
                target="", validation=blank_validation, code=-1, attempt=attempt,
                trigger=trigger, message=blank_validation["issues"][0],
            )
        self._set_status(
            "updating", f"正在验证本地库是否已包含 {target}", phase="validating",
            trigger=trigger, update_result="validating", target_session=target,
            actual_session="", validated_session=self._last_update_date(), attempt=attempt,
            max_attempts=_AUTO_MAX_ATTEMPTS if trigger != "manual" else 1,
            next_retry_at="", validation=blank_validation, target_source=target_source,
                managed=self._is_managed(),
        )
        preflight = self._validate_data(target)
        if preflight.get("accepted"):
            return self._finish_success(
                target=target, validation=preflight, code=0, attempt=attempt, trigger=trigger,
            )
        # A scheduled check commonly runs before free-stockdb has published
        # today's session (or while its provider circuit is open).  If an
        # already accepted session exists, keep serving it and defer the
        # updater instead of taking the local service offline for 30 minutes.
        last_validated = self._last_update_date()
        actual_session = str(preflight.get("actual_session") or "")
        if (
            trigger in {"schedule", "retry"}
            and last_validated
            and (not actual_session or actual_session <= last_validated)
        ):
            self._deferred_target = target
            message = (
                f"目标日 {target} 尚无新数据，继续使用已验收 {last_validated}"
                if actual_session
                else f"free-stockdb 暂不可用，继续使用已验收 {last_validated}"
            )
            self._set_status(
                "running", message, phase="deferred", update_result="deferred",
                trigger=trigger, target_session=target,
                actual_session=actual_session, validated_session=last_validated,
                attempt=attempt, max_attempts=_AUTO_MAX_ATTEMPTS,
                next_retry_at="", validation=preflight, managed=self._is_managed(),
            )
            return True
        if not updater.is_file():
            return self._finish_failure(
                target=target, validation=preflight, code=-1, attempt=attempt,
                trigger=trigger, message=f"未找到更新器：{updater}",
            )
        if not self._is_managed() and self._listening():
            message = "stockdb 由外部进程持有，无法安全自动停止；请手动更新"
            logger.warning(message)
            return self._finish_failure(
                target=target, validation=preflight, code=-1, attempt=attempt,
                trigger=trigger, message=message, allow_retry=False,
            )
        self._set_status(
            "updating", "正在停止本地数据库", phase="stopping",
            trigger=trigger, update_result="running", target_session=target,
            actual_session=str(preflight.get("actual_session") or ""),
            validated_session=self._last_update_date(), attempt=attempt,
            max_attempts=_AUTO_MAX_ATTEMPTS if trigger != "manual" else 1,
                    next_retry_at="", validation=preflight, managed=self._is_managed(),
        )
        if not self._stop_service():
            return self._finish_failure(
                target=target, validation=preflight, code=-1, attempt=attempt,
                trigger=trigger, message="本地数据库未能安全停止，已取消数据更新",
            )
        code = -1
        updater_error = ""
        try:
            code = self._run_updater(updater, root, trigger=trigger, target=target)
        except subprocess.TimeoutExpired:
            updater_error = "原生更新器运行超过 30 分钟，已终止"
            logger.error(updater_error)
        except OSError as exc:
            updater_error = f"原生更新器启动失败：{str(exc)[:300]}"
            logger.error("free-stockdb 自动更新失败：%s", exc)
        if not self._stop.is_set() and not self._wait_for_data_quiescent(root):
            message = updater_error or "更新器退出后数据目录仍在写入，未提前重启本地服务"
            logger.error(message)
            return self._finish_failure(
                target=target, validation=preflight, code=code, attempt=attempt,
                trigger=trigger, message=message,
            )
        restored = False
        if not self._stop.is_set():
            self._set_status(
                "updating", "数据同步结束，正在恢复本地服务", phase="restarting",
                trigger=trigger, update_result="running", target_session=target,
                actual_session=str(preflight.get("actual_session") or ""),
                validated_session=self._last_update_date(), attempt=attempt,
                max_attempts=_AUTO_MAX_ATTEMPTS if trigger != "manual" else 1,
                next_retry_at="", validation=preflight,
            )
            restored = self._start_service()
        try:
            from quantmaster.data.resilience import PROVIDER_HEALTH

            PROVIDER_HEALTH.reset("free-stockdb")
        except (ImportError, RuntimeError):
            logger.warning("free-stockdb 重启后重置数据源熔断失败", exc_info=True)
        if not restored:
            return self._finish_failure(
                target=target, validation=preflight, code=code, attempt=attempt,
                trigger=trigger, message="更新结束，但本地服务恢复失败",
            )
        self._set_status(
            "updating", f"正在验收 {target} 全市场日线", phase="validating",
            trigger=trigger, update_result="validating", target_session=target,
            actual_session=str(preflight.get("actual_session") or ""),
            validated_session=self._last_update_date(), attempt=attempt,
            max_attempts=_AUTO_MAX_ATTEMPTS if trigger != "manual" else 1,
                    next_retry_at="", validation=preflight, managed=self._is_managed(),
        )
        validation = self._validate_until_ready(target, root)
        if validation.get("accepted"):
            return self._finish_success(
                target=target, validation=validation, code=code,
                attempt=attempt, trigger=trigger,
            )
        issue = "；".join(validation.get("issues") or []) or "真实交易日验收未通过"
        message = updater_error or (
            f"更新器退出码 {code}；{issue}" if code else issue
        )
        return self._finish_failure(
            target=target, validation=validation, code=code, attempt=attempt,
            trigger=trigger, message=message,
        )

    def _reset_service_retry(self) -> dict[str, Any]:
        self._reset_service_retry_state()
        managed = bool(get_config().data.free_stockdb_managed)
        self._set_status(
            "degraded" if managed else "disabled",
            "已手动重置 free-stockdb 重试退避，监督器将立即重新检查",
            managed=self._is_managed(),
            **self._service_restart_status(),
        )
        return {"status": "reset", **self.status()}

    def request_reset_service_retry(self, timeout: float = 5.0) -> dict[str, Any]:
        """Queue a manual reset of service restart backoff for the owner."""
        try:
            command_id, _created = self._ensure_control().enqueue(
                "reset_service_retry", "manual",
            )
        except (OSError, sqlite3.Error):
            logger.warning("free-stockdb 重试退避重置入队失败", exc_info=True)
            return {"status": "degraded", "message": "重试退避重置失败；详细信息已写入本机日志"}
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            command = self._ensure_control().command(command_id)
            if command and command.get("status") == "completed":
                return dict(command.get("result") or {})
            time.sleep(0.05)
        return {
            "status": "queued",
            "message": "已请求重置 free-stockdb 重试退避，监督进程正在应用",
            **self.status(),
        }

    def request_update(self, trigger: str = "manual") -> bool:
        """Queue an owner-executed update; duplicate requests are coalesced."""
        if not self._owner and not self._supervised:
            with self._lock:
                if self._update_thread and self._update_thread.is_alive():
                    return False
                self._stop.clear()
                self._set_status(
                    "queued", "即将启动原生数据更新器", trigger=trigger,
                    phase="queued", update_result="queued",
                )
                worker = threading.Thread(
                    target=self.update_now, args=(trigger,),
                    name="free-stockdb-update-sidecar", daemon=True,
                )
                self._update_thread = worker
            worker.start()
            return True
        try:
            _command_id, created = self._ensure_control().enqueue("update", trigger)
        except (OSError, sqlite3.Error):
            logger.warning("free-stockdb 更新请求入队失败", exc_info=True)
            return False
        if created and self._owner:
            self._set_status(
                "queued", "即将启动原生数据更新器", trigger=trigger,
                phase="queued", update_result="queued", target_session="",
                actual_session="", validated_session=self._last_update_date(), attempt=0,
                max_attempts=1 if trigger == "manual" else _AUTO_MAX_ATTEMPTS,
                next_retry_at="", validation={}, managed=self._is_managed(),
            )
        return created

    def request_apply_config(self, changed_fields: list[str], timeout: float = 5.0) -> dict[str, Any]:
        try:
            command_id, _created = self._ensure_control().enqueue(
                "apply_config", "settings", {"changed_fields": changed_fields},
            )
        except (OSError, sqlite3.Error):
            logger.warning("free-stockdb 控制命令入队失败", exc_info=True)
            return {"status": "degraded", "message": "控制命令入队失败；详细信息已写入本机日志"}
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            command = self._ensure_control().command(command_id)
            if command and command.get("status") == "completed":
                return dict(command.get("result") or {})
            time.sleep(0.05)
        return {"status": "queued", "message": "设置已保存，监督进程正在应用"}

    @staticmethod
    def _scheduled_at(now: datetime) -> datetime:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("free-stockdb 调度时钟必须包含时区")
        now = now.astimezone(FREE_STOCKDB_MARKET_TIMEZONE)
        hour, minute = map(int, get_config().data.free_stockdb_update_time.split(":"))
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        return target if target > now else target + timedelta(days=1)

    @staticmethod
    def _scheduler_now() -> datetime:
        return datetime.now(FREE_STOCKDB_MARKET_TIMEZONE)

    def _apply_config_command(self, changed_fields: list[str]) -> dict[str, Any]:
        from quantmaster.config import load_config, set_config

        previous = get_config()
        previous_service = (
            previous.data.free_stockdb_url,
            previous.data.free_stockdb_root,
            previous.data.free_stockdb_managed,
        )
        set_config(load_config())
        current = get_config()
        current_service = (
            current.data.free_stockdb_url,
            current.data.free_stockdb_root,
            current.data.free_stockdb_managed,
        )
        service_fields = {
            "data.free_stockdb_url", "data.free_stockdb_root", "data.free_stockdb_managed",
        }
        if previous_service != current_service or any(
            field in service_fields for field in changed_fields
        ):
            self._stop_service()
            if current.data.free_stockdb_managed:
                active = self._start_service()
                return {"status": "applied" if active else "degraded", **self.status()}
            self._set_status("disabled", "托管已关闭", managed=False)
            return {"status": "disabled", **self.status()}
        return {"status": "applied", **self.status()}

    def _process_command(self) -> bool:
        command = self._ensure_control().claim_command()
        if command is None:
            return False
        try:
            if command["action"] == "update":
                success = self.update_now(str(command.get("trigger") or "manual"))
                result = {"status": "completed", "success": success, **self.status()}
            elif command["action"] == "apply_config":
                result = self._apply_config_command(
                    list(command.get("payload", {}).get("changed_fields") or []),
                )
            elif command["action"] == "reset_service_retry":
                result = self._reset_service_retry()
            else:
                result = {"status": "failed", "message": "未知控制命令"}
        except Exception as exc:
            logger.exception("free-stockdb 控制命令执行失败")
            result = {"status": "failed", "message": str(exc)[:500]}
        self._ensure_control().complete_command(str(command["id"]), result)
        return True

    def _supervise_service(self, cfg: Any) -> None:
        if (
            not cfg.free_stockdb_managed
            or self._update_lock.locked()
            or time.monotonic() - self._last_service_check < _SERVICE_CHECK_SECONDS
        ):
            return
        now = time.monotonic()
        self._last_service_check = now
        if self._listening():
            was_retrying = self._restart_failures > 0
            self._clear_service_restart_failure()
            if was_retrying:
                self._set_status(
                    "running", "free-stockdb 服务已恢复",
                    managed=self._is_managed(), **self._service_restart_status(),
                )
            return
        # Exponential backoff: keep increasing the delay for a persistent
        # crash loop, but cap it so a fixed-up installation can recover soon.
        if self._restart_failures:
            backoff = self._service_restart_backoff_seconds()
            if now - self._last_restart_fail < backoff:
                return
            self._set_status(
                "degraded",
                "free-stockdb 服务持续崩溃，冷却期后重试",
                managed=True, **self._service_restart_status(),
            )
        logger.warning("free-stockdb 服务失联，监督器尝试重新启动")
        if self._start_service():
            if self._restart_failures:
                self._clear_service_restart_failure()
                self._set_status(
                    "running", "free-stockdb 服务已恢复",
                    managed=self._is_managed(), **self._service_restart_status(),
                )
            return
        self._restart_failures += 1
        self._last_restart_fail = time.monotonic()
        backoff = self._service_restart_backoff_seconds()
        self._set_status(
            "degraded",
            f"free-stockdb 服务启动失败，将在 {backoff} 秒后重试",
            managed=True, **self._service_restart_status(),
        )

    def _scheduler(self) -> None:
        while not self._stop.is_set():
            try:
                if self._process_command():
                    continue
                now = self._scheduler_now()
                if self._next_retry_at and time.time() >= self._next_retry_at:
                    target, attempt = self._retry_target, self._retry_attempt
                    self._next_retry_at = 0.0
                    self.update_now("retry", target_session=target, attempt=attempt)
                    continue
                cfg = get_config().data
                self._supervise_service(cfg)
                scheduled_today = now.strftime("%H:%M") >= cfg.free_stockdb_update_time
                due_for_check = time.time() - self._last_target_check >= _TARGET_CHECK_SECONDS
                if (
                    cfg.free_stockdb_auto_update and scheduled_today and due_for_check
                    and not self._update_lock.locked() and not self._next_retry_at
                ):
                    force_notice = time.time() - self._last_vendor_force >= _AUTO_RETRY_SECONDS
                    self._last_target_check = time.time()
                    if force_notice:
                        self._last_vendor_force = time.time()
                    target, _source = self._target_session(force_notice=force_notice)
                    if (
                        target
                        and target > self._last_update_date()
                        and target != self._deferred_target
                    ):
                        self.update_now("schedule", target_session=target, attempt=1)
                        continue
                with self._lock:
                    heartbeat = dict(self._status)
                heartbeat.update({"owner_pid": os.getpid(), "supervised": False})
                self._write_owner_state(heartbeat)
            except (OSError, sqlite3.Error):
                logger.warning("free-stockdb owner 心跳写入失败", exc_info=True)
            except Exception:
                logger.exception("free-stockdb 后台调度失败，监督线程将在 5 秒后继续")
                self._last_target_check = time.time()
                self._set_status(
                    "degraded", "free-stockdb 后台调度异常；将在 5 秒后重试",
                    update_result="failed", managed=self._is_managed(),
                )
                self._stop.wait(5.0)
                continue
            self._stop.wait(1.0)

    def start(self) -> bool:
        self._owner = True
        self._supervised = False
        os.environ[_CONTROL_PATH_ENV] = str(self._control_path())
        control = self._ensure_control()
        shared = control.read_state()
        self._stop.clear()
        if get_config().data.free_stockdb_managed:
            ready = self._start_service()
        else:
            ready = False
            self._set_status("disabled", "托管已关闭", managed=False)
        if shared.get("update_result") == "retry_wait" and shared.get("next_retry_at"):
            try:
                retry_at = datetime.fromisoformat(str(shared["next_retry_at"])).timestamp()
            except ValueError:
                retry_at = 0.0
            if retry_at:
                self._next_retry_at = max(time.time(), retry_at)
                self._retry_target = str(shared.get("target_session") or "")
                self._retry_attempt = int(shared.get("attempt") or 0) + 1
                self._set_status(
                    "running" if ready else "disabled",
                    str(shared.get("message") or "等待下一次自动更新重试"),
                    managed=bool(ready),
                    update_result="retry_wait",
                    target_session=self._retry_target,
                    actual_session=shared.get("actual_session"),
                    validated_session=shared.get("validated_session"),
                    attempt=int(shared.get("attempt") or 0),
                    max_attempts=_AUTO_MAX_ATTEMPTS,
                    next_retry_at=str(shared["next_retry_at"]),
                    validation=shared.get("validation") or {},
                )
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(
                target=self._scheduler, name="free-stockdb-runtime", daemon=True,
            )
            self._thread.start()
        return ready

    def attach_to_supervisor(self) -> bool:
        """让热重载 worker 观察 sidecar，但不取得启停所有权。"""
        self._owner = False
        self._supervised = True
        self._stop.clear()
        shared = self._ensure_control().read_state()
        if shared:
            with self._lock:
                self._status = shared
            return str(shared.get("state")) not in {"error", "degraded", "stopped"}
        if self._listening():
            with self._lock:
                self._status = {
                    "state": "running", "message": "本地服务由热更新启动器托管",
                    "managed": True, "supervised": True,
                }
            return True
        with self._lock:
            self._status = {
                "state": "degraded",
                "message": "热更新启动器尚未发布 free-stockdb 状态",
                "supervised": True,
            }
        return False

    def claim_update_event(self) -> StockDBUpdateEvent | None:
        event = self._ensure_control().claim_event()
        if event is None:
            return None
        return StockDBUpdateEvent(
            event_key=str(event["event_key"]),
            kind=str(event.get("kind") or ""),
            payload=dict(event.get("payload") or {}),
        )

    def complete_update_event(self, event_key: str) -> None:
        self._ensure_control().complete_event(str(event_key))

    def stop(self) -> None:
        if not self._owner:
            self._stop.set()
            update_thread = self._update_thread
            if update_thread and update_thread is not threading.current_thread():
                update_thread.join(timeout=7)
            self._update_thread = None
            with self._lock:
                self._status = {"state": "stopped", "message": "worker 已停止观察 sidecar"}
            return
        self._stop.set()
        with self._lock:
            updater = self._updater_process
        if updater is not None:
            self._terminate_process(updater, timeout=5)
        thread = self._thread
        if thread and thread is not threading.current_thread():
            thread.join(timeout=2)
        self._thread = None
        update_thread = self._update_thread
        if update_thread and update_thread is not threading.current_thread():
            update_thread.join(timeout=7)
        self._update_thread = None
        self._stop_service()
        self._set_status("stopped", "QuantMaster 已停止托管服务")
        self._owner = False

    def restart(self) -> bool:
        if not self._owner:
            result = self.request_apply_config([
                "data.free_stockdb_url", "data.free_stockdb_root", "data.free_stockdb_managed",
            ])
            return result.get("status") == "applied"
        self.stop()
        return self.start()

    def status(self) -> dict[str, Any]:
        if self._supervised and not self._owner:
            try:
                status = self._ensure_control().read_state()
            except (OSError, sqlite3.Error):
                status = {}
            heartbeat = float(status.get("owner_heartbeat_at") or 0)
            if not status or time.time() - heartbeat > _OWNER_STALE_SECONDS:
                status = {
                    **status, "state": "degraded", "update_result": "failed",
                    "message": "free-stockdb 监督进程状态已失联", "supervised": True,
                }
        else:
            with self._lock:
                status = dict(self._status)
        root, _, updater = self._paths()
        marker = self._read_marker()
        try:
            next_update = self._scheduled_at(self._scheduler_now()).isoformat()
        except (ValueError, TypeError):
            next_update = ""
        from quantmaster.data.free_stockdb_source import resolve_free_stockdb_sdk_path

        sdk_path = resolve_free_stockdb_sdk_path()
        status.update({
            "managed": bool(status.get("managed", self._is_managed())),
            "service_url": get_config().data.free_stockdb_url,
            "sdk_engine": "stock_sdk" if sdk_path and sdk_path.is_file() else "http-compatible",
            "sdk_path": str(sdk_path or ""),
            "update_capability": "native_only" if updater.is_file() else "unavailable",
            "updater_path": str(updater),
            "root": str(root),
            "last_update_at": str(marker.get("updated_at") or marker.get("date") or ""),
            "market_timezone": str(FREE_STOCKDB_MARKET_TIMEZONE),
            "validated_session": str(
                status.get("validated_session") or marker.get("validated_session") or ""
            ),
            "target_session": str(status.get("target_session") or ""),
            "actual_session": str(status.get("actual_session") or ""),
            "attempt": int(status.get("attempt") or 0),
            "max_attempts": int(status.get("max_attempts") or _AUTO_MAX_ATTEMPTS),
            "next_retry_at": str(status.get("next_retry_at") or ""),
            "validation": dict(status.get("validation") or {}),
            "next_update_at": next_update,
            "vendor_notice": self._read_vendor_cache(),
            "supervised": self._supervised or bool(status.get("supervised")),
        })
        return status


free_stockdb_runtime = FreeStockDBRuntime()
