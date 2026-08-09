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
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import httpx

from quantmaster.config import get_config
from quantmaster.runtime.sqlite import connect_sqlite
from quantmaster.trading_sessions import market_date

logger = logging.getLogger(__name__)

_VENDOR_HOME = "https://a.123128.xyz/"
_VENDOR_NOTICE_TTL = 6 * 60 * 60
_CONTROL_PATH_ENV = "QM_FREE_STOCKDB_CONTROL_PATH"
_AUTO_MAX_ATTEMPTS = 3
_AUTO_RETRY_SECONDS = 15 * 60
_UPDATER_TIMEOUT_SECONDS = 30 * 60
_TARGET_CHECK_SECONDS = 5 * 60
_OWNER_STALE_SECONDS = 120
_VENDOR_STARTUP_URL = b"http://a.123128.xyz/\x00"


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
        self._event_thread: threading.Thread | None = None
        self._event_stop = threading.Event()
        self._owner = False
        self._supervised = False
        self._control: _RuntimeControl | None = None
        self._last_target_check = 0.0
        self._last_vendor_force = 0.0
        self._next_retry_at = 0.0
        self._retry_target = ""
        self._retry_attempt = 0
        self._status: dict[str, Any] = {"state": "stopped", "message": "尚未启动"}

    @staticmethod
    def _root() -> Path:
        value = Path(get_config().data.free_stockdb_root).expanduser()
        return value.resolve() if value.is_absolute() else (Path.cwd() / value).resolve()

    @classmethod
    def _paths(cls) -> tuple[Path, Path, Path]:
        root = cls._root()
        return root, root / "stockdb.exe", root / "数据更新.exe"

    @staticmethod
    def _patch_vendor_browser_launch(payload: bytes) -> bytes:
        """Disable only the vendor's unconditional startup-page launch.

        Current free-stockdb Windows builds contain one separate, hard-coded HTTP
        homepage passed to ``ShellExecuteA`` during startup.  The HTTPS version
        endpoint and the rest of the executable remain untouched, so QuantMaster
        can still surface upstream update notices.
        """
        occurrences = payload.count(_VENDOR_STARTUP_URL)
        if occurrences != 1:
            raise RuntimeError(
                "无法可靠识别 free-stockdb 启动页入口"
                f"（期望 1 处，实际 {occurrences} 处）"
            )
        replacement = b"\x00" + (b" " * (len(_VENDOR_STARTUP_URL) - 1))
        return payload.replace(_VENDOR_STARTUP_URL, replacement, 1)

    @classmethod
    def _headless_executable(cls, executable: Path) -> Path:
        """Return a content-addressed managed copy that cannot open the homepage."""
        payload = executable.read_bytes()
        patched = cls._patch_vendor_browser_launch(payload)
        digest = hashlib.sha256(payload).hexdigest()[:16]
        target = executable.parent / f".quantmaster-stockdb-headless-{digest}.exe"
        try:
            if target.read_bytes() == patched:
                return target
        except OSError:
            pass
        temporary = target.with_name(
            f"{target.name}.tmp-{os.getpid()}-{threading.get_ident()}"
        )
        try:
            temporary.write_bytes(patched)
            temporary.replace(target)
        finally:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
        return target

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
                self._ensure_control().write_state(payload)
            except (OSError, sqlite3.Error):
                logger.warning("free-stockdb 控制状态写入失败", exc_info=True)

    def _is_managed(self) -> bool:
        return self._daemon_started or self._process is not None

    def _start_service(self) -> bool:
        root, executable, _ = self._paths()
        config_path = root / "stockdb.conf"
        endpoint = self._endpoint()
        if endpoint is None:
            self._set_status("disabled", "服务地址不是本机回环地址，不执行进程托管")
            return False
        if self._stop.is_set():
            return False
        managed_executable: Path | None = None
        patch_error = ""
        if executable.is_file():
            try:
                managed_executable = self._headless_executable(executable)
            except (OSError, RuntimeError) as exc:
                patch_error = str(exc)
                logger.error("free-stockdb 无弹窗托管副本创建失败：%s", exc)
        if self._listening():
            if self._process is None and managed_executable is not None and self._recover_managed_orphan(
                executable, managed_executable,
            ):
                logger.info("free-stockdb 孤儿进程已回收，准备重新托管")
            else:
                self._set_status("running", "本地服务已运行", managed=self._is_managed())
                return True
        if not executable.is_file() or not config_path.is_file() or not (root / "data").is_dir():
            self._set_status("missing", f"等待完整发行包：{root}", managed=False)
            logger.warning("free-stockdb 未就绪，等待完整发行包：%s", root)
            return False
        if managed_executable is None:
            self._set_status(
                "error", f"无法创建 free-stockdb 无弹窗托管副本：{patch_error}", managed=False,
            )
            return False
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        self._process = subprocess.Popen(
            # daemon 模式仍会无条件 ShellExecute 供应商主页，因此使用从原始发行包
            # 派生的内容寻址副本；副本只清空该启动页常量，不修改版本检测能力。
            [str(managed_executable), "-d", str(config_path), "-s", "start"],
            cwd=root, stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=creationflags,
        )
        launcher = self._process
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not self._stop.is_set():
            if self._listening():
                self._daemon_started = True
                self._record_process_owner(managed_executable)
                self._set_status("running", "本地服务由 QuantMaster 托管", managed=True)
                logger.info("free-stockdb 已启动 · %s:%s", *endpoint)
                return True
            # daemon 启动器可能在后台服务开始监听前正常退出，不能据此提前失败。
            self._stop.wait(0.25)
        code = launcher.poll()
        if code is None:
            self._terminate_process(launcher, timeout=3)
        self._process = None
        self._daemon_started = False
        self._clear_process_owner()
        self._set_status("error", "本地服务未能在 10 秒内就绪", exit_code=code)
        logger.warning("free-stockdb 启动后未能在 10 秒内就绪")
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

    def _stop_service(self) -> bool:
        process = self._process
        if not self._daemon_started and process is None:
            return not self._listening()
        root, executable, _ = self._paths()
        config_path = root / "stockdb.conf"
        managed_executable: Path | None = None
        if executable.is_file():
            try:
                managed_executable = self._headless_executable(executable)
            except (OSError, RuntimeError):
                logger.error("free-stockdb 无弹窗停止程序创建失败", exc_info=True)
        if self._daemon_started and managed_executable is not None and config_path.is_file():
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
            try:
                subprocess.run(
                    [str(managed_executable), "-d", str(config_path), "-s", "stop"],
                    cwd=root, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL, creationflags=creationflags,
                    check=False, timeout=10,
                )
            except (OSError, subprocess.TimeoutExpired):
                logger.warning("free-stockdb 后台服务停止命令失败", exc_info=True)
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

    def _run_updater(self, updater: Path, root: Path, *, trigger: str) -> int:
        # 当前发行包没有公开、可验证的静默参数。手动触发时保留原生窗口，
        # 避免隐藏的模态完成框令 sidecar 永久等待。
        process = subprocess.Popen(
            [str(updater)], cwd=root, stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        with self._lock:
            self._updater_process = process
        deadline = time.monotonic() + _UPDATER_TIMEOUT_SECONDS
        started = time.monotonic()
        try:
            while process.poll() is None:
                self._set_status(
                    "updating", "正在同步 free-stockdb 本地数据",
                    phase="syncing", elapsed_seconds=int(time.monotonic() - started),
                    trigger=trigger, update_result="running",
                )
                if self._stop.wait(0.5):
                    self._terminate_process(process, timeout=5)
                    return -1
                if time.monotonic() >= deadline:
                    self._terminate_process(process, timeout=5)
                    raise subprocess.TimeoutExpired(str(updater), _UPDATER_TIMEOUT_SECONDS)
            return int(process.returncode or 0)
        finally:
            with self._lock:
                if self._updater_process is process:
                    self._updater_process = None

    def _marker_path(self) -> Path:
        return self._root() / ".quantmaster-update.json"

    @staticmethod
    def _vendor_cache_path() -> Path:
        root = Path(get_config().data.root).expanduser()
        root = root.resolve() if root.is_absolute() else (Path.cwd() / root).resolve()
        return root / "free_stockdb_vendor_notice.json"

    @staticmethod
    def _parse_vendor_notice(document: str) -> dict[str, str]:
        data_match = re.search(r"数据更新至\s*[:：]\s*(\d{4}-\d{2}-\d{2})", document)
        version_match = re.search(
            r"最新版本\s*v?([^<（(,，]+)", document, flags=re.IGNORECASE,
        )
        announcement_match = re.search(
            r"<span\b[^>]*>(.*?)</span>", document,
            flags=re.IGNORECASE | re.DOTALL,
        )
        version = html.unescape(version_match.group(1)).strip() if version_match else ""
        version = re.sub(r"\s+", " ", version).rstrip("。；; ")
        announcement = announcement_match.group(1) if announcement_match else ""
        announcement = re.sub(r"<[^>]+>", " ", announcement)
        announcement = re.sub(r"\s+", " ", html.unescape(announcement)).strip()
        return {
            "data_date": data_match.group(1) if data_match else "",
            "version": version,
            "announcement": announcement,
        }

    def _read_vendor_cache(self) -> dict[str, Any]:
        try:
            value = json.loads(self._vendor_cache_path().read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
        except (OSError, ValueError, TypeError):
            return {}

    def check_vendor_notice(self, *, force: bool = False) -> dict[str, Any]:
        """Read the vendor announcement without ever opening the vendor website."""
        cached = self._read_vendor_cache()
        checked_at = str(cached.get("checked_at") or "")
        if checked_at and not force:
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
                response = client.get(_VENDOR_HOME)
                response.raise_for_status()
            details = self._parse_vendor_notice(response.text)
            now = datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()
            fingerprint = (
                f"{details['data_date']}|{details['version']}|{details['announcement']}"
            )
            notice: dict[str, Any] = {
                "status": "ok", "checked_at": now, "url": _VENDOR_HOME,
                "data_date": details["data_date"], "version": details["version"],
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

    @staticmethod
    def _valid_date(value: object) -> str:
        try:
            parsed = date.fromisoformat(str(value or "")[:10])
        except ValueError:
            return ""
        return parsed.isoformat() if parsed <= market_date() else ""

    def _target_session(self, *, force_notice: bool = False) -> tuple[str, str]:
        notice = self.check_vendor_notice(force=force_notice)
        vendor_date = self._valid_date(notice.get("data_date"))
        if vendor_date:
            return vendor_date, "free-stockdb-vendor"
        try:
            from quantmaster.trading_sessions import expected_session

            expectation = expected_session()
            if expectation.ready and self._valid_date(expectation.session):
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
            "complete": False,
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
        except (httpx.HTTPError, ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
            result["issues"] = [f"读取 free-stockdb 验证截面失败：{str(exc)[:300]}"]
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
        issues = []
        if symbol_ratio < 1.0:
            issues.append(
                f"目标日截面仅覆盖 {observed}/{len(expected_trading)} 只应交易证券；"
                "其余缺失标的没有停牌/退市证据"
            )
        if suspension_error:
            issues.append(suspension_error)
        if required_ratio < 1.0:
            issues.append(f"目标日完整 OHLCV 比例仅 {required_ratio:.1%}")
        result.update({
            "catalog_symbols": len(symbols),
            "expected_trading_symbols": len(expected_trading),
            "observed_symbols": observed,
            "symbol_ratio": round(symbol_ratio, 6),
            "excused_suspended_symbols": sorted(excused_suspensions),
            "suspension_evidence": suspension_evidence,
            "required_ohlcv_ratio": round(required_ratio, 6),
            "invalid_ohlcv": {
                "nonfinite_rows": invalid_finite,
                "price_or_ohlc_rows": invalid_ohlc,
                "negative_volume_rows": invalid_volume,
            },
            "complete": not issues,
            "issues": issues,
        })
        return result

    def _emit_update_event(self, kind: str, target: str, payload: dict[str, Any]) -> None:
        try:
            self._ensure_control().emit(f"{kind}:{target}", kind, payload)
        except (OSError, sqlite3.Error):
            logger.warning("free-stockdb 更新事件写入失败", exc_info=True)

    def _finish_success(
        self, *, target: str, validation: dict[str, Any], code: int,
        attempt: int, trigger: str,
    ) -> bool:
        self._next_retry_at = 0.0
        self._retry_target = ""
        self._retry_attempt = 0
        self._record_update(code, target, validation, attempt)
        self._set_status(
            "running", f"数据已验证至 {target}，本地服务已恢复",
            phase="completed", update_result="success", exit_code=code,
            trigger=trigger, target_session=target,
            actual_session=str(validation.get("actual_session") or ""),
            validated_session=target, attempt=attempt,
            max_attempts=_AUTO_MAX_ATTEMPTS if trigger != "manual" else 1,
            next_retry_at="", validation=validation, managed=self._is_managed(),
        )
        # The SDK and native extension are replaced in place by some vendor
        # releases.  Do not let a long-lived scan service retain the old client.
        try:
            from quantmaster.after_close.service import reset_after_close_service
            from quantmaster.rotation.etf_research import reset_etf_research_service

            reset_after_close_service()
            reset_etf_research_service()
        except (ImportError, RuntimeError):
            logger.warning("free-stockdb 更新后重置盘后数据源失败", exc_info=True)
        self._emit_update_event("update_succeeded", target, {
            "target_session": target, "validation": validation, "trigger": trigger,
        })
        logger.info("free-stockdb 数据已验证至 %s", target)
        return True

    def _finish_failure(
        self, *, target: str, validation: dict[str, Any], code: int,
        attempt: int, trigger: str, message: str, allow_retry: bool = True,
    ) -> bool:
        automatic = trigger in {"schedule", "retry"}
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
            "target_session": target, "actual_session": "", "complete": False, "issues": [],
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
        if preflight.get("complete"):
            return self._finish_success(
                target=target, validation=preflight, code=0, attempt=attempt, trigger=trigger,
            )
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
            code = self._run_updater(updater, root, trigger=trigger)
        except subprocess.TimeoutExpired:
            updater_error = "原生更新器运行超过 30 分钟，已终止"
            logger.error(updater_error)
        except OSError as exc:
            updater_error = f"原生更新器启动失败：{str(exc)[:300]}"
            logger.error("free-stockdb 自动更新失败：%s", exc)
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
        validation = self._validate_data(target)
        if validation.get("complete"):
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
        hour, minute = map(int, get_config().data.free_stockdb_update_time.split(":"))
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        return target if target > now else target + timedelta(days=1)

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
            else:
                result = {"status": "failed", "message": "未知控制命令"}
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            logger.exception("free-stockdb 控制命令执行失败")
            result = {"status": "failed", "message": str(exc)[:500]}
        self._ensure_control().complete_command(str(command["id"]), result)
        return True

    def _scheduler(self) -> None:
        while not self._stop.is_set():
            if self._process_command():
                continue
            timezone = ZoneInfo(get_config().automation.timezone)
            now = datetime.now(timezone)
            if self._next_retry_at and time.time() >= self._next_retry_at:
                target, attempt = self._retry_target, self._retry_attempt
                self._next_retry_at = 0.0
                self.update_now("retry", target_session=target, attempt=attempt)
                continue
            cfg = get_config().data
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
                if target and target > self._last_update_date():
                    self.update_now("schedule", target_session=target, attempt=1)
                    continue
            try:
                with self._lock:
                    heartbeat = dict(self._status)
                heartbeat.update({"owner_pid": os.getpid(), "supervised": False})
                self._ensure_control().write_state(heartbeat)
            except (OSError, sqlite3.Error):
                logger.warning("free-stockdb owner 心跳写入失败", exc_info=True)
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

    def _deliver_event(self, event: dict[str, Any]) -> None:
        kind = str(event.get("kind") or "")
        payload = dict(event.get("payload") or {})
        cfg = get_config()
        if kind == "update_succeeded":
            if cfg.data.after_close_enabled and cfg.data.after_close_auto_run:
                from quantmaster.after_close.jobs import get_after_close_jobs

                get_after_close_jobs().submit(force=False)
                logger.info("free-stockdb 验收完成，已提交盘后研究扫描")
            return
        if kind != "update_failed" or not (
            cfg.data.after_close_notify and cfg.automation.enabled
        ):
            return
        from quantmaster.automation.models import AlertEvent, stable_hash
        from quantmaster.automation.runtime import get_runtime

        target = str(payload.get("target_session") or "未知")
        validation = dict(payload.get("validation") or {})
        actual = str(validation.get("actual_session") or "未知")
        message = str(payload.get("message") or "真实交易日验收未通过")[:500]
        get_runtime().service.process_event(AlertEvent(
            kind="task_failure", score=100, severity="warning",
            data_as_of=datetime.now(UTC).isoformat(),
            evidence=[
                f"目标交易日 {target}；本地实际 {actual}", message,
                f"自动更新已尝试 {payload.get('attempt') or _AUTO_MAX_ATTEMPTS} 次",
            ],
            dedupe_key=stable_hash({"free_stockdb_update_failed": target}),
            payload={"title": "free-stockdb 自动更新未完成", "target_session": target},
        ))

    def _event_bridge(self) -> None:
        while not self._event_stop.wait(1.0):
            try:
                event = self._ensure_control().claim_event()
                if event is None:
                    continue
                self._deliver_event(event)
                self._ensure_control().complete_event(str(event["event_key"]))
            except (ImportError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
                logger.warning("free-stockdb 更新事件消费失败", exc_info=True)
                self._event_stop.wait(2.0)

    def start_event_bridge(self) -> None:
        self._event_stop.clear()
        if self._event_thread is None or not self._event_thread.is_alive():
            self._event_thread = threading.Thread(
                target=self._event_bridge, name="free-stockdb-event-bridge", daemon=True,
            )
            self._event_thread.start()

    def stop_event_bridge(self) -> None:
        self._event_stop.set()
        thread = self._event_thread
        if thread and thread is not threading.current_thread():
            thread.join(timeout=2)
        self._event_thread = None

    def stop(self) -> None:
        self.stop_event_bridge()
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
            timezone = ZoneInfo(get_config().automation.timezone)
            next_update = self._scheduled_at(datetime.now(timezone)).isoformat()
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
