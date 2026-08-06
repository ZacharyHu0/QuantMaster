"""Manage the user-supplied free-stockdb process and its incremental updater."""

from __future__ import annotations

import json
import logging
import socket
import subprocess
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from quantmaster.config import get_config

logger = logging.getLogger(__name__)


class FreeStockDBRuntime:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._process: subprocess.Popen[bytes] | None = None
        self._updater_process: subprocess.Popen[bytes] | None = None
        self._thread: threading.Thread | None = None
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
            self._status = {
                "state": state, "message": message,
                "updated_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
                **extra,
            }

    def _start_service(self) -> bool:
        root, executable, _ = self._paths()
        endpoint = self._endpoint()
        if endpoint is None:
            self._set_status("disabled", "服务地址不是本机回环地址，不执行进程托管")
            return False
        if self._stop.is_set():
            return False
        if self._listening():
            self._set_status("running", "本地服务已运行", managed=bool(self._process))
            return True
        if not executable.is_file() or not (root / "stockdb.conf").is_file() or not (root / "data").is_dir():
            self._set_status("missing", f"等待完整发行包：{root}", managed=False)
            logger.warning("free-stockdb 未就绪，等待完整发行包：%s", root)
            return False
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        self._process = subprocess.Popen(
            [str(executable)], cwd=root, stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=creationflags,
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not self._stop.is_set():
            if self._listening():
                self._set_status("running", "本地服务由 QuantMaster 托管", managed=True)
                logger.info("free-stockdb 已启动 · %s:%s", *endpoint)
                return True
            if self._process.poll() is not None:
                break
            self._stop.wait(0.25)
        code = self._process.poll()
        self._process = None
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
        if process is None:
            return not self._listening()
        self._terminate_process(process)
        self._process = None
        return not self._listening()

    def _run_updater(self, updater: Path, root: Path) -> int:
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        process = subprocess.Popen(
            [str(updater)], cwd=root, stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=creationflags,
        )
        with self._lock:
            self._updater_process = process
        deadline = time.monotonic() + 6 * 60 * 60
        try:
            while process.poll() is None:
                if self._stop.wait(0.5):
                    self._terminate_process(process, timeout=5)
                    return -1
                if time.monotonic() >= deadline:
                    self._terminate_process(process, timeout=5)
                    raise subprocess.TimeoutExpired(str(updater), 6 * 60 * 60)
            return int(process.returncode or 0)
        finally:
            with self._lock:
                if self._updater_process is process:
                    self._updater_process = None

    def _marker_path(self) -> Path:
        return self._root() / ".quantmaster-update.json"

    def _last_update_date(self) -> str:
        try:
            value = json.loads(self._marker_path().read_text(encoding="utf-8"))
            return str(value.get("date") or "")
        except (OSError, ValueError, TypeError):
            return ""

    def _record_update(self, code: int) -> None:
        payload = {
            "date": datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat(),
            "exit_code": code,
        }
        self._marker_path().write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    def update_now(self) -> bool:
        root, _, updater = self._paths()
        if not updater.is_file():
            self._set_status("missing", f"未找到更新器：{updater}")
            return False
        with self._lock:
            if self._status.get("state") == "updating":
                return False
            self._set_status("updating", "正在增量更新本地数据")
        if self._process is None and self._listening():
            self._set_status("degraded", "7899 由外部进程占用，无法安全执行自动更新")
            logger.warning("free-stockdb 由外部进程运行，跳过自动更新以避免终止非托管进程")
            return False
        self._stop_service()
        code = -1
        try:
            code = self._run_updater(updater, root)
            if code == 0:
                self._record_update(code)
                logger.info("free-stockdb 增量更新完成")
            else:
                logger.error("free-stockdb 更新器退出码 %s", code)
        except (OSError, subprocess.TimeoutExpired) as exc:
            logger.error("free-stockdb 自动更新失败：%s", exc)
        finally:
            if not self._stop.is_set():
                self._start_service()
            try:
                from quantmaster.data.resilience import PROVIDER_HEALTH
                PROVIDER_HEALTH.reset("free-stockdb")
            except (ImportError, RuntimeError):
                logger.warning("free-stockdb 重启后重置数据源熔断失败", exc_info=True)
        return code == 0

    @staticmethod
    def _scheduled_at(now: datetime) -> datetime:
        hour, minute = map(int, get_config().data.free_stockdb_update_time.split(":"))
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        return target if target > now else target + timedelta(days=1)

    def _scheduler(self) -> None:
        timezone = ZoneInfo(get_config().automation.timezone)
        while not self._stop.is_set():
            cfg = get_config().data
            now = datetime.now(timezone)
            scheduled_today = now.strftime("%H:%M") >= cfg.free_stockdb_update_time
            if (cfg.free_stockdb_auto_update and scheduled_today
                    and self._last_update_date() != now.date().isoformat()):
                if not self.update_now():
                    self._stop.wait(300)
                continue
            target = self._scheduled_at(now)
            wait = max(1.0, min(300.0, (target - now).total_seconds()))
            self._stop.wait(wait)

    def start(self) -> bool:
        if not get_config().data.free_stockdb_managed:
            self._set_status("disabled", "托管已关闭")
            return False
        self._stop.clear()
        ready = self._start_service()
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(
                target=self._scheduler, name="free-stockdb-runtime", daemon=True,
            )
            self._thread.start()
        return ready

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            updater = self._updater_process
        if updater is not None:
            self._terminate_process(updater, timeout=5)
        thread = self._thread
        if thread and thread is not threading.current_thread():
            thread.join(timeout=2)
        self._thread = None
        self._stop_service()
        self._set_status("stopped", "QuantMaster 已停止托管服务")

    def restart(self) -> bool:
        self.stop()
        return self.start()

    def status(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._status)


free_stockdb_runtime = FreeStockDBRuntime()
