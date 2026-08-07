"""Manage the user-supplied free-stockdb process and its incremental updater."""

from __future__ import annotations

import html
import json
import logging
import re
import socket
import subprocess
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import httpx

from quantmaster.config import get_config

logger = logging.getLogger(__name__)

_VENDOR_HOME = "https://a.123128.xyz/"
_VENDOR_NOTICE_TTL = 6 * 60 * 60


class FreeStockDBRuntime:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._update_lock = threading.Lock()
        self._vendor_lock = threading.Lock()
        self._stop = threading.Event()
        self._process: subprocess.Popen[bytes] | None = None
        self._updater_process: subprocess.Popen[bytes] | None = None
        self._thread: threading.Thread | None = None
        self._update_thread: threading.Thread | None = None
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
        config_path = root / "stockdb.conf"
        endpoint = self._endpoint()
        if endpoint is None:
            self._set_status("disabled", "服务地址不是本机回环地址，不执行进程托管")
            return False
        if self._stop.is_set():
            return False
        if self._listening():
            self._set_status("running", "本地服务已运行", managed=bool(self._process))
            return True
        if not executable.is_file() or not config_path.is_file() or not (root / "data").is_dir():
            self._set_status("missing", f"等待完整发行包：{root}", managed=False)
            logger.warning("free-stockdb 未就绪，等待完整发行包：%s", root)
            return False
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        self._process = subprocess.Popen(
            # 无参数模式会启动托盘程序并打开 free-stockdb 官网。显式传入
            # 配置文件会进入可托管的前台服务器模式，不创建网页或托盘窗口。
            [str(executable), str(config_path)], cwd=root, stdin=subprocess.DEVNULL,
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

    def _run_updater(self, updater: Path, root: Path, *, trigger: str) -> int:
        # 当前发行包没有公开、可验证的静默参数。手动触发时保留原生窗口，
        # 避免隐藏的模态完成框令 sidecar 永久等待。
        process = subprocess.Popen(
            [str(updater)], cwd=root, stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        with self._lock:
            self._updater_process = process
        deadline = time.monotonic() + 6 * 60 * 60
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
                    raise subprocess.TimeoutExpired(str(updater), 6 * 60 * 60)
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

    def _last_update_date(self) -> str:
        try:
            value = json.loads(self._marker_path().read_text(encoding="utf-8"))
            return str(value.get("date") or "")
        except (OSError, ValueError, TypeError):
            return ""

    def _record_update(self, code: int) -> None:
        now = datetime.now(ZoneInfo("Asia/Shanghai"))
        payload = {
            "date": now.date().isoformat(),
            "updated_at": now.isoformat(),
            "exit_code": code,
        }
        self._marker_path().write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    def update_now(self, trigger: str = "manual") -> bool:
        if not self._update_lock.acquire(blocking=False):
            return False
        try:
            return self._update_now_locked(trigger=trigger)
        finally:
            self._update_lock.release()

    def _update_now_locked(self, *, trigger: str) -> bool:
        root, _, updater = self._paths()
        if self._stop.is_set():
            return False
        if not updater.is_file():
            self._set_status("missing", f"未找到更新器：{updater}")
            return False
        with self._lock:
            if self._status.get("state") == "updating":
                return False
            self._set_status(
                "updating", "正在停止本地数据库", phase="stopping",
                trigger=trigger, update_result="running",
            )
        if self._process is None and self._listening():
            self._set_status("degraded", "7899 由外部进程占用，无法安全执行自动更新")
            logger.warning("free-stockdb 由外部进程运行，跳过自动更新以避免终止非托管进程")
            return False
        if not self._stop_service():
            self._set_status(
                "degraded", "本地数据库未能安全停止，已取消数据更新",
                phase="stopping", trigger=trigger, update_result="failed",
            )
            return False
        if self._stop.is_set():
            return False
        code = -1
        try:
            code = self._run_updater(updater, root, trigger=trigger)
            if code == 0:
                self._record_update(code)
                logger.info("free-stockdb 增量更新完成")
            else:
                logger.error("free-stockdb 更新器退出码 %s", code)
        except (OSError, subprocess.TimeoutExpired) as exc:
            logger.error("free-stockdb 自动更新失败：%s", exc)
        finally:
            restored = False
            if not self._stop.is_set():
                self._set_status(
                    "updating", "数据同步结束，正在恢复本地服务", phase="restarting",
                    trigger=trigger, update_result="running",
                )
                restored = self._start_service()
            try:
                from quantmaster.data.resilience import PROVIDER_HEALTH
                PROVIDER_HEALTH.reset("free-stockdb")
            except (ImportError, RuntimeError):
                logger.warning("free-stockdb 重启后重置数据源熔断失败", exc_info=True)
            if restored:
                self._set_status(
                    "running",
                    "更新完成，本地服务已恢复" if code == 0 else "更新失败，本地服务已恢复",
                    phase="completed", update_result="success" if code == 0 else "failed",
                    exit_code=code, trigger=trigger,
                )
                if (
                    code == 0
                    and get_config().data.after_close_enabled
                    and get_config().data.after_close_auto_run
                ):
                    try:
                        from quantmaster.after_close.jobs import get_after_close_jobs

                        get_after_close_jobs().submit(force=False)
                        logger.info("free-stockdb 更新完成，已提交盘后研究扫描")
                    except (ImportError, RuntimeError, ValueError):
                        logger.warning("free-stockdb 更新完成，但盘后研究扫描未能提交", exc_info=True)
            elif not self._stop.is_set():
                self._set_status(
                    "error", "更新结束，但本地服务恢复失败", phase="completed",
                    update_result="failed", exit_code=code, trigger=trigger,
                )
        return code == 0

    def request_update(self, trigger: str = "manual") -> bool:
        """Queue one non-blocking sidecar update; duplicate requests are coalesced."""
        with self._lock:
            if self._status.get("state") in {"queued", "updating"} or self._update_lock.locked():
                return False
            if self._stop.is_set():
                return False
            self._set_status(
                "queued", "即将启动原生数据更新器", trigger=trigger,
                phase="queued", update_result="queued",
            )
            worker = threading.Thread(
                target=self.update_now,
                args=(trigger,),
                name="free-stockdb-update-sidecar",
                daemon=True,
            )
            self._update_thread = worker
        worker.start()
        return True

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
                if self._listening():
                    self._set_status(
                        "running",
                        "今日数据需要更新；当前更新器仅支持原生窗口，请在设置页手动触发",
                        managed=bool(self._process), phase="native_required",
                        update_result="native_required", trigger="schedule",
                    )
                target = self._scheduled_at(now)
                self._stop.wait(max(1.0, min(300.0, (target - now).total_seconds())))
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

    def attach_to_supervisor(self) -> bool:
        """让热重载 worker 观察 sidecar，但不取得启停所有权。"""
        if self._listening():
            self._set_status(
                "running",
                "本地服务由热更新启动器托管",
                managed=True,
                supervised=True,
            )
            return True
        self._set_status(
            "degraded",
            "热更新启动器尚未提供本地数据库服务",
            managed=True,
            supervised=True,
        )
        return False

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
        update_thread = self._update_thread
        if update_thread and update_thread is not threading.current_thread():
            update_thread.join(timeout=7)
        self._update_thread = None
        self._stop_service()
        self._set_status("stopped", "QuantMaster 已停止托管服务")

    def restart(self) -> bool:
        self.stop()
        return self.start()

    def status(self) -> dict[str, Any]:
        with self._lock:
            status = dict(self._status)
        root, _, updater = self._paths()
        try:
            marker = json.loads(self._marker_path().read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            marker = {}
        try:
            timezone = ZoneInfo(get_config().automation.timezone)
            next_update = self._scheduled_at(datetime.now(timezone)).isoformat()
        except (ValueError, TypeError):
            next_update = ""
        from quantmaster.data.free_stockdb_source import resolve_free_stockdb_sdk_path

        sdk_path = resolve_free_stockdb_sdk_path()
        status.update({
            "managed": bool(self._process),
            "service_url": get_config().data.free_stockdb_url,
            "sdk_engine": "stock_sdk" if sdk_path and sdk_path.is_file() else "http-compatible",
            "sdk_path": str(sdk_path or ""),
            "update_capability": "native_only" if updater.is_file() else "unavailable",
            "updater_path": str(updater),
            "root": str(root),
            "last_update_at": str(marker.get("updated_at") or marker.get("date") or ""),
            "next_update_at": next_update,
            "vendor_notice": self._read_vendor_cache(),
        })
        return status


free_stockdb_runtime = FreeStockDBRuntime()
