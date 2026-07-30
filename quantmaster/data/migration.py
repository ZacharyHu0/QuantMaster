"""数据根目录后台复制迁移：校验完成前绝不切换配置。"""

from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
import threading
import uuid
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from pathlib import Path

from quantmaster.config import get_config
from quantmaster.runtime.maintenance import MaintenanceLease, maintenance_barrier
from quantmaster.settings import ConfigManager


class MigrationError(ValueError):
    pass


@dataclass
class MigrationTask:
    id: str
    source: str
    target: str
    mode: str
    status: str = "pending"
    progress: int = 0
    phase: str = "等待开始"
    copied_bytes: int = 0
    total_bytes: int = 0
    error: str = ""
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    finished_at: str | None = None
    cancel_event: threading.Event = field(default_factory=threading.Event, repr=False)
    maintenance_lease: MaintenanceLease | None = field(default=None, repr=False)

    def public(self) -> dict:
        return {item.name: getattr(self, item.name) for item in fields(self)
                if item.name not in {"cancel_event", "maintenance_lease"}}


def _resolved(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()


def _is_sqlite_sidecar(path: Path) -> bool:
    for suffix in ("-wal", "-shm", "-journal"):
        if path.name.lower().endswith(suffix):
            database = path.with_name(path.name[:-len(suffix)])
            return database.suffix.lower() in {".sqlite", ".sqlite3", ".db"} and database.exists()
    return False


def _preflight(source: Path, target: Path, mode: str) -> tuple[int, list[Path]]:
    if mode not in {"copy", "switch"}:
        raise MigrationError("mode 仅支持 copy/switch")
    if source == target:
        raise MigrationError("新旧数据目录相同")
    if source in target.parents or target in source.parents:
        raise MigrationError("新旧数据目录不能互相嵌套")
    if not source.is_dir():
        raise MigrationError("原数据目录不存在")
    if mode == "copy" and target.exists() and (not target.is_dir() or any(target.iterdir())):
        raise MigrationError("目标目录不是空目录，拒绝覆盖")
    if mode == "switch" and (not target.exists() or not target.is_dir()):
        raise MigrationError("仅切换要求目标是已存在的数据目录")
    # SQLite 通过 backup API 生成自包含快照，绝不能再把源 WAL/SHM 侧车复制过去。
    files = [path for path in source.rglob("*")
             if path.is_file() and not _is_sqlite_sidecar(path)]
    if any(path.is_symlink() for path in source.rglob("*")):
        raise MigrationError("数据目录包含符号链接，无法保证复制边界")
    total = sum(path.stat().st_size for path in files)
    target.parent.mkdir(parents=True, exist_ok=True)
    if mode == "copy" and shutil.disk_usage(target.parent).free < total + max(16 * 1024 * 1024, total // 20):
        raise MigrationError("目标磁盘剩余空间不足")
    return total, files


def _sha256(path: Path, cancel: threading.Event | None = None) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            if cancel and cancel.is_set():
                raise InterruptedError
            digest.update(chunk)
    return digest.hexdigest()


def _source_state(root: Path) -> tuple[tuple[str, int, int], ...]:
    """Cheap final fence that includes SQLite sidecars and newly created files."""
    return tuple(sorted(
        (
            path.relative_to(root).as_posix(),
            path.stat().st_size,
            path.stat().st_mtime_ns,
        )
        for path in root.rglob("*")
        if path.is_file()
    ))


def _copy_sqlite(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(f"file:{source.as_posix()}?mode=ro", uri=True) as src:
        with sqlite3.connect(target) as dst:
            src.backup(dst)
            result = dst.execute("PRAGMA integrity_check").fetchone()
            if not result or result[0] != "ok":
                raise MigrationError(f"SQLite 校验失败: {source.name}")


class DataMigrationManager:
    def __init__(self, config_manager: ConfigManager | None = None):
        self.config_manager = config_manager or ConfigManager()
        self._tasks: dict[str, MigrationTask] = {}
        self._lock = threading.RLock()
        self._active_id: str | None = None

    @property
    def active(self) -> bool:
        with self._lock:
            task = self._tasks.get(self._active_id or "")
            return bool(task and task.status in {"pending", "running", "cancelling"})

    def create(self, target: str | Path, mode: str = "copy") -> dict:
        source = _resolved(get_config().data.root)
        target_path = _resolved(target)
        total, _ = _preflight(source, target_path, mode)
        lease = maintenance_barrier.enter("data_root_migration", timeout=30.0)
        try:
            with self._lock:
                if self.active:
                    raise MigrationError("已有数据迁移任务正在进行")
                task = MigrationTask(
                    id=uuid.uuid4().hex, source=str(source), target=str(target_path),
                    mode=mode, total_bytes=total, maintenance_lease=lease,
                )
                self._tasks[task.id] = task
                self._active_id = task.id
        except Exception:
            maintenance_barrier.exit(lease)
            raise
        if mode == "switch":
            # 仅切换仍走任务状态机，以便前端给出一致反馈。
            worker = threading.Thread(target=self._switch_only, args=(task,), daemon=True)
        else:
            worker = threading.Thread(target=self._copy_and_switch, args=(task,), daemon=True)
        worker.start()
        return task.public()

    def get(self, task_id: str) -> dict:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                raise KeyError("迁移任务不存在")
            return task.public()

    def cancel(self, task_id: str) -> dict:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                raise KeyError("迁移任务不存在")
            if task.status in {"completed", "failed", "cancelled"}:
                return task.public()
            task.status = "cancelling"
            task.phase = "正在取消"
            task.cancel_event.set()
            return task.public()

    def _finish(self, task: MigrationTask, status: str, error: str = "") -> None:
        with self._lock:
            task.status = status
            task.error = error
            task.finished_at = datetime.now(timezone.utc).isoformat()
            if status == "completed":
                task.progress = 100
                task.phase = "迁移完成"
            elif status == "cancelled":
                task.phase = "已取消，原目录未变"
            else:
                task.phase = "迁移失败，原目录未变"
            if self._active_id == task.id:
                self._active_id = None
            lease, task.maintenance_lease = task.maintenance_lease, None
        if lease is not None:
            try:
                maintenance_barrier.exit(lease)
            except Exception as exc:
                with self._lock:
                    task.error = "; ".join(filter(None, (task.error, str(exc))))
                    if task.status == "completed":
                        task.status = "failed"
                        task.phase = "迁移已完成，但后台组件恢复失败"

    def _switch_only(self, task: MigrationTask) -> None:
        try:
            task.status, task.phase, task.progress = "running", "仅切换数据目录", 50
            if task.cancel_event.is_set():
                raise InterruptedError
            self.config_manager.update_data_root(task.target)
            self._finish(task, "completed")
        except InterruptedError:
            self._finish(task, "cancelled")
        except Exception as exc:
            self._finish(task, "failed", str(exc))

    def _copy_and_switch(self, task: MigrationTask) -> None:
        source, target = Path(task.source), Path(task.target)
        temp = target.parent / f".{target.name}.qm-migration-{task.id}"
        try:
            task.status, task.phase = "running", "复制数据文件"
            _, files = _preflight(source, target, "copy")
            source_state = _source_state(source)
            temp.mkdir(parents=True, exist_ok=False)
            for directory in (path for path in source.rglob("*") if path.is_dir()):
                (temp / directory.relative_to(source)).mkdir(parents=True, exist_ok=True)
            checksums: list[tuple[Path, str, int]] = []
            for src in files:
                if task.cancel_event.is_set():
                    raise InterruptedError
                rel = src.relative_to(source)
                dst = temp / rel
                dst.parent.mkdir(parents=True, exist_ok=True)
                if src.suffix.lower() in {".sqlite", ".sqlite3", ".db"}:
                    _copy_sqlite(src, dst)
                    digest = "sqlite"
                else:
                    shutil.copy2(src, dst)
                    digest = _sha256(src, task.cancel_event)
                size = src.stat().st_size
                checksums.append((rel, digest, size))
                task.copied_bytes += size
                task.progress = min(85, round(85 * task.copied_bytes / max(task.total_bytes, 1)))
            task.phase, task.progress = "校验复制结果", 88
            for rel, digest, size in checksums:
                if task.cancel_event.is_set():
                    raise InterruptedError
                dst = temp / rel
                if not dst.is_file() or (digest != "sqlite" and dst.stat().st_size != size):
                    raise MigrationError(f"文件大小校验失败: {rel}")
                if digest != "sqlite" and _sha256(dst, task.cancel_event) != digest:
                    raise MigrationError(f"SHA-256 校验失败: {rel}")
            task.phase, task.progress = "确认源目录静止", 94
            if _source_state(source) != source_state:
                raise MigrationError(
                    "源数据目录在迁移期间发生写入；已放弃切换，请停止后台任务后重试"
                )
            task.phase, task.progress = "切换数据目录", 96
            if target.exists():
                target.rmdir()  # 预检已确认是空目录
            os.replace(temp, target)
            self.config_manager.update_data_root(target)
            self._finish(task, "completed")
        except InterruptedError:
            shutil.rmtree(temp, ignore_errors=True)
            self._finish(task, "cancelled")
        except Exception as exc:
            shutil.rmtree(temp, ignore_errors=True)
            self._finish(task, "failed", str(exc))


migration_manager = DataMigrationManager()
