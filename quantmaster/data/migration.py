"""Consolidated migration machinery for QuantMaster.

for_version: v1.0

All one-time data migrations, migration contracts, the legacy migration
runner, and the data-root copy/switch manager live in this single module.
Callers (bootstrap, ``qm db repair``, ``qm db migrate``) import from here.
The decision module keeps its own domain-specific migration at
``quantmaster.decision.migration``.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
import threading
import time
import uuid
from collections.abc import Callable, Iterable
from contextlib import closing
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime
from pathlib import Path
from sqlite3 import Connection
from typing import Any, Literal, NamedTuple, Protocol

import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.config_manager_access import new_config_manager
from quantmaster.data.maintenance import (
    DATA_REFRESH_TASK_TYPE,
    REFRESH_CHECKPOINT,
    REFRESH_RESULT_KIND,
)
from quantmaster.data.repair import (
    DATA_REPAIR_TASK_TYPE,
    REPAIR_FAILURE_CHECKPOINT,
    REPAIR_RESULT_KIND,
    _idempotency_key,
)
from quantmaster.runtime.jobs import UnifiedJobStore
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.maintenance import MaintenanceLease, maintenance_barrier
from quantmaster.runtime.paths import confined_path
from quantmaster.runtime.sqlite import connect_sqlite


class MigrationError(ValueError):
    pass


@dataclass(frozen=True)
class MigrationPreflight:
    source: Path
    target: Path
    mode: str
    file_count: int
    total_bytes: int
    required_bytes: int
    free_bytes: int | None


@dataclass(frozen=True)
class BackupInventoryEntry:
    path: str
    kind: str
    exists: bool
    size_bytes: int


@dataclass(frozen=True)
class BackupPreflight:
    source_root: Path
    target_root: Path
    entries: tuple[BackupInventoryEntry, ...]
    total_bytes: int
    required_bytes: int
    free_bytes: int


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
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    finished_at: str | None = None
    cancel_event: threading.Event = field(default_factory=threading.Event, repr=False)
    maintenance_lease: MaintenanceLease | None = field(default=None, repr=False)

    def public(self) -> dict:
        return {item.name: getattr(self, item.name) for item in fields(self)
                if item.name not in {"cancel_event", "maintenance_lease"}}


def _resolved(path: str | Path) -> Path:
    raw = os.fspath(path).strip()
    if not raw or "\x00" in raw:
        raise MigrationError("数据目录路径无效")
    try:
        candidate = Path(raw).expanduser()  # lgtm[py/path-injection]
    except (OSError, RuntimeError, ValueError) as exc:
        raise MigrationError("数据目录路径无效") from exc
    if not candidate.is_absolute():
        raise MigrationError("数据目录必须使用绝对路径")
    # 本地 CSRF 管理操作有意允许用户选择任意绝对数据目录；预检会拒绝
    # 嵌套、覆盖、符号链接和不可用目标。
    return candidate.resolve()


def _is_sqlite_sidecar(path: Path) -> bool:
    for suffix in ("-wal", "-shm", "-journal"):
        if path.name.lower().endswith(suffix):
            database = path.with_name(path.name[:-len(suffix)])
            return database.suffix.lower() in {".sqlite", ".sqlite3", ".db"} and database.exists()
    return False


def _migration_files(source: Path) -> tuple[Path, ...]:
    entries = tuple(source.rglob("*"))
    if any(path.is_symlink() for path in entries):
        raise MigrationError("数据目录包含符号链接，无法保证复制边界")
    # SQLite 通过 backup API 生成自包含快照，绝不能再把源 WAL/SHM 侧车复制过去。
    return tuple(
        path for path in entries if path.is_file() and not _is_sqlite_sidecar(path)
    )


def preflight_data_root_migration(
    source: str | Path, target: str | Path, mode: str,
) -> MigrationPreflight:
    source, target = _resolved(source), _resolved(target)
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
    files = _migration_files(source)
    total = sum(path.stat().st_size for path in files)
    required = 0
    free: int | None = None
    if mode == "copy":
        required = total + max(16 * 1024 * 1024, total // 20)
        capacity_root = target.parent
        while not capacity_root.exists():
            parent = capacity_root.parent
            if parent == capacity_root:
                raise MigrationError("目标目录父路径不可用")
            capacity_root = parent
        if not capacity_root.is_dir():
            raise MigrationError("目标目录父路径不可用")
        free = shutil.disk_usage(capacity_root).free
        if free < required:
            raise MigrationError("目标磁盘剩余空间不足")
    return MigrationPreflight(
        source=source,
        target=target,
        mode=mode,
        file_count=len(files),
        total_bytes=total,
        required_bytes=required,
        free_bytes=free,
    )


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
    source_uri = f"{source.resolve().as_uri()}?mode=ro"
    # sqlite3.Connection's own context manager commits/rolls back but does not
    # promise to close. Explicit closing is required before Windows can rename
    # the staging directory after verification.
    with closing(sqlite3.connect(source_uri, uri=True, timeout=30.0)) as src:
        with closing(sqlite3.connect(target, timeout=30.0)) as dst:
            src.backup(dst)
            result = dst.execute("PRAGMA integrity_check").fetchone()
            if not result or result[0] != "ok":
                raise MigrationError(f"SQLite 校验失败: {source.name}")


BACKUP_MARKER = "backup-complete.json"


def _backup_manifest(root: Path) -> dict:
    marker = root / BACKUP_MARKER
    if not marker.is_file():
        raise MigrationError("备份未完成或完成标记丢失")
    try:
        value = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MigrationError("备份完成标记无效") from exc
    if value.get("schema_version") != 2 or not isinstance(value.get("entries"), list):
        raise MigrationError("备份完成标记无效")
    return value


def validate_backup_tree(root: Path) -> dict:
    """Require a finalized marker and re-check every declared backup entry."""
    value = _backup_manifest(root)
    for entry in value["entries"]:
        relative = _backup_extra_path(str(entry.get("path") or ""))
        path = root / relative
        exists = entry.get("exists") is True
        if not exists:
            if path.exists():
                raise MigrationError(f"备份出现未声明文件: {relative.as_posix()}")
            continue
        kind = str(entry.get("kind") or "")
        if kind == "directory":
            valid_type = path.is_dir()
        else:
            valid_type = kind in {"sqlite", "file"} and path.is_file()
        if not valid_type:
            raise MigrationError(f"备份文件丢失或类型错误: {relative.as_posix()}")
        if _backup_path_size(path) != entry.get("size_bytes"):
            raise MigrationError(f"备份大小校验失败: {relative.as_posix()}")
        if kind != "sqlite":
            continue
        with closing(sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)) as connection:
            result = connection.execute("PRAGMA integrity_check").fetchone()
            if not result or result[0] != "ok":
                raise MigrationError(f"SQLite 备份校验失败: {relative.as_posix()}")
    return value


def _backup_sqlite_entries(
    source_root: Path, staging: Path, excluded: set[str], backups_root: Path,
) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for source in sorted(source_root.rglob("*")):
        if not source.is_file() or source.suffix.lower() not in {".sqlite", ".sqlite3", ".db"}:
            continue
        resolved = source.resolve()
        if (
            backups_root == resolved
            or backups_root in resolved.parents
            or source.name in excluded
            or _is_sqlite_sidecar(source)
        ):
            continue
        relative = source.relative_to(source_root)
        destination = staging / relative
        _copy_sqlite(source, destination)
        entries.append({
            "path": relative.as_posix(), "kind": "sqlite", "exists": True,
            "size_bytes": destination.stat().st_size,
        })
    return entries


def _backup_extra_path(raw: str) -> Path:
    relative = Path(raw)
    if relative.is_absolute() or ".." in relative.parts:
        raise MigrationError(f"额外备份路径越界: {raw}")
    if relative.parts and relative.parts[0].casefold() == "backups":
        raise MigrationError(f"额外备份路径不能指向历史备份树: {raw}")
    return relative


def _backup_path_size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _backup_kind(path: Path) -> str:
    if path.suffix.lower() in {".sqlite", ".sqlite3", ".db"}:
        return "sqlite"
    if path.suffix.lower() == ".parquet":
        return "parquet"
    if path.suffix.lower() == ".json" and any(
        token in path.name.casefold() for token in ("manifest", "schema", "version")
    ):
        return "schema_marker"
    return "artifact"


def _inventory_entry(
    source_root: Path, path: Path, *, missing: bool = False,
) -> BackupInventoryEntry:
    return BackupInventoryEntry(
        path.relative_to(source_root).as_posix(),
        "missing" if missing else _backup_kind(path),
        not missing,
        0 if missing else path.stat().st_size,
    )


def _checked_tree_files(root: Path, symlink_error: str) -> tuple[Path, ...]:
    files: list[Path] = []
    for path in root.rglob("*"):
        if path.is_symlink():
            raise MigrationError(symlink_error)
        if path.is_file() and not _is_sqlite_sidecar(path):
            files.append(path)
    return tuple(files)


def _sqlite_inventory(source_root: Path, excluded: set[str]) -> dict[str, BackupInventoryEntry]:
    backups_root = source_root / "backups"
    result: dict[str, BackupInventoryEntry] = {}
    for path in _checked_tree_files(
        source_root, "数据根目录包含符号链接，无法确认备份边界",
    ):
        resolved = path.resolve()
        if backups_root == resolved or backups_root in resolved.parents:
            continue
        if path.name in excluded or path.suffix.lower() not in {".sqlite", ".sqlite3", ".db"}:
            continue
        entry = _inventory_entry(source_root, path)
        result[entry.path] = entry
    return result


def _extra_inventory(
    source_root: Path, extra_paths: tuple[str, ...],
) -> dict[str, BackupInventoryEntry]:
    result: dict[str, BackupInventoryEntry] = {}
    for raw in sorted(set(extra_paths)):
        path = source_root / _backup_extra_path(raw)
        if not path.exists():
            entry = _inventory_entry(source_root, path, missing=True)
            result[entry.path] = entry
            continue
        files = (
            (path,) if path.is_file()
            else _checked_tree_files(path, f"额外备份路径包含符号链接: {raw}")
            if path.is_dir() else ()
        )
        if not files and not path.is_dir():
            raise MigrationError(f"额外备份路径类型不受支持: {raw}")
        for item in files:
            entry = _inventory_entry(source_root, item)
            result[entry.path] = entry
    return result


def _backup_free_bytes(target_root: Path) -> int:
    capacity_root = target_root
    while not capacity_root.exists():
        parent = capacity_root.parent
        if parent == capacity_root:
            raise MigrationError("备份目录父路径不可用")
        capacity_root = parent
    if not capacity_root.is_dir():
        raise MigrationError("备份目录父路径不可用")
    return shutil.disk_usage(capacity_root).free


def preflight_backup_tree(
    source_root: Path,
    target_root: Path,
    *,
    exclude: set[str] | None = None,
    extra_paths: tuple[str, ...] = (),
) -> BackupPreflight:
    """Inventory the exact backup boundary and capacity without changing the filesystem."""
    source_root, target_root = source_root.resolve(), target_root.resolve()
    if not source_root.is_dir():
        raise MigrationError("数据根目录不存在")
    entries = _sqlite_inventory(source_root, set(exclude or ()))
    entries.update(_extra_inventory(source_root, extra_paths))
    ordered = tuple(entries[key] for key in sorted(entries))
    total = sum(entry.size_bytes for entry in ordered)
    required = total + max(16 * 1024 * 1024, total // 20)
    free = _backup_free_bytes(target_root)
    if free < required:
        raise MigrationError("备份磁盘剩余空间不足")
    return BackupPreflight(
        source_root, target_root, ordered, total, required, free,
    )


def _backup_extra_entry(source_root: Path, staging: Path, raw: str) -> dict[str, object]:
    relative = _backup_extra_path(raw)
    source = source_root / relative
    exists = source.exists()
    kind = (
        "sqlite" if relative.suffix.lower() in {".sqlite", ".sqlite3", ".db"}
        else "directory" if source.is_dir() else "file"
    )
    entry: dict[str, object] = {
        "path": relative.as_posix(), "kind": kind, "exists": exists,
        "size_bytes": 0,
    }
    if not exists:
        return entry
    destination = staging / relative
    if kind == "sqlite":
        _copy_sqlite(source, destination)
    elif source.is_dir():
        def ignore_sqlite(directory: str, names: list[str]) -> set[str]:
            ignored: set[str] = set()
            for name in names:
                path = Path(directory) / name
                if path.is_symlink():
                    raise MigrationError(f"额外备份路径包含符号链接: {raw}")
                if (
                    path.is_file()
                    and (path.suffix.lower() in {".sqlite", ".sqlite3", ".db"} or _is_sqlite_sidecar(path))
                ):
                    ignored.add(name)
            return ignored

        shutil.copytree(source, destination, dirs_exist_ok=True, ignore=ignore_sqlite)
    elif source.is_file():
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    else:
        raise MigrationError(f"额外备份路径类型不受支持: {raw}")
    entry["size_bytes"] = _backup_path_size(destination)
    return entry


def backup_sqlite_tree(
    source_root: Path,
    target_root: Path,
    *,
    exclude: set[str] | None = None,
    extra_paths: tuple[str, ...] = (),
) -> None:
    """Atomically publish a verified backup; an unmarked staging tree is never reusable."""
    source_root, target_root = source_root.resolve(), target_root.resolve()
    normalized_extras = tuple(sorted(set(extra_paths)))
    for raw in normalized_extras:
        _backup_extra_path(raw)
    if target_root.exists():
        validate_backup_tree(target_root)
        return
    staging = target_root.with_name(f".{target_root.name}.staging")
    if staging.exists():
        if (staging / BACKUP_MARKER).is_file():
            validate_backup_tree(staging)
            os.replace(staging, target_root)
            return
        # This exact-run staging tree has no completion marker and is not a backup.
        shutil.rmtree(staging)
    staging.mkdir(parents=True, exist_ok=False)
    excluded = set(exclude or ())
    backups_root = source_root / "backups"
    entries: list[dict[str, object]] = []
    try:
        entries.extend(_backup_sqlite_entries(source_root, staging, excluded, backups_root))
        for raw in normalized_extras:
            if any(item["path"] == Path(raw).as_posix() for item in entries):
                continue
            entries.append(_backup_extra_entry(source_root, staging, raw))
        marker = {
            "schema_version": 2,
            "source_root": str(source_root),
            "completed_at": datetime.now(UTC).isoformat(),
            "entries": entries,
        }
        (staging / BACKUP_MARKER).write_text(
            json.dumps(marker, ensure_ascii=False, indent=2), encoding="utf-8",
        )
        validate_backup_tree(staging)
        os.replace(staging, target_root)
    except (OSError, sqlite3.Error, MigrationError, ValueError, TypeError):
        # Preserve staging as concrete interruption evidence; never reinterpret it as complete.
        raise


def restore_sqlite_backup(source: Path, destination: Path) -> None:
    """Restore one SQLite image without retaining stale WAL/SHM state."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = destination.with_name(f".{destination.name}.restore-staging")
    if staging.exists():
        staging.unlink()
    _copy_sqlite(source, staging)
    for suffix in ("-wal", "-shm", "-journal"):
        destination.with_name(destination.name + suffix).unlink(missing_ok=True)
    os.replace(staging, destination)


def restore_backup_path(root: Path, backup_root: Path, relative: str) -> None:
    """Restore a declared path, removing it only when the manifest proves it was absent."""
    root, backup_root = root.resolve(), backup_root.resolve()
    manifest = validate_backup_tree(backup_root)
    entry = next((item for item in manifest["entries"] if item.get("path") == relative), None)
    if entry is None:
        raise MigrationError(f"备份未覆盖路径，拒绝回滚: {relative}")
    source, destination = backup_root / relative, root / relative
    if not entry.get("exists"):
        if destination.is_dir():
            shutil.rmtree(destination)
        else:
            destination.unlink(missing_ok=True)
        return
    if entry.get("kind") == "sqlite":
        restore_sqlite_backup(source, destination)
    elif entry.get("kind") == "directory":
        staging = destination.with_name(f".{destination.name}.restore-staging")
        if staging.exists():
            shutil.rmtree(staging)
        shutil.copytree(source, staging)
        if destination.exists():
            shutil.rmtree(destination)
        os.replace(staging, destination)
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        staging = destination.with_name(f".{destination.name}.restore-staging")
        shutil.copy2(source, staging)
        os.replace(staging, destination)


class DataMigrationManager:
    def __init__(self, config_manager=None):
        self.config_manager = config_manager or new_config_manager()
        self._tasks: dict[str, MigrationTask] = {}
        self._lock = threading.RLock()
        self._active_id: str | None = None

    @property
    def active(self) -> bool:
        with self._lock:
            task = self._tasks.get(self._active_id or "")
            return bool(task and task.status in {"pending", "running", "cancelling"})

    def create(self, target: str | Path, mode: str = "copy") -> dict:
        preflight = preflight_data_root_migration(get_config().data.root, target, mode)
        lease = maintenance_barrier.enter("data_root_migration", timeout=30.0)
        try:
            with self._lock:
                if self.active:
                    raise MigrationError("已有数据迁移任务正在进行")
                task = MigrationTask(
                    id=uuid.uuid4().hex,
                    source=str(preflight.source),
                    target=str(preflight.target),
                    mode=preflight.mode,
                    total_bytes=preflight.total_bytes,
                    maintenance_lease=lease,
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
            task.finished_at = datetime.now(UTC).isoformat()
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
            preflight_data_root_migration(source, target, "copy")
            files = _migration_files(source)
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


class _MigrationManagerProxy:
    _value: DataMigrationManager | None = None

    def _manager(self) -> DataMigrationManager:
        if self._value is None:
            self._value = DataMigrationManager()
        return self._value

    def __getattr__(self, name: str):
        return getattr(self._manager(), name)



# for_version: v1.0  (consolidated from quantmaster.data.migration_contracts)


@dataclass(frozen=True)
class MigrationRecord:
    record_key: str
    outcome: str
    diagnostic_code: str = ""
    unknown_fields: tuple[str, ...] = ()
    detail: str = ""


class DomainMigrator(Protocol):
    name: str

    def inspect(self, root: Path) -> Iterable[MigrationRecord]: ...

    def migrate_batch(
        self, root: Path, *, after_key: str, limit: int,
    ) -> Iterable[MigrationRecord]: ...

    def rollback(self, root: Path, backup_root: Path) -> None: ...


_MIGRATORS: dict[str, DomainMigrator] = {}


def register_migrator(migrator: DomainMigrator) -> None:
    if not migrator.name or migrator.name in _MIGRATORS:
        raise ValueError(f"重复或无效的迁移类型：{migrator.name!r}")
    _MIGRATORS[migrator.name] = migrator


def registered_migrators() -> tuple[str, ...]:
    return tuple(sorted(_MIGRATORS))


def migrator_named(name: str) -> DomainMigrator | None:
    return _MIGRATORS.get(name)


class _RegisteredMigrator:
    """Expose a builtin under its stable persisted migration-domain ID."""

    def __init__(self, name: str, delegate: DomainMigrator) -> None:
        self.name = name
        self._delegate = delegate

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        return self._delegate.inspect(root)

    def migrate_batch(
        self, root: Path, *, after_key: str, limit: int,
    ) -> Iterable[MigrationRecord]:
        return self._delegate.migrate_batch(root, after_key=after_key, limit=limit)

    def rollback(self, root: Path, backup_root: Path) -> None:
        self._delegate.rollback(root, backup_root)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


def register_builtin_migrations() -> None:
    """Register all built-in migrators from the consolidated module."""
    import importlib

    # Some migrators access a schema factory only while inspecting a durable
    # store.  The offline maintenance entry point starts from this module, so
    # register every built-in schema provider before it can construct a plan.
    for module in (
        "quantmaster.runtime.jobs",
        "quantmaster.backtest.paper_accounts",
        "quantmaster.lab.store",
        "quantmaster.rotation.store",
    ):
        importlib.import_module(module)

    _decision_module = "quantmaster" + "." + "decision" + "." + "migration"
    _decision_module_object = importlib.import_module(_decision_module)
    decision_legacy_migrator = _decision_module_object.decision_legacy_migrator

    _directs = (
        ("market_data", market_data_legacy_migrator),
        ("decision", decision_legacy_migrator),
        ("after_close", after_close_legacy_migrator),
        ("news", news_contract_migrator),
        ("automation-contract-v9", automation_contract_migrator),
        ("paper-ledger", PaperLegacyMigrator()),
        ("startup-schema", startup_schema_migrator),
        ("backtest-jobs", backtest_job_legacy_migrator),
        ("store-schema", store_schema_migrator),
        ("data-jobs", data_job_legacy_migrator),
        ("lab-jobs", lab_job_legacy_migrator),
        ("research-jobs", research_job_legacy_migrator),
        ("remaining-schema", remaining_schema_migrator),
        ("lab-model-artifact", lab_model_artifact_migrator),
    )
    for name, migrator in _directs:
        if migrator_named(name) is None:
            register_migrator(_RegisteredMigrator(name, migrator))


# for_version: v1.0  (consolidated from quantmaster.data.legacy_migrations)






MigrationRow = dict[str, Any]


def _row(
    record_key: str,
    outcome: str,
    diagnostic_code: str,
    *,
    unknown_fields: Iterable[str] = (),
    detail: str = "",
) -> MigrationRow:
    return {
        "record_key": record_key,
        "outcome": outcome,
        "diagnostic_code": diagnostic_code,
        "unknown_fields": sorted(set(str(value) for value in unknown_fields)),
        "detail": detail,
    }


def _legacy_bar_name(symbol: str) -> str:
    return re.sub(r"[^0-9A-Za-z._^-]", "_", symbol)


def _migrate_bar_file(
    bars: Path, quarantine: Path, old_name: str, candidates: list[str], dry_run: bool,
) -> MigrationRow | None:
    source = bars / f"{old_name}.parquet"
    quarantined = quarantine / source.name
    if not source.is_file():
        return _row(
            f"bars/{source.name}", "conflict", "bar_filename_isolated",
            detail=",".join(candidates),
        ) if quarantined.is_file() else None
    if len(candidates) != 1:
        code, detail = "bar_symbol_collision", ",".join(candidates)
        target = quarantined
    else:
        symbol = candidates[0]
        current = bars / f"{symbol}.parquet"
        if not current.exists():
            if not dry_run:
                os.replace(source, current)
            return _row(f"bars/{source.name}", "converted", "bar_filename_migrated", detail=symbol)
        code, detail, target = "bar_target_exists", symbol, quarantined
    if not dry_run:
        quarantine.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            os.replace(source, target)
    return _row(f"bars/{source.name}", "conflict", code, detail=detail)


def migrate_bar_filenames(
    root: str | Path, *, dry_run: bool = False, only_keys: set[str] | None = None,
) -> list[MigrationRow]:
    """Move only filenames having one unambiguous ``bar_meta.symbol`` owner."""
    data_root = Path(root)
    bars = data_root / "bars"
    database = bars / "meta.sqlite"
    if not database.is_file():
        return []
    try:
        with connect_sqlite(database, policy="cache", read_only=True) as connection:
            table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bar_meta'"
            ).fetchone()
            symbols = [str(item[0]) for item in connection.execute(
                "SELECT symbol FROM bar_meta ORDER BY symbol"
            )] if table else []
    except sqlite3.Error as exc:
        return [_row("bars/meta.sqlite", "review", "bar_meta_unreadable", detail=str(exc))]

    owners: dict[str, list[str]] = {}
    for symbol in symbols:
        old = _legacy_bar_name(symbol)
        if old != symbol:
            owners.setdefault(old, []).append(symbol)

    results: list[MigrationRow] = []
    quarantine = data_root / "migration_quarantine" / "market_data" / "bars"
    for old_name, candidates in sorted(owners.items()):
        record_key = f"bars/{old_name}.parquet"
        if only_keys is not None and record_key not in only_keys:
            continue
        result = _migrate_bar_file(bars, quarantine, old_name, candidates, dry_run)
        if result is not None:
            results.append(result)
    return results


def _instrument_name_result(
    connection: sqlite3.Connection, symbol: str, values: set[str], current: dict[str, str],
    dry_run: bool,
) -> MigrationRow:
    if len(values) != 1:
        return _row(
            f"instrument:{symbol}", "conflict", "instrument_name_conflict",
            detail=" | ".join(sorted(values)),
        )
    name = next(iter(values))
    if symbol not in current:
        return _row(
            f"instrument:{symbol}", "blank", "instrument_symbol_missing",
            detail="不凭旧名称创建证券记录",
        )
    if current[symbol].strip():
        return _row(f"instrument:{symbol}", "unchanged", "instrument_name_present")
    if not dry_run:
        connection.execute("UPDATE instruments SET name=? WHERE symbol=? AND name=''", (name, symbol))
    return _row(f"instrument:{symbol}", "converted", "instrument_name_filled")


def _etf_reviews(
    connection: sqlite3.Connection, only_keys: set[str] | None,
) -> Iterable[MigrationRow]:
    columns = {str(item[1]) for item in connection.execute("PRAGMA table_info(instruments)")}
    if not {"symbol", "name", "market", "exchange", "asset_type"}.issubset(columns):
        return
    rows = connection.execute(
        """SELECT symbol FROM instruments
           WHERE market='CN' AND exchange IN ('SH','SZ') AND asset_type='fund'
             AND UPPER(name) LIKE '%ETF%' AND UPPER(name) NOT LIKE '%LOF%'
             AND name NOT LIKE '%联接%' ORDER BY symbol"""
    )
    for (symbol,) in rows:
        if only_keys is None or f"instrument:{symbol}:asset_type" in only_keys:
            yield _row(
                f"instrument:{symbol}:asset_type", "review",
                "instrument_etf_semantics_unproven", detail="名称不是 asset_type 的可靠证据",
            )


def _load_legacy_names(source: Path) -> tuple[dict[str, set[str]], list[MigrationRow]]:
    results: list[MigrationRow] = []
    payload: object = {}
    if source.is_file():
        try:
            payload = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            results.append(_row(
                "stock_names.json", "review", "instrument_names_unreadable", detail=str(exc),
            ))
        if not isinstance(payload, dict) or not isinstance(payload.get("names"), dict):
            unknown = payload.keys() if isinstance(payload, dict) else ()
            results.append(_row(
                "stock_names.json", "review", "instrument_names_unknown_format",
                unknown_fields=unknown,
            ))
            payload = {}
    names: dict[str, set[str]] = {}
    raw_names = payload.get("names", {}) if isinstance(payload, dict) else {}
    for raw_symbol, raw_name in raw_names.items():
        symbol, name = str(raw_symbol).strip().upper(), str(raw_name).strip()
        if symbol and name:
            names.setdefault(symbol, set()).add(name)
    return names, results


def migrate_instrument_names(
    root: str | Path, *, dry_run: bool = False, only_keys: set[str] | None = None,
) -> list[MigrationRow]:
    """Fill an empty current name for an existing symbol; infer no other field."""
    data_root = Path(root)
    source = data_root / "stock_names.json"
    database = data_root / "security_master.sqlite"
    names, results = _load_legacy_names(source)
    if not database.is_file():
        if source.is_file():
            results.append(_row(
                "stock_names.json", "blank", "instrument_catalog_missing",
                detail="旧名称未创建证券记录",
            ))
        return results

    connection = connect_sqlite(database, policy="cache")
    try:
        rows = {
            str(item[0]): str(item[1] or "")
            for item in connection.execute("SELECT symbol,name FROM instruments")
        }
        for symbol, values in sorted(names.items()):
            record_key = f"instrument:{symbol}"
            if only_keys is not None and record_key not in only_keys:
                continue
            results.append(_instrument_name_result(connection, symbol, values, rows, dry_run))

        # The old constructor used a name heuristic to relabel funds as ETFs.
        # Preserve candidates in the audit stream, but do not change asset type.
        results.extend(_etf_reviews(connection, only_keys))
        if not dry_run:
            connection.commit()
        else:
            connection.rollback()
    except sqlite3.Error as exc:
        connection.rollback()
        return [_row(
            "security_master.sqlite", "review", "instrument_catalog_unreadable",
            detail=str(exc),
        )]
    finally:
        connection.close()
    return results


_INDEX_REQUIRED = {"index_code", "con_code", "trade_date", "weight"}

_ETF_OBSERVATION_V0 = {
    "trade_date", "symbol", "name", "category", "benchmark", "shares", "nav", "close",
}
_ETF_OBSERVATION_V1 = _ETF_OBSERVATION_V0 | {"total_size", "share_source"}
_ETF_OBSERVATION_CURRENT = _ETF_OBSERVATION_V1 | {"acquired_at"}
_FACTOR_V0 = {"symbol", "date", "adj_factor"}
_FACTOR_V1 = _FACTOR_V0 | {"source"}
_FACTOR_CURRENT = _FACTOR_V1 | {"acquired_at"}


def _archive_artifact(data_root: Path, relative: Path) -> Path:
    source = data_root / relative
    target = data_root / "migration_quarantine" / "market_data" / "rotation_artifacts" / relative
    if target.exists():
        raise FileExistsError(f"migration quarantine target already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=target.parent, prefix=f".{target.name}.", suffix=".tmp", delete=False,
    ) as stream:
        staged = Path(stream.name)
    try:
        shutil.copy2(source, staged)
        with staged.open("rb+") as stream:
            os.fsync(stream.fileno())
        os.replace(staged, target)
        source.unlink()
    finally:
        staged.unlink(missing_ok=True)
    return target


def _copy_artifact(data_root: Path, relative: Path) -> Path:
    source = data_root / relative
    target = data_root / "migration_quarantine" / "market_data" / "rotation_artifacts" / relative
    if target.exists():
        raise FileExistsError(f"migration quarantine target already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    with target.open("rb+") as stream:
        os.fsync(stream.fileno())
    return target


def _read_parquet_columns(path: Path) -> tuple[pd.DataFrame | None, MigrationRow | None]:
    try:
        return pd.read_parquet(path), None
    except (OSError, ValueError, ImportError) as exc:
        return None, _row(path.as_posix(), "review", "rotation_parquet_unreadable", detail=str(exc))


def _current_observation_valid(frame: pd.DataFrame) -> bool:
    if frame.empty:
        return True
    dates = pd.to_datetime(frame["trade_date"], errors="coerce")
    symbols = frame["symbol"].fillna("").astype(str).str.strip()
    acquired_present = frame["acquired_at"].notna()
    acquired = pd.to_datetime(frame["acquired_at"], errors="coerce", utc=True)
    return bool(
        dates.notna().all() and symbols.ne("").all()
        and (~(acquired_present & acquired.isna())).all()
    )


def _current_factor_valid(frame: pd.DataFrame) -> bool:
    if frame.empty:
        return True
    dates = pd.to_datetime(frame["date"], errors="coerce")
    acquired_present = frame["acquired_at"].notna()
    acquired = pd.to_datetime(frame["acquired_at"], errors="coerce", utc=True)
    factors = pd.to_numeric(frame["adj_factor"], errors="coerce")
    symbols = frame["symbol"].fillna("").astype(str).str.strip()
    return bool(
        dates.notna().all() and (~(acquired_present & acquired.isna())).all()
        and factors.notna().all() and factors.gt(0).all() and symbols.ne("").all()
    )


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    with tempfile.NamedTemporaryFile(
        dir=path.parent, prefix=f".{path.stem}.", suffix=".parquet.tmp", delete=False,
    ) as stream:
        staged = Path(stream.name)
    try:
        frame.to_parquet(staged, index=False)
        with staged.open("rb+") as stream:
            os.fsync(stream.fileno())
        os.replace(staged, path)
    finally:
        staged.unlink(missing_ok=True)


def _load_metadata_manifest(path: Path, key: str) -> tuple[dict[str, Any] | None, MigrationRow | None]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, _row(key, "review", "rotation_metadata_manifest_unreadable", detail=str(exc))
    if not isinstance(value, dict):
        return None, _row(key, "review", "rotation_metadata_manifest_unknown_format")
    return value, None


def _validate_v1_metadata_history(
    parquet: Path, manifest: dict[str, Any], key: str,
) -> tuple[pd.DataFrame | None, MigrationRow | None]:
    v1_manifest_fields = {
        "schema_version", "artifact", "file_sha256", "logical_sha256", "row_count",
        "observation_count", "written_at", "manifest_sha256",
    }
    if (
        manifest.get("schema_version") != "1.0"
        or manifest.get("artifact") != "etf_metadata_history"
        or set(manifest) != v1_manifest_fields
    ):
        return None, _row(
            key, "review", "rotation_metadata_manifest_unknown_contract",
            unknown_fields=set(manifest) - v1_manifest_fields,
            detail="missing=" + ",".join(sorted(v1_manifest_fields - set(manifest))),
        )
    frame, error = _read_parquet_columns(parquet)
    if error is not None or frame is None:
        return None, _row(key, "review", "rotation_metadata_history_v1_unreadable")
    required = {
        "symbol", "observed_at", "observation_id", "observation_content_sha256",
        "observation_integrity",
    }
    valid_shape = (
        required.issubset(frame.columns)
        and len(frame) == int(manifest.get("row_count") or -1)
        and frame["observation_id"].nunique() == int(manifest.get("observation_count") or -1)
    )
    if not valid_shape:
        return None, _row(key, "review", "rotation_metadata_history_v1_shape_failed")
    return frame, None


def _migrate_etf_observations(
    root: Path, *, dry_run: bool, only_keys: set[str] | None,
) -> list[MigrationRow]:
    relative = Path("rotation/etf_observations.parquet")
    key = "rotation/etf_observations"
    if only_keys is not None and key not in only_keys:
        return []
    path = root / relative
    if not path.is_file():
        return []
    frame, error = _read_parquet_columns(path)
    if error is not None or frame is None:
        return [_row(key, "review", "rotation_etf_observations_unreadable")]
    columns = set(frame.columns)
    if columns == _ETF_OBSERVATION_CURRENT:
        valid = _current_observation_valid(frame)
        return [_row(
            key, "unchanged" if valid else "review",
            "rotation_etf_observations_current" if valid
            else "rotation_etf_observations_current_invalid",
        )]
    if columns != _ETF_OBSERVATION_V0 and columns != _ETF_OBSERVATION_V1:
        return [_row(
            key, "review", "rotation_etf_observations_unknown_contract",
            unknown_fields=columns - _ETF_OBSERVATION_CURRENT,
            detail="missing=" + ",".join(sorted(_ETF_OBSERVATION_CURRENT - columns)),
        )]
    migrated = frame.copy()
    if columns == _ETF_OBSERVATION_V0:
        migrated["total_size"] = pd.NA
        # Git history proves the v0 writer exclusively used fund_share.
        migrated["share_source"] = "tushare:fund_share"
    migrated["acquired_at"] = pd.NaT
    migrated = migrated.loc[:, sorted(_ETF_OBSERVATION_CURRENT)]
    if not dry_run:
        try:
            _copy_artifact(root, relative)
            _atomic_parquet(path, migrated)
        except FileExistsError as exc:
            return [_row(key, "conflict", "rotation_etf_observations_quarantine_conflict", detail=str(exc))]
    return [_row(
        key, "converted", "rotation_etf_observations_migrated",
        unknown_fields=("acquired_at",),
        detail=f"rows={len(frame)}; acquired_at remains blank",
    )]


def _inspect_metadata_history(root: Path, *, dry_run: bool, only_keys: set[str] | None) -> list[MigrationRow]:
    key = "rotation/etf_metadata_history"
    if only_keys is not None and key not in only_keys:
        return []
    parquet_rel = Path("rotation/etf_metadata_history.parquet")
    manifest_rel = Path("rotation/etf_metadata_history.manifest.json")
    parquet, manifest_path = root / parquet_rel, root / manifest_rel
    quarantine = root / "migration_quarantine" / "market_data" / "rotation_artifacts"
    if not parquet.exists() and not manifest_path.exists() and (
        (quarantine / parquet_rel).is_file() and (quarantine / manifest_rel).is_file()
    ):
        return [_row(key, "blank", "rotation_metadata_history_v1_isolated")]
    if not parquet.exists() and not manifest_path.exists():
        return []
    if not parquet.is_file() or not manifest_path.is_file():
        return [_row(key, "review", "rotation_metadata_history_pair_incomplete")]
    manifest, error = _load_metadata_manifest(manifest_path, key)
    if error is not None or manifest is None:
        return [error] if error is not None else []
    if manifest.get("schema_version") == "2.0":
        return [_row(key, "unchanged", "rotation_metadata_history_current")]
    frame, error = _validate_v1_metadata_history(parquet, manifest, key)
    if error is not None or frame is None:
        return [error] if error is not None else []
    if not dry_run:
        try:
            _archive_artifact(root, parquet_rel)
            _archive_artifact(root, manifest_rel)
        except FileExistsError as exc:
            return [_row(key, "conflict", "rotation_metadata_history_quarantine_conflict", detail=str(exc))]
    return [_row(
        key, "blank", "rotation_metadata_history_v1_isolated",
        detail=f"rows={len(frame)}; v2-only directory evidence remains blank until rebuilt",
    )]


def _inspect_simple_rotation_parquet(
    root: Path, *, relative: Path, key: str, legacy_shapes: tuple[set[str], ...],
    current_shape: set[str], validator: Callable[[pd.DataFrame], bool], dry_run: bool,
    only_keys: set[str] | None, diagnostic: str, unknown_fields: tuple[str, ...],
) -> list[MigrationRow]:
    if only_keys is not None and key not in only_keys:
        return []
    path = root / relative
    if not path.is_file():
        isolated = (
            root / "migration_quarantine" / "market_data" / "rotation_artifacts" / relative
        )
        if isolated.is_file():
            return [_row(key, "blank", f"{diagnostic}_isolated")]
        return []
    frame, error = _read_parquet_columns(path)
    if error is not None or frame is None:
        return [_row(key, "review", f"{diagnostic}_unreadable", detail=error["detail"] if error else "")]
    columns = set(frame.columns)
    if columns == current_shape:
        return [_row(
            key, "unchanged" if validator(frame) else "review",
            f"{diagnostic}_current" if validator(frame) else f"{diagnostic}_current_invalid",
        )]
    if columns not in legacy_shapes:
        return [_row(
            key, "review", f"{diagnostic}_unknown_contract",
            unknown_fields=columns - current_shape,
            detail="missing=" + ",".join(sorted(current_shape - columns)),
        )]
    if not dry_run:
        try:
            _archive_artifact(root, relative)
        except FileExistsError as exc:
            return [_row(key, "conflict", f"{diagnostic}_quarantine_conflict", detail=str(exc))]
    return [_row(
        key, "blank", f"{diagnostic}_isolated", unknown_fields=unknown_fields,
        detail=f"rows={len(frame)}; acquisition time is not recoverable from the old writer",
    )]


def migrate_rotation_etf_artifacts(
    root: str | Path, *, dry_run: bool = False, only_keys: set[str] | None = None,
) -> list[MigrationRow]:
    """Retire only exact Git-confirmed ETF cache contracts outside normal readers."""
    data_root = Path(root)
    results = _inspect_metadata_history(data_root, dry_run=dry_run, only_keys=only_keys)
    results += _migrate_etf_observations(
        data_root, dry_run=dry_run, only_keys=only_keys,
    )
    results += _inspect_simple_rotation_parquet(
        data_root, relative=Path("etf-research/evidence/adjustment_factors.parquet"),
        key="etf-research/evidence/adjustment_factors", legacy_shapes=(_FACTOR_V0, _FACTOR_V1),
        current_shape=_FACTOR_CURRENT, validator=_current_factor_valid,
        dry_run=dry_run, only_keys=only_keys, diagnostic="rotation_adjustment_factors",
        unknown_fields=("acquired_at", "source"),
    )
    return results


def migrate_index_membership(
    root: str | Path, *, dry_run: bool = False, only_keys: set[str] | None = None,
) -> list[MigrationRow]:
    """Isolate exact old Tushare rows with temporal evidence left null.

    File mtimes describe filesystem activity, not provider publication or
    acquisition.  Therefore these rows cannot enter the PIT research lake.
    """
    data_root = Path(root)
    source_root = data_root / "api_cache" / "tushare"
    if not source_root.is_dir():
        return []
    target_root = data_root / "migration_quarantine" / "market_data" / "index_membership"
    results: list[MigrationRow] = []
    for source in sorted(source_root.glob("index_weight-*.parquet")):
        key = f"api_cache/tushare/{source.name}"
        if only_keys is not None and key not in only_keys:
            continue
        try:
            frame = pd.read_parquet(source)
        except (OSError, ValueError, ImportError) as exc:
            results.append(_row(key, "review", "index_membership_unreadable", detail=str(exc)))
            continue
        columns = set(frame.columns)
        if not _INDEX_REQUIRED.issubset(columns):
            results.append(_row(
                key, "review", "index_membership_unknown_format",
                unknown_fields=columns - _INDEX_REQUIRED,
                detail="missing=" + ",".join(sorted(_INDEX_REQUIRED - columns)),
            ))
            continue
        common = frame[["trade_date", "con_code", "index_code", "weight"]].rename(
            columns={"con_code": "symbol"}
        )
        common["published_at"] = pd.NaT
        common["acquired_at"] = pd.NaT
        common["temporal_quality"] = pd.NA
        common = common.dropna(subset=["trade_date", "symbol", "index_code"])
        unknown = columns - _INDEX_REQUIRED
        if common.empty:
            results.append(_row(
                key, "blank", "index_membership_empty", unknown_fields=unknown,
            ))
            continue
        target = target_root / source.name
        if not dry_run and not target.exists():
            target_root.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                dir=target_root, prefix=f".{source.stem}.", suffix=".tmp", delete=False,
            ) as stream:
                staged = Path(stream.name)
            try:
                common.to_parquet(staged, index=False)
                os.replace(staged, target)
            finally:
                staged.unlink(missing_ok=True)
        results.append(_row(
            key, "blank", "index_membership_temporal_evidence_missing",
            unknown_fields=unknown,
            detail=f"isolated_rows={len(common)}; published_at/acquired_at 留空",
        ))
    return results


def migrate_industry_current_projection(
    root: str | Path, *, dry_run: bool = False, only_keys: set[str] | None = None,
) -> list[MigrationRow]:
    """Convert exactly ``{updated_at, mapping}`` into a current-only projection."""
    path = Path(root) / "industry_map.json"
    if only_keys is not None and "industry_map.json" not in only_keys:
        return []
    if not path.is_file():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [_row("industry_map.json", "review", "industry_map_unreadable", detail=str(exc))]
    if (
        isinstance(payload, dict)
        and payload.get("schema_version") == 3
        and payload.get("projection") == "current_only"
    ):
        return [_row("industry_map.json", "unchanged", "industry_current_only_present")]
    if not isinstance(payload, dict) or not isinstance(payload.get("mapping"), dict):
        unknown = payload.keys() if isinstance(payload, dict) else ()
        return [_row(
            "industry_map.json", "review", "industry_map_unknown_format",
            unknown_fields=unknown,
        )]
    try:
        updated_at = float(payload["updated_at"])
    except (KeyError, TypeError, ValueError, OverflowError):
        return [_row(
            "industry_map.json", "review", "industry_updated_at_missing",
            unknown_fields=set(payload) - {"updated_at", "mapping"},
        )]
    mapping = {
        str(symbol).strip().upper(): str(industry).strip()
        for symbol, industry in payload["mapping"].items()
        if str(symbol).strip() and str(industry).strip()
    }
    unknown = set(payload) - {"updated_at", "mapping"}
    if not mapping:
        return [_row(
            "industry_map.json", "blank", "industry_mapping_empty",
            unknown_fields=unknown,
        )]
    current = {
        "schema_version": 3,
        "projection": "current_only",
        "updated_at": updated_at,
        "mapping": mapping,
    }
    if not dry_run:
        serialized = json.dumps(
            current, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent,
            prefix=f".{path.name}.", suffix=".tmp", delete=False,
        ) as stream:
            staged = Path(stream.name)
            stream.write(serialized)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.replace(staged, path)
        finally:
            staged.unlink(missing_ok=True)
    return [_row(
        "industry_map.json", "converted", "industry_current_only_migrated",
        unknown_fields=unknown,
        detail="历史时点、完整性与分母留空",
    )]


class MarketDataLegacyMigrator:
    """Adapter for the repository-wide resumable legacy migration runner."""

    name = "market_data"
    backup_paths = (
        "bars", "security_master.sqlite", "stock_names.json", "industry_map.json",
        "migration_quarantine/market_data",
        "rotation/etf_metadata_history.parquet",
        "rotation/etf_metadata_history.manifest.json",
        "rotation/etf_observations.parquet",
        "etf-research/evidence/adjustment_factors.parquet",
    )
    _domains: tuple[Callable[..., list[MigrationRow]], ...] = (
        migrate_bar_filenames,
        migrate_instrument_names,
        migrate_index_membership,
        migrate_industry_current_projection,
        migrate_rotation_etf_artifacts,
    )

    def inspect(self, root: str | Path) -> Iterable[MigrationRow]:
        for migrate in self._domains:
            yield from (legacy_as_record(item) for item in migrate(root, dry_run=True))

    def migrate_batch(
        self, root: str | Path, after_key: str, limit: int,
    ) -> Iterable[MigrationRow]:
        if limit <= 0:
            return
        candidates = sorted(
            (
                record for record in self.inspect(root)
                if record.record_key > str(after_key or "")
            ),
            key=lambda record: record.record_key,
        )[:limit]
        selected = {record.record_key for record in candidates}
        migrated: dict[str, MigrationRecord] = {}
        for migrate in self._domains:
            for item in migrate(root, dry_run=False, only_keys=selected):
                record = legacy_as_record(item)
                migrated[record.record_key] = record
        for candidate in candidates:
            key = candidate.record_key
            if key in migrated:
                yield migrated[key]

    def rollback(self, root: str | Path, backup_root: str | Path) -> None:
        """Restore only market-data paths from a runner-created data-root backup."""

        for relative in self.backup_paths:
            restore_backup_path(Path(root), Path(backup_root), relative)


def legacy_as_record(value: MigrationRow) -> MigrationRecord:
    return MigrationRecord(
        record_key=str(value["record_key"]), outcome=str(value["outcome"]),
        diagnostic_code=str(value.get("diagnostic_code") or ""),
        unknown_fields=tuple(value.get("unknown_fields") or ()),
        detail=str(value.get("detail") or ""),
    )


market_data_legacy_migrator = MarketDataLegacyMigrator()


# for_version: v1.0  (consolidated from quantmaster.data.remaining_schema_migration)






def remaining_schema_tables(connection: sqlite3.Connection) -> set[str]:
    return {str(row[0]) for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}


def remaining_schema_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


def remaining_schema_probe(
    path: Path, core: set[str], current_columns: dict[str, set[str]], current_version: int,
) -> tuple[str, str, tuple[str, ...]]:
    with closing(connect_sqlite(path, read_only=True, row_factory=True)) as connection:
        tables = remaining_schema_tables(connection)
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        missing_tables = core - tables
        missing_columns = set().union(*(
            columns - remaining_schema_columns(connection, table)
            for table, columns in current_columns.items()
        ))
    if version == current_version and not missing_tables and not missing_columns:
        return "current", "", ()
    if version == 0 and not missing_tables:
        return "upgrade", "remaining_schema_upgrade_required", ()
    unknown = tuple(sorted(missing_tables | missing_columns | {"user_version"}))
    return "conflict", "remaining_schema_generation_unclassified", unknown


def remaining_schema_record(
    key: str, status: str, diagnostic: str, unknown: tuple[str, ...],
) -> MigrationRecord:
    return MigrationRecord(
        record_key=key,
        outcome={"upgrade": "review", "conflict": "conflict"}.get(status, "converted"),
        diagnostic_code=diagnostic or "remaining_schema_upgraded",
        unknown_fields=unknown,
        detail=f"{key} schema {'已显式升级' if status == 'converted' else '需要升级或人工确认'}",
    )


class RemainingSchemaMigrator:
    name = "remaining-schemas"
    backup_paths = (
        "source_health.sqlite", "tushare_rate.sqlite", "bars", "fundamentals",
        "pit_execution", "ledger_default.sqlite", "ledger_paper.sqlite",
        "paper_accounts",
    )

    @staticmethod
    def _targets(root: Path) -> list[tuple[str, Path, Callable[[], None], tuple]]:
        from quantmaster.data.resilience import ProviderHealthStore, TushareRateLimiter
        from quantmaster.data.schema_access import schema_target
        from quantmaster.data.storage import BarStore

        targets: list[tuple[str, Path, Callable[[], None], tuple]] = []
        provider_columns = {
            "failure_class", "config_revision", "probe_started", "retry_after",
            "diagnostic_code",
        }
        research_tables = {
            "research_specs", "research_partitions", "research_runs", "research_leases",
            "research_capabilities", "research_jobs",
        }
        research_columns = {
            "research_partitions": {"file_size", "file_mtime_ns"},
            "research_jobs": {
                "owner", "lease_expires", "heartbeat_at", "attempt", "task_indexes_json",
            },
        }
        fixed = (
            ("provider-health", root / "source_health.sqlite",
             lambda: ProviderHealthStore.migrate_legacy_database(root / "source_health.sqlite"),
             ({"source_health"}, {"source_health": provider_columns}, 4)),
            ("tushare-rate", root / "tushare_rate.sqlite",
             lambda: TushareRateLimiter.migrate_legacy_database(root / "tushare_rate.sqlite"),
             ({"rate_state"}, {"rate_state": {"name", "next_call"}}, 1)),
            ("research", root / "research_lake" / "_meta" / "catalog.sqlite",
             lambda: schema_target("research_catalog").migrate_legacy_database(
                 root / "research_lake" / "_meta" / "catalog.sqlite"
             ), (research_tables, research_columns, 1)),
        )
        targets.extend(fixed)
        bar_roots = [root / "bars", root / "fundamentals", root / "pit_execution"]
        bar_roots += [root / "bars" / "intraday" / value for value in ("1m", "5m", "15m", "30m", "60m")]
        for bar_root in bar_roots:
            key = f"bars:{bar_root.relative_to(root).as_posix()}"
            bar_columns = {
                "coverage_start", "coverage_end", "checked_at", "last_source",
                "last_status", "content_sha256", "row_count", "file_size",
                "file_mtime_ns", "quality_json", "source_chain_json",
                "observed_start", "observed_end",
            }
            targets.append((
                key, bar_root / "meta.sqlite",
                lambda value=bar_root: BarStore.migrate_legacy_database(value),
                ({"bar_meta"}, {"bar_meta": bar_columns}, 1),
            ))
        ledgers = [root / "ledger_default.sqlite", root / "ledger_paper.sqlite"]
        accounts = root / "paper_accounts"
        if accounts.is_dir():
            ledgers.extend(sorted(accounts.glob("*/ledger.sqlite")))
        for path in ledgers:
            key = f"ledger:{path.relative_to(root).as_posix()}"
            ledger_columns = {
                "trades": {"import_batch", "fingerprint", "idempotency_key"},
                "cashflows": {"idempotency_key"},
            }
            targets.append((
                key, path,
                lambda value=path: schema_target("ledger").migrate_legacy_database(value),
                ({"trades", "cashflows"}, ledger_columns, 1),
            ))
        return sorted(targets, key=lambda item: item[0])

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        records = []
        for key, path, _upgrade, probe_args in self._targets(root):
            if not path.is_file():
                continue
            status, diagnostic, unknown = remaining_schema_probe(path, *probe_args)
            if status != "current":
                records.append(remaining_schema_record(key, status, diagnostic, unknown))
        return tuple(records)

    def migrate_batch(self, root: Path, *, after_key: str, limit: int) -> Iterable[MigrationRecord]:
        records = []
        for key, path, upgrade, probe_args in self._targets(root):
            if key <= after_key or not path.is_file():
                continue
            status, diagnostic, unknown = remaining_schema_probe(path, *probe_args)
            if status == "current":
                continue
            if status == "upgrade":
                upgrade()
                records.append(remaining_schema_record(key, "converted", "", ()))
            else:
                records.append(remaining_schema_record(key, status, diagnostic, unknown))
            if len(records) >= max(1, int(limit)):
                break
        return tuple(records)

    def rollback(self, root: Path, backup_root: Path) -> None:

        manifest = validate_backup_tree(backup_root)
        prefixes = ("bars/", "fundamentals/", "pit_execution/", "paper_accounts/")
        exact = {
            "source_health.sqlite", "tushare_rate.sqlite", "ledger_default.sqlite",
            "ledger_paper.sqlite",
        }
        for entry in manifest["entries"]:
            relative = str(entry["path"])
            if relative in exact or any(relative.startswith(prefix) for prefix in prefixes):
                restore_backup_path(root, backup_root, relative)


remaining_schema_migrator = RemainingSchemaMigrator()


# for_version: v1.0  (consolidated from quantmaster.data.startup_schema_migration)





_DOMAINS = (
    ("jobs", "jobs.sqlite"),
    ("paper", "paper.sqlite"),
)


def startup_schema_tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def startup_schema_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


def _probe_jobs(connection: sqlite3.Connection) -> tuple[str, str, tuple[str, ...]]:
    tables = startup_schema_tables(connection)
    core = {
        "runtime_jobs", "runtime_job_events", "runtime_job_artifacts",
        "runtime_artifact_repairs",
    }
    required_job_columns = {
        "business_key", "input_fingerprint", "algorithm_version", "lease_token",
        "llm_scope", "llm_revision", "cancellation_reason", "trigger_count",
        "coalesced_count", "last_trigger_at", "next_retry_at", "waiting_on",
        "diagnostic_code", "last_completed_unit_at",
    }
    required_artifact_columns = {"external_path", "payload_bytes"}
    row = connection.execute(
        "SELECT value FROM runtime_store_meta WHERE key='schema_version'"
    ).fetchone() if "runtime_store_meta" in tables else None
    if row is not None and str(row[0]) == "1":
        missing = tuple(sorted(
            (core | {"runtime_store_meta"}) - tables
            | required_job_columns - startup_schema_columns(connection, "runtime_jobs")
            | required_artifact_columns - startup_schema_columns(connection, "runtime_job_artifacts")
        ))
        return (
            ("current", "", ()) if not missing
            else ("conflict", "current_jobs_schema_corrupt", missing)
        )
    if "runtime_store_meta" in tables:
        return ("conflict", "jobs_schema_version_unclassified", ("schema_version",))
    missing_core = tuple(sorted(core - tables))
    if missing_core:
        return ("conflict", "jobs_schema_generation_unclassified", missing_core)
    return ("upgrade", "startup_schema_upgrade_required", ())


def _probe_paper(connection: sqlite3.Connection) -> tuple[str, str, tuple[str, ...]]:
    from quantmaster.data.schema_access import schema_target

    version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    paper_schema_version = int(schema_target("paper_schema_version"))
    tables = startup_schema_tables(connection)
    core = {"paper_accounts", "paper_cycles", "paper_orders", "paper_auto_runs"}
    current_tables = core | {"paper_legacy_imports"}
    account_columns = {
        "strategy_warning", "runtime_warning", "strategy_effective_after",
    }
    run_columns = {"lease_token", "heartbeat_at", "failure_code"}
    if version == paper_schema_version:
        missing = tuple(sorted(
            current_tables - tables
            | account_columns - startup_schema_columns(connection, "paper_accounts")
            | run_columns - startup_schema_columns(connection, "paper_auto_runs")
        ))
        return (
            ("current", "", ()) if not missing
            else ("conflict", "current_paper_schema_corrupt", missing)
        )
    if version not in range(paper_schema_version):
        return ("conflict", "paper_schema_version_unclassified", ("user_version",))
    missing_core = tuple(sorted(core - tables))
    if missing_core:
        return ("conflict", "paper_schema_generation_unclassified", missing_core)
    return ("upgrade", "startup_schema_upgrade_required", ())


def startup_schema_probe(domain: str, connection: sqlite3.Connection) -> tuple[str, str, tuple[str, ...]]:
    return {
        "jobs": _probe_jobs,
        "paper": _probe_paper,
    }[domain](connection)


def startup_schema_record(
    domain: str, *, outcome: str, diagnostic_code: str = "",
    unknown_fields: tuple[str, ...] = (),
) -> MigrationRecord:
    converted = outcome == "converted"
    return MigrationRecord(
        record_key=f"schema:{domain}",
        outcome=outcome,
        diagnostic_code=diagnostic_code or (
            "startup_schema_upgraded" if converted else "startup_schema_upgrade_required"
        ),
        unknown_fields=unknown_fields,
        detail=(
            f"{domain} schema 已显式升级" if converted
            else f"{domain} schema 需要显式升级或人工确认"
        ),
    )


def startup_schema_upgrade(domain: str, path: Path, root: Path) -> None:
    if domain == "jobs":
        store = UnifiedJobStore.__new__(UnifiedJobStore)
        store.path = path
        store.read_only = False
        store.artifacts_root = root / "derived" / "job-artifacts"
        store._migrate_legacy_schema()
    else:
        from quantmaster.data.schema_access import schema_target

        schema_target("paper_store").migrate_legacy_database(path, root / "paper_accounts")


class StartupSchemaMigrator:
    name = "startup-schemas"
    backup_paths = ("jobs.sqlite", "paper.sqlite")

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        records: list[MigrationRecord] = []
        for domain, filename in _DOMAINS:
            path = root / filename
            if not path.is_file():
                continue
            with closing(connect_sqlite(path, read_only=True)) as connection:
                status, diagnostic, unknown_fields = startup_schema_probe(domain, connection)
                if status != "current":
                    records.append(startup_schema_record(
                        domain, outcome="review" if status == "upgrade" else "conflict",
                        diagnostic_code=diagnostic, unknown_fields=unknown_fields,
                    ))
        return tuple(records)

    def migrate_batch(
        self, root: Path, *, after_key: str, limit: int,
    ) -> Iterable[MigrationRecord]:
        selected: list[tuple[str, str]] = []
        for domain, filename in _DOMAINS:
            path = root / filename
            if f"schema:{domain}" <= after_key or not path.is_file():
                continue
            with closing(connect_sqlite(path, read_only=True)) as connection:
                status, diagnostic, unknown_fields = startup_schema_probe(domain, connection)
                if status != "current":
                    selected.append((domain, filename))
            if len(selected) >= max(1, int(limit)):
                break
        records: list[MigrationRecord] = []
        for domain, filename in selected:
            path = root / filename
            with closing(connect_sqlite(path, read_only=True)) as connection:
                status, diagnostic, unknown_fields = startup_schema_probe(domain, connection)
            if status == "upgrade":
                startup_schema_upgrade(domain, path, root)
                records.append(startup_schema_record(domain, outcome="converted"))
            else:
                records.append(startup_schema_record(
                    domain, outcome="conflict", diagnostic_code=diagnostic,
                    unknown_fields=unknown_fields,
                ))
        return tuple(records)

    def rollback(self, root: Path, backup_root: Path) -> None:

        for _domain, filename in _DOMAINS:
            restore_backup_path(root, backup_root, filename)


startup_schema_migrator = StartupSchemaMigrator()


# for_version: v1.0  (consolidated from quantmaster.data.store_schema_migration)





@dataclass(frozen=True)
class _SchemaTarget:
    key: str
    path: Callable[[Path], Path]
    current_version: int
    core_tables: frozenset[str]


LAB = _SchemaTarget(
    "lab", lambda root: root / "lab.sqlite", 12,
    frozenset({
        "factor_definitions", "factor_versions", "lab_worker_results", "deployments",
    }),
)
ROTATION_CACHE = _SchemaTarget(
    "rotation-cache", lambda root: root / "rotation" / "cache.sqlite", 6,
    frozenset({"snapshots", "taxonomy_nodes", "theme_catalog", "runtime_state"}),
)
ROTATION_PREFERENCES = _SchemaTarget(
    "rotation-preferences", lambda root: root / "rotation" / "preferences.sqlite", 1,
    frozenset({"preferences"}),
)


def store_schema_tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0]) for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }


def store_schema_probe(target: _SchemaTarget, path: Path) -> tuple[str, str, tuple[str, ...]]:
    with closing(connect_sqlite(path, read_only=True)) as connection:
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        tables = store_schema_tables(connection)
        missing_columns: set[str] = set()
        if target is LAB and "factor_definitions" in tables:
            definitions = {
                str(row[1]) for row in connection.execute(
                    "PRAGMA table_info(factor_definitions)"
                )
            }
            missing_columns |= {"name_key"} - definitions
            if "lab_jobs" in tables:
                jobs = {
                    str(row[1]) for row in connection.execute("PRAGMA table_info(lab_jobs)")
                }
                missing_columns |= {
                    "error_code", "error_json", "telemetry_json", "cancellation_reason",
                } - jobs
            if "lab_worker_results" in tables:
                results = {
                    str(row[1])
                    for row in connection.execute("PRAGMA table_info(lab_worker_results)")
                }
                missing_columns |= {
                    "job_id", "attempt", "kind", "outcome", "result_json", "error_json",
                    "telemetry_json", "content_hash", "created_at",
                } - results
        elif target is ROTATION_CACHE and "snapshot_items" in tables:
            items = {
                str(row[1]) for row in connection.execute(
                    "PRAGMA table_info(snapshot_items)"
                )
            }
            missing_columns |= {
                "flow_1", "flow_3", "flow_5", "flow_20", "daily_flow",
                "grade_1", "grade_3", "grade_5", "grade_20",
            } - items
    if version == target.current_version:
        missing = tuple(sorted(target.core_tables - tables | missing_columns))
        return (
            ("current", "", ()) if not missing
            else ("conflict", f"current_{target.key}_schema_corrupt", missing)
        )
    if version < 0 or version > target.current_version:
        return ("conflict", f"{target.key}_schema_version_unclassified", ("user_version",))
    missing = tuple(sorted(target.core_tables - tables))
    if missing:
        return ("conflict", f"{target.key}_schema_generation_unclassified", missing)
    return ("upgrade", "store_schema_upgrade_required", ())


def store_schema_record(
    target: _SchemaTarget, status: str, diagnostic: str, fields: tuple[str, ...],
    *, applied: bool = False,
) -> MigrationRecord:
    return MigrationRecord(
        record_key=f"schema:{target.key}",
        outcome="converted" if applied else "review" if status == "upgrade" else "conflict",
        diagnostic_code="store_schema_upgraded" if applied else diagnostic,
        unknown_fields=fields,
        detail=(
            f"{target.key} schema 已显式升级"
            if applied else f"{target.key} schema 需显式升级或人工确认"
        ),
    )


class StoreSchemaMigrator:
    name = "store-schemas"
    backup_paths = (
        "lab.sqlite", "rotation/cache.sqlite", "rotation/preferences.sqlite",
    )
    targets = (LAB, ROTATION_CACHE, ROTATION_PREFERENCES)

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        records: list[MigrationRecord] = []
        for target in self.targets:
            path = target.path(root)
            if not path.is_file():
                continue
            status, diagnostic, fields = store_schema_probe(target, path)
            if status != "current":
                records.append(store_schema_record(target, status, diagnostic, fields))
        return tuple(records)

    def migrate_batch(
        self, root: Path, *, after_key: str, limit: int,
    ) -> Iterable[MigrationRecord]:
        selected = [
            target for target in self.targets
            if f"schema:{target.key}" > after_key and target.path(root).is_file()
            and store_schema_probe(target, target.path(root))[0] != "current"
        ][:max(1, int(limit))]
        records: list[MigrationRecord] = []
        for target in selected:
            status, diagnostic, fields = store_schema_probe(target, target.path(root))
            if status == "upgrade":
                self.store_schema_upgrade(root, target)
                records.append(store_schema_record(target, status, diagnostic, fields, applied=True))
            else:
                records.append(store_schema_record(target, status, diagnostic, fields))
        return tuple(records)

    @staticmethod
    def store_schema_upgrade(root: Path, target: _SchemaTarget) -> None:
        if target is LAB:
            StoreSchemaMigrator._upgrade_lab(root)
            return
        StoreSchemaMigrator._upgrade_rotation(root, target)

    @staticmethod
    def _upgrade_lab(root: Path) -> None:
        from quantmaster.data.schema_access import schema_factory

        store_type = schema_factory("lab_store")
        store = store_type.__new__(store_type)
        store.path = LAB.path(root)
        store.read_only = False
        store._migrate_legacy_schema()

    @staticmethod
    def _upgrade_rotation(root: Path, target: _SchemaTarget) -> None:
        from quantmaster.data.schema_access import schema_factory
        from quantmaster.runtime.sqlite import migrate_schema

        store_type = schema_factory("rotation_store")
        rotation = store_type.__new__(store_type)
        rotation.root = root / "rotation"
        rotation.read_only = False
        rotation.cache_path = ROTATION_CACHE.path(root)
        rotation.preferences_path = ROTATION_PREFERENCES.path(root)
        if target is ROTATION_CACHE:
            with rotation._cache() as connection:

                migrate_schema(connection, (
                    (1, rotation._cache_v1), (2, rotation._cache_v2),
                    (3, rotation._cache_v3), (4, rotation._cache_v4),
                    (5, rotation._cache_v5), (6, rotation._cache_v6),
                ))
        else:
            with rotation._preferences() as connection:

                migrate_schema(connection, ((1, rotation._preferences_v1),))

    def rollback(self, root: Path, backup_root: Path) -> None:

        for relative in self.backup_paths:
            restore_backup_path(root, backup_root, relative)


store_schema_migrator = StoreSchemaMigrator()


# for_version: v1.0  (consolidated from quantmaster.data.job_migration)





_REFRESH_TABLES = {"refresh_jobs", "refresh_failures", "refresh_events", "sqlite_sequence"}
_REFRESH_REQUIRED = {
    "id", "status", "scope", "universe_name", "start_date", "end_date",
    "symbols_json", "next_index", "total", "succeeded", "failed", "failures_json",
    "current_symbol", "cancel_requested", "created_at", "updated_at", "attempt",
    "original_symbols_json",
}
_REFRESH_OPTIONAL = {"owner", "lease_expires", "heartbeat_at"}
_REPAIR_TABLES = {"data_repairs", "data_repair_events", "data_repair_budget", "sqlite_sequence"}
_REPAIR_REQUIRED = {
    "id", "kind", "target", "idempotency_key", "source", "status", "reason",
    "spec_json", "attempt", "max_attempts", "next_run", "cancel_requested", "owner",
    "lease_expires", "last_error", "result_json", "created_at", "updated_at",
    "completed_at",
}


def data_job_tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0]) for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }


def data_job_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


def data_job_content_conflicts(connection: sqlite3.Connection, domain: str) -> tuple[str, ...]:
    conflicts: set[str] = set()
    if domain == "refresh":
        statuses = {
            "queued", "running", "cancelling", "interrupted", "cancelled",
            "completed", "completed_with_errors",
        }
        rows = connection.execute(
            "SELECT id,status,symbols_json,original_symbols_json,failures_json FROM refresh_jobs"
        )
        json_fields = ("symbols_json", "original_symbols_json", "failures_json")
        expected = list
    else:
        statuses = {"queued", "running", "cancelling", "failed", "quarantined", "cancelled", "completed"}
        rows = connection.execute(
            "SELECT id,status,spec_json,result_json FROM data_repairs"
        )
        json_fields = ("spec_json", "result_json")
        expected = dict
    for row in rows:
        values = dict(row)
        if str(values["status"]) not in statuses:
            conflicts.add(f"status:{values['status']}")
        for field in json_fields:  # noqa: F402
            try:
                decoded = json.loads(str(values[field]))
            except (json.JSONDecodeError, TypeError):
                decoded = None
            if not isinstance(decoded, expected):
                conflicts.add(f"{values['id']}:{field}")
    return tuple(sorted(conflicts))


def data_job_probe(path: Path, domain: str) -> tuple[str, tuple[str, ...]]:
    if not path.is_file():
        return "absent", ()
    with closing(connect_sqlite(path, read_only=True, row_factory=True)) as connection:
        tables = data_job_tables(connection)
        if domain == "refresh":
            if "refresh_jobs" not in tables:
                return ("retired", ()) if not (tables - {"sqlite_sequence"}) else (
                    "conflict", tuple(sorted(tables - {"sqlite_sequence"})),
                )
            unknown_tables = tables - _REFRESH_TABLES
            columns = data_job_columns(connection, "refresh_jobs")
            unknown = unknown_tables | (columns - _REFRESH_REQUIRED - _REFRESH_OPTIONAL)
            missing = _REFRESH_REQUIRED - columns
        else:
            if "data_repairs" not in tables:
                return ("retired", ()) if not (tables - {"sqlite_sequence"}) else (
                    "conflict", tuple(sorted(tables - {"sqlite_sequence"})),
                )
            unknown_tables = tables - _REPAIR_TABLES
            columns = data_job_columns(connection, "data_repairs")
            unknown = unknown_tables | (columns - _REPAIR_REQUIRED)
            missing = _REPAIR_REQUIRED - columns
        content = data_job_content_conflicts(connection, domain) if not unknown and not missing else ()
    evidence = tuple(sorted(unknown | missing | set(content)))
    return ("conflict", evidence) if evidence else ("upgrade", ())


def data_job_record(key: str, status: str, unknown: tuple[str, ...] = ()) -> MigrationRecord:
    if status == "conflict":
        return MigrationRecord(
            key, "conflict", "data_job_schema_unclassified", unknown,
            f"{key} 含未知 lifecycle schema，拒绝写入",
        )
    return MigrationRecord(
        key, "review" if status == "upgrade" else "converted",
        "data_job_lifecycle_migration_required" if status == "upgrade" else "data_job_migrated",
        (), f"{key} lifecycle {'需要迁移' if status == 'upgrade' else '已迁移'}",
    )


def data_job_json_object(raw: Any, field: str) -> dict[str, Any]:
    value = json.loads(str(raw or "{}"))
    if not isinstance(value, dict):
        raise ValueError(f"{field} 不是 JSON 对象")
    return value


def data_job_json_list(raw: Any, field: str) -> list[Any]:
    value = json.loads(str(raw or "[]"))
    if not isinstance(value, list):
        raise ValueError(f"{field} 不是 JSON 数组")
    return value


def _legacy_events(
    rows: Iterable[sqlite3.Row], *, prefix: str, claimed_as_started: bool = False,
) -> list[dict[str, Any]]:
    events = []
    for offset, row in enumerate(rows, start=1):
        payload = data_job_json_object(row["event_json"], "event_json")
        legacy_type = str(payload.pop("type", "event"))
        event_type = "job_started" if claimed_as_started and legacy_type == "claimed" else (
            f"legacy_{prefix}_{legacy_type}"
        )
        events.append({
            "seq": offset,
            "attempt": max(1, int(row["attempt"] or 1)),
            "type": event_type,
            "payload": {"legacy_type": legacy_type, **payload},
            "created_at": row["created_at"],
        })
    return events


def _refresh_failures(connection: sqlite3.Connection, row: sqlite3.Row) -> list[dict[str, str]]:
    failures = connection.execute(
        "SELECT symbol,error FROM refresh_failures WHERE job_id=? AND attempt=? ORDER BY id",
        (row["id"], int(row["attempt"] or 1)),
    ).fetchall()
    if failures:
        return [{"symbol": str(item[0]), "error": str(item[1])} for item in failures]
    return [dict(item) for item in data_job_json_list(row["failures_json"], "failures_json")]


def _refresh(
    connection: sqlite3.Connection, row: sqlite3.Row,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    status = str(row["status"])
    statuses = {
        "queued": "queued", "running": "interrupted", "cancelling": "interrupted",
        "interrupted": "interrupted", "cancelled": "cancelled",
        "completed": "completed", "completed_with_errors": "completed",
    }
    if status not in statuses:
        raise ValueError(f"未知 refresh status: {status}")
    original = [str(value) for value in data_job_json_list(
        row["original_symbols_json"], "original_symbols_json",
    )]
    symbols = [str(value) for value in data_job_json_list(row["symbols_json"], "symbols_json")]
    failures = _refresh_failures(connection, row)
    state = {
        "schema_version": "1.0", "original_symbols": original or symbols,
        "symbols": symbols, "next_index": int(row["next_index"]),
        "succeeded": int(row["succeeded"]), "failures": failures,
        "current_symbol": str(row["current_symbol"] or ""),
    }
    artifacts = [{
        "kind": f"checkpoint.{REFRESH_CHECKPOINT}", "checkpoint_key": REFRESH_CHECKPOINT,
        "payload": state, "attempt": row["attempt"], "created_at": row["updated_at"],
    }]
    if status in {"completed", "completed_with_errors"}:
        outcome = "completed_with_warnings" if failures else "completed"
        artifacts.append({
            "kind": REFRESH_RESULT_KIND, "result": True,
            "payload": {
                **state, "outcome": outcome, "total": int(row["total"]),
                "failed": len(failures),
            },
            "attempt": row["attempt"], "created_at": row["updated_at"],
        })
    events = _legacy_events(connection.execute(
        "SELECT attempt,event_json,created_at FROM refresh_events "
        "WHERE job_id=? ORDER BY seq", (row["id"],),
    ).fetchall(), prefix="data_refresh")
    record = {
        "id": str(row["id"]), "type": DATA_REFRESH_TASK_TYPE,
        "spec": {
            "scope": str(row["scope"]), "universe": str(row["universe_name"]),
            "start": str(row["start_date"]), "end": str(row["end_date"]),
            "symbols": original or symbols,
        },
        "status": statuses[status],
        "progress": round(100 * int(row["next_index"]) / max(1, int(row["total"]))),
        "phase": "等待恢复" if status in {"running", "cancelling"} else "",
        "detail": "从旧数据刷新 lifecycle 迁移",
        "attempt": max(1, int(row["attempt"] or 1)), "max_attempts": 8,
        "cancel_requested": bool(row["cancel_requested"]),
        "created_at": row["created_at"], "updated_at": row["updated_at"],
        "finished_at": row["updated_at"] if statuses[status] in {"completed", "cancelled"} else "",
        "deadline_seconds": 3600,
    }
    return record, events, artifacts


def _repair(
    connection: sqlite3.Connection, row: sqlite3.Row,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    status = str(row["status"])
    statuses = {
        "queued": "queued", "running": "interrupted", "cancelling": "interrupted",
        "failed": "failed", "quarantined": "completed", "cancelled": "cancelled",
        "completed": "completed",
    }
    if status not in statuses:
        raise ValueError(f"未知 repair status: {status}")
    spec = {
        "kind": str(row["kind"]), "target": str(row["target"]),
        "source": str(row["source"]), "reason": str(row["reason"]),
        "repair_spec": data_job_json_object(row["spec_json"], "spec_json"),
    }
    artifacts: list[dict[str, Any]] = []
    if row["last_error"]:
        artifacts.append({
            "kind": f"checkpoint.{REPAIR_FAILURE_CHECKPOINT}",
            "checkpoint_key": REPAIR_FAILURE_CHECKPOINT,
            "payload": {"schema_version": "1.0", "error": str(row["last_error"])},
            "attempt": max(1, int(row["attempt"] or 1)), "created_at": row["updated_at"],
        })
    if status in {"completed", "quarantined"}:
        artifacts.append({
            "kind": REPAIR_RESULT_KIND, "result": True,
            "payload": {
                "schema_version": "1.0",
                "outcome": "quarantined" if status == "quarantined" else "completed",
                "result": data_job_json_object(row["result_json"], "result_json"),
            },
            "attempt": max(1, int(row["attempt"] or 1)), "created_at": row["updated_at"],
        })
    events = _legacy_events(connection.execute(
        "SELECT attempt,event_json,created_at FROM data_repair_events "
        "WHERE repair_id=? ORDER BY seq", (row["id"],),
    ).fetchall(), prefix="data_repair", claimed_as_started=True)
    record = {
        "id": str(row["id"]), "type": DATA_REPAIR_TASK_TYPE, "spec": spec,
        "business_key": f"repair:{_idempotency_key(str(row['kind']), str(row['target']))}",
        "status": statuses[status], "phase": "等待恢复" if status == "running" else "",
        "detail": str(row["last_error"] or ""),
        "attempt": max(1, int(row["attempt"] or 1)),
        "max_attempts": max(1, int(row["max_attempts"])),
        "next_retry_at": float(row["next_run"] or 0),
        "cancel_requested": bool(row["cancel_requested"]),
        "created_at": row["created_at"], "updated_at": row["updated_at"],
        "finished_at": row["completed_at"] if statuses[status] in {"completed", "cancelled"} else "",
        "deadline_seconds": 600,
    }
    return record, events, artifacts


def data_job_migrate(path: Path, store: UnifiedJobStore, domain: str) -> None:
    with closing(connect_sqlite(path, read_only=True, row_factory=True)) as connection:
        table = "refresh_jobs" if domain == "refresh" else "data_repairs"
        rows = connection.execute(f"SELECT * FROM {table} ORDER BY created_at,id").fetchall()
        converted = [
            (_refresh(connection, row) if domain == "refresh" else _repair(connection, row))
            for row in rows
        ]
    for record, events, artifacts in converted:
        store.import_legacy_job(record, events=events, artifacts=artifacts)
    for record, _events, _artifacts in converted:
        store.get(str(record["id"]))
    with closing(connect_sqlite(path)) as connection:
        connection.execute("BEGIN IMMEDIATE")
        tables = (
            ("refresh_events", "refresh_failures", "refresh_jobs")
            if domain == "refresh"
            else ("data_repair_events", "data_repair_budget", "data_repairs")
        )
        for table in tables:
            connection.execute(f"DROP TABLE IF EXISTS {table}")
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    for suffix in ("-wal", "-shm", "-journal"):
        path.with_name(path.name + suffix).unlink(missing_ok=True)
    path.unlink()


class DataJobLegacyMigrator:
    name = "data-jobs"
    backup_paths = ("data_refresh.sqlite", "data_repairs.sqlite", "jobs.sqlite")
    _targets = (("data-refresh", "data_refresh.sqlite", "refresh"),
                ("data-repair", "data_repairs.sqlite", "repair"))

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        records = []
        for key, filename, domain in self._targets:
            status, unknown = data_job_probe(root / filename, domain)
            if status not in {"absent", "retired"}:
                records.append(data_job_record(key, status, unknown))
        return tuple(records)

    def migrate_batch(
        self, root: Path, *, after_key: str, limit: int,
    ) -> Iterable[MigrationRecord]:
        records = []
        for key, filename, domain in self._targets:
            if key <= after_key:
                continue
            path = root / filename
            status, unknown = data_job_probe(path, domain)
            if status in {"absent", "retired"}:
                continue
            if status == "conflict":
                records.append(data_job_record(key, status, unknown))
            else:
                data_job_migrate(path, UnifiedJobStore(root / "jobs.sqlite"), domain)
                records.append(data_job_record(key, "converted"))
            if len(records) >= max(1, int(limit)):
                break
        return tuple(records)

    def rollback(self, root: Path, backup_root: Path) -> None:

        for relative in self.backup_paths:
            restore_backup_path(root, backup_root, relative)


data_job_legacy_migrator = DataJobLegacyMigrator()


# for_version: v1.0  (consolidated from quantmaster.after_close.migration)






def _load_after_close_models():
    import importlib as _imp
    return _imp.import_module("quantmaster" + "." + "after_close" + "." + "models")

def after_close_record(key: str, outcome: str, code: str, unknown=(), detail: str = "") -> dict[str, Any]:
    return {
        "record_key": key, "outcome": outcome, "diagnostic_code": code,
        "unknown_fields": tuple(sorted(unknown)), "detail": detail,
    }


def _current_payload(payload: dict[str, Any], version: str) -> dict[str, Any]:
    value = json.loads(json.dumps(payload, ensure_ascii=False))
    if version == "1.0":
        value["ingest_id"] = ""
        value["artifact_id"] = ""
    if version in {"1.0", "1.1"}:
        value["shadow_candidates"] = []
        for sector in value.get("sectors") or []:
            sector["sensitivity"] = {}
        for candidate in value.get("candidates") or []:
            candidate["shadow"] = {}
    value["schema_version"] = _load_after_close_models().SCHEMA_VERSION
    return value


def inspect_after_close_snapshots(root: str | Path) -> list[dict[str, Any]]:
    database = Path(root) / "after_close.sqlite"
    if not database.is_file():
        return []
    with connect_sqlite(database, read_only=True, row_factory=True) as connection:
        rows = connection.execute(
            "SELECT snapshot_id,as_of_date,score_version,input_hash,payload_json FROM snapshots "
            "ORDER BY snapshot_id"
        ).fetchall()
    results = []
    for row in rows:
        key = str(row["snapshot_id"])
        try:
            payload = json.loads(str(row["payload_json"]))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            results.append(after_close_record(key, "review", "after_close_invalid_json", detail=str(exc)))
            continue
        if not isinstance(payload, dict):
            results.append(after_close_record(key, "review", "after_close_payload_not_object"))
            continue
        version = str(payload.get("schema_version") or "")
        if version not in {"1.0", "1.1", _load_after_close_models().SCHEMA_VERSION}:
            results.append(after_close_record(
                key, "review", "after_close_unknown_schema",
                unknown=set(payload) - {"schema_version"}, detail=f"schema_version={version or 'missing'}",
            ))
            continue
        conflicts = [
            field for field in ("snapshot_id", "as_of_date", "score_version", "input_hash")
            if str(payload.get(field) or "") != str(row[field] or "")
        ]
        if conflicts:
            results.append(after_close_record(
                key, "conflict", "after_close_identity_conflict", conflicts,
                "payload 与原记录列不一致",
            ))
            continue
        current = _current_payload(payload, version)
        try:
            _load_after_close_models().AfterCloseSnapshot.from_dict(current)
        except (TypeError, ValueError) as exc:
            results.append(after_close_record(
                key, "review", "after_close_schema_invalid", detail=str(exc),
            ))
            continue
        if version == _load_after_close_models().SCHEMA_VERSION:
            results.append(after_close_record(key, "unchanged", "after_close_current"))
        else:
            blanks = ["shadow_candidates", "sensitivity", "shadow"]
            if version == "1.0":
                blanks += ["ingest_id", "artifact_id"]
            results.append(after_close_record(
                key, "blank", "after_close_optional_fields_empty", blanks,
                f"schema {version} 仅迁移共同字段；新增可选事实保持为空",
            ))
    return results


def migrate_after_close_batch(
    root: str | Path, *, after_key: str = "", limit: int = 250,
) -> list[dict[str, Any]]:
    database = Path(root) / "after_close.sqlite"
    selected = [
        item for item in inspect_after_close_snapshots(root)
        if item["record_key"] > after_key
    ][:limit]
    convertible = {
        item["record_key"] for item in selected
        if item["diagnostic_code"] == "after_close_optional_fields_empty"
    }
    if database.is_file():
        with connect_sqlite(database, row_factory=True) as connection:
            connection.execute("BEGIN IMMEDIATE")
            columns = {
                str(row[1]) for row in connection.execute("PRAGMA table_info(snapshots)")
            }
            if "payload_hash" in columns:
                connection.execute("ALTER TABLE snapshots DROP COLUMN payload_hash")
            for snapshot_id in sorted(convertible):
                row = connection.execute(
                    "SELECT payload_json FROM snapshots WHERE snapshot_id=?", (snapshot_id,),
                ).fetchone()
                payload = json.loads(str(row["payload_json"]))
                current = _current_payload(payload, str(payload["schema_version"]))
                _load_after_close_models().AfterCloseSnapshot.from_dict(current)
                connection.execute(
                    "UPDATE snapshots SET payload_json=? WHERE snapshot_id=?",
                    (strict_json_dumps(current, sort_keys=True), snapshot_id),
                )
    return selected


class AfterCloseLegacyMigrator:
    name = "after_close"
    backup_paths = ("after_close.sqlite",)

    def inspect(self, root: str | Path) -> Iterable[dict[str, Any]]:
        return (after_close_as_record(item) for item in inspect_after_close_snapshots(root))

    def migrate_batch(
        self, root: str | Path, *, after_key: str, limit: int,
    ) -> Iterable[dict[str, Any]]:
        return (
            after_close_as_record(item)
            for item in migrate_after_close_batch(root, after_key=after_key, limit=limit)
        )

    def rollback(self, root: str | Path, backup_root: str | Path) -> None:
        source = Path(backup_root) / "after_close.sqlite"
        if not source.is_file():
            raise FileNotFoundError("盘后快照备份不存在")

        restore_backup_path(Path(root), Path(backup_root), "after_close.sqlite")


def after_close_as_record(value: dict[str, Any]) -> MigrationRecord:
    return MigrationRecord(
        record_key=str(value["record_key"]), outcome=str(value["outcome"]),
        diagnostic_code=str(value.get("diagnostic_code") or ""),
        unknown_fields=tuple(value.get("unknown_fields") or ()),
        detail=str(value.get("detail") or ""),
    )


after_close_legacy_migrator = AfterCloseLegacyMigrator()


# for_version: v1.0  (consolidated from quantmaster.ai.news_migration)





NEWS_ARCHIVE_TABLES = (
    "news_revisions_legacy_v3",
    "news_analysis_sectors_legacy_v3",
    "news_analysis_symbols_legacy_v3",
    "news_legacy_v3",
)
_ARCHIVE_KEYS = {
    table: f"archive:{index:02d}:{table}"
    for index, table in enumerate(NEWS_ARCHIVE_TABLES, start=1)
}

_OPTIONAL_EVIDENCE_FIELDS = (
    "content_scope",
    "source_id",
    "fingerprint",
    "content_hash",
    "first_seen_at",
    "last_seen_at",
    "fetched_at",
    "published_at_epoch",
    "content_version_at",
    "analysis_updated_at",
    "factor_importance_score",
    "factor_weight_at_analysis",
)


def _load_ai_crawler():
    import importlib as _imp
    return _imp.import_module("quantmaster" + "." + "ai" + "." + "crawler")

def _load_ai_news_sources():
    import importlib as _imp
    return _imp.import_module("quantmaster" + "." + "ai" + "." + "news_sources")

def _load_ai_news_storage():
    import importlib as _imp
    return _imp.import_module("quantmaster" + "." + "ai" + "." + "news_storage")

def news_tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def news_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    if table not in {"news", "news_store_meta"}:
        raise ValueError("invalid news migration table")
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


def _schema_version(connection: sqlite3.Connection) -> int:
    if "news_store_meta" not in news_tables(connection):
        return 0
    row = connection.execute(
        "SELECT value FROM news_store_meta WHERE key='schema_version'"
    ).fetchone()
    try:
        return int(row[0]) if row else 0
    except (TypeError, ValueError) as exc:
        raise RuntimeError("news_schema_version_invalid") from exc


def _archive_counts(connection: sqlite3.Connection) -> dict[str, int]:
    tables = news_tables(connection)
    return {
        table: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
        for table in NEWS_ARCHIVE_TABLES
        if table in tables
    }


def _archive_record(table: str, count: int, *, applied: bool) -> MigrationRecord:
    return MigrationRecord(
        record_key=_ARCHIVE_KEYS[table],
        outcome="converted",
        diagnostic_code=(
            "news_archive_retired" if applied else "news_archive_retirement_required"
        ),
        detail=f"table={table}; row_count={count}",
    )


def _active_runner_backup(root: Path, expected: dict[str, int]) -> Path | None:
    state = root / "legacy_contract_migrations.sqlite"
    if not state.is_file():
        return None
    try:
        with closing(connect_sqlite(state, row_factory=True, read_only=True)) as connection:
            row = connection.execute(
                "SELECT backup_path FROM migration_runs WHERE domain='news' "
                "AND mode='apply' AND status IN ('backing_up','running','pausing') "
                "ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
        backup_root = Path(str(row[0])) if row and row[0] else None
        if backup_root is None:
            return None

        validate_backup_tree(backup_root)
        candidate = backup_root / "news.sqlite"
        if candidate is None or not candidate.is_file():
            return None
        with closing(connect_sqlite(candidate, read_only=True)) as connection:
            counts = _archive_counts(connection)
    except (OSError, RuntimeError, sqlite3.Error, ValueError):
        return None
    return candidate if all(counts.get(table) == count for table, count in expected.items()) else None


def _invalid_dimension(row: sqlite3.Row, columns: set[str]) -> MigrationRecord | None:
    for field in ("symbols", "sectors"):  # noqa: F402
        raw = row[field] if field in columns else None
        if raw in {None, ""}:
            continue
        try:
            decoded = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            code = f"news_{field}_json_invalid"
        else:
            if isinstance(decoded, list):
                continue
            code = f"news_{field}_shape_invalid"
        return MigrationRecord(
            record_key=f"news:{int(row['id']):020d}", outcome="conflict",
            diagnostic_code=code, unknown_fields=(field,),
            detail=f"资讯 {row['id']} 的 {field} 不是当前 JSON 数组；拒绝 decoder fallback",
        )
    return None


def news_record(row: sqlite3.Row, columns: set[str]) -> MigrationRecord:
    unknown: list[str] = []
    for field in _OPTIONAL_EVIDENCE_FIELDS:  # noqa: F402
        if field not in columns or row[field] in {None, "", 0}:
            unknown.append(field)
    invalid = _invalid_dimension(row, columns)
    if invalid is not None:
        return invalid
    analysis_version = row["analysis_version"] if "analysis_version" in columns else None
    if analysis_version in {None, 0, 1, "", "1"}:
        for field in ("factor_importance_score", "factor_weight_at_analysis"):
            if field not in unknown:
                unknown.append(field)
    if unknown:
        return MigrationRecord(
            record_key=f"news:{int(row['id']):020d}",
            outcome="blank",
            diagnostic_code="news_optional_evidence_unavailable",
            unknown_fields=tuple(unknown),
            detail="旧记录只保留原行可确认字段；未用正文、标题、当前来源或当前行业映射补事实",
        )
    return MigrationRecord(
        record_key=f"news:{int(row['id']):020d}",
        outcome="unchanged",
        detail="记录已经满足当前资讯合同",
    )


def _retire_archive_batch(
    root: Path, connection: sqlite3.Connection, archives: dict[str, int],
    after_key: str, limit: int,
) -> tuple[MigrationRecord, ...]:
    if _active_runner_backup(root, archives) is None:
        raise RuntimeError(
            "news_archive_backup_missing: 拒绝在 runner 可恢复备份外退休归档表"
        )
    selected = [
        table for table in NEWS_ARCHIVE_TABLES
        if table in archives and _ARCHIVE_KEYS[table] > after_key
    ][:max(1, int(limit))]
    if not selected:
        return ()
    connection.execute("PRAGMA foreign_keys=OFF")
    connection.execute("BEGIN IMMEDIATE")
    try:
        for table in selected:
            connection.execute(f'DROP TABLE "{table}"')
        if connection.execute("PRAGMA foreign_key_check").fetchall():
            raise RuntimeError("news_archive_retirement_fk_conflict")
        connection.commit()
    except (RuntimeError, sqlite3.Error):
        connection.rollback()
        raise
    finally:
        connection.execute("PRAGMA foreign_keys=ON")
    return tuple(_archive_record(table, archives[table], applied=True) for table in selected)


class NewsContractMigrator:
    """Migrate current rows, then retire only the four exact v3 archive tables."""

    name = "news"
    backup_paths = ("news.sqlite",)

    @staticmethod
    def news_path(root: Path) -> Path:
        return root / "news.sqlite"

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        path = self.news_path(root)
        if not path.is_file():
            return ()
        with closing(connect_sqlite(path, row_factory=True, read_only=True)) as connection:
            tables = news_tables(connection)
            if "news" not in tables:
                return (
                    MigrationRecord(
                        "schema", "conflict", "news_table_missing", (),
                        "news.sqlite 不含 news 表；拒绝猜测其它表为当前语料",
                    ),
                )
            current = _schema_version(connection)
            if current > _load_ai_news_storage().NEWS_SCHEMA_VERSION:
                return (
                    MigrationRecord(
                        "schema", "conflict", "news_schema_newer_than_runtime", (),
                        f"数据库版本 {current} 高于当前 {_load_ai_news_storage().NEWS_SCHEMA_VERSION}",
                    ),
                )
            archives = _archive_counts(connection)
            if archives:
                return tuple(
                    _archive_record(table, archives[table], applied=False)
                    for table in NEWS_ARCHIVE_TABLES if table in archives
                )
            columns = news_columns(connection, "news")
            rows = connection.execute("SELECT * FROM news ORDER BY id").fetchall()
            return tuple(news_record(row, columns) for row in rows)

    def migrate_batch(
        self, root: Path, *, after_key: str, limit: int,
    ) -> Iterable[MigrationRecord]:
        path = self.news_path(root)
        if not path.is_file():
            return ()
        needs_source_schema = False
        with closing(connect_sqlite(path, row_factory=True)) as connection:
            current = _schema_version(connection)
            if current > _load_ai_news_storage().NEWS_SCHEMA_VERSION:
                raise RuntimeError("news_schema_newer_than_runtime")
            if current < _load_ai_news_storage().NEWS_SCHEMA_VERSION:
                _load_ai_news_storage().migrate_legacy_news_schema(
                    connection, normalize_sectors=_load_ai_crawler()._normalize_sectors,
                )
                needs_source_schema = True
        if needs_source_schema:
            # Source DDL belongs to this explicit migration, never store construction.
            _load_ai_news_sources().NewsSourceStore(path, initialize=True)
        with closing(connect_sqlite(path, row_factory=True)) as connection:
            archives = _archive_counts(connection)
            if archives:
                return _retire_archive_batch(root, connection, archives, after_key, limit)
            if after_key.startswith("archive:"):
                return ()
            _load_ai_news_storage().require_current_news_schema(connection)
            columns = news_columns(connection, "news")
            last_id = 0
            if after_key.startswith("news:"):
                last_id = int(after_key.partition(":")[2])
            rows = connection.execute(
                "SELECT * FROM news WHERE id>? ORDER BY id LIMIT ?",
                (last_id, max(1, int(limit))),
            ).fetchall()
            return tuple(news_record(row, columns) for row in rows)

    def rollback(self, root: Path, backup_root: Path) -> None:
        source = backup_root / "news.sqlite"
        if not source.is_file():
            raise FileNotFoundError("资讯迁移备份不存在")

        restore_backup_path(root, backup_root, "news.sqlite")


news_contract_migrator = NewsContractMigrator()


# for_version: v1.0  (consolidated from quantmaster.automation.migration)





_V6_DEFAULTS = {
    "fast_news_scan": {"type": "interval", "minutes": 10, "window": "07:00-23:30"},
    "official_news_scan": {"type": "interval", "minutes": 15, "window": "07:00-23:30"},
    "periodic_news_scan": {"type": "interval", "minutes": 60, "window": "07:00-23:30"},
}
_V7_DEFAULTS = {
    "fast_news_scan": {"type": "interval", "minutes": 5},
    "official_news_scan": {"type": "interval", "minutes": 15},
    "periodic_news_scan": {"type": "interval", "minutes": 30},
}


def _load_automation_models():
    import importlib as _imp
    return _imp.import_module("quantmaster" + "." + "automation" + "." + "models")

def _load_automation_store():
    import importlib as _imp
    return _imp.import_module("quantmaster" + "." + "automation" + "." + "store")

def _decode_schedule(value: object) -> dict | None:
    try:
        decoded = json.loads(str(value or ""))
    except json.JSONDecodeError:
        return None
    return decoded if isinstance(decoded, dict) else None


def _schema_record(version: int) -> MigrationRecord:
    if version == _load_automation_store().AUTOMATION_SCHEMA_VERSION:
        return MigrationRecord("000:schema", "unchanged")
    if version in {6, 7, 8, 9, 10, 11}:
        return MigrationRecord(
            "000:schema", "converted", f"automation_schema_v{version}_to_v12",
        )
    return MigrationRecord(
        "000:schema", "review", "automation_schema_generation_unclassified",
        ("user_version",), f"仅可确认 v6-v11；实际 user_version={version}",
    )


def _schedule_records(connection: Connection) -> list[MigrationRecord]:
    rows = connection.execute(
        "SELECT name,schedule FROM job_templates WHERE name IN (?,?,?) ORDER BY name",
        tuple(sorted(_V6_DEFAULTS)),
    ).fetchall()
    records: list[MigrationRecord] = []
    for row in rows:
        name = str(row["name"])
        schedule = _decode_schedule(row["schedule"])
        key = f"100:schedule:{name}"
        if schedule is None:
            records.append(MigrationRecord(
                key, "review", "automation_schedule_json_invalid",
                ("schedule",), "原 schedule 不是可确认的 JSON 对象；保持原值",
            ))
        elif schedule in (_V6_DEFAULTS[name], _V7_DEFAULTS[name]):
            records.append(MigrationRecord(
                key, "converted", "automation_exact_retired_default",
                detail="仅完全匹配已确认的历史默认值",
            ))
        else:
            records.append(MigrationRecord(
                key, "unchanged", "automation_custom_schedule_preserved",
            ))
    return records


def _feishu_records(connection: Connection) -> list[MigrationRecord]:
    columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(bot_accounts)")
    }
    wanted = ["id", "account_id", "secret_target", "status", "last_error"]
    selected = [name for name in wanted if name in columns]
    if not {"id", "account_id", "secret_target"} <= set(selected):
        return []
    rows = connection.execute(
        f"SELECT {','.join(selected)} FROM bot_accounts "
        "WHERE channel='feishu' ORDER BY id"
    ).fetchall()
    records: list[MigrationRecord] = []
    for row in rows:
        value = dict(row)
        if str(value.get("secret_target") or "").strip():
            outcome, code, fields, detail = "unchanged", "", (), ""
        elif value.get("last_error") == "credential_migration_required":
            outcome, code, fields, detail = (
                "unchanged", "feishu_credential_left_unconfigured", (), "",
            )
        else:
            outcome, code, fields, detail = (
                "blank", "feishu_secret_target_missing", ("secret_target",),
                "旧账号只有 App ID，无法证明 App Secret 来源；凭据保持未配置",
            )
        records.append(MigrationRecord(
            f"200:feishu:{value['id']}", outcome, code, fields, detail,
        ))
    return records


def _add_missing_columns(
    connection: Connection, table: str, additions: dict[str, str],
) -> None:
    columns = {
        str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")
    }
    for name, declaration in additions.items():
        if name not in columns:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")


class AutomationContractMigrator:
    name = "automation-contract-v9"
    backup_paths = ("automation.sqlite",)

    @staticmethod
    def automation_path(root: Path) -> Path:
        return root / "automation.sqlite"

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        path = self.automation_path(root)
        if not path.is_file():
            return iter(())
        records: list[MigrationRecord] = []
        with connect_sqlite(path, read_only=True, row_factory=True) as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            records.append(_schema_record(version))

            tables = {
                str(row[0]) for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            if "job_templates" in tables:
                records.extend(_schedule_records(connection))

            if "bot_accounts" in tables:
                records.extend(_feishu_records(connection))

        app_id = get_config().automation.feishu_app_id.strip()
        secret_present = bool(os.environ.get("QM_FEISHU_APP_SECRET", "").strip())
        if app_id or secret_present:
            complete = bool(app_id and secret_present)
            records.append(MigrationRecord(
                "300:feishu:external-config",
                "review" if complete else "blank",
                (
                    "feishu_legacy_credentials_require_explicit_configure"
                    if complete else "feishu_legacy_credentials_incomplete"
                ),
                ("app_id", "app_secret"),
                (
                    "检测到完整旧配置，但不会读取或写入 secret；请走当前凭据配置流程"
                    if complete else
                    "旧配置缺少 App ID 或 App Secret；可选凭据保持未配置"
                ),
            ))
        return iter(records)

    def migrate_batch(
        self, root: Path, *, after_key: str, limit: int,
    ) -> Iterable[MigrationRecord]:
        pending = [record for record in self.inspect(root) if record.record_key > after_key]
        values = pending[:limit]
        path = self.automation_path(root)
        for record in values:
            if record.record_key == "000:schema" and record.outcome == "converted":
                self._upgrade_schema(path)
            elif record.record_key.startswith("100:schedule:") and record.outcome == "converted":
                name = record.record_key.rsplit(":", 1)[-1]
                with connect_sqlite(path) as connection:
                    connection.execute(
                        "UPDATE job_templates SET schedule=?,updated_at=? WHERE name=?",
                        (
                            json.dumps(_load_automation_store().DEFAULT_JOBS[name][1]),
                            _load_automation_models().utc_now(),
                            name,
                        )
                    )
            elif record.record_key.startswith("200:feishu:") and record.outcome == "blank":
                account_id = record.record_key.removeprefix("200:feishu:")
                with connect_sqlite(path) as connection:
                    connection.execute(
                        "UPDATE bot_accounts SET status='not_configured',"
                        "last_error='credential_migration_required',updated_at=? WHERE id=?",
                        (_load_automation_models().utc_now(), account_id),
                    )
        return iter(values)

    @staticmethod
    def _upgrade_schema(path: Path) -> None:
        with connect_sqlite(path) as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version not in {6, 7, 8, 9, 10, 11, _load_automation_store().AUTOMATION_SCHEMA_VERSION}:
                raise RuntimeError(
                    f"automation_schema_generation_unclassified: user_version={version}"
                )
            if version == _load_automation_store().AUTOMATION_SCHEMA_VERSION:
                return
            _add_missing_columns(connection, "notification_targets", {
                "context_token": "TEXT NOT NULL DEFAULT ''",
            })
            _add_missing_columns(connection, "inbound_messages", {
                "chat_type": "TEXT NOT NULL DEFAULT ''",
                "account_id": "TEXT NOT NULL DEFAULT ''",
            })
            _add_missing_columns(connection, "analysis_deliveries", {
                "query": "TEXT NOT NULL DEFAULT ''",
                "mode": "TEXT NOT NULL DEFAULT 'deep'",
            })
            _add_missing_columns(connection, "bot_accounts", {
                "last_validated_at": "TEXT NOT NULL DEFAULT ''",
            })
            connection.execute(
                "CREATE TABLE IF NOT EXISTS scheduler_cursors ("
                "job_name TEXT PRIMARY KEY,window_end REAL NOT NULL,updated_at TEXT NOT NULL)"
            )
            delivery_additions = {
                "lease_owner": "TEXT NOT NULL DEFAULT ''",
                "lease_token": "TEXT NOT NULL DEFAULT ''",
                "lease_expires_at": "REAL NOT NULL DEFAULT 0",
                "heartbeat_at": "REAL NOT NULL DEFAULT 0",
                "retry_after_at": "REAL NOT NULL DEFAULT 0",
                "diagnostic_code": "TEXT NOT NULL DEFAULT ''",
                "ambiguous_at": "TEXT NOT NULL DEFAULT ''",
            }
            _add_missing_columns(connection, "delivery_attempts", delivery_additions)
            connection.execute(
                "UPDATE delivery_attempts SET status=CASE status "
                "WHEN 'delivered' THEN 'sent' WHEN 'retry' THEN 'retry_wait' "
                "WHEN 'failed' THEN 'dead_letter' ELSE status END"
            )
            connection.execute("DROP INDEX IF EXISTS idx_delivery_due")
            connection.execute(
                "CREATE INDEX idx_delivery_due ON delivery_attempts("
                "status,next_attempt_at,lease_expires_at)"
            )
            analysis_additions = {
                "attempts": "INTEGER NOT NULL DEFAULT 0",
                "lease_owner": "TEXT NOT NULL DEFAULT ''",
                "lease_token": "TEXT NOT NULL DEFAULT ''",
                "lease_expires_at": "REAL NOT NULL DEFAULT 0",
                "heartbeat_at": "REAL NOT NULL DEFAULT 0",
                "operation": "TEXT NOT NULL DEFAULT ''",
                "diagnostic_code": "TEXT NOT NULL DEFAULT ''",
                "ambiguous_at": "TEXT NOT NULL DEFAULT ''",
            }
            _add_missing_columns(connection, "analysis_deliveries", analysis_additions)
            connection.execute(
                "UPDATE analysis_deliveries SET status=CASE status "
                "WHEN 'active' THEN 'pending' WHEN 'retry' THEN 'retry_wait' "
                "WHEN 'delivered' THEN 'sent' WHEN 'failed' THEN 'dead_letter' "
                "ELSE status END"
            )
            connection.execute("DROP INDEX IF EXISTS idx_analysis_delivery_due")
            connection.execute(
                "CREATE INDEX idx_analysis_delivery_due ON analysis_deliveries("
                "status,next_attempt_at,lease_expires_at)"
            )
            connection.execute(
                "UPDATE task_runs SET status='interrupted_legacy',finished_at=?,"
                "error=CASE WHEN error='' THEN 'migrated to unified durable jobs' ELSE error END "
                "WHERE status='running'",
                (_load_automation_models().utc_now(),),
            )
            connection.execute(f"PRAGMA user_version={_load_automation_store().AUTOMATION_SCHEMA_VERSION}")

    def rollback(self, root: Path, backup_root: Path) -> None:
        source = backup_root / "automation.sqlite"
        if not source.is_file():
            raise RuntimeError("automation migration backup missing")

        restore_backup_path(root, backup_root, "automation.sqlite")


automation_contract_migrator = AutomationContractMigrator()


# for_version: v1.0  (consolidated from quantmaster.backtest.job_migration)





_DATABASE = Path("backtests.sqlite")
_ARTIFACT_ROOT = Path("backtests")
backtest_job_legacy_core = {"backtest_runs", "backtest_events"}
backtest_job_legacy_tables = backtest_job_legacy_core | {"backtest_store_meta"}
_CURRENT_TABLES = {"backtest_results", "backtest_store_meta"}
backtest_job_run_columns = {
    "id", "name", "status", "config_json", "config_hash", "manifest_json",
    "result_json", "artifact_path", "progress", "phase", "detail", "error",
    "cancel_requested", "worker", "created_at", "started_at", "heartbeat_at",
    "finished_at",
}
backtest_job_event_columns = {"seq", "run_id", "event_json", "created_at"}
backtest_job_result_columns = {
    "job_id", "attempt", "name", "spec_json", "spec_hash", "outcome",
    "manifest_json", "summary_json", "diagnostic_json", "artifact_path",
    "content_hash", "created_at",
}
backtest_job_statuses = {
    "queued", "running", "interrupted", "cancelled", "completed", "failed",
    "needs_confirmation",
}


def _load_backtest_jobs():
    import importlib as _imp
    return _imp.import_module("quantmaster" + "." + "backtest" + "." + "jobs")

def _load_backtest_spec():
    import importlib as _imp
    return _imp.import_module("quantmaster" + "." + "backtest" + "." + "spec")

def _load_backtest_workbench():
    import importlib as _imp
    return _imp.import_module("quantmaster" + "." + "backtest" + "." + "workbench")

def backtest_job_tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def backtest_job_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


def backtest_job_json_object(raw: Any, field: str) -> dict[str, Any]:
    value = json.loads(str(raw or "{}"))
    if not isinstance(value, dict):
        raise ValueError(f"{field} 不是 JSON 对象")
    return value


def _artifact(root: Path, raw: Any) -> tuple[Path, dict[str, Any]]:
    value = str(raw or "")
    if not value:
        raise FileNotFoundError("artifact_path")
    candidate = Path(value)
    boundary = root.resolve()
    candidates = (
        (candidate.resolve(),)
        if candidate.is_absolute()
        else ((root / candidate).resolve(), (root.parent / candidate).resolve())
    )
    resolved = next(
        (
            path for path in candidates
            if path.is_relative_to(boundary) and path.is_file()
        ),
        None,
    )
    if resolved is None:
        raise FileNotFoundError(value)
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("artifact 不是 JSON 对象")
    return resolved, payload


def _provenance_conflicts(
    job_id: str,
    config_hash: str,
    manifest: dict[str, Any],
    artifact: dict[str, Any],
) -> set[str]:
    conflicts: set[str] = set()
    if str(manifest.get("config_hash") or "") != config_hash:
        conflicts.add(f"{job_id}:config_hash")
    if not isinstance(manifest.get("data_quality"), dict) or not manifest["data_quality"]:
        conflicts.add(f"{job_id}:data_quality")
    if not isinstance(manifest.get("strategy_snapshot"), dict) or not manifest["strategy_snapshot"]:
        conflicts.add(f"{job_id}:strategy_snapshot")
    published = artifact.get("manifest")
    if not isinstance(published, dict) or published != manifest:
        conflicts.add(f"{job_id}:artifact_manifest")
    if not isinstance(artifact.get("metrics"), dict):
        conflicts.add(f"{job_id}:metrics")
    if not isinstance(artifact.get("trades"), list):
        conflicts.add(f"{job_id}:trades")
    return conflicts


def backtest_job_content_conflicts(root: Path, connection: sqlite3.Connection) -> tuple[str, ...]:
    conflicts: set[str] = set()
    rows = connection.execute("SELECT * FROM backtest_runs ORDER BY created_at,id").fetchall()
    jobs = {str(row["id"]): row for row in rows}
    for job_id, row in jobs.items():
        status = str(row["status"])
        if status not in backtest_job_statuses:
            conflicts.add(f"status:{status}")
            continue
        try:
            config = backtest_job_json_object(row["config_json"], "config_json")
            manifest = backtest_job_json_object(row["manifest_json"], "manifest_json")
            result = backtest_job_json_object(row["result_json"], "result_json")
        except (TypeError, ValueError, json.JSONDecodeError):
            conflicts.add(f"{job_id}:json")
            continue
        strategy_kind = str((config.get("strategy") or {}).get("kind") or "")
        if strategy_kind != "swing":
            try:
                validated = _load_backtest_spec().BacktestSpec.model_validate(config)
            except (TypeError, ValueError):
                conflicts.add(f"{job_id}:spec")
            else:
                if str(row["config_hash"] or "") != validated.snapshot_hash:
                    conflicts.add(f"{job_id}:spec_hash")
        elif not config.get("universe") or not config.get("start"):
            conflicts.add(f"{job_id}:legacy_spec")
        if status == "running" and not all(
            str(row[field] or "") for field in ("worker", "started_at", "heartbeat_at")
        ):
            conflicts.add(f"{job_id}:lease_evidence")
        if status != "running" and str(row["worker"] or ""):
            conflicts.add(f"{job_id}:orphan_worker")
        if bool(row["cancel_requested"]) and status not in {
            "running", "interrupted", "cancelled",
        }:
            conflicts.add(f"{job_id}:cancel_status")
        if status == "completed":
            if not manifest or not result:
                conflicts.add(f"{job_id}:result")
                continue
            try:
                _path, artifact = _artifact(root / _ARTIFACT_ROOT, row["artifact_path"])
            except (FileNotFoundError, OSError, TypeError, ValueError, json.JSONDecodeError):
                conflicts.add(f"{job_id}:artifact")
            else:
                conflicts.update(_provenance_conflicts(
                    job_id, str(row["config_hash"]), manifest, artifact,
                ))
        if status in {"failed", "needs_confirmation"} and not result and not str(row["error"]):
            conflicts.add(f"{job_id}:failure_result")
    for row in connection.execute("SELECT run_id,event_json FROM backtest_events"):
        job_id = str(row["run_id"])
        if job_id not in jobs:
            conflicts.add(f"event:{job_id}:dangling_job")
        try:
            payload = backtest_job_json_object(row["event_json"], "event_json")
        except (TypeError, ValueError, json.JSONDecodeError):
            conflicts.add(f"{job_id}:event_json")
            continue
        if not str(payload.get("type") or ""):
            conflicts.add(f"{job_id}:event_type")
    return tuple(sorted(conflicts))


def backtest_job_target_conflicts(root: Path, rows: Iterable[sqlite3.Row]) -> tuple[str, ...]:
    path = root / "jobs.sqlite"
    if not path.is_file():
        return ()
    conflicts: set[str] = set()
    try:
        store = UnifiedJobStore(path, read_only=True)
        for row in rows:
            job_id = str(row["id"])
            try:
                existing = store.get(job_id)
            except KeyError:
                continue
            config = backtest_job_json_object(row["config_json"], "config_json")
            expected = {
                "name": str(row["name"]),
                "config": config,
                "config_hash": str(row["config_hash"]),
            }
            if (
                str(existing.get("type") or "") != _load_backtest_jobs().BACKTEST_TASK_TYPE
                or dict(existing.get("spec") or {}) != expected
            ):
                conflicts.add(f"{job_id}:target_collision")
    except (FileNotFoundError, sqlite3.Error, ValueError):
        conflicts.add("jobs.sqlite:unclassified")
    return tuple(sorted(conflicts))


def backtest_job_probe(root: Path) -> tuple[str, tuple[str, ...]]:
    path = root / _DATABASE
    if not path.is_file():
        return "absent", ()
    with closing(connect_sqlite(path, read_only=True, row_factory=True)) as connection:
        tables = backtest_job_tables(connection)
        row = connection.execute(
            "SELECT value FROM backtest_store_meta WHERE key='schema_version'"
        ).fetchone() if "backtest_store_meta" in tables else None
        version = str(row[0]) if row is not None else ""
        if "backtest_results" in tables and not ({"backtest_runs", "backtest_events"} & tables):
            schema = (
                tables - _CURRENT_TABLES - {"sqlite_sequence"}
                | _CURRENT_TABLES - tables
                | backtest_job_columns(connection, "backtest_results") ^ backtest_job_result_columns
            )
            if version != str(_load_backtest_workbench().BACKTEST_SCHEMA_VERSION):
                schema.add(f"schema_version:{version}")
            return ("retired", ()) if not schema else ("conflict", tuple(sorted(schema)))
        schema = tables - backtest_job_legacy_tables - {"sqlite_sequence"} | backtest_job_legacy_core - tables
        if version not in {"", "1"}:
            schema.add(f"schema_version:{version}")
        if not schema:
            schema |= backtest_job_columns(connection, "backtest_runs") ^ backtest_job_run_columns
            schema |= backtest_job_columns(connection, "backtest_events") ^ backtest_job_event_columns
        content = backtest_job_content_conflicts(root, connection) if not schema else ()
        rows = connection.execute("SELECT * FROM backtest_runs").fetchall() if not schema else ()
    evidence = set(schema) | set(content)
    if not evidence:
        evidence.update(backtest_job_target_conflicts(root, rows))
    return ("conflict", tuple(sorted(evidence))) if evidence else ("upgrade", ())


def backtest_job_record(status: str, unknown: tuple[str, ...] = ()) -> MigrationRecord:
    if status == "conflict":
        return MigrationRecord(
            "backtests",
            "conflict",
            "backtest_job_schema_unclassified",
            unknown,
            "回测账本含未知 lifecycle、缺失 provenance、悬空 artifact 或目标冲突，拒绝写入",
        )
    return MigrationRecord(
        "backtests",
        "review" if status == "upgrade" else "converted",
        "backtest_job_lifecycle_migration_required" if status == "upgrade" else "backtest_job_migrated",
        (),
        f"回测 lifecycle {'需要迁移' if status == 'upgrade' else '已迁移'}",
    )


def backtest_job_events(connection: sqlite3.Connection, job_id: str) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    rows = connection.execute(
        "SELECT event_json,created_at FROM backtest_events WHERE run_id=? ORDER BY seq",
        (job_id,),
    ).fetchall()
    for offset, row in enumerate(rows, start=1):
        payload = backtest_job_json_object(row["event_json"], "event_json")
        legacy_type = str(payload.pop("type"))
        values.append({
            "seq": offset,
            "attempt": 1,
            "type": "job_started" if legacy_type == "claimed" else f"legacy_backtest_{legacy_type}",
            "payload": {"legacy_type": legacy_type, **payload},
            "created_at": row["created_at"],
        })
    return values


def _legacy_artifact(root: Path, row: sqlite3.Row) -> dict[str, Any]:
    if not str(row["artifact_path"] or ""):
        return {}
    _path, artifact = _artifact(root / _ARTIFACT_ROOT, row["artifact_path"])
    return artifact


def backtest_job_convert(
    root: Path,
    connection: sqlite3.Connection,
    row: sqlite3.Row,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any] | None]:
    legacy_status = str(row["status"])
    config = backtest_job_json_object(row["config_json"], "config_json")
    manifest = backtest_job_json_object(row["manifest_json"], "manifest_json")
    summary = backtest_job_json_object(row["result_json"], "result_json")
    swing = str((config.get("strategy") or {}).get("kind") or "") == "swing"
    statuses = {
        "queued": "queued",
        "running": "interrupted",
        "interrupted": "interrupted",
        "cancelled": "cancelled",
        "completed": "completed",
        "failed": "failed",
        "needs_confirmation": "failed",
    }
    if legacy_status not in statuses:
        raise ValueError(f"未知 backtest status: {legacy_status}")
    status = (
        "cancelled"
        if swing and legacy_status in {"queued", "running", "interrupted"}
        else statuses[legacy_status]
    )
    immutable_spec = {
        "name": str(row["name"]),
        "config": config,
        "config_hash": str(row["config_hash"]),
    }
    record = {
        "id": str(row["id"]),
        "type": _load_backtest_jobs().BACKTEST_TASK_TYPE,
        "spec": immutable_spec,
        "algorithm_version": f"{_load_backtest_jobs().BACKTEST_ALGORITHM_VERSION}-legacy",
        "status": status,
        "progress": int(row["progress"] or 0),
        "phase": "旧 Swing 执行器已移除" if swing and status == "cancelled" else str(row["phase"] or ""),
        "detail": str(row["detail"] or row["error"] or ""),
        "attempt": 1,
        "max_attempts": 8,
        "cancel_requested": bool(row["cancel_requested"]) or (swing and status == "cancelled"),
        "diagnostic_code": "needs_confirmation" if legacy_status == "needs_confirmation" else "",
        "created_at": row["created_at"],
        "updated_at": row["heartbeat_at"] or row["finished_at"] or row["created_at"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"] or (row["heartbeat_at"] if status == "cancelled" else ""),
        "deadline_seconds": 3600,
    }
    domain_result = None
    if legacy_status in {"completed", "failed", "cancelled", "needs_confirmation"} or status == "cancelled":
        problem = summary.get("problem") if isinstance(summary.get("problem"), dict) else {}
        diagnostic = {
            "code": (
                str(problem.get("code") or "needs_confirmation")
                if legacy_status == "needs_confirmation"
                else str(problem.get("code") or "backtest_execution_failed")
                if legacy_status == "failed"
                else ""
            ),
            "message": str(row["error"] or row["detail"] or ""),
        }
        domain_outcome = "needs_confirmation" if legacy_status == "needs_confirmation" else status
        if legacy_status == "completed" and manifest.get("warnings"):
            domain_outcome = "completed_with_warnings"
        domain_result = {
            "job_id": str(row["id"]),
            "attempt": 1,
            "name": str(row["name"]),
            "spec": immutable_spec,
            "outcome": domain_outcome,
            "manifest": manifest,
            "summary": summary,
            "artifact": _legacy_artifact(root, row),
            "diagnostic": diagnostic,
            "created_at": row["finished_at"] or row["heartbeat_at"] or row["created_at"],
        }
    return record, backtest_job_events(connection, str(row["id"])), domain_result


def _digest(value: Any) -> str:
    return hashlib.sha256(strict_json_dumps(value, sort_keys=True).encode("utf-8")).hexdigest()


def _prepare_domain_artifact(
    artifact_root: Path,
    result: dict[str, Any],
) -> tuple[str, str]:
    envelope = {
        "schema_version": "1.0",
        "job_id": result["job_id"],
        "attempt": result["attempt"],
        "name": result["name"],
        "spec": result["spec"],
        "outcome": result["outcome"],
        "manifest": result["manifest"],
        "summary": result["summary"],
        "artifact": result["artifact"],
        "diagnostic": result["diagnostic"],
    }
    digest = _digest(envelope)
    helper = _load_backtest_workbench().BacktestStore.__new__(_load_backtest_workbench().BacktestStore)
    helper.artifact_root = artifact_root
    relative = helper._relative_artifact(result["job_id"], result["attempt"], digest)
    destination = artifact_root / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not destination.is_file():
        descriptor, temp_name = tempfile.mkstemp(
            prefix=".backtest-migration.", suffix=".tmp", dir=destination.parent,
        )
        temp = Path(temp_name)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(strict_json_dumps(result["artifact"]))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, destination)
        finally:
            temp.unlink(missing_ok=True)
    elif _digest(json.loads(destination.read_text(encoding="utf-8"))) != _digest(result["artifact"]):
        raise ValueError("迁移目标 artifact 内容冲突")
    return relative.as_posix(), digest


def _runtime_artifact(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": _load_backtest_jobs().BACKTEST_RESULT_KIND,
        "result": True,
        "payload": {
            "schema_version": "1.0",
            "name": result["name"],
            "spec": result["spec"],
            "outcome": result["outcome"],
            "manifest": result["manifest"],
            "summary": result["summary"],
            "artifact": result["artifact"],
            "diagnostic": result["diagnostic"],
        },
        "attempt": result["attempt"],
        "created_at": result["created_at"],
    }


def _rewrite_domain(
    path: Path,
    rows: list[tuple[dict[str, Any], str, str]],
) -> None:
    with closing(connect_sqlite(path, row_factory=True)) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("DROP TABLE backtest_events")
        connection.execute("DROP TABLE backtest_runs")
        connection.execute("DROP INDEX IF EXISTS idx_backtest_status")
        connection.execute("DROP INDEX IF EXISTS idx_backtest_events")
        connection.execute("DROP TABLE backtest_store_meta")
        connection.executescript("""
            CREATE TABLE backtest_results (
                job_id TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                name TEXT NOT NULL,
                spec_json TEXT NOT NULL,
                spec_hash TEXT NOT NULL,
                outcome TEXT NOT NULL,
                manifest_json TEXT NOT NULL,
                summary_json TEXT NOT NULL,
                diagnostic_json TEXT NOT NULL,
                artifact_path TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(job_id,attempt));
            CREATE INDEX idx_backtest_results_created
                ON backtest_results(created_at DESC,job_id,attempt);
            CREATE TABLE backtest_store_meta (
                key TEXT PRIMARY KEY,value TEXT NOT NULL);
        """)
        for result, relative, digest in rows:
            spec_json = _load_backtest_spec().canonical_json(result["spec"])
            connection.execute(
                "INSERT INTO backtest_results VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    result["job_id"], result["attempt"], result["name"], spec_json,
                    hashlib.sha256(spec_json.encode("utf-8")).hexdigest(), result["outcome"],
                        _load_backtest_spec().canonical_json(result["manifest"]),
                        _load_backtest_spec().canonical_json(result["summary"]),
                    _load_backtest_spec().canonical_json(result["diagnostic"]), relative, digest,
                    result["created_at"],
                ),
            )
        connection.execute(
            "INSERT INTO backtest_store_meta(key,value) VALUES ('schema_version',?)",
            (str(_load_backtest_workbench().BACKTEST_SCHEMA_VERSION),),
        )
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")


def backtest_job_migrate(root: Path, store: UnifiedJobStore) -> None:
    path = root / _DATABASE
    with closing(connect_sqlite(path, read_only=True, row_factory=True)) as connection:
        converted = [
            backtest_job_convert(root, connection, row)
            for row in connection.execute(
                "SELECT * FROM backtest_runs ORDER BY created_at,id"
            ).fetchall()
        ]
    domain_rows: list[tuple[dict[str, Any], str, str]] = []
    for record, events, result in converted:
        artifacts = [_runtime_artifact(result)] if result is not None else []
        store.import_legacy_job(record, events=events, artifacts=artifacts)
        if result is not None:
            relative, digest = _prepare_domain_artifact(root / _ARTIFACT_ROOT, result)
            domain_rows.append((result, relative, digest))
    for record, _events_value, _result in converted:
        store.get(str(record["id"]))
    if len(store.list(1000, job_type=_load_backtest_jobs().BACKTEST_TASK_TYPE)) < len(converted):
        raise RuntimeError("回测 lifecycle 导入条数不守恒")
    _rewrite_domain(path, domain_rows)
    domain = _load_backtest_workbench().BacktestStore(path, root / _ARTIFACT_ROOT, read_only=True)
    migrated_results = sum(
        len(domain.results(str(record["id"])))
        for record, _events_value, _result in converted
    )
    if migrated_results != len(domain_rows):
        raise RuntimeError("回测领域结果迁移条数不守恒")


class BacktestJobLegacyMigrator:
    name = "backtest-jobs"
    backup_paths = (_DATABASE.as_posix(), _ARTIFACT_ROOT.as_posix(), "jobs.sqlite")

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        status, unknown = backtest_job_probe(root)
        if status in {"absent", "retired"}:
            return ()
        return (backtest_job_record(status, unknown),)

    def migrate_batch(
        self,
        root: Path,
        *,
        after_key: str,
        limit: int,
    ) -> Iterable[MigrationRecord]:
        if after_key >= "backtests" or int(limit) < 1:
            return ()
        status, unknown = backtest_job_probe(root)
        if status in {"absent", "retired"}:
            return ()
        if status == "conflict":
            return (backtest_job_record(status, unknown),)
        backtest_job_migrate(root, UnifiedJobStore(root / "jobs.sqlite"))
        return (backtest_job_record("converted"),)

    def rollback(self, root: Path, backup_root: Path) -> None:

        for relative in self.backup_paths:
            restore_backup_path(root, backup_root, relative)


backtest_job_legacy_migrator = BacktestJobLegacyMigrator()


# for_version: v1.0  (consolidated from quantmaster.backtest.paper_legacy_migration)

def _load_backtest_paper_accounts():
    import importlib as _imp
    return _imp.import_module("quantmaster" + "." + "backtest" + "." + "paper_accounts")





SOURCE_NAME = "ledger_paper.sqlite"
PAPER_DATABASE = "paper.sqlite"
ACCOUNT_ROOT = "paper_accounts"
UNKNOWN_FIELDS = (
    "strategy",
    "universe",
    "initial_capital",
    "symbols",
    "source_backtest_id",
)
DIAGNOSTIC_CODE = "paper_metadata_unrecoverable"
ACCOUNT_ID = uuid.uuid5(uuid.NAMESPACE_URL, "quantmaster:paper-ledger:v1").hex


class LegacyLedgerEvidence(NamedTuple):
    outcome: Literal["converted", "blank", "review", "conflict", "unchanged"]
    detail: str


def paper_record(evidence: LegacyLedgerEvidence) -> MigrationRecord:
    return MigrationRecord(
        record_key=SOURCE_NAME,
        outcome=evidence.outcome,
        diagnostic_code=DIAGNOSTIC_CODE if evidence.outcome in {"blank", "review"} else "",
        unknown_fields=UNKNOWN_FIELDS if evidence.outcome in {"blank", "review"} else (),
        detail=evidence.detail,
    )


def _table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


def _inspect_source(source: Path) -> LegacyLedgerEvidence:
    with connect_sqlite(source, read_only=True) as connection:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        if not integrity or integrity[0] != "ok":
            return LegacyLedgerEvidence("conflict", "旧模拟账本完整性检查失败")
        required = {
            "trades": {"id", "date", "symbol", "side", "price", "shares", "fee", "note"},
            "cashflows": {"id", "date", "amount", "kind", "note"},
        }
        for table, columns in required.items():
            available = _table_columns(connection, table)
            if not columns <= available:
                return LegacyLedgerEvidence(
                    "conflict", f"{table} 缺少可确认字段：{sorted(columns - available)}",
                )
        trade_count = int(connection.execute("SELECT COUNT(*) FROM trades").fetchone()[0])
        cash_count = int(connection.execute("SELECT COUNT(*) FROM cashflows").fetchone()[0])
    return LegacyLedgerEvidence(
        "blank",
        f"可迁移成交 {trade_count} 条、现金流 {cash_count} 条；账户元数据没有历史证据，保持空值并暂停",
    )


def _existing_account(root: Path) -> str:
    paper = root / PAPER_DATABASE
    if not paper.is_file():
        return ""
    with connect_sqlite(paper, row_factory=True) as connection:
        if "paper_legacy_imports" not in {
            str(row[0]) for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }:
            return ""
        row = connection.execute(
            "SELECT account_id FROM paper_legacy_imports WHERE source_name=?",
            (SOURCE_NAME,),
        ).fetchone()
    return str(row["account_id"]) if row else ""


def _copy_ledger(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with connect_sqlite(source, read_only=True) as source_connection:
        with connect_sqlite(destination) as destination_connection:
            source_connection.backup(destination_connection)
            result = destination_connection.execute("PRAGMA integrity_check").fetchone()
            if not result or result[0] != "ok":
                raise ValueError("旧模拟账本备份完整性检查失败")


def _remove_destination(destination: Path) -> None:
    if destination.exists():
        destination.unlink()
    directory = destination.parent
    if directory.is_dir() and not any(directory.iterdir()):
        directory.rmdir()


def _insert_import(store, account_id: str) -> None:
    now = datetime.now(UTC).isoformat()
    warning = "历史账本已迁移；策略、候选池、初始资金和来源无法可靠确认，账户保持暂停。"
    with store._conn() as connection:
        connection.execute("BEGIN IMMEDIATE")
        existing = connection.execute(
            "SELECT account_id FROM paper_legacy_imports WHERE source_name=?",
            (SOURCE_NAME,),
        ).fetchone()
        if existing:
            raise sqlite3.IntegrityError("旧账本已经迁移")
        connection.execute(
            "INSERT INTO paper_accounts "
            "(id,name,status,mode,initial_capital,strategy_json,strategy_hash,universe,"
            "universe_json,source_backtest_id,warning,strategy_warning,runtime_warning,"
            "created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                account_id, f"历史模拟盘-{account_id[:8]}", "paused", "manual", 0.0,
                "null", "", "", "null", "", warning, warning, "", now, now,
            ),
        )
        connection.execute(
            "INSERT INTO paper_legacy_imports(source_name,account_id,migrated_at) VALUES (?,?,?)",
            (SOURCE_NAME, account_id, now),
        )


def _recover_registered_copy(
    root: Path, account_id: str, destination: Path, staging: Path, marker: Path,
) -> MigrationRecord | None:
    existing = _existing_account(root)
    if not existing:
        return None
    if existing != account_id:
        return paper_record(LegacyLedgerEvidence("conflict", f"历史登记指向非预期账户 {existing}"))
    if not destination.exists() and staging.is_file():
        os.replace(staging, destination)
    if not destination.is_file():
        return paper_record(LegacyLedgerEvidence("conflict", "登记已提交但账本 staging 丢失"))
    if _inspect_source(destination).outcome == "conflict":
        return paper_record(LegacyLedgerEvidence("conflict", "登记已提交但账本无法通过完整性校验"))
    marker.unlink(missing_ok=True)
    return paper_record(LegacyLedgerEvidence("unchanged", f"已迁移到账户 {account_id}"))


def _prepare_copy(source: Path, destination: Path, staging: Path, marker: Path) -> None:
    if destination.exists():
        if (
            not marker.is_file()
            or marker.read_text(encoding="utf-8") != SOURCE_NAME
            or _inspect_source(destination).outcome == "conflict"
        ):
            raise ValueError("目标账本已存在且没有可验证的迁移 marker")
        return
    if not staging.exists():
        _copy_ledger(source, staging)
    elif _inspect_source(staging).outcome == "conflict":
        raise ValueError("纸交易迁移 staging 无效")
    marker.write_text(SOURCE_NAME, encoding="utf-8")
    os.replace(staging, destination)


def _copy_and_insert(root: Path, source: Path, store, account_id: str) -> MigrationRecord:
    _paper_accounts = _load_backtest_paper_accounts()

    destination = store.ledger_path(account_id)
    staging = destination.with_name(f".{destination.name}.migration-staging")
    marker = destination.with_name(".legacy-paper-copy-ready")
    recovered = _recover_registered_copy(root, account_id, destination, staging, marker)
    if recovered is not None:
        return recovered
    try:
        _prepare_copy(source, destination, staging, marker)
        _paper_accounts.Ledger.migrate_legacy_database(destination)
        _insert_import(store, account_id)
        marker.unlink(missing_ok=True)
    except sqlite3.IntegrityError:
        existing = _existing_account(root)
        if not existing:
            raise
        if existing == account_id:
            marker.unlink(missing_ok=True)
        return paper_record(LegacyLedgerEvidence("unchanged", f"已迁移到账户 {existing}"))
    except (OSError, sqlite3.Error, ValueError):
        raise
    return paper_record(_inspect_source(source))


class PaperLegacyMigrator:
    """Import ledger facts once without inventing a current trading strategy."""

    name = "paper-ledger"
    backup_paths = (PAPER_DATABASE, ACCOUNT_ROOT)

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        source = root / SOURCE_NAME
        if not source.is_file():
            return ()
        account_id = _existing_account(root)
        if account_id:
            return (paper_record(LegacyLedgerEvidence("unchanged", f"已迁移到账户 {account_id}")),)
        return (paper_record(_inspect_source(source)),)

    def migrate_batch(
        self, root: Path, *, after_key: str, limit: int,
    ) -> Iterable[MigrationRecord]:
        if after_key >= SOURCE_NAME or limit < 1:
            return ()
        source = root / SOURCE_NAME
        if not source.is_file():
            return ()
        evidence = _inspect_source(source)
        if evidence.outcome == "conflict":
            return (paper_record(evidence),)
        existing = _existing_account(root)
        if existing:
            return (paper_record(LegacyLedgerEvidence("unchanged", f"已迁移到账户 {existing}")),)


        _paper_accounts = _load_backtest_paper_accounts()

        store = _paper_accounts.PaperStore(root / PAPER_DATABASE, root / ACCOUNT_ROOT)
        return (_copy_and_insert(root, source, store, ACCOUNT_ID),)

    def rollback(self, root: Path, backup_root: Path) -> None:

        restore_backup_path(root, backup_root, PAPER_DATABASE)
        restore_backup_path(root, backup_root, ACCOUNT_ROOT)


def _load_lab_jobs():
    import importlib as _imp
    return _imp.import_module("quantmaster" + "." + "lab" + "." + "jobs")

def _load_lab_models():
    import importlib as _imp
    return _imp.import_module("quantmaster" + "." + "lab" + "." + "models")

def _load_lab_store():
    import importlib as _imp
    return _imp.import_module("quantmaster" + "." + "lab" + "." + "store")

def register_paper_legacy_migrator() -> None:

    register_migrator(PaperLegacyMigrator())


__all__ = ["PaperLegacyMigrator", "register_paper_legacy_migrator"]


# for_version: v1.0  (consolidated from quantmaster.lab.job_migration)





_LAB = Path("lab.sqlite")
lab_job_domain_tables = {
    "factor_definitions",
    "factor_versions",
    "validation_reports",
    "approvals",
    "deployments",
    "deployment_evidence",
    "dataset_snapshots",
    "experiments",
    "copilot_suggestions",
    "optimization_studies",
    "bias_audits",
    "mining_runs",
    "mining_candidates",
    "lab_worker_results",
    "lab_publications",
    "lab_publication_events",
    "research_cycles",
    "strategy_candidates",
    "shadow_signals",
    "promotion_events",
}
lab_job_legacy_tables = {"lab_jobs", "lab_job_events", "lab_schedule_slots"}
lab_job_job_columns = {
    "id",
    "kind",
    "status",
    "params_json",
    "result_json",
    "dataset_id",
    "resource_class",
    "preflight_json",
    "progress",
    "phase",
    "detail",
    "error",
    "error_code",
    "error_json",
    "telemetry_json",
    "cancel_requested",
    "worker",
    "llm_scope",
    "llm_revision",
    "cancellation_reason",
    "created_at",
    "started_at",
    "heartbeat_at",
    "finished_at",
}
lab_job_event_columns = {"seq", "job_id", "event_json", "created_at"}
_SLOT_COLUMNS = {"slot", "created_at"}
lab_job_result_columns = {
    "job_id", "attempt", "kind", "outcome", "result_json", "error_json",
    "telemetry_json", "content_hash", "created_at",
}
lab_job_statuses = {
    "queued",
    "running",
    "cancelling",
    "interrupted",
    "paused",
    "cancelled",
    "completed",
    "completed_with_warnings",
    "failed",
}


def lab_job_tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def lab_job_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


def lab_job_json_object(raw: Any, field: str) -> dict[str, Any]:
    value = json.loads(str(raw or "{}"))
    if not isinstance(value, dict):
        raise ValueError(f"{field} 不是 JSON 对象")
    return value


def _table_job_links(
    connection: sqlite3.Connection,
    jobs: dict[str, sqlite3.Row],
    table: str,
    expected_kind: str,
) -> set[str]:
    conflicts: set[str] = set()
    for row in connection.execute(f"SELECT id,job_id FROM {table}"):
        job_id = str(row["job_id"] or "")
        if not job_id:
            continue
        job = jobs.get(job_id)
        if job is None:
            conflicts.add(f"{table}:{row['id']}:dangling_job")
        elif str(job["kind"]) != expected_kind:
            conflicts.add(f"{table}:{row['id']}:job_kind")
    return conflicts


def _job_domain_links(
    connection: sqlite3.Connection,
    jobs: dict[str, sqlite3.Row],
) -> set[str]:
    conflicts: set[str] = set()
    for job_id, row in jobs.items():
        try:
            params = lab_job_json_object(row["params_json"], "params_json")
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        kind = str(row["kind"])
        if kind == "optimize":
            study_id = str(params.get("study_id") or "")
            link = connection.execute(
                "SELECT job_id FROM optimization_studies WHERE id=?", (study_id,),
            ).fetchone()
            if link is None or str(link["job_id"] or "") not in {"", job_id}:
                conflicts.add(f"{job_id}:dangling_study")
        if kind == "discover_python":
            run_id = str(params.get("run_id") or "")
            link = connection.execute(
                "SELECT job_id FROM mining_runs WHERE id=?", (run_id,),
            ).fetchone()
            if link is None or str(link["job_id"] or "") not in {"", job_id}:
                conflicts.add(f"{job_id}:dangling_mining_run")
    return conflicts


def _domain_foreign_links(connection: sqlite3.Connection) -> set[str]:
    conflicts: set[str] = set()
    for row in connection.execute("SELECT id,experiment_id FROM optimization_studies"):
        experiment_id = str(row["experiment_id"] or "")
        if experiment_id and connection.execute(
            "SELECT 1 FROM experiments WHERE id=?", (experiment_id,),
        ).fetchone() is None:
            conflicts.add(f"optimization_studies:{row['id']}:dangling_experiment")
    for row in connection.execute("SELECT id,run_id,version_id FROM mining_candidates"):
        if connection.execute(
            "SELECT 1 FROM mining_runs WHERE id=?", (str(row["run_id"]),),
        ).fetchone() is None:
            conflicts.add(f"mining_candidates:{row['id']}:dangling_run")
        version_id = str(row["version_id"] or "")
        if version_id and connection.execute(
            "SELECT 1 FROM factor_versions WHERE id=?", (version_id,),
        ).fetchone() is None:
            conflicts.add(f"mining_candidates:{row['id']}:dangling_version")
    return conflicts


def _domain_links(connection: sqlite3.Connection, jobs: dict[str, sqlite3.Row]) -> set[str]:
    return (
        _table_job_links(connection, jobs, "optimization_studies", "optimize")
        | _table_job_links(connection, jobs, "mining_runs", "discover_python")
        | _job_domain_links(connection, jobs)
        | _domain_foreign_links(connection)
    )


def _artifact_path(root: Path, raw: Any) -> Path:
    value = str(raw or "")
    candidate = Path(value)
    if candidate.is_absolute():
        boundary = root.resolve()
        resolved = candidate.resolve()
        if not resolved.is_relative_to(boundary):
            raise ValueError("artifact 路径越出数据目录")
    else:
        resolved = confined_path(root, value, label="Lab artifact")
    if not resolved.is_file():
        raise FileNotFoundError(value)
    return resolved


def _trial_evidence_conflicts(
    study_id: str,
    result: dict[str, Any],
) -> set[str]:
    conflicts: set[str] = set()
    trials = result.get("trials")
    numbers: set[int] = set()
    if trials is not None:
        if not isinstance(trials, list):
            conflicts.add(f"optimization_studies:{study_id}:trials")
        else:
            for offset, trial in enumerate(trials):
                if not isinstance(trial, dict):
                    conflicts.add(f"optimization_studies:{study_id}:trial:{offset}")
                    continue
                number = trial.get("number")
                if isinstance(number, bool) or not isinstance(number, int) or number < 0:
                    conflicts.add(f"optimization_studies:{study_id}:trial:{offset}:number")
                elif number in numbers:
                    conflicts.add(f"optimization_studies:{study_id}:trial:{number}:duplicate")
                else:
                    numbers.add(number)
    recommended = result.get("recommended")
    if recommended is not None:
        if not isinstance(recommended, dict) or recommended.get("number") not in numbers:
            conflicts.add(f"optimization_studies:{study_id}:dangling_trial")
    return conflicts


def _nested_artifact_paths(
    study_id: str,
    result: dict[str, Any],
) -> tuple[set[str], list[tuple[str, Any]]]:
    conflicts: set[str] = set()
    paths: list[tuple[str, Any]] = []
    folds = result.get("fold_artifacts")
    if folds is not None:
        if not isinstance(folds, list):
            conflicts.add(f"optimization_studies:{study_id}:fold_artifacts")
        else:
            for offset, fold in enumerate(folds):
                if not isinstance(fold, dict) or not fold.get("artifact"):
                    conflicts.add(f"optimization_studies:{study_id}:fold_artifact:{offset}")
                else:
                    paths.append((f"fold_artifact:{offset}", fold["artifact"]))
    live = result.get("live_artifact")
    if live is not None:
        if not isinstance(live, dict) or not live.get("artifact"):
            conflicts.add(f"optimization_studies:{study_id}:live_artifact")
        else:
            paths.append(("live_artifact", live["artifact"]))
    if result.get("candidate") and not all(
        result.get(field) for field in ("prediction_artifact", "manifest", "fold_artifacts")
    ):
        conflicts.add(f"optimization_studies:{study_id}:candidate_artifacts")
    return conflicts, paths


def _artifact_evidence_conflicts(
    root: Path,
    study_id: str,
    result: dict[str, Any],
) -> set[str]:
    conflicts, paths = _nested_artifact_paths(study_id, result)
    for field in ("prediction_artifact", "manifest"):  # noqa: F402
        if result.get(field):
            paths.append((field, result[field]))
    for label, value in paths:
        try:
            _artifact_path(root, value)
        except (FileNotFoundError, OSError, ValueError):
            conflicts.add(f"optimization_studies:{study_id}:{label}:dangling")
    return conflicts


def _study_evidence_conflicts(
    root: Path,
    study_id: str,
    result: dict[str, Any],
) -> set[str]:
    return (
        _trial_evidence_conflicts(study_id, result)
        | _artifact_evidence_conflicts(root, study_id, result)
    )


def _mining_artifact_conflicts(
    root: Path,
    candidate_id: str,
    artifact: dict[str, Any],
) -> set[str]:
    if not artifact:
        return set()
    conflicts: set[str] = set()
    for field in ("manifest", "source"):  # noqa: F402
        try:
            _artifact_path(root, artifact.get(field))
        except (FileNotFoundError, OSError, ValueError):
            conflicts.add(f"mining_candidates:{candidate_id}:{field}:dangling")
    return conflicts


def lab_job_content_conflicts(root: Path, connection: sqlite3.Connection) -> tuple[str, ...]:
    conflicts: set[str] = set()
    rows = connection.execute("SELECT * FROM lab_jobs ORDER BY created_at,id").fetchall()
    jobs = {str(row["id"]): row for row in rows}
    for job_id, row in jobs.items():
        status = str(row["status"])
        kind = str(row["kind"])
        if status not in lab_job_statuses:
            conflicts.add(f"status:{status}")
        if kind not in _load_lab_jobs().LAB_KINDS:
            conflicts.add(f"kind:{kind}")
        for field in (  # noqa: F402
            "params_json", "result_json", "preflight_json", "error_json", "telemetry_json",
        ):
            try:
                lab_job_json_object(row[field], field)
            except (TypeError, ValueError, json.JSONDecodeError):
                conflicts.add(f"{job_id}:{field}")
        if status in {"running", "cancelling"} and not all(
            str(row[field] or "") for field in ("worker", "started_at", "heartbeat_at")
        ):
            conflicts.add(f"{job_id}:lease_evidence")
        if bool(row["cancel_requested"]) and status not in {
            "running", "cancelling", "interrupted", "cancelled",
        }:
            conflicts.add(f"{job_id}:cancel_status")
        if kind in {"discover_llm", "discover_python"}:
            scope = str(row["llm_scope"] or "")
            revision = str(row["llm_revision"] or "")
            manual = status == "interrupted" and str(row["phase"] or "") == "需要手动重试"
            if not manual and (not scope or not revision):
                conflicts.add(f"{job_id}:llm_revision")
    for row in connection.execute("SELECT job_id,event_json FROM lab_job_events"):
        job_id = str(row["job_id"])
        if job_id not in jobs:
            conflicts.add(f"event:{job_id}:dangling_job")
        try:
            payload = lab_job_json_object(row["event_json"], "event_json")
        except (TypeError, ValueError, json.JSONDecodeError):
            conflicts.add(f"{job_id}:event_json")
            continue
        if not str(payload.get("type") or ""):
            conflicts.add(f"{job_id}:event_type")
    for table, fields in (  # noqa: F402
        ("optimization_studies", ("config_json", "result_json")),
        ("mining_runs", ("config_json", "split_json", "result_json")),
        ("mining_candidates", ("proposal_json", "metrics_json", "artifact_json")),
    ):
        for row in connection.execute(f"SELECT * FROM {table}"):
            for field in fields:
                try:
                    payload = lab_job_json_object(row[field], field)
                except (TypeError, ValueError, json.JSONDecodeError):
                    conflicts.add(f"{table}:{row['id']}:{field}")
                    continue
                if table == "optimization_studies" and field == "result_json":
                    conflicts.update(_study_evidence_conflicts(root, str(row["id"]), payload))
                if table == "mining_candidates" and field == "artifact_json":
                    conflicts.update(_mining_artifact_conflicts(root, str(row["id"]), payload))
    conflicts.update(_domain_links(connection, jobs))
    return tuple(sorted(conflicts))


def lab_job_target_conflicts(root: Path, rows: Iterable[sqlite3.Row]) -> tuple[str, ...]:
    path = root / "jobs.sqlite"
    if not path.is_file():
        return ()
    conflicts: set[str] = set()
    try:
        store = UnifiedJobStore(path, read_only=True)
        for row in rows:
            job_id = str(row["id"])
            try:
                existing = store.get(job_id)
            except KeyError:
                continue
            expected_spec = {
                "kind": str(row["kind"]),
                "params": lab_job_json_object(row["params_json"], "params_json"),
                "preflight": lab_job_json_object(row["preflight_json"], "preflight_json"),
                "dataset_id": str(row["dataset_id"] or ""),
                "resource_class": str(row["resource_class"] or "cpu"),
            }
            if (
                str(existing.get("type") or "") != f"lab.{row['kind']}"
                or dict(existing.get("spec") or {}) != expected_spec
            ):
                conflicts.add(f"{job_id}:target_collision")
    except (FileNotFoundError, sqlite3.Error, ValueError):
        conflicts.add("jobs.sqlite:unclassified")
    return tuple(sorted(conflicts))


def lab_job_probe(root: Path) -> tuple[str, tuple[str, ...]]:
    path = root / _LAB
    if not path.is_file():
        return "absent", ()
    with closing(connect_sqlite(path, read_only=True, row_factory=True)) as connection:
        tables = lab_job_tables(connection)
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        legacy = lab_job_legacy_tables & tables
        if not legacy:
            unknown = (tables - lab_job_domain_tables - {"sqlite_sequence"}) | (
                lab_job_domain_tables - tables
            )
            if version == _load_lab_store().LAB_SCHEMA_VERSION and not unknown:
                return "retired", ()
            return "conflict", tuple(sorted(unknown | {f"user_version:{version}"}))
        expected_domain = lab_job_domain_tables - ({"lab_worker_results"} if version == 11 else set())
        schema_conflicts = (
            tables - expected_domain - lab_job_legacy_tables - {"sqlite_sequence"}
        ) | (expected_domain - tables) | (lab_job_legacy_tables - tables)
        if version not in {11, _load_lab_store().LAB_SCHEMA_VERSION}:
            schema_conflicts.add(f"user_version:{version}")
        if not schema_conflicts:
            schema_conflicts |= lab_job_columns(connection, "lab_jobs") ^ lab_job_job_columns
            schema_conflicts |= lab_job_columns(connection, "lab_job_events") ^ lab_job_event_columns
            schema_conflicts |= lab_job_columns(connection, "lab_schedule_slots") ^ _SLOT_COLUMNS
            if "lab_worker_results" in tables:
                schema_conflicts |= lab_job_columns(connection, "lab_worker_results") ^ lab_job_result_columns
        content = lab_job_content_conflicts(root, connection) if not schema_conflicts else ()
        rows = connection.execute("SELECT * FROM lab_jobs").fetchall() if not schema_conflicts else ()
    evidence = set(schema_conflicts) | set(content)
    if not evidence:
        evidence.update(lab_job_target_conflicts(root, rows))
    return ("conflict", tuple(sorted(evidence))) if evidence else ("upgrade", ())


def lab_job_record(status: str, unknown: tuple[str, ...] = ()) -> MigrationRecord:
    if status == "conflict":
        return MigrationRecord(
            "quant-lab",
            "conflict",
            "lab_job_schema_unclassified",
            unknown,
            "Quant Lab 含未知 lifecycle、冲突 lease 或悬空领域关联，拒绝写入",
        )
    return MigrationRecord(
        "quant-lab",
        "review" if status == "upgrade" else "converted",
        "lab_job_lifecycle_migration_required" if status == "upgrade" else "lab_job_migrated",
        (),
        f"Quant Lab lifecycle {'需要迁移' if status == 'upgrade' else '已迁移'}",
    )


def lab_job_events(connection: sqlite3.Connection, job_id: str) -> list[dict[str, Any]]:
    result = []
    rows = connection.execute(
        "SELECT seq,event_json,created_at FROM lab_job_events WHERE job_id=? ORDER BY seq",
        (job_id,),
    ).fetchall()
    for offset, row in enumerate(rows, start=1):
        payload = lab_job_json_object(row["event_json"], "event_json")
        legacy_type = str(payload.pop("type"))
        result.append({
            "seq": offset,
            "attempt": 1,
            "type": f"legacy_lab_{legacy_type}",
            "payload": {"legacy_type": legacy_type, **payload},
            "created_at": row["created_at"],
        })
    return result


def lab_job_convert(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any] | None]:
    legacy_status = str(row["status"])
    statuses = {
        "queued": "queued",
        "running": "interrupted",
        "cancelling": "interrupted",
        "interrupted": "interrupted",
        "paused": "interrupted",
        "cancelled": "cancelled",
        "completed": "completed",
        "completed_with_warnings": "completed",
        "failed": "failed",
    }
    if legacy_status not in statuses:
        raise ValueError(f"未知 Lab status: {legacy_status}")
    kind = str(row["kind"])
    params = lab_job_json_object(row["params_json"], "params_json")
    result = lab_job_json_object(row["result_json"], "result_json")
    preflight = lab_job_json_object(row["preflight_json"], "preflight_json")
    error_info = lab_job_json_object(row["error_json"], "error_json")
    telemetry = lab_job_json_object(row["telemetry_json"], "telemetry_json")
    outcome = (
        "completed_with_warnings" if legacy_status == "completed_with_warnings"
        else "paused" if legacy_status == "paused"
        else legacy_status if legacy_status in {"completed", "failed", "cancelled"}
        else ""
    )
    events = lab_job_events(connection, str(row["id"]))
    checkpoint = next(
        (
            {"schema_version": "1.0", "type": "partition_checkpoint", **event["payload"]}
            for event in reversed(events)
            if event["payload"].get("legacy_type") == "partition_checkpoint"
        ),
        None,
    )
    artifacts: list[dict[str, Any]] = []
    if checkpoint is not None:
        artifacts.append({
            "kind": f"checkpoint.{_load_lab_jobs().LAB_PROGRESS_CHECKPOINT}",
            "checkpoint_key": _load_lab_jobs().LAB_PROGRESS_CHECKPOINT,
            "payload": checkpoint,
            "attempt": 1,
            "created_at": row["heartbeat_at"] or row["created_at"],
        })
    domain_result = None
    if outcome:
        payload = {
            "schema_version": "1.0",
            "kind": kind,
            "outcome": outcome,
            "result": result,
            "error_info": error_info,
            "telemetry": telemetry,
        }
        artifacts.append({
            "kind": _load_lab_jobs().LAB_RESULT_KIND,
            "result": True,
            "payload": payload,
            "attempt": 1,
            "created_at": row["finished_at"] or row["created_at"],
        })
        domain_result = {
            "job_id": str(row["id"]),
            "attempt": 1,
            "kind": kind,
            "outcome": outcome,
            "result": result,
            "error_info": error_info,
            "telemetry": telemetry,
            "created_at": row["finished_at"] or row["created_at"],
        }
    record = {
        "id": str(row["id"]),
        "type": f"lab.{kind}",
        "spec": {
            "kind": kind,
            "params": params,
            "preflight": preflight,
            "dataset_id": str(row["dataset_id"] or ""),
            "resource_class": str(row["resource_class"] or "cpu"),
        },
        "algorithm_version": _load_lab_jobs().LAB_ALGORITHM_VERSION,
        "status": statuses[legacy_status],
        "progress": int(row["progress"] or 0),
        "phase": "等待恢复" if legacy_status in {"running", "cancelling"} else str(row["phase"]),
        "detail": str(row["detail"] or row["error"] or ""),
        "attempt": 1,
        "max_attempts": 8,
        "cancel_requested": bool(row["cancel_requested"]),
        "llm_scope": str(row["llm_scope"] or ""),
        "llm_revision": str(row["llm_revision"] or ""),
        "diagnostic_code": str(row["error_code"] or ""),
        "created_at": row["created_at"],
        "updated_at": row["heartbeat_at"] or row["finished_at"] or row["created_at"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"] if statuses[legacy_status] in {
            "completed", "cancelled", "failed",
        } else "",
        "deadline_seconds": 3600,
    }
    return record, events, artifacts, domain_result


def _result_ddl(connection: sqlite3.Connection) -> None:
    connection.execute(
        "CREATE TABLE IF NOT EXISTS lab_worker_results ("
        "job_id TEXT NOT NULL,attempt INTEGER NOT NULL,kind TEXT NOT NULL,outcome TEXT NOT NULL,"
        "result_json TEXT NOT NULL,error_json TEXT NOT NULL DEFAULT '{}',"
        "telemetry_json TEXT NOT NULL DEFAULT '{}',content_hash TEXT NOT NULL,"
        "created_at TEXT NOT NULL,PRIMARY KEY(job_id,attempt))"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_lab_worker_results_kind "
        "ON lab_worker_results(kind,created_at DESC)"
    )


def lab_job_migrate(root: Path) -> None:
    path = root / _LAB
    with closing(connect_sqlite(path, read_only=True, row_factory=True)) as connection:
        converted = [
            lab_job_convert(connection, row)
            for row in connection.execute(
                "SELECT * FROM lab_jobs ORDER BY created_at,id"
            ).fetchall()
        ]
    store = UnifiedJobStore(root / "jobs.sqlite")
    for record, events, artifacts, _domain_result in converted:
        store.import_legacy_job(record, events=events, artifacts=artifacts)
    for record, events, artifacts, _domain_result in converted:
        imported = store.get(str(record["id"]))
        if str(imported["type"]) not in _load_lab_jobs().LAB_JOB_TYPES:
            raise ValueError(f"Lab job 导入类型不守恒: {record['id']}")
        if len(store.events(str(record["id"]), 0, 2000)) < len(events):
            raise ValueError(f"Lab job event 导入数量不守恒: {record['id']}")
        if any(item.get("result") for item in artifacts) and not imported["result_artifact_id"]:
            raise ValueError(f"Lab worker result 导入缺失: {record['id']}")
    with closing(connect_sqlite(path, row_factory=True)) as connection:
        connection.execute("BEGIN IMMEDIATE")
        _result_ddl(connection)
        for _record_value, _events_value, _artifacts_value, domain_result in converted:
            if domain_result is None:
                continue
            digest = _load_lab_models().content_hash({
                "kind": domain_result["kind"],
                "outcome": domain_result["outcome"],
                "result": domain_result["result"],
                "error_info": domain_result["error_info"],
                "telemetry": domain_result["telemetry"],
            })
            existing = connection.execute(
                "SELECT content_hash FROM lab_worker_results WHERE job_id=? AND attempt=?",
                (domain_result["job_id"], domain_result["attempt"]),
            ).fetchone()
            if existing is not None and str(existing["content_hash"]) != digest:
                raise ValueError(f"Lab worker result 冲突: {domain_result['job_id']}")
            connection.execute(
                "INSERT OR IGNORE INTO lab_worker_results "
                "(job_id,attempt,kind,outcome,result_json,error_json,telemetry_json,"
                "content_hash,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    domain_result["job_id"],
                    domain_result["attempt"],
                    domain_result["kind"],
                    domain_result["outcome"],
                    _load_backtest_spec().canonical_json(domain_result["result"]),
                    _load_backtest_spec().canonical_json(domain_result["error_info"]),
                    _load_backtest_spec().canonical_json(domain_result["telemetry"]),
                    digest,
                    domain_result["created_at"],
                ),
            )
        connection.execute("DROP TABLE lab_job_events")
        connection.execute("DROP TABLE lab_jobs")
        connection.execute("DROP TABLE lab_schedule_slots")
        connection.execute(f"PRAGMA user_version={_load_lab_store().LAB_SCHEMA_VERSION}")
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    _load_lab_store().LabStore(path, read_only=True)


class LabJobLegacyMigrator:
    name = "lab-jobs"
    backup_paths = ("lab.sqlite", "jobs.sqlite")

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        status, unknown = lab_job_probe(root)
        if status in {"absent", "retired"}:
            return ()
        return (lab_job_record(status, unknown),)

    def migrate_batch(
        self,
        root: Path,
        *,
        after_key: str,
        limit: int,
    ) -> Iterable[MigrationRecord]:
        if after_key >= "quant-lab" or int(limit) < 1:
            return ()
        status, unknown = lab_job_probe(root)
        if status in {"absent", "retired"}:
            return ()
        if status == "conflict":
            return (lab_job_record(status, unknown),)
        lab_job_migrate(root)
        return (lab_job_record("converted"),)

    def rollback(self, root: Path, backup_root: Path) -> None:

        for relative in self.backup_paths:
            restore_backup_path(root, backup_root, relative)


lab_job_legacy_migrator = LabJobLegacyMigrator()


# for_version: v1.0  (consolidated from quantmaster.lab.model_migration)




_V1_REQUIRED = {
    "schema_version", "kind", "features", "sequence_length", "horizon",
    "artifact", "artifact_sha256",
}
_V2_REQUIRED = {
    "schema_version", "kind", "horizons", "features", "feature_names",
    "sequence_length", "protocol", "prediction_artifact", "prediction_sha256",
    "fold_artifacts", "live_artifact", "calibration", "calibration_models",
}


def _manifests(root: Path) -> list[Path]:
    artifact_root = root / "lab_artifacts"
    return sorted(path for path in artifact_root.rglob("manifest*.json") if path.is_file())


def _inspect_one(root: Path, path: Path) -> MigrationRecord | None:
    key = path.relative_to(root).as_posix()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return MigrationRecord(
            key, "conflict", "lab_model_manifest_invalid_json",
            detail="清单无法解析；拒绝猜测格式",
        )
    if not isinstance(payload, dict):
        return MigrationRecord(key, "conflict", "lab_model_manifest_invalid_shape")
    version = payload.get("schema_version")
    if version == 2:
        missing = tuple(sorted(_V2_REQUIRED - payload.keys()))
        return None if not missing else MigrationRecord(
            key, "conflict", "lab_model_schema_v2_corrupt", missing,
            "已标记 current 的清单缺少必需字段；不得误判为旧格式",
        )
    if version == 1:
        missing = tuple(sorted(_V1_REQUIRED - payload.keys()))
        return MigrationRecord(
            key, "conflict" if missing else "review",
            "lab_model_schema_v1_incomplete" if missing else "lab_model_schema_v1_requires_isolation",
            missing or ("protocol", "live_artifact", "prediction_artifact", "calibration", "ood"),
            "v1 权重不能可靠转换为 v2；仅可隔离并使引用不可部署",
        )
    return MigrationRecord(
        key, "conflict", "lab_model_schema_unclassified", ("schema_version",),
        "缺少或未知 schema 标签；拒绝按特征猜测",
    )


class LabModelArtifactMigrator:
    name = "lab-model-artifacts"
    backup_paths = ("lab.sqlite", "lab_artifacts", "migration_quarantine/lab_models")

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        for path in _manifests(root):
            record = _inspect_one(root, path)
            if record is not None:
                yield record

    def migrate_batch(
        self, root: Path, *, after_key: str, limit: int,
    ) -> Iterable[MigrationRecord]:
        candidates = [
            record for record in self.inspect(root)
            if record.record_key > after_key
        ][:max(1, int(limit))]
        for record in candidates:
            if record.diagnostic_code != "lab_model_schema_v1_requires_isolation":
                yield record
                continue
            manifest = root / record.record_key
            self._retire_references(root, record.record_key)
            source = manifest.parent
            relative = source.relative_to(root / "lab_artifacts")
            target = root / "migration_quarantine" / "lab_models" / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                if source.exists():
                    raise FileExistsError(f"Lab 模型隔离目标冲突：{target}")
            else:
                os.replace(source, target)
            yield MigrationRecord(
                record.record_key, "blank", "lab_model_schema_v1_isolated",
                record.unknown_fields,
                "旧工件已隔离；manifest 引用留空，版本归档且不可部署",
            )

    @staticmethod
    def _retire_references(root: Path, manifest: str) -> None:
        database = root / "lab.sqlite"
        if not database.is_file():
            return
        with connect_sqlite(database) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute("SELECT id,spec_json FROM factor_versions").fetchall()
            for row in rows:
                spec = json.loads(str(row["spec_json"]))
                model = dict(spec.get("model") or {})
                if model.get("manifest") != manifest:
                    continue
                model["manifest"] = ""
                model["availability"] = "unavailable"
                model["diagnostic_code"] = "lab_model_schema_v1_isolated"
                spec["model"] = model
                connection.execute(
                    "UPDATE factor_versions SET spec_json=?,status='archived' WHERE id=?",
                    (strict_json_dumps(spec, sort_keys=True), row["id"]),
                )
            experiment_rows = connection.execute("SELECT id,result_json FROM experiments").fetchall()
            for row in experiment_rows:
                result = json.loads(str(row["result_json"] or "{}"))
                if result.get("manifest") != manifest:
                    continue
                result["manifest"] = None
                result["model_availability"] = "unavailable"
                result["diagnostic_code"] = "lab_model_schema_v1_isolated"
                connection.execute(
                    "UPDATE experiments SET result_json=? WHERE id=?",
                    (strict_json_dumps(result, sort_keys=True), row["id"]),
                )

    def rollback(self, root: Path, backup_root: Path) -> None:
        for relative in self.backup_paths:
            restore_backup_path(root, backup_root, relative)


lab_model_artifact_migrator = LabModelArtifactMigrator()


# for_version: v1.0  (consolidated from quantmaster.research.job_migration)





_CATALOG = Path("research_lake") / "_meta" / "catalog.sqlite"
research_job_domain_tables = {
    "research_specs",
    "research_partitions",
    "research_runs",
    "research_leases",
    "research_partition_intents",
    "research_capabilities",
}
research_job_legacy_tables = {"research_jobs", "research_job_events"}
research_job_job_columns = {
    "id",
    "status",
    "mode",
    "plan_json",
    "next_index",
    "total",
    "succeeded",
    "failed",
    "cancel_requested",
    "current_task",
    "failures_json",
    "manifest_json",
    "created_at",
    "updated_at",
    "owner",
    "lease_expires",
    "heartbeat_at",
    "attempt",
    "task_indexes_json",
}
research_job_event_columns = {"seq", "job_id", "attempt", "event_json", "created_at"}
research_job_statuses = {
    "queued",
    "running",
    "cancelling",
    "interrupted",
    "cancelled",
    "completed",
    "completed_with_errors",
    "failed",
}


def _load_research_catalog():
    import importlib as _imp
    return _imp.import_module("quantmaster" + "." + "research" + "." + "catalog")

def _load_research_contracts():
    import importlib as _imp
    return _imp.import_module("quantmaster" + "." + "research" + "." + "contracts")

def _load_research_jobs():
    import importlib as _imp
    return _imp.import_module("quantmaster" + "." + "research" + "." + "jobs")

def research_job_tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def research_job_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


def research_job_json_object(raw: Any, field: str) -> dict[str, Any]:
    value = json.loads(str(raw or "{}"))
    if not isinstance(value, dict):
        raise ValueError(f"{field} 不是 JSON 对象")
    return value


def research_job_json_list(raw: Any, field: str) -> list[Any]:
    value = json.loads(str(raw or "[]"))
    if not isinstance(value, list):
        raise ValueError(f"{field} 不是 JSON 数组")
    return value


def _validate_partition_links(
    connection: sqlite3.Connection,
    job_id: str,
    manifest: dict[str, Any],
) -> set[str]:
    conflicts: set[str] = set()
    for field in ("input_partitions", "output_partitions"):  # noqa: F402
        values = manifest.get(field) or []
        if not isinstance(values, list):
            conflicts.add(f"{job_id}:{field}")
            continue
        for index, value in enumerate(values):
            if not isinstance(value, dict) or not str(value.get("partition_key") or ""):
                conflicts.add(f"{job_id}:{field}:{index}:partition_key")
                continue
            exists = connection.execute(
                "SELECT 1 FROM research_partitions WHERE partition_key=?",
                (str(value["partition_key"]),),
            ).fetchone()
            if exists is None:
                conflicts.add(f"{job_id}:{field}:{index}:dangling")
    return conflicts


def _job_content_conflicts(connection: sqlite3.Connection, row: sqlite3.Row) -> set[str]:
    job_id = str(row["id"])
    status = str(row["status"])
    if status not in research_job_statuses:
        return {f"status:{status}"}
    try:
        plan = research_job_json_object(row["plan_json"], "plan_json")
        _load_research_contracts().ExecutionPlan.from_dict(plan)
        failures = research_job_json_list(row["failures_json"], "failures_json")
        manifest = research_job_json_object(row["manifest_json"], "manifest_json")
        task_indexes = research_job_json_list(row["task_indexes_json"], "task_indexes_json")
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return {f"{job_id}:json"}
    tasks = list(plan.get("tasks") or ())
    conflicts = set()
    if (
        any(not isinstance(item, dict) for item in failures)
        or any(not isinstance(item, int) or item < 0 or item >= len(tasks) for item in task_indexes)
        or int(row["next_index"]) < 0
        or int(row["next_index"]) > len(task_indexes)
        or int(row["total"]) != len(task_indexes)
    ):
        conflicts.add(f"{job_id}:progress")
    conflicts.update(_validate_partition_links(connection, job_id, manifest))
    if status in {"completed", "completed_with_errors"}:
        run = connection.execute(
            "SELECT manifest_json FROM research_runs WHERE run_id=?", (job_id,),
        ).fetchone()
        if run is None:
            conflicts.add(f"{job_id}:run_manifest")
        else:
            try:
                published = research_job_json_object(run["manifest_json"], "run_manifest")
            except (TypeError, ValueError, json.JSONDecodeError):
                conflicts.add(f"{job_id}:run_manifest")
            else:
                if str(published.get("plan_hash") or "") != str(plan.get("plan_hash") or ""):
                    conflicts.add(f"{job_id}:plan_hash")
    return conflicts


def _event_content_conflicts(row: sqlite3.Row) -> set[str]:
    try:
        payload = research_job_json_object(row["event_json"], "event_json")
    except (TypeError, ValueError, json.JSONDecodeError):
        return {f"{row['job_id']}:event_json"}
    return set() if str(payload.get("type") or "") else {f"{row['job_id']}:event_type"}


def research_job_content_conflicts(connection: sqlite3.Connection) -> tuple[str, ...]:
    conflicts: set[str] = set()
    for row in connection.execute("SELECT * FROM research_jobs ORDER BY created_at,id"):
        conflicts.update(_job_content_conflicts(connection, row))
    for row in connection.execute("SELECT job_id,event_json FROM research_job_events"):
        conflicts.update(_event_content_conflicts(row))
    return tuple(sorted(conflicts))


def research_job_probe(path: Path) -> tuple[str, tuple[str, ...]]:
    if not path.is_file():
        return "absent", ()
    with closing(connect_sqlite(path, read_only=True, row_factory=True)) as connection:
        tables = research_job_tables(connection)
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if not (research_job_legacy_tables & tables):
            unknown = (tables - research_job_domain_tables - {"sqlite_sequence"}) | (
                research_job_domain_tables - tables
            )
            if version == _load_research_catalog().RESEARCH_SCHEMA_VERSION and not unknown:
                return "retired", ()
            return "conflict", tuple(sorted(unknown | {f"user_version:{version}"}))
        unknown_tables = (
            tables - research_job_domain_tables
            - research_job_legacy_tables - {"sqlite_sequence"}
        )
        missing_tables = (research_job_domain_tables | research_job_legacy_tables) - tables
        unknown_columns = (research_job_columns(connection, "research_jobs") - research_job_job_columns) | (
            research_job_columns(connection, "research_job_events") - research_job_event_columns
        )
        missing_columns = (research_job_job_columns - research_job_columns(connection, "research_jobs")) | (
            research_job_event_columns - research_job_columns(connection, "research_job_events")
        )
        schema_conflicts = (
            unknown_tables | missing_tables | unknown_columns | missing_columns
            | ({f"user_version:{version}"} if version != 1 else set())
        )
        content = research_job_content_conflicts(connection) if not schema_conflicts else ()
    evidence = tuple(sorted(schema_conflicts | set(content)))
    return ("conflict", evidence) if evidence else ("upgrade", ())


def research_job_record(status: str, unknown: tuple[str, ...] = ()) -> MigrationRecord:
    if status == "conflict":
        return MigrationRecord(
            "research-lake",
            "conflict",
            "research_job_schema_unclassified",
            unknown,
            "Research Lake 含未知 lifecycle 或悬空 provenance，拒绝写入",
        )
    return MigrationRecord(
        "research-lake",
        "review" if status == "upgrade" else "converted",
        (
            "research_job_lifecycle_migration_required"
            if status == "upgrade"
            else "research_job_migrated"
        ),
        (),
        f"Research Lake lifecycle {'需要迁移' if status == 'upgrade' else '已迁移'}",
    )


def research_job_events(connection: sqlite3.Connection, job_id: str) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    rows = connection.execute(
        "SELECT attempt,event_json,created_at FROM research_job_events "
        "WHERE job_id=? ORDER BY seq",
        (job_id,),
    ).fetchall()
    for offset, row in enumerate(rows, start=1):
        payload = research_job_json_object(row["event_json"], "event_json")
        legacy_type = str(payload.pop("type"))
        values.append({
            "seq": offset,
            "attempt": max(1, int(row["attempt"] or 1)),
            "type": "job_started" if legacy_type == "claimed" else f"legacy_research_{legacy_type}",
            "payload": {"legacy_type": legacy_type, **payload},
            "created_at": row["created_at"],
        })
    return values


def research_job_convert(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    status = str(row["status"])
    statuses = {
        "queued": "queued",
        "running": "interrupted",
        "cancelling": "interrupted",
        "interrupted": "interrupted",
        "cancelled": "cancelled",
        "completed": "completed",
        "completed_with_errors": "completed",
        "failed": "failed",
    }
    if status not in statuses:
        raise ValueError(f"未知 research status: {status}")
    plan = research_job_json_object(row["plan_json"], "plan_json")
    failures = [dict(item) for item in research_job_json_list(row["failures_json"], "failures_json")]
    manifest = research_job_json_object(row["manifest_json"], "manifest_json")
    task_indexes = [int(item) for item in research_job_json_list(
        row["task_indexes_json"], "task_indexes_json",
    )]
    outcome = "completed_with_warnings" if status == "completed_with_errors" else (
        "completed" if status == "completed" else ""
    )
    state = {
        "schema_version": "1.0",
        "task_indexes": task_indexes,
        "next_index": int(row["next_index"]),
        "total": int(row["total"]),
        "succeeded": int(row["succeeded"]),
        "failed": int(row["failed"]),
        "failures": failures,
        "current_task": str(row["current_task"] or ""),
        "manifest": manifest,
        "outcome": outcome,
    }
    attempt = max(1, int(row["attempt"] or 1))
    artifacts: list[dict[str, Any]] = [{
        "kind": f"checkpoint.{_load_research_jobs().RESEARCH_CHECKPOINT}",
        "checkpoint_key": _load_research_jobs().RESEARCH_CHECKPOINT,
        "payload": state,
        "attempt": attempt,
        "created_at": row["updated_at"],
    }]
    if status in {"completed", "completed_with_errors"}:
        artifacts.append({
            "kind": _load_research_jobs().RESEARCH_RESULT_KIND,
            "result": True,
            "payload": state,
            "attempt": attempt,
            "created_at": row["updated_at"],
        })
    record = {
        "id": str(row["id"]),
        "type": _load_research_jobs().RESEARCH_TASK_TYPE,
        "spec": {"mode": str(row["mode"]), "plan": plan},
        "algorithm_version": "research-lake-v2",
        "status": statuses[status],
        "progress": round(100 * int(row["next_index"]) / max(1, int(row["total"]))),
        "phase": "等待恢复" if status in {"running", "cancelling"} else "",
        "detail": "从旧 Research Lake lifecycle 迁移",
        "attempt": attempt,
        "max_attempts": 8,
        "cancel_requested": bool(row["cancel_requested"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "finished_at": (
            row["updated_at"] if statuses[status] in {"completed", "cancelled"} else ""
        ),
        "deadline_seconds": 3600,
    }
    return record, research_job_events(connection, str(row["id"])), artifacts


def research_job_migrate(path: Path, store: UnifiedJobStore) -> None:
    with closing(connect_sqlite(path, read_only=True, row_factory=True)) as connection:
        converted = [
            research_job_convert(connection, row)
            for row in connection.execute(
                "SELECT * FROM research_jobs ORDER BY created_at,id"
            ).fetchall()
        ]
    for record, events, artifacts in converted:
        store.import_legacy_job(record, events=events, artifacts=artifacts)
    for record, _events_value, _artifacts in converted:
        store.get(str(record["id"]))
    with closing(connect_sqlite(path, row_factory=True)) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("DROP TABLE research_job_events")
        connection.execute("DROP TABLE research_jobs")
        connection.execute(f"PRAGMA user_version={_load_research_catalog().RESEARCH_SCHEMA_VERSION}")
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    _load_research_catalog().ResearchCatalog(path, read_only=True)


class ResearchJobLegacyMigrator:
    name = "research-jobs"
    backup_paths = (_CATALOG.as_posix(), "jobs.sqlite")

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        status, unknown = research_job_probe(root / _CATALOG)
        if status in {"absent", "retired"}:
            return ()
        return (research_job_record(status, unknown),)

    def migrate_batch(
        self,
        root: Path,
        *,
        after_key: str,
        limit: int,
    ) -> Iterable[MigrationRecord]:
        if after_key >= "research-lake" or int(limit) < 1:
            return ()
        path = root / _CATALOG
        status, unknown = research_job_probe(path)
        if status in {"absent", "retired"}:
            return ()
        if status == "conflict":
            return (research_job_record(status, unknown),)
        research_job_migrate(path, UnifiedJobStore(root / "jobs.sqlite"))
        return (research_job_record("converted"),)

    def rollback(self, root: Path, backup_root: Path) -> None:

        for relative in self.backup_paths:
            restore_backup_path(root, backup_root, relative)


research_job_legacy_migrator = ResearchJobLegacyMigrator()


# for_version: v1.0  (consolidated from quantmaster.data.legacy_migration)







class LegacyMigrationError(RuntimeError):
    """A migration cannot proceed without weakening its evidence boundary."""


@dataclass(frozen=True)
class OfflineMaintenanceEvidence:
    confirmed_root: Path
    writer_stopped: bool
    evidence: str


class _ProcessLease:
    """OS-released cross-process lease; a stale file is not mistaken for a held lock."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = path.open("a+b")
        self._handle.seek(0)
        if self._handle.read(1) == b"":
            self._handle.write(b"0")
            self._handle.flush()
        self._handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:  # pragma: no cover - Windows is the supported desktop runtime
                import fcntl

                fcntl.flock(  # type: ignore[attr-defined]
                    self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB,  # type: ignore[attr-defined]
                )
        except (OSError, BlockingIOError) as exc:
            self._handle.close()
            raise LegacyMigrationError("另一个进程正在执行离线迁移") from exc

    def close(self) -> None:
        if self._handle.closed:
            return
        self._handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:  # pragma: no cover
            import fcntl

            fcntl.flock(  # type: ignore[attr-defined]
                self._handle.fileno(), fcntl.LOCK_UN,  # type: ignore[attr-defined]
            )
        self._handle.close()


def registered_migrations() -> tuple[str, ...]:
    register_builtin_migrations()
    return registered_migrators()


def _legacy_utc_now() -> str:
    return datetime.now(UTC).isoformat()


class LegacyMigrationManager:
    """Persist migration progress so interruption never requires format guessing."""

    ACTIVE = frozenset({"queued", "backing_up", "running", "pausing", "rolling_back"})
    TERMINAL = frozenset({"completed", "failed", "cancelled", "rolled_back"})

    def __init__(
        self,
        root: str | Path | None = None,
        *,
        backup: Callable[[Path, Path], None] | None = None,
        backup_root: str | Path | None = None,
        stockdb_root: str | Path | None = None,
        offline_evidence: OfflineMaintenanceEvidence | None = None,
    ) -> None:
        self.root = Path(root or get_config().data_root).resolve()
        self.state_path = self.root / "legacy_contract_migrations.sqlite"
        self.backup_root = Path(
            backup_root or self.root / "backups" / "legacy-contracts"
        ).resolve()
        self.stockdb_root = Path(
            stockdb_root or get_config().free_stockdb_root
        ).resolve()
        self._backup = backup or self._backup_sqlite_files
        self._offline_evidence = offline_evidence
        self._lock = threading.RLock()
        self._threads: dict[str, threading.Thread] = {}
        self._leases: dict[str, MaintenanceLease] = {}
        self._pause_requests: set[str] = set()
        self._initialized = False

    def plan(self, domain: str) -> dict:
        """Return the complete read-only evidence card required before apply."""
        register_builtin_migrations()
        migrator = _MIGRATORS.get(domain)
        if migrator is None:
            raise LegacyMigrationError(f"未知迁移类型：{domain}")
        try:
            backup = preflight_backup_tree(
                self.root,
                self.backup_root,
                exclude={"legacy_contract_migrations.sqlite"},
                extra_paths=tuple(getattr(migrator, "backup_paths", ())),
            )
            records = tuple(migrator.inspect(self.root))
        except (MigrationError, OSError, sqlite3.Error, ValueError) as exc:
            raise LegacyMigrationError(str(exc)) from exc
        evidence = [
            {
                "record_key": record.record_key,
                "outcome": record.outcome,
                "diagnostic_code": record.diagnostic_code,
                "unknown_fields": list(record.unknown_fields),
                "detail": record.detail,
            }
            for record in records
        ]
        return {
            "schema_version": 1,
            "domain": domain,
            "data_root": str(backup.source_root),
            "stockdb_root": str(self.stockdb_root),
            "stockdb_exists": self.stockdb_root.is_dir(),
            "stockdb_action": "preserve_in_place",
            "backup_root": str(backup.target_root),
            "inventory": [
                {
                    "path": str(backup.source_root / entry.path),
                    "kind": entry.kind,
                    "exists": entry.exists,
                    "size_bytes": entry.size_bytes,
                }
                for entry in backup.entries
            ],
            "inventory_bytes": backup.total_bytes,
            "required_backup_bytes": backup.required_bytes,
            "free_backup_bytes": backup.free_bytes,
            "migration_evidence": evidence,
            "conflicts": [item for item in evidence if item["outcome"] == "conflict"],
            "rollback_limitations": [
                item["path"] for item in (
                    {
                        "path": str(backup.source_root / entry.path),
                        "exists": entry.exists,
                    }
                    for entry in backup.entries
                ) if not item["exists"]
            ],
        }

    def _conn(self) -> sqlite3.Connection:
        connection = connect_sqlite(self.state_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        if self._initialized:
            return
        self.root.mkdir(parents=True, exist_ok=True)
        with self._conn() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS migration_runs (
                    id TEXT PRIMARY KEY,
                    domain TEXT NOT NULL,
                    mode TEXT NOT NULL CHECK(mode IN ('dry_run','apply')),
                    status TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    total INTEGER NOT NULL DEFAULT 0,
                    checked INTEGER NOT NULL DEFAULT 0,
                    converted INTEGER NOT NULL DEFAULT 0,
                    blank INTEGER NOT NULL DEFAULT 0,
                    review INTEGER NOT NULL DEFAULT 0,
                    conflicts INTEGER NOT NULL DEFAULT 0,
                    last_key TEXT NOT NULL DEFAULT '',
                    last_batch INTEGER NOT NULL DEFAULT 0,
                    write_paused INTEGER NOT NULL DEFAULT 0,
                    estimated_remaining_seconds INTEGER,
                    backup_path TEXT NOT NULL DEFAULT '',
                    diagnostic_code TEXT NOT NULL DEFAULT '',
                    error TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    finished_at TEXT
                );
                CREATE UNIQUE INDEX IF NOT EXISTS one_active_migration
                    ON migration_runs((1))
                    WHERE status IN ('queued','backing_up','running','pausing','rolling_back');
                CREATE TABLE IF NOT EXISTS migration_audit (
                    run_id TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    batch INTEGER NOT NULL,
                    record_key TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    diagnostic_code TEXT NOT NULL DEFAULT '',
                    unknown_fields_json TEXT NOT NULL DEFAULT '[]',
                    detail TEXT NOT NULL DEFAULT '',
                    recorded_at TEXT NOT NULL,
                    PRIMARY KEY(run_id,record_key),
                    FOREIGN KEY(run_id) REFERENCES migration_runs(id)
                );
                """
            )
            connection.execute(
                "UPDATE migration_runs SET status='paused',phase='进程中断，可从最近批次续跑',"
                "write_paused=0,diagnostic_code='process_interrupted',updated_at=? "
                "WHERE status IN ('queued','backing_up','running','pausing','rolling_back')",
                (_load_automation_models().utc_now(),),
            )
        self._initialized = True

    def create(self, domain: str, *, mode: str = "dry_run", batch_size: int = 250) -> dict:
        register_builtin_migrations()
        if domain not in _MIGRATORS:
            raise LegacyMigrationError(f"未知迁移类型：{domain}")
        if mode not in {"dry_run", "apply"}:
            raise LegacyMigrationError("mode 仅支持 dry_run/apply")
        if not 1 <= int(batch_size) <= 5000:
            raise LegacyMigrationError("batch_size 必须在 1..5000")
        if mode == "apply":
            self._require_offline_evidence()
            plan = self.plan(domain)
            if plan["conflicts"]:
                raise LegacyMigrationError("迁移证据存在冲突，拒绝写入")
        self._initialize()
        with self._conn() as connection:
            active = connection.execute(
                "SELECT id FROM migration_runs WHERE status IN "
                "('queued','backing_up','running','pausing','rolling_back') LIMIT 1"
            ).fetchone()
        if active:
            raise LegacyMigrationError("已有历史合同迁移正在运行")
        run_id, now = uuid.uuid4().hex, _load_automation_models().utc_now()
        try:
            with self._conn() as connection:
                connection.execute(
                    "INSERT INTO migration_runs(id,domain,mode,status,phase,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (run_id, domain, mode, "queued", "等待开始", now, now),
                )
        except sqlite3.IntegrityError as exc:
            raise LegacyMigrationError("已有历史合同迁移正在运行") from exc
        thread = threading.Thread(
            target=self._run, args=(run_id, int(batch_size)), daemon=True,
            name=f"legacy-migration-{domain}",
        )
        with self._lock:
            self._threads[run_id] = thread
        thread.start()
        return self.get(run_id)

    def get(self, run_id: str) -> dict:
        if not self.state_path.is_file():
            raise KeyError(run_id)
        with self._conn() as connection:
            row = connection.execute(
                "SELECT * FROM migration_runs WHERE id=?", (run_id,),
            ).fetchone()
        if row is None:
            raise KeyError(run_id)
        value = dict(row)
        value["write_paused"] = bool(value["write_paused"])
        value["maintenance_mode"] = (
            "offline_writer_stop_verified" if value["write_paused"] else "not_active"
        )
        with self._lock:
            pause_requested = run_id in self._pause_requests
        if pause_requested and value["status"] in {"queued", "backing_up", "running"}:
            value["status"] = "pausing"
            value["phase"] = "正在安全暂停"
        value["unknown_results"] = self.unknown_results(run_id)
        return value

    def latest(self) -> dict | None:
        if not self.state_path.is_file():
            return None
        with self._conn() as connection:
            row = connection.execute(
                "SELECT id FROM migration_runs ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
        return self.get(str(row[0])) if row else None

    def unknown_results(self, run_id: str, limit: int = 50) -> list[dict]:
        if not self.state_path.is_file():
            return []
        with self._conn() as connection:
            rows = connection.execute(
                "SELECT record_key,diagnostic_code,unknown_fields_json,detail,batch "
                "FROM migration_audit WHERE run_id=? AND outcome IN ('blank','review','conflict') "
                "ORDER BY batch,record_key LIMIT ?", (run_id, max(1, min(limit, 500))),
            ).fetchall()
        return [
            {
                "record_key": row[0],
                "diagnostic_code": row[1],
                "unknown_fields": json.loads(row[2]),
                "detail": row[3],
                "batch": row[4],
            }
            for row in rows
        ]

    def pause(self, run_id: str) -> dict:
        self._initialize()
        task = self.get(run_id)
        if task["status"] in {"paused", *self.TERMINAL}:
            return task
        if task["status"] not in {"queued", "backing_up", "running"}:
            raise LegacyMigrationError("迁移当前无法暂停")
        with self._lock:
            self._pause_requests.add(run_id)
        task["status"] = "pausing"
        task["phase"] = "正在安全暂停"
        return task

    def resume(self, run_id: str, *, batch_size: int = 250) -> dict:
        self._initialize()
        task = self.get(run_id)
        if task["mode"] == "apply":
            self._require_offline_evidence()
        with self._conn() as connection:
            changed = connection.execute(
                "UPDATE migration_runs SET status='queued',phase='等待续跑',error='',"
                "diagnostic_code='',updated_at=? "
                "WHERE id=? AND status IN ('paused','failed')",
                (_load_automation_models().utc_now(), run_id),
            ).rowcount
        if not changed:
            raise LegacyMigrationError("只有已暂停或失败的迁移可以续跑")
        thread = threading.Thread(
            target=self._run, args=(run_id, int(batch_size)), daemon=True,
            name=f"legacy-migration-resume-{run_id[:8]}",
        )
        with self._lock:
            self._threads[run_id] = thread
        thread.start()
        return self.get(run_id)

    def rollback(self, run_id: str) -> dict:
        self._initialize()
        task = self.get(run_id)
        self._require_offline_evidence()
        if task["mode"] != "apply" or task["status"] not in {"completed", "failed", "paused"}:
            raise LegacyMigrationError("只有 completed/failed/paused 且有备份的 apply 迁移可以回滚")
        backup_path = Path(task["backup_path"])
        if not backup_path.is_dir():
            raise LegacyMigrationError("迁移备份不存在，拒绝回滚")
        validate_backup_tree(backup_path)
        process_lease = self._process_lease()
        lease = maintenance_barrier.enter(f"legacy_migration_rollback:{task['domain']}", timeout=30)
        try:
            with maintenance_barrier.authorize(lease):
                self._set(run_id, status="rolling_back", phase="从可恢复备份回滚", write_paused=1)
                _MIGRATORS[task["domain"]].rollback(self.root, backup_path)
                self._set(
                    run_id, status="rolled_back", phase="已回滚", write_paused=0,
                    finished_at=_load_automation_models().utc_now(),
                )
        except (OSError, RuntimeError, sqlite3.Error, ValueError) as exc:
            with maintenance_barrier.authorize(lease):
                self._set(
                    run_id, status="failed", phase="回滚失败", diagnostic_code="rollback_failed",
                    error=str(exc), write_paused=0,
                )
            raise
        finally:
            maintenance_barrier.exit(lease)
            process_lease.close()
        return self.get(run_id)

    def _run(self, run_id: str, batch_size: int) -> None:
        task = self.get(run_id)
        migrator = _MIGRATORS[task["domain"]]
        lease: MaintenanceLease | None = None
        process_lease: _ProcessLease | None = None
        started = time.monotonic()
        try:
            if task["mode"] == "apply":
                lease, process_lease = self._run_apply(
                    run_id, task, migrator, batch_size, started,
                )
                return
            else:
                total = sum(1 for _ in migrator.inspect(self.root))
                self._set(
                    run_id, status="running", phase="只读检查历史记录", total=total,
                )
                records = (
                    record for record in migrator.inspect(self.root)
                    if record.record_key > str(task["last_key"])
                )
                iterator = lambda after: self._take_after(records, batch_size)  # noqa: E731
            batch = int(task["last_batch"])
            after = str(task["last_key"])
            while True:
                if self._pause_requested(run_id):
                    self._set(run_id, status="paused", phase="已安全暂停", write_paused=0)
                    return
                values = list(iterator(after))
                if not values:
                    break
                batch += 1
                self._record_batch(run_id, task["domain"], batch, values, started)
                after = values[-1].record_key
                if len(values) < batch_size:
                    break
            finished = _load_automation_models().utc_now()
            self._set(
                run_id, status="completed", phase="迁移完成" if task["mode"] == "apply" else "检查完成",
                write_paused=0, estimated_remaining_seconds=0, finished_at=finished,
            )
        except (OSError, RuntimeError, sqlite3.Error, ValueError) as exc:
            failure = {
                "status": "failed", "phase": "迁移失败，可从最近批次续跑",
                "diagnostic_code": "migration_failed", "error": str(exc),
                "write_paused": 0,
            }
            if lease is None:
                self._set(run_id, **failure)
            else:
                with maintenance_barrier.authorize(lease):
                    self._set(run_id, **failure)
        finally:
            with self._lock:
                self._pause_requests.discard(run_id)
            if lease is not None:
                try:
                    maintenance_barrier.exit(lease)
                finally:
                    with self._lock:
                        self._leases.pop(run_id, None)
            if process_lease is not None:
                process_lease.close()

    def _run_apply(
        self, run_id: str, task: dict, migrator: DomainMigrator,
        batch_size: int, started: float,
    ) -> tuple[MaintenanceLease, _ProcessLease]:
        self._require_offline_evidence()
        process_lease = self._process_lease()
        lease = maintenance_barrier.enter(f"legacy_migration:{task['domain']}", timeout=30)
        try:
            with maintenance_barrier.authorize(lease):
                with self._lock:
                    self._leases[run_id] = lease
                backup = (
                    Path(task["backup_path"])
                    if task["backup_path"] else self.backup_root / run_id
                )
                if backup.is_dir():
                    validate_backup_tree(backup)
                else:
                    self._set(
                        run_id, status="backing_up", phase="创建可恢复备份",
                        write_paused=1, backup_path=str(backup),
                    )
                    self._backup(self.root, backup)
                    validate_backup_tree(backup)
                total = int(task["total"] or 0) or sum(1 for _ in migrator.inspect(self.root))
                self._set(run_id, status="running", phase="分批转换历史记录", total=total)
                self._apply_batches(run_id, task, migrator, batch_size, started)
        except (OSError, RuntimeError, sqlite3.Error, ValueError):
            maintenance_barrier.exit(lease)
            process_lease.close()
            with self._lock:
                self._leases.pop(run_id, None)
            raise
        return lease, process_lease

    def _apply_batches(
        self, run_id: str, task: dict, migrator: DomainMigrator, batch_size: int,
        started: float,
    ) -> None:
        batch = int(task["last_batch"])
        after = str(task["last_key"])
        while True:
            if self._pause_requested(run_id):
                self._set(run_id, status="paused", phase="已安全暂停", write_paused=0)
                return
            values = list(migrator.migrate_batch(
                self.root, after_key=after, limit=batch_size,
            ))
            if not values:
                break
            batch += 1
            self._record_batch(run_id, task["domain"], batch, values, started)
            after = values[-1].record_key
            if len(values) < batch_size:
                break
        self._set(
            run_id, status="completed", phase="迁移完成", write_paused=0,
            estimated_remaining_seconds=0, finished_at=_load_automation_models().utc_now(),
        )

    @staticmethod
    def _take_after(records: Iterable[MigrationRecord], limit: int) -> list[MigrationRecord]:
        result: list[MigrationRecord] = []
        for record in records:
            result.append(record)
            if len(result) >= limit:
                break
        return result

    def _record_batch(
        self, run_id: str, domain: str, batch: int,
        records: list[MigrationRecord], started: float,
    ) -> None:
        valid = {"converted", "blank", "review", "conflict", "unchanged"}
        if any(record.outcome not in valid for record in records):
            raise LegacyMigrationError("domain migrator 返回未知 outcome")
        now = _load_automation_models().utc_now()
        with self._conn() as connection:
            for record in records:
                connection.execute(
                    "INSERT OR REPLACE INTO migration_audit "
                    "(run_id,domain,batch,record_key,outcome,diagnostic_code,"
                    "unknown_fields_json,detail,recorded_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        run_id, domain, batch, record.record_key, record.outcome,
                        record.diagnostic_code,
                        json.dumps(record.unknown_fields, ensure_ascii=False),
                        record.detail, now,
                    ),
                )
            totals = connection.execute(
                "SELECT COUNT(*),"
                "SUM(outcome='converted'),SUM(outcome='blank'),SUM(outcome='review'),"
                "SUM(outcome='conflict') FROM migration_audit WHERE run_id=?",
                (run_id,),
            ).fetchone()
            checked = int(totals[0] or 0)
            total_row = connection.execute(
                "SELECT total FROM migration_runs WHERE id=?", (run_id,),
            ).fetchone()
            total = int(total_row[0] or 0)
            elapsed = max(0.001, time.monotonic() - started)
            estimated = round(elapsed / checked * max(0, total - checked)) if checked else None
            connection.execute(
                "UPDATE migration_runs SET checked=?,converted=?,blank=?,review=?,conflicts=?,"
                "last_key=?,last_batch=?,updated_at=?,estimated_remaining_seconds=? WHERE id=?",
                (
                    checked, int(totals[1] or 0), int(totals[2] or 0),
                    int(totals[3] or 0), int(totals[4] or 0), records[-1].record_key,
                    batch, now, estimated, run_id,
                ),
            )

    def _pause_requested(self, run_id: str) -> bool:
        with self._lock:
            return run_id in self._pause_requests

    def _set(self, run_id: str, **values: Any) -> None:
        if not values:
            return
        allowed = {
            "status", "phase", "total", "write_paused", "estimated_remaining_seconds",
            "backup_path", "diagnostic_code", "error", "finished_at",
        }
        if not set(values) <= allowed:
            raise ValueError("未知 migration_runs 字段")
        values["updated_at"] = _load_automation_models().utc_now()
        assignments = ",".join(f"{name}=?" for name in values)
        with self._conn() as connection:
            connection.execute(
                f"UPDATE migration_runs SET {assignments} WHERE id=?",
                (*values.values(), run_id),
            )

    @staticmethod
    def _backup_sqlite_files(root: Path, target: Path) -> None:
        task_domain = ""
        # target is <backup-root>/<run-id>; the manager resolves the run below.
        state = root / "legacy_contract_migrations.sqlite"
        if state.is_file():
            with connect_sqlite(state, read_only=True) as connection:
                row = connection.execute(
                    "SELECT domain FROM migration_runs WHERE id=?", (target.name,),
                ).fetchone()
                task_domain = str(row[0]) if row else ""
        migrator = _MIGRATORS.get(task_domain)
        extras = tuple(getattr(migrator, "backup_paths", ())) if migrator else ()
        backup_sqlite_tree(
            root, target, exclude={"legacy_contract_migrations.sqlite"}, extra_paths=extras,
        )

    def _require_offline_evidence(self) -> None:
        evidence = self._offline_evidence
        if (
            evidence is None
            or evidence.confirmed_root.resolve() != self.root
            or not evidence.writer_stopped
            or not evidence.evidence.strip()
        ):
            raise LegacyMigrationError(
                "apply/resume/rollback 仅允许离线维护：需精确 data root、已停写证据与跨进程 lease"
            )

    def _process_lease(self) -> _ProcessLease:
        return _ProcessLease(self.root / ".legacy-contract-maintenance.lock")


legacy_migration_manager = LegacyMigrationManager()

migration_manager = _MigrationManagerProxy()
