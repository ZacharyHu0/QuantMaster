"""数据根目录后台复制迁移：校验完成前绝不切换配置。"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import threading
import uuid
from contextlib import closing
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime
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
    # 本地 CSRF 管理操作有意允许用户选择任意绝对数据目录；_preflight 会拒绝
    # 嵌套、覆盖、符号链接和不可用目标。
    return candidate.resolve()


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
    if value.get("schema_version") != 1 or not isinstance(value.get("entries"), list):
        raise MigrationError("备份完成标记无效")
    return value


def validate_backup_tree(root: Path) -> dict:
    """Require a finalized marker and re-check every backed-up SQLite database."""
    value = _backup_manifest(root)
    for entry in value["entries"]:
        if entry.get("kind") != "sqlite" or not entry.get("exists"):
            continue
        path = root / str(entry["path"])
        if not path.is_file():
            raise MigrationError(f"备份文件丢失: {entry['path']}")
        with closing(sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)) as connection:
            result = connection.execute("PRAGMA integrity_check").fetchone()
            if not result or result[0] != "ok":
                raise MigrationError(f"SQLite 备份校验失败: {entry['path']}")
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
        _copy_sqlite(source, staging / relative)
        entries.append({"path": relative.as_posix(), "kind": "sqlite", "exists": True})
    return entries


def _backup_extra_entry(source_root: Path, staging: Path, raw: str) -> dict[str, object]:
    relative = Path(raw)
    if relative.is_absolute() or ".." in relative.parts:
        raise MigrationError(f"额外备份路径越界: {raw}")
    if relative.parts and relative.parts[0].casefold() == "backups":
        raise MigrationError(f"额外备份路径不能指向历史备份树: {raw}")
    source = source_root / relative
    exists = source.exists()
    kind = (
        "sqlite" if relative.suffix.lower() in {".sqlite", ".sqlite3", ".db"}
        else "directory" if source.is_dir() else "file"
    )
    entry: dict[str, object] = {
        "path": relative.as_posix(), "kind": kind, "exists": exists,
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
        relative = Path(raw)
        if relative.is_absolute() or ".." in relative.parts:
            raise MigrationError(f"额外备份路径越界: {raw}")
        if relative.parts and relative.parts[0].casefold() == "backups":
            raise MigrationError(f"额外备份路径不能指向历史备份树: {raw}")
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
            "schema_version": 1,
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
