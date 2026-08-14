"""Explicit filesystem and SQLite boundaries for local runtime storage.

The helpers in this module deliberately do not guess a storage root from the
current working directory.  Callers must name the workspace, runtime instance,
purpose and access intent before a writable path can be resolved.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import uuid
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from quantmaster.runtime.sqlite import connect_sqlite_recovery

StorageAccess = Literal["read-only", "writable"]
StoragePurpose = Literal[
    "artifacts", "pytest-cache", "pytest-basetemp", "runtime", "database", "provider-cache",
]


class StorageBoundaryError(ValueError):
    """A requested path would cross an instance or task boundary."""


@dataclass(frozen=True)
class StorageRequest:
    workspace: Path
    runtime_instance: str
    purpose: StoragePurpose
    access: StorageAccess
    task_worktree: Path | None = None
    test_context: bool = False


@dataclass(frozen=True)
class ResolvedStorage:
    path: Path
    instance: str
    purpose: StoragePurpose
    access: StorageAccess
    task_slug: str = ""


def _absolute_directory(path: Path, label: str) -> Path:
    value = Path(path).expanduser()
    if not value.is_absolute():
        raise StorageBoundaryError(f"{label}必须使用绝对路径")
    return value.resolve()


def _safe_name(value: str, label: str) -> str:
    text = str(value or "").strip()
    if not text or any(character not in "abcdefghijklmnopqrstuvwxyz0123456789-" for character in text):
        raise StorageBoundaryError(f"{label}只允许小写字母、数字和单连字符")
    if text.startswith("-") or text.endswith("-") or "--" in text:
        raise StorageBoundaryError(f"{label}格式无效")
    return text


def resolve_storage(request: StorageRequest) -> ResolvedStorage:
    """Resolve one storage location without cwd or home-directory fallbacks."""

    workspace = _absolute_directory(request.workspace, "workspace")
    instance = _safe_name(request.runtime_instance, "runtime instance")
    task_slug = ""
    if request.task_worktree is not None:
        task = _absolute_directory(request.task_worktree, "task worktree")
        expected_parent = (workspace / ".worktrees").resolve()
        if task.parent != expected_parent:
            raise StorageBoundaryError("task worktree 不属于 workspace/.worktrees")
        task_slug = _safe_name(task.name, "task slug")

    if request.test_context and request.access == "writable" and not task_slug:
        raise StorageBoundaryError("测试写入必须绑定独占 task worktree")

    if task_slug:
        root = workspace / ".artifacts" / "worktrees" / task_slug
        mapping = {
            "artifacts": root,
            "pytest-cache": root / "pytest" / "cache",
            "pytest-basetemp": root / "pytest" / "runs",
            "runtime": root / "runtime" / instance,
            "database": root / "runtime" / instance / "databases",
            "provider-cache": root / "runtime" / instance / "provider-cache",
        }
    else:
        root = workspace / ".runtime-instances" / instance
        mapping = {
            "artifacts": workspace / ".artifacts" / "instances" / instance,
            "pytest-cache": workspace / ".artifacts" / "instances" / instance / "pytest-cache",
            "pytest-basetemp": workspace / ".artifacts" / "instances" / instance / "pytest-runs",
            "runtime": root,
            "database": root / "databases",
            "provider-cache": root / "provider-cache",
        }
    target = mapping[request.purpose].resolve()
    boundary = (workspace / (".artifacts" if task_slug else ".runtime-instances")).resolve()
    if request.purpose == "artifacts" and not task_slug:
        boundary = (workspace / ".artifacts").resolve()
    if target != boundary and not target.is_relative_to(boundary):
        raise StorageBoundaryError("解析结果越出存储边界")
    return ResolvedStorage(target, instance, request.purpose, request.access, task_slug)


@dataclass(frozen=True)
class ACLStatus:
    path: str
    owner: str
    inherited: bool | None
    readable: bool
    writable: bool
    error: str = ""


def inspect_acl(path: str | Path) -> ACLStatus:
    """Return useful ACL evidence without changing owner or permissions."""

    target = Path(path).resolve()
    readable = os.access(target, os.R_OK)
    writable = os.access(target, os.W_OK)
    if os.name != "nt":
        owner = str(target.stat().st_uid) if target.exists() else ""
        return ACLStatus(str(target), owner, None, readable, writable)
    script = (
        "$a=Get-Acl -LiteralPath $env:QM_ACL_TARGET -ErrorAction Stop;"
        "[Console]::OutputEncoding=[Text.Encoding]::UTF8;"
        "Write-Output ('OWNER=' + [string]$a.Owner);"
        "Write-Output ('INHERITED=' + [string](-not $a.AreAccessRulesProtected))"
    )
    environment = os.environ.copy()
    environment["QM_ACL_TARGET"] = str(target)
    timeouts: list[str] = []
    for attempt in range(1, 3):
        try:
            result = subprocess.run(
                ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
                env=environment, capture_output=True, text=True, encoding="utf-8",
                errors="replace", timeout=10, check=True,
            )
            lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
            values = dict(line.split("=", 1) for line in lines if "=" in line)
            return ACLStatus(
                str(target), values.get("OWNER", ""),
                values.get("INHERITED", "").lower() == "true" if "INHERITED" in values else None,
                readable, writable,
            )
        except subprocess.TimeoutExpired as exc:
            timeouts.append(f"attempt {attempt}/2 TimeoutExpired: {exc}")
            if attempt == 1:
                continue
            return ACLStatus(str(target), "", None, readable, writable, "; ".join(timeouts))
        except (OSError, subprocess.SubprocessError) as exc:
            return ACLStatus(str(target), "", None, readable, writable, f"{type(exc).__name__}: {exc}")
    raise AssertionError("unreachable ACL inspection retry state")


def prepare_writable_directory(path: str | Path, *, require_inheritance: bool = True) -> ACLStatus:
    """Pre-create a writable directory and verify Windows ACL inheritance."""

    target = Path(path).resolve()
    target.mkdir(parents=True, exist_ok=True)
    status = inspect_acl(target)
    if not status.writable:
        raise PermissionError(f"目录不可写: {target}; owner={status.owner or '未知'}")
    if os.name == "nt" and require_inheritance and status.inherited is False:
        raise PermissionError(
            f"目录未保留 Windows ACL 继承: {target}; owner={status.owner or '未知'}; {status.error}"
        )
    if os.name == "nt" and require_inheritance and status.inherited is None:
        raise RuntimeError(
            f"无法验证 Windows ACL 继承: {target}; owner={status.owner or '未知'}; {status.error}"
        )
    return status


def create_inheriting_temporary_directory(
    parent: str | Path, *, prefix: str = ".tmp-", attempts: int = 10,
) -> Path:
    """Atomically create a private temporary directory without severing Windows ACL inheritance."""

    root = Path(parent).resolve()
    prepare_writable_directory(root)
    for _attempt in range(max(1, int(attempts))):
        target = root / f"{prefix}{uuid.uuid4().hex}"
        try:
            target.mkdir(mode=0o777)
        except FileExistsError:
            continue
        try:
            prepare_writable_directory(target)
        except BaseException:
            shutil.rmtree(target, ignore_errors=True)
            raise
        return target
    raise FileExistsError(f"无法在安全重试次数内创建临时目录：{root / prefix}")


def classify_sqlite_error(error: BaseException) -> str:
    text = str(error).lower()
    if "locked" in text or "busy" in text:
        return "SQLITE_LOCKED"
    if "readonly" in text or "read-only" in text:
        return "SQLITE_READ_ONLY"
    if "full" in text or "disk is full" in text:
        return "STORAGE_SPACE_INSUFFICIENT"
    if "malformed" in text or "corrupt" in text or "not a database" in text:
        return "SQLITE_CORRUPT"
    if "unable to open" in text:
        return "SQLITE_OPEN_FAILED"
    if "disk i/o" in text or "input/output" in text:
        return "SQLITE_IO_ERROR"
    if isinstance(error, PermissionError):
        return "STORAGE_PERMISSION_DENIED"
    return "SQLITE_ERROR"


def diagnose_sqlite(path: str | Path) -> dict[str, object]:
    """Inspect a SQLite database and sidecars through a real read-only connection."""

    target = Path(path).expanduser()
    if not target.is_absolute():
        raise StorageBoundaryError("SQLite 诊断目标必须使用绝对路径")
    target = target.resolve()
    directory_acl = inspect_acl(target.parent)
    file_acl = inspect_acl(target) if target.exists() else ACLStatus(str(target), "", None, False, False)
    sidecars = []
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = target.with_name(target.name + suffix)
        if sidecar.exists():
            sidecars.append({
                "path": str(sidecar),
                "size": sidecar.stat().st_size,
                "acl": inspect_acl(sidecar).__dict__,
            })
    result: dict[str, object] = {
        "path": str(target), "exists": target.is_file(), "read_only": True,
        "directory_acl": directory_acl.__dict__, "file_acl": file_acl.__dict__, "sidecars": sidecars,
        "free_bytes": shutil.disk_usage(target.parent).free if target.parent.exists() else 0,
    }
    if not target.is_file():
        return {**result, "status": "error", "diagnostic_code": "SQLITE_MISSING"}
    try:
        with connect_sqlite_recovery(target, read_only=True) as connection:
            connection.execute("PRAGMA query_only=ON")
            result.update({
                "journal_mode": str(connection.execute("PRAGMA journal_mode").fetchone()[0]),
                "schema_version": int(connection.execute("PRAGMA user_version").fetchone()[0]),
                "quick_check": [str(row[0]) for row in connection.execute("PRAGMA quick_check")],
            })
        healthy = result["quick_check"] == ["ok"]
        return {
            **result,
            "status": "ok" if healthy else "error",
            "diagnostic_code": "OK" if healthy else "SQLITE_CORRUPT",
        }
    except (OSError, sqlite3.Error) as exc:
        return {**result, "status": "error", "diagnostic_code": classify_sqlite_error(exc), "error": str(exc)}


@dataclass(frozen=True)
class InstanceRepairTarget:
    database: Path
    instance_root: Path
    confirmation: str
    purpose: Literal["repair-instance-data"] = "repair-instance-data"
    test_context: bool = False
    dry_run: bool = False
    writer_active: bool = False
    maintenance_confirmed: bool = False


def validate_instance_repair_target(target: InstanceRepairTarget) -> Path:
    """Reject fixtures, dry-runs and ambiguous production repair targets."""

    database = Path(target.database).expanduser()
    root = Path(target.instance_root).expanduser()
    if not database.is_absolute() or not root.is_absolute():
        raise StorageBoundaryError("实例修复目标必须使用绝对路径")
    database, root = database.resolve(), root.resolve()
    if target.test_context or target.dry_run:
        raise StorageBoundaryError("测试 fixture 或 dry-run 不得指向实例修复模式")
    if target.writer_active or not target.maintenance_confirmed:
        raise StorageBoundaryError("实例存在活跃写入者或未确认维护屏障")
    if target.purpose != "repair-instance-data" or target.confirmation != str(database):
        raise StorageBoundaryError("必须逐字确认真实实例 DB 绝对路径")
    if not database.is_file() or not database.is_relative_to(root):
        raise StorageBoundaryError("实例 DB 不存在或越出已确认实例根")
    lowered = {part.lower() for part in database.parts}
    if ".artifacts" in lowered or ".worktrees" in lowered or ".test-runtime" in lowered:
        raise StorageBoundaryError("实例修复拒绝任务/测试存储目标")
    return database


def repair_instance_database(
    target: InstanceRepairTarget,
    migrate: Callable[[sqlite3.Connection], None],
    *,
    backup_directory: str | Path,
) -> dict[str, object]:
    """Back up, health-check and transactionally run one idempotent repair."""

    database = validate_instance_repair_target(target)
    before = diagnose_sqlite(database)
    if before.get("diagnostic_code") != "OK":
        raise sqlite3.DatabaseError(f"修复前健康检查失败: {before.get('diagnostic_code')}")
    backup_root = _absolute_directory(Path(backup_directory), "backup directory")
    backup_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    backup = backup_root / f"{database.name}.{stamp}.{uuid.uuid4().hex[:8]}.bak"
    partial = backup.with_suffix(backup.suffix + ".partial")
    with closing(connect_sqlite_recovery(database, read_only=True)) as source:
        with closing(connect_sqlite_recovery(partial)) as destination:
            source.backup(destination)
            if destination.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise sqlite3.DatabaseError("备份健康检查失败")
    with partial.open("rb+") as stream:
        os.fsync(stream.fileno())
    os.replace(partial, backup)
    with connect_sqlite_recovery(database) as connection:
        tables = [
            str(row[0]) for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        ]
        before_counts = {
            table: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
            for table in tables
        }
        before_changes = connection.total_changes
        connection.execute("BEGIN IMMEDIATE")
        # The connection context rolls back any exception from the caller's
        # migration and preserves the verified backup for explicit recovery.
        migrate(connection)
        connection.commit()
        changes = connection.total_changes - before_changes
        after_counts = {
            table: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
            for table in tables
        }
    after = diagnose_sqlite(database)
    if after.get("diagnostic_code") != "OK":
        raise sqlite3.DatabaseError(f"修复后健康检查失败: {after.get('diagnostic_code')}")
    return {
        "database": str(database), "backup": str(backup), "changes": changes,
        "row_counts_before": before_counts, "row_counts_after": after_counts,
        "before": before, "after": after, "completed_at": datetime.now(UTC).isoformat(),
    }
