"""Side-effect-free public storage diagnostics.

This module never opens a writable SQLite connection.  In particular, the
free-stockdb control mailbox lives outside ``data_root`` and may have an active
writer, so diagnostics inspect its main file with SQLite's immutable read-only
mode and report sidecars separately rather than creating ``-shm`` or changing
the journal mode.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from quantmaster.config import get_config
from quantmaster.runtime.sqlite import connect_sqlite_diagnostic


def _configured_stockdb_root() -> Path:
    value = Path(get_config().data.free_stockdb_root).expanduser()
    return value.resolve() if value.is_absolute() else (Path.cwd() / value).resolve()


def _control_path() -> Path:
    configured = os.environ.get("QM_FREE_STOCKDB_CONTROL_PATH", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return _configured_stockdb_root() / ".quantmaster-control.sqlite"


def _display_path(path: Path) -> str:
    """Return an instance-relative label without exposing a user profile."""

    root = _configured_stockdb_root()
    try:
        relative = path.resolve().relative_to(root)
    except (OSError, ValueError):
        return f"<configured-instance>/{path.name}"
    return f"<configured-instance>/{relative.as_posix()}"


def _iso_timestamp(value: object) -> str:
    try:
        timestamp = float(value or 0)
    except (TypeError, ValueError):
        return ""
    if timestamp <= 0:
        return ""
    return datetime.fromtimestamp(timestamp, UTC).isoformat()


def _owner_writer_count(root: Path) -> int:
    marker = root / ".quantmaster-stockdb-owner.json"
    try:
        value = json.loads(marker.read_text(encoding="utf-8"))
        process = dict(value.get("process") or {})
        pid = int(process.get("pid") or 0)
        expected = Path(str(process.get("image") or "")).resolve()
        configured = (root / "stockdb.exe").resolve()
        if pid <= 0 or expected != configured:
            return 0
        from quantmaster.data.free_stockdb_runtime import FreeStockDBRuntime

        identity = FreeStockDBRuntime._process_identity(pid)
        return int(bool(
            identity
            and int(identity.get("created") or 0) == int(process.get("created") or 0)
            and Path(str(identity.get("image") or "")).resolve() == configured
        ))
    except (ImportError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0


def _decode_mapping(value: object) -> dict[str, Any]:
    try:
        decoded = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _state_summary(connection: sqlite3.Connection) -> dict[str, Any]:
    row = connection.execute(
        "SELECT payload_json,updated_at FROM runtime_state WHERE singleton=1"
    ).fetchone()
    if not row:
        return {}
    payload = _decode_mapping(row[0])
    update_result = str(payload.get("update_result") or "")
    state = str(payload.get("state") or "")
    return {
        "last_success_at": _iso_timestamp(row[1]) if update_result == "success" else "",
        "last_error": (
            "free-stockdb 控制状态最近一次更新失败"
            if update_result == "failed" or state in {"degraded", "error"} else ""
        ),
    }


def _command_summary(connection: sqlite3.Connection) -> dict[str, Any]:
    rows = connection.execute(
        "SELECT action,status,completed_at,result_json FROM commands "
        "ORDER BY created_at DESC LIMIT 100"
    ).fetchall()
    successes = [
        float(row[2] or 0) for row in rows
        if str(row[1]) == "completed"
        and (
            (payload := _decode_mapping(row[3])).get("success") is True
            or payload.get("status") in {"completed", "applied"}
        )
    ]
    return {
        "last_success_at": _iso_timestamp(max(successes)) if successes else "",
        "affected_tasks": sorted({
            str(row[0]) for row in rows if str(row[1]) in {"queued", "running"}
        }),
    }


def _runtime_summary(connection: sqlite3.Connection) -> dict[str, Any]:
    result: dict[str, Any] = {
        "last_success_at": "", "last_error": "", "affected_tasks": [],
    }
    tables = {
        str(row[0]) for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "runtime_state" in tables:
        result.update(_state_summary(connection))
    if "commands" in tables:
        commands = _command_summary(connection)
        result["affected_tasks"] = commands["affected_tasks"]
        if commands["last_success_at"]:
            result["last_success_at"] = commands["last_success_at"]
    return result


def storage_status() -> dict[str, Any]:
    """Build the stable, path-redacted storage status used by diagnostics UI."""

    cfg = get_config()
    data_root = cfg.data_root
    control = _control_path()
    stockdb_root = _configured_stockdb_root()
    wal = control.with_name(f"{control.name}-wal")
    shm = control.with_name(f"{control.name}-shm")
    journal = control.with_name(f"{control.name}-journal")
    wal_present = wal.is_file() and wal.stat().st_size > 0
    status: dict[str, Any] = {
        "status": "ready" if data_root.is_dir() else "unavailable",
        "purpose": "核心数据与 StockDB 控制状态",
        "instance": "configured-runtime",
        "access": "read-write",
        "diagnostic_access": "read-only",
        "display_path": _display_path(control),
        "free_bytes": None,
        "estimated_bytes": None,
        "database": control.name,
        "journal_mode": "unknown",
        "wal_present": wal_present,
        "shm_present": shm.is_file(),
        "journal_present": journal.is_file(),
        "active_writers": _owner_writer_count(stockdb_root),
        "last_success_at": "",
        "last_error": "",
        "affected_tasks": [],
        "diagnostic_code": "STORAGE_READY",
        "quick_check": "not_run",
    }
    try:
        status["free_bytes"] = shutil.disk_usage(control.parent).free
    except OSError:
        status["diagnostic_code"] = "PARENT_UNAVAILABLE"
    if not control.is_file():
        status["diagnostic_code"] = "CONTROL_DB_MISSING"
        return status
    try:
        status["estimated_bytes"] = (
            control.stat().st_size
            + (wal.stat().st_size if wal.is_file() else 0)
            + (shm.stat().st_size if shm.is_file() else 0)
        )
        with connect_sqlite_diagnostic(control) as connection:
            status["journal_mode"] = str(
                connection.execute("PRAGMA journal_mode").fetchone()[0]
            ).lower()
            messages = [
                str(row[0]) for row in connection.execute("PRAGMA quick_check").fetchall()
            ]
            status["quick_check"] = "ok" if messages == ["ok"] else "failed"
            status.update(_runtime_summary(connection))
        if status["quick_check"] != "ok":
            status["status"] = "degraded"
            status["diagnostic_code"] = "SQLITE_CORRUPT"
            status["last_error"] = "StockDB 控制库主文件完整性检查未通过"
        elif wal_present:
            status["status"] = "degraded"
            status["diagnostic_code"] = "WAL_REQUIRES_QUIESCENT_CHECK"
            status["last_error"] = "存在未合并 WAL；需协调写入者后复验完整状态"
        elif status["active_writers"]:
            status["diagnostic_code"] = "ACTIVE_WRITER"
        else:
            status["diagnostic_code"] = "SQLITE_OK"
    except (OSError, sqlite3.Error, TypeError, ValueError, IndexError):
        status["status"] = "degraded"
        status["diagnostic_code"] = "SQLITE_UNREADABLE"
        status["last_error"] = "StockDB 控制库只读诊断未完成，请查看本机日志"
    return status
