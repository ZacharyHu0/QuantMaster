"""Auditable recovery for the paper database; dry-run unless ``--apply``."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from quantmaster.backtest.paper_accounts import PAPER_SCHEMA_VERSION, PaperStore
from quantmaster.config import get_config
from quantmaster.runtime.sqlite import connect_sqlite, reset_sqlite_runtime_for_tests
from quantmaster.runtime.worker_ipc import call_worker_command

PAPER_TABLES = (
    "paper_accounts", "paper_cycles", "paper_orders", "paper_auto_runs",
    "paper_order_fills", "paper_order_events", "paper_recovery_audit",
)


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def inspect_sqlite(path: Path) -> dict[str, Any]:
    resolved = path.resolve(strict=True)
    with connect_sqlite(resolved, read_only=True) as conn:
        tables = _tables(conn)
        return {
            "path": str(resolved),
            "user_version": int(conn.execute("PRAGMA user_version").fetchone()[0]),
            "quick_check": str(conn.execute("PRAGMA quick_check").fetchone()[0]),
            "counts": {
                table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in sorted(tables)
                if table in {*PAPER_TABLES, "trades", "cashflows"}
            },
        }


def inspect_database(path: Path) -> dict[str, Any]:
    value = inspect_sqlite(path)
    if "paper_orders" not in value["counts"]:
        raise ValueError("目标不是 QuantMaster paper 数据库")
    return value


def _column(conn: sqlite3.Connection, table: str, name: str, fallback: str) -> str:
    columns = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    return name if name in columns else f"{fallback} AS {name}"


def recovery_plan(path: Path, *, now: float | None = None) -> dict[str, Any]:
    snapshot = inspect_database(path)
    current = time.time() if now is None else float(now)
    with connect_sqlite(path.resolve(), read_only=True, row_factory=True) as conn:
        accounts = {
            str(row["id"]): dict(row)
            for row in conn.execute("SELECT id,status FROM paper_accounts")
        }
        cycles = {
            str(row["id"]): dict(row)
            for row in conn.execute("SELECT id,status,signal_date FROM paper_cycles")
        }
        version = _column(conn, "paper_orders", "version", "0")
        rows: list[dict[str, Any]] = []
        for raw in conn.execute(
            f"SELECT id,cycle_id,account_id,symbol,status,reason,{version} "
            "FROM paper_orders ORDER BY created_at,id"
        ):
            order, target = dict(raw), str(raw["status"])
            account = accounts.get(str(raw["account_id"]), {})
            cycle = cycles.get(str(raw["cycle_id"]), {})
            reason = "preserve"
            if target in {"skipped", "superseded", "filled", "cancelled", "expired", "rejected", "unproven"}:
                reason = "terminal_preserved_no_fill_invented"
            elif target in {"queued", "blocked", "proposed"}:
                target, reason = "waiting_market_data", "market_data_gap"
                if account.get("status") != "active":
                    reason += ";account_paused"
            rows.append({
                "kind": "order", "id": order["id"], "from": order["status"],
                "to": target, "reason": reason, "expected_version": order["version"],
                "symbol": order["symbol"], "required_start": cycle.get("signal_date") or "",
                "required_end": cycle.get("signal_date") or "",
            })
        diagnostic = _column(conn, "paper_auto_runs", "diagnostic_code", "''")
        for raw in conn.execute(
            "SELECT run_date,account_id,status,lease_owner,lease_expires,heartbeat_at,"
            f"last_error,failure_code,next_retry_at,attempts,{diagnostic} "
            "FROM paper_auto_runs ORDER BY run_date,account_id"
        ):
            run = dict(raw)
            code, task_status = "", "idle"
            if run["status"] == "running" and not run["lease_owner"]:
                code, task_status = "owner_missing", "orphaned"
            elif run["status"] == "running" and float(run["lease_expires"] or 0) < current:
                code, task_status = "lease_expired", "stalled"
            elif run["status"] == "manual_recovery":
                code, task_status = str(run["failure_code"] or "manual_recovery"), "manual_attention"
            elif run["status"] == "failed":
                code, task_status = (
                    ("retry_due", "retry_wait")
                    if float(run["next_retry_at"] or 0) <= current
                    else ("retry_scheduled", "retry_wait")
                )
            rows.append({
                "kind": "run", "id": f"{run['run_date']}:{run['account_id']}",
                "from": run["status"], "to": run["status"], "task_status": task_status,
                "diagnostic_code": code, "current_diagnostic": run["diagnostic_code"],
                "next_attempt": run["next_retry_at"], "attempts": run["attempts"],
                "last_error": run["last_error"], "failure_code": run["failure_code"],
                "expected": {
                    "status": run["status"], "lease_owner": run["lease_owner"],
                    "lease_expires": run["lease_expires"], "heartbeat_at": run["heartbeat_at"],
                    "next_retry_at": run["next_retry_at"], "attempts": run["attempts"],
                },
            })
    return {**snapshot, "rows": rows}


def ledger_paths(path: Path) -> list[Path]:
    root = path.resolve().parent / "paper_accounts"
    return sorted(root.glob("*/ledger.sqlite")) if root.is_dir() else []


def _assert_test_path(path: Path) -> None:
    resolved = path.resolve()
    configured = (get_config().data_root / "paper.sqlite").resolve()
    if resolved == configured:
        raise ValueError("测试数据库不得指向原实例")
    if not any(part.lower().startswith(("tmp", "temp", ".task-basetemp", "test")) for part in resolved.parts):
        raise ValueError("--test-db 仅允许临时测试路径")


@contextmanager
def maintenance_lease(path: Path, *, test_db: bool) -> Iterator[dict[str, Any]]:
    if test_db:
        _assert_test_path(path)
        yield {"token": "test-bypass", "pid": os.getpid(), "test_bypass": True}
        return
    configured = (get_config().data_root / "paper.sqlite").resolve()
    if path.resolve() != configured:
        raise ValueError("production apply 必须精确指向 configured paper.sqlite")
    lease = call_worker_command(
        "maintenance.enter", {"reason": "paper recovery", "timeout": 30},
        timeout=35, root=path.resolve().parent,
    )
    try:
        yield lease
    finally:
        call_worker_command(
            "maintenance.exit", {"token": lease["token"]}, timeout=10,
            root=path.resolve().parent,
        )


def _verify_lease(path: Path, lease: dict[str, Any], *, test_db: bool) -> None:
    if test_db:
        if lease.get("token") != "test-bypass":
            raise RuntimeError("测试维护租约无效")
        return
    status = call_worker_command(
        "maintenance.status", {"token": lease["token"]}, timeout=2,
        root=path.resolve().parent,
    )
    if not status.get("valid") or status.get("pid") != lease.get("pid"):
        raise RuntimeError("跨进程维护租约已失效")


def online_backup(source: Path, destination: Path) -> None:
    if destination.exists():
        raise ValueError(f"备份目标已存在：{destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with connect_sqlite(source, read_only=True) as src, connect_sqlite(destination) as dst:
        src.backup(dst)


def _overwrite_from_backup(source: Path, destination: Path) -> None:
    """Atomically replace one live SQLite image through SQLite's backup transaction."""

    with sqlite3.connect(source) as src, sqlite3.connect(destination) as dst:
        src.backup(dst)
        if str(dst.execute("PRAGMA quick_check").fetchone()[0]) != "ok":
            raise RuntimeError(f"SQLite restore quick_check failed: {destination}")


def _backup_set(path: Path, operation: Path) -> dict[str, Any]:
    if operation.exists():
        raise ValueError(f"operation 目录已存在：{operation}")
    operation.mkdir(parents=True)
    paper = operation / "paper.sqlite"
    online_backup(path, paper)
    items = [{"kind": "paper", "source": str(path.resolve()), "backup": str(paper),
              "before": inspect_database(path)}]
    for ledger in ledger_paths(path):
        saved = operation / "ledgers" / ledger.parent.name / "ledger.sqlite"
        online_backup(ledger, saved)
        items.append({"kind": "ledger", "account_id": ledger.parent.name,
                      "source": str(ledger.resolve()), "backup": str(saved),
                      "before": inspect_sqlite(ledger)})
    manifest = {"operation_id": operation.name, "created_at": datetime.now(UTC).isoformat(),
                "items": items}
    (operation / "manifest.json").write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return manifest


def _load_manifest(operation: Path) -> dict[str, Any]:
    value = json.loads((operation / "manifest.json").read_text(encoding="utf-8"))
    for item in value.get("items") or []:
        actual = (
            inspect_database(Path(item["backup"]))
            if item["kind"] == "paper"
            else inspect_sqlite(Path(item["backup"]))
        )
        if actual["quick_check"] != "ok" or actual["counts"] != item["before"]["counts"]:
            raise ValueError(f"备份 manifest 校验失败：{item['backup']}")
    return value


def _restore_manifest(operation: Path) -> None:
    manifest = _load_manifest(operation)
    staging = operation / "restore-staging" / uuid.uuid4().hex
    staging.mkdir(parents=True)
    staged: list[tuple[Path, Path, Path]] = []
    for index, item in enumerate(manifest["items"]):
        source, target = Path(item["backup"]), Path(item["source"])
        temporary = staging / f"{index}-restore.sqlite"
        shutil.copy2(source, temporary)
        check = inspect_database(temporary) if item["kind"] == "paper" else inspect_sqlite(temporary)
        if check["counts"] != item["before"]["counts"] or check["quick_check"] != "ok":
            raise ValueError(f"临时恢复校验失败：{target}")
        current = staging / f"{index}-current.sqlite"
        online_backup(target, current)
        staged.append((temporary, target, current))
    reset_sqlite_runtime_for_tests()
    replaced: list[tuple[Path, Path]] = []
    try:
        for temporary, target, current in staged:
            _overwrite_from_backup(temporary, target)
            replaced.append((current, target))
    except Exception as replace_error:
        reverse_failures: list[str] = []
        for current, target in reversed(replaced):
            try:
                _overwrite_from_backup(current, target)
            except Exception as reverse_error:
                reverse_failures.append(f"{target}: {type(reverse_error).__name__}: {reverse_error}")
        if reverse_failures:
            raise RuntimeError(
                "FATAL: rollback replacement failed and reverse restore was incomplete; "
                + "; ".join(reverse_failures)
            ) from replace_error
        raise
    for item in manifest["items"]:
        actual = (
            inspect_database(Path(item["source"]))
            if item["kind"] == "paper"
            else inspect_sqlite(Path(item["source"]))
        )
        if actual["counts"] != item["before"]["counts"]:
            raise RuntimeError(f"恢复后计数不一致：{item['source']}")
    reset_sqlite_runtime_for_tests()


def _actual_plan_rows(conn: sqlite3.Connection, plan: dict[str, Any]) -> list[dict[str, Any]]:
    """Capture the real post-DML values for every planned row in the same transaction."""

    rows: list[dict[str, Any]] = []
    for planned in plan["rows"]:
        if planned["kind"] == "order":
            actual = conn.execute(
                "SELECT id,status,waiting_reason,next_check_at,integrity_code,version "
                "FROM paper_orders WHERE id=?", (planned["id"],),
            ).fetchone()
            fills = [
                dict(row) for row in conn.execute(
                    "SELECT fill_key,quantity,price,fee,filled_at,market_ref,rule_version "
                    "FROM paper_order_fills WHERE order_id=? ORDER BY id", (planned["id"],),
                )
            ]
            rows.append({"kind": "order", "id": planned["id"], "actual": dict(actual),
                         "fills": fills})
        else:
            run_date, account_id = planned["id"].split(":", 1)
            actual = conn.execute(
                "SELECT run_date,account_id,status,attempts,next_retry_at,lease_owner,"
                "lease_expires,heartbeat_at,last_error,failure_code,diagnostic_code "
                "FROM paper_auto_runs WHERE run_date=? AND account_id=?",
                (run_date, account_id),
            ).fetchone()
            rows.append({"kind": "run", "id": planned["id"], "task_status": planned["task_status"],
                         "actual": dict(actual)})
    return rows


def _needs_apply(plan: dict[str, Any]) -> bool:
    if plan["user_version"] < PAPER_SCHEMA_VERSION:
        return True
    return any(
        row["from"] != row["to"]
        or (row["kind"] == "run" and row["diagnostic_code"] != row["current_diagnostic"])
        for row in plan["rows"]
    )


def _validate_apply_target(path: Path, confirmed_path: str, *, test_db: bool) -> None:
    if path != Path(confirmed_path).resolve():
        raise ValueError("--confirm-path 必须与目标数据库精确一致")
    if test_db:
        _assert_test_path(path)
        return
    if path != (get_config().data_root / "paper.sqlite").resolve():
        raise ValueError("production apply 必须指向 configured paper.sqlite")


def _apply_planned_row(
    conn: sqlite3.Connection, row: dict[str, Any], *, changed_at: str,
) -> int | None:
    if row["kind"] == "order" and row["from"] != row["to"]:
        return int(conn.execute(
            "UPDATE paper_orders SET status=?,waiting_reason=?,next_check_at=?,"
            "version=version+1,updated_at=? WHERE id=? AND version=? AND status=?",
            (
                row["to"], row["reason"], changed_at, changed_at, row["id"],
                row["expected_version"], row["from"],
            ),
        ).rowcount)
    if row["kind"] != "run" or row["diagnostic_code"] == row["current_diagnostic"]:
        return None
    expected = row["expected"]
    run_date, account_id = row["id"].split(":", 1)
    return int(conn.execute(
        "UPDATE paper_auto_runs SET diagnostic_code=?,updated_at=? WHERE "
        "run_date=? AND account_id=? AND status IS ? AND lease_owner IS ? AND "
        "lease_expires IS ? AND heartbeat_at IS ? AND next_retry_at IS ? AND attempts IS ?",
        (
            row["diagnostic_code"], changed_at, run_date, account_id,
            expected["status"], expected["lease_owner"], expected["lease_expires"],
            expected["heartbeat_at"], expected["next_retry_at"], expected["attempts"],
        ),
    ).rowcount)


def _restore_failed_apply(
    operation: Path,
    path: Path,
    before: dict[str, Any],
    recovery_error: Exception,
) -> None:
    _restore_manifest(operation)
    restored = inspect_database(path)
    if (
        restored["user_version"] != before["user_version"]
        or restored["counts"] != before["counts"]
    ):
        raise RuntimeError("自动恢复验证失败") from recovery_error


def apply_recovery(
    path: Path, backup_root: Path, *, confirmed_path: str, test_db: bool = False,
    fail_after: int | None = None,
) -> dict[str, Any]:
    path = path.resolve()
    _validate_apply_target(path, confirmed_path, test_db=test_db)
    before = recovery_plan(path)
    if before["user_version"] not in range(4, PAPER_SCHEMA_VERSION + 1):
        raise ValueError(f"恢复仅接受 user_version=4..{PAPER_SCHEMA_VERSION}")
    if not _needs_apply(before):
        return {"noop": True, "affected_rows": 0, "before": before, "after": inspect_database(path)}
    operation = backup_root.resolve() / f"paper-recovery-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex}"
    with maintenance_lease(path, test_db=test_db) as lease:
        _verify_lease(path, lease, test_db=test_db)
        manifest = _backup_set(path, operation)
        try:
            _verify_lease(path, lease, test_db=test_db)
            if before["user_version"] < PAPER_SCHEMA_VERSION:
                PaperStore.migrate_legacy_database(
                    path, path.parent / "paper_accounts",
                )
            plan = recovery_plan(path)
            affected = 0
            with connect_sqlite(path, row_factory=True) as conn:
                conn.execute("BEGIN IMMEDIATE")
                _verify_lease(path, lease, test_db=test_db)
                before_rows = _actual_plan_rows(conn, plan)
                before_counts = {
                    table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                    for table in sorted(_tables(conn)) if table in PAPER_TABLES
                }
                ledger_counts = {
                    item.get("account_id", "paper"): item["before"]["counts"]
                    for item in manifest["items"]
                }
                for index, row in enumerate(plan["rows"], start=1):
                    if fail_after is not None and index > fail_after:
                        raise RuntimeError("故障注入")
                    changed = _apply_planned_row(
                        conn, row, changed_at=datetime.now(UTC).isoformat(),
                    )
                    if changed is None:
                        continue
                    if changed != 1:
                        raise RuntimeError(f"CAS 前置条件失效：{row['id']}")
                    affected += 1
                after_rows = _actual_plan_rows(conn, plan)
                after_counts = {
                    table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                    for table in sorted(_tables(conn)) if table in PAPER_TABLES
                }
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS paper_recovery_audit (id INTEGER PRIMARY KEY "
                    "AUTOINCREMENT,operation TEXT NOT NULL UNIQUE,before_json TEXT NOT NULL,"
                    "after_json TEXT NOT NULL,backup_path TEXT NOT NULL,affected_rows INTEGER "
                    "NOT NULL,created_at TEXT NOT NULL)"
                )
                conn.execute(
                    "INSERT INTO paper_recovery_audit(operation,before_json,after_json,backup_path,"
                    "affected_rows,created_at) VALUES (?,?,?,?,?,?)",
                    (manifest["operation_id"], json.dumps({"snapshot": before, "rows": before_rows,
                                                           "counts": before_counts,
                                                           "ledger_counts": ledger_counts}, sort_keys=True),
                     json.dumps({"rows": after_rows, "counts": after_counts,
                                 "ledger_counts": ledger_counts}, sort_keys=True), str(operation),
                     affected, datetime.now(UTC).isoformat()),
                )
            after = recovery_plan(path)
            (operation / "after.json").write_text(json.dumps(after, sort_keys=True), encoding="utf-8")
            return {"noop": False, "operation": str(operation), "affected_rows": affected,
                    "before": before, "after": after, "manifest": manifest}
        except Exception as recovery_error:
            _restore_failed_apply(operation, path, before, recovery_error)
            raise


def rollback(
    database: Path, operation: Path, *, confirmed_path: str, test_db: bool = False,
) -> None:
    database = database.resolve()
    if database != Path(confirmed_path).resolve():
        raise ValueError("--confirm-path 必须与目标数据库精确一致")
    with maintenance_lease(database, test_db=test_db) as lease:
        _verify_lease(database, lease, test_db=test_db)
        manifest = _load_manifest(operation.resolve())
        papers = [item for item in manifest["items"] if item["kind"] == "paper"]
        if len(papers) != 1 or Path(papers[0]["source"]).resolve() != database:
            raise ValueError("rollback manifest 与目标 paper 数据库不匹配")
        _verify_lease(database, lease, test_db=test_db)
        _restore_manifest(operation.resolve())


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser()
    value.add_argument("database", type=Path)
    value.add_argument("--apply", action="store_true")
    value.add_argument("--confirm-path", default="")
    value.add_argument("--backup-root", type=Path)
    value.add_argument("--operation", type=Path)
    value.add_argument("--test-db", action="store_true")
    value.add_argument("--rollback", action="store_true")
    return value


def main() -> int:
    args = parser().parse_args()
    target = args.database.resolve()
    if not args.apply:
        print(json.dumps({"dry_run": True, **recovery_plan(target)}, ensure_ascii=False, indent=2))
        return 0
    if args.rollback:
        if args.operation is None:
            raise SystemExit("rollback 必须提供 --operation")
        rollback(target, args.operation, confirmed_path=args.confirm_path, test_db=args.test_db)
        return 0
    if args.backup_root is None:
        raise SystemExit("apply 必须提供 --backup-root")
    result = apply_recovery(
        target, args.backup_root, confirmed_path=args.confirm_path, test_db=args.test_db,
    )
    print(json.dumps({"dry_run": False, **result}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
