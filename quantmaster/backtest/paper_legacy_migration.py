"""One-shot migration for the retired single paper-trading ledger."""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, NamedTuple

from quantmaster.data.legacy_migration import MigrationRecord
from quantmaster.runtime.sqlite import connect_sqlite

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


class LegacyLedgerEvidence(NamedTuple):
    outcome: Literal["converted", "blank", "review", "conflict", "unchanged"]
    detail: str


def _record(evidence: LegacyLedgerEvidence) -> MigrationRecord:
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


def _copy_and_insert(root: Path, source: Path, store, account_id: str) -> MigrationRecord:
    destination = store.ledger_path(account_id)
    if destination.exists():
        return _record(LegacyLedgerEvidence("conflict", "目标账本路径已存在，拒绝覆盖"))
    try:
        _copy_ledger(source, destination)
        _insert_import(store, account_id)
    except sqlite3.IntegrityError:
        existing = _existing_account(root)
        if not existing:
            _remove_destination(destination)
            raise
        _remove_destination(destination)
        return _record(LegacyLedgerEvidence("unchanged", f"已迁移到账户 {existing}"))
    except (OSError, sqlite3.Error, ValueError):
        _remove_destination(destination)
        raise
    return _record(_inspect_source(source))


class PaperLegacyMigrator:
    """Import ledger facts once without inventing a current trading strategy."""

    name = "paper-ledger"

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        source = root / SOURCE_NAME
        if not source.is_file():
            return ()
        account_id = _existing_account(root)
        if account_id:
            return (_record(LegacyLedgerEvidence("unchanged", f"已迁移到账户 {account_id}")),)
        return (_record(_inspect_source(source)),)

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
            return (_record(evidence),)
        existing = _existing_account(root)
        if existing:
            return (_record(LegacyLedgerEvidence("unchanged", f"已迁移到账户 {existing}")),)

        from quantmaster.backtest.paper_accounts import PaperStore

        store = PaperStore(root / PAPER_DATABASE, root / ACCOUNT_ROOT)
        account_id = uuid.uuid4().hex
        return (_copy_and_insert(root, source, store, account_id),)

    def rollback(self, root: Path, backup_root: Path) -> None:
        paper_backup = backup_root / PAPER_DATABASE
        if not paper_backup.is_file():
            raise ValueError("paper.sqlite 备份不存在")
        account_id = _existing_account(root)
        if account_id:
            ledger_dir = root / ACCOUNT_ROOT / account_id
            ledger = ledger_dir / "ledger.sqlite"
            if ledger.is_file():
                ledger.unlink()
            if ledger_dir.is_dir() and not any(ledger_dir.iterdir()):
                ledger_dir.rmdir()
        destination = root / PAPER_DATABASE
        staging = destination.with_name(f".{destination.name}.paper-rollback")
        if staging.exists():
            staging.unlink()
        _copy_ledger(paper_backup, staging)
        staging.replace(destination)


def register_paper_legacy_migrator() -> None:
    from quantmaster.data.legacy_migration import register_migrator

    register_migrator(PaperLegacyMigrator())


__all__ = ["PaperLegacyMigrator", "register_paper_legacy_migrator"]
