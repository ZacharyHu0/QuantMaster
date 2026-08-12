from __future__ import annotations

import sqlite3

import pytest

from quantmaster.backtest.paper_accounts import PaperService, PaperStore
from quantmaster.backtest.paper_legacy_migration import (
    DIAGNOSTIC_CODE,
    SOURCE_NAME,
    UNKNOWN_FIELDS,
    PaperLegacyMigrator,
)


def _legacy_ledger(path) -> None:
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE trades (
                id INTEGER PRIMARY KEY, date TEXT, symbol TEXT, side TEXT,
                price REAL, shares REAL, fee REAL, note TEXT);
            CREATE TABLE cashflows (
                id INTEGER PRIMARY KEY, date TEXT, amount REAL, kind TEXT, note TEXT);
            INSERT INTO trades VALUES
                (1,'2024-01-03','600000.SH','buy',10.5,100,5,'confirmed fill');
            INSERT INTO cashflows VALUES
                (1,'2024-01-02',100000,'deposit','confirmed cash');
            """
        )


def test_paper_service_constructor_never_imports_old_ledger(tmp_path):
    _legacy_ledger(tmp_path / SOURCE_NAME)
    store = PaperStore(tmp_path / "paper.sqlite", tmp_path / "paper_accounts")

    PaperService(store)

    assert store.accounts(include_archived=True) == []
    assert not list((tmp_path / "paper_accounts").glob("*/ledger.sqlite"))


def test_paper_migrator_dry_run_reports_exact_blank_fields_without_writes(tmp_path):
    _legacy_ledger(tmp_path / SOURCE_NAME)
    migrator = PaperLegacyMigrator()

    records = list(migrator.inspect(tmp_path))

    assert len(records) == 1
    assert records[0].record_key == SOURCE_NAME
    assert records[0].outcome == "blank"
    assert records[0].diagnostic_code == DIAGNOSTIC_CODE
    assert records[0].unknown_fields == UNKNOWN_FIELDS
    assert "成交 1 条、现金流 1 条" in records[0].detail
    assert not (tmp_path / "paper.sqlite").exists()
    assert not (tmp_path / "accounts").exists()


def test_paper_migrator_preserves_ledger_facts_and_is_idempotent(tmp_path):
    _legacy_ledger(tmp_path / SOURCE_NAME)
    migrator = PaperLegacyMigrator()

    first = list(migrator.migrate_batch(tmp_path, after_key="", limit=1))
    second = list(migrator.migrate_batch(tmp_path, after_key="", limit=1))
    resumed = list(migrator.migrate_batch(tmp_path, after_key=SOURCE_NAME, limit=1))

    assert first[0].outcome == "blank"
    assert second[0].outcome == "unchanged"
    assert resumed == []
    store = PaperStore(tmp_path / "paper.sqlite", tmp_path / "paper_accounts")
    accounts = store.accounts(include_archived=True)
    assert len(accounts) == 1
    account = accounts[0]
    assert account["status"] == "paused"
    assert account["mode"] == "manual"
    assert account["strategy"] is None
    assert account["universe_snapshot"] is None
    assert account["initial_capital"] is None
    assert account["universe"] is None
    assert account["source_backtest_id"] is None
    assert account["created_at"] is None
    with store.ledger(account["id"])._conn() as connection:
        trade = connection.execute(
            "SELECT date,symbol,side,price,shares,fee,note FROM trades"
        ).fetchone()
        cash = connection.execute(
            "SELECT date,amount,kind,note FROM cashflows"
        ).fetchone()
    assert trade == ("2024-01-03", "600000.SH", "buy", 10.5, 100.0, 5.0, "confirmed fill")
    assert cash == ("2024-01-02", 100000.0, "deposit", "confirmed cash")
    with pytest.raises(ValueError, match="暂停"):
        PaperService(store).propose(account["id"], panel={})


def test_paper_migrator_conflict_does_not_create_current_data(tmp_path):
    with sqlite3.connect(tmp_path / SOURCE_NAME) as connection:
        connection.execute("CREATE TABLE trades(id INTEGER PRIMARY KEY)")
    migrator = PaperLegacyMigrator()

    inspected = list(migrator.inspect(tmp_path))
    applied = list(migrator.migrate_batch(tmp_path, after_key="", limit=1))

    assert inspected[0].outcome == "conflict"
    assert applied[0].outcome == "conflict"
    assert not (tmp_path / "paper.sqlite").exists()
    assert not (tmp_path / "accounts").exists()
