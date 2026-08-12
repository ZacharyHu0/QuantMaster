import sqlite3
import time
from pathlib import Path

import pytest

from scripts.paper_recovery import (
    apply_recovery,
    inspect_database,
    maintenance_lease,
    recovery_plan,
    rollback,
)


def make_v4(path, *, classified=False, ledger=False):
    now = "2026-08-13T00:00:00+00:00"
    with sqlite3.connect(path) as conn:
        conn.executescript("""
        CREATE TABLE paper_accounts(id TEXT PRIMARY KEY,name TEXT,status TEXT,mode TEXT,
          initial_capital REAL,strategy_json TEXT,strategy_hash TEXT,universe TEXT,
          universe_json TEXT,created_at TEXT,updated_at TEXT);
        CREATE TABLE paper_cycles(id TEXT PRIMARY KEY,account_id TEXT,signal_date TEXT,
          status TEXT,strategy_hash TEXT,target_json TEXT,reference_json TEXT,created_at TEXT);
        CREATE TABLE paper_orders(id TEXT PRIMARY KEY,cycle_id TEXT,account_id TEXT,symbol TEXT,
          target_weight REAL,side TEXT,shares REAL DEFAULT 0,price REAL DEFAULT 0,
          fee REAL DEFAULT 0,status TEXT,reason TEXT DEFAULT '',idempotency_key TEXT UNIQUE,
          created_at TEXT,updated_at TEXT);
        CREATE TABLE paper_auto_runs(run_date TEXT,account_id TEXT,status TEXT,attempts INTEGER,
          next_retry_at REAL,lease_owner TEXT,lease_expires REAL,lease_token TEXT DEFAULT '',
          heartbeat_at REAL,result_json TEXT DEFAULT '{}',last_error TEXT,failure_code TEXT,
          updated_at TEXT,PRIMARY KEY(run_date,account_id));
        PRAGMA user_version=4;
        """)
        conn.execute(
            "INSERT INTO paper_accounts VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            ("account", "fixture", "paused", "auto", 100000, "{}", "s", "demo", "{}", now, now),
        )
        conn.execute(
            "INSERT INTO paper_cycles VALUES (?,?,?,?,?,?,?,?)",
            ("cycle", "account", "2026-08-07", "confirmed", "s", "{}", "{}", now),
        )
        if classified:
            for index in range(18):
                statuses = ("skipped", "superseded", "filled", "cancelled", "expired", "rejected")
                status = statuses[index % len(statuses)]
                conn.execute(
                    "INSERT INTO paper_orders(id,cycle_id,account_id,symbol,target_weight,side,"
                    "status,idempotency_key,created_at,updated_at) VALUES (?,?,?,?,0,'hold',?,?,?,?)",
                    (f"terminal-{index}", "cycle", "account", f"T{index}.US", status,
                     f"terminal-key-{index}", now, now),
                )
            for index in range(11):
                conn.execute(
                    "INSERT INTO paper_orders(id,cycle_id,account_id,symbol,target_weight,side,"
                    "status,idempotency_key,created_at,updated_at) VALUES (?,?,?,?,0,'rebalance',"
                    "'queued',?,?,?)",
                    (f"waiting-{index}", "cycle", "account", f"W{index}.US",
                     f"waiting-key-{index}", now, now),
                )
            for index in range(13):
                status = "running" if index == 0 else "manual_recovery" if index < 9 else "failed"
                conn.execute(
                    "INSERT INTO paper_auto_runs(run_date,account_id,status,attempts,next_retry_at,"
                    "lease_owner,lease_expires,heartbeat_at,last_error,failure_code,updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (f"2026-07-{index + 1:02d}", "account", status, 6, 1,
                     "dead-owner" if index == 0 else "", 1, 1,
                     "market data unavailable", "market_data_unavailable", now),
                )
    if ledger:
        ledger_path = path.parent / "paper_accounts" / "account" / "ledger.sqlite"
        ledger_path.parent.mkdir(parents=True)
        with sqlite3.connect(ledger_path) as conn:
            conn.execute("CREATE TABLE trades(id INTEGER PRIMARY KEY,value TEXT)")
            conn.execute("CREATE TABLE cashflows(id INTEGER PRIMARY KEY,value TEXT)")
            conn.execute("INSERT INTO trades(value) VALUES ('before')")
    return path


def test_true_v4_42_plan_and_apply_is_noop_on_second_run(tmp_path):
    database = make_v4(tmp_path / "paper.sqlite", classified=True, ledger=True)
    plan = recovery_plan(database, now=time.time())
    orders = [row for row in plan["rows"] if row["kind"] == "order"]
    runs = [row for row in plan["rows"] if row["kind"] == "run"]
    assert len(plan["rows"]) == 42
    assert sum(row["reason"] == "terminal_preserved_no_fill_invented" for row in orders) == 18
    assert sum(row["to"] == "waiting_market_data" for row in orders) == 11
    assert sum(row["task_status"] == "stalled" for row in runs) == 1
    assert sum(row["task_status"] == "manual_attention" for row in runs) == 8
    assert sum(row["task_status"] == "retry_wait" for row in runs) == 4

    first = apply_recovery(
        database, tmp_path / "backups", confirmed_path=str(database), test_db=True,
    )
    second = apply_recovery(
        database, tmp_path / "backups", confirmed_path=str(database), test_db=True,
    )
    assert first["before"]["user_version"] == 4
    assert first["after"]["user_version"] == 5
    assert first["affected_rows"] == 24
    assert second["noop"] is True and second["affected_rows"] == 0
    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT COUNT(*) FROM paper_recovery_audit").fetchone()[0] == 1
        before_json, after_json = conn.execute(
            "SELECT before_json,after_json FROM paper_recovery_audit"
        ).fetchone()
    before_audit, after_audit = __import__("json").loads(before_json), __import__("json").loads(after_json)
    assert len(before_audit["rows"]) == 42
    assert len(after_audit["rows"]) == 42
    assert after_audit["counts"]["paper_orders"] == 29
    assert after_audit["ledger_counts"]["account"]["trades"] == 1
    operations = list((tmp_path / "backups").glob("paper-recovery-*"))
    assert operations == [Path(first["operation"])]


def test_failure_injection_restores_v4_and_all_counts(tmp_path):
    database = make_v4(tmp_path / "paper.sqlite", classified=True, ledger=True)
    before = inspect_database(database)
    with pytest.raises(RuntimeError, match="故障注入"):
        apply_recovery(
            database, tmp_path / "backups", confirmed_path=str(database), test_db=True,
            fail_after=2,
        )
    restored = inspect_database(database)
    assert restored["user_version"] == 4
    assert restored["counts"] == before["counts"]


def test_manifest_rollback_restores_paper_and_ledger(tmp_path):
    database = make_v4(tmp_path / "paper.sqlite", classified=True, ledger=True)
    applied = apply_recovery(
        database, tmp_path / "backups", confirmed_path=str(database), test_db=True,
    )
    ledger = tmp_path / "paper_accounts" / "account" / "ledger.sqlite"
    with sqlite3.connect(ledger) as conn:
        conn.execute("UPDATE trades SET value='after'")
    rollback(
        database, Path(applied["operation"]),
        confirmed_path=str(database), test_db=True,
    )
    assert inspect_database(database)["user_version"] == 4
    with sqlite3.connect(ledger) as conn:
        assert conn.execute("SELECT value FROM trades").fetchone()[0] == "before"


def test_rollback_preflight_rejects_incomplete_backup_before_replacing_any_file(tmp_path):
    database = make_v4(tmp_path / "paper.sqlite", classified=True, ledger=True)
    applied = apply_recovery(
        database, tmp_path / "backups", confirmed_path=str(database), test_db=True,
    )
    operation = Path(applied["operation"])
    ledger = tmp_path / "paper_accounts" / "account" / "ledger.sqlite"
    with sqlite3.connect(database) as conn:
        conn.execute("UPDATE paper_orders SET status='rejected' WHERE id='waiting-0'")
    with sqlite3.connect(ledger) as conn:
        conn.execute("UPDATE trades SET value='after'")
    next(operation.glob("ledgers/*/ledger.sqlite")).unlink()

    with pytest.raises((FileNotFoundError, sqlite3.OperationalError)):
        rollback(database, operation, confirmed_path=str(database), test_db=True)

    with sqlite3.connect(database) as conn:
        assert conn.execute(
            "SELECT status FROM paper_orders WHERE id='waiting-0'"
        ).fetchone()[0] == "rejected"
    with sqlite3.connect(ledger) as conn:
        assert conn.execute("SELECT value FROM trades").fetchone()[0] == "after"


def test_production_path_and_test_path_are_explicit(tmp_path, monkeypatch):
    database = make_v4(tmp_path / "paper.sqlite")
    with pytest.raises(ValueError, match="configured"):
        apply_recovery(database, tmp_path / "backups", confirmed_path=str(database))
    monkeypatch.setattr("scripts.paper_recovery._assert_test_path", lambda _path: None)
    with pytest.raises(ValueError, match="精确一致"):
        apply_recovery(
            database, tmp_path / "backups", confirmed_path=str(tmp_path / "wrong"), test_db=True,
        )


def test_production_maintenance_lease_releases_in_finally(tmp_path, monkeypatch):
    database = tmp_path / "paper.sqlite"
    calls = []

    class Config:
        data_root = tmp_path

    def call(operation, payload, **_kwargs):
        calls.append((operation, payload))
        if operation == "maintenance.enter":
            return {"token": "lease-token", "pid": 123}
        return {"released": True}

    monkeypatch.setattr("scripts.paper_recovery.get_config", lambda: Config())
    monkeypatch.setattr("scripts.paper_recovery.call_worker_command", call)
    with pytest.raises(RuntimeError, match="boom"):
        with maintenance_lease(database, test_db=False):
            raise RuntimeError("boom")
    assert [operation for operation, _payload in calls] == [
        "maintenance.enter", "maintenance.exit",
    ]
    assert calls[-1][1] == {"token": "lease-token"}
