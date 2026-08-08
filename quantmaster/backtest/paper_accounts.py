"""多账户模拟盘：不可变策略快照、提案确认与 T+1 开盘撮合。"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import sqlite3
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd

from quantmaster.backtest.execution import (
    buy_cost,
    executable_buy_shares,
    quote_open,
    sell_cost,
)
from quantmaster.backtest.spec import (
    FactorStrategySpec,
    PaperAccountSpec,
    StrategySpec,
    build_strategy,
    canonical_json,
    content_hash,
    signal_is_due,
)
from quantmaster.config import get_config
from quantmaster.portfolio.ledger import Ledger, TradeRecord
from quantmaster.portfolio.performance import ledger_report
from quantmaster.runtime.sqlite import connect_sqlite, execute_sql_script, migrate_schema
from quantmaster.trading_sessions import SHANGHAI

logger = logging.getLogger(__name__)
PAPER_SCHEMA_VERSION = 2


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class PaperStore:
    def __init__(self, path: str | Path | None = None, account_root: str | Path | None = None):
        self.path = Path(path) if path else get_config().data_root / "paper.sqlite"
        self.account_root = Path(account_root) if account_root else get_config().data_root / "paper_accounts"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.account_root.mkdir(parents=True, exist_ok=True)
        self._migrate()

    def _conn(self) -> sqlite3.Connection:
        return connect_sqlite(self.path, row_factory=True)

    def _migrate(self) -> None:
        def schema_v1(conn: sqlite3.Connection) -> None:
            execute_sql_script(
                conn,
                """
                CREATE TABLE IF NOT EXISTS paper_accounts (
                    id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, status TEXT NOT NULL,
                    mode TEXT NOT NULL, initial_capital REAL NOT NULL,
                    strategy_json TEXT NOT NULL, strategy_hash TEXT NOT NULL,
                    universe TEXT NOT NULL, universe_json TEXT NOT NULL,
                    source_backtest_id TEXT NOT NULL DEFAULT '', warning TEXT NOT NULL DEFAULT '',
                    strategy_warning TEXT NOT NULL DEFAULT '',
                    runtime_warning TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS paper_cycles (
                    id TEXT PRIMARY KEY, account_id TEXT NOT NULL, signal_date TEXT NOT NULL,
                    execution_date TEXT NOT NULL DEFAULT '', status TEXT NOT NULL,
                    strategy_hash TEXT NOT NULL, target_json TEXT NOT NULL,
                    reference_json TEXT NOT NULL, warning_json TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL, confirmed_at TEXT NOT NULL DEFAULT '',
                    finished_at TEXT NOT NULL DEFAULT '',
                    UNIQUE(account_id,signal_date,strategy_hash),
                    FOREIGN KEY(account_id) REFERENCES paper_accounts(id));
                CREATE TABLE IF NOT EXISTS paper_orders (
                    id TEXT PRIMARY KEY, cycle_id TEXT NOT NULL, account_id TEXT NOT NULL,
                    symbol TEXT NOT NULL, target_weight REAL NOT NULL, side TEXT NOT NULL,
                    shares REAL NOT NULL DEFAULT 0, price REAL NOT NULL DEFAULT 0,
                    fee REAL NOT NULL DEFAULT 0, status TEXT NOT NULL,
                    reason TEXT NOT NULL DEFAULT '', idempotency_key TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    FOREIGN KEY(cycle_id) REFERENCES paper_cycles(id),
                    FOREIGN KEY(account_id) REFERENCES paper_accounts(id));
                CREATE TABLE IF NOT EXISTS paper_migrations (
                    source_path TEXT PRIMARY KEY, source_hash TEXT NOT NULL,
                    account_id TEXT NOT NULL, migrated_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS paper_auto_runs (
                    run_date TEXT NOT NULL, account_id TEXT NOT NULL,
                    status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                    next_retry_at REAL NOT NULL DEFAULT 0,
                    lease_owner TEXT NOT NULL DEFAULT '', lease_expires REAL NOT NULL DEFAULT 0,
                    lease_token TEXT NOT NULL DEFAULT '', heartbeat_at REAL NOT NULL DEFAULT 0,
                    result_json TEXT NOT NULL DEFAULT '{}', last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(run_date,account_id),
                    FOREIGN KEY(account_id) REFERENCES paper_accounts(id));
                CREATE INDEX IF NOT EXISTS idx_paper_cycles
                    ON paper_cycles(account_id,created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_paper_orders
                    ON paper_orders(account_id,status,created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_paper_auto_runs
                    ON paper_auto_runs(status,next_retry_at,lease_expires);
            """,
            )
            account_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(paper_accounts)")}
            for name in ("strategy_warning", "runtime_warning"):
                if name not in account_columns:
                    conn.execute(f"ALTER TABLE paper_accounts ADD COLUMN {name} TEXT NOT NULL DEFAULT ''")
            run_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(paper_auto_runs)")}
            if "lease_token" not in run_columns:
                conn.execute("ALTER TABLE paper_auto_runs ADD COLUMN lease_token TEXT NOT NULL DEFAULT ''")
            if "heartbeat_at" not in run_columns:
                conn.execute("ALTER TABLE paper_auto_runs ADD COLUMN heartbeat_at REAL NOT NULL DEFAULT 0")
            conn.execute(
                "UPDATE paper_accounts SET strategy_warning=warning "
                "WHERE strategy_warning='' AND runtime_warning='' AND warning<>''"
            )

        def schema_v2(conn: sqlite3.Connection) -> None:
            columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(paper_accounts)")}
            if "strategy_effective_after" not in columns:
                conn.execute(
                    "ALTER TABLE paper_accounts ADD COLUMN strategy_effective_after TEXT NOT NULL DEFAULT ''"
                )

        with self._conn() as conn:
            migrate_schema(conn, ((1, schema_v1), (PAPER_SCHEMA_VERSION, schema_v2)))

    @staticmethod
    def _account_value(row: sqlite3.Row | None) -> dict | None:
        if row is None:
            return None
        value = dict(row)
        value["strategy"] = json.loads(value.pop("strategy_json"))
        value["universe_snapshot"] = json.loads(value.pop("universe_json"))
        value["strategy_warning"] = str(value.get("strategy_warning") or "")
        value["runtime_warning"] = str(value.get("runtime_warning") or "")
        value["strategy_effective_after"] = str(value.get("strategy_effective_after") or "")
        value["warning"] = value["runtime_warning"] or value["strategy_warning"]
        return value

    @staticmethod
    def _cycle_value(row: sqlite3.Row | None) -> dict | None:
        if row is None:
            return None
        value = dict(row)
        value["target_weights"] = json.loads(value.pop("target_json"))
        value["reference_prices"] = json.loads(value.pop("reference_json"))
        value["warnings"] = json.loads(value.pop("warning_json") or "[]")
        return value

    def ledger_path(self, account_id: str) -> Path:
        try:
            safe_id = os.path.basename(uuid.UUID(account_id).hex)
        except (AttributeError, TypeError, ValueError):
            raise ValueError("模拟账户 ID 非法") from None
        if safe_id != account_id:
            raise ValueError("模拟账户 ID 非法")
        directory = self.account_root / safe_id
        directory.mkdir(parents=True, exist_ok=True)
        return directory / "ledger.sqlite"

    def ledger(self, account_id: str) -> Ledger:
        if self.account(account_id) is None:
            raise KeyError("模拟账户不存在")
        return Ledger(path=self.ledger_path(account_id))

    def create_account(
        self,
        spec: PaperAccountSpec,
        *,
        symbols: list[str],
        universe_meta: dict | None = None,
        warning: str = "",
    ) -> dict:
        account_id, now = uuid.uuid4().hex, utc_now()
        strategy = spec.strategy.model_dump(mode="json")
        universe_snapshot = {
            "name": spec.universe,
            "symbols": sorted(set(symbols)),
            **(universe_meta or {}),
        }
        strategy_hash = content_hash({"strategy": strategy, "universe": universe_snapshot})
        with self._conn() as conn:
            try:
                conn.execute(
                    "INSERT INTO paper_accounts "
                    "(id,name,status,mode,initial_capital,strategy_json,strategy_hash,universe,"
                    "universe_json,source_backtest_id,warning,strategy_warning,runtime_warning,"
                    "created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        account_id,
                        spec.name.strip(),
                        "active",
                        spec.mode,
                        spec.initial_capital,
                        canonical_json(strategy),
                        strategy_hash,
                        spec.universe,
                        canonical_json(universe_snapshot),
                        spec.source_backtest_id,
                        warning[:500],
                        warning[:500],
                        "",
                        now,
                        now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError(f"模拟账户名称已存在：{spec.name.strip()}") from exc
        Ledger(path=self.ledger_path(account_id)).add_cashflow(
            str(pd.Timestamp.now().date()),
            spec.initial_capital,
            "deposit",
            "模拟盘初始资金",
            idempotency_key=f"account:{account_id}:initial",
        )
        return self.account(account_id) or {}

    def account(self, account_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM paper_accounts WHERE id=?", (account_id,)).fetchone()
        return self._account_value(row)

    def accounts(self, include_archived: bool = False) -> list[dict]:
        where = "" if include_archived else "WHERE status<>'archived'"
        with self._conn() as conn:
            rows = conn.execute(f"SELECT * FROM paper_accounts {where} ORDER BY created_at DESC").fetchall()
        return [self._account_value(row) or {} for row in rows]

    @staticmethod
    def _account_name(value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("模拟账户名称不能为空")
        if len(normalized) > 40:
            raise ValueError("模拟账户名称不能超过 40 个字符")
        return normalized

    def update_account(
        self,
        account_id: str,
        *,
        name: str | None = None,
        status: str | None = None,
        mode: str | None = None,
    ) -> dict:
        if status is not None and status not in {"active", "paused", "archived"}:
            raise ValueError("账户状态只允许 active/paused/archived")
        if mode is not None and mode not in {"manual", "auto"}:
            raise ValueError("执行模式只允许 manual/auto")
        fields, params = [], []
        normalized_name = ""
        if name is not None:
            normalized_name = self._account_name(name)
            fields.append("name=?")
            params.append(normalized_name)
        if status is not None:
            fields.append("status=?")
            params.append(status)
        if mode is not None:
            fields.append("mode=?")
            params.append(mode)
        if not fields:
            return self.account(account_id) or {}
        fields.append("updated_at=?")
        params.extend([utc_now(), account_id])
        with self._conn() as conn:
            try:
                changed = conn.execute(
                    f"UPDATE paper_accounts SET {','.join(fields)} WHERE id=?",
                    params,
                ).rowcount
            except sqlite3.IntegrityError as exc:
                raise ValueError(f"模拟账户名称已存在：{normalized_name}") from exc
        if not changed:
            raise KeyError("模拟账户不存在")
        return self.account(account_id) or {}

    def account_activity(self, account_id: str) -> dict[str, int | bool]:
        account = self.account(account_id)
        if account is None:
            raise KeyError("模拟账户不存在")
        with self._conn() as conn:
            row = conn.execute(
                "SELECT (SELECT COUNT(*) FROM paper_cycles WHERE account_id=?),"
                "(SELECT COUNT(*) FROM paper_orders WHERE account_id=?)",
                (account_id, account_id),
            ).fetchone()
        trades = self.ledger(account_id).trades()
        cycles = int(row[0] or 0)
        orders = int(row[1] or 0)
        trade_count = len(trades)
        return {
            "cycles": cycles,
            "orders": orders,
            "trades": trade_count,
            "strategy_editable": cycles == 0 and trade_count == 0,
        }

    def replace_strategy(
        self,
        account_id: str,
        spec: PaperAccountSpec,
        *,
        symbols: list[str],
        universe_meta: dict | None = None,
        warning: str = "",
        effective_after: str = "",
    ) -> dict:
        """Start a new strategy segment while preserving every historical cycle and trade."""
        strategy = spec.strategy.model_dump(mode="json")
        universe_snapshot = {
            "name": spec.universe,
            "symbols": sorted(set(symbols)),
            **(universe_meta or {}),
        }
        strategy_hash = content_hash({"strategy": strategy, "universe": universe_snapshot})
        now = utc_now()
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT id FROM paper_accounts WHERE id=?",
                (account_id,),
            ).fetchone()
            if row is None:
                raise KeyError("模拟账户不存在")
            conn.execute(
                "UPDATE paper_orders SET status='superseded',reason='strategy_changed',"
                "updated_at=? WHERE account_id=? AND status IN ('proposed','queued','blocked')",
                (now, account_id),
            )
            conn.execute(
                "UPDATE paper_cycles SET status='superseded',finished_at=? WHERE account_id=? "
                "AND status IN ('proposed','confirmed','blocked')",
                (now, account_id),
            )
            try:
                conn.execute(
                    "UPDATE paper_accounts SET name=?,mode=?,strategy_json=?,strategy_hash=?,"
                    "universe=?,universe_json=?,source_backtest_id=?,warning=?,strategy_warning=?,"
                    "runtime_warning='',strategy_effective_after=?,updated_at=? WHERE id=?",
                    (
                        spec.name.strip(),
                        spec.mode,
                        canonical_json(strategy),
                        strategy_hash,
                        spec.universe,
                        canonical_json(universe_snapshot),
                        spec.source_backtest_id,
                        warning[:500],
                        warning[:500],
                        effective_after,
                        now,
                        account_id,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError(f"模拟账户名称已存在：{spec.name.strip()}") from exc
        return self.account(account_id) or {}

    def clear_strategy_transition(self, account_id: str, strategy_hash: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE paper_accounts SET strategy_effective_after='',updated_at=? "
                "WHERE id=? AND strategy_hash=?",
                (utc_now(), account_id, strategy_hash),
            )

    def archive_account(self, account_id: str) -> dict:
        """Soft-delete an account and fence every unfinished automation path."""
        now = utc_now()
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT id FROM paper_accounts WHERE id=?",
                (account_id,),
            ).fetchone()
            if row is None:
                raise KeyError("模拟账户不存在")
            conn.execute(
                "UPDATE paper_orders SET status='superseded',reason='account_archived',updated_at=? "
                "WHERE account_id=? AND status IN ('proposed','queued','blocked')",
                (now, account_id),
            )
            conn.execute(
                "UPDATE paper_cycles SET status='superseded',finished_at=? "
                "WHERE account_id=? AND status IN ('proposed','confirmed','blocked')",
                (now, account_id),
            )
            conn.execute(
                "UPDATE paper_auto_runs SET status='cancelled',next_retry_at=0,lease_owner='',"
                "lease_expires=0,lease_token='',heartbeat_at=0,updated_at=? "
                "WHERE account_id=? AND status<>'completed'",
                (now, account_id),
            )
            conn.execute(
                "UPDATE paper_accounts SET status='archived',updated_at=? WHERE id=?",
                (now, account_id),
            )
        return self.account(account_id) or {}

    def set_warning(self, account_id: str, warning: str, *, pause: bool = False) -> None:
        """Compatibility wrapper: operational failures are runtime warnings."""
        self.set_runtime_warning(account_id, warning, pause=pause)

    def set_runtime_warning(
        self,
        account_id: str,
        warning: str,
        *,
        pause: bool = False,
    ) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE paper_accounts SET runtime_warning=?,warning=?,"
                "status=CASE WHEN ? THEN 'paused' ELSE status END,updated_at=? WHERE id=?",
                (warning[:500], warning[:500], int(pause), utc_now(), account_id),
            )

    def clear_runtime_warning(self, account_id: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE paper_accounts SET runtime_warning='',warning=strategy_warning,"
                "updated_at=? WHERE id=?",
                (utc_now(), account_id),
            )

    def claim_auto_run(
        self,
        run_date: str,
        account_id: str,
        owner: str,
        *,
        now: float | None = None,
        lease_seconds: float = 90,
    ) -> str | None:
        current = time.time() if now is None else float(now)
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT status,attempts,next_retry_at,lease_expires FROM paper_auto_runs "
                "WHERE run_date=? AND account_id=?",
                (run_date, account_id),
            ).fetchone()
            if row is not None:
                if str(row["status"]) == "completed":
                    return None
                if str(row["status"]) == "manual_recovery":
                    return None
                if str(row["status"]) == "running" and float(row["lease_expires"] or 0) > current:
                    return None
                if str(row["status"]) == "failed" and float(row["next_retry_at"] or 0) > current:
                    return None
            attempts = int(row["attempts"] or 0) + 1 if row is not None else 1
            if attempts > 6:
                return None
            token = uuid.uuid4().hex
            conn.execute(
                "INSERT INTO paper_auto_runs "
                "(run_date,account_id,status,attempts,next_retry_at,lease_owner,lease_expires,"
                "lease_token,heartbeat_at,result_json,last_error,updated_at) "
                "VALUES (?,?, 'running', ?,0,?,?,?,?, '{}','',?) "
                "ON CONFLICT(run_date,account_id) DO UPDATE SET status='running',"
                "attempts=excluded.attempts,next_retry_at=0,lease_owner=excluded.lease_owner,"
                "lease_expires=excluded.lease_expires,lease_token=excluded.lease_token,"
                "heartbeat_at=excluded.heartbeat_at,last_error='',updated_at=excluded.updated_at",
                (
                    run_date,
                    account_id,
                    attempts,
                    owner,
                    current + max(15.0, float(lease_seconds)),
                    token,
                    current,
                    utc_now(),
                ),
            )
        return token

    def heartbeat_auto_run(
        self,
        run_date: str,
        account_id: str,
        owner: str,
        token: str,
        *,
        now: float | None = None,
        lease_seconds: float = 90,
    ) -> bool:
        current = time.time() if now is None else float(now)
        with self._conn() as conn:
            changed = conn.execute(
                "UPDATE paper_auto_runs SET heartbeat_at=?,lease_expires=?,updated_at=? "
                "WHERE run_date=? AND account_id=? AND status='running' AND lease_owner=? "
                "AND lease_token=? AND lease_expires>?",
                (
                    current,
                    current + max(15.0, float(lease_seconds)),
                    utc_now(),
                    run_date,
                    account_id,
                    owner,
                    token,
                    current,
                ),
            ).rowcount
        return bool(changed)

    def complete_auto_run(
        self,
        run_date: str,
        account_id: str,
        owner: str,
        token: str,
        result: dict,
        *,
        now: float | None = None,
    ) -> bool:
        current = time.time() if now is None else float(now)
        with self._conn() as conn:
            changed = conn.execute(
                "UPDATE paper_auto_runs SET status='completed',next_retry_at=0,lease_owner='',"
                "lease_expires=0,lease_token='',result_json=?,last_error='',updated_at=? "
                "WHERE run_date=? AND account_id=? AND status='running' AND lease_owner=? "
                "AND lease_token=? AND lease_expires>?",
                (canonical_json(result), utc_now(), run_date, account_id, owner, token, current),
            ).rowcount
        return bool(changed)

    def fail_auto_run(
        self,
        run_date: str,
        account_id: str,
        owner: str,
        token: str,
        error: str,
        *,
        now: float | None = None,
    ) -> bool:
        current = time.time() if now is None else float(now)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT attempts FROM paper_auto_runs WHERE run_date=? AND account_id=? "
                "AND status='running' AND lease_owner=? AND lease_token=? AND lease_expires>?",
                (run_date, account_id, owner, token, current),
            ).fetchone()
            if row is None:
                return False
            attempts = max(1, int(row["attempts"] or 1))
            delays = (5 * 60, 15 * 60, 30 * 60, 60 * 60, 2 * 60 * 60)
            exhausted = attempts >= 6
            next_retry = 0 if exhausted else current + delays[min(attempts - 1, len(delays) - 1)]
            changed = conn.execute(
                "UPDATE paper_auto_runs SET status=?,next_retry_at=?,lease_owner='',"
                "lease_expires=0,lease_token='',last_error=?,updated_at=? "
                "WHERE run_date=? AND account_id=? AND status='running' AND lease_owner=? "
                "AND lease_token=?",
                (
                    "manual_recovery" if exhausted else "failed",
                    next_retry,
                    error[:500],
                    utc_now(),
                    run_date,
                    account_id,
                    owner,
                    token,
                ),
            ).rowcount
        return bool(changed)

    def recover_auto_run(self, run_date: str, account_id: str) -> bool:
        with self._conn() as conn:
            changed = conn.execute(
                "UPDATE paper_auto_runs SET status='failed',attempts=0,next_retry_at=0,"
                "last_error='',updated_at=? WHERE run_date=? AND account_id=? "
                "AND status='manual_recovery'",
                (utc_now(), run_date, account_id),
            ).rowcount
        return bool(changed)

    def latest_auto_run(self, account_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM paper_auto_runs WHERE account_id=? ORDER BY run_date DESC LIMIT 1",
                (account_id,),
            ).fetchone()
        if row is None:
            return None
        value = dict(row)
        value["result"] = json.loads(value.pop("result_json") or "{}")
        return value

    def create_cycle(
        self,
        account: dict,
        signal_date: str,
        target_weights: dict[str, float],
        reference_prices: dict[str, float],
        warnings: list[dict[str, str]],
    ) -> tuple[dict, bool]:
        cycle_id, now = uuid.uuid4().hex, utc_now()
        with self._conn() as conn:
            try:
                conn.execute(
                    "INSERT INTO paper_cycles "
                    "(id,account_id,signal_date,status,strategy_hash,target_json,reference_json,"
                    "warning_json,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        cycle_id,
                        account["id"],
                        signal_date,
                        "proposed",
                        account["strategy_hash"],
                        canonical_json(target_weights),
                        canonical_json(reference_prices),
                        canonical_json(warnings),
                        now,
                    ),
                )
            except sqlite3.IntegrityError:
                row = conn.execute(
                    "SELECT * FROM paper_cycles WHERE account_id=? AND signal_date=? AND strategy_hash=?",
                    (account["id"], signal_date, account["strategy_hash"]),
                ).fetchone()
                return self._cycle_value(row) or {}, False
            for symbol in sorted(target_weights):
                conn.execute(
                    "INSERT INTO paper_orders "
                    "(id,cycle_id,account_id,symbol,target_weight,side,status,idempotency_key,"
                    "created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        uuid.uuid4().hex,
                        cycle_id,
                        account["id"],
                        symbol,
                        float(target_weights[symbol]),
                        "rebalance",
                        "proposed",
                        f"paper:{cycle_id}:{symbol}",
                        now,
                        now,
                    ),
                )
        return self.cycle(cycle_id) or {}, True

    def cycle(self, cycle_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM paper_cycles WHERE id=?", (cycle_id,)).fetchone()
        value = self._cycle_value(row)
        if value is not None:
            value["orders"] = self.orders(cycle_id=cycle_id)
        return value

    def cycles(self, account_id: str, limit: int = 30) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM paper_cycles WHERE account_id=? ORDER BY created_at DESC LIMIT ?",
                (account_id, max(1, min(limit, 200))),
            ).fetchall()
        result = []
        for row in rows:
            value = self._cycle_value(row) or {}
            value["orders"] = self.orders(cycle_id=value["id"])
            result.append(value)
        return result

    def orders(self, *, cycle_id: str = "", account_id: str = "", limit: int = 500) -> list[dict]:
        if cycle_id:
            where, param = "cycle_id=?", cycle_id
        elif account_id:
            where, param = "account_id=?", account_id
        else:
            raise ValueError("需要 cycle_id 或 account_id")
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT * FROM paper_orders WHERE {where} ORDER BY created_at,symbol LIMIT ?",
                (param, max(1, min(limit, 2000))),
            ).fetchall()
        return [dict(row) for row in rows]

    def confirm(self, cycle_id: str) -> dict:
        cycle = self.cycle(cycle_id)
        if cycle is None:
            raise KeyError("调仓提案不存在")
        if cycle["status"] != "proposed":
            return cycle
        now = utc_now()
        with self._conn() as conn:
            conn.execute(
                "UPDATE paper_orders SET status='superseded',reason='newer_cycle',updated_at=? "
                "WHERE account_id=? AND cycle_id<>? AND status IN ('queued','blocked')",
                (now, cycle["account_id"], cycle_id),
            )
            conn.execute(
                "UPDATE paper_cycles SET status='superseded',finished_at=? WHERE account_id=? "
                "AND id<>? AND status IN ('confirmed','blocked')",
                (now, cycle["account_id"], cycle_id),
            )
            conn.execute(
                "UPDATE paper_cycles SET status='confirmed',confirmed_at=? WHERE id=? AND status='proposed'",
                (now, cycle_id),
            )
            conn.execute(
                "UPDATE paper_orders SET status='queued',updated_at=? WHERE cycle_id=? AND status='proposed'",
                (now, cycle_id),
            )
        return self.cycle(cycle_id) or {}

    def update_order(
        self,
        order_id: str,
        *,
        status: str,
        side: str,
        shares: float = 0,
        price: float = 0,
        fee: float = 0,
        reason: str = "",
    ) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE paper_orders SET status=?,side=?,shares=?,price=?,fee=?,reason=?,"
                "updated_at=? WHERE id=?",
                (status, side, shares, price, fee, reason, utc_now(), order_id),
            )

    def update_cycle_status(self, cycle_id: str, status: str, execution_date: str = "") -> dict:
        finished = utc_now() if status in {"completed", "superseded"} else ""
        with self._conn() as conn:
            conn.execute(
                "UPDATE paper_cycles SET status=?,execution_date=CASE WHEN ?='' THEN "
                "execution_date ELSE ? END,finished_at=CASE WHEN ?='' THEN finished_at ELSE ? END "
                "WHERE id=?",
                (status, execution_date, execution_date, finished, finished, cycle_id),
            )
        return self.cycle(cycle_id) or {}

    def migrate_legacy(self) -> dict | None:
        source = get_config().data_root / "ledger_paper.sqlite"
        if not source.is_file():
            return None
        source_key = str(source.resolve())
        with self._conn() as conn:
            row = conn.execute(
                "SELECT account_id FROM paper_migrations WHERE source_path=?",
                (source_key,),
            ).fetchone()
        if row:
            return self.account(row["account_id"])
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        account_id, now = uuid.uuid4().hex, utc_now()
        name = "历史模拟盘"
        with self._conn() as conn:
            if conn.execute("SELECT 1 FROM paper_accounts WHERE name=?", (name,)).fetchone():
                name = f"历史模拟盘-{digest[:6]}"
        strategy = FactorStrategySpec().model_dump(mode="json")
        universe = {"name": "legacy", "symbols": [], "source": source_key}
        destination = self.ledger_path(account_id)
        # A byte copy of the main file loses committed rows that are still in
        # the source WAL. SQLite's online backup API snapshots main + WAL while
        # leaving the legacy database untouched and readable.
        with connect_sqlite(source) as source_conn, connect_sqlite(destination) as destination_conn:
            source_conn.backup(destination_conn)
        Ledger(path=destination)
        warning = "由旧模拟账本导入；策略来源未知，已暂停。"
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO paper_accounts "
                "(id,name,status,mode,initial_capital,strategy_json,strategy_hash,universe,"
                "universe_json,source_backtest_id,warning,strategy_warning,runtime_warning,"
                "created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    account_id,
                    name,
                    "paused",
                    "manual",
                    0.0,
                    canonical_json(strategy),
                    content_hash({"strategy": strategy, "legacy": digest}),
                    "legacy",
                    canonical_json(universe),
                    "",
                    warning,
                    warning,
                    "",
                    now,
                    now,
                ),
            )
            conn.execute(
                "INSERT INTO paper_migrations VALUES (?,?,?,?)",
                (source_key, digest, account_id, now),
            )
        return self.account(account_id)


class PaperService:
    def __init__(self, store: PaperStore | None = None):
        self.store = store or PaperStore()
        self.store.migrate_legacy()

    @staticmethod
    def _resolve_universe(name: str, as_of: str) -> tuple[list[str], dict]:
        if name.lower() == "csi800":
            from quantmaster.lab.dataset import load_csi800_members_as_of

            snapshot = load_csi800_members_as_of(as_of)
            return snapshot["symbols"], {
                "as_of": snapshot["as_of"],
                "snapshot_dates": snapshot["snapshot_dates"],
                "quality": "production",
            }
        from quantmaster.data.universe import load_universe

        return load_universe(name), {"as_of": as_of, "quality": "sandbox"}

    def _materialize_account_spec(
        self,
        spec: PaperAccountSpec,
    ) -> tuple[PaperAccountSpec, list[str], dict]:
        from quantmaster.backtest.spec import LabVersionStrategySpec, pin_decision_strategy

        if isinstance(spec.strategy, LabVersionStrategySpec):
            raise ValueError(
                "Lab 版本历史回测使用滚动 OOF，不能直接提升模拟账户；"
                "请先完成偏差审计、人工批准和 Champion 部署，再使用 Decision 策略。"
            )

        symbols, meta = self._resolve_universe(
            spec.universe,
            str(pd.Timestamp.now().date()),
        )
        strategy = pin_decision_strategy(
            spec.strategy,
            spec.universe,
            symbols=symbols,
        )
        if strategy is not spec.strategy:
            spec = spec.model_copy(update={"strategy": strategy})
        return spec, symbols, meta

    def create_account(self, spec: PaperAccountSpec) -> dict:
        spec, symbols, meta = self._materialize_account_spec(spec)
        return self.store.create_account(
            spec,
            symbols=symbols,
            universe_meta=meta,
            warning=self._strategy_warning(spec),
        )

    def account_details(self, account_id: str) -> dict:
        account = self.store.account(account_id)
        if account is None:
            raise KeyError("模拟账户不存在")
        return self._with_management(account)

    def _with_management(self, account: dict) -> dict:
        """Expose the stable account-management contract used by API clients."""
        activity = self.store.account_activity(account["id"])
        archived = account["status"] == "archived"
        account["activity"] = activity
        account["management"] = {
            "strategy_editable": not archived,
            "pending_strategy_change": bool(account.get("strategy_effective_after")),
            "strategy_effective_after": account.get("strategy_effective_after", ""),
            "can_archive": not archived,
            "can_restore": archived,
            "delete_mode": "archive",
        }
        return account

    def update_account(
        self,
        account_id: str,
        *,
        name: str | None = None,
        status: str | None = None,
        mode: str | None = None,
        strategy: StrategySpec | None = None,
        universe: str | None = None,
    ) -> dict:
        account = self.store.account(account_id)
        if account is None:
            raise KeyError("模拟账户不存在")
        if strategy is None and universe is None:
            return self.store.update_account(
                account_id,
                name=name,
                status=status,
                mode=mode,
            )
        candidate_strategy = strategy if strategy is not None else account["strategy"]
        strategy_payload = (
            candidate_strategy.model_dump(mode="json")
            if hasattr(candidate_strategy, "model_dump")
            else candidate_strategy
        )
        candidate_universe = universe if universe is not None else account["universe"]
        if strategy_payload == account["strategy"] and candidate_universe == account["universe"]:
            return self.store.update_account(
                account_id,
                name=name,
                status=status,
                mode=mode,
            )
        if status is not None and status != account["status"]:
            raise ValueError("修改策略时请单独保存账户状态")
        spec = PaperAccountSpec.model_validate(
            {
                "name": name if name is not None else account["name"],
                "strategy": candidate_strategy,
                "universe": candidate_universe,
                "initial_capital": account["initial_capital"],
                "mode": mode if mode is not None else account["mode"],
                # Once the snapshot changes it is no longer identical to its source backtest.
                "source_backtest_id": "",
            }
        )
        spec, symbols, meta = self._materialize_account_spec(spec)
        effective_after = self._strategy_change_signal_date()
        updated = self.store.replace_strategy(
            account_id,
            spec,
            symbols=symbols,
            universe_meta=meta,
            warning=self._strategy_warning(spec),
            effective_after=effective_after,
        )
        try:
            transition = self.propose(account_id)
        except (KeyError, OSError, RuntimeError, ValueError):
            logger.warning("模拟账户策略已保存，但强制调仓提案生成失败", exc_info=True)
            message = f"策略已保存，等待行情就绪后按 {effective_after} 信号日生成强制调仓"
            self.store.set_runtime_warning(account_id, message)
            updated = self.store.account(account_id) or updated
            updated["transition"] = {
                "status": "waiting_data",
                "signal_date": effective_after,
                "message": message,
            }
            return updated
        updated = self.store.account(account_id) or updated
        updated["transition"] = transition
        return updated

    @staticmethod
    def _strategy_change_signal_date(now: datetime | None = None) -> str:
        current = now or datetime.now(SHANGHAI)
        if current.tzinfo is None:
            current = current.replace(tzinfo=SHANGHAI)
        else:
            current = current.astimezone(SHANGHAI)
        signal_day = current.date()
        if (current.hour, current.minute) >= (15, 0):
            signal_day += timedelta(days=1)
        return signal_day.isoformat()

    def archive_account(self, account_id: str) -> dict:
        return self.store.archive_account(account_id)

    @staticmethod
    def _strategy_warning(spec: PaperAccountSpec) -> str:
        """未批准策略可以进入模拟盘，但必须持续显示来源风险。"""
        strategy = spec.strategy
        from quantmaster.backtest.spec import DecisionStrategySpec

        if isinstance(strategy, DecisionStrategySpec):
            components = strategy.policy_snapshot.get("components") or []
            champions = [item for item in components if item.get("role") in {"factor", "ml"}]
            if champions:
                return ""
            return "Hybrid v2 当前仅使用规则基线；可用于模拟验证，尚未叠加 Quant Lab Champion。"
        if not isinstance(strategy, FactorStrategySpec):
            return "该规则策略未关联 Quant Lab 批准版本；可用于模拟验证，不代表已通过研究门禁。"
        from quantmaster.backtest.spec import split_factor_references

        names = split_factor_references(strategy.factor)
        try:
            from quantmaster.lab.store import LabStore

            catalog = LabStore().list_factors(limit=500).get("items", [])
            status_by_slug = {str(item.get("slug")): str(item.get("status")) for item in catalog}
        except Exception:
            status_by_slug = {}
        unapproved = [name for name in names if status_by_slug.get(name) not in {"approved", "production"}]
        if not unapproved:
            return ""
        shown = "、".join(unapproved[:5])
        suffix = f" 等 {len(unapproved)} 项" if len(unapproved) > 5 else ""
        return f"因子 {shown}{suffix} 未关联已批准版本；允许模拟交易，但结果需结合研究验证判断。"

    def clone_account(self, account_id: str, *, name: str, mode: str = "manual") -> dict:
        account = self.store.account(account_id)
        if account is None:
            raise KeyError("模拟账户不存在")
        spec = PaperAccountSpec.model_validate(
            {
                "name": name,
                "strategy": account["strategy"],
                "universe": account["universe"],
                "initial_capital": account["initial_capital"],
                "mode": mode,
                "source_backtest_id": account["source_backtest_id"],
            }
        )
        symbols = account["universe_snapshot"].get("symbols", [])
        return self.store.create_account(
            spec,
            symbols=symbols,
            universe_meta={"cloned_from": account_id},
            warning=self._strategy_warning(spec),
        )

    @staticmethod
    def _prices_from_row(row: pd.Series) -> dict[str, float]:
        return {
            str(symbol): float(value)
            for symbol, value in row.items()
            if value is not None and math.isfinite(float(value)) and float(value) > 0
        }

    def propose(
        self,
        account_id: str,
        *,
        panel: dict[str, pd.DataFrame] | None = None,
        lookback_days: int = 400,
    ) -> dict:
        account = self.store.account(account_id)
        if account is None:
            raise KeyError("模拟账户不存在")
        if account["status"] != "active":
            raise ValueError("账户已暂停或归档，不能生成新提案")
        transition_after = str(account.get("strategy_effective_after") or "")
        force_transition = bool(transition_after)
        eligible_symbols = list(account["universe_snapshot"].get("symbols") or [])
        symbols = list(eligible_symbols)
        symbols.extend(
            position.symbol
            for position in self.store.ledger(account_id).positions()
            if position.shares > 0 and position.symbol not in symbols
        )
        if not eligible_symbols:
            raise ValueError("账户候选快照为空")
        loaded_live = panel is None
        if panel is None:
            from quantmaster.data import load_panel

            end = pd.Timestamp.now().normalize()
            start = end - pd.Timedelta(days=lookback_days)
            panel = load_panel(symbols, str(start.date()), str(end.date()))
        close = panel.get("close")
        if close is None or close.empty:
            raise ValueError("没有可用于生成信号的收盘行情")
        close = close.sort_index()
        latest_date = pd.Timestamp(close.index[-1])
        warnings: list[dict[str, str]] = []
        if loaded_live and (pd.Timestamp.now().normalize() - latest_date.normalize()).days > 7:
            message = f"最新行情停留在 {latest_date.date()}，账户已暂停以避免使用过期数据。"
            self.store.set_warning(account_id, message, pause=True)
            raise ValueError(message)
        strategy_spec = account["strategy"]
        if strategy_spec.get("kind") == "swing":
            from quantmaster.backtest.spec import SwingStrategySpec

            parsed_strategy = SwingStrategySpec.model_validate(strategy_spec)
        elif strategy_spec.get("kind") == "decision":
            from quantmaster.backtest.spec import DecisionStrategySpec

            parsed_strategy = DecisionStrategySpec.model_validate(strategy_spec)
        elif strategy_spec.get("kind") == "lab_version":
            raise ValueError("Lab OOF 回测策略不能生成实时模拟提案")
        else:
            parsed_strategy = FactorStrategySpec.model_validate(strategy_spec)
        if not force_transition and not signal_is_due(
            parsed_strategy,
            close.index,
            len(close.index) - 1,
        ):
            return {
                "status": "not_due",
                "account_id": account_id,
                "signal_date": latest_date.strftime("%Y-%m-%d"),
                "message": "今天不是该策略的调仓日，未生成提案。",
            }
        strategy_panel = {
            key: frame.reindex(columns=eligible_symbols)
            for key, frame in panel.items()
            if isinstance(frame, pd.DataFrame)
        }
        strategy = build_strategy(
            parsed_strategy,
            eligible_symbols,
            pd.Timestamp(close.index[0]).strftime("%Y-%m-%d"),
            latest_date.strftime("%Y-%m-%d"),
            universe=account["universe"],
        )
        weights_frame = strategy.target_weights(strategy_panel)
        latest = pd.to_numeric(weights_frame.iloc[-1], errors="coerce").fillna(0.0).clip(lower=0)
        target = {str(symbol): float(value) for symbol, value in latest.items() if value > 0}
        for position in self.store.ledger(account_id).positions():
            if position.shares > 0:
                target.setdefault(position.symbol, 0.0)
        prices = self._prices_from_row(close.iloc[-1])
        missing = sorted(symbol for symbol in target if symbol not in prices)
        if missing:
            warnings.append(
                {
                    "code": "missing_close",
                    "level": "warning",
                    "message": f"{len(missing)} 只标的缺少信号日收盘价，将等待可用行情。",
                }
            )
        if force_transition:
            warnings.append(
                {
                    "code": "strategy_changed",
                    "level": "info",
                    "message": (
                        f"策略或候选已修改；按 {transition_after} 作为信号日，"
                        "在其后的首个真实交易日开盘执行新旧持仓切换。"
                    ),
                }
            )
        signal_date = transition_after or latest_date.strftime("%Y-%m-%d")
        cycle, created = self.store.create_cycle(
            account,
            signal_date,
            target,
            prices,
            warnings,
        )
        if (force_transition or account["mode"] == "auto") and (cycle.get("status") == "proposed"):
            cycle = self.store.confirm(cycle["id"])
        if force_transition and cycle.get("id"):
            self.store.clear_strategy_transition(account_id, account["strategy_hash"])
            self.store.clear_runtime_warning(account_id)
        cycle["created"] = created
        cycle["ledger_written"] = False
        return cycle

    @staticmethod
    def _available_to_sell(ledger: Ledger, symbol: str, execution_date: str) -> float:
        trades = ledger.trades()
        if trades.empty:
            return 0.0
        eligible = trades[(trades["symbol"] == symbol) & (trades["date"] < execution_date)]
        buys = float(eligible.loc[eligible["side"] == "buy", "shares"].sum())
        sells = float(eligible.loc[eligible["side"] == "sell", "shares"].sum())
        return max(0.0, buys - sells)

    def process(
        self,
        account_id: str,
        *,
        panel: dict[str, pd.DataFrame] | None = None,
    ) -> dict:
        account = self.store.account(account_id)
        if account is None:
            raise KeyError("模拟账户不存在")
        if account["status"] != "active":
            return {
                "status": "paused",
                "account_id": account_id,
                "message": "账户已暂停或归档，待开盘订单没有处理。",
            }
        cycles = [
            cycle
            for cycle in self.store.cycles(account_id, limit=100)
            if cycle["status"] in {"confirmed", "blocked"}
        ]
        if not cycles:
            return {"status": "idle", "account_id": account_id, "message": "没有待撮合订单。"}
        cycle = cycles[-1]
        ledger = self.store.ledger(account_id)
        held = [position.symbol for position in ledger.positions() if position.shares > 0]
        symbols = sorted(set(cycle["target_weights"]) | set(held))
        if panel is None:
            from quantmaster.data import load_panel

            start = str((pd.Timestamp(cycle["signal_date"]) - pd.Timedelta(days=7)).date())
            panel = load_panel(symbols, start, str(pd.Timestamp.now().date()))
        close, open_prices = panel.get("close"), panel.get("open")
        if close is None or open_prices is None or close.empty or open_prices.empty:
            raise ValueError("缺少开盘价或昨收价，订单继续等待")
        dates = pd.DatetimeIndex(close.index).sort_values()
        after = pd.Timestamp(cycle["execution_date"] or cycle["signal_date"])
        eligible_dates = dates[dates > after]
        if eligible_dates.empty:
            return {
                "status": "waiting_open",
                "cycle": cycle,
                "message": "信号后的下一交易日开盘价尚未到达，未写入成交。",
            }
        execution = eligible_dates[0]
        execution_date = execution.strftime("%Y-%m-%d")
        previous_dates = dates[dates < execution]
        previous = previous_dates[-1] if len(previous_dates) else None
        day_open = open_prices.reindex(index=dates).loc[execution]
        day_previous = close.loc[previous] if previous is not None else pd.Series(dtype=float)
        valuation = self._prices_from_row(day_open)
        report = ledger_report(ledger, prices=valuation)
        total_assets, cash = float(report["total_assets"]), float(report["cash"])
        current = {position.symbol: position.shares for position in ledger.positions()}
        trade_config = get_config().trade
        orders = [order for order in cycle["orders"] if order["status"] in {"queued", "blocked"}]
        ledger_trades = ledger.trades()
        if not ledger_trades.empty and "idempotency_key" in ledger_trades:
            existing = {
                str(row["idempotency_key"]): row
                for _, row in ledger_trades.loc[ledger_trades["idempotency_key"].notna()].iterrows()
            }
            pending_orders = []
            for order in orders:
                trade = existing.get(order["idempotency_key"])
                if trade is None:
                    pending_orders.append(order)
                    continue
                self.store.update_order(
                    order["id"],
                    status="filled",
                    side=str(trade["side"]),
                    shares=float(trade["shares"]),
                    price=float(trade["price"]),
                    fee=float(trade["fee"]),
                    reason="reconciled",
                )
            orders = pending_orders
        executable: list[tuple[dict, str, float, float, float, float | None]] = []
        for order in orders:
            symbol = order["symbol"]
            raw_open = day_open.get(symbol)
            raw_previous = day_previous.get(symbol) if previous is not None else None
            open_value = float(raw_open) if pd.notna(raw_open) else 0.0
            previous_value = (
                float(raw_previous) if raw_previous is not None and pd.notna(raw_previous) else None
            )
            current_shares = float(current.get(symbol, 0.0))
            target_value = total_assets * float(order["target_weight"])
            target_shares = (
                math.floor(target_value / open_value / trade_config.lot_size) * trade_config.lot_size
                if open_value > 0
                else current_shares
            )
            diff = target_shares - current_shares
            side = "buy" if diff > 0 else "sell" if diff < 0 else "hold"
            desired_value = abs(target_value - current_shares * open_value)
            executable.append(
                (
                    order,
                    side,
                    abs(diff),
                    desired_value,
                    open_value,
                    previous_value,
                )
            )
        executable.sort(key=lambda item: 0 if item[1] == "sell" else 1)
        filled, blocked = [], []
        for order, side, desired_shares, desired_value, raw_open, previous_close in executable:
            symbol = order["symbol"]
            if side == "hold" or desired_shares <= 0:
                self.store.update_order(order["id"], status="skipped", side="hold")
                continue
            quote = quote_open(symbol, side, raw_open, previous_close, trade_config)
            if quote.blocked_reason:
                self.store.update_order(
                    order["id"],
                    status="blocked",
                    side=side,
                    reason=quote.blocked_reason,
                )
                blocked.append({"symbol": symbol, "side": side, "reason": quote.blocked_reason})
                continue
            if side == "sell":
                available = self._available_to_sell(ledger, symbol, execution_date)
                shares = min(desired_shares, available, float(current.get(symbol, 0.0)))
                if shares < float(current.get(symbol, 0.0)) - 1e-9:
                    shares = math.floor(shares / trade_config.lot_size) * trade_config.lot_size
                if shares <= 0:
                    self.store.update_order(
                        order["id"],
                        status="blocked",
                        side=side,
                        reason="t_plus_one",
                    )
                    blocked.append({"symbol": symbol, "side": side, "reason": "t_plus_one"})
                    continue
                amount = shares * quote.execution_price
                fee = sell_cost(amount, trade_config)
            else:
                shares = executable_buy_shares(cash, desired_value, raw_open, trade_config)
                if shares <= 0:
                    self.store.update_order(
                        order["id"],
                        status="blocked",
                        side=side,
                        reason="insufficient_cash",
                    )
                    blocked.append({"symbol": symbol, "side": side, "reason": "insufficient_cash"})
                    continue
                amount = shares * quote.execution_price
                fee = buy_cost(amount, trade_config)
            trade = TradeRecord(
                date=execution_date,
                symbol=symbol,
                side=side,
                price=round(quote.execution_price, 4),
                shares=shares,
                fee=round(fee, 2),
                note=f"paper cycle {cycle['id']}",
            )
            written = ledger.add_trade(trade, idempotency_key=order["idempotency_key"])
            self.store.update_order(
                order["id"],
                status="filled",
                side=side,
                shares=shares,
                price=trade.price,
                fee=trade.fee,
            )
            if side == "buy":
                cash -= amount + fee
                current[symbol] = current.get(symbol, 0.0) + shares
            else:
                cash += amount - fee
                current[symbol] = max(0.0, current.get(symbol, 0.0) - shares)
            filled.append({**trade.__dict__, "written": written})
        status = "blocked" if blocked else "completed"
        cycle = self.store.update_cycle_status(cycle["id"], status, execution_date)
        final_report = ledger_report(ledger, prices=valuation)
        if float(final_report["cash"]) < -1e-6:
            message = "撮合后现金为负，账户已暂停；请检查账本完整性。"
            self.store.set_warning(account_id, message, pause=True)
            raise RuntimeError(message)
        return {
            "status": status,
            "cycle": cycle,
            "filled": filled,
            "blocked": blocked,
            "report": final_report,
        }

    def report(self, account_id: str) -> dict:
        from quantmaster.data.storage import BarStore
        from quantmaster.portfolio.nav import daily_nav, nav_warnings

        account = self.store.account(account_id)
        if account is None:
            raise KeyError("模拟账户不存在")
        ledger = self.store.ledger(account_id)
        trades = ledger.trades()
        store = BarStore()
        price_series: dict[str, pd.Series] = {}
        price_map: dict[str, float] = {}
        symbols = sorted(trades["symbol"].unique()) if not trades.empty else []
        freshness = []
        for symbol in symbols:
            cached = store.get(symbol)
            if cached is None or cached.empty or cached["close"].dropna().empty:
                freshness.append({"symbol": symbol, "status": "missing"})
                continue
            series = cached["close"].dropna()
            price_series[symbol] = series
            price_map[symbol] = float(series.iloc[-1])
            freshness.append(
                {
                    "symbol": symbol,
                    "status": "ready",
                    "as_of": pd.Timestamp(series.index[-1]).strftime("%Y-%m-%d"),
                }
            )
        report = ledger_report(ledger, prices=price_map)
        dates: list[str] = []
        twr: list[float] = []
        warnings = list(report.get("warnings") or [])
        if not trades.empty and price_series:
            nav = daily_nav(ledger, pd.DataFrame(price_series))
            if not nav.empty:
                dates = [pd.Timestamp(item).strftime("%Y-%m-%d") for item in nav.index]
                twr = [round(float(value), 6) for value in nav["twr_nav"]]
                warnings.extend(nav_warnings(nav))
        if account["warning"]:
            warnings.insert(0, account["warning"])
        automation = self.store.latest_auto_run(account_id)
        if automation is not None:
            automation["health"] = (
                "needs_manual_recovery"
                if automation.get("status") == "manual_recovery"
                else "healthy"
                if automation.get("status") == "completed"
                else "retrying"
                if automation.get("status") in {"failed", "running"}
                else "idle"
            )
        account = self._with_management(account)
        return {
            "account": account,
            "report": report,
            "dates": dates,
            "twr": twr,
            "warnings": list(dict.fromkeys(warnings)),
            "data_freshness": freshness,
            "cycles": self.store.cycles(account_id),
            "warning": account["warning"],
            "strategy_warning": account["strategy_warning"],
            "runtime_warning": account["runtime_warning"],
            "automation": automation,
        }

    def run_auto_account(
        self,
        account_id: str,
        *,
        expected_signal_date: str | None = None,
    ) -> dict:
        """Process one auto account and create/confirm its newest due proposal."""
        account = self.store.account(account_id)
        if account is None:
            raise KeyError("模拟账户不存在")
        if account["status"] != "active" or account["mode"] != "auto":
            return {
                "status": "skipped",
                "account_id": account_id,
                "message": "账户未启用自动交易。",
            }
        processed = self.process(account_id)
        proposal = self.propose(account_id)
        signal_date = str(proposal.get("signal_date") or "")
        if expected_signal_date and signal_date < expected_signal_date:
            raise RuntimeError(
                f"自动交易等待 {expected_signal_date} 收盘行情；当前最新信号日为 {signal_date or '未知'}"
            )
        return {
            "status": "ok",
            "account_id": account_id,
            "processed": processed,
            "proposal": proposal,
        }

    def run_auto_accounts(self, *, expected_signal_date: str | None = None) -> dict:
        """Run only active accounts whose execution mode is explicitly ``auto``."""
        items = []
        for account in self.store.accounts():
            if account["status"] != "active" or account["mode"] != "auto":
                continue
            try:
                row = self.run_auto_account(
                    account["id"],
                    expected_signal_date=expected_signal_date,
                )
            except (KeyError, OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
                message = str(exc)[:500]
                self.store.set_warning(account["id"], message)
                row = {
                    "status": "failed",
                    "account_id": account["id"],
                    "name": account["name"],
                    "error": message,
                }
                logger.warning("模拟账户自动处理失败 account=%s: %s", account["id"], exc)
            items.append(row)
        return {
            "accounts": items,
            "processed": sum(item.get("status") == "ok" for item in items),
            "failed": sum(item.get("status") == "failed" for item in items),
        }

    def run_active_accounts(self) -> dict:
        """供每日例程/自动化调用：处理已确认订单，并为自动账户生成提案。"""
        items = []
        for account in self.store.accounts():
            if account["status"] != "active":
                continue
            row: dict = {"account_id": account["id"], "name": account["name"]}
            try:
                row["processed"] = self.process(account["id"])
                if account["mode"] == "auto":
                    row["proposal"] = self.propose(account["id"])
                row["status"] = "ok"
            except (KeyError, OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
                message = str(exc)[:500]
                self.store.set_warning(account["id"], message)
                row.update({"status": "failed", "error": message})
                logger.warning("模拟账户自动处理失败 account=%s: %s", account["id"], exc)
            items.append(row)
        return {
            "accounts": items,
            "processed": sum(item.get("status") == "ok" for item in items),
            "failed": sum(item.get("status") == "failed" for item in items),
        }


_service: PaperService | None = None
_service_root = ""
_paper_singleton_lock = threading.RLock()


def get_paper_service() -> PaperService:
    global _service, _service_root
    root = str(get_config().data_root.resolve())
    with _paper_singleton_lock:
        if _service is None or root != _service_root:
            _service = PaperService()
            _service_root = root
    return _service
