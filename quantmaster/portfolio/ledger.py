"""实盘交易账本：记录真实成交，核算持仓成本与已实现盈亏。

数据全部存本地 SQLite。支持三类记录：
- trade    买入/卖出成交（手动添加或券商导出 CSV 导入）
- cash     出入金（计算收益率必须区分「入金」和「盈利」）
- dividend 分红/送转（简化为现金分红入账）

成本核算用 FIFO（先进先出）：卖出时按最早买入的批次配对成本，
与多数券商 App 展示的「摊薄成本」略有差异，但对已实现盈亏更准确。

券商 CSV 导入格式（表头需含，编码 UTF-8/GBK 均可）：
    date,symbol,side,price,shares,fee
    2024-01-08,600519.SH,buy,1620.0,100,8.1
"""

from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from quantmaster.config import get_config
from quantmaster.runtime.sqlite import connect_sqlite


class LedgerIntegrityError(ValueError):
    """Stored trades violate a ledger accounting invariant."""


def _finite_number(
    value: Any,
    label: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{label}必须是有限数字") from exc
    if not math.isfinite(number):
        raise ValueError(f"{label}必须是有限数字")
    if positive and number <= 0:
        raise ValueError(f"{label}必须为正数")
    if nonnegative and number < 0:
        raise ValueError(f"{label}不能为负数")
    return number


def _normalized_date(value: object) -> str:
    try:
        timestamp = pd.to_datetime(value, errors="raise")
    except (ValueError, TypeError, OverflowError) as exc:
        raise ValueError("date 必须是有效日期") from exc
    if pd.isna(timestamp):
        raise ValueError("date 必须是有效日期")
    return str(pd.Timestamp(timestamp).date())


@dataclass
class TradeRecord:
    date: str          # YYYY-MM-DD
    symbol: str
    side: str          # buy / sell
    price: float
    shares: float
    fee: float = 0.0
    note: str = ""


@dataclass
class Position:
    symbol: str
    shares: float
    avg_cost: float            # FIFO 剩余批次的加权成本
    realized_pnl: float        # 该标的累计已实现盈亏（含费用）
    cost_basis_complete: bool = True
    unknown_cost_shares: float = 0.0


class Ledger:
    def __init__(self, path: Path | None = None, name: str = "default"):
        self.path = path or get_config().data_root / f"ledger_{name}.sqlite"
        with self._conn() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS trades ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, symbol TEXT,"
                "side TEXT, price REAL, shares REAL, fee REAL, note TEXT,"
                "import_batch TEXT, fingerprint TEXT, idempotency_key TEXT)"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS cashflows ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT,"
                "amount REAL, kind TEXT, note TEXT, idempotency_key TEXT)"   # kind: deposit/withdraw/dividend
            )
            columns = {row[1] for row in conn.execute("PRAGMA table_info(trades)")}
            if "import_batch" not in columns:
                conn.execute("ALTER TABLE trades ADD COLUMN import_batch TEXT")
            if "fingerprint" not in columns:
                conn.execute("ALTER TABLE trades ADD COLUMN fingerprint TEXT")
            if "idempotency_key" not in columns:
                conn.execute("ALTER TABLE trades ADD COLUMN idempotency_key TEXT")
            cash_columns = {row[1] for row in conn.execute("PRAGMA table_info(cashflows)")}
            if "idempotency_key" not in cash_columns:
                conn.execute("ALTER TABLE cashflows ADD COLUMN idempotency_key TEXT")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS import_batches ("
                "id TEXT PRIMARY KEY, file_hash TEXT NOT NULL, filename TEXT,"
                "encoding TEXT, imported_at TEXT DEFAULT CURRENT_TIMESTAMP, row_count INTEGER)"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_fingerprint ON trades(fingerprint)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_import_file_hash ON import_batches(file_hash)")
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_idempotency "
                "ON trades(idempotency_key) WHERE idempotency_key IS NOT NULL")
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_cashflows_idempotency "
                "ON cashflows(idempotency_key) WHERE idempotency_key IS NOT NULL")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS ledger_anomalies ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,kind TEXT NOT NULL,"
                "reference_id INTEGER NOT NULL,symbol TEXT NOT NULL,trade_date TEXT NOT NULL,"
                "details_json TEXT NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                "UNIQUE(kind,reference_id))"
            )
            self._migrate_historical_inventory_anomalies(conn)

    def _conn(self) -> sqlite3.Connection:
        return connect_sqlite(self.path)

    @staticmethod
    def _migrate_historical_inventory_anomalies(connection: sqlite3.Connection) -> None:
        """Classify legacy unmatched sells without inventing a zero cost basis."""
        rows = connection.execute(
            "SELECT id,date,symbol,side,shares FROM trades ORDER BY symbol,date,id"
        ).fetchall()
        balances: dict[str, float] = {}
        for trade_id, trade_date, symbol, side, raw_shares in rows:
            shares = _finite_number(raw_shares, "账本成交数量", positive=True)
            key = str(symbol)
            balance = balances.get(key, 0.0)
            if str(side).lower() == "buy":
                balances[key] = balance + shares
                continue
            if str(side).lower() != "sell":
                continue
            remaining = balance - shares
            if remaining >= -1e-9:
                balances[key] = max(0.0, remaining)
                continue
            unknown = abs(remaining)
            connection.execute(
                "INSERT OR IGNORE INTO ledger_anomalies "
                "(kind,reference_id,symbol,trade_date,details_json) VALUES (?,?,?,?,?)",
                (
                    "unknown_cost_sell", int(trade_id), key, str(trade_date),
                    json.dumps({
                        "unknown_cost_shares": unknown,
                        "accounting_effect": "excluded_from_realized_pnl",
                        "migration": "legacy_unmatched_sell_v1",
                    }, ensure_ascii=False, allow_nan=False, separators=(",", ":")),
                ),
            )
            # The unmatched quantity represents pre-ledger inventory.  It is consumed by
            # this historical sale and must not make later, known purchases look negative.
            balances[key] = 0.0

    # ---- 写入 ----

    @staticmethod
    def _validate_inventory(
        connection: sqlite3.Connection, records: list[dict[str, Any]],
    ) -> None:
        """Reject any chronological prefix whose inventory becomes negative."""
        symbols = sorted({str(record["symbol"]) for record in records})
        if not symbols:
            return
        placeholders = ",".join("?" for _ in symbols)
        rows = connection.execute(
            "SELECT id,date,symbol,side,shares FROM trades "
            f"WHERE symbol IN ({placeholders})",
            symbols,
        ).fetchall()
        legacy_anomaly_ids = {
            int(row[0]) for row in connection.execute(
                "SELECT reference_id FROM ledger_anomalies WHERE kind='unknown_cost_sell'"
            ).fetchall()
        }
        events: dict[str, list[tuple[str, int, int, str, float]]] = {
            symbol: [] for symbol in symbols
        }
        for row in rows:
            shares = _finite_number(row[4], "账本成交数量", positive=True)
            side = str(row[3]).lower()
            if side not in {"buy", "sell"}:
                raise LedgerIntegrityError(f"账本包含非法方向: {side}")
            events[str(row[2])].append((str(row[1]), 0, int(row[0]), side, shares))
        for index, record in enumerate(records):
            events[str(record["symbol"])].append((
                str(record["date"]), 1, index, str(record["side"]), float(record["shares"]),
            ))
        for symbol, values in events.items():
            balance = 0.0
            for trade_date, source, order, side, shares in sorted(values):
                balance += shares if side == "buy" else -shares
                if balance < -1e-9:
                    if source == 0 and order in legacy_anomaly_ids:
                        balance = 0.0
                        continue
                    raise LedgerIntegrityError(
                        f"{symbol} 在 {trade_date} 卖出超过可用持仓 "
                        f"{abs(balance):g} 股；请先补录买入或修正成交数量"
                    )

    @staticmethod
    def _normalize_trade(trade: TradeRecord | dict[str, Any]) -> dict[str, Any]:
        from quantmaster.data.universe import normalize_symbol

        value = trade.__dict__ if isinstance(trade, TradeRecord) else dict(trade)
        side = str(value.get("side") or "").lower()
        if side not in ("buy", "sell"):
            raise ValueError(f"side 必须是 buy/sell: {value.get('side')}")
        try:
            symbol = normalize_symbol(str(value.get("symbol") or ""))
        except (ValueError, TypeError) as exc:
            raise ValueError(str(exc)) from None
        return {
            **value,
            "date": _normalized_date(value.get("date")),
            "symbol": symbol,
            "side": side,
            "price": _finite_number(value.get("price"), "price", positive=True),
            "shares": _finite_number(value.get("shares"), "shares", positive=True),
            "fee": _finite_number(value.get("fee", 0), "fee", nonnegative=True),
            "note": str(value.get("note") or "")[:1000],
        }

    def add_trade(self, trade: TradeRecord, idempotency_key: str | None = None) -> bool:
        from quantmaster.runtime.maintenance import maintenance_barrier

        maintenance_barrier.require_writable()
        value = self._normalize_trade(trade)
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if idempotency_key and conn.execute(
                "SELECT 1 FROM trades WHERE idempotency_key=?", (idempotency_key,),
            ).fetchone():
                return False
            self._validate_inventory(conn, [value])
            cursor = conn.execute(
                "INSERT OR IGNORE INTO trades "
                "(date,symbol,side,price,shares,fee,note,idempotency_key) VALUES (?,?,?,?,?,?,?,?)",
                (value["date"], value["symbol"], value["side"], value["price"],
                 value["shares"], value["fee"], value["note"], idempotency_key),
            )
        return cursor.rowcount == 1

    def add_cashflow(self, date: str, amount: float, kind: str = "deposit", note: str = "",
                     idempotency_key: str | None = None) -> bool:
        from quantmaster.runtime.maintenance import maintenance_barrier

        maintenance_barrier.require_writable()
        if kind not in ("deposit", "withdraw", "dividend"):
            raise ValueError(f"kind 必须是 deposit/withdraw/dividend: {kind}")
        normalized_amount = _finite_number(amount, "amount")
        if normalized_amount == 0:
            raise ValueError("amount 必须为非零数")
        normalized_date = _normalized_date(date)
        with self._conn() as conn:
            cursor = conn.execute(
                "INSERT OR IGNORE INTO cashflows "
                "(date,amount,kind,note,idempotency_key) VALUES (?,?,?,?,?)",
                (normalized_date, abs(normalized_amount), kind, note[:1000], idempotency_key),
            )
        return cursor.rowcount == 1

    def import_csv(self, csv_path: str | Path) -> int:
        """导入券商成交记录 CSV。返回导入条数。"""
        from quantmaster.portfolio.csv_import import parse_broker_csv

        path = Path(csv_path)
        parsed = parse_broker_csv(path.read_bytes(), existing_fingerprints=self.fingerprints())
        bad = [row for row in parsed.rows if row.errors]
        if bad:
            raise ValueError(f"CSV 有 {len(bad)} 行校验失败: 第 {bad[0].row_number} 行 {bad[0].errors[0]}")
        records = [row.record for row in parsed.rows if row.record and not row.duplicate]
        return self.import_records(records, parsed.file_hash, path.name, parsed.encoding)

    def fingerprints(self) -> set[str]:
        from quantmaster.portfolio.csv_import import trade_fingerprint

        with self._conn() as conn:
            rows = conn.execute(
                "SELECT date,symbol,side,price,shares,fee,fingerprint FROM trades"
            ).fetchall()
        return {str(row[6]) if row[6] else trade_fingerprint({
            "date": row[0], "symbol": row[1], "side": row[2], "price": row[3],
            "shares": row[4], "fee": row[5],
        }) for row in rows}

    def has_import_hash(self, file_hash: str) -> bool:
        with self._conn() as conn:
            return conn.execute(
                "SELECT 1 FROM import_batches WHERE file_hash=? LIMIT 1", (file_hash,)
            ).fetchone() is not None

    def import_records(self, records: list[dict], file_hash: str, filename: str,
                       encoding: str) -> int:
        """在单个 SQLite 事务中写入最终记录；任一失败会整体回滚。"""
        import uuid

        from quantmaster.runtime.maintenance import maintenance_barrier

        maintenance_barrier.require_writable()

        if not records:
            return 0
        normalized = [self._normalize_trade(record) for record in records]
        batch_id = uuid.uuid4().hex
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._validate_inventory(conn, normalized)
            conn.executemany(
                "INSERT INTO trades "
                "(date,symbol,side,price,shares,fee,note,import_batch,fingerprint) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                [(record["date"], record["symbol"], record["side"],
                  record["price"], record["shares"], record["fee"],
                  str(record.get("note", "")), batch_id, record.get("fingerprint"))
                 for record in normalized],
            )
            conn.execute(
                "INSERT INTO import_batches (id,file_hash,filename,encoding,row_count) VALUES (?,?,?,?,?)",
                (batch_id, file_hash, filename, encoding, len(normalized)),
            )
        return len(normalized)

    # ---- 读取 ----

    def trades(self) -> pd.DataFrame:
        with self._conn() as conn:
            return pd.read_sql_query(
                "SELECT * FROM trades ORDER BY date, id", conn)

    def cashflows(self) -> pd.DataFrame:
        with self._conn() as conn:
            return pd.read_sql_query(
                "SELECT * FROM cashflows ORDER BY date, id", conn)

    def anomalies(self) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id,kind,reference_id,symbol,trade_date,details_json,created_at "
                "FROM ledger_anomalies ORDER BY id"
            ).fetchall()
        return [
            {
                "id": row[0], "kind": row[1], "reference_id": row[2],
                "symbol": row[3], "trade_date": row[4],
                "details": json.loads(row[5]), "created_at": row[6],
            }
            for row in rows
        ]

    # ---- 核算 ----

    def positions(self) -> list[Position]:
        """FIFO 核算当前持仓与各标的已实现盈亏。"""
        trades = self.trades()
        result: list[Position] = []
        for symbol, group in trades.groupby("symbol"):
            lots: list[list[float]] = []   # [shares, price_with_fee_per_share]
            realized = 0.0
            unknown_cost_shares = 0.0
            for _, t in group.iterrows():
                if t["side"] == "buy":
                    per_share_cost = t["price"] + t["fee"] / t["shares"]
                    lots.append([t["shares"], per_share_cost])
                else:
                    remaining = t["shares"]
                    proceeds_per_share = t["price"] - t["fee"] / t["shares"]
                    while remaining > 1e-9 and lots:
                        lot = lots[0]
                        used = min(lot[0], remaining)
                        realized += used * (proceeds_per_share - lot[1])
                        lot[0] -= used
                        remaining -= used
                        if lot[0] <= 1e-9:
                            lots.pop(0)
                    if remaining > 1e-9:
                        # Legacy broker imports may start after the position was opened.
                        # Proceeds for that unmatched quantity are intentionally excluded
                        # until the missing acquisition cost is supplied.
                        unknown_cost_shares += remaining
            total_shares = sum(lot[0] for lot in lots)
            avg_cost = (
                sum(lot[0] * lot[1] for lot in lots) / total_shares if total_shares > 0 else 0.0
            )
            result.append(Position(symbol=str(symbol), shares=total_shares,
                                   avg_cost=avg_cost, realized_pnl=realized,
                                   cost_basis_complete=unknown_cost_shares <= 1e-9,
                                   unknown_cost_shares=unknown_cost_shares))
        return result
