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

import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from quantmaster.config import get_config
from quantmaster.runtime.sqlite import connect_sqlite


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

    def _conn(self) -> sqlite3.Connection:
        return connect_sqlite(self.path)

    # ---- 写入 ----

    def add_trade(self, trade: TradeRecord, idempotency_key: str | None = None) -> bool:
        from quantmaster.data.universe import normalize_symbol

        side = trade.side.lower()
        if side not in ("buy", "sell"):
            raise ValueError(f"side 必须是 buy/sell: {trade.side}")
        if trade.price <= 0 or trade.shares <= 0 or trade.fee < 0:
            raise ValueError("price/shares 必须为正数")
        try:
            date = str(pd.to_datetime(trade.date, errors="raise").date())
            symbol = normalize_symbol(trade.symbol)
        except (ValueError, TypeError) as exc:
            raise ValueError(str(exc)) from None
        with self._conn() as conn:
            cursor = conn.execute(
                "INSERT OR IGNORE INTO trades "
                "(date,symbol,side,price,shares,fee,note,idempotency_key) VALUES (?,?,?,?,?,?,?,?)",
                (date, symbol, side, trade.price, trade.shares, trade.fee, trade.note,
                 idempotency_key),
            )
        return cursor.rowcount == 1

    def add_cashflow(self, date: str, amount: float, kind: str = "deposit", note: str = "",
                     idempotency_key: str | None = None) -> bool:
        if kind not in ("deposit", "withdraw", "dividend"):
            raise ValueError(f"kind 必须是 deposit/withdraw/dividend: {kind}")
        if not amount:
            raise ValueError("amount 必须为非零数")
        try:
            normalized_date = str(pd.to_datetime(date, errors="raise").date())
        except (ValueError, TypeError) as exc:
            raise ValueError(str(exc)) from None
        with self._conn() as conn:
            cursor = conn.execute(
                "INSERT OR IGNORE INTO cashflows "
                "(date,amount,kind,note,idempotency_key) VALUES (?,?,?,?,?)",
                (normalized_date, abs(amount), kind, note, idempotency_key),
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

        if not records:
            return 0
        batch_id = uuid.uuid4().hex
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.executemany(
                "INSERT INTO trades "
                "(date,symbol,side,price,shares,fee,note,import_batch,fingerprint) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                [(record["date"], record["symbol"], record["side"],
                  float(record["price"]), float(record["shares"]), float(record.get("fee", 0)),
                  str(record.get("note", "")), batch_id, record.get("fingerprint"))
                 for record in records],
            )
            conn.execute(
                "INSERT INTO import_batches (id,file_hash,filename,encoding,row_count) VALUES (?,?,?,?,?)",
                (batch_id, file_hash, filename, encoding, len(records)),
            )
        return len(records)

    # ---- 读取 ----

    def trades(self) -> pd.DataFrame:
        with self._conn() as conn:
            return pd.read_sql_query(
                "SELECT * FROM trades ORDER BY date, id", conn)

    def cashflows(self) -> pd.DataFrame:
        with self._conn() as conn:
            return pd.read_sql_query(
                "SELECT * FROM cashflows ORDER BY date, id", conn)

    # ---- 核算 ----

    def positions(self) -> list[Position]:
        """FIFO 核算当前持仓与各标的已实现盈亏。"""
        trades = self.trades()
        result: list[Position] = []
        for symbol, group in trades.groupby("symbol"):
            lots: list[list[float]] = []   # [shares, price_with_fee_per_share]
            realized = 0.0
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
                        # 卖出超过持仓（可能有未录入的买入），按零成本计
                        realized += remaining * proceeds_per_share
            total_shares = sum(lot[0] for lot in lots)
            avg_cost = (
                sum(lot[0] * lot[1] for lot in lots) / total_shares if total_shares > 0 else 0.0
            )
            result.append(Position(symbol=str(symbol), shares=total_shares,
                                   avg_cost=avg_cost, realized_pnl=realized))
        return result
