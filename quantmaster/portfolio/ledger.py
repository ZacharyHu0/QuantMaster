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
                "side TEXT, price REAL, shares REAL, fee REAL, note TEXT)"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS cashflows ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT,"
                "amount REAL, kind TEXT, note TEXT)"   # kind: deposit/withdraw/dividend
            )

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)

    # ---- 写入 ----

    def add_trade(self, trade: TradeRecord) -> None:
        side = trade.side.lower()
        if side not in ("buy", "sell"):
            raise ValueError(f"side 必须是 buy/sell: {trade.side}")
        if trade.price <= 0 or trade.shares <= 0:
            raise ValueError("price/shares 必须为正数")
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO trades (date,symbol,side,price,shares,fee,note) VALUES (?,?,?,?,?,?,?)",
                (trade.date, trade.symbol, side, trade.price, trade.shares, trade.fee, trade.note),
            )

    def add_cashflow(self, date: str, amount: float, kind: str = "deposit", note: str = "") -> None:
        if kind not in ("deposit", "withdraw", "dividend"):
            raise ValueError(f"kind 必须是 deposit/withdraw/dividend: {kind}")
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO cashflows (date,amount,kind,note) VALUES (?,?,?,?)",
                (date, abs(amount), kind, note),
            )

    def import_csv(self, csv_path: str | Path) -> int:
        """导入券商成交记录 CSV。返回导入条数。"""
        try:
            df = pd.read_csv(csv_path, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(csv_path, encoding="gbk")
        df.columns = [str(c).strip().lower() for c in df.columns]
        required = {"date", "symbol", "side", "price", "shares"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"CSV 缺少列: {missing}（需要 {sorted(required)}）")
        count = 0
        for _, row in df.iterrows():
            self.add_trade(TradeRecord(
                date=str(pd.to_datetime(row["date"]).date()),
                symbol=str(row["symbol"]).strip(),
                side=str(row["side"]).strip().lower(),
                price=float(row["price"]),
                shares=float(row["shares"]),
                fee=float(row.get("fee", 0) or 0),
            ))
            count += 1
        return count

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
