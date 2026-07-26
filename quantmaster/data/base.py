"""数据源抽象与标准化数据模型。

所有数据源统一输出同一种日线 DataFrame 结构：
    index:   DatetimeIndex（交易日）
    columns: open, high, low, close, volume, amount(可选), turnover(可选)

symbol 约定（跨市场统一标识）：
    A股      600519.SH / 000001.SZ / 300750.SZ / 688111.SH
    港股     00700.HK
    美股     AAPL.US
    指数     000300.SH(沪深300) ^N225.JP ^KS11.KR ^GSPC.US ^HSI.HK
    期货主力 AU0.SHF(沪金) CU0.SHF(沪铜) SC0.INE(原油) 等
"""

from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd

OHLCV_COLUMNS = ["open", "high", "low", "close", "volume"]


class Market(str, enum.Enum):
    CN = "cn"          # A 股
    HK = "hk"          # 港股
    US = "us"          # 美股
    JP = "jp"          # 日本
    KR = "kr"          # 韩国
    FUTURES = "fut"    # 商品期货/期指
    INDEX = "idx"      # 指数


@dataclass
class Bar:
    symbol: str
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float


def guess_market(symbol: str) -> Market:
    """根据 symbol 后缀推断市场。"""
    suffix = symbol.rsplit(".", 1)[-1].upper() if "." in symbol else ""
    if suffix in ("SH", "SZ", "BJ"):
        return Market.CN
    if suffix == "HK":
        return Market.HK
    if suffix == "US":
        return Market.US
    if suffix == "JP":
        return Market.JP
    if suffix == "KR":
        return Market.KR
    if suffix in ("SHF", "INE", "DCE", "CZC", "CFX", "CFFEX"):
        return Market.FUTURES
    return Market.CN


def normalize_daily(df: pd.DataFrame) -> pd.DataFrame:
    """把任意来源的日线数据规范为标准结构。"""
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]
    rename = {
        "日期": "date", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low",
        "成交量": "volume", "成交额": "amount", "换手率": "turnover",
        "adj close": "close",
    }
    df = df.rename(columns=rename)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
    df.index = pd.to_datetime(df.index)
    df.index.name = "date"
    keep = [c for c in ["open", "high", "low", "close", "volume", "amount", "turnover"] if c in df.columns]
    df = df[keep].sort_index()
    return df.astype(float)


class DataSource(ABC):
    """数据源接口。实现方按需覆盖能力，不支持的抛 NotImplementedError。"""

    name: str = "base"
    markets: tuple[Market, ...] = ()

    @abstractmethod
    def daily(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        """标准化日线。"""

    def spot(self, symbols: list[str]) -> pd.DataFrame:  # pragma: no cover - 依赖网络
        """实时快照：columns = [symbol, name, price, change_pct]。"""
        raise NotImplementedError

    def index_members(self, index_symbol: str) -> list[str]:  # pragma: no cover
        """指数成分股列表。"""
        raise NotImplementedError

    def supports(self, market: Market) -> bool:
        return market in self.markets
