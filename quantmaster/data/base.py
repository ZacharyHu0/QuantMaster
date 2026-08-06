"""数据源抽象与标准化数据模型。

所有数据源统一输出同一种日线 DataFrame 结构：
    index:   DatetimeIndex（交易日）
    columns: open, high, low, close, volume, amount(可选), turnover(可选)

symbol 约定（跨市场统一标识）：
    A股      600519.SH / 000001.SZ / 300750.SZ / 688111.SH
    港股     00700.HK
    美股     AAPL.US
    指数     000300.SH(沪深300) 931743.CSI ^N225.JP ^KS11.KR ^GSPC.US ^HSI.HK
    期货主力 AU0.SHF(沪金) CU0.SHF(沪铜) SC0.INE(原油) 等
"""

from __future__ import annotations

import enum
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd

OHLCV_COLUMNS = ["open", "high", "low", "close", "volume"]
INTRADAY_FREQUENCIES = ("1m", "5m", "15m", "30m", "60m")
_SYMBOL_PATTERN = re.compile(r"[0-9A-Za-z._^=-]{1,48}")


class Market(enum.StrEnum):
    CN = "cn"          # A 股
    HK = "hk"          # 港股
    US = "us"          # 美股
    JP = "jp"          # 日本
    KR = "kr"          # 韩国
    FUTURES = "fut"    # 商品期货/期指
    INDEX = "idx"      # 指数


class DataCapability(enum.StrEnum):
    DAILY = "daily"
    DAILY_CROSS_SECTION = "daily_cross_section"
    INTRADAY = "intraday"
    SPOT = "spot"
    INDEX_MEMBERS = "index_members"
    INDUSTRY = "industry"
    THEMES = "themes"
    BOARD_HIERARCHY = "board_hierarchy"
    NATIVE_INDICATORS = "native_indicators"


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
    if suffix == "CSI":
        return Market.INDEX
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


def normalize_bars(df: pd.DataFrame) -> pd.DataFrame:
    """把任意来源的日线或分钟线规范为统一 OHLCV 结构。"""
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]
    rename = {
        "日期": "date", "时间": "date", "日期时间": "date",
        "开盘": "open", "收盘": "close", "最高": "high", "最低": "low",
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


def normalize_daily(df: pd.DataFrame) -> pd.DataFrame:
    """向后兼容的日线标准化入口。"""
    return normalize_bars(df)


def validate_frequency(frequency: str) -> str:
    """规范并校验 K 线频率。"""
    value = frequency.strip().lower()
    aliases = {"d": "1d", "day": "1d", "daily": "1d", "60min": "60m"}
    value = aliases.get(value, value.replace("min", "m"))
    if value != "1d" and value not in INTRADAY_FREQUENCIES:
        raise ValueError(f"不支持的频率 {frequency!r}，可选: 1d/{'/'.join(INTRADAY_FREQUENCIES)}")
    return value


def validate_symbol(symbol: str) -> str:
    """Validate the canonical market identifier accepted by file-backed stores."""
    value = symbol.strip()
    if _SYMBOL_PATTERN.fullmatch(value) is None:
        raise ValueError("标的代码仅支持 1–48 位字母、数字及 ._^=- 字符")
    return value


class DataSource(ABC):
    """数据源接口。实现方按需覆盖能力，不支持的抛 NotImplementedError。"""

    name: str = "base"
    markets: tuple[Market, ...] = ()
    capabilities: frozenset[DataCapability] = frozenset({DataCapability.DAILY})

    @abstractmethod
    def daily(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        """标准化日线。"""

    def daily_many(
        self, symbols: list[str], start: str, end: str,
    ) -> dict[str, pd.DataFrame]:  # pragma: no cover - 默认兼容实现
        """批量日线；支持批量接口的数据源应覆盖此方法。"""
        return {symbol: self.daily(symbol, start, end) for symbol in symbols}

    def intraday(
        self, symbol: str, start: str, end: str, frequency: str = "5m"
    ) -> pd.DataFrame:  # pragma: no cover - 依赖网络
        """标准化分钟线；frequency 为 1m/5m/15m/30m/60m。"""
        raise NotImplementedError

    def spot(self, symbols: list[str]) -> pd.DataFrame:  # pragma: no cover - 依赖网络
        """实时快照：columns = [symbol, name, price, change_pct]。"""
        raise NotImplementedError

    def index_members(self, index_symbol: str) -> list[str]:  # pragma: no cover
        """指数成分股列表。"""
        raise NotImplementedError

    def daily_cross_section(
        self, symbols: list[str], start: str, end: str,
    ) -> pd.DataFrame:  # pragma: no cover - 可选数据源能力
        """点时日频截面；至少包含 symbol/date/OHLCV，缺失字段保留为空。"""
        raise NotImplementedError

    def board_hierarchy(self) -> list[dict]:  # pragma: no cover - 可选数据源能力
        """返回带 category/level/members 的版本化板块目录。"""
        raise NotImplementedError

    def native_indicators(
        self, names: list[str], symbols: list[str], start: str, end: str,
    ) -> dict:  # pragma: no cover - 仅用于交叉校验或加速
        """数据源原生指标；不得作为不可审计的唯一评分路径。"""
        raise NotImplementedError

    def supports(self, market: Market) -> bool:
        return market in self.markets

    def supports_capability(self, capability: DataCapability | str) -> bool:
        value = (
            capability
            if isinstance(capability, DataCapability)
            else DataCapability(str(capability))
        )
        if value in self.capabilities:
            return True
        method_name = {
            DataCapability.DAILY: "daily",
            DataCapability.INTRADAY: "intraday",
            DataCapability.SPOT: "spot",
            DataCapability.INDEX_MEMBERS: "index_members",
        }.get(value)
        if method_name is None:
            return False
        return getattr(type(self), method_name) is not getattr(DataSource, method_name)
