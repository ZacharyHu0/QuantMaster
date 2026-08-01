"""Validated per-symbol fallback routes for global reference markets."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd

from quantmaster.data.base import normalize_daily
from quantmaster.data.resilience import akshare_call


@dataclass(frozen=True)
class ReferenceFetch:
    frame: pd.DataFrame
    source: str
    attempts: tuple[dict[str, str], ...]


class ReferenceMarketUnavailable(RuntimeError):
    """Every semantically compatible provider failed for one reference symbol."""

    def __init__(self, symbol: str, attempts: list[dict[str, str]]):
        self.symbol = symbol
        self.attempts = tuple(attempts)
        detail = "；".join(
            f"{item['source']}: {item['detail']}" for item in attempts
        ) or "没有已配置的数据源"
        super().__init__(f"{symbol} 暂不可用：{detail}")


_SINA_US = {"^GSPC.US": ".INX", "^IXIC.US": ".IXIC", "^DJI.US": ".DJI"}
_SINA_HK = {"^HSI.HK": "HSI", "^HSTECH.HK": "HSTECH"}
_SINA_GLOBAL = {"^N225.JP": "日经225指数", "^KS11.KR": "首尔综合指数"}
_SINA_FUTURES = {"GC=F.US": "GC", "CL=F.US": "CL", "HG=F.US": "HG"}


def _normalize(raw: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame()
    value = raw.copy().rename(columns={
        "最新价": "close",
        "收盘价": "close",
        "bid_open": "open",
        "bid_high": "high",
        "bid_low": "low",
        "bid_close": "close",
        "trade_date": "date",
        "美国国债收益率10年": "close",
    })
    frame = normalize_daily(value)
    if "close" not in frame:
        return pd.DataFrame()
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame[frame["close"].notna() & (frame["close"] > 0)].copy()
    if frame.empty:
        return frame
    for column in ("open", "high", "low"):
        if column not in frame:
            frame[column] = frame["close"]
        else:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(frame["close"])
    if "volume" not in frame:
        frame["volume"] = 0.0
    frame = frame[~frame.index.duplicated(keep="last")].sort_index()
    return frame.loc[start:end]


def _akshare_route(symbol: str, start: str) -> tuple[str, Callable[[], pd.DataFrame]] | None:
    try:
        import akshare as ak
    except ModuleNotFoundError:
        return None
    if symbol in _SINA_US:
        return "sina:us-index", lambda: akshare_call(
            f"index_us_stock_sina({_SINA_US[symbol]})",
            ak.index_us_stock_sina,
            symbol=_SINA_US[symbol],
            lane="akshare:sina-reference",
        )
    if symbol in _SINA_HK:
        return "sina:hk-index", lambda: akshare_call(
            f"stock_hk_index_daily_sina({_SINA_HK[symbol]})",
            ak.stock_hk_index_daily_sina,
            symbol=_SINA_HK[symbol],
            lane="akshare:sina-reference",
        )
    if symbol in _SINA_GLOBAL:
        return "sina:global-index", lambda: akshare_call(
            f"index_global_hist_sina({_SINA_GLOBAL[symbol]})",
            ak.index_global_hist_sina,
            symbol=_SINA_GLOBAL[symbol],
            lane="akshare:sina-reference",
        )
    if symbol in _SINA_FUTURES:
        return "sina:foreign-futures", lambda: akshare_call(
            f"futures_foreign_hist({_SINA_FUTURES[symbol]})",
            ak.futures_foreign_hist,
            symbol=_SINA_FUTURES[symbol],
            lane="akshare:sina-reference",
        )
    if symbol == "^TNX.US":
        compact = pd.Timestamp(start).strftime("%Y%m%d")
        return "akshare:us-treasury", lambda: akshare_call(
            f"bond_zh_us_rate({compact})",
            ak.bond_zh_us_rate,
            start_date=compact,
            lane="akshare:bond-reference",
        )
    if symbol == "DX-Y.NYB.US":
        return "akshare:global-index", lambda: akshare_call(
            "index_global_hist_em(美元指数)",
            ak.index_global_hist_em,
            symbol="美元指数",
            lane="akshare:eastmoney-reference",
        )
    return None


def _tushare_cny(start: str, end: str) -> pd.DataFrame:
    from quantmaster.data.tushare_source import TushareSource

    return TushareSource()._call(
        "fx_daily",
        1,
        provider_lane="tushare:fx-reference",
        required_nonempty=True,
        required_columns=("trade_date",),
        ts_code="USDCNH.FXCM",
        start_date=pd.Timestamp(start).strftime("%Y%m%d"),
        end_date=pd.Timestamp(end).strftime("%Y%m%d"),
        fields="ts_code,trade_date,bid_open,bid_close,bid_high,bid_low",
    )


def fetch_reference(symbol: str, start: str, end: str) -> ReferenceFetch:
    """Fetch one reference without letting another symbol's failure block it."""
    attempts: list[dict[str, str]] = []
    routes: list[tuple[str, Callable[[], pd.DataFrame]]] = []
    akshare_route = _akshare_route(symbol, start)
    if akshare_route is not None:
        routes.append(akshare_route)
    if symbol == "CNY=X.US":
        routes.append(("tushare:fx", lambda: _tushare_cny(start, end)))

    from quantmaster.data.yfinance_source import YFinanceSource

    routes.append(("yfinance", lambda: YFinanceSource().daily(symbol, start, end)))
    for source, operation in routes:
        try:
            frame = _normalize(operation(), start, end)
            if frame.empty:
                raise ValueError("返回空数据或缺少有效收盘价")
            return ReferenceFetch(frame=frame, source=source, attempts=tuple(attempts))
        except (ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
            from quantmaster.logging_config import redact_sensitive_text

            attempts.append({
                "source": source,
                "code": type(exc).__name__,
                "detail": (redact_sensitive_text(exc).strip() or "请求失败")[:180],
            })
    raise ReferenceMarketUnavailable(symbol, attempts)
