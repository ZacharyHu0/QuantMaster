"""Validated per-symbol fallback routes for global reference markets."""

from __future__ import annotations

import json
import math
import sqlite3
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.base import normalize_daily
from quantmaster.data.frame_quality_access import assess_daily_frame, covers_requested_range
from quantmaster.data.resilience import akshare_call, provider_call
from quantmaster.data.storage import BarStore


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


_SINA_US = {"SPX.INDEX": ".INX", "IXIC.INDEX": ".IXIC", "DJI.INDEX": ".DJI"}
_SINA_HK = {"HSI.INDEX": "HSI", "HSTECH.INDEX": "HSTECH"}
_SINA_GLOBAL = {"N225.INDEX": "日经225指数", "KS11.INDEX": "首尔综合指数"}
_TUSHARE_GLOBAL = {
    **{symbol: symbol.partition(".")[0] for symbol in (*_SINA_US, *_SINA_GLOBAL, "HSI.INDEX")},
    "HSTECH.INDEX": "HKTECH",
}
_SINA_FUTURES = {"GC.CONTINUOUS": "GC", "CL.CONTINUOUS": "CL", "HG.CONTINUOUS": "HG"}
INDEX_TIMEZONES = {
    **dict.fromkeys((*_SINA_US, "DXY.INDEX"), "America/New_York"),
    **dict.fromkeys(_SINA_HK, "Asia/Hong_Kong"),
    "N225.INDEX": "Asia/Tokyo", "KS11.INDEX": "Asia/Seoul",
}
_REFERENCE_SYMBOLS = frozenset((*INDEX_TIMEZONES, *_SINA_FUTURES, "USD-CNY.FX", "US10Y.RATE"))


def index_contract(frame: pd.DataFrame, symbol: str) -> bool:
    """Only newly acquired, provider-bound index points qualify as references."""
    return bool(
        symbol in INDEX_TIMEZONES
        and frame.attrs.get("instrument") == symbol
        and frame.attrs.get("unit_status") == "reference_index_points_v1"
        and frame.attrs.get("quote_unit") == "point"
        and frame.attrs.get("timezone") == INDEX_TIMEZONES[symbol]
        and frame.attrs.get("provider_interface") in {
            "sina:us-index", "sina:hk-index", "sina:global-index",
            "akshare:global-index", "sina:dollar-index", "tushare:global-index", "yfinance",
        }
    )


def _normalize_index(raw: pd.DataFrame, symbol: str, source: str, start: str, end: str) -> pd.DataFrame:
    if raw.attrs.get("instrument", symbol) != symbol:
        raise ValueError("指数响应身份与请求不符")
    frame = normalize_daily(raw.rename(columns={
        "开": "open", "高": "high", "低": "low", "最新价": "close", "收盘价": "close",
    }))
    if frame.index.hasnans or frame.index.has_duplicates:
        raise ValueError("指数响应日期无效或重复")
    frame = frame.loc[start:end].copy()
    # Keep actual provider fields. Missing OHLC/turnover must never be invented.
    prices = ["open", "high", "low", "close"]
    if any(field not in frame for field in prices):
        raise ValueError("指数响应缺少真实 OHLC")
    if not frame[prices].map(lambda v: math.isfinite(v) and v > 0).all().all():
        raise ValueError("指数响应价格非有限或非正")
    frame.attrs.update(
        instrument=symbol, provider_interface=source, quote_unit="point",
        unit_status="reference_index_points_v1", timezone=INDEX_TIMEZONES[symbol],
        observation_time="provider_observation_date", provider_published_at="",
    )
    return frame


def yield_request_end(end: str, now: object) -> str:
    """Use completed New York dates; provider publication time is undocumented.

    This is a conservative request bound, not a fabricated business calendar
    or a claim that yesterday's observation has already been published.
    """
    current = pd.Timestamp(now).tz_convert("America/New_York")
    ceiling = current.tz_localize(None).normalize() - pd.Timedelta(days=1)
    return min(pd.Timestamp(end).normalize(), ceiling).date().isoformat()


def yield_contract(frame: pd.DataFrame) -> bool:
    """Admit only the provider-bound single-value percent series, never old bars."""
    return (
        list(frame.columns) == ["close"]
        and frame.attrs.get("instrument") == "US10Y.RATE"
        and frame.attrs.get("provider_interface") == "bond_zh_us_rate:EMG00001310"
        and frame.attrs.get("quote_unit") == "percent_points"
        and frame.attrs.get("timezone") == "America/New_York"
    )


def _normalize_yield(raw: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    # Eastmoney's zmgzsyl.js explicitly labels EMG00001310 as percent.
    # The installed AKShare adapter only renames/converts it; do not rescale.
    fields = ["日期", "美国国债收益率10年"]
    if raw is None or any(field not in raw for field in fields):
        raise ValueError("US10Y 响应缺少字段：日期/美国国债收益率10年")
    frame = raw[fields].rename(columns={fields[0]: "date", fields[1]: "close"}).copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="raise")
    frame["close"] = pd.to_numeric(frame["close"], errors="raise")
    # The combined CN/US table includes dates with no US observation.
    # Drop explicitly absent observations without filling or inventing values.
    frame = frame.dropna(subset=["close"]).set_index("date").sort_index().loc[start:end]
    if frame.index.hasnans or frame.index.has_duplicates:
        raise ValueError("US10Y 响应日期无效或重复")
    if not frame["close"].map(lambda value: float("-inf") < value < float("inf")).all():
        raise ValueError("US10Y 收益率存在非有限数值")
    frame.attrs.update(
        instrument="US10Y.RATE", provider_interface="bond_zh_us_rate:EMG00001310",
        quote_unit="percent_points", timezone="America/New_York",
        observation_time="provider_observation_date", provider_published_at="",
    )
    return frame


def is_reference_symbol(symbol: str) -> bool:
    """Whether a symbol has a dedicated, semantically compatible route."""

    return symbol.upper() in _REFERENCE_SYMBOLS


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


def _akshare_reference_call(*args, **kwargs):
    try:
        from py_mini_racer import LibNotFoundError
    except ModuleNotFoundError:
        return akshare_call(*args, **kwargs)
    try:
        return akshare_call(*args, **kwargs)
    except LibNotFoundError as exc:
        raise ImportError("新浪历史解码所需的 JavaScript 原生运行库不可用") from exc


def _akshare_route(symbol: str, start: str) -> tuple[str, Callable[[], pd.DataFrame]] | None:
    try:
        import akshare as ak
    except ModuleNotFoundError:
        return None

    if symbol in _SINA_US:
        return "sina:us-index", lambda: _akshare_reference_call(
            f"index_us_stock_sina({_SINA_US[symbol]})",
            ak.index_us_stock_sina,
            symbol=_SINA_US[symbol],
            lane="akshare:sina-reference",
        )
    if symbol in _SINA_HK:
        return "sina:hk-index", lambda: _akshare_reference_call(
            f"stock_hk_index_daily_sina({_SINA_HK[symbol]})",
            ak.stock_hk_index_daily_sina,
            symbol=_SINA_HK[symbol],
            lane="akshare:sina-reference",
        )
    if symbol in _SINA_GLOBAL:
        return "sina:global-index", lambda: _akshare_reference_call(
            f"index_global_hist_sina({_SINA_GLOBAL[symbol]})",
            ak.index_global_hist_sina,
            symbol=_SINA_GLOBAL[symbol],
            lane="akshare:sina-reference",
        )
    if symbol in _SINA_FUTURES:
        return "sina:foreign-futures", lambda: _akshare_reference_call(
            f"futures_foreign_hist({_SINA_FUTURES[symbol]})",
            ak.futures_foreign_hist,
            symbol=_SINA_FUTURES[symbol],
            lane="akshare:sina-reference",
        )
    if symbol == "US10Y.RATE":
        compact = pd.Timestamp(start).strftime("%Y%m%d")
        return "akshare:us-treasury", lambda: _akshare_reference_call(
            f"bond_zh_us_rate({compact})",
            ak.bond_zh_us_rate,
            start_date=compact,
            lane="akshare:bond-reference",
        )
    if symbol == "DXY.INDEX":
        return "akshare:global-index", lambda: _akshare_reference_call(
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


def _sina_dollar_index() -> pd.DataFrame:
    """Decode the DINIW daily feed used by Sina's own forex chart.

    The official datas/k.js decoder specifies date/open/low/high/close.
    Its empty volume field is absent evidence, never zero turnover.
    """
    import requests

    response = requests.get(
        "https://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/quantmaster/NewForexService.getDayKLine",
        params={"symbol": "DINIW"}, timeout=15,
    )
    response.raise_for_status()
    prefix, suffix = "quantmaster(", ");"
    text = response.text.strip()
    offset = text.find(prefix)
    if offset < 0 or not text.endswith(suffix):
        raise ValueError("DINIW 响应格式无效")
    value = json.loads(text[offset + len(prefix):-len(suffix)])
    if not isinstance(value, str):
        raise ValueError("DINIW 响应类型无效")
    rows = [row.split(",") for row in value.split("|")]
    if any(len(row) < 5 for row in rows):
        raise ValueError("DINIW 日线缺少价格字段")
    return pd.DataFrame([row[:5] for row in rows], columns=["date", "open", "low", "high", "close"])


def _tushare_index(symbol: str, start: str, end: str) -> pd.DataFrame:
    from quantmaster.data.tushare_source import TushareSource

    alias = _TUSHARE_GLOBAL[symbol]
    raw = TushareSource()._call(
        "index_global", 1, provider_lane="tushare:global-index", required_nonempty=True,
        required_columns=("ts_code", "trade_date", "open", "high", "low", "close"),
        ts_code=alias, start_date=pd.Timestamp(start).strftime("%Y%m%d"),
        end_date=pd.Timestamp(end).strftime("%Y%m%d"),
    )
    if set(raw["ts_code"].dropna().astype(str)) != {alias}:
        raise ValueError("国际指数响应代码与请求不符")
    return raw.rename(columns={"trade_date": "date", "vol": "volume"})


def _normalize_reference(raw: pd.DataFrame, symbol: str, source: str, start: str, end: str) -> pd.DataFrame:
    if symbol in INDEX_TIMEZONES:
        frame = _normalize_index(raw, symbol, source, start, end)
        if not covers_requested_range(frame, start, end, symbol=symbol):
            raise ValueError("指数响应边界、频率或价格结构校验失败")
        return frame
    if symbol == "US10Y.RATE":
        return _normalize_yield(raw, start, end)
    return _normalize(raw, start, end)


def fetch_reference(symbol: str, start: str, end: str) -> ReferenceFetch:
    """Fetch one reference without letting another symbol's failure block it."""
    attempts: list[dict[str, str]] = []
    routes: list[tuple[str, Callable[[], pd.DataFrame]]] = []
    akshare_route = _akshare_route(symbol, start)
    if akshare_route is not None:
        routes.append(akshare_route)
    if symbol in _TUSHARE_GLOBAL:
        routes.append(("tushare:global-index", lambda: _tushare_index(symbol, start, end)))
    if symbol == "USD-CNY.FX":
        routes.append(("tushare:fx", lambda: _tushare_cny(start, end)))
    if symbol == "DXY.INDEX":
        routes.append(("sina:dollar-index", lambda: provider_call(
            "sina:dollar-reference", "DINIW:daily", _sina_dollar_index, empty_opens=True,
        )))

    from quantmaster.data.yfinance_source import YFinanceSource

    if symbol != "US10Y.RATE":
        routes.append(("yfinance", lambda: YFinanceSource().daily(symbol, start, end)))
    for source, operation in routes:
        try:
            frame = _normalize_reference(operation(), symbol, source, start, end)
            if frame.empty:
                raise ValueError("返回空数据或缺少有效收盘价")
            return ReferenceFetch(frame=frame, source=source, attempts=tuple(attempts))
        except (AttributeError, ImportError, KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
            from quantmaster.logging_config import redact_sensitive_text

            attempts.append({
                "source": source,
                "code": type(exc).__name__,
                "detail": (redact_sensitive_text(exc).strip() or "请求失败")[:180],
            })
    raise ReferenceMarketUnavailable(symbol, attempts)


def _needs_refresh(meta: dict | None, start: str, end: str, refresh: str) -> bool:
    if not meta:
        return True
    coverage_start = str(meta.get("coverage_start") or meta.get("start") or "")
    coverage_end = str(meta.get("coverage_end") or meta.get("end") or "")
    if not coverage_start or coverage_start > start or not coverage_end or coverage_end < end:
        return True
    if refresh == "incremental":
        return True
    checked_at = float(meta.get("checked_at") or 0)
    return time.time() - checked_at >= get_config().data.cache_days * 86400


def refresh_reference_panel(
    symbols: list[str],
    start: str,
    end: str,
    refresh: str,
    store: BarStore,
) -> tuple[dict[str, pd.DataFrame], dict[str, dict]]:
    """Refresh global reference symbols through one validated cache interface."""
    from quantmaster.data.resilience import data_priority

    plans: dict[str, str] = {}
    for symbol in symbols:
        if symbol == "US10Y.RATE":
            plans[symbol] = start
            continue
        meta = store.metadata(symbol)
        if not _needs_refresh(meta, start, end, refresh):
            continue
        cached = store.get(symbol)
        if cached is None or cached.empty:
            plans[symbol] = start
        elif str((meta or {}).get("coverage_start") or (meta or {}).get("start") or "") > start:
            plans[symbol] = start
        else:
            plans[symbol] = str(cached.index[max(0, len(cached) - 5)].date())

    failures: dict[str, dict] = {}

    def sync_one(symbol: str, fetch_start: str) -> None:
        try:
            if symbol == "US10Y.RATE" or symbol in INDEX_TIMEZONES:
                from quantmaster.data.market_data_access import refresh_history

                envelope = refresh_history(symbol, start, end, store=store, mode=refresh)
                envelope.require_data()
                if envelope.quality.stale:
                    raise ValueError("刷新失败，正在显示旧参考数据")
                return
            with data_priority("interactive"):
                fetched = fetch_reference(symbol, fetch_start, end)
            frame = fetched.frame
            if (
                frame is None
                or frame.empty
                or not covers_requested_range(frame, fetch_start, end, symbol=symbol)
            ):
                raise ValueError("响应缺失有效交易日或内部过于稀疏")
            with store.lock(symbol):
                cached = store.get(symbol)
                merged = frame if cached is None or cached.empty else pd.concat([cached, frame])
                merged = merged[~merged.index.duplicated(keep="last")].sort_index()
                quality = assess_daily_frame(
                    merged.loc[start:end], start, end, symbol=symbol, source=fetched.source,
                )
                store.put(
                    symbol,
                    merged,
                    replace=True,
                    request_start=fetch_start,
                    request_end=end,
                    source=fetched.source,
                    quality=quality.to_dict(),
                )
        except ReferenceMarketUnavailable as exc:
            failures[symbol] = {
                "error_code": "all_sources_unavailable",
                "message": str(exc)[:500],
                "source_attempts": list(exc.attempts),
            }
            previous_source = str((store.metadata(symbol) or {}).get("last_source") or "")
            store.mark_status(symbol, "stale", source=previous_source)
        except (
            AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error,
        ) as exc:
            failures[symbol] = {
                "error_code": type(exc).__name__,
                "message": (str(exc).strip() or "同步失败")[:500],
                "source_attempts": [],
            }
            previous_source = str((store.metadata(symbol) or {}).get("last_source") or "")
            store.mark_status(symbol, "stale", source=previous_source)

    with ThreadPoolExecutor(
        max_workers=min(6, max(1, len(plans))),
        thread_name_prefix="reference-market",
    ) as executor:
        pending = [
            executor.submit(sync_one, symbol, fetch_start)
            for symbol, fetch_start in plans.items()
        ]
        for future in as_completed(pending):
            future.result()

    result: dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        cached = store.get(symbol)
        if cached is not None and not cached.empty:
            sliced = cached.loc[start:end]
            if not sliced.empty:
                result[symbol] = sliced
    return result, failures
