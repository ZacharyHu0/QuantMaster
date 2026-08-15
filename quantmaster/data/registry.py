"""数据源注册与统一入口：按市场路由，带本地缓存与自动降级。

用法：
    from quantmaster.data import refresh_history, refresh_panel
    df = refresh_history("600519.SH", "2022-01-01", "2024-12-31").require_data()
    panel = refresh_panel(["600519.SH", "000858.SZ"], "2022-01-01", "2024-12-31").require_data()
"""

from __future__ import annotations

import json
import logging
import math
import re
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import UTC, date, datetime, time
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.base import (
    OHLCV_COLUMNS,
    BarDataEnvelope,
    BarDataQuality,
    DataCapability,
    DataSource,
    Market,
    MarketDataUnavailable,
    validate_frequency,
)
from quantmaster.data.cache_freshness import (
    BarRefreshBatchStore,
    CachePurpose,
    assess_daily_freshness,
)
from quantmaster.data.frame_quality_access import register_frame_quality
from quantmaster.data.resilience import (
    bypass_endpoint_cache,
    data_priority,
    local_only_data_access,
    remote_io_allowed,
)
from quantmaster.data.semantics import NumericSemantics, PriceType
from quantmaster.data.storage import BarStore, IntradayBarStore
from quantmaster.market_data_access import register_history_refresh
from quantmaster.market_identity import guess_market
from quantmaster.runtime.sqlite import connect_sqlite
from quantmaster.trading_sessions import SessionExpectation, market_date, market_now

logger = logging.getLogger(__name__)

QualityStatus = Literal["verified", "degraded", "unavailable"]
_QUALITY_RANK: dict[QualityStatus, int] = {
    "verified": 0,
    "degraded": 1,
    "unavailable": 2,
}
_MINUTE_FREQUENCY_MINUTES = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "60m": 60,
}

_MARKET_TIMEZONES: dict[Market, str] = {
    Market.CN: "Asia/Shanghai",
    Market.HK: "Asia/Hong_Kong",
    Market.US: "America/New_York",
}


def _market_timezone(symbol: str) -> ZoneInfo | None:
    """Return only an IANA timezone proved by the symbol's market identity."""
    try:
        name = _MARKET_TIMEZONES.get(guess_market(symbol))
    except ValueError:
        return None
    if not name:
        return None
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:  # pragma: no cover - deployment data defect
        return None


def _market_wall_time(value: object, zone: ZoneInfo) -> pd.Timestamp:
    """Interpret request boundaries in an explicit exchange timezone."""
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        return stamp
    return stamp.tz_convert(zone).tz_localize(None)


def _market_sessions(
    market: Market,
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    observed_dates: pd.DatetimeIndex | None = None,
) -> tuple[pd.DatetimeIndex, str, bool]:
    """Resolve session dates without inventing holidays from weekdays.

    CN can use the locally published calendar.  HK/US currently have no local
    holiday feed, so their own dated bars are usable as observed-session evidence
    for clock/bucket validation, but never certify that the requested range is
    complete.
    """
    if market == Market.CN:
        sessions, source = _local_sessions(start, end)
        return sessions, source, bool(len(sessions))
    if market in {Market.HK, Market.US} and observed_dates is not None:
        selected = observed_dates[(observed_dates >= start) & (observed_dates <= end)]
        return (
            selected.unique().sort_values(),
            f"{market.value}:observed-session-dates",
            False,
        )
    return pd.DatetimeIndex([]), f"{market.value}:unsupported-calendar", False


def _trading_windows(market: Market, day: pd.Timestamp) -> tuple[tuple[pd.Timestamp, pd.Timestamp], ...]:
    """Exchange-local regular/CAS trading windows used for bar coverage."""
    base = day.normalize()
    if market == Market.CN:
        return (
            (base + pd.Timedelta(hours=9, minutes=30), base + pd.Timedelta(hours=11, minutes=30)),
            (base + pd.Timedelta(hours=13), base + pd.Timedelta(hours=15)),
        )
    if market == Market.HK:
        return (
            (base + pd.Timedelta(hours=9, minutes=30), base + pd.Timedelta(hours=12)),
            (base + pd.Timedelta(hours=13), base + pd.Timedelta(hours=16)),
        )
    if market == Market.US:
        return ((base + pd.Timedelta(hours=9, minutes=30), base + pd.Timedelta(hours=16)),)
    return ()


def _daily_close(market: Market, day: date, zone: ZoneInfo) -> datetime | None:
    close = {
        Market.CN: time(15),
        Market.HK: time(16, 10),
        Market.US: time(16),
    }.get(market)
    return datetime.combine(day, close, zone) if close is not None else None


def _exact_attr_instant(frame: pd.DataFrame, name: str) -> datetime | None:
    """Read an exact provider instant; naive evidence is deliberately unusable."""
    raw = frame.attrs.get(name)
    if raw in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)

# 各市场按优先级排列的数据源工厂
_SOURCE_FACTORIES: dict[str, list] = {}
_DEFAULT_STORE_LOCK = threading.Lock()
_DEFAULT_BAR_STORES: dict[tuple[str, type[BarStore], bool], BarStore] = {}


def _default_bar_store() -> BarStore:
    """Reuse the default daily store without affecting explicitly supplied stores."""
    root = (Path(get_config().data_root) / "bars").resolve()
    read_only = not remote_io_allowed()
    # Never reuse a writable store object for a page read.  Apart from making
    # the boundary enforceable, this avoids a hidden schema migration or
    # integrity backfill after a cache miss.
    key = (str(root), BarStore, read_only)
    with _DEFAULT_STORE_LOCK:
        store = _DEFAULT_BAR_STORES.get(key)
        if store is None:
            store = BarStore(root=root, read_only=read_only)
            _DEFAULT_BAR_STORES[key] = store
        return store


def _default_read_bar_store() -> BarStore:
    """Return a strict local-only daily store even outside an HTTP context."""

    root = (Path(get_config().data_root) / "bars").resolve()
    key = (str(root), BarStore, True)
    with _DEFAULT_STORE_LOCK:
        store = _DEFAULT_BAR_STORES.get(key)
        if store is None:
            store = BarStore(root=root, read_only=True)
            _DEFAULT_BAR_STORES[key] = store
        return store


def _local_read_store(
    store: BarStore | IntradayBarStore,
    frequency: str,
) -> BarStore | IntradayBarStore:
    """Return a non-mutating view when a caller supplied a writable store.

    Tests and worker code often retain one ``BarStore`` instance and pass it
    through several layers.  The request boundary must still win over that
    convenience object: a local page read gets a new SQLite ``mode=ro`` view
    of the same files.
    """

    if getattr(store, "read_only", False):
        return store
    if frequency == "1d":
        return BarStore(root=store.root, read_only=True)
    return IntradayBarStore(
        frequency,
        root=store.root.parent,
        read_only=True,
    )


class RefreshMode(StrEnum):
    AUTO = "auto"
    INCREMENTAL = "incremental"
    FULL = "full"


class AdjustmentMismatch(RuntimeError):
    pass


def _instrument_range(
    symbol: str, start: pd.Timestamp, end: pd.Timestamp,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Narrow a request only with explicit listing evidence, never weekday guesses."""
    if not symbol:
        return start, end
    try:
        from quantmaster.data.instruments import InstrumentStore

        # The same evidence assessor runs in Web reads and worker refreshes.
        # A page read may consult the security master but may not bootstrap or
        # migrate it merely to discover a listing date.
        instrument = InstrumentStore(read_only=not remote_io_allowed()).get(symbol)
    except (ImportError, OSError, RuntimeError, TypeError, ValueError):
        instrument = None
    if instrument is None:
        return start, end
    try:
        listed = pd.Timestamp(instrument.list_date).normalize() if instrument.list_date else None
    except (TypeError, ValueError):
        listed = None
    try:
        delisted = pd.Timestamp(instrument.delist_date).normalize() if instrument.delist_date else None
    except (TypeError, ValueError):
        delisted = None
    if listed is not None:
        start = max(start, listed)
    if delisted is not None:
        end = min(end, delisted)
    return start, end


def _unit_contract(symbol: str) -> tuple[tuple[tuple[str, str], ...], str]:
    """Resolve units from local instrument evidence; never infer asset type."""
    unknown = tuple((field, "unknown") for field in (*OHLCV_COLUMNS[:-1], "volume", "amount"))
    if not symbol:
        return unknown, "缺少标的，无法确认行情单位"
    try:
        from quantmaster.data.instruments import InstrumentStore

        instrument = InstrumentStore(read_only=not remote_io_allowed()).get(symbol)
    except (ImportError, OSError, RuntimeError, TypeError, ValueError):
        instrument = None
    if instrument is None:
        return unknown, f"{symbol} 缺少证券主数据，单位未知"
    asset_type = str(instrument.asset_type or "").lower()
    currency = str(instrument.currency or "").upper()
    if asset_type == "index":
        return (
            (*((field, "point") for field in OHLCV_COLUMNS[:-1]),
             ("volume", "unknown"), ("amount", "unknown")),
            "指数成交量/成交额单位缺少版本化证明",
        )
    if asset_type in {"stock", "etf", "fund"} and currency:
        return (
            (*((field, f"{currency}/share") for field in OHLCV_COLUMNS[:-1]),
             ("volume", "share"), ("amount", currency)),
            "",
        )
    return unknown, f"{symbol} 的 {asset_type or 'unknown'} 品种缺少可验证单位契约"


def _numeric_semantics(
    symbol: str, source: str, frame: pd.DataFrame, units: tuple[tuple[str, str], ...],
) -> tuple[NumericSemantics, tuple[str, ...]]:
    """Build a provider-boundary contract without guessing absent dimensions."""
    unit_map = dict(units)
    adjustment = str(frame.attrs.get("adjustment") or "raw").lower()
    price_type = {
        "none": PriceType.RAW, "raw": PriceType.RAW,
        "qfq": PriceType.FORWARD_ADJUSTED, "hfq": PriceType.BACKWARD_ADJUSTED,
    }.get(adjustment, PriceType.RAW)
    issues: list[str] = []
    factor_coverage = "not_applicable"
    provider_definition = ""
    company_actions = ""
    anchor = ""
    if price_type != PriceType.RAW:
        factor_coverage = str(frame.attrs.get("factor_coverage") or "unconfirmed")
        provider_definition = str(frame.attrs.get("adjustment_provider_definition") or "")
        company_actions = str(frame.attrs.get("adjustment_company_actions") or "")
        anchor = str(frame.attrs.get("adjustment_anchor_date") or "")
        if factor_coverage != "complete" or not provider_definition or not company_actions:
            issues.append(
                "factor_contract_incomplete: 缺少完整因子链、provider 定义或公司行为范围"
            )
    currency = str(unit_map.get("amount") or "")
    if currency == "unknown" or "/" in currency:
        currency = ""
    continuous = guess_market(symbol) == Market.FUTURES and symbol.partition(".")[0].endswith("0")
    if continuous:
        price_type = PriceType.CONTINUOUS_FUTURES
        issues.append(
            "continuous_contract_unconfirmed: 连续序列缺少具体合约、roll、乘数与 tick"
        )
    semantics = NumericSemantics(
        instrument=symbol,
        observation_time="exchange_session" if frame.index.name == "date" else "",
        price_type=price_type,
        currency=currency,
        price_unit=str(unit_map.get("close") or "unknown"),
        volume_unit=str(unit_map.get("volume") or "unknown"),
        amount_unit=str(unit_map.get("amount") or "unknown"),
        provider=source or "unknown",
        provider_interface=str(frame.attrs.get("provider_interface") or source or "unknown"),
        adjustment_anchor_date=anchor,
        adjustment_provider_definition=provider_definition,
        adjustment_company_actions=company_actions,
        factor_coverage=factor_coverage,
        roll_method=str(frame.attrs.get("roll_method") or ""),
        intended_use="display",
    )
    return semantics, tuple(issues)


def _local_sessions(start: pd.Timestamp, end: pd.Timestamp) -> tuple[pd.DatetimeIndex, str]:
    """Return only locally published trading-session evidence."""
    try:
        from quantmaster.data.free_stockdb_ingest import StockDBIngestStore

        for snapshot in StockDBIngestStore().history(limit=20):
            if not str(snapshot.session_source).startswith("tushare:"):
                continue
            values = pd.DatetimeIndex(
                pd.to_datetime(snapshot.session_dates, errors="coerce")
            ).dropna().normalize()
            selected = values[(values >= start) & (values <= end)]
            tolerance = pd.Timedelta(days=14)
            if (
                len(selected)
                and selected.min() - start <= tolerance
                and end - selected.max() <= tolerance
            ):
                source = snapshot.session_source or "published_sessions"
                return selected.unique().sort_values(), f"stockdb-ingest:{source}"
    except (ImportError, OSError, RuntimeError, TypeError, ValueError):
        logger.debug("本地 StockDB 交易日证据不可用", exc_info=True)
    try:
        path = get_config().data_root / "research_lake" / "_meta" / "catalog.sqlite"
        if path.is_file():
            with connect_sqlite(path, read_only=not remote_io_allowed()) as connection:
                values = [
                    str(row[0]) for row in connection.execute(
                        "SELECT DISTINCT trade_date FROM research_partitions "
                        "WHERE kind=? AND asset_class=? AND frequency=? "
                        "AND trade_date>=? AND trade_date<=? ORDER BY trade_date",
                        (
                            "raw", "stock", "1d",
                            start.date().isoformat(), end.date().isoformat(),
                        ),
                    ).fetchall()
                ]
            sessions = pd.DatetimeIndex(pd.to_datetime(values, errors="coerce")).dropna().normalize()
            if len(sessions):
                return sessions.unique().sort_values(), "research_lake"
    except (ImportError, OSError, RuntimeError, TypeError, ValueError):
        logger.debug("本地交易日证据不可用", exc_info=True)
    return pd.DatetimeIndex([]), "unavailable"


def _assess_daily_frame(
    df: pd.DataFrame | None,
    start: str,
    end: str,
    *,
    symbol: str = "",
    source: str = "",
    stale: bool = False,
    extra_issues: tuple[str, ...] = (),
) -> BarDataQuality:
    try:
        market = guess_market(symbol) if symbol else Market.CN
    except ValueError:
        market = Market.CN
    zone = _market_timezone(symbol) if symbol else ZoneInfo("Asia/Shanghai")
    current = market_now()
    market_today = current.astimezone(zone).date() if zone is not None else market_date()
    requested_start = pd.Timestamp(start).normalize()
    requested_end = min(pd.Timestamp(end).normalize(), pd.Timestamp(market_today))
    effective_start, effective_end = _instrument_range(
        symbol, requested_start, requested_end,
    )
    issues = list(extra_issues)
    units, unit_issue = _unit_contract(symbol)
    if unit_issue:
        issues.append(unit_issue)
    if df is None or df.empty:
        issues.append("行情为空")
        return BarDataQuality(
            "unavailable", start, end, sources=(source,) if source else (),
            issues=tuple(issues), stale=stale, partial=True,
            timezone=str(zone) if zone is not None else "unknown", adjustment="qfq",
            units=units,
            requested_symbols=(symbol,) if symbol else (),
            missing_symbols=(symbol,) if symbol else (),
        )
    index = pd.DatetimeIndex(pd.to_datetime(df.index, errors="coerce"))
    if index.tz is not None:
        index = index.tz_localize(None)
    index = index.normalize()
    valid_index = index[~index.isna()]
    future_rows = int((valid_index > requested_end).sum())
    if future_rows:
        issues.append(f"存在 {future_rows} 行晚于 as_of {requested_end.date()} 的未来数据")
    observed_start = valid_index.min() if len(valid_index) else pd.NaT
    observed_end = valid_index.max() if len(valid_index) else pd.NaT
    duplicate_rows = int(index.duplicated(keep=False).sum())
    if duplicate_rows:
        issues.append(f"存在 {duplicate_rows} 行重复交易日")
    sparse_cadence = False
    unique_index = valid_index.unique().sort_values()
    if len(unique_index) >= 10:
        median_gap_days = float(pd.Series(unique_index).diff().dropna().dt.days.median())
        sparse_cadence = median_gap_days > 2
        if sparse_cadence:
            issues.append(f"日频观测间隔中位数异常：{median_gap_days:.1f} 天")
    missing_columns = [column for column in OHLCV_COLUMNS if column not in df]
    if missing_columns:
        issues.append("缺少必需列：" + "、".join(missing_columns))
    invalid_numeric = 0
    invalid_semantics = 0
    if not missing_columns:
        numeric = df[OHLCV_COLUMNS].apply(pd.to_numeric, errors="coerce")
        finite = numeric.map(math.isfinite).all(axis=1)
        invalid_numeric = int((~finite).sum())
        if invalid_numeric:
            issues.append(
                f"存在 {invalid_numeric} 行无法识别的开盘、最高、最低、收盘或成交量"
            )
        prices = numeric[["open", "high", "low", "close"]]
        scale = prices.abs().max(axis=1).clip(lower=1.0)
        tolerance = scale.mul(1e-8)
        future_family = symbol.upper().split(".", 1)[0].rstrip("0123456789")
        energy_future = (
            guess_market(symbol) == Market.FUTURES
            and future_family in {"CL", "SC", "WTI", "BRENT"}
        )
        valid_price_domain = prices.notna().all(axis=1) if energy_future else prices.gt(0).all(axis=1)
        semantic = (
            valid_price_domain
            & numeric["high"].add(tolerance).ge(prices[["open", "close"]].max(axis=1))
            & numeric["low"].sub(tolerance).le(prices[["open", "close"]].min(axis=1))
            & numeric["high"].add(tolerance).ge(numeric["low"])
            & numeric["volume"].ge(0)
        )
        invalid_semantics = int((finite & ~semantic).sum())
        if invalid_semantics:
            issues.append(f"存在 {invalid_semantics} 行价格高低关系或成交量不合理")
    if source.startswith("free-stockdb") and df.attrs.get("unit_status") not in {
        "verified", "verified_local_stockdb_schema_v1",
    }:
        issues.append("本地 StockDB 未附带可核验的单位说明，当前按每股价格和人民币金额使用")
    semantics, semantic_issues = _numeric_semantics(symbol, source, df, units)
    issues.extend(semantic_issues)
    adjustment = semantics.price_type.value
    if source.startswith("free-stockdb") and df.attrs.get("adjustment_status") != "verified":
        adjustment = "forward_adjusted_unverified"
        issues.append("本地 StockDB 返回了前复权行情，但没有附带可核验的复权因子记录")
    boundary_tolerance = pd.Timedelta(days=14)
    if pd.isna(observed_start) or pd.isna(observed_end):
        issues.append("没有有效交易日期")
    else:
        if observed_start - effective_start > boundary_tolerance:
            issues.append(
                f"响应起点 {observed_start.date()} 晚于有证据的请求起点 {effective_start.date()}"
            )
        if effective_end - observed_end > boundary_tolerance:
            issues.append(
                f"响应终点 {observed_end.date()} 早于有证据的请求终点 {effective_end.date()}"
            )
    sessions, calendar_source, calendar_complete = _market_sessions(
        market, effective_start, effective_end, observed_dates=valid_index.normalize(),
    )
    coverage_ratio: float | None = None
    partial = False
    if len(sessions):
        expected = sessions[(sessions >= effective_start) & (sessions <= effective_end)]
        if len(expected):
            coverage_ratio = len(expected.intersection(valid_index.unique())) / len(expected)
            partial = coverage_ratio < 1.0
            if coverage_ratio < 0.95:
                issues.append(f"有证据交易日覆盖率仅 {coverage_ratio:.1%}")
    elif market in {Market.CN, Market.HK, Market.US}:
        partial = True
        issues.append("缺少权威交易日历，仅完成边界与结构校验")
    else:
        partial = True
        issues.append("MARKET_SESSION_UNSUPPORTED: 缺少该交易所/产品的已验证时区与交易时段模板")
    if not calendar_complete and market in {Market.HK, Market.US}:
        partial = True
        issues.append("交易日仅由已返回 bar 观测，缺少独立节假日日历，不能证明区间完整")
    if zone is not None and not pd.isna(observed_end) and observed_end.date() == market_today:
        close = _daily_close(market, market_today, zone)
        now_utc = current.astimezone(UTC)
        published = _exact_attr_instant(df, "provider_published_at")
        ingested = _exact_attr_instant(df, "ingested_at")
        coverage_complete = bool(df.attrs.get("coverage_complete"))
        if close is not None and now_utc < close.astimezone(UTC):
            partial = True
            issues.append("CURRENT_SESSION_PARTIAL: 当日 bar 尚处于交易时段，不能用于正式研究")
        elif close is not None and (published is None or published > now_utc):
            partial = True
            issues.append("CURRENT_SESSION_CLOSED_WAITING_PROVIDER: 收盘后仍缺少 provider 发布时间")
        elif close is not None and (
            ingested is None or ingested > now_utc or not coverage_complete
        ):
            partial = True
            issues.append(
                "CURRENT_SESSION_PROVIDER_PUBLISHED_WAITING_INGEST: "
                "等待 cutoff 前的本地摄取与完整覆盖证据"
            )
    blocking = bool(
        duplicate_rows or future_rows or sparse_cadence or missing_columns
        or invalid_numeric or invalid_semantics
        or any("响应起点" in item or "响应终点" in item or "覆盖率仅" in item for item in issues)
    )
    status: QualityStatus
    if blocking:
        status = "unavailable"
    elif stale or partial or issues:
        status = "degraded"
    else:
        status = "verified"
    return BarDataQuality(
        status,
        start,
        end,
        observed_start="" if pd.isna(observed_start) else observed_start.date().isoformat(),
        observed_end="" if pd.isna(observed_end) else observed_end.date().isoformat(),
        coverage_ratio=round(coverage_ratio, 6) if coverage_ratio is not None else None,
        calendar_source=calendar_source,
        sources=(source,) if source else (),
        issues=tuple(dict.fromkeys(issues)),
        stale=stale,
        partial=partial,
        timezone=str(zone) if zone is not None else "unknown",
        adjustment=adjustment,
        units=units,
        duplicate_rows=duplicate_rows,
        future_rows=future_rows,
        semantics=semantics,
        semantic_diagnostic_code=(semantic_issues[0].split(":", 1)[0] if semantic_issues else ""),
        missing_reason_counts=(
            (("not_published", 1),) if df is None or df.empty else ()
        ),
        anomaly_counts=tuple((key, value) for key, value in (
            ("duplicate_time", duplicate_rows),
            ("future_time", future_rows),
            ("nonfinite_ohlcv", invalid_numeric),
            ("ohlcv_conflict", invalid_semantics),
        ) if value),
        requested_symbols=(symbol,) if symbol else (),
        observed_symbols=(symbol,) if symbol else (),
    )


def _covers_requested_range(
    df: pd.DataFrame, start: str, end: str, *, symbol: str = "",
) -> bool:
    """Compatibility for internal validators: true only without blocking issues."""
    return _assess_daily_frame(df, start, end, symbol=symbol).status != "unavailable"


def _is_complete_refresh(
    fresh: pd.DataFrame,
    cached: pd.DataFrame | None,
    start: str,
    end: str,
    *,
    symbol: str = "",
) -> bool:
    """只有确认新响应完整时，才允许覆盖前复权日线缓存。

    AKShare 偶尔会返回有数据但缺头、缺尾或缺中间分块的响应。边界覆盖之外，
    还要确保旧缓存中已经确认存在的交易日没有在新响应里消失。
    """
    if not _covers_requested_range(fresh, start, end, symbol=symbol):
        return False
    if cached is None or cached.empty:
        return True
    fresh_index = pd.DatetimeIndex(fresh.index).normalize()
    cached_index = pd.DatetimeIndex(cached.index).normalize()
    known = cached_index[
        (cached_index >= pd.Timestamp(start).normalize()) & (cached_index <= pd.Timestamp(end).normalize())
    ]
    return known.difference(fresh_index).empty


def _mode(use_cache: bool, refresh: RefreshMode | str | None) -> RefreshMode:
    if refresh is None:
        return RefreshMode.AUTO if use_cache else RefreshMode.FULL
    return refresh if isinstance(refresh, RefreshMode) else RefreshMode(refresh)


def _unavailable_error(
    symbols: tuple[str, ...],
    start: str,
    end: str,
    issues: list[str] | tuple[str, ...],
    *,
    frequency: str = "1d",
) -> MarketDataUnavailable:
    clean_issues = tuple(dict.fromkeys(str(value) for value in issues if str(value)))
    sources = tuple(dict.fromkeys(
        value.split(":", 1)[0]
        for value in clean_issues
        if ":" in value and value.split(":", 1)[0]
    ))
    quality = BarDataQuality(
        "unavailable",
        start,
        end,
        sources=sources,
        issues=clean_issues or ("所有候选行情源均未返回可验证数据",),
        stale=False,
        partial=True,
        timezone="exchange-date" if frequency == "1d" else "Asia/Shanghai",
        adjustment="qfq" if frequency == "1d" else "none",
        requested_symbols=symbols,
        observed_symbols=(),
        missing_symbols=symbols,
    )
    provenance = tuple(
        {"attempt": value, "status": "failed"}
        for value in clean_issues
    )
    return MarketDataUnavailable(quality, provenance)


def _cached_slice(cached: pd.DataFrame | None, start: str, end: str) -> pd.DataFrame | None:
    if cached is None or cached.empty:
        return None
    result = cached.loc[start:end]
    return result if not result.empty else None


def _shanghai_wall_time(value: object) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert("Asia/Shanghai").tz_localize(None)
    return stamp


def _intraday_index_assessment(
    frame: pd.DataFrame,
    zone: ZoneInfo | None,
) -> tuple[pd.DatetimeIndex, str, bool, int, list[str]]:
    raw_index = pd.DatetimeIndex(pd.to_datetime(frame.index, errors="coerce"))
    timezone = str(zone) if zone is not None else "unknown"
    issues: list[str] = []
    time_unzoned = False
    if raw_index.tz is None:
        provider_timezone = str(frame.attrs.get("timezone") or "").strip()
        try:
            provider_zone = ZoneInfo(provider_timezone) if provider_timezone else None
        except ZoneInfoNotFoundError:
            provider_zone = None
        if provider_zone is None or zone is None:
            issues.append("TIME_UNZONED: 分钟行情没有可解释的 IANA provider 时区")
            time_unzoned = True
            wall_index = raw_index
        else:
            wall_index = raw_index.tz_localize(provider_zone).tz_convert(zone).tz_localize(None)
    else:
        wall_index = (
            raw_index.tz_convert(zone).tz_localize(None)
            if zone is not None else raw_index.tz_convert("UTC").tz_localize(None)
        )
    valid_index = wall_index[~wall_index.isna()]
    duplicate_rows = int(valid_index.duplicated(keep=False).sum())
    if duplicate_rows:
        issues.append(f"存在 {duplicate_rows} 行重复分钟时间戳")
    return valid_index, timezone, time_unzoned, duplicate_rows, issues


def _intraday_row_assessment(
    frame: pd.DataFrame,
) -> tuple[list[str], list[str], int, int]:
    issues: list[str] = []
    missing_columns = [column for column in OHLCV_COLUMNS if column not in frame]
    if missing_columns:
        issues.append("缺少必需列：" + "、".join(missing_columns))
        return issues, missing_columns, 0, 0
    numeric = frame[OHLCV_COLUMNS].apply(pd.to_numeric, errors="coerce")
    finite = numeric.map(math.isfinite).all(axis=1)
    invalid_numeric = int((~finite).sum())
    if invalid_numeric:
        issues.append(
            f"存在 {invalid_numeric} 行无法识别的开盘、最高、最低、收盘或成交量"
        )
    prices = numeric[["open", "high", "low", "close"]]
    semantic = (
        prices.gt(0).all(axis=1)
        & numeric["high"].ge(prices[["open", "close"]].max(axis=1))
        & numeric["low"].le(prices[["open", "close"]].min(axis=1))
        & numeric["high"].ge(numeric["low"])
        & numeric["volume"].ge(0)
    )
    invalid_semantics = int((finite & ~semantic).sum())
    if invalid_semantics:
        issues.append(f"存在 {invalid_semantics} 行价格高低关系或成交量不合理")
    return issues, missing_columns, invalid_numeric, invalid_semantics


def _intraday_requested_range(
    start: str,
    end: str,
    *,
    symbol: str,
    frequency: str,
    request_zone: ZoneInfo,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    requested_start = _market_wall_time(start, request_zone)
    requested_end = _market_wall_time(end, request_zone)
    if frequency == "1d":
        current_date = market_now().astimezone(request_zone).date()
        requested_end = min(requested_end.normalize(), pd.Timestamp(current_date))
        requested_start, requested_end = _instrument_range(
            symbol, requested_start.normalize(), requested_end,
        )
    if len(str(end).strip()) <= 10:
        requested_end += pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
    return requested_start, min(
        requested_end, _market_wall_time(market_now(), request_zone),
    )


def _intraday_boundary_assessment(
    valid_index: pd.DatetimeIndex,
    requested_start: pd.Timestamp,
    requested_end: pd.Timestamp,
) -> tuple[Any, Any, bool, list[str]]:
    observed_start = valid_index.min() if len(valid_index) else pd.NaT
    observed_end = valid_index.max() if len(valid_index) else pd.NaT
    if pd.isna(observed_start) or pd.isna(observed_end):
        return observed_start, observed_end, True, ["分钟行情没有有效时间戳"]
    issues: list[str] = []
    span = requested_end - requested_start
    start_tolerance = pd.Timedelta(days=4 if span >= pd.Timedelta(days=4) else 1)
    if observed_start - requested_start > start_tolerance:
        issues.append(
            f"响应起点 {observed_start.isoformat()} 严重晚于请求起点 {requested_start.isoformat()}"
        )
    if requested_end - observed_end > pd.Timedelta(days=4):
        issues.append(
            f"响应终点 {observed_end.isoformat()} 严重早于请求终点 {requested_end.isoformat()}"
        )
    return observed_start, observed_end, bool(issues), issues


def _add_intraday_window_evidence(
    unique_index: pd.DatetimeIndex,
    expected_buckets: set[pd.Timestamp],
    observed_buckets: set[pd.Timestamp],
    *,
    session_start: pd.Timestamp,
    session_end: pd.Timestamp,
    requested_start: pd.Timestamp,
    requested_end: pd.Timestamp,
    frequency_minutes: int,
) -> int:
    window_start = max(session_start, requested_start)
    window_end = min(session_end, requested_end)
    if window_end <= window_start:
        return 0
    expected_grid = pd.date_range(
        session_start + pd.Timedelta(minutes=frequency_minutes),
        session_end,
        freq=f"{frequency_minutes}min",
    )
    expected_buckets.update(
        pd.Timestamp(value)
        for value in expected_grid[
            (expected_grid > window_start) & (expected_grid <= window_end)
        ]
    )
    window_values = unique_index[
        (unique_index >= session_start) & (unique_index <= session_end)
    ]
    start_labeled = session_start in window_values and session_end not in window_values
    off_grid_rows = 0
    for stamp in window_values:
        offset_minutes = (stamp - session_start).total_seconds() / 60
        aligned = (
            stamp.second == 0
            and stamp.microsecond == 0
            and offset_minutes % frequency_minutes == 0
        )
        if not aligned:
            off_grid_rows += 1
        if start_labeled:
            bucket = stamp + pd.Timedelta(minutes=frequency_minutes)
        else:
            steps = max(1, math.ceil(offset_minutes / frequency_minutes))
            bucket = session_start + pd.Timedelta(
                minutes=steps * frequency_minutes,
            )
        if bucket in expected_buckets:
            observed_buckets.add(bucket)
    return off_grid_rows


def _intraday_bucket_assessment(
    market: Market,
    sessions: pd.DatetimeIndex,
    valid_index: pd.DatetimeIndex,
    *,
    requested_start: pd.Timestamp,
    requested_end: pd.Timestamp,
    frequency_minutes: int,
) -> tuple[float | None, int, int, int]:
    unique_index = pd.DatetimeIndex(valid_index.unique()).sort_values()
    expected_buckets: set[pd.Timestamp] = set()
    observed_buckets: set[pd.Timestamp] = set()
    off_grid_rows = 0
    for session in sessions:
        day = pd.Timestamp(session).normalize()
        for session_start, session_end in _trading_windows(market, day):
            off_grid_rows += _add_intraday_window_evidence(
                unique_index,
                expected_buckets,
                observed_buckets,
                session_start=session_start,
                session_end=session_end,
                requested_start=requested_start,
                requested_end=requested_end,
                frequency_minutes=frequency_minutes,
            )
    coverage = (
        min(1.0, len(observed_buckets) / len(expected_buckets))
        if expected_buckets
        else None
    )
    return coverage, len(observed_buckets), len(expected_buckets), off_grid_rows


def _intraday_coverage_assessment(
    market: Market,
    sessions: pd.DatetimeIndex,
    calendar_complete: bool,
    valid_index: pd.DatetimeIndex,
    *,
    requested_start: pd.Timestamp,
    requested_end: pd.Timestamp,
    frequency: str,
    frequency_minutes: int,
) -> tuple[float | None, bool, list[str]]:
    if not len(sessions) or not len(valid_index):
        return None, True, ["缺少独立交易日历，无法验证分钟历史覆盖"]
    observed_dates = pd.DatetimeIndex(valid_index.normalize().unique())
    coverage_ratio = len(sessions.intersection(observed_dates)) / len(sessions)
    partial = coverage_ratio < 1.0
    issues = (
        [f"有证据交易日覆盖率仅 {coverage_ratio:.1%}"]
        if partial
        else []
    )
    if frequency_minutes > 0:
        bucket_coverage, observed, expected, off_grid_rows = (
            _intraday_bucket_assessment(
                market,
                sessions,
                valid_index,
                requested_start=requested_start,
                requested_end=requested_end,
                frequency_minutes=frequency_minutes,
            )
        )
        if bucket_coverage is not None:
            coverage_ratio = min(coverage_ratio, bucket_coverage)
            if bucket_coverage < 1.0:
                partial = True
                issues.append(
                    f"{frequency} 交易时段桶覆盖率仅 {bucket_coverage:.1%}"
                    f"（{observed}/{expected}）"
                )
        if off_grid_rows:
            issues.append(f"存在 {off_grid_rows} 行未对齐 {frequency} 桶边界的时间戳")
    if not calendar_complete and market in {Market.HK, Market.US}:
        partial = True
        issues.append("交易日仅由已返回 bar 观测，缺少独立节假日日历，不能证明区间完整")
    return coverage_ratio, partial, issues


def _intraday_quality_status(
    *,
    duplicate_rows: int,
    missing_columns: list[str],
    invalid_numeric: int,
    invalid_semantics: int,
    boundary_failure: bool,
    coverage_ratio: float | None,
    frequency_minutes: int,
    issues: list[str],
    time_unzoned: bool,
    unsupported_market: bool,
    partial: bool,
) -> QualityStatus:
    blocking = bool(
        duplicate_rows or missing_columns or invalid_numeric or invalid_semantics
        or boundary_failure
        or (coverage_ratio is not None and coverage_ratio < 0.80)
        or frequency_minutes <= 0
        or any("未对齐" in item for item in issues)
        or time_unzoned or unsupported_market
    )
    if blocking:
        return "unavailable"
    return "degraded" if issues or partial else "verified"


def _assess_intraday_frame(
    frame: pd.DataFrame | None,
    start: str,
    end: str,
    *,
    symbol: str,
    frequency: str,
    source: str = "",
    stale: bool = False,
) -> BarDataQuality:
    issues: list[str] = []
    try:
        market = guess_market(symbol)
    except ValueError:
        market = Market.FUTURES
    zone = _market_timezone(symbol)
    unsupported_market = zone is None
    if unsupported_market:
        issues.append(
            "MARKET_SESSION_UNSUPPORTED: 缺少该交易所/产品的已验证时区与交易时段模板"
        )
    frequency_minutes = _MINUTE_FREQUENCY_MINUTES.get(frequency, 0)
    if frequency_minutes <= 0:
        issues.append("无法确认分钟行情的时间间隔")
    units, unit_issue = _unit_contract(symbol)
    if unit_issue:
        issues.append(unit_issue)
    if frame is None or frame.empty:
        return BarDataQuality(
            "unavailable", start, end, sources=(source,) if source else (),
            issues=tuple((*issues, "分钟行情为空")), stale=stale, partial=True,
            timezone="unknown", adjustment="none", units=units,
            requested_symbols=(symbol,), missing_symbols=(symbol,),
        )
    valid_index, timezone, time_unzoned, duplicate_rows, index_issues = (
        _intraday_index_assessment(frame, zone)
    )
    issues.extend(index_issues)
    row_issues, missing_columns, invalid_numeric, invalid_semantics = (
        _intraday_row_assessment(frame)
    )
    issues.extend(row_issues)
    request_zone = zone or ZoneInfo("UTC")
    requested_start, requested_end = _intraday_requested_range(
        start,
        end,
        symbol=symbol,
        frequency=frequency,
        request_zone=request_zone,
    )
    observed_start, observed_end, boundary_failure, boundary_issues = (
        _intraday_boundary_assessment(valid_index, requested_start, requested_end)
    )
    issues.extend(boundary_issues)
    observed_dates = pd.DatetimeIndex(valid_index.normalize().unique())
    sessions, calendar_source, calendar_complete = _market_sessions(
        market,
        requested_start.normalize(),
        requested_end.normalize(),
        observed_dates=observed_dates,
    )
    coverage_ratio, coverage_partial, coverage_issues = (
        _intraday_coverage_assessment(
            market,
            sessions,
            calendar_complete,
            valid_index,
            requested_start=requested_start,
            requested_end=requested_end,
            frequency=frequency,
            frequency_minutes=frequency_minutes,
        )
    )
    issues.extend(coverage_issues)
    partial = boundary_failure or stale or coverage_partial
    if stale:
        issues.append("行情刷新失败，正在使用旧分钟缓存")
    status = _intraday_quality_status(
        duplicate_rows=duplicate_rows,
        missing_columns=missing_columns,
        invalid_numeric=invalid_numeric,
        invalid_semantics=invalid_semantics,
        boundary_failure=boundary_failure,
        coverage_ratio=coverage_ratio,
        frequency_minutes=frequency_minutes,
        issues=issues,
        time_unzoned=time_unzoned,
        unsupported_market=unsupported_market,
        partial=partial,
    )
    return BarDataQuality(
        status,
        start,
        end,
        observed_start="" if pd.isna(observed_start) else observed_start.isoformat(),
        observed_end="" if pd.isna(observed_end) else observed_end.isoformat(),
        coverage_ratio=round(coverage_ratio, 6) if coverage_ratio is not None else None,
        calendar_source=calendar_source,
        sources=(source,) if source else (),
        issues=tuple(dict.fromkeys(issues)),
        stale=stale,
        partial=partial,
        timezone=timezone,
        adjustment="none",
        units=units,
        duplicate_rows=duplicate_rows,
        requested_symbols=(symbol,),
        observed_symbols=(symbol,),
    )


def _assess_bar_quality(
    frame: pd.DataFrame,
    *,
    symbol: str,
    start: str,
    end: str,
    frequency: str,
    source: str,
    stale: bool,
) -> BarDataQuality:
    if frequency == "1d":
        return _assess_daily_frame(
            frame, start, end, symbol=symbol, source=source, stale=stale,
        )
    return _assess_intraday_frame(
        frame, start, end, symbol=symbol, frequency=frequency,
        source=source, stale=stale,
    )


def _with_daily_freshness(
    quality: BarDataQuality,
    frame: pd.DataFrame,
    *,
    symbol: str,
    end: str,
    metadata: dict[str, Any],
    purpose: CachePurpose | str,
) -> BarDataQuality:
    expected = SessionExpectation()
    historical = str(purpose) in {
        CachePurpose.HISTORICAL.value,
        CachePurpose.FORMAL_RESEARCH.value,
        "historical_replay",
    }
    current = pd.Timestamp(market_now())
    requested = (
        min(pd.Timestamp(end).normalize(), pd.Timestamp(market_date()).normalize())
        if historical else current.tz_localize(None).normalize()
    )
    calendar_ready = False
    if historical:
        market = guess_market(symbol)
        observed = pd.DatetimeIndex(pd.to_datetime(frame.index, errors="coerce")).dropna()
        if observed.tz is not None:
            observed = observed.tz_localize(None)
        if market in {Market.CN, Market.INDEX}:
            sessions, calendar_source = _local_sessions(
                requested - pd.Timedelta(days=45), requested,
            )
            calendar_ready = bool(len(sessions))
        else:
            sessions, calendar_source, calendar_ready = _market_sessions(
                market,
                requested - pd.Timedelta(days=45),
                requested,
                observed_dates=observed.normalize(),
            )
    else:
        sessions, calendar_source = _local_sessions(
            requested - pd.Timedelta(days=45), requested,
        )
        calendar_ready = bool(len(sessions))
    if calendar_ready and len(sessions):
        expected = SessionExpectation(
            sessions.max().date().isoformat(), calendar_source, True,
            "已发布本地交易日证据",
        )
    freshness = assess_daily_freshness(
        symbol=symbol,
        frame=frame,
        requested_end=end,
        checked_at=float(metadata.get("checked_at") or 0),
        purpose=purpose,
        expectation=expected,
        display_ttl_seconds=get_config().data.cache_days * 86400,
    )
    freshness_stale = freshness.state in {"stale", "unchecked", "incomplete"}
    return replace(
        quality,
        status=(
            "degraded"
            if freshness_stale and quality.status == "verified"
            else "unavailable" if freshness.future_rows else quality.status
        ),
        stale=quality.stale or freshness_stale,
        issues=tuple(dict.fromkeys((
            *quality.issues,
            *((freshness.refresh_reason,) if freshness.refresh_reason else ()),
        ))),
        freshness_state=freshness.state,
        age_seconds=freshness.age_seconds,
        stale_while_revalidate=freshness.stale_while_revalidate,
        refresh_reason=freshness.refresh_reason,
        expected_session=freshness.expected_session,
        future_rows=max(quality.future_rows, freshness.future_rows),
    )


def _decoded_source_provenance(metadata: dict[str, Any]) -> list[object]:
    try:
        raw_provenance = json.loads(str(metadata.get("source_chain_json") or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return raw_provenance if isinstance(raw_provenance, list) else []


def _provenance_event_overlaps(
    event: dict[str, object],
    requested_start: pd.Timestamp,
    requested_end: pd.Timestamp,
) -> bool:
    for prefix in ("requested", "affected"):
        event_start = str(event.get(f"{prefix}_start") or "")
        event_end = str(event.get(f"{prefix}_end") or "")
        if not event_start or not event_end:
            continue
        try:
            normalized_start = _shanghai_wall_time(event_start)
            normalized_end = _shanghai_wall_time(event_end)
        except (TypeError, ValueError):
            return True
        return normalized_start <= requested_end and normalized_end >= requested_start
    return True


def _provenance_for_range(
    metadata: dict[str, Any],
    requested_start: pd.Timestamp,
    requested_end: pd.Timestamp,
) -> list[dict[str, object]]:
    return [
        dict(item)
        for item in _decoded_source_provenance(metadata)
        if isinstance(item, dict)
        and _provenance_event_overlaps(item, requested_start, requested_end)
    ]


def _provenance_event_interval(
    event: dict[str, object],
) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    for prefix in ("requested", "affected"):
        event_start = str(event.get(f"{prefix}_start") or "")
        event_end = str(event.get(f"{prefix}_end") or "")
        if not event_start or not event_end:
            continue
        try:
            return _shanghai_wall_time(event_start), _shanghai_wall_time(event_end)
        except (TypeError, ValueError):
            return None
    return None


def _provenance_event_status(event: dict[str, object]) -> str:
    contract = event.get("quality")
    if isinstance(contract, dict):
        status = str(contract.get("status") or "")
        if status:
            return status
    return str(event.get("status") or "")


def _lineage_interval_contiguous(
    interval_start: pd.Timestamp,
    merged_end: pd.Timestamp,
    frequency: str,
) -> bool:
    if frequency == "1d":
        return interval_start <= merged_end + pd.offsets.BDay(1)
    if interval_start.normalize() == merged_end.normalize():
        tolerance = pd.Timedelta(
            minutes=_MINUTE_FREQUENCY_MINUTES.get(frequency, 0),
        )
        return interval_start <= merged_end + tolerance
    return interval_start.normalize() <= merged_end.normalize() + pd.offsets.BDay(1)


def _lineage_covers(
    events: list[dict[str, object]],
    requested_start: pd.Timestamp,
    requested_end: pd.Timestamp,
    frequency: str,
) -> bool:
    intervals = sorted(
        interval
        for event in events
        if _provenance_event_status(event) in _QUALITY_RANK
        for interval in (_provenance_event_interval(event),)
        if interval is not None
    )
    if not intervals or intervals[0][0] > requested_start:
        return False
    merged_end = intervals[0][1]
    for interval_start, interval_end in intervals[1:]:
        if not _lineage_interval_contiguous(interval_start, merged_end, frequency):
            return False
        merged_end = max(merged_end, interval_end)
    return merged_end >= requested_end


def _persisted_quality_contracts(
    provenance: list[dict[str, object]],
    metadata: dict[str, Any],
) -> list[dict[str, Any]]:
    # Persisted quality is decoded JSON and therefore intentionally schema-dynamic.
    persisted_contracts: list[dict[str, Any]] = []
    for event in provenance:
        contract = event.get("quality")
        if isinstance(contract, dict) and contract:
            value = dict(contract)
            event_source = str(event.get("source") or "")
            value["sources"] = list(dict.fromkeys((
                *(str(item) for item in value.get("sources") or () if item),
                *(item for item in (event_source,) if item),
            )))
            persisted_contracts.append(value)
    if not persisted_contracts:
        try:
            persisted = json.loads(str(metadata.get("quality_json") or "{}"))
            if isinstance(persisted, dict) and persisted:
                persisted_contracts.append(persisted)
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
    return persisted_contracts


def _with_legacy_lineage_warning(
    quality: BarDataQuality,
    provenance: list[dict[str, object]],
    frequency: str,
) -> BarDataQuality:
    # A former release persisted per-symbol Tushare witnesses beside StockDB
    # bytes.  They have no immutable batch-manifest identity, so they remain
    # viewable but cannot silently satisfy the new formal evidence contract.
    legacy = frequency == "1d" and any(
        "stockdb-price+tushare-contract-v2" in str(event.get("source") or "")
        for event in provenance
    )
    if not legacy:
        return quality
    return replace(
        quality,
        status="degraded" if quality.status == "verified" else quality.status,
        partial=True,
        issues=tuple(dict.fromkeys((
            *quality.issues,
            "旧版逐标的交叉证据缺少整批内容清单，仅可作为预览快照",
        ))),
    )


def _merge_persisted_quality(
    quality: BarDataQuality,
    persisted_contracts: list[dict[str, Any]],
) -> BarDataQuality:
    rank = _QUALITY_RANK
    for persisted in persisted_contracts:
        persisted_status_raw = str(persisted.get("status") or "")
        if persisted_status_raw not in rank:
            continue
        persisted_status = cast(QualityStatus, persisted_status_raw)
        status = max((quality.status, persisted_status), key=lambda value: rank[value])
        ratios = [
            float(value) for value in (quality.coverage_ratio, persisted.get("coverage_ratio"))
            if value is not None
        ]
        persisted_sources = tuple(str(value) for value in persisted.get("sources") or () if value)
        quality = replace(
            quality,
            status=status,
            coverage_ratio=min(ratios) if ratios else None,
            sources=tuple(dict.fromkeys((*quality.sources, *persisted_sources))),
            issues=tuple(dict.fromkeys((
                *quality.issues,
                *(str(value) for value in persisted.get("issues") or () if value),
            ))),
            stale=quality.stale or bool(persisted.get("stale")),
            partial=quality.partial or bool(persisted.get("partial")),
            duplicate_rows=max(quality.duplicate_rows, int(persisted.get("duplicate_rows") or 0)),
            adjustment=(
                str(persisted.get("adjustment"))
                if str(persisted.get("adjustment") or "") not in {"", "qfq"}
                else quality.adjustment
            ),
        )
    return quality


def _with_lineage_completion(
    quality: BarDataQuality,
    provenance: list[dict[str, object]],
    *,
    source: str,
    lineage_complete: bool,
    requested_start: pd.Timestamp,
    requested_end: pd.Timestamp,
) -> tuple[BarDataQuality, list[dict[str, object]]]:
    if not provenance:
        missing_provenance: dict[str, object] = {
            "status": "provenance_missing",
            "diagnostic_code": "provenance_missing",
            "observed_start": quality.observed_start,
            "observed_end": quality.observed_end,
        }
        if source:
            missing_provenance["source"] = source
        provenance = [missing_provenance]
        quality = replace(
            quality,
            status="degraded" if quality.status == "verified" else quality.status,
            partial=True,
            issues=tuple(dict.fromkeys((
                *quality.issues,
                "provenance_missing: 缓存记录没有请求区间的来源证据",
            ))),
        )
        return quality, provenance
    if lineage_complete:
        return quality, provenance
    provenance.append({
            "status": "lineage_gap",
            "diagnostic_code": "provenance_incomplete",
            "requested_start": requested_start.isoformat(),
            "requested_end": requested_end.isoformat(),
    })
    quality = replace(
        quality,
        status="degraded" if quality.status == "verified" else quality.status,
        partial=True,
        issues=tuple(dict.fromkeys((
            *quality.issues,
            "provenance_incomplete: 来源证据没有覆盖完整请求区间",
        ))),
    )
    return quality, provenance


def _bar_envelope(
    frame: pd.DataFrame,
    *,
    symbol: str,
    start: str,
    end: str,
    store: BarStore,
    frequency: str,
    metadata: dict[str, Any] | None = None,
    purpose: CachePurpose | str = CachePurpose.CURRENT_ANALYSIS,
) -> BarDataEnvelope[pd.DataFrame]:
    metadata = metadata if metadata is not None else store.metadata(symbol) or {}
    source = str(metadata.get("last_source") or "")
    stale = str(metadata.get("last_status") or "") in {"stale", "refresh_failed"}
    quality = _assess_bar_quality(
        frame,
        symbol=symbol,
        start=start,
        end=end,
        frequency=frequency,
        source=source,
        stale=stale,
    )
    if frequency == "1d":
        quality = _with_daily_freshness(
            quality, frame, symbol=symbol, end=end, metadata=metadata, purpose=purpose,
        )
    requested_start = _shanghai_wall_time(start)
    requested_end = _shanghai_wall_time(end)
    provenance = _provenance_for_range(metadata, requested_start, requested_end)
    lineage_complete = _lineage_covers(
        provenance, requested_start, requested_end, frequency,
    )
    persisted_contracts = _persisted_quality_contracts(provenance, metadata)
    quality = _with_legacy_lineage_warning(quality, provenance, frequency)
    quality = _merge_persisted_quality(quality, persisted_contracts)
    quality, provenance = _with_lineage_completion(
        quality,
        provenance,
        source=source,
        lineage_complete=lineage_complete,
        requested_start=requested_start,
        requested_end=requested_end,
    )
    return BarDataEnvelope(frame, quality, tuple(provenance))


def _align_increment(
    cached: pd.DataFrame,
    fresh: pd.DataFrame,
    direction: str,
) -> pd.DataFrame:
    """用重叠交易日对齐动态前复权基准，再合并边界增量。"""
    cached = cached.sort_index()
    fresh = fresh.sort_index()
    common = cached.index.intersection(fresh.index)
    if common.empty or "close" not in cached or "close" not in fresh:
        raise AdjustmentMismatch("增量响应没有可用于校准的重叠交易日")
    ratios = (
        (fresh.loc[common, "close"] / cached.loc[common, "close"])
        .replace([float("inf"), float("-inf")], pd.NA)
        .dropna()
    )
    ratios = ratios[ratios > 0]
    if ratios.empty:
        raise AdjustmentMismatch("重叠交易日价格无效")
    ratio = float(ratios.median())
    if len(ratios) >= 2:
        deviations = (ratios / ratio - 1).abs()
        stable = deviations <= 0.005
        # 允许少量上游修订或异常重叠行，但至少 80% 日期必须支持同一比例。
        if int(stable.sum()) < max(2, math.ceil(len(ratios) * 0.8)):
            raise AdjustmentMismatch("重叠交易日无法形成稳定的复权比例")
        ratio = float(ratios.loc[stable].median())
    ohlc = [
        column
        for column in ("open", "high", "low", "close")
        if column in cached.columns and column in fresh.columns
    ]
    if direction == "left":
        aligned = fresh.copy()
        aligned[ohlc] = aligned[ohlc] / ratio
        merged = pd.concat([aligned, cached])
    else:
        aligned = cached.copy()
        aligned[ohlc] = aligned[ohlc] * ratio
        merged = pd.concat([aligned, fresh])
    return merged[~merged.index.duplicated(keep="last")].sort_index()


def _accept_local_stockdb_without_remote_upgrade(
    source: DataSource,
    quality: BarDataQuality,
) -> bool:
    """Accept structurally complete local rows despite evidence-only warnings."""
    return bool(
        quality.status == "degraded"
        and not quality.stale
        and not quality.partial
        and source.name.startswith("free-stockdb")
        and get_config().data.primary_provider == "free-stockdb"
    )


def _full_refresh(
    symbol: str,
    start: str,
    end: str,
    cached: pd.DataFrame | None,
    store: BarStore,
    priority: str,
    provider: str = "",
) -> pd.DataFrame:
    market = guess_market(symbol)
    errors: list[str] = []
    degraded_candidate: tuple[pd.DataFrame, BarDataQuality, str] | None = None

    def persist(
        frame: pd.DataFrame,
        quality: BarDataQuality,
        storage_source: str,
    ) -> pd.DataFrame:
        store.put(
            symbol,
            frame,
            replace=True,
            request_start=start,
            request_end=end,
            source=storage_source,
            quality=quality.to_dict(),
            replace_coverage=True,
        )
        stored = store.get(symbol)
        if stored is None:
            raise RuntimeError(f"{storage_source} 日线写入后无法读取")
        return stored.loc[start:end]

    for factory in _request_factories(
        priority=priority, allow_online=False, provider=provider,
    ).get(market, []):
        try:
            source = factory()
            with data_priority(priority), bypass_endpoint_cache():
                frame = source.daily(symbol, start, end)
            if frame is None or frame.empty:
                errors.append(f"{factory.__name__}: 返回空数据")
                continue
            if not _is_complete_refresh(frame, cached, start, end, symbol=symbol):
                errors.append(f"{factory.__name__}: 响应缺失已有交易日或内部过于稀疏")
                continue
            quality = _assess_daily_frame(
                frame, start, end, symbol=symbol, source=source.name,
            )
            storage_source = source.name
            if quality.status == "verified" or (
                source.name.startswith("free-stockdb") and quality.status == "degraded"
            ):
                return persist(frame, quality, storage_source)
            # StockDB is the configured local source of truth. Its SDK does
            # not provide an independently versioned unit/adjustment manifest,
            # so usable rows remain explicitly degraded. That evidence-only
            # downgrade must not fan out to every remote provider.
            if _accept_local_stockdb_without_remote_upgrade(source, quality):
                return persist(frame, quality, storage_source)
            errors.append(
                f"{factory.__name__}: 数据完整但真实性契约为 {quality.status}，继续后备源"
            )
            if quality.status == "degraded" and (
                degraded_candidate is None
                or pd.Timestamp(frame.index.max())
                > pd.Timestamp(degraded_candidate[0].index.max())
            ):
                degraded_candidate = (frame, quality, storage_source)
        except (
            httpx.HTTPError,
            ImportError,
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
        ) as exc:
            errors.append(f"{factory.__name__}: {exc}")
            logger.debug("数据源 %s 全量获取 %s 失败: %s", factory.__name__, symbol, exc)
    if degraded_candidate is not None:
        return persist(*degraded_candidate)
    if cached is not None and not cached.empty:
        store.mark_status(symbol, "refresh_failed")
        logger.debug("全量刷新失败，保留本地缓存: %s", symbol)
        return cached.loc[start:end]
    raise _unavailable_error((symbol,), start, end, errors, frequency="1d")


def _fetch_segment(
    symbol: str,
    start: str,
    end: str,
    direction: str,
    cached: pd.DataFrame | None,
    store: BarStore,
    priority: str,
    refresh_provider_cache: bool = False,
    provider: str = "",
) -> tuple[pd.DataFrame | None, list[str], bool]:
    market = guess_market(symbol)
    errors: list[str] = []
    cached_latest = (
        pd.Timestamp(cached.index.max()).normalize() if cached is not None and not cached.empty else None
    )
    prefer_extension = (
        direction == "right" and cached_latest is not None and cached_latest < pd.Timestamp(end).normalize()
    )
    best: tuple[DataSource, pd.DataFrame, pd.DataFrame, pd.Timestamp] | None = None
    degraded_best: tuple[
        DataSource,
        pd.DataFrame,
        pd.DataFrame,
        pd.Timestamp,
        tuple[pd.DataFrame, BarDataQuality, str],
    ] | None = None

    def evaluate(
        source: DataSource,
        evidence: pd.DataFrame,
    ) -> tuple[pd.DataFrame, BarDataQuality, str]:
        quality = _assess_daily_frame(
            evidence,
            start,
            end,
            symbol=symbol,
            source=source.name,
        )
        storage_source = source.name
        return evidence, quality, storage_source

    def save(
        source: DataSource | None,
        merged: pd.DataFrame,
        evidence: pd.DataFrame,
        evaluated: tuple[pd.DataFrame, BarDataQuality, str] | None = None,
    ) -> tuple[pd.DataFrame, list[str], bool]:
        if evaluated is None:
            if source is None:
                raise ValueError("行情写入缺少来源")
            _, quality, storage_source = evaluate(source, evidence)
        else:
            _, quality, storage_source = evaluated
        store.put(
            symbol,
            merged,
            replace=True,
            request_start=start,
            request_end=end,
            source=storage_source,
            quality=quality.to_dict(),
        )
        return store.get(symbol), errors, True

    # These canonical reference symbols intentionally retain their ``.US``
    # suffix, so the generic market classifier would otherwise send them
    # through the US-equity lane.  Use their dedicated routes first: they
    # validate the provider ticker (Sina GC/CL/HG or Tushare USDCNH) and make
    # the per-symbol Yahoo fallback explicit.  A failure remains local to this
    # symbol and the generic candidates below can still be tried.
    from quantmaster.data.reference_market import (
        ReferenceMarketUnavailable,
        fetch_reference,
        is_reference_symbol,
    )
    if is_reference_symbol(symbol):
        try:
            reference = fetch_reference(symbol, start, end)
            frame = reference.frame
            if not _covers_requested_range(frame, start, end, symbol=symbol):
                errors.append("reference-market: 响应内部过于稀疏")
            else:
                merged = frame if cached is None or cached.empty else _align_increment(
                    cached, frame, direction,
                )
                fresh_latest = pd.Timestamp(frame.index.max()).normalize()
                evaluated = (
                    frame,
                    _assess_daily_frame(
                        frame, start, end, symbol=symbol, source=reference.source,
                    ),
                    reference.source,
                )
                if prefer_extension and cached_latest is not None and fresh_latest <= cached_latest:
                    errors.append(
                        f"reference-market: 未返回 {cached_latest.date()} 之后的新行情"
                    )
                else:
                    return save(None, merged, frame, evaluated)
        except ReferenceMarketUnavailable as exc:
            errors.extend(
                f"reference-market/{item['source']}: {item['detail']}"
                for item in exc.attempts
            )
        except (ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
            errors.append(f"reference-market: {exc}")

    for factory in _request_factories(
        priority=priority, allow_online=True, provider=provider,
    ).get(market, []):
        try:
            source = factory()
            with data_priority(priority), bypass_endpoint_cache(refresh_provider_cache):
                frame = source.daily(symbol, start, end)
            if frame is None or frame.empty:
                errors.append(f"{factory.__name__}: 返回空数据")
                continue
            if not _covers_requested_range(frame, start, end, symbol=symbol):
                errors.append(f"{factory.__name__}: 响应内部过于稀疏")
                continue
            merged = frame if cached is None or cached.empty else _align_increment(cached, frame, direction)
            fresh_latest = pd.Timestamp(frame.index.max()).normalize()
            evaluated = evaluate(source, frame)
            if prefer_extension and cached_latest is not None and fresh_latest <= cached_latest:
                errors.append(f"{factory.__name__}: 未返回 {cached_latest.date()} 之后的新行情")
                if (
                    evaluated[1].status != "unavailable"
                    and (best is None or fresh_latest > best[3])
                ):
                    best = (source, merged, frame, fresh_latest)
                continue
            if evaluated[1].status == "verified" or (
                source.name.startswith("free-stockdb") and evaluated[1].status == "degraded"
            ):
                return save(source, merged, frame, evaluated)
            if _accept_local_stockdb_without_remote_upgrade(source, evaluated[1]):
                return save(source, merged, frame, evaluated)
            errors.append(
                f"{factory.__name__}: 增量真实性契约为 {evaluated[1].status}，继续后备源"
            )
            if evaluated[1].status == "degraded" and degraded_best is None:
                degraded_best = (source, merged, frame, fresh_latest, evaluated)
        except AdjustmentMismatch as exc:
            errors.append(f"{factory.__name__}: {exc}")
        except Exception as exc:
            errors.append(f"{factory.__name__}: {exc}")
            logger.debug("数据源 %s 增量获取 %s 失败: %s", factory.__name__, symbol, exc)
    if degraded_best is not None:
        return save(
            degraded_best[0],
            degraded_best[1],
            degraded_best[2],
            degraded_best[4],
        )
    if best is not None:
        # 周末、休市或所有上游尚未发布时，保留最新的有效响应。
        saved, collected_errors, _ = save(best[0], best[1], best[2])
        store.mark_status(symbol, "stale", source=best[0].name)
        return saved, collected_errors, False
    return cached, errors, False


def _session_refresh_due(
    symbol: str,
    requested_end: pd.Timestamp,
    cached: pd.DataFrame | None,
    checked_at: float,
    *,
    now: pd.Timestamp | None = None,
) -> bool:
    """Return true once an early-day CN check crosses the market-close boundary."""
    if guess_market(symbol) not in {Market.CN, Market.INDEX}:
        return False
    if cached is None or cached.empty or not checked_at:
        return False
    current = now if now is not None else pd.Timestamp(market_now())
    if current.tzinfo is None:
        current = current.tz_localize("Asia/Shanghai")
    else:
        current = current.tz_convert("Asia/Shanghai")
    today = current.normalize()
    close = today + pd.Timedelta(hours=15, minutes=30)
    cached_end = pd.Timestamp(cached.index.max()).date()
    return bool(
        requested_end.date() >= today.date()
        and cached_end < today.date()
        and current >= close
        and checked_at < close.timestamp()
    )


def _factories() -> dict[Market, list]:
    from quantmaster.data.akshare_source import AkshareSource
    from quantmaster.data.free_stockdb_source import (
        FreeStockDBOnlineSource,
        FreeStockDBSource,
    )
    from quantmaster.data.tushare_source import TushareSource
    from quantmaster.data.yfinance_source import YFinanceSource

    ak, free, free_online, yf, tu = (
        AkshareSource,
        FreeStockDBSource,
        FreeStockDBOnlineSource,
        YFinanceSource,
        TushareSource,
    )
    cfg = get_config().data

    def enabled(*factories):
        switches = {
            ak: cfg.akshare_enabled,
            tu: cfg.tushare_enabled,
            yf: cfg.yfinance_enabled,
        }
        return [factory for factory in factories if switches.get(factory, True)]

    # The public endpoint is an explicitly enabled, last-resort interactive
    # supplement. _request_factories removes it from normal/background work.
    online_tail = [free_online] if cfg.free_stockdb_online_enabled else []
    local_first = [free, *enabled(tu, ak), *online_tail]
    orders = {
        "free-stockdb": local_first,
        "akshare": [*enabled(ak, tu), free, *online_tail],
        "tushare": [*enabled(tu, ak), free, *online_tail],
    }
    selected = str(get_config().data.primary_provider).strip().lower()
    if selected not in orders:
        raise ValueError(
            f"未知主数据源 {selected!r}；可选 free-stockdb/akshare/tushare"
        )
    cn = orders[selected]
    return {
        Market.CN: cn,
        Market.HK: enabled(ak, yf),
        Market.US: enabled(yf),
        Market.JP: enabled(yf),
        Market.KR: enabled(yf),
        Market.FUTURES: enabled(ak, yf),
        Market.INDEX: enabled(tu, ak, yf),
    }


def _request_factories(
    *,
    priority: str,
    allow_online: bool,
    provider: str = "",
) -> dict[Market, list]:
    # A Web page is allowed to consume an already-published local snapshot but
    # never to turn a cache miss, stale flag or formal-evidence failure into an
    # upstream wait.  Keep the empty result explicit so the caller can return
    # a fast unavailable/degraded envelope instead of accidentally using a
    # lower-priority remote provider.
    if not remote_io_allowed():
        return {market: [] for market in Market}
    selected_provider = str(provider or "").strip().lower()
    if selected_provider:
        cfg = get_config().data
        enabled = {
            "free-stockdb-online": cfg.free_stockdb_online_enabled,
            "tushare": cfg.tushare_enabled,
        }
        if selected_provider in enabled and not enabled[selected_provider]:
            raise ValueError(f"数据源 {selected_provider} 已在设置中关闭")
        from quantmaster.data.free_stockdb_source import (
            FreeStockDBOnlineSource,
            FreeStockDBSource,
        )
        from quantmaster.data.tushare_source import TushareSource

        selected = {
            "free-stockdb": FreeStockDBSource,
            "free-stockdb-online": FreeStockDBOnlineSource,
            "tushare": TushareSource,
        }.get(selected_provider)
        if selected is None:
            raise ValueError(f"不支持的数据源: {selected_provider}")
        return {
            market: [selected] if market in selected.markets else []
            for market in Market
        }
    factories = _factories()
    if allow_online and priority == "interactive":
        return factories
    return {
        market: [factory for factory in ordered if factory.name != "free-stockdb-online"]
        for market, ordered in factories.items()
    }


def data_source_capabilities() -> dict[str, object]:
    """Return configured priority and honest, non-probed capability evidence."""
    factories = _factories()
    providers: dict[str, dict[str, object]] = {}
    priority: dict[str, list[str]] = {}
    for market, ordered in factories.items():
        priority[market.value] = [str(factory.name) for factory in ordered]
        for factory in ordered:
            name = str(factory.name)
            declared = sorted(value.value for value in factory.capabilities)
            capability_status: dict[str, dict[str, object]] = {
                value: {
                    "state": "declared",
                    "installed": None,
                    "connected": None,
                    "data_ready": None,
                    "verified": False,
                    "asset_classes": [],
                    "frequencies": [],
                    "coverage": None,
                    "as_of_date": "",
                }
                for value in declared
            }
            if name.startswith("free-stockdb"):
                from quantmaster.data.free_stockdb_source import resolve_free_stockdb_sdk_path

                sdk_available = resolve_free_stockdb_sdk_path() is not None
                local_configured = bool(str(get_config().data.free_stockdb_url or "").strip())
                latest_ingest = None
                try:
                    from quantmaster.data.free_stockdb_ingest import StockDBIngestStore

                    history = StockDBIngestStore().history(1)
                    latest_ingest = history[0] if history else None
                except (ImportError, OSError, RuntimeError, TypeError, ValueError):
                    latest_ingest = None
                shape = {
                    DataCapability.DAILY_BARS: (["stock", "etf"], ["1d"]),
                    DataCapability.DAILY: (["stock", "etf"], ["1d"]),
                    DataCapability.DAILY_CROSS_SECTION: (["stock", "etf"], ["1d"]),
                    DataCapability.INTRADAY_BARS: (["stock", "etf"], ["1m", "5m", "15m", "30m", "60m"]),
                    DataCapability.INTRADAY: (["stock", "etf"], ["1m", "5m", "15m", "30m", "60m"]),
                    DataCapability.EOD_SNAPSHOT: (["stock", "etf"], ["1d"]),
                    DataCapability.SECURITY_CATALOG: (["stock", "etf", "fund"], ["snapshot"]),
                    DataCapability.ADJUSTMENT_FACTORS: (["stock"], ["1d"]),
                    DataCapability.ETF_SHARES: (["etf"], ["1d"]),
                    DataCapability.INDUSTRY: (["stock"], ["snapshot"]),
                    DataCapability.THEMES: (["stock"], ["snapshot"]),
                    DataCapability.BOARD_HIERARCHY: (["stock"], ["snapshot"]),
                    DataCapability.NATIVE_INDICATORS: (["stock", "etf"], ["1d"]),
                }
                for capability in factory.capabilities:
                    needs_sdk = capability in {
                        DataCapability.DAILY_CROSS_SECTION,
                        DataCapability.BOARD_HIERARCHY,
                        DataCapability.SECURITY_CATALOG,
                        DataCapability.NATIVE_INDICATORS,
                    }
                    installed = sdk_available if needs_sdk else local_configured
                    capability_status[capability.value] = {
                        "state": "unverified" if installed else "unavailable",
                        "installed": installed,
                        "connected": None,
                        "data_ready": bool(latest_ingest) if installed else False,
                        "verified": False,
                        "asset_classes": shape.get(capability, ([], []))[0],
                        "frequencies": shape.get(capability, ([], []))[1],
                        "coverage": latest_ingest.coverage if latest_ingest else None,
                        "as_of_date": latest_ingest.as_of_date if latest_ingest else "",
                    }
            providers.setdefault(
                name,
                {
                    "name": name,
                    "markets": sorted(value.value for value in factory.markets),
                    "capabilities": declared,
                    "capability_status": capability_status,
                },
            )
    return {
        "selected": str(get_config().data.primary_provider),
        "priority": priority,
        "providers": list(providers.values()),
    }


def get_source(
    market: Market,
    capability: DataCapability | str = DataCapability.DAILY,
) -> DataSource:
    """返回该市场第一个声明所需能力且可以初始化的数据源。"""
    required = capability if isinstance(capability, DataCapability) else DataCapability(str(capability))
    errors = []
    for factory in _factories().get(market, []):
        try:
            source = factory()
            if source.supports_capability(required):
                return source
        except Exception as e:  # pragma: no cover - 依赖安装情况
            errors.append(f"{factory.__name__}: {e}")
    raise RuntimeError(f"市场 {market.value} 没有支持 {required.value} 的可用数据源: {errors}")


def _load_spot_frame(
    symbols: list[str], *, priority: str = "normal",
) -> tuple[pd.DataFrame, tuple[dict[str, object], ...], tuple[str, ...]]:
    """Internal spot loader with per-source evidence for the public envelope."""
    requested = list(dict.fromkeys(str(symbol).upper() for symbol in symbols))
    by_code = {symbol.partition(".")[0].zfill(6): symbol for symbol in requested}
    rows: dict[str, dict] = {}
    errors: list[str] = []
    provenance: list[dict[str, object]] = []
    allow_online = priority == "interactive" and len(requested) <= 20
    for factory in _request_factories(
        priority=priority,
        allow_online=allow_online,
    ).get(Market.CN, []):
        missing = [symbol for code, symbol in by_code.items() if code not in rows]
        if not missing:
            break
        try:
            source = factory()
            if not source.supports_capability(DataCapability.SPOT):
                continue
            snapshot = source.spot(missing)
            snapshot_codes = snapshot.get("code", pd.Series(dtype=str)).astype(str).str.zfill(6)
            duplicates = int(snapshot_codes.duplicated(keep=False).sum())
            if duplicates:
                errors.append(f"{source.name}: 存在 {duplicates} 行重复快照代码")
                continue
            accepted: list[str] = []
            for _, value in snapshot.iterrows():
                code = str(value.get("code") or "").zfill(6)
                if code in by_code and code not in rows:
                    item = value.to_dict()
                    item["code"] = code
                    item["source"] = source.name
                    rows[code] = item
                    accepted.append(by_code[code])
            provenance.append({
                "source": source.name,
                "fetched_at": market_now().isoformat(),
                "requested_symbols": missing,
                "observed_symbols": accepted,
            })
        except Exception as exc:
            errors.append(f"{factory.__name__}: {exc}")
            logger.debug("快照数据源 %s 失败: %s", factory.__name__, exc)
    if not rows:
        today = market_date().isoformat()
        raise _unavailable_error(
            tuple(requested), today, today,
            errors or ["没有可用快照数据"], frequency="spot",
        )
    return (
        pd.DataFrame(rows.values()).reset_index(drop=True),
        tuple(provenance),
        tuple(errors),
    )


def _parse_spot_timestamp(value: object) -> pd.Timestamp | None:
    """Parse only timestamps that carry an explicit date; never attach today's date to a time."""
    if value is None or (not isinstance(value, (str, bytes)) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.isdigit() and len(text) in {10, 13, 16, 19}:
            unit = {10: "s", 13: "ms", 16: "us", 19: "ns"}[len(text)]
            return pd.to_datetime(int(text), unit=unit, utc=True).tz_convert("Asia/Shanghai")
        if not re.search(r"\b20\d{2}[-/]\d{1,2}[-/]\d{1,2}\b", text):
            return None
        stamp = pd.Timestamp(value)
        if stamp.tzinfo is None:
            return stamp.tz_localize("Asia/Shanghai")
        return stamp.tz_convert("Asia/Shanghai")
    except (OverflowError, TypeError, ValueError):
        return None


def refresh_spot(
    symbols: list[str], *, work_class: str = "normal",
) -> BarDataEnvelope[pd.DataFrame]:
    """Refresh A-share spot data in a worker, with evidence attached.

    Spot data has no page-readable local snapshot contract yet, so this is
    deliberately refresh-only.  A page must render its published derivative
    snapshot instead of calling an upstream spot provider.
    """
    requested = tuple(dict.fromkeys(str(symbol).upper() for symbol in symbols))
    frame, provenance, errors = _load_spot_frame(list(requested), priority=work_class)
    by_code = {symbol.partition(".")[0].zfill(6): symbol for symbol in requested}
    observed = tuple(
        by_code[code]
        for code in frame.get("code", pd.Series(dtype=str)).astype(str).str.zfill(6)
        if code in by_code
    )
    missing = tuple(symbol for symbol in requested if symbol not in observed)
    issues = list(errors)
    sources = tuple(dict.fromkeys(str(value) for value in frame.get("source", []) if value))
    time_column = next(
        (column for column in ("datetime", "trade_time", "updated_at", "time") if column in frame),
        "",
    )
    observed_start = ""
    observed_end = ""
    oldest_stamp: pd.Timestamp | None = None
    newest_stamp: pd.Timestamp | None = None
    missing_timestamp_rows = len(frame)
    future_timestamp = False
    if time_column:
        parsed_values = [
            stamp
            for stamp in (_parse_spot_timestamp(value) for value in frame[time_column])
            if stamp is not None
        ]
        missing_timestamp_rows = len(frame) - len(parsed_values)
        if parsed_values:
            oldest_stamp = min(parsed_values)
            newest_stamp = max(parsed_values)
            observed_start = oldest_stamp.isoformat()
            observed_end = newest_stamp.isoformat()
    if newest_stamp is None:
        issues.append("快照响应缺少可验证的上游观测时点")
    elif missing_timestamp_rows:
        issues.append(f"有 {missing_timestamp_rows} 行快照缺少可验证观测时点")
    now = pd.Timestamp(market_now())
    stale = False
    timestamp_span_failure = False
    if oldest_stamp is not None and newest_stamp is not None:
        if newest_stamp > now + pd.Timedelta(minutes=5):
            future_timestamp = True
            issues.append("快照观测时点晚于上海市场当前时间")
        if now - oldest_stamp > pd.Timedelta(minutes=15):
            stale = True
            issues.append(
                f"最旧快照观测时点已滞后 {(now - oldest_stamp).total_seconds() / 60:.0f} 分钟"
            )
        if newest_stamp - oldest_stamp > pd.Timedelta(minutes=5):
            timestamp_span_failure = True
            issues.append(
                "同批快照观测时点跨度超过 5 分钟，不能用最新一行代表整批新鲜度"
            )
    missing_numeric_columns = [
        column for column in ("price", "change_pct") if column not in frame
    ]
    invalid_numeric_rows = 0
    if missing_numeric_columns:
        issues.append("快照缺少必需列：" + "、".join(missing_numeric_columns))
    else:
        numeric = frame[["price", "change_pct"]].apply(pd.to_numeric, errors="coerce")
        invalid_numeric_rows = int((~numeric.map(math.isfinite)).any(axis=1).sum())
        if invalid_numeric_rows:
            issues.append(f"有 {invalid_numeric_rows} 行快照价格或涨跌幅非有限")
    unit_contracts = [_unit_contract(symbol) for symbol in observed]
    for _, issue in unit_contracts:
        if issue:
            issues.append(issue)
    unit_values = tuple(dict.fromkeys(contract for contract, _ in unit_contracts))
    units = unit_values[0] if len(unit_values) == 1 else BarDataQuality(
        "degraded", "", "",
    ).units
    if len(unit_values) > 1:
        issues.append("快照包含多种或未知单位")
    if missing:
        issues.append("缺少请求标的：" + "、".join(missing[:20]))
    timestamp_rows_unverified = bool(time_column and missing_timestamp_rows)
    blocking = bool(
        future_timestamp
        or timestamp_span_failure
        or timestamp_rows_unverified
        or missing_numeric_columns
        or invalid_numeric_rows
    )
    status: QualityStatus = "unavailable" if blocking else (
        "verified" if not issues and not missing else "degraded"
    )
    today = market_date().isoformat()
    quality = BarDataQuality(
        status,
        today,
        today,
        observed_start=observed_start,
        observed_end=observed_end,
        coverage_ratio=round(len(observed) / len(requested), 6) if requested else 1.0,
        calendar_source="realtime-snapshot",
        sources=sources,
        issues=tuple(dict.fromkeys(issues)),
        stale=stale,
        partial=bool(missing or timestamp_rows_unverified or invalid_numeric_rows),
        timezone="Asia/Shanghai",
        adjustment="none",
        units=units,
        requested_symbols=requested,
        observed_symbols=observed,
        missing_symbols=missing,
    )
    return BarDataEnvelope(frame, quality, provenance)


def _load_history_locked(
    symbol: str,
    start: str,
    end: str,
    use_cache: bool = True,
    store: BarStore | None = None,
    *,
    refresh: RefreshMode | str | None = None,
    priority: str = "normal",
    provider: str = "",
) -> pd.DataFrame:
    """已持有单标的锁时加载标准化日线。"""
    store = store or _default_bar_store()
    cfg = get_config()
    cached = store.get(symbol)
    mode = _mode(use_cache, refresh)
    # Evidence eligibility is intentionally *not* a refresh mode.  The former
    # ``priority='formal'`` branch promoted a merely degraded local cache to a
    # full upstream refresh and could do so once per symbol in a panel.  Formal
    # callers now receive the local envelope and make a pure, fail-fast
    # eligibility decision themselves; refresh work belongs to the durable
    # background queue.
    if mode == RefreshMode.FULL:
        fetch_start, fetch_end = start, end
        if cached is not None and not cached.empty:
            fetch_start = min(start, str(cached.index.min().date()))
            fetch_end = max(end, str(cached.index.max().date()))
        return _full_refresh(
            symbol, fetch_start, fetch_end, cached, store, priority, provider,
        ).loc[start:end]

    meta = store.metadata(symbol) or {}
    try:
        cached_quality = json.loads(str(meta.get("quality_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        cached_quality = {}
    cached_semantics = dict(cached_quality.get("semantics") or {})
    requested_end = pd.Timestamp(end).normalize()
    near_current = requested_end >= pd.Timestamp(market_date()) - pd.Timedelta(days=7)
    coverage_start = str(meta.get("coverage_start") or meta.get("start") or "")
    coverage_end = str(meta.get("coverage_end") or meta.get("end") or "")
    covers_start = bool(coverage_start and coverage_start <= start)
    covers_end = bool(coverage_end and coverage_end >= end)
    checked = store.check_freshness(symbol)
    ttl_fresh = checked is not None and checked < cfg.data.cache_days * 86400
    session_refresh_due = _session_refresh_due(
        symbol, requested_end, cached, float(meta.get("checked_at") or 0)
    )
    if session_refresh_due:
        ttl_fresh = False
    sliced = _cached_slice(cached, start, end)
    if sliced is not None and covers_start and covers_end:
        if not near_current or (mode == RefreshMode.AUTO and ttl_fresh):
            return sliced

    segments: list[tuple[str, str, str]] = []
    if cached is None or cached.empty:
        segments.append((start, end, "initial"))
    else:
        if not covers_start:
            overlap_end = str(cached.index[min(4, len(cached) - 1)].date())
            segments.append((start, overlap_end, "left"))
        force_tail = near_current and (mode == RefreshMode.INCREMENTAL or not ttl_fresh)
        if not covers_end or force_tail:
            overlap_start = str(cached.index[max(0, len(cached) - 5)].date())
            segments.append((overlap_start, end, "right"))

    # Legacy bytes remain viewable, but an incremental response must not be
    # spliced onto a cache whose numeric semantics were never recorded.
    if segments and cached is not None and not cached.empty and not cached_semantics:
        fetch_start = min(start, str(cached.index.min().date()))
        fetch_end = max(end, str(cached.index.max().date()))
        return _full_refresh(
            symbol, fetch_start, fetch_end, cached, store, priority, provider,
        ).loc[start:end]

    errors: list[str] = []
    all_segments_succeeded = True
    for fetch_start, fetch_end, direction in segments:
        cached, segment_errors, succeeded = _fetch_segment(
            symbol,
            fetch_start,
            fetch_end,
            direction,
            cached,
            store,
            priority,
            refresh_provider_cache=(mode == RefreshMode.INCREMENTAL or session_refresh_due),
            provider=provider,
        )
        errors.extend(segment_errors)
        all_segments_succeeded = all_segments_succeeded and succeeded
        if cached is None or cached.empty:
            break

    available = store.get(symbol)
    sliced = _cached_slice(available, start, end)
    if sliced is not None:
        if segments and not all_segments_succeeded:
            store.mark_status(symbol, "stale")
        return sliced
    raise _unavailable_error(
        (symbol,), start, end, errors or ["没有可用日线数据"], frequency="1d",
    )


def _load_history_frame(
    symbol: str,
    start: str,
    end: str,
    use_cache: bool = True,
    store: BarStore | None = None,
    *,
    refresh: RefreshMode | str | None = None,
    priority: str = "normal",
    provider: str = "",
) -> pd.DataFrame:
    """Internal daily-bar loader; public callers must consume the envelope."""
    store = store or _default_bar_store()
    with store.lock(symbol):
        return _load_history_locked(
            symbol,
            start,
            end,
            use_cache=use_cache,
            store=store,
            refresh=refresh,
            priority=priority,
            provider=provider,
        )


def refresh_history(
    symbol: str,
    start: str,
    end: str,
    use_cache: bool = True,
    store: BarStore | None = None,
    *,
    mode: RefreshMode | str | None = None,
    work_class: str = "normal",
    source_name: str = "",
    purpose: CachePurpose | str = CachePurpose.CURRENT_ANALYSIS,
) -> BarDataEnvelope[pd.DataFrame]:
    """Refresh daily bars in a worker context and return their evidence.

    Page handlers must use :func:`read_history`; this entry point may acquire
    providers and write the cache.  Its public arguments intentionally do not
    expose the old mixed read/refresh contract.
    """
    resolved_store = store or _default_bar_store()
    frame = _load_history_frame(
        symbol,
        start,
        end,
        use_cache=use_cache,
        store=resolved_store,
        refresh=mode,
        priority=work_class,
        provider=source_name,
    )
    return _bar_envelope(
        frame, symbol=symbol, start=start, end=end, store=resolved_store, frequency="1d",
        purpose=purpose,
    )


def read_history(
    symbol: str,
    start: str,
    end: str,
    store: BarStore | None = None,
    *,
    purpose: CachePurpose | str = CachePurpose.DISPLAY,
) -> BarDataEnvelope[pd.DataFrame]:
    """Read daily bars from the local cache only.

    This is the page-read contract.  It never acquires a symbol lock, mutates
    freshness metadata, falls back to a provider, or retries a failed source.
    A missing range is represented by an unavailable envelope so callers can
    render an honest cold-start state in bounded time.
    """

    # The public reader is strict even when invoked outside FastAPI (for
    # example from a report exporter or a contract test).  HTTP middleware is
    # a second line of defence, not the only thing preventing a future helper
    # from turning this method into a provider request or a schema bootstrap.
    with local_only_data_access():
        resolved_store = (
            _local_read_store(store, "1d")
            if store is not None else _default_read_bar_store()
        )
        cached = resolved_store.get(symbol)
        frame = _cached_slice(cached, start, end)
        if frame is None:
            frame = pd.DataFrame(columns=OHLCV_COLUMNS)
        return _bar_envelope(
            frame,
            symbol=symbol,
            start=start,
            end=end,
            store=resolved_store,
            frequency="1d",
            purpose=purpose,
        )


def read_bars(
    symbol: str,
    start: str,
    end: str,
    frequency: str = "1d",
    *,
    store: BarStore | IntradayBarStore | None = None,
) -> BarDataEnvelope[pd.DataFrame]:
    """Read daily or intraday bars locally, without an implicit refresh."""

    with local_only_data_access():
        normalized = validate_frequency(frequency)
        if normalized == "1d":
            return read_history(symbol, start, end, store=cast(BarStore | None, store))
        resolved_store = (
            cast(IntradayBarStore, _local_read_store(cast(IntradayBarStore, store), normalized))
            if store is not None
            else IntradayBarStore(normalized, read_only=True)
        )
        cached = resolved_store.get(symbol)
        start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
        if len(str(end).strip()) <= 10:
            end_ts = end_ts + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
        frame = pd.DataFrame(columns=OHLCV_COLUMNS)
        if cached is not None and not cached.empty:
            frame = cached.loc[start_ts:end_ts]
        return _bar_envelope(
            frame,
            symbol=symbol,
            start=start,
            end=end,
            store=resolved_store,
            frequency=normalized,
        )


def _load_intraday_frame(
    symbol: str,
    start: str,
    end: str,
    frequency: str = "5m",
    use_cache: bool = True,
    store: IntradayBarStore | None = None,
    *,
    priority: str = "normal",
) -> pd.DataFrame:
    """加载分钟线并持久化。

    分钟数据按 ``symbol + frequency`` 独立保存；联网失败时仍会返回本地已有区间。
    AKShare 的 1 分钟历史通常仅覆盖最近 5 个交易日，因此长期研究应每日增量归档。
    """
    frequency = validate_frequency(frequency)
    if frequency == "1d":
        return _load_history_frame(symbol, start, end, use_cache=use_cache, priority=priority)
    store = store or IntradayBarStore(frequency)

    def finish(frame: pd.DataFrame) -> pd.DataFrame:
        return frame

    start_is_date = len(str(start).strip()) <= 10
    end_is_date = len(str(end).strip()) <= 10
    start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
    requested_end_date = end_ts.normalize()
    if end_is_date:
        end_ts = end_ts + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
    fetch_start = start_ts.strftime("%Y-%m-%d %H:%M:%S")
    fetch_end = end_ts.strftime("%Y-%m-%d %H:%M:%S")
    cached = store.get(symbol)
    if use_cache and cached is not None and not cached.empty:
        covers_end = (
            cached.index.max().normalize() >= requested_end_date
            if end_is_date
            else cached.index.max() >= end_ts
        )
        covers_start = (
            cached.index.min().normalize() <= start_ts.normalize()
            if start_is_date
            else cached.index.min() <= start_ts
        )
        covers = covers_start and covers_end
        fresh = store.freshness(symbol)
        if end_is_date and requested_end_date >= pd.Timestamp(market_date()):
            covers = covers and fresh is not None and (fresh < get_config().data.intraday_cache_minutes * 60)
        if covers:
            sliced = cached.loc[start_ts:end_ts]
            if not sliced.empty:
                return finish(sliced)

    market = guess_market(symbol)
    errors = []
    for factory in _request_factories(priority=priority, allow_online=True).get(market, []):
        try:
            source = factory()
            if not source.supports_capability(DataCapability.INTRADAY):
                continue
            with data_priority(priority):
                df = source.intraday(symbol, fetch_start, fetch_end, frequency)
            if df is not None and not df.empty:
                duplicate_rows = int(pd.DatetimeIndex(df.index).duplicated(keep=False).sum())
                if duplicate_rows:
                    errors.append(
                        f"{factory.__name__}: 存在 {duplicate_rows} 行重复分钟时间戳"
                    )
                    continue
                missing_columns = [column for column in OHLCV_COLUMNS if column not in df]
                if missing_columns:
                    errors.append(
                        f"{factory.__name__}: 缺少必需列 {','.join(missing_columns)}"
                    )
                    continue
                numeric = df[OHLCV_COLUMNS].apply(pd.to_numeric, errors="coerce")
                if int((~numeric.map(math.isfinite)).any(axis=1).sum()):
                    errors.append(f"{factory.__name__}: 存在非有限 OHLCV")
                    continue
                # 分钟线不采用日线的整段前复权替换语义：免费接口回溯有限，
                # 每日归档必须合并才能形成可长期复用的本地历史。
                candidate = (
                    df if cached is None or cached.empty
                    else pd.concat((cached, df)).sort_index()
                )
                candidate = candidate[~candidate.index.duplicated(keep="last")]
                quality = _assess_intraday_frame(
                    df,
                    fetch_start,
                    fetch_end,
                    symbol=symbol,
                    frequency=frequency,
                    source=source.name,
                )
                if quality.status == "unavailable":
                    errors.append(
                        f"{factory.__name__}: " + "；".join(quality.issues)
                    )
                    continue
                store.put(
                    symbol,
                    candidate,
                    replace=True,
                    request_start=fetch_start,
                    request_end=fetch_end,
                    source=source.name,
                    quality=quality.to_dict(),
                )
                stored = store.get(symbol)
                if stored is None:
                    raise RuntimeError(f"{source.name} 分钟线写入后无法读取")
                return finish(stored.loc[start_ts:end_ts])
            errors.append(f"{factory.__name__}: 返回空数据")
        except Exception as e:
            errors.append(f"{factory.__name__}: {e}")
            logger.debug("数据源 %s 获取 %s %s 失败: %s", factory.__name__, symbol, frequency, e)
    if cached is not None and not cached.empty:
        logger.debug("全部分钟数据源失败，使用本地缓存: %s %s", symbol, frequency)
        store.mark_status(symbol, "stale")
        return finish(cached.loc[start_ts:end_ts])
    raise _unavailable_error(
        (symbol,), start, end, errors or ["没有可用分钟数据"], frequency=frequency,
    )


def refresh_intraday(
    symbol: str,
    start: str,
    end: str,
    frequency: str = "5m",
    use_cache: bool = True,
    store: IntradayBarStore | None = None,
    *,
    work_class: str = "normal",
) -> BarDataEnvelope[pd.DataFrame]:
    """Refresh minute bars in a worker context with explicit evidence."""
    normalized = validate_frequency(frequency)
    if normalized == "1d":
        return refresh_history(
            symbol, start, end, use_cache=use_cache, work_class=work_class,
        )
    resolved_store = store or IntradayBarStore(normalized)
    frame = _load_intraday_frame(
        symbol,
        start,
        end,
        normalized,
        use_cache=use_cache,
        store=resolved_store,
        priority=work_class,
    )
    envelope = _bar_envelope(
        frame,
        symbol=symbol,
        start=start,
        end=end,
        store=resolved_store,
        frequency=normalized,
    )
    return envelope


def refresh_bars(
    symbol: str,
    start: str,
    end: str,
    frequency: str = "1d",
    use_cache: bool = True,
    *,
    mode: RefreshMode | str | None = None,
    work_class: str = "normal",
) -> BarDataEnvelope[pd.DataFrame]:
    """Refresh daily or minute bars; never call this from a page read."""
    frequency = validate_frequency(frequency)
    if frequency == "1d":
        return refresh_history(symbol, start, end, use_cache=use_cache, mode=mode, work_class=work_class)
    return refresh_intraday(symbol, start, end, frequency, use_cache=use_cache, work_class=work_class)


def _load_bar_panel_frame(
    symbols: list[str],
    start: str,
    end: str,
    frequency: str = "1d",
    field: str | None = None,
    use_cache: bool = True,
    progress: Callable[[int, int, str, bool], None] | None = None,
    *,
    refresh: RefreshMode | str | None = None,
    priority: str = "normal",
    max_workers: int = 8,
    provider: str = "",
) -> pd.DataFrame | dict[str, pd.DataFrame]:
    """加载日线或分钟线多标的面板数据。

    field=None 时返回 {字段: DataFrame(date × symbol)} 的字典（open/high/low/close/volume...），
    指定 field（如 "close"）时直接返回该字段的 DataFrame。
    """
    frequency = validate_frequency(frequency)
    daily_store = _default_bar_store() if frequency == "1d" else None
    intraday_store = IntradayBarStore(frequency) if frequency != "1d" else None
    frames: dict[str, pd.DataFrame] = {}
    failures: list[tuple[str, str]] = []
    total = len(symbols)
    batch_store: BarRefreshBatchStore | None = None
    batch_id = ""
    attempt_symbols = tuple(symbols)
    if symbols:
        active_store = daily_store if daily_store is not None else intraday_store
        assert active_store is not None
        batch_root = active_store.root
        batch_store = BarRefreshBatchStore(batch_root)
        batch_id, attempt_symbols, resumed = batch_store.begin_or_resume(
            symbols,
            start,
            end,
            frequency=frequency,
            provider=provider or get_config().data.primary_provider,
        )
        if resumed:
            # Already-published successes remain immediately usable; only
            # durable pending items consume workers/provider requests.
            local_store = daily_store if daily_store is not None else intraday_store
            assert local_store is not None
            for symbol in symbols:
                if symbol in attempt_symbols:
                    continue
                cached = local_store.get(symbol)
                sliced = _cached_slice(cached, start, end)
                if sliced is not None:
                    frames[symbol] = sliced

    def notify_progress(completed: int, symbol: str, success: bool) -> None:
        if progress is None:
            return
        try:
            progress(completed, total, symbol, success)
        except Exception as exc:
            logger.warning("行情进度回调失败（不影响数据加载）: %s", exc)

    # A fresh local database can satisfy an uncached A-share universe in one SDK
    # round trip. Existing/partial caches still use the normal per-symbol merge
    # path so adjustment alignment and coverage checks remain authoritative.
    if (
        frequency == "1d"
        and daily_store is not None
        and symbols
        and not provider
        and get_config().data.primary_provider == "free-stockdb"
    ):
        batch_symbols = [
            symbol
            for symbol in attempt_symbols
            if guess_market(symbol) == Market.CN
            and ((cached := daily_store.get(symbol)) is None or cached.empty)
        ]
        if batch_symbols:
            try:
                from quantmaster.data.free_stockdb_source import FreeStockDBSource

                source = FreeStockDBSource()
                if source.native_batch_available():
                    with data_priority(priority):
                        batch = source.daily_many(batch_symbols, start, end)

                    def prepare_stockdb_batch(
                        symbol: str,
                    ) -> tuple[str, pd.DataFrame] | None:
                        frame = batch.get(symbol)
                        if (
                            frame is None
                            or frame.empty
                            or not _is_complete_refresh(
                                frame,
                                None,
                                start,
                                end,
                                symbol=symbol,
                            )
                        ):
                            return None
                        source_name = source.name
                        quality = _assess_daily_frame(
                            frame,
                            start,
                            end,
                            symbol=symbol,
                            source=source.name,
                        )
                        # A batch from the local StockDB is still useful as a
                        # preview even before its immutable acceptance manifest
                        # has cross-source evidence.  Do not fan out into one
                        # remote fallback per symbol merely to upgrade it.
                        if quality.status == "unavailable":
                            return None
                        with daily_store.lock(symbol):
                            daily_store.put(
                                symbol,
                                frame,
                                replace=True,
                                request_start=start,
                                request_end=end,
                                source=source_name,
                                quality=quality.to_dict(),
                                replace_coverage=True,
                            )
                        return symbol, frame.loc[start:end]

                    batch_workers = min(8, max(1, len(batch_symbols)))
                    with ThreadPoolExecutor(
                        max_workers=batch_workers,
                        thread_name_prefix="stockdb-contract",
                    ) as batch_executor:
                        batch_futures = {
                            batch_executor.submit(prepare_stockdb_batch, symbol): symbol
                            for symbol in batch_symbols
                        }
                        for batch_future in as_completed(batch_futures):
                            symbol = batch_futures[batch_future]
                            try:
                                result = batch_future.result()
                            except (
                                AdjustmentMismatch,
                                httpx.HTTPError,
                                ImportError,
                                OSError,
                                RuntimeError,
                                TypeError,
                                ValueError,
                            ):
                                logger.debug(
                                    "%s free-stockdb 批量行情验收失败，回退逐标的加载",
                                    symbol,
                                    exc_info=True,
                                )
                                continue
                            if result is None:
                                continue
                            accepted_symbol, accepted_frame = result
                            frames[accepted_symbol] = accepted_frame
                            if batch_store is not None:
                                batch_store.record_success(batch_id, accepted_symbol)
                            notify_progress(len(frames), accepted_symbol, True)
            except (httpx.HTTPError, ImportError, OSError, RuntimeError, TypeError, ValueError):
                logger.debug("free-stockdb 批量预取失败，回退逐标的加载", exc_info=True)

    def one(symbol: str) -> pd.DataFrame:
        if frequency == "1d":
            assert daily_store is not None
            return _load_history_frame(
                symbol,
                start,
                end,
                use_cache=use_cache,
                store=daily_store,
                refresh=refresh,
                priority=priority,
                provider=provider,
            )
        assert intraday_store is not None
        return _load_intraday_frame(
            symbol,
            start,
            end,
            frequency=frequency,
            use_cache=use_cache,
            store=intraday_store,
            priority=priority,
        )

    pending = [symbol for symbol in attempt_symbols if symbol not in frames]
    workers = min(max(1, int(max_workers)), 8, max(1, len(pending)))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="bar-panel") as executor:
        futures = {executor.submit(one, symbol): symbol for symbol in pending}
        for completed, panel_future in enumerate(
            as_completed(futures),
            start=len(frames) + 1,
        ):
            symbol = futures[panel_future]
            success = False
            try:
                frame = panel_future.result()
                if frame is not None and not frame.empty:
                    frames[symbol] = frame
                    success = True
            except Exception as exc:
                failures.append((symbol, str(exc)))
                if batch_store is not None:
                    batch_store.record_failure(
                        batch_id, symbol, str(exc), type(exc).__name__.casefold(),
                    )
            else:
                if batch_store is not None:
                    if success:
                        batch_store.record_success(batch_id, symbol)
                    else:
                        batch_store.record_failure(
                            batch_id, symbol, "返回结果未包含可用行情", "empty_or_missing",
                        )
            notify_progress(completed, symbol, success)
    if failures:
        samples = "；".join(f"{symbol}: {error}" for symbol, error in failures[:5])
        logger.warning(
            "行情批量加载失败 %s/%s 个标的（样本：%s）",
            len(failures),
            total,
            samples,
        )
    if not frames:
        raise _unavailable_error(
            tuple(symbols), start, end,
            [
                *(f"{symbol}: {error}" for symbol, error in failures),
                *(() if failures else ("没有任何标的成功加载数据",)),
            ],
            frequency=frequency,
        )

    frames = {symbol: frames[symbol] for symbol in symbols if symbol in frames}
    fields = sorted({c for df in frames.values() for c in df.columns})
    panel = {
        f: pd.DataFrame({s: df[f] for s, df in frames.items() if f in df.columns}).sort_index()
        for f in fields
    }
    if field is not None:
        return panel[field]
    return panel


def refresh_bar_panel(
    symbols: list[str],
    start: str,
    end: str,
    frequency: str = "1d",
    field: str | None = None,
    use_cache: bool = True,
    progress: Callable[[int, int, str, bool], None] | None = None,
    *,
    mode: RefreshMode | str | None = None,
    work_class: str = "normal",
    concurrency: int = 8,
    source_name: str = "",
    purpose: CachePurpose | str = CachePurpose.CURRENT_ANALYSIS,
) -> BarDataEnvelope[pd.DataFrame | dict[str, pd.DataFrame]]:
    """Refresh a panel without silently shrinking the requested universe."""
    normalized = validate_frequency(frequency)
    requested = tuple(dict.fromkeys(str(symbol).upper() for symbol in symbols))
    data = _load_bar_panel_frame(
        list(requested),
        start,
        end,
        frequency=normalized,
        field=field,
        use_cache=use_cache,
        progress=progress,
        refresh=mode,
        priority=work_class,
        max_workers=concurrency,
        provider=source_name,
    )
    if isinstance(data, pd.DataFrame):
        observed = tuple(str(value) for value in data.columns)
    else:
        observed_set = {
            str(value)
            for frame in data.values()
            for value in frame.columns
        }
        observed = tuple(symbol for symbol in requested if symbol in observed_set)
    missing = tuple(symbol for symbol in requested if symbol not in observed)
    store: BarStore = _default_bar_store() if normalized == "1d" else IntradayBarStore(normalized)
    sources = tuple(dict.fromkeys(
        str((store.metadata(symbol) or {}).get("last_source") or "local-cache")
        for symbol in observed
    ))
    issues: list[str] = []
    provenance: list[dict[str, object]] = []
    batch_summary = BarRefreshBatchStore(store.root).latest_exact(
        requested,
        start,
        end,
        frequency=normalized,
        provider=source_name or get_config().data.primary_provider,
    )
    if batch_summary:
        provenance.append({"refresh_batch": batch_summary})
    per_symbol: list[BarDataQuality] = []
    for symbol in observed:
        cached = store.get(symbol)
        sliced = pd.DataFrame() if cached is None else cached.loc[start:end]
        envelope = _bar_envelope(
            sliced,
            symbol=symbol,
            start=start,
            end=end,
            store=store,
            frequency=normalized,
            purpose=purpose,
        )
        per_symbol.append(envelope.quality)
        issues.extend(f"{symbol}: {item}" for item in envelope.quality.issues)
        provenance.append({"symbol": symbol, "quality": envelope.quality.to_dict()})
        provenance.extend({"symbol": symbol, **item} for item in envelope.provenance)
    if missing:
        issues.append("缺少请求标的：" + "、".join(missing[:20]))
    stale = any(item.stale for item in per_symbol)
    observed_start = ""
    observed_end = ""
    starts = [pd.Timestamp(item.observed_start) for item in per_symbol if item.observed_start]
    ends = [pd.Timestamp(item.observed_end) for item in per_symbol if item.observed_end]
    if starts:
        observed_start = max(starts).isoformat()
    if ends:
        observed_end = min(ends).isoformat()
    symbol_ratio = len(observed) / len(requested) if requested else 1.0
    date_ratios = [item.coverage_ratio for item in per_symbol if item.coverage_ratio is not None]
    ratio = min([symbol_ratio, *date_ratios]) if date_ratios else symbol_ratio
    rank = _QUALITY_RANK
    worst: QualityStatus = max(
        (item.status for item in per_symbol),
        key=lambda value: rank[value],
        default="verified",
    )
    status: QualityStatus
    if symbol_ratio < 0.90:
        status = "unavailable"
        issues.append(f"请求标的覆盖率仅 {symbol_ratio:.1%}，低于 90% 计算门禁")
    else:
        status = "degraded" if missing and worst == "verified" else worst
    calendars = tuple(dict.fromkeys(item.calendar_source for item in per_symbol))
    unit_values = tuple(dict.fromkeys(item.units for item in per_symbol))
    units = unit_values[0] if len(unit_values) == 1 else BarDataQuality(
        "degraded", start, end,
    ).units
    if len(unit_values) > 1:
        issues.append("面板包含多种单位；逐标的契约见 provenance.quality")
        if status == "verified":
            status = "degraded"
    timezones = tuple(dict.fromkeys(item.timezone for item in per_symbol))
    adjustments = tuple(dict.fromkeys(item.adjustment for item in per_symbol))
    quality = BarDataQuality(
        status,
        start,
        end,
        observed_start=observed_start,
        observed_end=observed_end,
        coverage_ratio=round(ratio, 6),
        calendar_source=calendars[0] if len(calendars) == 1 else "mixed:per-symbol-quality",
        sources=sources,
        issues=tuple(issues),
        stale=stale,
        partial=bool(missing) or any(item.partial for item in per_symbol),
        timezone=timezones[0] if len(timezones) == 1 else "mixed",
        adjustment=adjustments[0] if len(adjustments) == 1 else "mixed",
        units=units,
        duplicate_rows=sum(item.duplicate_rows for item in per_symbol),
        requested_symbols=requested,
        observed_symbols=observed,
        missing_symbols=missing,
        freshness_state=(
            "stale" if stale
            else "fresh" if per_symbol and all(item.freshness_state == "fresh" for item in per_symbol)
            else "unknown"
        ),
        age_seconds=max(
            (item.age_seconds for item in per_symbol if item.age_seconds is not None),
            default=None,
        ),
        stale_while_revalidate=any(item.stale_while_revalidate for item in per_symbol),
        refresh_reason="；".join(dict.fromkeys(
            item.refresh_reason for item in per_symbol if item.refresh_reason
        )),
        expected_session=max(
            (item.expected_session for item in per_symbol if item.expected_session),
            default="",
        ),
        future_rows=sum(item.future_rows for item in per_symbol),
    )
    return BarDataEnvelope(data, quality, tuple(provenance))


def refresh_panel(
    symbols: list[str],
    start: str,
    end: str,
    field: str | None = None,
    use_cache: bool = True,
    progress: Callable[[int, int, str, bool], None] | None = None,
    *,
    mode: RefreshMode | str | None = None,
    work_class: str = "normal",
    concurrency: int = 8,
    source_name: str = "",
    purpose: CachePurpose | str = CachePurpose.CURRENT_ANALYSIS,
) -> BarDataEnvelope[pd.DataFrame | dict[str, pd.DataFrame]]:
    """Refresh a daily panel; page handlers must use :func:`read_panel`."""
    return refresh_bar_panel(
        symbols,
        start,
        end,
        frequency="1d",
        field=field,
        use_cache=use_cache,
        progress=progress,
        mode=mode,
        work_class=work_class,
        concurrency=concurrency,
        source_name=source_name,
        purpose=purpose,
    )


def read_panel(
    symbols: list[str],
    start: str,
    end: str,
    field: str | None = None,
    *,
    store: BarStore | None = None,
    progress: Callable[[int, int, str, bool], None] | None = None,
    purpose: CachePurpose | str = CachePurpose.DISPLAY,
) -> BarDataEnvelope[pd.DataFrame | dict[str, pd.DataFrame]]:
    """Compose a daily panel from local bar files only.

    The bounded local batch reader never contacts a provider.  Callers that
    need a full cache rebuild must submit a refresh job instead.
    """

    resolved_store = _local_read_store(store, "1d") if store is not None else _default_read_bar_store()
    requested = tuple(dict.fromkeys(str(value).upper() for value in symbols))
    frames: dict[str, pd.DataFrame] = {}
    envelopes: dict[str, BarDataEnvelope[pd.DataFrame]] = {}
    with local_only_data_access():
        batch = resolved_store.read_many(
            list(requested),
            start=start,
            end=end,
            max_workers=8,
            enqueue_repair=False,
        )
        metadata_by_symbol = resolved_store.metadata_many(list(requested))
        for index, symbol in enumerate(requested, start=1):
            frame = batch.frames.get(symbol)
            if frame is None:
                frame = pd.DataFrame(columns=OHLCV_COLUMNS)
            envelope = _bar_envelope(
                frame,
                symbol=symbol,
                start=start,
                end=end,
                store=resolved_store,
                frequency="1d",
                metadata=metadata_by_symbol.get(symbol, {}),
                purpose=purpose,
            )
            envelopes[symbol] = envelope
            if not envelope.data.empty:
                frames[symbol] = envelope.data
            if progress is not None:
                try:
                    progress(index, len(requested), symbol, not envelope.data.empty)
                except Exception:
                    logger.debug("本地面板进度回调失败", exc_info=True)

    fields = sorted({column for frame in frames.values() for column in frame.columns})
    panels = {
        column: pd.DataFrame(
            {symbol: frame[column] for symbol, frame in frames.items() if column in frame},
        ).sort_index()
        for column in fields
    }
    data: pd.DataFrame | dict[str, pd.DataFrame]
    data = panels.get(field, pd.DataFrame()) if field is not None else panels
    observed = tuple(symbol for symbol in requested if symbol in frames)
    missing = tuple(symbol for symbol in requested if symbol not in frames)
    qualities = [envelopes[symbol].quality for symbol in requested]
    issues = [
        f"{symbol}: {issue}"
        for symbol, envelope in envelopes.items()
        for issue in envelope.quality.issues
    ]
    if missing:
        issues.append("缺少请求标的：" + "、".join(missing[:20]))
    stale = any(item.stale for item in qualities)
    partial = bool(missing) or any(item.partial for item in qualities)
    if not observed:
        status: QualityStatus = "unavailable"
    elif any(item.status == "unavailable" for item in qualities):
        # Missing symbols are already captured above.  Existing local symbols
        # with invalid evidence keep the panel visible but explicitly degraded.
        status = "degraded"
    elif any(item.status == "degraded" for item in qualities) or partial or stale:
        status = "degraded"
    else:
        status = "verified"
    coverage = len(observed) / len(requested) if requested else 1.0
    sources = tuple(dict.fromkeys(
        source for item in qualities for source in item.sources
    )) or ("local-cache",)
    starts = [item.observed_start for item in qualities if item.observed_start]
    ends = [item.observed_end for item in qualities if item.observed_end]
    quality = BarDataQuality(
        status,
        start,
        end,
        observed_start=max(starts) if starts else "",
        observed_end=min(ends) if ends else "",
        coverage_ratio=round(coverage, 6),
        calendar_source="local-cache",
        sources=sources,
        issues=tuple(dict.fromkeys(issues)),
        stale=stale,
        partial=partial,
        timezone="Asia/Shanghai",
        adjustment="per-symbol-local-contract",
        requested_symbols=requested,
        observed_symbols=observed,
        missing_symbols=missing,
        freshness_state=(
            "stale" if stale
            else "fresh" if qualities and all(item.freshness_state == "fresh" for item in qualities)
            else "unknown"
        ),
        age_seconds=max(
            (item.age_seconds for item in qualities if item.age_seconds is not None),
            default=None,
        ),
        stale_while_revalidate=any(item.stale_while_revalidate for item in qualities),
        refresh_reason="；".join(dict.fromkeys(
            item.refresh_reason for item in qualities if item.refresh_reason
        )),
        expected_session=max(
            (item.expected_session for item in qualities if item.expected_session),
            default="",
        ),
        future_rows=sum(item.future_rows for item in qualities),
    )
    provenance = tuple(
        {"symbol": symbol, "quality": envelope.quality.to_dict(), "read_mode": "local_only"}
        for symbol, envelope in envelopes.items()
    )
    return BarDataEnvelope(data, quality, provenance)


register_history_refresh(refresh_history)
register_frame_quality(_assess_daily_frame, _covers_requested_range)
