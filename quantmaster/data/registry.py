"""数据源注册与统一入口：按市场路由，带本地缓存与自动降级。

用法：
    from quantmaster.data import load_history, load_panel
    df = load_history("600519.SH", "2022-01-01", "2024-12-31")
    panel = load_panel(["600519.SH", "000858.SZ"], "2022-01-01", "2024-12-31")
"""

from __future__ import annotations

import logging
import math
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import StrEnum

import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.base import (
    DataCapability,
    DataSource,
    Market,
    guess_market,
    validate_frequency,
)
from quantmaster.data.resilience import bypass_endpoint_cache, data_priority
from quantmaster.data.storage import BarStore, IntradayBarStore

logger = logging.getLogger(__name__)

# 各市场按优先级排列的数据源工厂
_SOURCE_FACTORIES: dict[str, list] = {}


class RefreshMode(StrEnum):
    AUTO = "auto"
    INCREMENTAL = "incremental"
    FULL = "full"


class AdjustmentMismatch(RuntimeError):
    pass


def _covers_requested_range(df: pd.DataFrame, start: str, end: str) -> bool:
    """判断响应自身是否连续；上市前和退市后的自然空白不算缺行。"""
    if df is None or df.empty:
        return False
    first = pd.Timestamp(df.index.min()).normalize()
    last = pd.Timestamp(df.index.max()).normalize()
    # 只在实际有数据的边界内判断密度。请求起点早于上市日、终点晚于退市日
    # 都是合法情况；已有缓存日期是否丢失由 _is_complete_refresh 单独校验。
    expected = len(pd.bdate_range(first, last))
    if expected < 10:
        return True
    index = pd.DatetimeIndex(df.index).normalize()
    actual = index[(index >= first) & (index <= last)].nunique()
    return actual / expected >= 0.8


def _is_complete_refresh(
    fresh: pd.DataFrame, cached: pd.DataFrame | None, start: str, end: str,
) -> bool:
    """只有确认新响应完整时，才允许覆盖前复权日线缓存。

    AKShare 偶尔会返回有数据但缺头、缺尾或缺中间分块的响应。边界覆盖之外，
    还要确保旧缓存中已经确认存在的交易日没有在新响应里消失。
    """
    if not _covers_requested_range(fresh, start, end):
        return False
    if cached is None or cached.empty:
        return True
    fresh_index = pd.DatetimeIndex(fresh.index).normalize()
    cached_index = pd.DatetimeIndex(cached.index).normalize()
    known = cached_index[
        (cached_index >= pd.Timestamp(start).normalize())
        & (cached_index <= pd.Timestamp(end).normalize())
    ]
    return known.difference(fresh_index).empty


def _mode(use_cache: bool, refresh: RefreshMode | str | None) -> RefreshMode:
    if refresh is None:
        return RefreshMode.AUTO if use_cache else RefreshMode.FULL
    return refresh if isinstance(refresh, RefreshMode) else RefreshMode(refresh)


def _cached_slice(cached: pd.DataFrame | None, start: str, end: str) -> pd.DataFrame | None:
    if cached is None or cached.empty:
        return None
    result = cached.loc[start:end]
    return result if not result.empty else None


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
    ratios = (fresh.loc[common, "close"] / cached.loc[common, "close"]).replace(
        [float("inf"), float("-inf")], pd.NA).dropna()
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
    ohlc = [column for column in ("open", "high", "low", "close")
            if column in cached.columns and column in fresh.columns]
    if direction == "left":
        aligned = fresh.copy()
        aligned[ohlc] = aligned[ohlc] / ratio
        merged = pd.concat([aligned, cached])
    else:
        aligned = cached.copy()
        aligned[ohlc] = aligned[ohlc] * ratio
        merged = pd.concat([aligned, fresh])
    return merged[~merged.index.duplicated(keep="last")].sort_index()


def _full_refresh(
    symbol: str,
    start: str,
    end: str,
    cached: pd.DataFrame | None,
    store: BarStore,
    priority: str,
) -> pd.DataFrame:
    market = guess_market(symbol)
    errors: list[str] = []
    for factory in _factories().get(market, []):
        try:
            source = factory()
            with data_priority(priority), bypass_endpoint_cache():
                frame = source.daily(symbol, start, end)
            if frame is None or frame.empty:
                errors.append(f"{factory.__name__}: 返回空数据")
                continue
            if not _is_complete_refresh(frame, cached, start, end):
                errors.append(f"{factory.__name__}: 响应缺失已有交易日或内部过于稀疏")
                continue
            store.put(symbol, frame, replace=True)
            store.mark_checked(
                symbol, start, end, source=source.name, replace_coverage=True)
            stored = store.get(symbol)
            if stored is None:
                raise RuntimeError(f"{source.name} 日线写入后无法读取")
            return stored.loc[start:end]
        except (ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
            errors.append(f"{factory.__name__}: {exc}")
            logger.debug("数据源 %s 全量获取 %s 失败: %s", factory.__name__, symbol, exc)
    if cached is not None and not cached.empty:
        store.mark_status(symbol, "refresh_failed")
        logger.debug("全量刷新失败，保留本地缓存: %s", symbol)
        return cached.loc[start:end]
    raise RuntimeError(f"获取 {symbol} 日线失败: {errors}")


def _fetch_segment(
    symbol: str,
    start: str,
    end: str,
    direction: str,
    cached: pd.DataFrame | None,
    store: BarStore,
    priority: str,
    refresh_provider_cache: bool = False,
) -> tuple[pd.DataFrame | None, list[str], bool]:
    market = guess_market(symbol)
    errors: list[str] = []
    cached_latest = (
        pd.Timestamp(cached.index.max()).normalize()
        if cached is not None and not cached.empty else None
    )
    prefer_extension = (
        direction == "right"
        and cached_latest is not None
        and cached_latest < pd.Timestamp(end).normalize()
    )
    best: tuple[DataSource, pd.DataFrame, pd.Timestamp] | None = None

    def save(source: DataSource, merged: pd.DataFrame) -> tuple[pd.DataFrame, list[str], bool]:
        store.put(symbol, merged, replace=True)
        store.mark_checked(symbol, start, end, source=source.name)
        return store.get(symbol), errors, True

    for factory in _factories().get(market, []):
        try:
            source = factory()
            with data_priority(priority), bypass_endpoint_cache(refresh_provider_cache):
                frame = source.daily(symbol, start, end)
            if frame is None or frame.empty:
                errors.append(f"{factory.__name__}: 返回空数据")
                continue
            if not _covers_requested_range(frame, start, end):
                errors.append(f"{factory.__name__}: 响应内部过于稀疏")
                continue
            merged = frame if cached is None or cached.empty else _align_increment(
                cached, frame, direction)
            fresh_latest = pd.Timestamp(frame.index.max()).normalize()
            if prefer_extension and cached_latest is not None and fresh_latest <= cached_latest:
                errors.append(
                    f"{factory.__name__}: 未返回 {cached_latest.date()} 之后的新行情")
                if best is None or fresh_latest > best[2]:
                    best = (source, merged, fresh_latest)
                continue
            return save(source, merged)
        except AdjustmentMismatch as exc:
            errors.append(f"{factory.__name__}: {exc}")
        except Exception as exc:
            errors.append(f"{factory.__name__}: {exc}")
            logger.debug("数据源 %s 增量获取 %s 失败: %s", factory.__name__, symbol, exc)
    if best is not None:
        # 周末、休市或所有上游尚未发布时，保留最新的有效响应。
        saved, collected_errors, _ = save(best[0], best[1])
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
    current = now if now is not None else pd.Timestamp.now(tz="Asia/Shanghai")
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
    from quantmaster.data.free_stockdb_source import FreeStockDBSource
    from quantmaster.data.tushare_source import TushareSource
    from quantmaster.data.yfinance_source import YFinanceSource

    ak, free, yf, tu = AkshareSource, FreeStockDBSource, YFinanceSource, TushareSource
    orders = {
        "free-stockdb": [free, ak, tu],
        "akshare": [ak, tu, free],
        "tushare": [tu, ak, free],
    }
    selected = str(get_config().data.primary_provider).strip().lower()
    cn = orders.get(selected, orders["free-stockdb"])
    return {
        Market.CN: cn,
        Market.HK: [ak, yf],
        Market.US: [yf],
        Market.JP: [yf],
        Market.KR: [yf],
        Market.FUTURES: [ak, yf],
        Market.INDEX: [tu, ak, yf],
    }


def get_source(
    market: Market,
    capability: DataCapability | str = DataCapability.DAILY,
) -> DataSource:
    """返回该市场第一个声明所需能力且可以初始化的数据源。"""
    required = (
        capability
        if isinstance(capability, DataCapability)
        else DataCapability(str(capability))
    )
    errors = []
    for factory in _factories().get(market, []):
        try:
            source = factory()
            if source.supports_capability(required):
                return source
        except Exception as e:  # pragma: no cover - 依赖安装情况
            errors.append(f"{factory.__name__}: {e}")
    raise RuntimeError(
        f"市场 {market.value} 没有支持 {required.value} 的可用数据源: {errors}"
    )


def load_spot(symbols: list[str]) -> pd.DataFrame:
    """加载 A 股快照；按设置主源优先，并用后续来源补齐缺失标的。"""
    requested = list(dict.fromkeys(str(symbol).upper() for symbol in symbols))
    by_code = {symbol.partition(".")[0].zfill(6): symbol for symbol in requested}
    rows: dict[str, dict] = {}
    errors: list[str] = []
    for factory in _factories().get(Market.CN, []):
        missing = [symbol for code, symbol in by_code.items() if code not in rows]
        if not missing:
            break
        try:
            source = factory()
            if not source.supports_capability(DataCapability.SPOT):
                continue
            snapshot = source.spot(missing)
            for _, value in snapshot.iterrows():
                code = str(value.get("code") or "").zfill(6)
                if code in by_code and code not in rows:
                    item = value.to_dict()
                    item["code"] = code
                    item["source"] = source.name
                    rows[code] = item
        except Exception as exc:
            errors.append(f"{factory.__name__}: {exc}")
            logger.debug("快照数据源 %s 失败: %s", factory.__name__, exc)
    if not rows:
        raise RuntimeError(f"实时快照不可用: {errors or ['没有可用数据']}")
    return pd.DataFrame(rows.values()).reset_index(drop=True)


def _load_history_locked(
    symbol: str,
    start: str,
    end: str,
    use_cache: bool = True,
    store: BarStore | None = None,
    *,
    refresh: RefreshMode | str | None = None,
    priority: str = "normal",
) -> pd.DataFrame:
    """已持有单标的锁时加载标准化日线。"""
    store = store or BarStore()
    cfg = get_config()
    cached = store.get(symbol)
    mode = _mode(use_cache, refresh)
    if mode == RefreshMode.FULL:
        fetch_start, fetch_end = start, end
        if cached is not None and not cached.empty:
            fetch_start = min(start, str(cached.index.min().date()))
            fetch_end = max(end, str(cached.index.max().date()))
        return _full_refresh(
            symbol, fetch_start, fetch_end, cached, store, priority).loc[start:end]

    meta = store.metadata(symbol) or {}
    requested_end = pd.Timestamp(end).normalize()
    near_current = requested_end >= pd.Timestamp.now().normalize() - pd.Timedelta(days=7)
    coverage_start = str(meta.get("coverage_start") or meta.get("start") or "")
    coverage_end = str(meta.get("coverage_end") or meta.get("end") or "")
    covers_start = bool(coverage_start and coverage_start <= start)
    covers_end = bool(coverage_end and coverage_end >= end)
    checked = store.check_freshness(symbol)
    ttl_fresh = checked is not None and checked < cfg.data.cache_days * 86400
    session_refresh_due = _session_refresh_due(
        symbol, requested_end, cached, float(meta.get("checked_at") or 0))
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
        force_tail = near_current and (
            mode == RefreshMode.INCREMENTAL or not ttl_fresh)
        if not covers_end or force_tail:
            overlap_start = str(cached.index[max(0, len(cached) - 5)].date())
            segments.append((overlap_start, end, "right"))

    errors: list[str] = []
    all_segments_succeeded = True
    for fetch_start, fetch_end, direction in segments:
        cached, segment_errors, succeeded = _fetch_segment(
            symbol, fetch_start, fetch_end, direction, cached, store, priority,
            refresh_provider_cache=(mode == RefreshMode.INCREMENTAL or session_refresh_due),
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
    raise RuntimeError(f"获取 {symbol} 日线失败: {errors or ['没有可用数据']}")


def load_history(
    symbol: str,
    start: str,
    end: str,
    use_cache: bool = True,
    store: BarStore | None = None,
    *,
    refresh: RefreshMode | str | None = None,
    priority: str = "normal",
) -> pd.DataFrame:
    """加载标准化日线；普通请求只补边界增量，全量重拉必须显式指定。"""
    store = store or BarStore()
    with store.lock(symbol):
        return _load_history_locked(
            symbol, start, end, use_cache=use_cache, store=store,
            refresh=refresh, priority=priority,
        )


def load_intraday(
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
        return load_history(symbol, start, end, use_cache=use_cache, priority=priority)
    store = store or IntradayBarStore(frequency)
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
            if end_is_date else cached.index.max() >= end_ts
        )
        covers_start = (
            cached.index.min().normalize() <= start_ts.normalize()
            if start_is_date else cached.index.min() <= start_ts
        )
        covers = covers_start and covers_end
        fresh = store.freshness(symbol)
        if end_is_date and requested_end_date >= pd.Timestamp.now().normalize():
            covers = covers and fresh is not None and (
                fresh < get_config().data.intraday_cache_minutes * 60)
        if covers:
            sliced = cached.loc[start_ts:end_ts]
            if not sliced.empty:
                return sliced

    market = guess_market(symbol)
    errors = []
    for factory in _factories().get(market, []):
        try:
            source = factory()
            if not source.supports_capability(DataCapability.INTRADAY):
                continue
            with data_priority(priority):
                df = source.intraday(symbol, fetch_start, fetch_end, frequency)
            if df is not None and not df.empty:
                # 分钟线不采用日线的整段前复权替换语义：免费接口回溯有限，
                # 每日归档必须合并才能形成可长期复用的本地历史。
                store.put(symbol, df, replace=False)
                stored = store.get(symbol)
                if stored is None:
                    raise RuntimeError(f"{source.name} 分钟线写入后无法读取")
                return stored.loc[start_ts:end_ts]
            errors.append(f"{factory.__name__}: 返回空数据")
        except Exception as e:
            errors.append(f"{factory.__name__}: {e}")
            logger.debug("数据源 %s 获取 %s %s 失败: %s", factory.__name__, symbol, frequency, e)
    if cached is not None and not cached.empty:
        logger.debug("全部分钟数据源失败，使用本地缓存: %s %s", symbol, frequency)
        return cached.loc[start_ts:end_ts]
    raise RuntimeError(f"获取 {symbol} {frequency} 分钟线失败: {errors}")


def load_bars(
    symbol: str,
    start: str,
    end: str,
    frequency: str = "1d",
    use_cache: bool = True,
    *,
    refresh: RefreshMode | str | None = None,
    priority: str = "normal",
) -> pd.DataFrame:
    """日线/分钟线统一入口。"""
    frequency = validate_frequency(frequency)
    if frequency == "1d":
        return load_history(
            symbol, start, end, use_cache=use_cache, refresh=refresh, priority=priority)
    return load_intraday(
        symbol, start, end, frequency, use_cache=use_cache, priority=priority)


def load_bar_panel(
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
) -> pd.DataFrame | dict[str, pd.DataFrame]:
    """加载日线或分钟线多标的面板数据。

    field=None 时返回 {字段: DataFrame(date × symbol)} 的字典（open/high/low/close/volume...），
    指定 field（如 "close"）时直接返回该字段的 DataFrame。
    """
    frequency = validate_frequency(frequency)
    daily_store = BarStore() if frequency == "1d" else None
    intraday_store = IntradayBarStore(frequency) if frequency != "1d" else None
    frames: dict[str, pd.DataFrame] = {}
    failures: list[tuple[str, str]] = []
    total = len(symbols)

    def one(symbol: str) -> pd.DataFrame:
        if frequency == "1d":
            assert daily_store is not None
            return load_history(
                symbol, start, end, use_cache=use_cache, store=daily_store,
                refresh=refresh, priority=priority,
            )
        assert intraday_store is not None
        return load_intraday(
            symbol, start, end, frequency=frequency, use_cache=use_cache,
            store=intraday_store, priority=priority,
        )

    workers = min(max(1, int(max_workers)), 8, max(1, total))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="bar-panel") as executor:
        futures = {executor.submit(one, symbol): symbol for symbol in symbols}
        for completed, future in enumerate(as_completed(futures), start=1):
            symbol = futures[future]
            success = False
            try:
                frame = future.result()
                if frame is not None and not frame.empty:
                    frames[symbol] = frame
                    success = True
            except Exception as exc:
                failures.append((symbol, str(exc)))
            if progress:
                try:
                    progress(completed, total, symbol, success)
                except Exception as exc:
                    logger.warning("行情进度回调失败（不影响数据加载）: %s", exc)
    if failures:
        samples = "；".join(f"{symbol}: {error}" for symbol, error in failures[:5])
        logger.warning(
            "行情批量加载失败 %s/%s 个标的（样本：%s）",
            len(failures), total, samples,
        )
    if not frames:
        raise RuntimeError("没有任何标的成功加载数据")

    frames = {symbol: frames[symbol] for symbol in symbols if symbol in frames}
    fields = sorted({c for df in frames.values() for c in df.columns})
    panel = {
        f: pd.DataFrame({s: df[f] for s, df in frames.items() if f in df.columns}).sort_index()
        for f in fields
    }
    if field is not None:
        return panel[field]
    return panel


def load_panel(
    symbols: list[str],
    start: str,
    end: str,
    field: str | None = None,
    use_cache: bool = True,
    progress: Callable[[int, int, str, bool], None] | None = None,
    *,
    refresh: RefreshMode | str | None = None,
    priority: str = "normal",
    max_workers: int = 8,
) -> pd.DataFrame | dict[str, pd.DataFrame]:
    """向后兼容的日线面板入口。"""
    return load_bar_panel(
        symbols, start, end, frequency="1d", field=field, use_cache=use_cache,
        progress=progress, refresh=refresh, priority=priority, max_workers=max_workers,
    )
