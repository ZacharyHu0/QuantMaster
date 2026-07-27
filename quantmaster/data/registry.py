"""数据源注册与统一入口：按市场路由，带本地缓存与自动降级。

用法：
    from quantmaster.data import load_history, load_panel
    df = load_history("600519.SH", "2022-01-01", "2024-12-31")
    panel = load_panel(["600519.SH", "000858.SZ"], "2022-01-01", "2024-12-31")
"""

from __future__ import annotations

import logging
from collections.abc import Callable

import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.base import DataSource, Market, guess_market, validate_frequency
from quantmaster.data.storage import BarStore, IntradayBarStore

logger = logging.getLogger(__name__)

# 各市场按优先级排列的数据源工厂
_SOURCE_FACTORIES: dict[str, list] = {}


def _covers_requested_range(df: pd.DataFrame, start: str, end: str) -> bool:
    """判断日线是否覆盖请求边界且没有明显的大面积缺行。"""
    if df is None or df.empty:
        return False
    first = pd.Timestamp(df.index.min()).normalize()
    last = pd.Timestamp(df.index.max()).normalize()
    boundaries_ok = (
        first <= pd.Timestamp(start).normalize() + pd.Timedelta(days=14)
        and last >= pd.Timestamp(end).normalize() - pd.Timedelta(days=14)
    )
    if not boundaries_ok:
        return False
    # 中国市场节假日会少于工作日，但正常年份交易日密度约九成以上。
    # 低于 80% 可确定是接口缺了较大分块；短区间受长假影响大，不做密度判断。
    expected = len(pd.bdate_range(start, end))
    if expected < 10:
        return True
    index = pd.DatetimeIndex(df.index).normalize()
    actual = index[(index >= pd.Timestamp(start)) & (index <= pd.Timestamp(end))].nunique()
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
    if fresh_index.min() > cached_index.min() or fresh_index.max() < cached_index.max():
        return False
    return cached_index.difference(fresh_index).empty


def _factories() -> dict[Market, list]:
    from quantmaster.data.akshare_source import AkshareSource
    from quantmaster.data.tushare_source import TushareSource
    from quantmaster.data.yfinance_source import YFinanceSource

    ak, yf, tu = AkshareSource, YFinanceSource, TushareSource
    return {
        Market.CN: [ak, tu],
        Market.HK: [ak, yf],
        Market.US: [yf],
        Market.JP: [yf],
        Market.KR: [yf],
        Market.FUTURES: [ak, yf],
        Market.INDEX: [ak, yf],
    }


def get_source(market: Market) -> DataSource:
    """返回该市场第一个可用的数据源。"""
    errors = []
    for factory in _factories().get(market, []):
        try:
            return factory()
        except Exception as e:  # pragma: no cover - 依赖安装情况
            errors.append(f"{factory.__name__}: {e}")
    raise RuntimeError(f"市场 {market.value} 无可用数据源: {errors}")


def load_history(
    symbol: str,
    start: str,
    end: str,
    use_cache: bool = True,
    store: BarStore | None = None,
) -> pd.DataFrame:
    """加载单只标的的标准化日线，命中缓存则不请求网络。"""
    store = store or BarStore()
    cfg = get_config()
    cached = store.get(symbol)
    if use_cache:
        fresh = store.freshness(symbol)
        if cached is not None and not cached.empty and fresh is not None:
            covers = str(cached.index.min().date()) <= start and str(cached.index.max().date()) >= end
            # 「新鲜」只保证 end 端接近今天，还必须覆盖 start 端，
            # 否则长区间请求会被无声截断成缓存里的短区间
            requested_end = pd.Timestamp(end).normalize()
            cached_end = pd.Timestamp(cached.index.max()).normalize()
            end_close_enough = cached_end >= requested_end - pd.Timedelta(days=7)
            fresh_enough = (
                fresh < cfg.data.cache_days * 86400
                and str(cached.index.min().date()) <= start
                and end_close_enough
            )
            if covers or fresh_enough:
                sliced = cached.loc[start:end]
                if not sliced.empty:
                    return sliced

    # 触网时把请求区间放宽到与旧缓存的并集。确认响应完整时整体替换，
    # 以保持 A 股前复权基准一致；不完整响应则增量合并，不能丢掉已经取得的
    # 完整历史分块。
    fetch_start, fetch_end = start, end
    if cached is not None and not cached.empty:
        fetch_start = min(start, str(cached.index.min().date()))
        fetch_end = max(end, str(cached.index.max().date()))

    market = guess_market(symbol)
    errors = []
    for factory in _factories().get(market, []):
        try:
            source = factory()
            df = source.daily(symbol, fetch_start, fetch_end)
            if df is not None and not df.empty:
                complete = _is_complete_refresh(df, cached, fetch_start, fetch_end)
                store.put(symbol, df, replace=complete)
                available = store.get(symbol)
                if not complete:
                    logger.warning(
                        "数据源 %s 返回 %s 的部分日线（%s 至 %s），已合并保留，不覆盖旧缓存",
                        factory.__name__, symbol, df.index.min(), df.index.max(),
                    )
                if available is not None and _covers_requested_range(available, start, end):
                    return available.loc[start:end]
                errors.append(f"{factory.__name__}: 部分数据已保留，但未覆盖请求区间")
                # 当前来源只取得部分区间时，再尝试下一来源补齐；已经写入的部分
                # 不会因后续来源失败而丢失。
                cached = available
                continue
            errors.append(f"{factory.__name__}: 返回空数据")
        except Exception as e:
            errors.append(f"{factory.__name__}: {e}")
            logger.debug("数据源 %s 获取 %s 失败: %s", factory.__name__, symbol, e)

    # 全部失败但缓存有部分数据时，退回缓存
    cached = store.get(symbol)
    if cached is not None and not cached.empty:
        logger.warning("全部数据源失败，使用本地缓存: %s", symbol)
        return cached.loc[start:end]
    raise RuntimeError(f"获取 {symbol} 日线失败: {errors}")


def load_intraday(
    symbol: str,
    start: str,
    end: str,
    frequency: str = "5m",
    use_cache: bool = True,
    store: IntradayBarStore | None = None,
) -> pd.DataFrame:
    """加载分钟线并持久化。

    分钟数据按 ``symbol + frequency`` 独立保存；联网失败时仍会返回本地已有区间。
    AKShare 的 1 分钟历史通常仅覆盖最近 5 个交易日，因此长期研究应每日增量归档。
    """
    frequency = validate_frequency(frequency)
    if frequency == "1d":
        return load_history(symbol, start, end, use_cache=use_cache)
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
            df = source.intraday(symbol, fetch_start, fetch_end, frequency)
            if df is not None and not df.empty:
                # 分钟线不采用日线的整段前复权替换语义：免费接口回溯有限，
                # 每日归档必须合并才能形成可长期复用的本地历史。
                store.put(symbol, df, replace=False)
                return store.get(symbol).loc[start_ts:end_ts]
            errors.append(f"{factory.__name__}: 返回空数据")
        except Exception as e:
            errors.append(f"{factory.__name__}: {e}")
            logger.debug("数据源 %s 获取 %s %s 失败: %s", factory.__name__, symbol, frequency, e)
    if cached is not None and not cached.empty:
        logger.warning("全部分钟数据源失败，使用本地缓存: %s %s", symbol, frequency)
        return cached.loc[start_ts:end_ts]
    raise RuntimeError(f"获取 {symbol} {frequency} 分钟线失败: {errors}")


def load_bars(
    symbol: str, start: str, end: str, frequency: str = "1d", use_cache: bool = True
) -> pd.DataFrame:
    """日线/分钟线统一入口。"""
    frequency = validate_frequency(frequency)
    if frequency == "1d":
        return load_history(symbol, start, end, use_cache=use_cache)
    return load_intraday(symbol, start, end, frequency, use_cache=use_cache)


def load_bar_panel(
    symbols: list[str],
    start: str,
    end: str,
    frequency: str = "1d",
    field: str | None = None,
    use_cache: bool = True,
    progress: Callable[[int, int, str, bool], None] | None = None,
) -> pd.DataFrame | dict[str, pd.DataFrame]:
    """加载日线或分钟线多标的面板数据。

    field=None 时返回 {字段: DataFrame(date × symbol)} 的字典（open/high/low/close/volume...），
    指定 field（如 "close"）时直接返回该字段的 DataFrame。
    """
    frequency = validate_frequency(frequency)
    store: BarStore | IntradayBarStore = (
        BarStore() if frequency == "1d" else IntradayBarStore(frequency)
    )
    frames: dict[str, pd.DataFrame] = {}
    total = len(symbols)
    for completed, symbol in enumerate(symbols, start=1):
        success = False
        try:
            if frequency == "1d":
                frames[symbol] = load_history(
                    symbol, start, end, use_cache=use_cache, store=store)
            else:
                frames[symbol] = load_intraday(
                    symbol, start, end, frequency=frequency, use_cache=use_cache, store=store)
            success = not frames[symbol].empty
        except Exception as e:
            logger.warning("跳过 %s: %s", symbol, e)
        finally:
            if progress:
                try:
                    progress(completed, total, symbol, success)
                except Exception as e:
                    logger.warning("行情进度回调失败（不影响数据加载）: %s", e)
    if not frames:
        raise RuntimeError("没有任何标的成功加载数据")

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
) -> pd.DataFrame | dict[str, pd.DataFrame]:
    """向后兼容的日线面板入口。"""
    return load_bar_panel(
        symbols, start, end, frequency="1d", field=field, use_cache=use_cache,
        progress=progress,
    )
