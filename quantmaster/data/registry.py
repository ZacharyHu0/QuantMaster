"""数据源注册与统一入口：按市场路由，带本地缓存与自动降级。

用法：
    from quantmaster.data import load_history, load_panel
    df = load_history("600519.SH", "2022-01-01", "2024-12-31")
    panel = load_panel(["600519.SH", "000858.SZ"], "2022-01-01", "2024-12-31")
"""

from __future__ import annotations

import logging

import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.base import DataSource, Market, guess_market
from quantmaster.data.storage import BarStore

logger = logging.getLogger(__name__)

# 各市场按优先级排列的数据源工厂
_SOURCE_FACTORIES: dict[str, list] = {}


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
            fresh_enough = (
                fresh < cfg.data.cache_days * 86400
                and str(cached.index.min().date()) <= start
            )
            if covers or fresh_enough:
                sliced = cached.loc[start:end]
                if not sliced.empty:
                    return sliced

    # 触网时把请求区间放宽到与旧缓存的并集并整体替换缓存，而不是增量合并：
    # A 股数据源默认前复权（qfq），除权除息后全体历史价会重算，
    # 增量合并会把两种复权基准拼进同一序列，接缝处出现虚假跳空。
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
                store.put(symbol, df, replace=True)
                return df.loc[start:end]
            errors.append(f"{factory.__name__}: 返回空数据")
        except Exception as e:
            errors.append(f"{factory.__name__}: {e}")
            logger.warning("数据源 %s 获取 %s 失败: %s", factory.__name__, symbol, e)

    # 全部失败但缓存有部分数据时，退回缓存
    cached = store.get(symbol)
    if cached is not None and not cached.empty:
        logger.warning("全部数据源失败，使用本地缓存: %s", symbol)
        return cached.loc[start:end]
    raise RuntimeError(f"获取 {symbol} 日线失败: {errors}")


def load_panel(
    symbols: list[str],
    start: str,
    end: str,
    field: str | None = None,
    use_cache: bool = True,
) -> pd.DataFrame | dict[str, pd.DataFrame]:
    """加载多标的面板数据。

    field=None 时返回 {字段: DataFrame(date × symbol)} 的字典（open/high/low/close/volume...），
    指定 field（如 "close"）时直接返回该字段的 DataFrame。
    """
    store = BarStore()
    frames: dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        try:
            frames[symbol] = load_history(symbol, start, end, use_cache=use_cache, store=store)
        except Exception as e:
            logger.warning("跳过 %s: %s", symbol, e)
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
