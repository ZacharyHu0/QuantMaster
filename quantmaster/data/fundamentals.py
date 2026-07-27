"""基本面数据层：每日估值指标 + 季度财务指标的获取、缓存与时间对齐。

两类基本面数据的「时间结构」完全不同，混用是新手最常见的未来函数来源：

- 每日估值指标（PE / PE_TTM / PB / 股息率 / 总市值）：由当日收盘价与最近
  已披露的财务数据计算，交易日收盘后即可得，天然日频，直接与行情面板
  按日期对齐即可。
- 季度财务指标（ROE 等）：以「报告期」（3-31 / 6-30 / 9-30 / 12-31）为索引，
  但财报要在报告期结束后 1~4 个月才对外披露。若直接把 ROE 摆在报告期
  当天使用，等于「在 3 月 31 日就读到了 4 月底才公布的财报」——回测收益
  会被显著高估。因此 quarterly_to_daily() 强制加 lag_days 的发布滞后，
  再向前填充（ffill 只把过去的值带到现在，方向安全）。

存储沿用 BarStore（parquet 文件 + sqlite 元信息），根目录为
data_root/fundamentals，与行情 bars 缓存互相隔离。
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from functools import partial

import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.akshare_source import _require_akshare
from quantmaster.data.resilience import akshare_call
from quantmaster.data.storage import BarStore

logger = logging.getLogger(__name__)

# 每日估值指标的标准字段（对应 akshare stock_a_indicator_lg 的返回列）
DAILY_FIELDS = ("pe", "pe_ttm", "pb", "dv_ratio", "total_mv")

# fundamental_panel 的默认字段集合（tuple 不可变，可安全作默认参数）
DEFAULT_FIELDS = ("pe_ttm", "pb", "dv_ratio", "total_mv", "roe")


def fundamental_store() -> BarStore:
    """基本面缓存库：与行情缓存隔离的独立目录 data_root/fundamentals。"""
    return BarStore(root=get_config().data_root / "fundamentals")


def _six_digit(symbol: str) -> str:
    """"600519.SH" -> "600519"。基本面接口仅支持 A 股个股（六位数字代码）。"""
    code, _, suffix = symbol.partition(".")
    if suffix and suffix.upper() not in ("SH", "SZ", "BJ"):
        raise ValueError(f"基本面数据仅支持 A 股个股，收到: {symbol!r}")
    if not (len(code) == 6 and code.isdigit()):
        raise ValueError(f"基本面数据需要六位数字代码，收到: {symbol!r}")
    return code


def _roe_key(symbol: str) -> str:
    """季度 ROE 在缓存库中的键（与每日指标的键区分开）。"""
    return f"{symbol}#roe"


def fetch_daily_indicators(symbol: str) -> pd.DataFrame:  # pragma: no cover - 网络
    """拉取每日估值指标（akshare stock_a_indicator_lg，数据源为乐咕乐股）。

    返回 index=DatetimeIndex 的 DataFrame，列为 pe / pe_ttm / pb / dv_ratio /
    total_mv（列名标准化为小写）。该接口一次返回上市以来的全部历史，
    适合整体写入缓存后按日期切片使用。
    """
    code = _six_digit(symbol)
    try:
        ak = _require_akshare()
        raw = akshare_call(
            f"stock_a_indicator_lg({symbol})", ak.stock_a_indicator_lg,
            symbol=code, lane="akshare:other")
        if raw is None or raw.empty:
            raise RuntimeError("AKShare 返回空数据")
    except Exception as ak_error:
        if not get_config().data.tushare_token:
            raise
        logger.warning("AKShare 每日指标失败，降级 Tushare daily_basic: %s", ak_error)
        from quantmaster.data.tushare_source import TushareSource

        return TushareSource().daily_indicators(symbol)
    df = raw.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    date_col = "trade_date" if "trade_date" in df.columns else df.columns[0]
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.set_index(date_col).sort_index()
    df.index.name = "date"
    keep = [c for c in DAILY_FIELDS if c in df.columns]
    return df[keep].astype(float)


def fetch_quarterly_roe(symbol: str, start_year: str = "2018") -> pd.DataFrame:  # pragma: no cover - 网络
    """拉取季度净资产收益率（akshare stock_financial_analysis_indicator）。

    返回 DataFrame(index=报告期 DatetimeIndex, columns=["roe"])。注意：索引
    是「报告期」而非「公布日」，使用前必须经过 quarterly_to_daily() 加滞后。
    """
    code = _six_digit(symbol)
    try:
        ak = _require_akshare()
        raw = akshare_call(
            f"stock_financial_analysis_indicator({symbol})",
            ak.stock_financial_analysis_indicator, symbol=code, start_year=start_year,
            lane="akshare:other",
        )
        if raw is None or raw.empty:
            raise RuntimeError("AKShare 返回空数据")
        return extract_roe(raw)
    except Exception as ak_error:
        if not get_config().data.tushare_token:
            raise
        logger.warning("AKShare ROE 失败，降级 Tushare fina_indicator: %s", ak_error)
        from quantmaster.data.tushare_source import TushareSource

        return TushareSource().quarterly_roe(symbol, start_year=start_year)


def extract_roe(raw: pd.DataFrame) -> pd.DataFrame:
    """从财务分析指标表中抽取净资产收益率列，标准化为季度 DataFrame。

    兼容 akshare 不同版本：报告期可能在「日期」列或行索引里；优先取
    「净资产收益率(%)」，找不到精确列名时回退到任何包含该关键词的列。
    纯函数，不触网，可离线测试。
    """
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["roe"], index=pd.DatetimeIndex([], name="report_date"))
    df = raw.copy()
    if "日期" in df.columns:
        df["日期"] = pd.to_datetime(df["日期"])
        df = df.set_index("日期")
    else:
        df.index = pd.to_datetime(df.index)
    col = None
    if "净资产收益率(%)" in df.columns:
        col = "净资产收益率(%)"
    else:
        for c in df.columns:
            if "净资产收益率" in str(c):
                col = c
                break
    if col is None:
        raise ValueError("财务指标表缺少净资产收益率列")
    out = pd.DataFrame({"roe": pd.to_numeric(df[col], errors="coerce")}).sort_index()
    out = out[~out.index.duplicated(keep="last")]
    out.index.name = "report_date"
    return out


# A 股财报披露截止日距报告期结束的天数（按报告期月份区分）：
#   一季报(3-31)/三季报(9-30) 截止次月末     ≈ 31 天
#   半年报(6-30)              截止 8-31      ≈ 62 天
#   年报(12-31)               截止次年 4-30  ≈ 120 天
# 按「截止日」滞后是保守口径：实际多数公司在截止日前披露，回测因此略偏保守，
# 但绝不会提前读到未披露的财报（用统一 45 天滞后时，年报/半年报会泄漏未来数据）。
DISCLOSURE_LAG_DAYS = {3: 31, 6: 62, 9: 31, 12: 120}


def quarterly_to_daily(
    quarterly_df: pd.DataFrame,
    dates: pd.DatetimeIndex,
    lag_days: int | None = None,
) -> pd.DataFrame:
    """季度数据（报告期索引）对齐到日频，显式加入财报发布滞后。

    步骤：
    1. 报告期 + 披露滞后 = 「可见日」。lag_days=None（默认）按报告期月份
       使用 A 股披露截止日（DISCLOSURE_LAG_DAYS：季报 31 / 半年报 62 /
       年报 120 天）；传入整数则统一使用该滞后（仅建议研究实验用，统一值
       会让年报/半年报在真实披露前就「可见」，构成未来函数）。
    2. 以可见日为索引，与目标日期取并集后 ffill，最后取出目标日期。
       ffill 只会把「过去」的值带到「现在」，方向上安全；之所以先并集
       再 ffill 而不是直接 reindex(dates).ffill()，是因为可见日可能落在
       周末 / 非交易日，直接 reindex 会把那期财报整个丢掉。

    已知局限：数据源返回的是「最新值」（业绩修正/重述后会覆盖原值），
    并非 point-in-time 数据库；对绝大多数量价+基本面研究影响有限，
    但请勿用它研究「业绩修正事件」本身。

    纯函数，不触网，可离线测试。
    """
    dates = pd.DatetimeIndex(dates)
    if quarterly_df is None or quarterly_df.empty:
        columns = list(quarterly_df.columns) if quarterly_df is not None else []
        return pd.DataFrame(index=dates, columns=columns, dtype=float)
    published = quarterly_df.copy()
    report_dates = pd.to_datetime(published.index)
    # 显式滞后：报告期 -> 可见日（防未来函数的关键一步）
    if lag_days is None:
        lags = pd.Series(report_dates.month, index=report_dates).map(
            DISCLOSURE_LAG_DAYS).fillna(120).astype(int)
        published.index = report_dates + pd.to_timedelta(lags.to_numpy(), unit="D")
    else:
        published.index = report_dates + pd.Timedelta(days=int(lag_days))
    published = published.sort_index()
    published = published[~published.index.duplicated(keep="last")]
    combined = published.reindex(published.index.union(dates)).ffill()
    return combined.reindex(dates).astype(float)


def _load_cached_or_fetch(
    key: str,
    store: BarStore,
    fetch: Callable[[], pd.DataFrame],
    end: str,
    cache_days: int | None = None,
) -> pd.DataFrame | None:
    """缓存优先的加载：缓存覆盖到 end 或仍新鲜就直接用，否则才触网。

    触网失败时退回旧缓存（可能为 None），由调用方决定是否跳过该标的。
    """
    cfg = get_config()
    max_age_days = cfg.data.cache_days if cache_days is None else cache_days
    cached = store.get(key)
    fresh = store.freshness(key)
    if cached is not None and not cached.empty and fresh is not None:
        covers = str(cached.index.max().date()) >= end
        if covers or fresh < max_age_days * 86400:
            return cached
    try:
        df = fetch()
    except Exception as e:
        logger.warning("获取基本面数据失败 %s: %s", key, e)
        return cached
    if df is None or df.empty:
        return cached
    store.put(key, df)
    merged = store.get(key)
    return merged if merged is not None else df


def fundamental_panel(
    symbols: list[str],
    start: str,
    end: str,
    fields: Sequence[str] = DEFAULT_FIELDS,
    lag_days: int | None = None,
    store: BarStore | None = None,
) -> dict[str, pd.DataFrame]:
    """多标的基本面面板：{字段: DataFrame(date × symbol)}。

    - 每日估值字段（pe/pe_ttm/pb/dv_ratio/total_mv）直接按日期切片；
    - roe 为季度数据，先经 quarterly_to_daily() 加发布滞后再对齐
      （默认按 A 股披露截止日区分报告期：季报 31 / 半年报 62 / 年报 120 天）；
    - 优先读本地 parquet 缓存，缺失或过期才触网；单标的失败仅
      logger.warning 并跳过，不影响其余标的（与行情 load_panel 风格一致）。
    """
    store = store or fundamental_store()
    fields = list(fields)
    for f in fields:
        if f != "roe" and f not in DAILY_FIELDS:
            logger.warning("未知基本面字段，忽略: %s", f)
    daily_fields = [f for f in fields if f in DAILY_FIELDS]
    want_roe = "roe" in fields
    # 多拉一年财报，保证 start 当天也有「已披露的上一期」可用（ffill 的种子值）
    roe_start_year = str(max(1990, int(start[:4]) - 1))

    daily_frames: dict[str, pd.DataFrame] = {}
    roe_frames: dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        if daily_fields:
            df = _load_cached_or_fetch(symbol, store, partial(fetch_daily_indicators, symbol), end)
            if df is not None and not df.empty:
                daily_frames[symbol] = df.loc[start:end]
            else:
                logger.warning("跳过 %s 的每日估值指标：无缓存且获取失败", symbol)
        if want_roe:
            q = _load_cached_or_fetch(
                _roe_key(symbol),
                store,
                partial(fetch_quarterly_roe, symbol, start_year=roe_start_year),
                end,
                cache_days=get_config().data.fundamental_cache_days,
            )
            if q is not None and not q.empty:
                roe_frames[symbol] = q
            else:
                logger.warning("跳过 %s 的季度 ROE：无缓存且获取失败", symbol)

    all_dates: pd.DatetimeIndex | None = None
    for df in daily_frames.values():
        all_dates = df.index if all_dates is None else all_dates.union(df.index)
    if all_dates is None or len(all_dates) == 0:
        all_dates = pd.bdate_range(start, end)

    result: dict[str, pd.DataFrame] = {}
    for f in daily_fields:
        cols = {s: df[f].reindex(all_dates) for s, df in daily_frames.items() if f in df.columns}
        if cols:
            result[f] = pd.DataFrame(cols)
    if want_roe and roe_frames:
        result["roe"] = pd.DataFrame(
            {s: quarterly_to_daily(q, all_dates, lag_days=lag_days)["roe"] for s, q in roe_frames.items()}
        )
    if not result:
        logger.warning("基本面面板为空：所有标的均加载失败或字段无效")
    return result
