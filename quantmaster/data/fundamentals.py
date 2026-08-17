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
import time
from collections.abc import Callable, Sequence
from functools import partial

import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.akshare_source import _require_akshare
from quantmaster.data.free_stockdb_source import FreeStockDBSource
from quantmaster.data.resilience import akshare_call
from quantmaster.data.storage import BarStore
from quantmaster.data.temporal import TemporalContractError

logger = logging.getLogger(__name__)

# 每日估值指标的标准字段（Tushare 与 AKShare 归一化后的公共口径）
DAILY_FIELDS = ("pe", "pe_ttm", "pb", "dv_ratio", "total_mv")

# AKShare 1.18.81 的现行个股历史估值接口按指标分别返回两列数据。
# 百度总市值的数值单位为亿元；乘 10_000 后与 Tushare daily_basic 的万元口径一致。
_AKSHARE_VALUATION_FIELDS = (
    ("市盈率(静)", "pe", 1.0),
    ("市盈率(TTM)", "pe_ttm", 1.0),
    ("市净率", "pb", 1.0),
    ("总市值", "total_mv", 10_000.0),
)

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


def _akshare_valuation_period(start: str | None) -> str:
    """选择能覆盖请求起点的百度估值窗口，避免无条件下载全部历史。"""
    if not start:
        return "全部"
    age_days = max(
        0,
        (pd.Timestamp.now().normalize() - pd.Timestamp(start).normalize()).days,
    )
    for days, period in (
        (366, "近一年"),
        (3 * 366, "近三年"),
        (5 * 366, "近五年"),
        (10 * 366, "近十年"),
    ):
        if age_days <= days:
            return period
    return "全部"


def _fetch_akshare_daily_indicators(
    ak,
    code: str,
    start: str | None,
    end: str | None,
) -> pd.DataFrame:
    """从 AKShare 当前百度估值接口拼接标准化的每日估值面板。"""
    period = _akshare_valuation_period(start)
    frames: list[pd.DataFrame] = []
    for indicator, field, scale in _AKSHARE_VALUATION_FIELDS:
        raw = akshare_call(
            f"stock_zh_valuation_baidu({code},{indicator})",
            ak.stock_zh_valuation_baidu,
            symbol=code,
            indicator=indicator,
            period=period,
            lane="akshare:other",
        )
        if raw is None or raw.empty:
            continue
        if not {"date", "value"}.issubset(raw.columns):
            raise ValueError(f"AKShare {indicator} 返回列不完整")
        frame = raw[["date", "value"]].copy()
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame[field] = pd.to_numeric(frame["value"], errors="coerce") * scale
        frame = frame.dropna(subset=["date"]).drop_duplicates("date", keep="last")
        frames.append(frame.set_index("date")[[field]])
    if not frames:
        raise RuntimeError("AKShare 返回空数据")
    result = pd.concat(frames, axis=1).sort_index()
    result.index.name = "date"
    if start:
        result = result.loc[pd.Timestamp(start):]
    if end:
        result = result.loc[:pd.Timestamp(end)]
    if result.empty:
        raise RuntimeError("AKShare 返回数据不覆盖请求区间")
    # 当前官方接口不提供历史股息率；保留 NaN 列维持稳定 schema，不合成数据。
    return result.reindex(columns=DAILY_FIELDS).astype(float)


def _fetch_free_stockdb_daily_indicators(
    source: FreeStockDBSource,
    symbol: str,
    start: str | None,
    end: str | None,
) -> pd.DataFrame:
    """从本机 StockDB 盘后截面提取每日估值字段。

    StockDB 的截面合同目前只证实 ``pe_ttm``、``pb`` 和 ``total_mv``；
    ``pe``、``dv_ratio`` 不在已验证字段集合中，因此保持为空而不推算或
    触发另一条远端请求。没有明确区间时不读取全历史，保留原有直接调用
    ``fetch_daily_indicators(symbol)`` 的提供商行为。
    """
    if not start or not end:
        raise ValueError("本机 StockDB 每日估值读取需要明确 start 和 end")

    frame = source.daily_cross_section([symbol], start, end)
    required = {"symbol", "date", "pe_ttm", "pb", "total_mv"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"本机 StockDB 截面缺少估值列: {sorted(missing)}")

    code = _six_digit(symbol)
    observed_code = frame["symbol"].astype(str).str.partition(".")[0].str.zfill(6)
    frame = frame.loc[observed_code.eq(code)].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.dropna(subset=["date"])
    frame = frame.loc[
        frame["date"].between(pd.Timestamp(start), pd.Timestamp(end))
    ]
    if frame.empty:
        raise RuntimeError(f"本机 StockDB 没有返回 {symbol} 的可验证估值日期")
    if frame["date"].duplicated().any():
        raise ValueError(f"本机 StockDB 估值截面存在重复日期: {symbol}")

    for field in ("pe_ttm", "pb", "total_mv"):
        frame[field] = pd.to_numeric(frame[field], errors="coerce")
    if frame[["pe_ttm", "pb", "total_mv"]].notna().sum().sum() == 0:
        raise RuntimeError(f"本机 StockDB 没有返回 {symbol} 的可用估值字段")

    result = frame.set_index("date").sort_index().reindex(columns=DAILY_FIELDS)
    result.index.name = "date"
    result = result.astype(float)
    result.attrs.update(frame.attrs)
    result.attrs.update({
        "source": "free-stockdb:daily_cross_section",
        "valuation_fields": ("pe_ttm", "pb", "total_mv"),
    })
    return result


def fetch_daily_indicators(
    symbol: str,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:  # pragma: no cover - 网络
    """拉取每日估值指标（本机 StockDB、AKShare，失败时降级 Tushare）。

    返回 index=DatetimeIndex 的 DataFrame，列为 pe / pe_ttm / pb / dv_ratio /
    total_mv。StockDB 目前证实 pe_ttm / pb / total_mv，其他字段保留为空；
    AKShare 当前也不提供历史股息率，因此 dv_ratio 保留为空。按请求起点选择
    一、三、五、十年或全部历史窗口。
    """
    code = _six_digit(symbol)
    from quantmaster.data.tushare_source import TushareSource

    tushare = TushareSource()
    cached_tushare = tushare.cached_daily_indicators(symbol, start=start, end=end)
    if cached_tushare is not None:
        return cached_tushare
    try:
        return _fetch_free_stockdb_daily_indicators(
            FreeStockDBSource(), symbol, start, end,
        )
    except (ImportError, OSError, RuntimeError, TypeError, ValueError, KeyError) as stockdb_error:
        logger.debug("本机 StockDB 每日指标不可用，继续既有提供商回退: %s", stockdb_error)
    try:
        ak = _require_akshare()
        return _fetch_akshare_daily_indicators(ak, code, start, end)
    except Exception as ak_error:
        if not get_config().data.tushare_token:
            raise
        logger.debug("AKShare 每日指标失败，降级 Tushare daily_basic: %s", ak_error)
        if start or end:
            return tushare.daily_indicators(symbol, start=start, end=end)
        return tushare.daily_indicators(symbol)


def fetch_quarterly_roe(symbol: str, start_year: str = "2018") -> pd.DataFrame:  # pragma: no cover - 网络
    """拉取季度净资产收益率（akshare stock_financial_analysis_indicator）。

    返回 DataFrame(index=报告期 DatetimeIndex, columns=["roe"])。注意：索引
    是「报告期」而非「公布日」，使用前必须经过 quarterly_to_daily() 加滞后。
    """
    code = _six_digit(symbol)
    from quantmaster.data.tushare_source import TushareSource

    tushare = TushareSource()
    cached_tushare = tushare.cached_quarterly_roe(symbol, start_year=start_year)
    if cached_tushare is not None:
        return cached_tushare
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
        logger.debug("AKShare ROE 失败，降级 Tushare fina_indicator: %s", ak_error)
        return tushare.quarterly_roe(symbol, start_year=start_year)


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


def _session_dates(dates: pd.DatetimeIndex) -> pd.DatetimeIndex:
    """Return the distinct market-local session dates represented by ``dates``.

    ``dates`` is the caller's already selected trading-session index.  Reusing it
    avoids inventing sessions with ``BDay`` (which is wrong on exchange holidays).
    """
    if dates.tz is None:
        local = dates.normalize()
    else:
        local = dates.tz_convert("Asia/Shanghai").tz_localize(None).normalize()
    return pd.DatetimeIndex(local.unique()).sort_values()


def _next_session_visibility(
    availability_dates: pd.DatetimeIndex,
    dates: pd.DatetimeIndex,
) -> pd.DatetimeIndex:
    """Map date-only evidence to the first later requested trading session.

    A provider date is not an instant.  In particular, interpreting an
    ``ann_date`` as 00:00 would expose a report to an intraday decision made
    before the announcement.  The earliest safe date-only policy is therefore
    the next session represented by the caller's verified market index.
    """
    sessions = _session_dates(dates)
    mapped: list[pd.Timestamp] = []
    for raw in availability_dates:
        day = pd.Timestamp(raw)
        if day.tzinfo is not None:
            day = day.tz_convert("Asia/Shanghai").tz_localize(None)
        day = day.normalize()
        later = sessions[sessions > day]
        mapped.append(later[0] if len(later) else pd.NaT)
    return pd.DatetimeIndex(mapped)


def _precise_publication_index(values: pd.Series) -> pd.DatetimeIndex:
    """Parse provider publication instants without accepting naive timestamps."""
    result: list[pd.Timestamp] = []
    for value in values:
        if pd.isna(value):
            raise TemporalContractError("published_at 缺失，不能用于正式财务研究")
        try:
            instant = pd.Timestamp(value)
        except (TypeError, ValueError) as exc:
            raise TemporalContractError("published_at 不是可识别的精确时刻") from exc
        if instant.tzinfo is None or instant.utcoffset() is None:
            raise TemporalContractError("published_at 必须包含时区")
        result.append(instant.tz_convert("UTC"))
    return pd.DatetimeIndex(result)


def quarterly_to_daily(
    quarterly_df: pd.DataFrame,
    dates: pd.DatetimeIndex,
    lag_days: int | None = None,
) -> pd.DataFrame:
    """季度数据（报告期索引）对齐到日频，显式加入财报发布滞后。

    步骤：
    1. ``report_date`` 只表示报告期，绝不直接作为发布时间。报告期 + 披露
       滞后得到保守的日期级证据；``ann_date`` 同样只是日期。两者均从调用方
       提供的真实交易索引选择严格晚于该日期的首个 session，禁止默认为
       当日 00:00。lag_days=None（默认）按报告期月份
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
    # 精确发布时间只能与同样精确、带时区的 cutoff 索引比较。日期索引无法
    # 表达盘中 cutoff，若在这里默认为收盘或午夜都会制造未来数据。
    if "published_at" in published.columns:
        if dates.tz is None:
            raise TemporalContractError(
                "精确 published_at 需要带时区的 cutoff 索引，日期索引不能代表盘中 cutoff"
            )
        published.index = _precise_publication_index(published.pop("published_at"))
        target_index = dates.tz_convert("UTC")
    # Tushare 当前 fina_indicator 只有 date-only ann_date。最早从下一条真实
    # session 可见，不能在公告日盘中使用。
    elif published.index.name == "ann_date":
        announcement_dates = pd.to_datetime(published.index, errors="coerce")
        if announcement_dates.isna().any():
            raise TemporalContractError("ann_date 不是可识别的业务日期")
        if any(pd.Timestamp(value).time() != pd.Timestamp(value).normalize().time()
               for value in announcement_dates):
            raise TemporalContractError("ann_date 必须是日期，精确时刻应使用 published_at")
        published.index = _next_session_visibility(announcement_dates, dates)
        published = published.loc[~published.index.isna()]
        target_index = dates
    else:
        report_dates = pd.to_datetime(published.index)
        if lag_days is None:
            lags = pd.Series(report_dates.month, index=report_dates).map(
                DISCLOSURE_LAG_DAYS).fillna(120).astype(int)
            availability_dates = report_dates + pd.to_timedelta(lags.to_numpy(), unit="D")
        else:
            availability_dates = report_dates + pd.Timedelta(days=int(lag_days))
        published.index = _next_session_visibility(availability_dates, dates)
        published = published.loc[~published.index.isna()]
        target_index = dates
    published = published.drop(
        columns=["report_date", "report_period", "update_flag", "ann_date"], errors="ignore",
    )
    published = published.sort_index()
    published = published[~published.index.duplicated(keep="last")]
    combined = published.reindex(published.index.union(target_index)).ffill()
    result = combined.reindex(target_index).astype(float)
    result.index = dates
    return result


def _load_cached_or_fetch(
    key: str,
    store: BarStore,
    fetch: Callable[[], pd.DataFrame],
    start: str,
    end: str,
    cache_days: int | None = None,
    *,
    columns: list[str] | None = None,
    log_failure: bool = True,
) -> pd.DataFrame | None:
    """以单标的共享锁保护缓存检查与 API 拉取，避免并行任务重复触网。"""
    with store.lock(key):
        return _load_cached_or_fetch_locked(
            key,
            store,
            fetch,
            start,
            end,
            cache_days,
            columns=columns,
            log_failure=log_failure,
        )


def _load_cached_or_fetch_locked(
    key: str,
    store: BarStore,
    fetch: Callable[[], pd.DataFrame],
    start: str,
    end: str,
    cache_days: int | None = None,
    *,
    columns: list[str] | None = None,
    log_failure: bool = True,
) -> pd.DataFrame | None:
    """缓存优先的加载：缓存覆盖到 end 或仍新鲜就直接用，否则才触网。

    触网失败时退回旧缓存（可能为 None），由调用方决定是否跳过该标的。
    """
    cfg = get_config()
    max_age_days = cfg.data.cache_days if cache_days is None else cache_days
    cached = store.get(key, columns=columns)
    # 锁内读取元信息，确保并发请求能看到另一个任务刚落盘的覆盖范围和检查时间。
    meta = store.metadata(key) or {}
    checked_at = meta.get("checked_at") or meta.get("updated_at")
    fresh = time.time() - float(checked_at) if checked_at is not None else None
    if cached is not None and not cached.empty:
        # 即使旧 parquet 的 SQLite 元信息遗失，只要文件本身已经覆盖目标结束日，
        # 也应直接使用本地数据，不能因为 freshness=None 再次请求提供商。
        file_covers_end = str(cached.index.max().date()) >= end
        checked_covers_range = bool(
            meta.get("coverage_start")
            and str(meta["coverage_start"]) <= start
            and meta.get("coverage_end")
            and str(meta["coverage_end"]) >= end
        )
        if file_covers_end or checked_covers_range or (
            fresh is not None and fresh < max_age_days * 86400
        ):
            return cached
    try:
        df = fetch()
    except Exception as e:
        log = logger.warning if log_failure else logger.debug
        log("获取基本面数据失败 %s: %s", key, e)
        return cached
    if df is None or df.empty:
        if cached is not None and not cached.empty and meta:
            # 提供商确认当前没有增量时刷新“已检查”时间，避免同一份旧缓存被每个
            # 排队任务反复触发相同 API 请求；不虚构实际数据覆盖范围。
            cached_start = str(cached.index.min().date())
            cached_end = str(cached.index.max().date())
            store.mark_checked(
                key, cached_start, cached_end,
                source="fundamentals", status="stale",
            )
        return cached
    store.put(key, df)
    store.mark_checked(key, start, end, source="fundamentals")
    merged = store.get(key, columns=columns)
    return merged if merged is not None else df


def fundamental_panel(
    symbols: list[str],
    start: str,
    end: str,
    fields: Sequence[str] = DEFAULT_FIELDS,
    lag_days: int | None = None,
    store: BarStore | None = None,
    progress=None,
    cancelled=None,
) -> dict[str, pd.DataFrame]:
    """多标的基本面面板：{字段: DataFrame(date × symbol)}。

    - 每日估值字段（pe/pe_ttm/pb/dv_ratio/total_mv）直接按日期切片；
    - roe 为季度数据，先经 quarterly_to_daily() 加发布滞后再对齐
      （默认按 A 股披露截止日区分报告期：季报 31 / 半年报 62 / 年报 120 天）；
    - 优先读本地 parquet 缓存，缺失或过期才触网；单标的失败仅
      logger.warning 并跳过，不影响其余标的（与行情 refresh_panel 风格一致）。
    """
    store = store or fundamental_store()
    fields = list(dict.fromkeys(fields))
    symbols = list(dict.fromkeys(symbols))
    for f in fields:
        if f != "roe" and f not in DAILY_FIELDS:
            logger.warning("未知基本面字段，忽略: %s", f)
    daily_fields = [f for f in fields if f in DAILY_FIELDS]
    want_roe = "roe" in fields
    # 多拉一年财报，保证 start 当天也有「已披露的上一期」可用（ffill 的种子值）
    roe_start_year = str(max(1990, int(start[:4]) - 1))

    daily_frames: dict[str, pd.DataFrame] = {}
    roe_frames: dict[str, pd.DataFrame] = {}
    daily_failures: list[str] = []
    roe_failures: list[str] = []
    total = len(symbols)
    for number, symbol in enumerate(symbols, start=1):
        if cancelled and cancelled():
            raise InterruptedError("基本面数据加载已取消")
        loaded = False
        if daily_fields:
            df = _load_cached_or_fetch(
                symbol,
                store,
                partial(fetch_daily_indicators, symbol, start, end),
                start,
                end,
                columns=daily_fields,
                log_failure=False,
            )
            if df is not None and not df.empty:
                daily_frames[symbol] = df.loc[start:end]
                loaded = True
            else:
                daily_failures.append(symbol)
        if want_roe:
            q = _load_cached_or_fetch(
                _roe_key(symbol),
                store,
                partial(fetch_quarterly_roe, symbol, start_year=roe_start_year),
                start,
                end,
                cache_days=get_config().data.fundamental_cache_days,
                log_failure=False,
            )
            if q is not None and not q.empty:
                roe_frames[symbol] = q
                loaded = True
            else:
                roe_failures.append(symbol)
        if progress:
            progress(number, total, symbol, loaded)

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
    if daily_failures or roe_failures:
        details = []
        if daily_failures:
            details.append(
                f"每日估值 {len(daily_failures)}/{total}（样本：{', '.join(daily_failures[:5])}）"
            )
        if roe_failures:
            details.append(
                f"季度 ROE {len(roe_failures)}/{total}（样本：{', '.join(roe_failures[:5])}）"
            )
        logger.warning("基本面批量加载存在缺失：%s", "；".join(details))
    elif not result:
        logger.warning("基本面面板为空：所有标的均加载失败或字段无效")
    return result
