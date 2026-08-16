"""Tushare 2000 积分档数据源：限速、落盘缓存、前复权日线与基本面。

核心行情只使用仓库 ``docs/tushare_2000_guide.md`` 明确列为 2000 积分可用的接口：
``daily``、``adj_factor``、``index_daily``、``daily_basic``、
``fina_indicator``、``stk_limit``、``suspend_d``、``trade_cal``、
``index_classify``、``index_member_all`` 与 ``index_weight``。板块联动仅在东方财富
概念不可用时尝试权限隔离的 ``dc_index + dc_member``（当前需 6000 积分）；缺少
权限不会影响核心 Tushare 行情通道。

Tushare 是 AKShare 连续重试失败后的 A 股日线备用源；每次接口响应还会按
``endpoint + params`` 缓存在 ``data/api_cache/tushare``，避免研究/回测重复
消耗调用次数。
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta
from datetime import time as datetime_time
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.base import DataCapability, DataSource, Market, normalize_daily
from quantmaster.data.resilience import (
    TUSHARE_LIMITER,
    EndpointFrameCache,
    ProviderContractChanged,
    bypass_endpoint_cache,
    endpoint_cache_bypassed,
    provider_call,
)
from quantmaster.index_source_access import register_index_source
from quantmaster.instrument_source_access import register_instrument_source
from quantmaster.temporal import (
    ProviderDateFormat,
    parse_provider_date,
)
from quantmaster.trading_session_sources import register_official_calendar
from quantmaster.trading_sessions import market_date

logger = logging.getLogger(__name__)
_CHINA_TZ = ZoneInfo("Asia/Shanghai")
_IDENTITY_COLUMNS = frozenset({
    "ts_code", "index_code", "l1_code", "exchange", "market", "list_status", "status",
})
_DATE_COLUMNS = ("trade_date", "cal_date", "ann_date", "end_date")


class TushareProviderError(RuntimeError):
    """Normalize the SDK's untyped provider exceptions at the source boundary."""


def _parse_tushare_dates(
    values: pd.Series,
    *,
    field: str,
    allow_missing: bool = False,
) -> pd.Series:
    """Parse Tushare's documented YYYYMMDD fields with vectorized conversion.

    Replaces the per-row Python loop with pd.to_datetime(format=...) for 10-20x speedup
    on large tables (5000+ rows). Missing/empty values produce NaT (allow_missing=True)
    or raise ProviderContractChanged (allow_missing=False).
    """
    raw_str = values.astype(str).str.strip()
    mask_empty = values.isna() | raw_str.eq("")
    parsed = pd.to_datetime(raw_str, format="%Y%m%d", errors="coerce")
    if not allow_missing and mask_empty.any():
        positions = list(values.index[mask_empty])[:5]
        raise ProviderContractChanged(
            f"Tushare {field} \u5b58\u5728\u7f3a\u5931\u503c [missing_provider_date], "
            f"\u793a\u4f8b\u7d22\u5f15: {positions}"
        )
    bad_mask = ~mask_empty & parsed.isna()
    if bad_mask.any():
        positions = list(values.index[bad_mask])[:5]
        raise ProviderContractChanged(
            f"Tushare {field} \u5b58\u5728\u65e0\u6cd5\u89e3\u6790\u7684\u65e5\u671f "
            f"[invalid_provider_date], "
            f"\u793a\u4f8b\u7d22\u5f15: {positions}"
        )
    return parsed



def _require_tushare():
    try:
        import tushare as ts
    except ImportError as e:  # pragma: no cover
        raise ImportError("未安装 tushare。请执行: pip install 'quantmaster[tushare]'") from e
    token = get_config().data.tushare_token
    if not token:
        raise RuntimeError("未配置 TUSHARE_TOKEN（config.yaml 的 data.tushare_token 或环境变量）")
    # 直接注入 token，避免 ts.set_token() 在用户主目录写 tk.csv；容器和
    # 受限服务账户通常没有 Home 写权限，也不应额外落一份明文密钥文件。
    return ts.pro_api(token)


def _validate_tushare_frame(
    endpoint: str,
    params: dict[str, Any],
    frame: pd.DataFrame,
    *,
    required_nonempty: bool,
    required_columns: tuple[str, ...],
) -> None:
    declared = tuple(
        item.strip() for item in str(params.get("fields") or "").split(",") if item.strip()
    )
    missing = [column for column in (*required_columns, *declared) if column not in frame.columns]
    if missing:
        raise ProviderContractChanged(
            f"{endpoint} 响应缺少必需列: {', '.join(dict.fromkeys(missing))}"
        )
    if required_nonempty and frame.empty:
        from quantmaster.data.resilience import EmptyProviderResponse

        raise EmptyProviderResponse(f"{endpoint} 返回空数据")
    if frame.empty:
        return

    for column in _IDENTITY_COLUMNS.intersection(params).intersection(frame.columns):
        expected = str(params[column]).strip().casefold()
        actual = {str(value).strip().casefold() for value in frame[column].dropna()}
        if actual and actual != {expected}:
            raise ProviderContractChanged(
                f"{endpoint} 响应 {column} 与请求不一致: {sorted(actual)} != {expected}"
            )

    for column in _DATE_COLUMNS:
        if column not in params or column not in frame.columns:
            continue
        expected = pd.Timestamp(parse_provider_date(
            params[column], field=f"Tushare.params.{column}",
            provider_format=ProviderDateFormat.YYYYMMDD,
        ))
        actual = _parse_tushare_dates(frame[column], field=column, allow_missing=True).dropna()
        if not actual.empty and set(actual) != {expected}:
            raise ProviderContractChanged(f"{endpoint} 响应 {column} 与请求日期不一致")

    range_column = next((column for column in _DATE_COLUMNS if column in frame.columns), None)
    if range_column and (params.get("start_date") or params.get("end_date")):
        dates = _parse_tushare_dates(frame[range_column], field=range_column, allow_missing=True).dropna()
        start = pd.Timestamp(parse_provider_date(
            params["start_date"], field="Tushare.params.start_date",
            provider_format=ProviderDateFormat.YYYYMMDD,
        )) if params.get("start_date") else None
        end = pd.Timestamp(parse_provider_date(
            params["end_date"], field="Tushare.params.end_date",
            provider_format=ProviderDateFormat.YYYYMMDD,
        )) if params.get("end_date") else None
        if ((start is not None and (dates < start).any()) or
                (end is not None and (dates > end).any())):
            raise ProviderContractChanged(f"{endpoint} 响应日期超出请求范围")


def _instrument_type(symbol: str) -> str:
    try:
        from quantmaster.data.instruments import InstrumentStore

        instrument = InstrumentStore().get(symbol)
        return instrument.asset_type if instrument else ""
    except (ImportError, OSError, RuntimeError, sqlite3.Error, TypeError, ValueError):
        return ""


def _is_a_share_index(symbol: str) -> bool:
    code, _, suffix = symbol.partition(".")
    return (
        suffix.upper() == "CSI"
        or (suffix.upper() == "SH" and code.startswith("000"))
        or (suffix.upper() == "SZ" and code.startswith("399"))
        or _instrument_type(symbol) == "index"
    )


def _cache_ttl(end: str | None, default_days: int) -> int:
    """已结束的历史区间长期缓存；包含近期日期的请求按配置刷新。"""
    if end:
        try:
            end_date = pd.Timestamp(end).date()
            if end_date < market_date() - timedelta(days=3):
                return 3650
        except (TypeError, ValueError):
            pass
    return max(0, int(default_days))


def _current_session_cache_floor(
    end: str | None,
    *,
    now: datetime | None = None,
) -> float | None:
    """Invalidate responses captured before today's A-share close.

    A request made shortly after midnight legitimately returns the previous trading day,
    but it must not remain reusable after the 15:30 close boundary.
    """
    if not end:
        return None
    current = now or datetime.now(_CHINA_TZ)
    if current.tzinfo is None:
        current = current.replace(tzinfo=_CHINA_TZ)
    else:
        current = current.astimezone(_CHINA_TZ)
    try:
        requested_end = pd.Timestamp(end).date()
    except (TypeError, ValueError):
        return None
    close = datetime.combine(
        current.date(), datetime_time(hour=15, minute=30), tzinfo=_CHINA_TZ)
    if requested_end >= current.date() and current >= close:
        return close.timestamp()
    return None


class TushareSource(DataSource):
    name = "tushare"
    markets = (Market.CN, Market.INDEX)
    capabilities = frozenset({DataCapability.DAILY, DataCapability.INDUSTRY})

    def __init__(self, cache: EndpointFrameCache | None = None):
        self.cache = cache or EndpointFrameCache("tushare")
        self._api = None

    def _pro(self):
        if self._api is None:
            self._api = _require_tushare()
        return self._api

    def _call(
        self,
        endpoint: str,
        ttl_days: int,
        *,
        provider_lane: str = "",
        required_nonempty: bool = False,
        required_columns: tuple[str, ...] = (),
        **params: Any,
    ) -> pd.DataFrame:
        clean = {key: value for key, value in params.items() if value not in (None, "")}
        provider_lane = provider_lane or f"tushare:{endpoint}"
        force_refresh = endpoint_cache_bypassed()
        min_mtime = _current_session_cache_floor(clean.get("end_date"))

        def read_cache() -> pd.DataFrame | None:
            if force_refresh:
                return None
            return self.cache.get(
                endpoint,
                clean,
                ttl_days,
                min_mtime=min_mtime,
                required_nonempty=required_nonempty,
                required_columns=required_columns,
            )

        cached = read_cache()
        if cached is not None:
            return cached.copy()
        key = endpoint + ":" + self.cache._digest(endpoint, clean)
        if force_refresh:
            key += ":refresh"

        def fetch() -> pd.DataFrame:
            # 任务进入提供商队列后再次检查，确保排队期间已完成的相同响应直接复用。
            cached = read_cache()
            if cached is not None:
                return cached.copy()
            method = getattr(self._pro(), endpoint)
            TUSHARE_LIMITER.wait()
            cached = read_cache()
            if cached is not None:
                return cached.copy()
            try:
                result = method(**clean)
            except Exception as exc:  # Tushare SDK intentionally raises plain Exception
                raise TushareProviderError(str(exc).strip() or f"{endpoint} 调用失败") from exc
            frame = result if isinstance(result, pd.DataFrame) else pd.DataFrame(result)
            _validate_tushare_frame(
                endpoint,
                clean,
                frame,
                required_nonempty=required_nonempty,
                required_columns=required_columns,
            )
            self.cache.put(
                endpoint,
                clean,
                frame,
                required_nonempty=required_nonempty,
                required_columns=required_columns,
            )
            return frame.copy()

        if required_nonempty:
            return provider_call(provider_lane, key, fetch, empty_opens=True)
        return provider_call(provider_lane, key, fetch)

    @staticmethod
    def _normalize_market_frame(raw: pd.DataFrame) -> pd.DataFrame:
        if raw is None or raw.empty:
            return pd.DataFrame()
        frame = raw.rename(columns={"trade_date": "date", "vol": "volume"}).copy()
        if "date" not in frame:
            raise ProviderContractChanged(
                "Tushare 日线响应缺少 trade_date [missing_required_field]"
            )
        frame["date"] = _parse_tushare_dates(frame["date"], field="trade_date")
        # Tushare 日线 volume=手、amount=千元；统一成股、元，与 AKShare 一致。
        if "volume" in frame:
            frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce") * 100
        if "amount" in frame:
            frame["amount"] = pd.to_numeric(frame["amount"], errors="coerce") * 1000
        return normalize_daily(frame)

    def daily(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        """A 股前复权日线，或无需复权的指数/基金日线。"""
        start_c, end_c = start.replace("-", ""), end.replace("-", "")
        ttl = _cache_ttl(end, get_config().data.tushare_cache_days)
        fields = "ts_code,trade_date,open,high,low,close,vol,amount"
        if _is_a_share_index(symbol):
            raw = self._call(
                "index_daily", ttl, ts_code=symbol, start_date=start_c,
                end_date=end_c, fields=fields,
            )
            return self._normalize_market_frame(raw).loc[start:end]

        if _instrument_type(symbol) in {"etf", "fund"}:
            raw = self._call(
                "fund_daily", ttl, ts_code=symbol, start_date=start_c,
                end_date=end_c, fields=fields,
            )
            return self._normalize_market_frame(raw).loc[start:end]

        raw = self._call(
            "daily", ttl, ts_code=symbol, start_date=start_c,
            end_date=end_c, fields=fields,
        )
        factors = self._call(
            "adj_factor", ttl, ts_code=symbol, start_date=start_c,
            end_date=end_c, fields="ts_code,trade_date,adj_factor",
        )
        if raw.empty or factors.empty:
            return pd.DataFrame()
        merged = raw.merge(
            factors[["trade_date", "adj_factor"]], on="trade_date", how="inner",
        )
        merged["_parsed_trade_date"] = _parse_tushare_dates(
            merged["trade_date"], field="trade_date"
        )
        merged = merged.sort_values("_parsed_trade_date")
        factor = pd.to_numeric(merged["adj_factor"], errors="coerce")
        latest = factor.dropna().iloc[-1] if factor.notna().any() else None
        if latest is None or latest == 0:
            raise RuntimeError(f"Tushare {symbol} 复权因子为空")
        ratio = factor / latest
        for column in ("open", "high", "low", "close"):
            merged[column] = pd.to_numeric(merged[column], errors="coerce") * ratio
        merged = merged.drop(columns="_parsed_trade_date")
        return self._normalize_market_frame(merged).loc[start:end]

    def cached_daily(self, symbol: str, start: str, end: str) -> pd.DataFrame | None:
        """Read an already-cached daily contract without ever contacting Tushare.

        Batch StockDB acceptance uses this before scheduling its bounded
        cross-source sample.  Keeping the read here makes the cache key and
        normalization exactly match :meth:`daily`, while making accidental
        request-path network I/O impossible to hide behind a cache probe.
        """

        start_c, end_c = start.replace("-", ""), end.replace("-", "")
        ttl = _cache_ttl(end, get_config().data.tushare_cache_days)
        floor = _current_session_cache_floor(end_c)

        def local(endpoint: str, **params: Any) -> pd.DataFrame | None:
            clean = {key: value for key, value in params.items() if value not in (None, "")}
            return self.cache.get(endpoint, clean, ttl, min_mtime=floor)

        fields = "ts_code,trade_date,open,high,low,close,vol,amount"
        if _is_a_share_index(symbol):
            raw = local(
                "index_daily", ts_code=symbol, start_date=start_c,
                end_date=end_c, fields=fields,
            )
            return None if raw is None else self._normalize_market_frame(raw).loc[start:end]
        if _instrument_type(symbol) in {"etf", "fund"}:
            raw = local(
                "fund_daily", ts_code=symbol, start_date=start_c,
                end_date=end_c, fields=fields,
            )
            return None if raw is None else self._normalize_market_frame(raw).loc[start:end]
        raw = local(
            "daily", ts_code=symbol, start_date=start_c,
            end_date=end_c, fields=fields,
        )
        factors = local(
            "adj_factor", ts_code=symbol, start_date=start_c,
            end_date=end_c, fields="ts_code,trade_date,adj_factor",
        )
        if raw is None or factors is None or raw.empty or factors.empty:
            return None
        if not {"trade_date", "adj_factor"}.issubset(factors):
            return None
        merged = raw.merge(
            factors[["trade_date", "adj_factor"]], on="trade_date", how="inner",
        )
        if merged.empty:
            return None
        merged["_parsed_trade_date"] = _parse_tushare_dates(
            merged["trade_date"], field="trade_date"
        )
        merged = merged.sort_values("_parsed_trade_date")
        factor = pd.to_numeric(merged["adj_factor"], errors="coerce")
        latest = factor.dropna().iloc[-1] if factor.notna().any() else None
        if latest is None or latest == 0:
            return None
        ratio = factor / latest
        for column in ("open", "high", "low", "close"):
            merged[column] = pd.to_numeric(merged[column], errors="coerce") * ratio
        merged = merged.drop(columns="_parsed_trade_date")
        return self._normalize_market_frame(merged).loc[start:end]

    def research_daily(
        self, symbol: str, start: str, end: str, *, calendar: pd.DatetimeIndex | None = None,
    ) -> dict[str, pd.DataFrame]:
        """返回不可变总收益信号流与真实成交约束，供 production 研究使用。

        与 ``daily`` 不同，总收益价格直接使用 ``raw_price * adj_factor``，不会因
        请求结束日变化而重写历史。成交始终使用未复权价格。
        """
        if _is_a_share_index(symbol) or _instrument_type(symbol) in {"etf", "fund"}:
            raise ValueError("research_daily 首版只支持 A 股股票")
        start_c, end_c = start.replace("-", ""), end.replace("-", "")
        ttl = _cache_ttl(end, get_config().data.tushare_cache_days)
        raw = self._call(
            "daily", ttl, ts_code=symbol, start_date=start_c, end_date=end_c,
            fields="ts_code,trade_date,open,high,low,close,vol,amount",
        )
        factors = self._call(
            "adj_factor", ttl, ts_code=symbol, start_date=start_c, end_date=end_c,
            fields="ts_code,trade_date,adj_factor",
        )
        limits = self._call(
            "stk_limit", ttl, ts_code=symbol, start_date=start_c, end_date=end_c,
            fields="ts_code,trade_date,pre_close,up_limit,down_limit",
        )
        suspensions = self._call(
            "suspend_d", ttl, ts_code=symbol, start_date=start_c, end_date=end_c,
            fields="ts_code,trade_date,suspend_timing,suspend_type",
        )
        if raw.empty or factors.empty or limits.empty:
            raise RuntimeError(f"{symbol} 缺少原始日线、复权因子或真实涨跌停价")
        merged = raw.merge(
            factors[["trade_date", "adj_factor"]], on="trade_date", how="inner",
        ).merge(
            limits[["trade_date", "up_limit", "down_limit"]], on="trade_date", how="left",
        )
        merged["trade_date"] = _parse_tushare_dates(
            merged["trade_date"], field="trade_date"
        )
        merged = merged.sort_values("trade_date").set_index("trade_date")
        merged.index.name = "date"
        numeric = ("open", "high", "low", "close", "vol", "amount", "adj_factor",
                   "up_limit", "down_limit")
        for column in numeric:
            merged[column] = pd.to_numeric(merged[column], errors="coerce")
        raw_frame = pd.DataFrame({
            "open": merged["open"], "high": merged["high"],
            "low": merged["low"], "close": merged["close"],
            "volume": merged["vol"] * 100, "amount": merged["amount"] * 1000,
        })
        signal = raw_frame.copy()
        for column in ("open", "high", "low", "close"):
            signal[column] = raw_frame[column] * merged["adj_factor"]
        suspension_index = (calendar if calendar is not None else self.trade_calendar(start, end))
        suspension_index = suspension_index.union(merged.index).sort_values()
        suspended = pd.Series(False, index=suspension_index, name="suspended")
        if not suspensions.empty and "trade_date" in suspensions:
            suspension_types = (
                suspensions["suspend_type"].astype(str).str.upper()
                if "suspend_type" in suspensions
                else pd.Series("S", index=suspensions.index)
            )
            stopped = suspensions.loc[
                suspension_types != "R"
            ].copy()
            if "suspend_timing" in stopped:
                timing = stopped["suspend_timing"].fillna("").astype(str).str.strip()
                stopped = stopped.loc[timing.eq("") | timing.str.contains("09:30", regex=False)]
            suspension_dates = pd.to_datetime(
                stopped["trade_date"], errors="coerce",
            ).dropna().dt.normalize()
            suspended.loc[suspended.index.normalize().isin(suspension_dates)] = True
        elif not suspensions.empty and "suspend_date" in suspensions:
            # 兼容注入的区间型数据源；Tushare 官方 suspend_d 使用上面的逐日 trade_date。
            for _, event in suspensions.iterrows():
                suspended_at = pd.to_datetime(event.get("suspend_date"), errors="coerce")
                if pd.isna(suspended_at):
                    continue
                resumed_at = pd.to_datetime(event.get("resume_date"), errors="coerce")
                event_type = str(event.get("suspend_type") or "").upper()
                if event_type.startswith("R"):
                    continue
                stop = (
                    pd.Timestamp(resumed_at).normalize() - pd.Timedelta(days=1)
                    if pd.notna(resumed_at) else pd.Timestamp(end).normalize()
                )
                mask = (
                    (suspended.index.normalize() >= pd.Timestamp(suspended_at).normalize())
                    & (suspended.index.normalize() <= stop)
                )
                suspended.loc[mask] = True
        return {
            "signal": signal.loc[start:end],
            "raw": raw_frame.loc[start:end],
            "adj_factor": merged[["adj_factor"]].loc[start:end],
            "limits": merged[["up_limit", "down_limit"]].loc[start:end],
            "suspended": suspended.loc[start:end].to_frame(),
        }

    def trade_calendar(self, start: str, end: str, exchange: str = "SSE") -> pd.DatetimeIndex:
        """读取官方交易日历；production 数据包禁止用普通工作日替代。"""
        ttl = _cache_ttl(end, get_config().data.tushare_cache_days)
        frame = self._call(
            "trade_cal", ttl, exchange=exchange, start_date=start.replace("-", ""),
            end_date=end.replace("-", ""), fields="exchange,cal_date,is_open,pretrade_date",
        )
        if frame.empty or "cal_date" not in frame:
            raise RuntimeError("交易日历为空")
        enabled = frame.loc[pd.to_numeric(frame.get("is_open"), errors="coerce") == 1]
        return pd.DatetimeIndex(
            _parse_tushare_dates(enabled["cal_date"], field="cal_date")
        ).normalize().sort_values()

    def suspension_snapshot(self, trade_date: str) -> dict[str, object]:
        """Return an official full-day suspension set for one exact session."""
        from quantmaster.data.instrument_snapshots import (
            SUSPENSION_CONTRACT,
            SUSPENSION_SCHEMA_VERSION,
            SUSPENSION_SOURCE,
            content_hash,
            tushare_suspension_request_evidence,
        )

        target = pd.Timestamp(trade_date).normalize()
        if pd.isna(target):
            raise ValueError("停牌证据日期无效")
        compact = target.strftime("%Y%m%d")
        # A pre-close empty/partial endpoint cache must never be relabelled with the
        # post-close acquisition time of this authoritative full-day observation.
        # Immutable suspension artifacts are the cache for accepted evidence.
        with bypass_endpoint_cache():
            raw = self._call(
                "suspend_d",
                _cache_ttl(target.date().isoformat(), get_config().data.tushare_cache_days),
                trade_date=compact,
                fields="ts_code,trade_date,suspend_timing,suspend_type",
            )
        frame = raw.astype(object).where(pd.notna(raw), None)
        rows, request_evidence = tushare_suspension_request_evidence(
            target.date().isoformat(),
            raw_records=frame.to_dict("records"),
            raw_columns=[str(column) for column in frame.columns],
        )
        acquired_at = datetime.now(_CHINA_TZ).isoformat()
        contract = {
            "schema_version": SUSPENSION_SCHEMA_VERSION,
            "contract": SUSPENSION_CONTRACT,
            "source": SUSPENSION_SOURCE,
            "trade_date": target.date().isoformat(),
            "acquired_at": acquired_at,
            "rows": rows,
            "request_evidence": request_evidence,
        }
        return {
            **contract,
            "content_hash": content_hash(contract),
            "symbols": sorted({item["symbol"] for item in rows}),
        }

    def daily_indicators(
        self, symbol: str, start: str | None = None, end: str | None = None,
    ) -> pd.DataFrame:
        """2000 积分 ``daily_basic``：估值、股息率和市值。"""
        ttl = _cache_ttl(end, get_config().data.tushare_cache_days)
        params: dict[str, Any] = self._daily_indicator_params(symbol, start, end)
        raw = self._call("daily_basic", ttl, **params)
        return self._normalize_daily_indicators(raw)

    @staticmethod
    def _daily_indicator_params(
        symbol: str, start: str | None, end: str | None,
    ) -> dict[str, str | None]:
        return {
            "ts_code": symbol,
            "start_date": start.replace("-", "") if start else None,
            "end_date": end.replace("-", "") if end else None,
            "fields": "ts_code,trade_date,pe,pe_ttm,pb,dv_ratio,total_mv",
        }

    @staticmethod
    def _normalize_daily_indicators(raw: pd.DataFrame) -> pd.DataFrame:
        if raw.empty:
            return pd.DataFrame()
        frame = raw.copy()
        frame["trade_date"] = _parse_tushare_dates(
            frame["trade_date"], field="trade_date"
        )
        frame = frame.set_index("trade_date").sort_index()
        frame.index.name = "date"
        fields = [c for c in ("pe", "pe_ttm", "pb", "dv_ratio", "total_mv") if c in frame]
        return frame[fields].apply(pd.to_numeric, errors="coerce")

    def cached_daily_indicators(
        self, symbol: str, start: str | None = None, end: str | None = None,
    ) -> pd.DataFrame | None:
        """只读取已有接口缓存；缓存未命中时绝不连接 Tushare。"""
        ttl = _cache_ttl(end, get_config().data.tushare_cache_days)
        params = {
            key: value
            for key, value in self._daily_indicator_params(symbol, start, end).items()
            if value not in (None, "")
        }
        raw = self.cache.get("daily_basic", params, ttl)
        return None if raw is None else self._normalize_daily_indicators(raw)

    def instrument_catalog(self) -> tuple[list[dict], list[dict]]:
        """读取内地股票/场内基金/指数及港股目录，供证券主数据增量更新。"""
        from quantmaster.data.instrument_snapshots import (
            tushare_catalog_partition_evidence,
        )

        records: list[dict] = []
        outcomes: list[dict] = []

        def fetch_partition(
            endpoint: str,
            partition_key: str,
            partition_value: str,
            *,
            required_columns: tuple[str, ...],
            **params: Any,
        ) -> pd.DataFrame:
            frame = self._call(endpoint, 7, **params)
            missing = sorted(set(required_columns) - set(frame.columns))
            if missing:
                raise RuntimeError(
                    f"{endpoint} {partition_key}={partition_value} 缺少字段: {missing}"
                )
            raw = frame.astype(object).where(pd.notna(frame), None).to_dict("records")
            normalized, evidence = tushare_catalog_partition_evidence(
                endpoint,
                partition_key,
                partition_value,
                params=params,
                raw_records=raw,
                raw_columns=[str(column) for column in frame.columns],
            )
            records.extend(normalized)
            outcomes.append(evidence)
            return frame

        for status in ("L", "D", "P"):
            fetch_partition(
                "stock_basic", "list_status", status,
                required_columns=("ts_code", "list_status", "list_date", "delist_date"),
                exchange="", list_status=status,
                fields=("ts_code,symbol,name,fullname,enname,exchange,curr_type,"
                        "list_status,list_date,delist_date"),
            )
        for status in ("L", "D"):
            fetch_partition(
                "fund_basic", "status", status,
                required_columns=(
                    "ts_code", "name", "fund_type", "status", "list_date", "delist_date",
                ),
                market="E", status=status,
                fields="ts_code,name,fund_type,status,list_date,delist_date",
            )
        for market in ("CSI", "SSE", "SZSE"):
            fetch_partition(
                "index_basic", "market", market,
                required_columns=("ts_code", "name", "market"),
                market=market,
                fields="ts_code,name,fullname,market",
            )
        for status in ("L", "D", "P"):
            fetch_partition(
                "hk_basic", "list_status", status,
                required_columns=(
                    "ts_code", "name", "list_status", "list_date", "delist_date",
                ),
                list_status=status,
                fields=(
                    "ts_code,symbol,name,fullname,enname,list_status,list_date,delist_date"
                ),
            )
        return records, outcomes

    def quarterly_roe(self, symbol: str, start_year: str = "2018") -> pd.DataFrame:
        """2000 积分 ``fina_indicator``：保留公告日与修订序列的 PIT ROE。"""
        ttl = max(1, int(get_config().data.fundamental_cache_days))
        params: dict[str, Any] = self._quarterly_roe_params(symbol, start_year)
        raw = self._call("fina_indicator", ttl, **params)
        return self._normalize_quarterly_roe(raw)

    @staticmethod
    def _quarterly_roe_params(symbol: str, start_year: str) -> dict[str, str]:
        return {
            "ts_code": symbol,
            "start_date": f"{start_year}0101",
            "fields": "ts_code,ann_date,end_date,roe,update_flag",
        }

    @staticmethod
    def _normalize_quarterly_roe(raw: pd.DataFrame) -> pd.DataFrame:
        if raw.empty or "roe" not in raw:
            return pd.DataFrame(columns=["report_date", "roe", "update_flag"])
        frame = raw.copy()
        if "end_date" not in frame or "ann_date" not in frame:
            raise ProviderContractChanged(
                "Tushare fina_indicator 响应缺少 ann_date 或 end_date "
                "[missing_required_field]"
            )
        frame["report_date"] = _parse_tushare_dates(
            frame["end_date"], field="end_date"
        )
        frame["ann_date"] = _parse_tushare_dates(
            frame["ann_date"], field="ann_date", allow_missing=True
        )
        frame["roe"] = pd.to_numeric(frame["roe"], errors="coerce")
        update_flag = frame.get("update_flag")
        frame["update_flag"] = (
            update_flag.fillna("").astype(str) if update_flag is not None else ""
        )
        frame = frame.dropna(subset=["ann_date", "report_date", "roe"])
        frame = frame.sort_values(["ann_date", "report_date", "update_flag"])
        frame = frame.drop_duplicates(["ann_date", "report_date"], keep="last")
        frame = frame.set_index("ann_date")
        frame.index.name = "ann_date"
        return frame[["report_date", "roe", "update_flag"]]

    def cached_quarterly_roe(
        self, symbol: str, start_year: str = "2018",
    ) -> pd.DataFrame | None:
        """只读取已有公告日 ROE 接口缓存；未命中时不触发 API。"""
        ttl = max(1, int(get_config().data.fundamental_cache_days))
        raw = self.cache.get(
            "fina_indicator", self._quarterly_roe_params(symbol, start_year), ttl,
        )
        return None if raw is None else self._normalize_quarterly_roe(raw)

    def industry_map(self) -> dict[str, str]:
        """2000 积分申万 2021 一级行业映射，原始响应缓存 30 天。

        ``index_member_all`` 单次最多 2000 行，不能无参数一次拉全市场；因此先
        取 31 个一级行业，再逐行业取最新成分。第二次及以后全部命中本地缓存。
        """
        classes = self._call(
            "index_classify", 30, level="L1", src="SW2021",
            fields="index_code,industry_name,level",
        )
        mapping: dict[str, str] = {}
        for _, row in classes.iterrows():
            code = str(row.get("index_code", ""))
            name = str(row.get("industry_name", ""))
            if not code or not name:
                continue
            try:
                members = self._call(
                    "index_member_all", 30, l1_code=code, is_new="Y",
                    fields="l1_code,l1_name,ts_code,is_new",
                )
            except Exception as exc:
                logger.warning("Tushare 申万行业 %s 成分获取失败: %s", name, exc)
                continue
            for symbol in members.get("ts_code", pd.Series(dtype=str)).dropna().astype(str):
                mapping[symbol] = name
        return mapping

    def index_weights(self, index_symbol: str, start: str, end: str) -> pd.DataFrame:
        """读取指数历史成分权重，用于 point-in-time 候选。

        官方把该接口定义为月度数据并建议按自然月请求。这里逐月拉取、去重并
        单独缓存每个月，既避免长区间响应被截断，也让失败月份可以独立重试。
        沪深300/中证500只在主代码返回空表时尝试兼容别名。
        """
        aliases = {
            "000300.SH": ("000300.SH", "399300.SZ"),
            "000905.SH": ("000905.SH", "399905.SZ"),
        }
        candidates = aliases.get(index_symbol.upper(), (index_symbol.upper(),))
        ttl = _cache_ttl(end, get_config().data.tushare_cache_days)
        start_at, end_at = pd.Timestamp(start).normalize(), pd.Timestamp(end).normalize()
        if start_at > end_at:
            raise ValueError("start 不能晚于 end")
        batches = []
        for period in pd.period_range(start_at, end_at, freq="M"):
            month_start = max(start_at, period.start_time.normalize())
            month_end = min(end_at, period.end_time.normalize())
            for requested in candidates:
                raw = self._call(
                    "index_weight", ttl, index_code=requested,
                    start_date=month_start.strftime("%Y%m%d"),
                    end_date=month_end.strftime("%Y%m%d"),
                    fields="index_code,con_code,trade_date,weight",
                )
                if not raw.empty:
                    batches.append(raw)
                    break
        if not batches:
            return pd.DataFrame(columns=["index_code", "symbol", "trade_date", "weight"])
        raw = pd.concat(batches, ignore_index=True)
        frame = raw.rename(columns={"con_code": "symbol"}).copy()
        frame["trade_date"] = _parse_tushare_dates(
            frame["trade_date"], field="trade_date"
        )
        frame["weight"] = pd.to_numeric(frame.get("weight"), errors="coerce")
        if "index_code" not in frame:
            frame["index_code"] = index_symbol.upper()
        return frame[["index_code", "symbol", "trade_date", "weight"]].dropna(
            subset=["symbol", "trade_date"]
        ).drop_duplicates(
            subset=["index_code", "symbol", "trade_date"], keep="last"
        ).sort_values(["trade_date", "symbol"])


def _official_calendar(start, end) -> list[str]:
    calendar = TushareSource().trade_calendar(start.isoformat(), end.isoformat())
    return [str(value.date()) for value in calendar]


register_official_calendar(_official_calendar)
register_index_source(TushareSource)


def _instrument_source() -> TushareSource:
    # Resolve the class at call time so the seam remains replaceable for
    # catalogue refresh tests and provider hot-swaps.
    return TushareSource()


register_instrument_source(_instrument_source)
