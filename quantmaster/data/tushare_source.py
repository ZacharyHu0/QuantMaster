"""Tushare 2000 积分档数据源：限速、落盘缓存、前复权日线与基本面。

当前只使用仓库 ``docs/tushare_2000_guide.md`` 明确列为 2000 积分可用的接口：
``daily``、``adj_factor``、``index_daily``、``daily_basic``、
``fina_indicator``、``stk_limit``、``suspend_d``、``trade_cal``、
``index_classify``、``index_member_all`` 与 ``index_weight``。

Tushare 是 AKShare 连续重试失败后的 A 股日线备用源；每次接口响应还会按
``endpoint + params`` 缓存在 ``data/api_cache/tushare``，避免研究/回测重复
消耗调用次数。
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from datetime import time as datetime_time
from zoneinfo import ZoneInfo

import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.base import DataSource, Market, normalize_daily
from quantmaster.data.resilience import (
    TUSHARE_LIMITER,
    EndpointFrameCache,
    endpoint_cache_bypassed,
    provider_call,
)

logger = logging.getLogger(__name__)
_CHINA_TZ = ZoneInfo("Asia/Shanghai")


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


def _instrument_type(symbol: str) -> str:
    try:
        from quantmaster.data.instruments import InstrumentStore

        instrument = InstrumentStore().get(symbol)
        return instrument.asset_type if instrument else ""
    except Exception:
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
            if end_date < date.today() - timedelta(days=3):
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

    def __init__(self, cache: EndpointFrameCache | None = None):
        self.cache = cache or EndpointFrameCache("tushare")
        self._api = None

    def _pro(self):
        if self._api is None:
            self._api = _require_tushare()
        return self._api

    def _call(self, endpoint: str, ttl_days: int, **params) -> pd.DataFrame:
        clean = {key: value for key, value in params.items() if value not in (None, "")}
        force_refresh = endpoint_cache_bypassed()
        min_mtime = _current_session_cache_floor(clean.get("end_date"))

        def read_cache() -> pd.DataFrame | None:
            if force_refresh:
                return None
            return self.cache.get(
                endpoint, clean, ttl_days, min_mtime=min_mtime)

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
            result = method(**clean)
            frame = result if isinstance(result, pd.DataFrame) else pd.DataFrame(result)
            self.cache.put(endpoint, clean, frame)
            return frame.copy()

        return provider_call("tushare", key, fetch)

    @staticmethod
    def _normalize_market_frame(raw: pd.DataFrame) -> pd.DataFrame:
        if raw is None or raw.empty:
            return pd.DataFrame()
        frame = raw.rename(columns={"trade_date": "date", "vol": "volume"}).copy()
        # Tushare 日线 volume=手、amount=千元；统一成股、元，与 AKShare 一致。
        if "volume" in frame:
            frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce") * 100
        if "amount" in frame:
            frame["amount"] = pd.to_numeric(frame["amount"], errors="coerce") * 1000
        return normalize_daily(frame)

    def daily(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        """A 股前复权日线，或沪深指数日线。

        股票使用 ``daily + adj_factor`` 在本地计算前复权，避免旧实现的
        Tushare 未复权价格与 AKShare qfq 缓存口径不一致。
        """
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
            factors[["trade_date", "adj_factor"]], on="trade_date", how="inner")
        merged["trade_date"] = pd.to_datetime(merged["trade_date"])
        merged = merged.sort_values("trade_date")
        factor = pd.to_numeric(merged["adj_factor"], errors="coerce")
        latest = factor.dropna().iloc[-1] if factor.notna().any() else None
        if latest is None or latest == 0:
            raise RuntimeError(f"Tushare {symbol} 复权因子为空")
        ratio = factor / latest
        for column in ("open", "high", "low", "close"):
            merged[column] = pd.to_numeric(merged[column], errors="coerce") * ratio
        return self._normalize_market_frame(merged).loc[start:end]

    def research_daily(self, symbol: str, start: str, end: str) -> dict[str, pd.DataFrame]:
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
        merged["trade_date"] = pd.to_datetime(merged["trade_date"])
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
        suspension_index = self.trade_calendar(start, end).union(merged.index).sort_values()
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
        return pd.DatetimeIndex(pd.to_datetime(enabled["cal_date"])).normalize().sort_values()

    def daily_indicators(
        self, symbol: str, start: str | None = None, end: str | None = None,
    ) -> pd.DataFrame:
        """2000 积分 ``daily_basic``：估值、股息率和市值。"""
        ttl = _cache_ttl(end, get_config().data.tushare_cache_days)
        params = self._daily_indicator_params(symbol, start, end)
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
        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
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

    def instrument_catalog(self) -> list[dict]:
        """读取内地股票/场内基金/指数及港股目录，供证券主数据增量更新。"""
        records: list[dict] = []

        def text(value) -> str:
            return "" if pd.isna(value) else str(value).strip()

        stocks = self._call(
            "stock_basic", 7, exchange="", list_status="L",
            fields=("ts_code,symbol,name,fullname,enname,exchange,curr_type,"
                    "list_status,list_date,delist_date"),
        )
        for row in stocks.to_dict("records"):
            symbol = text(row.get("ts_code")).upper()
            if symbol:
                records.append({
                    "symbol": symbol, "provider_symbol": symbol,
                    "name": text(row.get("name")), "full_name": text(row.get("fullname")),
                    "en_name": text(row.get("enname")), "market": "CN",
                    "exchange": symbol.rsplit(".", 1)[-1], "asset_type": "stock",
                    "currency": text(row.get("curr_type")) or "CNY",
                    "status": text(row.get("list_status")) or "L",
                    "list_date": text(row.get("list_date")),
                    "delist_date": text(row.get("delist_date")),
                })
        funds = self._call("fund_basic", 7, market="E", status="L")
        for row in funds.to_dict("records"):
            symbol = text(row.get("ts_code")).upper()
            name, fund_type = text(row.get("name")), text(row.get("fund_type")).upper()
            if symbol and name:
                records.append({
                    "symbol": symbol, "name": name, "market": "CN",
                    "exchange": symbol.rsplit(".", 1)[-1],
                    "asset_type": (
                        "etf" if "ETF" in fund_type or "ETF" in name.upper()
                        or "交易型" in fund_type else "fund"
                    ),
                    "currency": "CNY", "status": "L",
                    "list_date": text(row.get("list_date")),
                })
        for market in ("CSI", "SSE", "SZSE"):
            indexes = self._call("index_basic", 7, market=market)
            for row in indexes.to_dict("records"):
                symbol, name = text(row.get("ts_code")).upper(), text(row.get("name"))
                if symbol and name:
                    records.append({
                        "symbol": symbol, "name": name,
                        "full_name": text(row.get("fullname")), "market": "CN",
                        "exchange": symbol.rsplit(".", 1)[-1], "asset_type": "index",
                        "currency": "CNY", "status": "listed",
                    })
        hong_kong = self._call("hk_basic", 7, list_status="L")
        for row in hong_kong.to_dict("records"):
            provider = text(row.get("ts_code")).upper()
            code = (text(row.get("symbol")) or provider.partition(".")[0]).zfill(5)
            name = text(row.get("name"))
            if code and name:
                records.append({
                    "symbol": f"{code}.HK", "provider_symbol": provider,
                    "name": name, "full_name": text(row.get("fullname")),
                    "en_name": text(row.get("enname")), "market": "HK",
                    "exchange": "HKEX", "asset_type": "stock", "currency": "HKD",
                    "status": "L", "list_date": text(row.get("list_date")),
                    "delist_date": text(row.get("delist_date")),
                })
        return records

    def quarterly_roe(self, symbol: str, start_year: str = "2018") -> pd.DataFrame:
        """2000 积分 ``fina_indicator``：保留公告日与修订序列的 PIT ROE。"""
        ttl = max(1, int(get_config().data.fundamental_cache_days))
        params = self._quarterly_roe_params(symbol, start_year)
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
        frame["report_date"] = pd.to_datetime(frame["end_date"], errors="coerce")
        frame["ann_date"] = pd.to_datetime(frame.get("ann_date"), errors="coerce")
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
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
        frame["weight"] = pd.to_numeric(frame.get("weight"), errors="coerce")
        if "index_code" not in frame:
            frame["index_code"] = index_symbol.upper()
        return frame[["index_code", "symbol", "trade_date", "weight"]].dropna(
            subset=["symbol", "trade_date"]
        ).drop_duplicates(
            subset=["index_code", "symbol", "trade_date"], keep="last"
        ).sort_values(["trade_date", "symbol"])
