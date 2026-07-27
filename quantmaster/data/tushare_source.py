"""Tushare 2000 积分档数据源：限速、落盘缓存、前复权日线与基本面。

当前只使用仓库 ``docs/tushare_2000_guide.md`` 明确列为 2000 积分可用的接口：
``daily``、``adj_factor``、``index_daily``、``daily_basic``、
``fina_indicator``、``index_classify``、``index_member_all`` 与 ``index_weight``。

Tushare 是 AKShare 连续重试失败后的 A 股日线备用源；每次接口响应还会按
``endpoint + params`` 缓存在 ``data/api_cache/tushare``，避免研究/回测重复
消耗调用次数。
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.base import DataSource, Market, normalize_daily
from quantmaster.data.resilience import (
    TUSHARE_LIMITER,
    EndpointFrameCache,
    provider_call,
)

logger = logging.getLogger(__name__)


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


def _is_a_share_index(symbol: str) -> bool:
    code, _, suffix = symbol.partition(".")
    return (
        (suffix.upper() == "SH" and code.startswith("000"))
        or (suffix.upper() == "SZ" and code.startswith("399"))
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
        cached = self.cache.get(endpoint, clean, ttl_days)
        if cached is not None:
            return cached.copy()
        key = endpoint + ":" + self.cache._digest(endpoint, clean)

        def fetch() -> pd.DataFrame:
            # 任务进入提供商队列后再次检查，确保排队期间已完成的相同响应直接复用。
            cached = self.cache.get(endpoint, clean, ttl_days)
            if cached is not None:
                return cached.copy()
            method = getattr(self._pro(), endpoint)
            TUSHARE_LIMITER.wait()
            cached = self.cache.get(endpoint, clean, ttl_days)
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

    def daily_indicators(
        self, symbol: str, start: str | None = None, end: str | None = None,
    ) -> pd.DataFrame:
        """2000 积分 ``daily_basic``：估值、股息率和市值。"""
        ttl = _cache_ttl(end, get_config().data.tushare_cache_days)
        raw = self._call(
            "daily_basic", ttl, ts_code=symbol,
            start_date=start.replace("-", "") if start else None,
            end_date=end.replace("-", "") if end else None,
            fields="ts_code,trade_date,pe,pe_ttm,pb,dv_ratio,total_mv",
        )
        if raw.empty:
            return pd.DataFrame()
        frame = raw.copy()
        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
        frame = frame.set_index("trade_date").sort_index()
        frame.index.name = "date"
        fields = [c for c in ("pe", "pe_ttm", "pb", "dv_ratio", "total_mv") if c in frame]
        return frame[fields].apply(pd.to_numeric, errors="coerce")

    def quarterly_roe(self, symbol: str, start_year: str = "2018") -> pd.DataFrame:
        """2000 积分 ``fina_indicator``：季度 ROE，按报告期返回。"""
        ttl = max(1, int(get_config().data.fundamental_cache_days))
        raw = self._call(
            "fina_indicator", ttl, ts_code=symbol,
            start_date=f"{start_year}0101",
            fields="ts_code,ann_date,end_date,roe,update_flag",
        )
        if raw.empty or "roe" not in raw:
            return pd.DataFrame(columns=["roe"])
        frame = raw.copy()
        frame["end_date"] = pd.to_datetime(frame["end_date"])
        if "ann_date" in frame:
            frame["ann_date"] = pd.to_datetime(frame["ann_date"], errors="coerce")
            frame = frame.sort_values(["end_date", "ann_date"])
        frame = frame.drop_duplicates("end_date", keep="last").set_index("end_date")
        frame.index.name = "report_date"
        return pd.DataFrame({"roe": pd.to_numeric(frame["roe"], errors="coerce")})

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
