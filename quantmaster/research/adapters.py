"""Capability-aware cross-sectional data adapters for the research lake."""

from __future__ import annotations

import importlib.util
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any

import pandas as pd

from quantmaster.config import get_config
from quantmaster.research.catalog import ResearchCatalog
from quantmaster.research.contracts import AssetClass, CapabilityState, Frequency


@dataclass(frozen=True)
class DatasetDefinition:
    id: str
    name: str
    asset_class: AssetClass
    frequency: Frequency
    endpoint: str
    columns: tuple[str, ...]
    min_points: int = 2000
    revision_sessions: int = 5
    partitioning: str = "date"
    premium: bool = False
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["asset_class"] = self.asset_class.value
        value["frequency"] = self.frequency.value
        value["columns"] = list(self.columns)
        return value


DATASETS: tuple[DatasetDefinition, ...] = (
    DatasetDefinition(
        "stock_bars", "A 股未复权日线", AssetClass.STOCK, Frequency.DAILY, "daily",
        ("symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"),
        description="全市场按交易日抓取；另以 adj_factor 构造不可变研究价格。",
    ),
    DatasetDefinition(
        "stock_adj_factor", "A 股复权因子", AssetClass.STOCK, Frequency.DAILY, "adj_factor",
        ("symbol", "trade_date", "adj_factor"),
    ),
    DatasetDefinition(
        "stock_daily_basic", "A 股每日指标", AssetClass.STOCK, Frequency.DAILY, "daily_basic",
        ("symbol", "trade_date", "turnover_rate", "turnover_rate_f", "pe", "pe_ttm", "pb",
         "dv_ratio", "total_mv"),
    ),
    DatasetDefinition(
        "etf_bars", "场内 ETF 日线", AssetClass.ETF, Frequency.DAILY, "fund_daily",
        ("symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"),
        description="2000 积分基线使用 fund_daily；高级复权权限不是启动条件。",
    ),
    DatasetDefinition(
        "etf_basic", "场内 ETF 基础信息", AssetClass.ETF, Frequency.DAILY, "fund_basic",
        ("symbol", "trade_date", "name", "management", "custodian", "fund_type",
         "found_date", "list_date", "delist_date", "status", "invest_type", "market"),
        revision_sessions=30, partitioning="snapshot",
        description="上市场内基金快照，用于宇宙筛选和点时标识。",
    ),
    DatasetDefinition(
        "future_contracts", "期货合约目录", AssetClass.FUTURE, Frequency.DAILY, "fut_basic",
        ("symbol", "trade_date", "exchange", "fut_code", "multiplier", "list_date", "delist_date"),
        revision_sessions=30, partitioning="snapshot",
    ),
    DatasetDefinition(
        "future_bars", "期货合约日线", AssetClass.FUTURE, Frequency.DAILY, "fut_daily",
        ("symbol", "trade_date", "open", "high", "low", "close", "settle", "pre_settle",
         "volume", "amount", "open_interest"),
    ),
    DatasetDefinition(
        "future_main_mapping", "期货主力/连续映射", AssetClass.FUTURE, Frequency.DAILY,
        "fut_mapping", ("symbol", "trade_date", "mapping_ts_code"),
    ),
    DatasetDefinition(
        "stock_minutes", "A 股历史分钟", AssetClass.STOCK, Frequency.MINUTE_1, "stk_mins",
        ("symbol", "trade_date", "event_time_utc", "open", "high", "low", "close", "volume"),
        premium=True, description="需要 Tushare 单独历史分钟授权。",
    ),
    DatasetDefinition(
        "etf_minutes", "ETF 历史分钟", AssetClass.ETF, Frequency.MINUTE_1, "fund_mins",
        ("symbol", "trade_date", "event_time_utc", "open", "high", "low", "close", "volume"),
        premium=True, description="需要 Tushare 单独历史分钟授权。",
    ),
    DatasetDefinition(
        "future_minutes", "期货历史分钟", AssetClass.FUTURE, Frequency.MINUTE_1, "fut_mins",
        ("symbol", "trade_date", "event_time_utc", "open", "high", "low", "close", "volume"),
        premium=True, description="需要 Tushare 单独历史分钟授权。",
    ),
)

DATASET_BY_ID = {item.id: item for item in DATASETS}
DEFAULT_DATASETS = {
    AssetClass.STOCK: ("stock_bars", "stock_adj_factor", "stock_daily_basic"),
    AssetClass.ETF: ("etf_basic", "etf_bars"),
    AssetClass.FUTURE: ("future_contracts", "future_bars", "future_main_mapping"),
}


def dataset_catalog() -> list[dict[str, Any]]:
    return [item.to_dict() for item in DATASETS]


def _permission_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(token in message for token in (
        "权限", "积分", "permission", "privilege", "access denied", "没有访问",
    ))


class TushareResearchAdapter:
    def __init__(self, catalog: ResearchCatalog, source=None):
        self.catalog = catalog
        if source is None:
            from quantmaster.data.tushare_source import TushareSource

            source = TushareSource()
        self.source = source

    def capabilities(self) -> list[dict[str, Any]]:
        configured = bool(get_config().data.tushare_token)
        installed = importlib.util.find_spec("tushare") is not None
        saved = {item["endpoint"]: item for item in self.catalog.capabilities()}
        result = []
        for definition in DATASETS:
            cached = saved.get(definition.endpoint)
            if not configured:
                state, detail = CapabilityState.UNCONFIGURED, "尚未配置 Tushare Token"
            elif not installed:
                state, detail = CapabilityState.UNCONFIGURED, "未安装 quantmaster[tushare]"
            elif cached and cached["state"] in {
                CapabilityState.MISSING_PERMISSION.value,
                CapabilityState.TEMPORARY_FAILURE.value,
            }:
                state, detail = CapabilityState(cached["state"]), cached["detail"]
            elif definition.premium:
                state, detail = CapabilityState.MISSING_PERMISSION, definition.description
            else:
                state, detail = CapabilityState.AVAILABLE, f"按 {definition.min_points} 积分基线启用"
            result.append({
                "dataset_id": definition.id,
                "endpoint": definition.endpoint,
                "state": state.value,
                "min_points": definition.min_points,
                "premium": definition.premium,
                "detail": detail,
                "checked_at": (cached or {}).get("checked_at", ""),
            })
        return result

    def _call(self, definition: DatasetDefinition, ttl: int = 1, **params) -> pd.DataFrame:
        try:
            frame = self.source._call(definition.endpoint, ttl, **params)
        except Exception as exc:
            state = (
                CapabilityState.MISSING_PERMISSION
                if _permission_error(exc) else CapabilityState.TEMPORARY_FAILURE
            )
            self.catalog.set_capability(
                definition.endpoint, state, min_points=definition.min_points,
                detail=str(exc)[:300],
            )
            raise
        self.catalog.set_capability(
            definition.endpoint, CapabilityState.AVAILABLE,
            min_points=definition.min_points, detail="接口调用成功",
        )
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame(frame)

    def official_calendar(
        self, asset_class: AssetClass, start: str, end: str,
    ) -> tuple[pd.DatetimeIndex, str]:
        try:
            if asset_class == AssetClass.FUTURE:
                definition = DatasetDefinition(
                    "future_calendar", "期货交易日历", asset_class, Frequency.DAILY,
                    "fut_trade_cal", ("trade_date",),
                )
                frames = []
                for exchange in ("CFFEX", "DCE", "CZCE", "SHFE", "INE", "GFEX"):
                    frame = self._call(
                        definition, 7, exchange=exchange,
                        start_date=start.replace("-", ""), end_date=end.replace("-", ""),
                        is_open=1,
                    )
                    if not frame.empty:
                        frames.append(frame)
                if not frames:
                    raise RuntimeError("期货官方交易日历为空")
                values = pd.concat(frames, ignore_index=True)["cal_date"]
            else:
                source_calendar = self.source.trade_calendar(start, end)
                return source_calendar, "tushare:trade_cal"
            calendar = pd.DatetimeIndex(pd.to_datetime(values).dropna().unique()).sort_values()
            return calendar, "tushare:fut_trade_cal"
        except Exception as exc:
            return pd.bdate_range(start, end), f"fallback:business_days ({str(exc)[:120]})"

    def fetch_date(self, dataset_id: str, trade_date: str) -> pd.DataFrame:
        try:
            definition = DATASET_BY_ID[dataset_id]
        except KeyError:
            raise KeyError(f"未知研究数据集: {dataset_id}") from None
        if definition.premium:
            raise PermissionError(f"{definition.name} 需要单独权限；日线任务不受影响")
        compact = pd.Timestamp(trade_date).strftime("%Y%m%d")
        fields = self._provider_fields(definition)
        if definition.endpoint == "fut_basic":
            frames = []
            for exchange in ("CFFEX", "DCE", "CZCE", "SHFE", "INE", "GFEX"):
                frame = self._call(
                    definition, 7, exchange=exchange, fut_type="",
                    fields=("ts_code,symbol,exchange,name,fut_code,multiplier,trade_unit,"
                            "list_date,delist_date,quote_unit,per_unit,d_mode_desc"),
                )
                if not frame.empty:
                    frames.append(frame)
            raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        elif definition.endpoint == "fund_basic":
            raw = self._call(
                definition, 7, market="E", status="L", fields=fields,
            )
        else:
            raw = self._call(
                definition, 1, trade_date=compact, fields=fields,
            )
        return self._normalize(definition, raw, trade_date)

    @staticmethod
    def _provider_fields(definition: DatasetDefinition) -> str:
        mapping = {
            "stock_bars": "ts_code,trade_date,open,high,low,close,vol,amount",
            "stock_adj_factor": "ts_code,trade_date,adj_factor",
            "stock_daily_basic": (
                "ts_code,trade_date,turnover_rate,turnover_rate_f,pe,pe_ttm,pb,dv_ratio,total_mv"
            ),
            "etf_bars": "ts_code,trade_date,open,high,low,close,vol,amount",
            "etf_basic": (
                "ts_code,name,management,custodian,fund_type,found_date,list_date,"
                "delist_date,status,invest_type,market"
            ),
            "future_bars": (
                "ts_code,trade_date,pre_close,pre_settle,open,high,low,close,settle,vol,"
                "amount,oi,oi_chg,delv_settle"
            ),
            "future_main_mapping": "ts_code,trade_date,mapping_ts_code",
        }
        return mapping.get(definition.id, "")

    @staticmethod
    def _normalize(
        definition: DatasetDefinition, raw: pd.DataFrame, trade_date: str,
    ) -> pd.DataFrame:
        if raw is None or raw.empty:
            return pd.DataFrame(columns=definition.columns)
        value = raw.copy()
        if "ts_code" in value and "symbol" in value:
            value = value.rename(columns={"symbol": "contract_symbol"})
        value = value.rename(columns={
            "ts_code": "symbol", "trade_date": "trade_date", "vol": "volume",
            "oi": "open_interest", "oi_chg": "open_interest_change",
        }).copy()
        value["trade_date"] = pd.Timestamp(trade_date).normalize()
        if "symbol" in value:
            value["symbol"] = value["symbol"].astype(str).str.upper()
        if definition.id in {"stock_bars", "etf_bars"}:
            value["volume"] = pd.to_numeric(value["volume"], errors="coerce") * 100
            value["amount"] = pd.to_numeric(value["amount"], errors="coerce") * 1000
            value["research_price"] = pd.to_numeric(value["close"], errors="coerce")
            value["adjustment"] = "none"
        elif definition.id == "future_bars":
            value["volume"] = pd.to_numeric(value["volume"], errors="coerce")
            value["amount"] = pd.to_numeric(value["amount"], errors="coerce") * 10_000
        value["asset_class"] = definition.asset_class.value
        if "symbol" in value:
            value["exchange"] = value["symbol"].str.rsplit(".", n=1).str[-1]
        return value


def default_dates(start: str, end: str) -> list[str]:
    return [str(item.date()) for item in pd.bdate_range(start, end)]


def incremental_start(end: str, sessions: int = 5) -> str:
    return str((pd.Timestamp(end) - pd.tseries.offsets.BDay(max(1, sessions))).date())


def recent_probe_date() -> str:
    current = date.today()
    while current.weekday() >= 5:
        current -= timedelta(days=1)
    return str(current)
