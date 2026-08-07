"""Capability-aware cross-sectional data adapters for the research lake."""

from __future__ import annotations

import importlib.util
import json
from dataclasses import asdict, dataclass
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
            return pd.DatetimeIndex([]), f"fallback:unavailable ({str(exc)[:120]})"

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
                definition,
                1,
                trade_date=compact,
                fields=fields,
                required_nonempty=True,
                required_columns=("ts_code", "trade_date"),
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


class StockDBResearchAdapter:
    """Local date-partition adapter; the upstream data remains Tushare."""

    LOCAL_DATASETS = frozenset({
        "stock_bars", "stock_adj_factor", "stock_daily_basic", "etf_bars",
    })

    def __init__(
        self, catalog: ResearchCatalog, source=None, instruments=None, ingest_store=None,
    ):
        self.catalog = catalog
        if source is None:
            from quantmaster.data.free_stockdb_source import FreeStockDBSource

            source = FreeStockDBSource()
        if instruments is None:
            from quantmaster.data.instruments import InstrumentStore

            instruments = InstrumentStore()
        self.source = source
        self.instruments = instruments
        if ingest_store is None:
            from quantmaster.data.free_stockdb_ingest import StockDBIngestStore

            ingest_store = StockDBIngestStore()
        self.ingest_store = ingest_store
        self._date_cache: dict[tuple[str, str], pd.DataFrame] = {}

    def capabilities(self) -> list[dict[str, Any]]:
        available = bool(getattr(self.source, "sdk_path", ""))
        ready_names = {
            name
            for snapshot in self.ingest_store.history(30)
            if snapshot.status == "complete"
            for name in snapshot.content_hashes
        }
        content_name = {
            "stock_bars": "stock_daily", "stock_adj_factor": "stock_adjustment_factors",
            "stock_daily_basic": "stock_daily", "etf_bars": "etf_daily",
        }
        return [{
            "dataset_id": definition.id, "endpoint": f"stockdb:{definition.endpoint}",
            "state": (
                CapabilityState.DATA_READY.value
                if content_name.get(definition.id) in ready_names
                else CapabilityState.INSTALLED.value
                if available and definition.id in self.LOCAL_DATASETS
                else CapabilityState.UNAVAILABLE.value
            ),
            "min_points": 0, "premium": False,
            "detail": (
                "Tushare 上游经 free-stockdb 本地分发；首次读取后升级为 data_ready"
                if definition.id in self.LOCAL_DATASETS else "该数据集仍由 Tushare 直连接口提供"
            ),
            "upstream": "tushare", "distribution": "free-stockdb",
            "independent_cross_validation": False, "checked_at": "",
        } for definition in DATASETS]

    def _asset_frame(self, asset: AssetClass, trade_date: str) -> pd.DataFrame:
        key = (asset.value, trade_date)
        if key in self._date_cache:
            return self._date_cache[key].copy()
        content_name = "etf_daily" if asset == AssetClass.ETF else "stock_daily"
        target_date = pd.Timestamp(trade_date).normalize()
        for snapshot in self.ingest_store.history(30):
            if content_name not in snapshot.content_hashes:
                continue
            if not (snapshot.start_date <= trade_date <= snapshot.end_date):
                continue
            cached = self.ingest_store.load_frame(snapshot, content_name)
            if cached.empty or "date" not in cached:
                continue
            cached = cached[pd.to_datetime(cached["date"], errors="coerce").dt.normalize().eq(
                target_date
            )].copy()
            if not cached.empty:
                cached["ingest_id"] = snapshot.ingest_id
                self._date_cache[key] = cached
                return cached.copy()
        active = {"listed", "active", "l"}
        if asset == AssetClass.STOCK:
            instruments = [item for item in self.instruments.list(market="CN", asset_type="stock")
                           if item.status.casefold() in active]
        else:
            from quantmaster.rotation.etf_research import is_exchange_etf

            instruments = [item for item in self.instruments.list(market="CN") if is_exchange_etf(item)]
        symbols = [item.symbol for item in instruments]
        frames = []
        for offset in range(0, len(symbols), 300):
            frames.append(self.source.daily_cross_section(
                symbols[offset:offset + 300], trade_date, trade_date,
            ))
        frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        self._date_cache[key] = frame.copy()
        return frame

    def fetch_date(self, dataset_id: str, trade_date: str) -> pd.DataFrame:
        definition = DATASET_BY_ID[dataset_id]
        if dataset_id not in self.LOCAL_DATASETS:
            return pd.DataFrame(columns=definition.columns)
        if dataset_id == "stock_adj_factor":
            target_date = pd.Timestamp(trade_date).normalize()
            for snapshot in self.ingest_store.history(30):
                if "stock_adjustment_factors" not in snapshot.content_hashes:
                    continue
                raw = self.ingest_store.load_frame(snapshot, "stock_adjustment_factors")
                if raw.empty:
                    continue
                raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
                # Factors are sparse corporate-action observations.  Point-in-time
                # research needs the effective value as of the requested session.
                raw = raw[raw["date"].dt.normalize().le(target_date)]
                if raw.empty:
                    continue
                value = raw.sort_values("date").groupby("symbol", as_index=False).tail(1)
                value = value.rename(columns={"date": "factor_observed_date"})
                value["trade_date"] = target_date
                value["upstream"] = "tushare"
                value["distribution"] = "free-stockdb"
                value["ingest_id"] = snapshot.ingest_id
                value["field_provenance"] = json.dumps({
                    "adj_factor": "tushare:via-free-stockdb",
                }, ensure_ascii=False, sort_keys=True)
                return value[[
                    "symbol", "trade_date", "adj_factor", "factor_observed_date",
                    "upstream", "distribution", "ingest_id", "field_provenance",
                ]]
            return pd.DataFrame(columns=definition.columns)
        asset = AssetClass.ETF if dataset_id == "etf_bars" else AssetClass.STOCK
        raw = self._asset_frame(asset, trade_date)
        if raw.empty:
            return pd.DataFrame(columns=definition.columns)
        value = raw.rename(columns={"date": "trade_date"}).copy()
        value["trade_date"] = pd.Timestamp(trade_date).normalize()
        value["upstream"] = "tushare"
        value["distribution"] = "free-stockdb"
        if dataset_id in {"stock_bars", "etf_bars"}:
            keep = [*definition.columns, "pre_close", "pct_chg", "amplitude",
                    "turnover", "vol_ratio", "total_share", "float_share"]
            value["research_price"] = pd.to_numeric(value["close"], errors="coerce")
            value["adjustment"] = "none"
        else:
            value = value.rename(columns={"turnover": "turnover_rate"})
            keep = [*definition.columns, "float_mv", "total_share", "float_share", "vol_ratio"]
        value["field_provenance"] = json.dumps({
            column: "tushare:via-free-stockdb" for column in keep if column in value
        }, ensure_ascii=False, sort_keys=True)
        keep = [column for column in (*keep, "research_price", "adjustment", "upstream",
                                      "distribution", "ingest_id", "field_provenance")
                if column in value]
        return value[keep].drop_duplicates(["trade_date", "symbol"], keep="last")


class CompositeResearchAdapter:
    """Prefer verified local distribution, then disclose direct-Tushare fallback rows."""

    def __init__(
        self, catalog: ResearchCatalog, local: StockDBResearchAdapter | None = None,
        direct: TushareResearchAdapter | None = None,
    ):
        self.catalog = catalog
        self.local = local or StockDBResearchAdapter(catalog)
        self.direct = direct or TushareResearchAdapter(catalog)

    def capabilities(self) -> list[dict[str, Any]]:
        local = {item["dataset_id"]: item for item in self.local.capabilities()}
        direct = {item["dataset_id"]: item for item in self.direct.capabilities()}
        result = []
        for definition in DATASETS:
            result.append({
                **direct[definition.id],
                "routes": [local[definition.id], direct[definition.id]],
                "preferred_route": (
                    "tushare:via-free-stockdb" if definition.id in self.local.LOCAL_DATASETS
                    else "tushare:direct"
                ),
                "independent_cross_validation": False,
            })
        return result

    def official_calendar(
        self, asset_class: AssetClass, start: str, end: str,
    ) -> tuple[pd.DatetimeIndex, str]:
        return self.direct.official_calendar(asset_class, start, end)

    def fetch_date(self, dataset_id: str, trade_date: str) -> pd.DataFrame:
        if dataset_id not in self.local.LOCAL_DATASETS:
            value = self.direct.fetch_date(dataset_id, trade_date)
            if not value.empty:
                value["upstream"] = "tushare"
                value["distribution"] = "direct"
            return value
        try:
            local = self.local.fetch_date(dataset_id, trade_date)
        except (OSError, RuntimeError, TypeError, ValueError):
            local = pd.DataFrame()
        if local.empty:
            direct = self.direct.fetch_date(dataset_id, trade_date)
            if not direct.empty:
                direct["upstream"] = "tushare"
                direct["distribution"] = "direct-fallback"
                direct["field_provenance"] = json.dumps({
                    column: "tushare:direct" for column in direct.columns
                }, ensure_ascii=False, sort_keys=True)
            return direct
        # A local partial partition is publishable only after explicit direct
        # completion. Rows retain their distribution route, so mixing is visible.
        expected_asset = "etf" if dataset_id == "etf_bars" else "stock"
        try:
            from quantmaster.rotation.etf_research import is_exchange_etf

            universe = self.local.instruments.list(market="CN")
            expected = {
                item.symbol for item in universe
                if (is_exchange_etf(item) if expected_asset == "etf" else item.asset_type == "stock")
                and item.status.casefold() in {"listed", "active", "l"}
            }
        except (AttributeError, OSError, RuntimeError):
            expected = set(local["symbol"].astype(str))
        observed = set(local["symbol"].astype(str))
        if not expected or len(observed) / len(expected) >= 0.98:
            return local
        direct = self.direct.fetch_date(dataset_id, trade_date)
        missing = direct[~direct["symbol"].astype(str).isin(observed)].copy() if not direct.empty else direct
        if missing.empty:
            return local
        missing["upstream"] = "tushare"
        missing["distribution"] = "direct-fallback"
        missing["field_provenance"] = json.dumps({
            column: "tushare:direct" for column in missing.columns
        }, ensure_ascii=False, sort_keys=True)
        return pd.concat((local, missing), ignore_index=True, sort=False)
