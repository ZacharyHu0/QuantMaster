"""All-exchange ETF research backed by local Tushare-distributed stockdb data."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import threading
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.free_stockdb_ingest import (
    STOCKDB_INGEST_SCHEMA_VERSION,
    StockDBIngestService,
    StockDBIngestStore,
)
from quantmaster.data.free_stockdb_source import FreeStockDBSource
from quantmaster.data.instruments import Instrument, InstrumentStore
from quantmaster.research.contracts import content_hash
from quantmaster.rotation.etf_models import (
    ETF_RESEARCH_MODEL_VERSION,
    ETF_SCHEMA_VERSION,
    EtfProfile,
    EtfResearchItem,
    EtfResearchSnapshot,
)
from quantmaster.rotation.etf_v2 import (
    ETF_CATEGORIES,
    adjusted_daily_metrics,
    build_sector_research,
    classify_etf_profile,
    fund_evidence,
)

Progress = Callable[[int, str, str], None]
Cancelled = Callable[[], bool]


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(fd)
    temp = Path(temp_name)
    try:
        temp.write_text(value, encoding="utf-8")
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def classify_etf(
    name: str,
    *,
    benchmark: str = "",
    fund_type: str = "",
    invest_type: str = "",
) -> tuple[str, tuple[str, ...]]:
    taxonomy = classify_etf_profile(
        name,
        benchmark=benchmark,
        fund_type=fund_type,
        invest_type=invest_type,
    )
    return taxonomy["category"], taxonomy["classification_evidence"]


def _frame_hash(frame: pd.DataFrame, columns: tuple[str, ...]) -> str:
    if frame is None or frame.empty:
        return content_hash([])
    selected = frame[[column for column in columns if column in frame]].copy()
    for column in selected:
        if pd.api.types.is_datetime64_any_dtype(selected[column]):
            selected[column] = selected[column].dt.strftime("%Y-%m-%d")
    selected = selected.sort_values(list(selected.columns)).reset_index(drop=True)
    hashed = pd.util.hash_pandas_object(selected, index=False).values.tobytes()
    return hashlib.sha256(hashed).hexdigest()


def is_exchange_etf(instrument: Instrument) -> bool:
    if instrument.exchange not in {"SH", "SZ"}:
        return False
    if instrument.status.casefold() not in {"listed", "active", "l"}:
        return False
    text = instrument.name.upper()
    if "LOF" in text or "联接" in text:
        return False
    return instrument.asset_type == "etf" or (
        instrument.asset_type == "fund" and ("ETF" in text or "交易型" in text)
    )


class EtfResearchStore:
    def __init__(self, root: str | Path | None = None):
        self.root = Path(root or (get_config().data_root / "etf-research")).resolve()
        self._lock = threading.RLock()
        self._cache: dict[str, tuple[int, int, EtfResearchSnapshot]] = {}

    def publish(self, snapshot: EtfResearchSnapshot) -> EtfResearchSnapshot:
        encoded = json.dumps(snapshot.to_dict(), ensure_ascii=False, sort_keys=True, indent=2, default=str)
        with self._lock:
            target = self.root / "snapshots" / f"{snapshot.snapshot_id}.json"
            if target.exists():
                existing = EtfResearchSnapshot.from_dict(json.loads(target.read_text(encoding="utf-8")))
                identity = (
                    "ingest_id",
                    "input_hash",
                    "research_model_version",
                    "schema_version",
                )
                if any(getattr(existing, key) != getattr(snapshot, key) for key in identity):
                    raise RuntimeError(f"ETF 研究快照不可变: {snapshot.snapshot_id}")
                snapshot = existing
            else:
                _atomic_text(target, encoded)
            _atomic_text(
                self.root / "latest.json",
                json.dumps(
                    {
                        "snapshot_id": snapshot.snapshot_id,
                        "last_failure": {},
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
            )
            self._cache.clear()
            stat = target.stat()
            self._cache[snapshot.snapshot_id] = (stat.st_mtime_ns, stat.st_size, snapshot)
            snapshots_root = self.root / "snapshots"
            for path in snapshots_root.glob("*.json"):
                if path == target:
                    continue
                try:
                    value = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, ValueError, json.JSONDecodeError):
                    continue
                if (
                    value.get("schema_version") != ETF_SCHEMA_VERSION
                    or value.get("research_model_version") != ETF_RESEARCH_MODEL_VERSION
                ):
                    path.unlink(missing_ok=True)
            return snapshot

    def get(self, snapshot_id: str) -> EtfResearchSnapshot | None:
        path = self.root / "snapshots" / f"{snapshot_id}.json"
        try:
            stat = path.stat()
            signature = (stat.st_mtime_ns, stat.st_size)
            with self._lock:
                cached = self._cache.get(snapshot_id)
                if cached is not None and cached[:2] == signature:
                    return cached[2]
            value = json.loads(path.read_text(encoding="utf-8"))
            snapshot = EtfResearchSnapshot.from_dict(value)
            with self._lock:
                self._cache[snapshot_id] = (*signature, snapshot)
            return snapshot
        except (FileNotFoundError, OSError, TypeError, ValueError, json.JSONDecodeError):
            # Obsolete contracts are deliberately unavailable instead of being reinterpreted.
            return None

    def latest(self) -> EtfResearchSnapshot | None:
        try:
            state = json.loads((self.root / "latest.json").read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
            return None
        snapshot = self.get(str(state.get("snapshot_id") or ""))
        failure = state.get("last_failure") or {}
        if snapshot is not None and failure:
            data = snapshot.to_dict()
            data["staleness"] = {
                "stale": True,
                "reason": str(failure.get("reason") or "ETF 研究刷新失败"),
                "last_attempt_at": str(failure.get("attempted_at") or ""),
            }
            return EtfResearchSnapshot.from_dict(data)
        return snapshot

    def history(self, limit: int = 50) -> list[dict[str, Any]]:
        paths = sorted(
            (self.root / "snapshots").glob("*.json"), key=lambda item: item.stat().st_mtime_ns, reverse=True
        )
        result = []
        for path in paths:
            if len(result) >= max(1, limit):
                break
            try:
                snapshot = self.get(path.stem)
                if snapshot is None:
                    continue
                result.append(
                    {
                        "snapshot_id": snapshot.snapshot_id,
                        "ingest_id": snapshot.ingest_id,
                        "as_of_date": snapshot.as_of_date,
                        "generated_at": snapshot.generated_at,
                        "coverage": snapshot.coverage,
                        "item_count": len(snapshot.items),
                        "categories": snapshot.categories,
                    }
                )
            except (OSError, KeyError, ValueError, json.JSONDecodeError):
                continue
        return result

    def record_failure(self, reason: str) -> None:
        from datetime import UTC, datetime

        latest = self.latest()
        _atomic_text(
            self.root / "latest.json",
            json.dumps(
                {
                    "snapshot_id": latest.snapshot_id if latest else "",
                    "last_failure": {
                        "reason": reason[:500],
                        "attempted_at": datetime.now(UTC).isoformat(timespec="seconds"),
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
        )


class EtfResearchService:
    def __init__(
        self,
        *,
        source: FreeStockDBSource | None = None,
        instruments: InstrumentStore | None = None,
        ingest_store: StockDBIngestStore | None = None,
        store: EtfResearchStore | None = None,
    ):
        self.source = source or FreeStockDBSource()
        self.instruments = instruments or InstrumentStore()
        self.ingest_store = ingest_store or StockDBIngestStore()
        self.store = store or EtfResearchStore()
        self._profile_capabilities: dict[str, Any] = {}
        self._detail_history_cache: dict[tuple[str, str, str], list[dict[str, Any]]] = {}

    @staticmethod
    def _official_metadata() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
        cfg = get_config().data
        if not cfg.tushare_token:
            return {}, {
                "status": "fallback",
                "source": "quantmaster:local-rules",
                "reason": "当前进程未读取到 Tushare 凭据，使用本地证券主表与显式主题词典",
            }
        try:
            from quantmaster.data.tushare_source import TushareSource

            source = TushareSource()
            basic = source._call(
                "etf_basic",
                7,
                list_status="L",
                fields=(
                    "ts_code,extname,cname,index_code,index_name,list_date,list_status,"
                    "exchange,mgr_name,custod_name,mgt_fee,etf_type"
                ),
            )
        except (ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
            return {}, {
                "status": "fallback",
                "source": "quantmaster:local-rules",
                "reason": f"etf_basic 不可用：{str(exc)[:160]}",
            }
        if basic.empty or "ts_code" not in basic:
            return {}, {
                "status": "fallback",
                "source": "quantmaster:local-rules",
                "reason": "etf_basic 返回空目录",
            }
        benchmark_rows: dict[str, dict[str, Any]] = {}
        benchmark_capability: dict[str, Any]
        try:
            benchmarks = source._call(
                "mkt_idx_bmk",
                5,
                fields="ts_code,name,fullname,bmk_level,bmk_type,bmk_src,idx_type",
            )
            if not benchmarks.empty and "ts_code" in benchmarks:
                benchmark_rows = {
                    str(row.get("ts_code") or "").upper(): row
                    for row in benchmarks.to_dict("records")
                    if row.get("ts_code")
                }
            benchmark_capability = {
                "status": "ready" if benchmark_rows else "fallback",
                "source": "tushare:mkt_idx_bmk",
                "covered_indices": len(benchmark_rows),
                "reason": ("官方业绩基准分类可用" if benchmark_rows else "mkt_idx_bmk 返回空目录"),
            }
        except (ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
            benchmark_capability = {
                "status": "fallback",
                "source": "quantmaster:explicit-rules",
                "covered_indices": 0,
                "reason": f"mkt_idx_bmk 不可用：{str(exc)[:160]}",
            }
        metadata: dict[str, dict[str, Any]] = {}
        for row in basic.to_dict("records"):
            symbol = str(row.get("ts_code") or "").upper()
            if not symbol:
                continue
            benchmark = benchmark_rows.get(str(row.get("index_code") or "").upper(), {})
            metadata[symbol] = {
                "name": str(row.get("extname") or row.get("cname") or ""),
                "benchmark_code": str(row.get("index_code") or ""),
                "index_name": str(row.get("index_name") or ""),
                "benchmark_type": str(benchmark.get("bmk_type") or ""),
                "benchmark_level": str(benchmark.get("bmk_level") or ""),
                "index_type": str(benchmark.get("idx_type") or ""),
                "index_provider": str(benchmark.get("bmk_src") or ""),
                "manager": str(row.get("mgr_name") or ""),
                "custodian": str(row.get("custod_name") or ""),
                "management_fee": pd.to_numeric(pd.Series([row.get("mgt_fee")]), errors="coerce").iloc[0],
                "etf_type": str(row.get("etf_type") or ""),
                "list_date": str(row.get("list_date") or ""),
            }
        return metadata, {
            "status": "ready",
            "source": "tushare:etf_basic",
            "covered_symbols": len(metadata),
            "reason": "官方 ETF 基础信息可用",
            "benchmark_classification": benchmark_capability,
        }

    def profiles(self) -> list[EtfProfile]:
        observations = self._direct_share_observations()
        share_metadata: dict[str, dict[str, str]] = {}
        if not observations.empty:
            for symbol, group in observations.groupby("symbol"):
                last = group.sort_values("trade_date").iloc[-1]
                share_metadata[str(symbol).upper()] = {
                    key: str(last.get(key) or "") for key in ("benchmark", "fund_type", "invest_type")
                }
        cached = pd.DataFrame()
        try:
            from quantmaster.rotation.store import RotationStore

            cached = RotationStore().etf_metadata()
        except (ImportError, OSError, RuntimeError, TypeError, ValueError):
            cached = pd.DataFrame()
        directory: dict[str, dict[str, Any]] = {}
        if not cached.empty and "symbol" in cached:
            cached = cached.copy()
            cached["symbol"] = cached["symbol"].astype(str).str.upper()
            directory = {
                str(row.get("symbol") or "").upper(): row
                for row in cached.to_dict("records")
                if row.get("symbol")
            }
            sources = sorted(
                {
                    str(value)
                    for value in cached.get("metadata_source", pd.Series(dtype=str)).dropna()
                    if str(value)
                }
            )
            source_values = cached.get("metadata_source", pd.Series("", index=cached.index)).astype(str)
            official_covered = int(source_values.str.startswith("tushare:", na=False).sum())
            enhanced_covered = int(
                source_values.str.contains("tushare:etf_basic", na=False).sum()
            )
            benchmark_covered = int(
                (
                    cached.get("benchmark_code", pd.Series("", index=cached.index))
                    .fillna("")
                    .astype(str)
                    .str.strip()
                    .ne("")
                    | cached.get("benchmark", pd.Series("", index=cached.index))
                    .fillna("")
                    .astype(str)
                    .str.strip()
                    .ne("")
                ).sum()
            )
            self._profile_capabilities = {
                "status": "ready" if directory else "fallback",
                "source": ", ".join(sources) or "ETF 元数据缓存",
                "covered_symbols": len(directory),
                "official_covered_symbols": official_covered,
                "enhanced_covered_symbols": enhanced_covered,
                "benchmark_covered_symbols": benchmark_covered,
                "reason": (
                    "本地全市场 ETF 目录与 Tushare 官方基金目录可用；etf_basic 增强权限不足"
                    if official_covered >= len(directory) and enhanced_covered < len(directory)
                    else "本地全市场 ETF 目录可用；官方字段按可用权限增强"
                    if official_covered < len(directory)
                    else "官方 ETF 增强信息已覆盖本地全市场目录"
                ),
            }
        else:
            directory, capability = self._official_metadata()
            self._profile_capabilities = capability
        result = []
        for instrument in self.instruments.list(market="CN"):
            if not is_exchange_etf(instrument):
                continue
            extra = share_metadata.get(instrument.symbol, {})
            rich = directory.get(instrument.symbol, {})
            raw_source = str(rich.get("metadata_source") or self._profile_capabilities.get("source") or "")
            source = (
                "etf_basic"
                if "etf_basic" in raw_source
                else "fund_basic"
                if "tushare:fund_basic" in raw_source
                else "local_stockdb"
            )
            benchmark = str(rich.get("benchmark") or extra.get("benchmark") or "")
            index_name = str(rich.get("index_name") or rich.get("benchmark") or "")
            taxonomy = classify_etf_profile(
                instrument.name,
                benchmark=benchmark,
                benchmark_code=str(rich.get("benchmark_code") or ""),
                index_name=index_name,
                fund_type=str(rich.get("fund_type") or extra.get("fund_type") or ""),
                invest_type=str(rich.get("invest_type") or extra.get("invest_type") or ""),
                etf_type=str(rich.get("etf_type") or ""),
                benchmark_type=str(rich.get("benchmark_type") or ""),
                index_type=str(rich.get("index_type") or ""),
                metadata_source=source,
            )
            fee = rich.get("management_fee", rich.get("mgt_fee"))
            numeric_fee = pd.to_numeric(pd.Series([fee]), errors="coerce").iloc[0]
            result.append(
                EtfProfile(
                    symbol=instrument.symbol,
                    name=instrument.name,
                    category=taxonomy["category"],
                    asset_class=taxonomy["asset_class"],
                    sector_id=taxonomy["sector_id"],
                    sector_name=taxonomy["sector_name"],
                    benchmark=benchmark,
                    benchmark_code=str(rich.get("benchmark_code") or ""),
                    benchmark_type=str(rich.get("benchmark_type") or ""),
                    benchmark_level=str(rich.get("benchmark_level") or ""),
                    index_type=str(rich.get("index_type") or ""),
                    index_provider=str(rich.get("index_provider") or ""),
                    normalized_index=taxonomy["normalized_index"],
                    fund_type=str(rich.get("fund_type") or extra.get("fund_type") or ""),
                    invest_type=str(rich.get("invest_type") or extra.get("invest_type") or ""),
                    manager=str(rich.get("manager") or rich.get("mgr_name") or ""),
                    custodian=str(rich.get("custodian") or rich.get("custod_name") or ""),
                    management_fee=float(numeric_fee) if pd.notna(numeric_fee) else None,
                    metadata_source=source,
                    classification_source=taxonomy["classification_source"],
                    classification_confidence=taxonomy["classification_confidence"],
                    list_date=str(rich.get("list_date") or instrument.list_date),
                    status=instrument.status,
                    classification_evidence=taxonomy["classification_evidence"],
                )
            )
        return sorted(result, key=lambda item: item.symbol)

    @staticmethod
    def _direct_share_observations() -> pd.DataFrame:
        try:
            from quantmaster.rotation.store import RotationStore

            return RotationStore().etf_observations()
        except (ImportError, OSError, RuntimeError, TypeError, ValueError):
            return pd.DataFrame()

    @staticmethod
    def _direct_metadata() -> pd.DataFrame:
        try:
            from quantmaster.rotation.store import RotationStore

            return RotationStore().etf_metadata()
        except (ImportError, OSError, RuntimeError, TypeError, ValueError):
            return pd.DataFrame()

    def _adjustment_factors(
        self,
        daily: pd.DataFrame,
        *,
        progress: Progress,
        cancelled: Cancelled,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        target = self.store.root / "evidence" / "adjustment_factors.parquet"
        try:
            cached = pd.read_parquet(target) if target.is_file() else pd.DataFrame()
        except (OSError, ValueError):
            cached = pd.DataFrame()
        if not cached.empty:
            cached["date"] = pd.to_datetime(cached.get("date"), errors="coerce")
            cached["symbol"] = cached.get("symbol", pd.Series(dtype=str)).astype(str).str.upper()
            cached["adj_factor"] = pd.to_numeric(cached.get("adj_factor"), errors="coerce")
            if "source" not in cached:
                cached["source"] = "verified:legacy-factor-cache"
            cached = cached.dropna(subset=["date", "symbol", "adj_factor"])

        if "adj_factor" in daily:
            embedded = daily[["symbol", "date", "adj_factor"]].copy()
            embedded["date"] = pd.to_datetime(embedded["date"], errors="coerce")
            embedded["adj_factor"] = pd.to_numeric(embedded["adj_factor"], errors="coerce")
            embedded["source"] = "free-stockdb:embedded-factor"
            cached = pd.concat([cached, embedded], ignore_index=True)

        grid = daily[["symbol", "date"]].copy()
        grid["symbol"] = grid["symbol"].astype(str).str.upper()
        grid["date"] = pd.to_datetime(grid["date"], errors="coerce").dt.normalize()
        grid = grid.dropna().drop_duplicates().sort_values(["symbol", "date"])
        dates = sorted(grid["date"].unique())
        symbols = sorted(daily["symbol"].dropna().astype(str).str.upper().unique())
        start = pd.Timestamp(dates[0]).date().isoformat() if dates else ""
        end = pd.Timestamp(dates[-1]).date().isoformat() if dates else ""
        expected = grid.groupby("symbol")["date"].nunique()

        def missing_symbols(frame: pd.DataFrame) -> list[str]:
            counts = (
                frame.groupby("symbol")["date"].nunique()
                if frame is not None and not frame.empty
                else pd.Series(dtype=int)
            )
            return [
                symbol
                for symbol in symbols
                if int(counts.get(symbol, 0)) < max(1, round(int(expected.get(symbol, 0)) * 0.95))
            ]

        missing = missing_symbols(cached)
        capability: dict[str, Any] = {
            "status": "ready" if not missing else "partial",
            "source": "adjustment-factor-cache",
            "covered_symbols": len(symbols) - len(missing),
            "expected_symbols": len(symbols),
            "reason": "可核查复权因子已覆盖研究窗口" if not missing else "复权因子缓存覆盖不足",
        }

        if missing and start and end:
            try:
                local_events = self.source.adjustment_factors(missing, start, end)
                if local_events.empty and not bool(local_events.attrs.get("authoritative")):
                    raise RuntimeError("stockdb 未确认累计复权事件表可用")
                local_events = local_events.copy()
                local_events["symbol"] = local_events.get(
                    "symbol", pd.Series(dtype=str)
                ).astype(str).str.upper()
                local_events["date"] = pd.to_datetime(
                    local_events.get("date"), errors="coerce"
                ).dt.normalize()
                local_events["adj_factor"] = pd.to_numeric(
                    local_events.get("adj_factor"), errors="coerce"
                )
                local_events = local_events.dropna(subset=["symbol", "date", "adj_factor"])
                dense_frames: list[pd.DataFrame] = []
                for symbol in missing:
                    symbol_dates = grid[grid["symbol"].eq(symbol)][["symbol", "date"]]
                    events = local_events[local_events["symbol"].eq(symbol)][
                        ["date", "adj_factor"]
                    ].sort_values("date")
                    if events.empty:
                        dense = symbol_dates.copy()
                        dense["adj_factor"] = 1.0
                    else:
                        dense = pd.merge_asof(
                            symbol_dates.sort_values("date"),
                            events,
                            on="date",
                            direction="backward",
                        )
                        dense["symbol"] = symbol
                        dense["adj_factor"] = dense["adj_factor"].fillna(1.0)
                    dense["source"] = "free-stockdb:cum-factor-events"
                    dense_frames.append(dense[["symbol", "date", "adj_factor", "source"]])
                if dense_frames:
                    cached = pd.concat([cached, *dense_frames], ignore_index=True)
                capability.update(
                    {
                        "source": "free-stockdb:cum-factor-events",
                        "reason": "已将 stockdb 累计复权事件展开到逐交易日研究因子",
                    }
                )
                progress(63, "读取本地 ETF 复权证据", f"{len(missing)} 只产品")
            except InterruptedError:
                raise
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
                capability.update(
                    {
                        "status": "partial" if not cached.empty else "unavailable",
                        "reason": f"本地 stockdb 复权证据不可用：{str(exc)[:180]}",
                    }
                )

        missing = missing_symbols(cached)
        if missing and get_config().data.tushare_token and start and end:
            try:
                from quantmaster.data.tushare_source import TushareSource

                source = TushareSource()
                frames: list[pd.DataFrame] = []
                for offset in range(0, len(missing), 2):
                    if cancelled():
                        raise InterruptedError("ETF 复权因子同步已取消")
                    batch = missing[offset : offset + 2]
                    result = source._call(
                        "fund_adj",
                        30,
                        ts_code=",".join(batch),
                        start_date=start.replace("-", ""),
                        end_date=end.replace("-", ""),
                        fields="ts_code,trade_date,adj_factor",
                    ).rename(columns={"ts_code": "symbol", "trade_date": "date"})
                    if {"symbol", "date", "adj_factor"}.issubset(result.columns):
                        result["source"] = "tushare:fund_adj"
                        frames.append(result[["symbol", "date", "adj_factor", "source"]])
                    progress(
                        58 + int(9 * min(offset + len(batch), len(missing)) / max(1, len(missing))),
                        "同步 ETF 复权证据",
                        f"{min(offset + len(batch), len(missing))}/{len(missing)}",
                    )
                if frames:
                    cached = pd.concat([cached, *frames], ignore_index=True)
                    capability["source"] = "free-stockdb + tushare:fund_adj"
            except InterruptedError:
                raise
            except (ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
                capability.update(
                    {
                        "status": "partial" if not cached.empty else "unavailable",
                        "reason": f"fund_adj 不可用：{str(exc)[:180]}",
                    }
                )
        elif missing and not get_config().data.tushare_token:
            capability.update(
                {
                    "status": "partial" if not cached.empty else "unavailable",
                    "reason": (
                        f"{capability.get('reason', '本地复权证据不完整')}；"
                        "当前进程未读取到 Tushare 凭据，保留明确降级"
                    ),
                }
            )

        if not cached.empty:
            cached["date"] = pd.to_datetime(cached["date"], errors="coerce")
            cached["symbol"] = cached["symbol"].astype(str).str.upper()
            cached["adj_factor"] = pd.to_numeric(cached["adj_factor"], errors="coerce")
            if "source" not in cached:
                cached["source"] = "verified:adjustment-factor"
            cached = (
                cached.dropna(subset=["date", "symbol", "adj_factor"])
                .drop_duplicates(["symbol", "date"], keep="last")
                .sort_values(["symbol", "date"])
            )
            target.parent.mkdir(parents=True, exist_ok=True)
            fd, temp_name = tempfile.mkstemp(
                prefix=".adjustment_factors.", suffix=".parquet.tmp", dir=target.parent
            )
            os.close(fd)
            temp = Path(temp_name)
            try:
                cached.to_parquet(temp, index=False)
                os.replace(temp, target)
            finally:
                temp.unlink(missing_ok=True)
        missing = missing_symbols(cached)
        covered = len(symbols) - len(missing)
        capability["covered_symbols"] = covered
        capability["coverage"] = covered / len(symbols) if symbols else 0.0
        if covered == len(symbols) and symbols:
            capability.update(
                {"status": "ready", "reason": "可核查复权因子已覆盖全部产品研究窗口"}
            )
        return cached, capability

    @staticmethod
    def _minute_metrics(frame: pd.DataFrame) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for symbol, group in frame.groupby("symbol"):
            values = group.sort_values("date").copy()
            close = pd.to_numeric(values["close"], errors="coerce")
            volume = pd.to_numeric(values["volume"], errors="coerce")
            amount = pd.to_numeric(values.get("amount"), errors="coerce")
            vwap = amount.sum() / volume.sum() if volume.sum() > 0 and amount.notna().any() else np.nan
            returns = close.pct_change().replace([np.inf, -np.inf], np.nan)
            times = pd.to_datetime(values["date"], errors="coerce")
            total_amount = amount.sum()
            first = amount[times.dt.time <= pd.Timestamp("10:30").time()].sum()
            last = amount[times.dt.time >= pd.Timestamp("14:00").time()].sum()
            result[str(symbol)] = {
                "rows": len(values),
                "complete_session": len(values) >= 240,
                "vwap_deviation": float(close.iloc[-1] / vwap - 1) if vwap and np.isfinite(vwap) else None,
                "realized_volatility": float(returns.std()) if returns.notna().any() else None,
                "intraday_drawdown": float((close / close.cummax() - 1).min())
                if close.notna().any()
                else None,
                "first_hour_amount_share": float(first / total_amount) if total_amount > 0 else None,
                "last_hour_amount_share": float(last / total_amount) if total_amount > 0 else None,
                "scoring_input": False,
            }
        return result

    def intraday(self, symbol: str, *, as_of_date: str) -> dict[str, Any]:
        """Read and cache one ETF minute series only when its trend view requests it."""

        canonical = str(symbol or "").upper()
        session = pd.Timestamp(as_of_date).date().isoformat()
        safe_symbol = re.sub(r"[^A-Z0-9._-]", "", canonical).replace(".", "_")
        target = self.store.root / "evidence" / "intraday" / f"{safe_symbol}_{session}.parquet"
        frame = pd.DataFrame()
        cache_hit = False
        if target.is_file():
            try:
                frame = pd.read_parquet(target)
                cache_hit = not frame.empty
            except (OSError, ValueError):
                frame = pd.DataFrame()
        if frame.empty:
            start = f"{session} 09:30:00"
            end = f"{session} 15:00:00"
            frame = self.source.intraday_many([canonical], start, end, "1m")
            if not frame.empty:
                target.parent.mkdir(parents=True, exist_ok=True)
                fd, temp_name = tempfile.mkstemp(
                    prefix=f".{safe_symbol}.",
                    suffix=".parquet.tmp",
                    dir=target.parent,
                )
                os.close(fd)
                temp = Path(temp_name)
                try:
                    frame.to_parquet(temp, index=False)
                    os.replace(temp, target)
                finally:
                    temp.unlink(missing_ok=True)
        if frame.empty:
            return {
                "symbol": canonical,
                "date": session,
                "status": "missing",
                "source": "free-stockdb",
                "cache_hit": cache_hit,
                "metrics": {"rows": 0, "complete_session": False, "scoring_input": False},
                "series": [],
            }
        values = frame.copy()
        if "symbol" not in values:
            values["symbol"] = canonical
        for column in ("close", "volume", "amount"):
            if column not in values:
                values[column] = np.nan
        values["symbol"] = values["symbol"].astype(str).str.upper()
        values = values[values["symbol"].eq(canonical)]
        values["date"] = pd.to_datetime(values.get("date"), errors="coerce")
        values = values.dropna(subset=["date"]).sort_values("date")
        metrics = self._minute_metrics(values).get(
            canonical,
            {"rows": len(values), "complete_session": False, "scoring_input": False},
        )
        return {
            "symbol": canonical,
            "date": session,
            "status": "ready" if metrics.get("complete_session") else "partial",
            "source": "free-stockdb",
            "cache_hit": cache_hit,
            "metrics": metrics,
            "series": [
                {
                    "time": row.date.isoformat(timespec="minutes"),
                    "close": float(row.close) if pd.notna(row.close) else None,
                    "volume": float(row.volume) if pd.notna(row.volume) else None,
                    "amount": float(row.amount) if pd.notna(row.amount) else None,
                }
                for row in values[["date", "close", "volume", "amount"]].itertuples(index=False)
            ],
        }

    def product_history(
        self,
        symbol: str,
        *,
        as_of_date: str,
        adjustment_hash: str = "",
    ) -> list[dict[str, Any]]:
        canonical = str(symbol or "").upper()
        key = (canonical, as_of_date, adjustment_hash)
        cached = self._detail_history_cache.get(key)
        if cached is not None:
            return cached
        end = pd.Timestamp(as_of_date).normalize()
        start = end - pd.DateOffset(years=3, days=20)
        daily = self.source.daily_cross_section(
            [canonical],
            start.date().isoformat(),
            end.date().isoformat(),
        )
        factor_path = self.store.root / "evidence" / "adjustment_factors.parquet"
        try:
            factors = pd.read_parquet(factor_path) if factor_path.is_file() else pd.DataFrame()
        except (OSError, ValueError):
            factors = pd.DataFrame()
        if not factors.empty and "symbol" in factors:
            factors = factors[factors["symbol"].astype(str).str.upper().eq(canonical)]
        history = adjusted_daily_metrics(daily, factors).get("history") or []
        self._detail_history_cache = {
            cache_key: value
            for cache_key, value in self._detail_history_cache.items()
            if cache_key[1] == as_of_date
        }
        self._detail_history_cache[key] = history
        return history

    def scan(
        self,
        *,
        as_of: str = "",
        progress: Progress | None = None,
        cancelled: Cancelled | None = None,
        refresh_warnings: list[str] | tuple[str, ...] = (),
    ) -> EtfResearchSnapshot:
        cfg = get_config().data
        if not cfg.free_stockdb_etf_research_enabled:
            raise RuntimeError("ETF 研究已在设置中停用")
        progress = progress or (lambda *_: None)
        cancelled = cancelled or (lambda: False)
        profiles = self.profiles()
        if not profiles:
            raise RuntimeError("证券主数据中没有沪深场内 ETF")
        end = pd.Timestamp(as_of or date.today()).normalize()
        start = end - pd.DateOffset(years=3, days=20)
        symbols = [item.symbol for item in profiles]
        master_id = "etf_master_" + content_hash([item.to_dict() for item in profiles])[:24]
        data_session = StockDBIngestService._data_session(str(end.date()))
        identity = self.source.artifact_identity(data_session=data_session)
        cache_key = content_hash(
            {
                "schema_version": STOCKDB_INGEST_SCHEMA_VERSION,
                "asset": "etf",
                "artifact": identity.artifact_id,
                "master": master_id,
                "start": str(start.date()),
                "end": str(end.date()),
                "symbols": symbols,
                "etf_research_schema": ETF_SCHEMA_VERSION,
            }
        )
        ingest = next(
            (
                item
                for item in self.ingest_store.history(100)
                if item.provenance.get("cache_key") == cache_key and "etf" in item.assets
            ),
            None,
        )
        daily = pd.DataFrame()
        if ingest is not None:
            daily = self.ingest_store.load_frame(ingest, "etf_daily")
        if daily.empty:
            frames = []
            for offset in range(0, len(symbols), 300):
                if cancelled():
                    raise InterruptedError("ETF 研究扫描已取消")
                batch = symbols[offset : offset + 300]
                frames.append(
                    self.source.daily_cross_section(
                        batch,
                        str(start.date()),
                        str(end.date()),
                    )
                )
                progress(
                    5 + int(50 * (offset + len(batch)) / len(symbols)),
                    "读取 ETF 日线",
                    f"{offset + len(batch)}/{len(symbols)}",
                )
            daily = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            if daily.empty:
                raise RuntimeError("free-stockdb 没有返回 ETF 日频截面")
            actual = pd.to_datetime(daily["date"], errors="coerce").max().date().isoformat()
            latest = daily[
                pd.to_datetime(daily["date"], errors="coerce").dt.date == date.fromisoformat(actual)
            ]
            observed = int(latest["symbol"].nunique())
            ratio = observed / len(symbols)
            required_ratio = float(
                latest[["open", "high", "low", "close", "volume"]].notna().all(axis=1).mean()
            )
            if ratio < 0.80 or required_ratio < 0.95:
                raise RuntimeError(
                    f"ETF 完整性门未通过：覆盖 {observed}/{len(symbols)}，OHLCV {required_ratio:.1%}"
                )
            coverage = {
                "status": "complete",
                "expected_symbols": len(symbols),
                "observed_symbols": observed,
                "symbol_ratio": round(ratio, 6),
                "required_ohlcv_ratio": round(required_ratio, 6),
            }
            etf_sessions = sorted(
                pd.to_datetime(
                    daily["date"],
                    errors="coerce",
                )
                .dropna()
                .dt.strftime("%Y-%m-%d")
                .unique()
                .tolist()
            )
            coverage["fields"] = StockDBIngestService.field_contracts(
                daily,
                actual,
                asset_class="etf",
                source=self.source.name,
            )
            ingest = self.ingest_store.publish_etf(
                daily=daily,
                minutes=pd.DataFrame(),
                profiles=[item.to_dict() for item in profiles],
                as_of_date=actual,
                artifact_id=identity.artifact_id,
                master_snapshot_id=master_id,
                start_date=str(start.date()),
                end_date=str(end.date()),
                coverage=coverage,
                provenance={
                    "cache_key": cache_key,
                    "upstream": "tushare",
                    "distribution": "free-stockdb",
                    "artifact": identity.to_dict(),
                    "ingest_schema_version": STOCKDB_INGEST_SCHEMA_VERSION,
                    "price_storage": "raw",
                },
                session_dates=etf_sessions,
                session_source="stockdb_broad_coverage",
            )
        actual = ingest.as_of_date
        daily["symbol"] = daily["symbol"].astype(str).str.upper()
        daily["date"] = pd.to_datetime(daily["date"], errors="coerce")
        session_dates = sorted(daily["date"].dropna().dt.date.astype(str).unique().tolist())
        factors, adjustment_capability = self._adjustment_factors(
            daily, progress=progress, cancelled=cancelled
        )
        direct = self._direct_share_observations()
        if not direct.empty:
            direct["trade_date"] = pd.to_datetime(direct["trade_date"], errors="coerce")
            direct["symbol"] = direct["symbol"].astype(str).str.upper()
            direct = direct[direct["trade_date"].dt.date <= date.fromisoformat(actual)]
        metadata_cache = self._direct_metadata()
        evidence_hashes = {
            "行情": content_hash(ingest.content_hashes),
            "份额": _frame_hash(
                direct,
                (
                    "symbol",
                    "trade_date",
                    "shares",
                    "total_size",
                    "nav",
                    "close",
                    "share_source",
                    "source",
                ),
            ),
            "复权": _frame_hash(factors, ("symbol", "date", "adj_factor", "source")),
            "元数据": (
                _frame_hash(
                    metadata_cache,
                    (
                        "symbol",
                        "name",
                        "benchmark",
                        "benchmark_code",
                        "benchmark_type",
                        "benchmark_level",
                        "index_type",
                        "index_provider",
                        "fund_type",
                        "invest_type",
                        "mgt_fee",
                        "metadata_source",
                    ),
                )
                if not metadata_cache.empty
                else content_hash([profile.to_dict() for profile in profiles])
            ),
        }
        input_hash = content_hash(
            {
                "ingest_id": ingest.ingest_id,
                "research_model_version": ETF_RESEARCH_MODEL_VERSION,
                "evidence_hashes": evidence_hashes,
            }
        )
        snapshot_id = (
            "etf_"
            + hashlib.sha256(f"{actual}:{ETF_RESEARCH_MODEL_VERSION}:{input_hash}".encode()).hexdigest()[:24]
        )
        existing = self.store.get(snapshot_id)
        if existing is not None:
            existing = self.store.publish(existing)
            self.ingest_store.pin(
                existing.ingest_id,
                "etf_research",
                existing.snapshot_id,
                {"as_of_date": existing.as_of_date},
            )
            progress(100, "复用 ETF 板块研究", existing.snapshot_id)
            return existing

        progress(70, "计算 ETF 板块证据", "趋势、位置、活跃度分别公开")
        daily_groups = {str(symbol): group for symbol, group in daily.groupby("symbol")}
        factor_groups = (
            {str(symbol): group for symbol, group in factors.groupby("symbol")} if not factors.empty else {}
        )
        rows: list[dict[str, Any]] = []
        for profile in profiles:
            metric = adjusted_daily_metrics(
                daily_groups.get(profile.symbol, pd.DataFrame()),
                factor_groups.get(profile.symbol),
            )
            observations = direct[direct["symbol"].eq(profile.symbol)] if not direct.empty else pd.DataFrame()
            funds = fund_evidence(
                observations,
                as_of_date=actual,
                session_dates=session_dates,
                fallback_price=metric.get("close"),
            )
            total_size = None
            if not observations.empty and "total_size" in observations:
                sizes = pd.to_numeric(observations["total_size"], errors="coerce").dropna()
                total_size = float(sizes.iloc[-1]) if not sizes.empty else None
            if total_size is None and funds.get("share") is not None and metric.get("close") is not None:
                total_size = float(funds["share"] * metric["close"])
            rows.append(
                {
                    "profile": profile,
                    "metrics": metric,
                    "funds": funds,
                    "total_size": total_size,
                }
            )
        sectors, representative_by_symbol, queues, candidate_queues, summaries = build_sector_research(rows)
        items: list[EtfResearchItem] = []
        for row in rows:
            profile = row["profile"]
            metrics = {key: value for key, value in row["metrics"].items() if key != "history"}
            representative_symbol = representative_by_symbol[profile.symbol]
            items.append(
                EtfResearchItem(
                    symbol=profile.symbol,
                    name=profile.name,
                    category=profile.category,
                    asset_class=profile.asset_class,
                    sector_id=profile.sector_id,
                    sector_name=profile.sector_name,
                    normalized_index=profile.normalized_index,
                    benchmark_code=profile.benchmark_code,
                    is_representative=profile.symbol == representative_symbol,
                    representative_symbol=representative_symbol,
                    metrics=metrics,
                    funds=row["funds"],
                    metadata={
                        "manager": profile.manager,
                        "custodian": profile.custodian,
                        "management_fee": profile.management_fee,
                        "total_size": row["total_size"],
                        "benchmark_type": profile.benchmark_type,
                        "benchmark_level": profile.benchmark_level,
                        "index_type": profile.index_type,
                        "index_provider": profile.index_provider,
                        "list_date": profile.list_date,
                        "classification_confidence": profile.classification_confidence,
                        "classification_evidence": profile.classification_evidence,
                    },
                    coverage={
                        "daily": profile.symbol in daily_groups,
                        "adjustment": metrics.get("adjustment_status")
                        in {"official", "verified_local"},
                        "shares": row["funds"].get("status") in {"confirmed_zero", "confirmed_change"},
                    },
                    provenance={
                        "price": "tushare:via-free-stockdb",
                        "adjustment": metrics.get("adjustment_source") or "unavailable",
                        "shares": row["funds"].get("source") or "unavailable",
                        "metadata": profile.metadata_source,
                        "classification": profile.classification_source,
                    },
                    as_of_date=actual,
                    snapshot_id=snapshot_id,
                    ingest_id=ingest.ingest_id,
                    artifact_id=ingest.artifact_id,
                )
            )
        items.sort(key=lambda item: (ETF_CATEGORIES.index(item.category), item.sector_name, item.symbol))

        share_date = (
            direct["trade_date"].max().date().isoformat()
            if not direct.empty and direct["trade_date"].notna().any()
            else ""
        )
        factor_date = (
            factors["date"].max().date().isoformat()
            if not factors.empty and factors["date"].notna().any()
            else ""
        )
        metadata_date = ""
        if not metadata_cache.empty and "updated_at" in metadata_cache:
            parsed_metadata_dates = pd.to_datetime(metadata_cache["updated_at"], errors="coerce").dropna()
            if not parsed_metadata_dates.empty:
                metadata_date = parsed_metadata_dates.max().date().isoformat()
        confirmed_shares = sum(
            item.funds.get("status") in {"confirmed_zero", "confirmed_change"} for item in items
        )
        verified_adjustments = sum(
            item.metrics.get("adjustment_status") in {"official", "verified_local"}
            for item in items
        )
        usable_metadata = sum(bool(item.name and item.sector_name) for item in items)
        official_metadata = sum(
            item.provenance.get("metadata") in {"etf_basic", "fund_basic"} for item in items
        )
        enhanced_metadata = sum(
            item.provenance.get("metadata") == "etf_basic" for item in items
        )
        freshness = {
            "research": {"date": actual, "status": "ready", "coverage": 1.0},
            "market": {
                "date": actual,
                "status": "ready",
                "coverage": float(ingest.coverage.get("symbol_ratio") or 0),
                "source": "free-stockdb",
            },
            "shares": {
                "date": share_date,
                "status": "ready" if share_date == actual else ("stale" if share_date else "missing"),
                "coverage": confirmed_shares / len(items) if items else 0.0,
                "source": "etf_share_size/fund_share",
            },
            "adjustment": {
                "date": factor_date,
                "status": adjustment_capability["status"],
                "coverage": verified_adjustments / len(items) if items else 0.0,
                "source": adjustment_capability.get("source", "adjustment-factor-cache"),
            },
            "metadata": {
                "date": metadata_date or actual,
                "status": self._profile_capabilities.get("status", "fallback"),
                "coverage": usable_metadata / len(items) if items else 0.0,
                "official_coverage": official_metadata / len(items) if items else 0.0,
                "enhanced_coverage": enhanced_metadata / len(items) if items else 0.0,
                "source": self._profile_capabilities.get("source", "security-master"),
            },
        }
        capabilities = {
            "metadata": self._profile_capabilities,
            "adjustment": {
                **adjustment_capability,
                "research_covered_symbols": verified_adjustments,
                "research_expected_symbols": len(items),
                "research_coverage": verified_adjustments / len(items) if items else 0.0,
            },
            "shares": {
                "status": freshness["shares"]["status"],
                "source": "etf_share_size 优先，fund_share 降级",
                "confirmed_symbols": confirmed_shares,
                "expected_symbols": len(items),
            },
            "intraday": {
                "status": "on_demand",
                "source": "free-stockdb",
                "scoring_input": False,
                "reason": "仅在打开单只 ETF 趋势标签时读取并缓存",
            },
            "refresh_warnings": list(refresh_warnings),
        }
        share_status_counts = {
            status: sum(item.funds.get("status") == status for item in items)
            for status in ("confirmed_change", "confirmed_zero", "stale", "missing")
        }
        snapshot = EtfResearchSnapshot(
            snapshot_id=snapshot_id,
            ingest_id=ingest.ingest_id,
            artifact_id=ingest.artifact_id,
            as_of_date=actual,
            coverage={
                **ingest.coverage,
                "product_count": len(items),
                "sector_count": len(sectors),
                "verified_adjustment_products": verified_adjustments,
                "official_metadata_products": official_metadata,
                "enhanced_metadata_products": enhanced_metadata,
                "share_status_counts": share_status_counts,
            },
            provenance={
                "upstream": "tushare",
                "distribution": "free-stockdb + Tushare evidence cache",
                "calculation": "QuantMaster ETF Sector Radar V3",
            },
            items=tuple(items),
            sectors=tuple(sectors),
            queues=queues,
            candidate_queues=candidate_queues,
            summaries=summaries,
            freshness=freshness,
            capabilities=capabilities,
            evidence_hashes=evidence_hashes,
            categories=tuple(
                category for category in ETF_CATEGORIES if any(item.category == category for item in items)
            ),
            input_hash=input_hash,
        )
        snapshot = self.store.publish(snapshot)
        self.ingest_store.pin(
            snapshot.ingest_id,
            "etf_research",
            snapshot.snapshot_id,
            {"as_of_date": snapshot.as_of_date},
        )
        progress(100, "ETF 研究完成", f"{len(sectors)} 个板块 · {len(items)} 只产品")
        return snapshot


_lock = threading.Lock()
_instance: EtfResearchService | None = None


def get_etf_research_service() -> EtfResearchService:
    global _instance
    with _lock:
        if _instance is None:
            _instance = EtfResearchService()
        return _instance


def reset_etf_research_service() -> None:
    global _instance
    with _lock:
        _instance = None
