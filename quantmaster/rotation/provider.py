"""Layered online acquisition for rotation inputs.

Automatic refresh prefers Tushare's date-partitioned research data and strict SW2021
classification.  Concepts prefer Eastmoney through AKShare and fall back to Tushare's
DC catalog when that interface is available.  A failed source never deletes the
previous catalog or ETF observations.
"""

from __future__ import annotations

import hashlib
import logging
import math
import re
import time
from collections.abc import Callable
from typing import Any

import httpx
import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.free_stockdb_source import FreeStockDBSource
from quantmaster.data.instruments import Instrument, InstrumentStore
from quantmaster.data.resilience import PROVIDER_HEALTH, akshare_call, provider_call
from quantmaster.data.tushare_source import TushareSource
from quantmaster.logging_config import redact_sensitive_text
from quantmaster.rotation.store import RotationStore
from quantmaster.rotation.taxonomy import SW2021_L1
from quantmaster.trading_sessions import daily_signal_cutoff, market_date, market_now

logger = logging.getLogger(__name__)

_BROAD_CORE_TERMS = (
    "沪深300",
    "中证A50",
    "中证A500",
    "中证1000",
    "中证2000",
    "中证500",
    "中证800",
    "中证100",
    "上证50",
    "上证180",
    "上证380",
    "上证综指",
    "上证指数",
    "深证100",
    "深证成指",
    "深证主板50",
    "创业板指",
    "创业板50",
    "科创50",
    "科创100",
    "北证50",
    "MSCI中国A",
    "富时中国A50",
    "标普中国A股",
)
_BROAD_STRATEGY_TERMS = (
    "红利",
    "股息",
    "低波",
    "自由现金流",
    "央企",
    "国企",
    "民企",
)
_SECTOR_OR_NON_CN_TERMS = (
    "医药",
    "医疗",
    "消费",
    "食品",
    "酒",
    "金融",
    "银行",
    "证券",
    "保险",
    "地产",
    "能源",
    "煤炭",
    "有色",
    "化工",
    "电力",
    "新能源",
    "光伏",
    "电池",
    "半导体",
    "芯片",
    "计算机",
    "传媒",
    "通信",
    "军工",
    "汽车",
    "农业",
    "家电",
    "机械",
    "材料",
    "资源",
    "港股",
    "恒生",
    "纳指",
    "标普500",
    "日经",
    "德国",
    "法国",
    "美国",
    "黄金",
    "白银",
    "商品",
    "债",
    "货币",
)
_EASTMONEY_THEME_SOURCE = "eastmoney-concept"
_FREE_STOCKDB_THEME_SOURCE = "free-stockdb:concept"
_TUSHARE_THEME_SOURCE = "tushare:dc-concept"
_THS_THEME_SOURCE = "ths:concept"


class RotationProviderCallError(RuntimeError):
    """An upstream provider call failed at the classified network boundary."""


class ThemeSourceUnavailable(RuntimeError):
    """A theme provider cannot currently supply a usable coherent catalog."""


def _compact_error(exc: BaseException, limit: int = 180) -> str:
    return (redact_sensitive_text(exc).replace("\n", " ").strip() or type(exc).__name__)[:limit]


def _symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.endswith((".SH", ".SZ", ".BJ")):
        return text
    digits = text.zfill(6) if text.isdigit() else text
    if len(digits) != 6 or not digits.isdigit():
        return ""
    suffix = "BJ" if digits.startswith(("4", "8", "920")) else "SH" if digits.startswith(("6", "9")) else "SZ"
    return f"{digits}.{suffix}"


def _broad_etf_category(name: Any, benchmark: Any = "") -> str:
    """Classify only auditable mainland broad-market and broad-strategy ETFs."""
    name_text = "" if name is None or pd.isna(name) else str(name)
    benchmark_text = "" if benchmark is None or pd.isna(benchmark) else str(benchmark)
    text = f"{name_text} {benchmark_text}".upper().replace(" ", "")
    if any(term.upper() in text for term in _SECTOR_OR_NON_CN_TERMS):
        return ""
    if any(term.upper() in text for term in _BROAD_CORE_TERMS):
        return "核心宽基"
    if any(term.upper() in text for term in _BROAD_STRATEGY_TERMS):
        return "策略宽基"
    return ""


class RotationProvider:
    def __init__(
        self,
        store: RotationStore,
        source: TushareSource | None = None,
        *,
        local_source: FreeStockDBSource | None = None,
        instrument_store: InstrumentStore | None = None,
    ):
        injected_source = source is not None
        self.store = store
        self.source = source or TushareSource()
        self.local_source = (
            local_source if local_source is not None else (None if injected_source else FreeStockDBSource())
        )
        self.instrument_store = (
            instrument_store
            if instrument_store is not None
            else (None if injected_source else InstrumentStore())
        )
        self._ths_last_request = 0.0

    def _tushare_call(self, endpoint: str, ttl_days: int, **params) -> pd.DataFrame:
        try:
            return self.source._call(endpoint, ttl_days, **params)
        except Exception as exc:  # Tushare SDK raises a plain Exception for permissions
            raise RotationProviderCallError(f"Tushare {endpoint} 调用失败：{_compact_error(exc)}") from exc

    def _trusted_etf_master_evidence(
        self,
        end: pd.Timestamp,
    ) -> dict[str, Any]:
        """Load a verified immutable catalog; the mutable master is never a denominator."""

        from quantmaster.data.instrument_snapshots import (
            InstrumentCatalogEvidenceError,
            load_instrument_catalog_snapshot,
        )

        target = pd.Timestamp(end).normalize()
        cutoff = pd.Timestamp(daily_signal_cutoff(target.date()))
        try:
            snapshot, expected, evidence = load_instrument_catalog_snapshot(
                as_of=target.date().isoformat(),
                market="CN",
                asset_type="etf",
            )
        except (InstrumentCatalogEvidenceError, OSError, TypeError, ValueError) as exc:
            return {
                "status": "unavailable",
                "source": "tushare:catalog",
                "acquired_at": market_now().isoformat(),
                "cutoff_at": cutoff.isoformat(),
                "freshness": "unverified",
                "master_record_count": 0,
                "master_batch_record_count": 0,
                "expected_symbols": 0,
                "observed_symbols": 0,
                "records": (),
                "catalog_evidence": {},
                "reasons": [f"不可变 Tushare 证券目录证据不可用：{_compact_error(exc)}"],
            }

        records = {
            str(row.get("symbol") or "").upper(): dict(row)
            for row in snapshot.records
            if str(row.get("symbol") or "").upper() in expected
        }
        reasons: list[str] = []
        if set(records) != expected:
            reasons.append(
                "不可变证券目录 records 与其 PIT expected 集合不一致"
            )
        for symbol, row in records.items():
            listed = pd.to_datetime(row.get("list_date"), errors="coerce")
            delisted = pd.to_datetime(row.get("delist_date"), errors="coerce")
            status = str(row.get("status") or "").strip().casefold()
            if str(row.get("exchange") or "").upper() not in {"SH", "SZ"}:
                reasons.append(f"{symbol} 不是沪深交易所 ETF")
            if str(row.get("asset_type") or "").casefold() != "etf":
                reasons.append(f"{symbol} 目录资产类型不是 ETF")
            if pd.isna(listed) or pd.Timestamp(listed).normalize() > target:
                reasons.append(f"{symbol} 缺少目标日有效 list_date")
            if not status:
                reasons.append(f"{symbol} 缺少目录生命周期状态")
            if status in {"d", "delisted", "terminated"} and pd.isna(delisted):
                reasons.append(f"{symbol} 已退市但缺少 delist_date")
        acquired = pd.Timestamp(snapshot.acquired_at)
        catalog_evidence = {
            **evidence,
            "record_count": int(snapshot.manifest["record_count"]),
        }
        return {
            "status": "verified_complete" if not reasons else "unavailable",
            "source": snapshot.source,
            "acquired_at": acquired.isoformat(),
            "cutoff_at": cutoff.isoformat(),
            "freshness": "fresh" if not reasons else "unverified",
            "master_record_count": int(snapshot.manifest["record_count"]),
            "master_batch_record_count": int(snapshot.manifest["record_count"]),
            "expected_symbols": len(expected),
            "observed_symbols": len(records),
            "records": tuple(records[symbol] for symbol in sorted(records)),
            "catalog_evidence": catalog_evidence,
            "reasons": reasons,
        }

    def _local_etf_metadata(self, previous: pd.DataFrame, end: pd.Timestamp) -> pd.DataFrame:
        """Freeze a complete ETF directory observation before remote field enhancement."""
        from quantmaster.rotation.etf_research import etf_directory_master_hash
        from quantmaster.rotation.etf_v2 import classify_etf_profile

        existing = self.store.etf_metadata_history()
        existing_rows = (
            {
                str(row.get("symbol") or "").upper(): row
                for row in existing.to_dict("records")
                if row.get("symbol")
            }
            if not existing.empty and "symbol" in existing
            else {}
        )
        share_rows: dict[str, dict[str, Any]] = {}
        if not previous.empty and {"symbol", "trade_date"}.issubset(previous.columns):
            values = previous.copy()
            values["trade_date"] = pd.to_datetime(values["trade_date"], errors="coerce")
            values["symbol"] = values["symbol"].astype(str).str.upper()
            share_rows = {
                str(row.get("symbol") or "").upper(): row
                for row in (
                    values.sort_values("trade_date")
                    .drop_duplicates("symbol", keep="last")
                    .to_dict("records")
                )
            }

        def clean(*candidates: Any) -> str:
            for candidate in candidates:
                if candidate is None or pd.isna(candidate):
                    continue
                value = str(candidate).strip()
                if value and value.casefold() != "nan":
                    return value
            return ""

        target = pd.Timestamp(end).normalize()
        selected_instruments: list[Any] = []
        master_evidence = self._trusted_etf_master_evidence(target)
        if master_evidence["status"] == "verified_complete":
            acquired_epoch = pd.Timestamp(master_evidence["acquired_at"]).timestamp()
            selected_instruments = [
                Instrument(
                    symbol=str(row.get("symbol") or "").upper(),
                    code=str(row.get("symbol") or "").split(".", 1)[0],
                    name=str(row.get("name") or ""),
                    market="CN",
                    exchange=str(row.get("exchange") or "").upper(),
                    asset_type="etf",
                    provider_symbol=str(row.get("provider_symbol") or row.get("symbol") or ""),
                    status=str(row.get("status") or ""),
                    source=master_evidence["source"],
                    list_date=str(row.get("list_date") or ""),
                    delist_date=str(row.get("delist_date") or ""),
                    observed_at=acquired_epoch,
                )
                for row in master_evidence["records"]
            ]
        elif self.instrument_store is not None:
            for instrument in self.instrument_store.list(market="CN"):
                name = clean(instrument.name)
                if instrument.exchange not in {"SH", "SZ"}:
                    continue
                if "LOF" in name.upper() or "联接" in name:
                    continue
                if instrument.asset_type != "etf" and not (
                    instrument.asset_type == "fund"
                    and ("ETF" in name.upper() or "交易型" in name)
                ):
                    continue
                listed = pd.to_datetime(instrument.list_date, errors="coerce")
                delisted = pd.to_datetime(instrument.delist_date, errors="coerce")
                if pd.notna(listed) and pd.Timestamp(listed).normalize() > target:
                    continue
                if pd.notna(delisted) and pd.Timestamp(delisted).normalize() < target:
                    continue
                selected_instruments.append(instrument)
        rows: list[dict[str, Any]] = []
        for instrument in selected_instruments:
            name = clean(instrument.name)
            rich = existing_rows.get(instrument.symbol, {})
            share = share_rows.get(instrument.symbol, {})
            metadata_source = clean(rich.get("metadata_source"), "free-stockdb:security-master")
            benchmark = clean(rich.get("benchmark"), share.get("benchmark"))
            index_name = clean(rich.get("index_name"), benchmark)
            fund_type = clean(rich.get("fund_type"), share.get("fund_type"), "ETF")
            invest_type = clean(rich.get("invest_type"), share.get("invest_type"))
            taxonomy_source = (
                "etf_basic"
                if "etf_basic" in metadata_source
                else "fund_basic"
                if "fund_basic" in metadata_source
                else "local_stockdb"
            )
            taxonomy = classify_etf_profile(
                clean(rich.get("name"), name),
                benchmark=benchmark,
                benchmark_code=clean(rich.get("benchmark_code")),
                index_name=index_name,
                fund_type=fund_type,
                invest_type=invest_type,
                etf_type=clean(rich.get("etf_type")),
                benchmark_type=clean(rich.get("benchmark_type")),
                index_type=clean(rich.get("index_type")),
                metadata_source=taxonomy_source,
            )
            rows.append(
                {
                    "symbol": instrument.symbol,
                    "name": clean(rich.get("name"), name),
                    "benchmark": benchmark,
                    "benchmark_code": clean(rich.get("benchmark_code")),
                    "index_name": index_name,
                    "benchmark_level": clean(rich.get("benchmark_level")),
                    "benchmark_type": clean(rich.get("benchmark_type")),
                    "index_provider": clean(rich.get("index_provider")),
                    "index_type": clean(rich.get("index_type")),
                    "fund_type": fund_type,
                    "invest_type": invest_type,
                    "mgr_name": clean(rich.get("mgr_name"), rich.get("manager")),
                    "custod_name": clean(rich.get("custod_name"), rich.get("custodian")),
                    "mgt_fee": rich.get("mgt_fee", rich.get("management_fee")),
                    "etf_type": clean(rich.get("etf_type")),
                    "exchange": instrument.exchange,
                    "asset_type": instrument.asset_type,
                    "status": instrument.status,
                    "list_date": clean(instrument.list_date, rich.get("list_date")),
                    "delist_date": clean(instrument.delist_date),
                    "category": taxonomy["category"],
                    "asset_class": taxonomy["asset_class"],
                    "sector_id": taxonomy["sector_id"],
                    "sector_name": taxonomy["sector_name"],
                    "normalized_index": taxonomy["normalized_index"],
                    "classification_source": taxonomy["classification_source"],
                    "classification_confidence": taxonomy["classification_confidence"],
                    "metadata_source": metadata_source,
                    "updated_at": clean(rich.get("updated_at"), end.date().isoformat()),
                    "directory_member_source": str(instrument.source),
                    "directory_member_observed_at": pd.Timestamp(
                        float(instrument.observed_at or 0), unit="s", tz="UTC"
                    ).isoformat()
                    if float(instrument.observed_at or 0) > 0
                    else master_evidence["acquired_at"],
                }
            )
        frame = pd.DataFrame(rows)
        if not frame.empty:
            frame = frame.sort_values("symbol").reset_index(drop=True)
            effective_date = end.date().isoformat()
            observed = pd.Timestamp(market_now())
            acquired = pd.Timestamp(master_evidence["acquired_at"])
            if acquired.tzinfo is None:
                acquired = acquired.tz_localize("Asia/Shanghai")
            if observed.tzinfo is None:
                observed = observed.tz_localize("Asia/Shanghai")
            if observed.tz_convert("UTC") < acquired.tz_convert("UTC"):
                observed = acquired.tz_convert("Asia/Shanghai")
            observed_at = observed.isoformat()
            frame["effective_date"] = effective_date
            frame["observed_at"] = observed_at
            frame["directory_source"] = master_evidence["source"]
            frame["directory_acquired_at"] = master_evidence["acquired_at"]
            frame["directory_cutoff_at"] = master_evidence["cutoff_at"]
            frame["directory_freshness"] = master_evidence["freshness"]
            frame["directory_master_record_count"] = master_evidence[
                "master_record_count"
            ]
            frame["directory_master_batch_record_count"] = master_evidence[
                "master_batch_record_count"
            ]
            frame["directory_expected_symbols"] = master_evidence["expected_symbols"]
            frame["directory_observed_symbols"] = master_evidence["observed_symbols"]
            frame["directory_quality_reason"] = "；".join(master_evidence["reasons"])
            catalog = master_evidence["catalog_evidence"]
            frame["directory_catalog_snapshot_id"] = catalog.get("snapshot_id", "")
            frame["directory_catalog_records_sha256"] = catalog.get("records_sha256", "")
            frame["directory_catalog_file_sha256"] = catalog.get("file_sha256", "")
            frame["directory_catalog_file_size"] = str(catalog.get("file_size", 0))
            frame["directory_catalog_file_mtime_ns"] = str(
                catalog.get("file_mtime_ns", 0)
            )
            frame["directory_catalog_relative_path"] = catalog.get("relative_path", "")
            frame["directory_catalog_as_of"] = catalog.get("as_of", effective_date)
            frame["directory_catalog_expected_count"] = catalog.get("expected_count", 0)
            frame["directory_master_snapshot_sha256"] = catalog.get("snapshot_id", "")
            complete = master_evidence["status"] == "verified_complete"
            if complete:
                attestation_hash = etf_directory_master_hash(frame)
                frame["directory_snapshot_id"] = "etf_directory_" + attestation_hash[:24]
            else:
                attestation_hash = hashlib.sha256(
                    repr(
                        (
                            effective_date,
                            tuple(frame["symbol"].astype(str)),
                            tuple(master_evidence["reasons"]),
                        )
                    ).encode("utf-8")
                ).hexdigest()
                frame["directory_snapshot_id"] = (
                    "etf_directory_unverified_" + attestation_hash[:24]
                )
            frame["directory_attestation_sha256"] = attestation_hash
            frame["directory_complete"] = complete
            self.store.save_etf_metadata(frame)
        return frame

    @staticmethod
    def _eastmoney_theme_call(
        label: str,
        function: Callable[..., pd.DataFrame],
        *args,
        **params,
    ) -> pd.DataFrame:
        try:
            return akshare_call(
                label,
                function,
                *args,
                lane="akshare:eastmoney-concept",
                **params,
            )
        except Exception as exc:  # AKShare/requests provider boundary
            raise ThemeSourceUnavailable(f"东方财富 {label} 调用失败：{_compact_error(exc)}") from exc

    def sync_market_history(
        self,
        progress,
        cancelled,
        *,
        rebuild: bool = False,
        as_of: str = "",
    ) -> dict[str, Any]:
        """Materialize three years of date-partitioned full-market stock bars."""
        from quantmaster.data.instrument_snapshots import (
            InstrumentCatalogEvidenceError,
            load_instrument_catalog_snapshot,
        )
        from quantmaster.data.instruments import refresh_authoritative_instrument_catalog
        from quantmaster.research.contracts import ArtifactKind, AssetClass, Frequency
        from quantmaster.research.engine import ResearchEngine
        from quantmaster.research.lake import ResearchLake
        from quantmaster.rotation.service import _expected_market_session

        # Before the close, today's official calendar entry is already open but
        # the daily endpoint legitimately has no completed bar yet.  Planning to
        # the same completed-session boundary used by snapshot freshness avoids
        # turning that expected empty response into a provider circuit failure.
        end = (
            _expected_market_session(as_of=as_of)
            if as_of
            else _expected_market_session()
        )
        if not end:
            raise RuntimeError("无法确认最近完成交易日；请配置 Tushare 日历或先同步全市场日线")
        catalog_snapshot_id = ""
        try:
            catalog, _symbols, _evidence = load_instrument_catalog_snapshot(
                as_of=end, market="CN", asset_type="stock",
            )
            catalog_snapshot_id = catalog.snapshot_id
        except (InstrumentCatalogEvidenceError, OSError, TypeError, ValueError):
            target = pd.Timestamp(end).date()
            current = market_now()
            if current >= daily_signal_cutoff(target):
                progress(1, "冻结证券目录", f"补齐 {end} 全市场横截面分母")
                refresh_authoritative_instrument_catalog(
                    source=self.source,
                    store=self.instrument_store,
                )
                catalog, _symbols, _evidence = load_instrument_catalog_snapshot(
                    as_of=end, market="CN", asset_type="stock",
                )
                catalog_snapshot_id = catalog.snapshot_id
        start = (pd.Timestamp(end) - pd.DateOffset(years=3, days=20)).date().isoformat()
        lake = ResearchLake()
        existing = lake.catalog.partitions(
            kind=ArtifactKind.RAW,
            asset_class=AssetClass.STOCK,
            frequency=Frequency.DAILY,
            dataset_id="stock_bars",
            start=start,
            end=end,
        )
        engine = ResearchEngine(lake=lake)
        plan = engine.plan(
            start,
            end,
            asset_classes=(AssetClass.STOCK,),
            datasets=("stock_bars",),
            mode="historical" if rebuild or not existing else "incremental",
        )
        if not plan.tasks:
            progress(28, "全市场日线已就绪", f"{len(existing)} 个交易日分区")
            return {
                "partitions": len(existing),
                "tasks": 0,
                "expected_as_of": max(plan.target_dates, default=""),
                "catalog_snapshot_id": catalog_snapshot_id,
                "failed_tasks": [],
                "issues": [],
            }

        def on_progress(index, total, task):
            if cancelled():
                raise InterruptedError("板块联动数据同步已取消")
            value = 3 + round(25 * index / max(1, total))
            progress(value, "同步全市场日线", f"{index}/{total} · {task.trade_date}")

        result = engine.execute(
            plan,
            cancelled=cancelled,
            progress=on_progress,
            continue_on_sync_error=True,
        )
        return {
            "partitions": len(existing) + len(plan.tasks),
            "tasks": len(plan.tasks),
            "run_id": result["run_id"],
            "expected_as_of": max(plan.target_dates, default=""),
            "catalog_snapshot_id": catalog_snapshot_id,
            "failed_tasks": result.get("failed_tasks", []),
            "issues": [
                f"{item['trade_date']} 行情同步失败：{item['error']}"
                for item in result.get("failed_tasks", [])
            ],
        }

    def sync_industry_taxonomy(self, progress, cancelled) -> dict[str, Any]:
        """Fetch L1/L2 memberships per L1 so provider row limits cannot truncate the market."""
        classes = self._tushare_call(
            "index_classify",
            30,
            level="L1",
            src="SW2021",
            fields="index_code,industry_name,level",
        )
        previous = {str(item.get("code")): item for item in self.store.taxonomy_nodes()}
        strict_l1 = dict(SW2021_L1)
        nodes: dict[str, dict[str, Any]] = {}
        rows = [
            row
            for _, row in classes.iterrows()
            if strict_l1.get(str(row.get("index_code") or "").strip().upper())
            == str(row.get("industry_name") or "").strip()
        ]
        for index, row in enumerate(rows, start=1):
            if cancelled():
                raise InterruptedError("申万行业同步已取消")
            l1_code = str(row.get("index_code") or "").strip().upper()
            l1_name = str(row.get("industry_name") or "").strip()
            if strict_l1.get(l1_code) != l1_name:
                continue
            try:
                members = self._tushare_call(
                    "index_member_all",
                    30,
                    l1_code=l1_code,
                    is_new="Y",
                    fields="l1_code,l1_name,l2_code,l2_name,ts_code,is_new",
                )
            except RotationProviderCallError as exc:
                logger.warning(
                    "申万行业 %s 成分同步失败，保留旧目录：%s",
                    l1_name,
                    _compact_error(exc),
                )
                for code, item in previous.items():
                    if code == l1_code or str(item.get("parent_code") or "") == l1_code:
                        nodes[code] = item
                continue
            l1_members: list[str] = []
            l2: dict[str, dict[str, Any]] = {}
            for _, member in members.iterrows():
                symbol = _symbol(member.get("ts_code"))
                if not symbol:
                    continue
                l1_members.append(symbol)
                l2_code = str(member.get("l2_code") or "").strip().upper()
                l2_name = str(member.get("l2_name") or "").strip()
                if l2_code and l2_name:
                    node = l2.setdefault(
                        l2_code,
                        {
                            "code": l2_code,
                            "name": l2_name,
                            "level": "L2",
                            "parent_code": l1_code,
                            "members": [],
                            "source": "SW2021",
                        },
                    )
                    node["members"].append(symbol)
            nodes[l1_code] = {
                "code": l1_code,
                "name": l1_name,
                "level": "L1",
                "parent_code": "",
                "members": sorted(set(l1_members)),
                "source": "SW2021",
            }
            for code, node in l2.items():
                node["members"] = sorted(set(node["members"]))
                nodes[code] = node
            progress(
                29 + round(10 * index / max(1, len(rows))),
                "同步申万层级",
                f"{index}/{len(rows)} · {l1_name}",
            )
        if nodes:
            self.store.replace_taxonomy_nodes(list(nodes.values()))
        return {
            "l1": sum(item.get("level") == "L1" for item in nodes.values()),
            "l2": sum(item.get("level") == "L2" for item in nodes.values()),
        }

    @staticmethod
    def _themes_from_source(
        previous_items: list[dict[str, Any]],
        source: str,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        matching = [item for item in previous_items if str(item.get("source") or "") == source]
        return (
            {str(item.get("code") or ""): item for item in matching},
            {str(item.get("name") or ""): item for item in matching},
        )

    def _sync_free_stockdb_themes(
        self,
        progress,
        cancelled,
    ) -> dict[str, Any]:  # pragma: no cover - 本地外部数据服务
        """Publish the coherent concept catalog maintained by free-stockdb."""
        if cancelled():
            raise InterruptedError("free-stockdb 题材同步已取消")
        progress(39, "扫描细分题材", "读取 free-stockdb 本地板块目录")
        try:
            themes = FreeStockDBSource().themes()
        except InterruptedError:
            raise
        except (ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
            raise ThemeSourceUnavailable(f"free-stockdb 概念目录不可用：{_compact_error(exc)}") from exc
        if not themes:
            raise ThemeSourceUnavailable("free-stockdb 概念目录为空")
        if cancelled():
            raise InterruptedError("free-stockdb 题材同步已取消")
        self.store.replace_themes(themes)
        progress(55, "扫描细分题材", f"free-stockdb 可用 {len(themes)} 个概念板块")
        return {
            "catalog": len(themes),
            "available": len(themes),
            "fresh": len(themes),
            "source": _FREE_STOCKDB_THEME_SOURCE,
            "coverage": 1.0,
            "quality_status": "complete",
            "issues": [],
        }

    def _sync_eastmoney_themes(
        self,
        progress,
        cancelled,
        previous_items: list[dict[str, Any]],
    ) -> dict[str, Any]:  # pragma: no cover - 网络
        """Scan Eastmoney concepts without mixing a prior provider's taxonomy."""
        try:
            import akshare as ak
        except ModuleNotFoundError as exc:
            raise ThemeSourceUnavailable("AKShare 可选数据扩展未安装") from exc

        boards = self._eastmoney_theme_call(
            "stock_board_concept_name_em",
            ak.stock_board_concept_name_em,
        )
        if boards is None or boards.empty:
            raise ThemeSourceUnavailable("东方财富概念目录为空")
        previous_code, previous_name = self._themes_from_source(
            previous_items,
            _EASTMONEY_THEME_SOURCE,
        )
        previous_matching = list(previous_code.values())
        themes: dict[str, dict[str, Any]] = {}
        rows = [row for _, row in boards.iterrows()]
        fresh_count = 0
        member_failures = 0
        for index, row in enumerate(rows, start=1):
            if cancelled():
                raise InterruptedError("东方财富概念扫描已取消")
            name = str(row.get("板块名称") or row.get("名称") or "").strip()
            raw_code = str(row.get("板块代码") or row.get("代码") or "").strip().upper()
            code = raw_code or f"EMC_{hashlib.sha1(name.encode('utf-8')).hexdigest()[:10].upper()}"
            if not name:
                continue
            member_symbol = raw_code if raw_code.startswith("BK") and raw_code[2:].isdigit() else name
            try:
                members = self._eastmoney_theme_call(
                    f"stock_board_concept_cons_em({member_symbol})",
                    ak.stock_board_concept_cons_em,
                    symbol=member_symbol,
                )
                values = [
                    symbol for raw in members.get("代码", pd.Series(dtype=str)) if (symbol := _symbol(raw))
                ]
                if not values:
                    raise ThemeSourceUnavailable("概念成分为空")
                themes[code] = {
                    "code": code,
                    "name": name,
                    "members": sorted(set(values)),
                    "aliases": [],
                    "source": _EASTMONEY_THEME_SOURCE,
                }
                fresh_count += 1
                member_failures = 0
            except ThemeSourceUnavailable as exc:
                member_failures += 1
                old = previous_code.get(code) or previous_name.get(name)
                if old:
                    themes[code] = old
                logger.warning(
                    "概念 %s 成分同步失败，保留旧快照：%s",
                    name,
                    _compact_error(exc),
                )
                if fresh_count == 0 and member_failures >= 3:
                    break
            if index % 20 == 0 or index == len(rows):
                progress(
                    39 + round(16 * index / max(1, len(rows))),
                    "扫描细分题材",
                    f"东方财富 {index}/{len(rows)} · 成功 {fresh_count}",
                )
        if not fresh_count:
            raise ThemeSourceUnavailable("东方财富概念成分连续不可用")
        unprocessed = {
            str(item.get("code") or ""): item
            for item in previous_matching
            if str(item.get("code") or "") not in themes
        }
        self.store.replace_themes([*themes.values(), *unprocessed.values()])
        return {
            "catalog": len(rows),
            "available": len(themes),
            "fresh": fresh_count,
            "source": _EASTMONEY_THEME_SOURCE,
            "issues": [],
        }

    def _sync_tushare_themes(
        self,
        progress,
        cancelled,
        previous_items: list[dict[str, Any]],
        *,
        as_of: str = "",
    ) -> dict[str, Any]:  # pragma: no cover - 网络
        """Use the permission-gated Tushare DC catalog as the second provider."""
        end = pd.Timestamp(as_of) if as_of else pd.Timestamp(market_date())
        start = end - pd.Timedelta(days=14)
        sessions = list(self.source.trade_calendar(str(start.date()), str(end.date())))
        if not sessions:
            raise ThemeSourceUnavailable("官方交易日历没有可查询的题材日期")
        boards = pd.DataFrame()
        trade_date = ""
        for session in reversed(sessions[-7:]):
            if cancelled():
                raise InterruptedError("Tushare DC 概念扫描已取消")
            candidate = pd.Timestamp(session).strftime("%Y%m%d")
            boards = self._tushare_call(
                "dc_index",
                1,
                provider_lane="tushare:dc-concept",
                trade_date=candidate,
                idx_type="概念板块",
                fields="ts_code,trade_date,name,idx_type",
            )
            if boards is not None and not boards.empty:
                trade_date = candidate
                break
        if boards is None or boards.empty or not trade_date:
            raise ThemeSourceUnavailable("Tushare DC 概念目录为空")
        if not {"ts_code", "name"}.issubset(boards.columns):
            raise ThemeSourceUnavailable("Tushare DC 概念目录缺少代码或名称列")

        previous_code, previous_name = self._themes_from_source(
            previous_items,
            _TUSHARE_THEME_SOURCE,
        )
        previous_matching = list(previous_code.values())
        themes: dict[str, dict[str, Any]] = {}
        rows = [row for _, row in boards.drop_duplicates("ts_code").iterrows()]
        fresh_count = 0
        member_failures = 0
        for index, row in enumerate(rows, start=1):
            if cancelled():
                raise InterruptedError("Tushare DC 概念扫描已取消")
            code = str(row.get("ts_code") or "").strip().upper()
            name = str(row.get("name") or "").strip()
            if not code or not name:
                continue
            try:
                members = self._tushare_call(
                    "dc_member",
                    7,
                    provider_lane="tushare:dc-concept",
                    trade_date=trade_date,
                    ts_code=code,
                    fields="trade_date,ts_code,con_code,name",
                )
                values = [
                    symbol
                    for raw in members.get("con_code", pd.Series(dtype=str))
                    if (symbol := _symbol(raw))
                ]
                if not values:
                    raise ThemeSourceUnavailable("概念成分为空")
                themes[code] = {
                    "code": code,
                    "name": name,
                    "members": sorted(set(values)),
                    "aliases": [],
                    "source": _TUSHARE_THEME_SOURCE,
                }
                fresh_count += 1
                member_failures = 0
            except (RotationProviderCallError, ThemeSourceUnavailable) as exc:
                member_failures += 1
                old = previous_code.get(code) or previous_name.get(name)
                if old:
                    themes[code] = old
                logger.warning(
                    "Tushare DC 概念 %s 成分同步失败，保留同源旧快照：%s",
                    name,
                    _compact_error(exc),
                )
                if fresh_count == 0 and member_failures >= 3:
                    break
            if index % 20 == 0 or index == len(rows):
                progress(
                    39 + round(16 * index / max(1, len(rows))),
                    "扫描细分题材",
                    f"Tushare DC {index}/{len(rows)} · 成功 {fresh_count}",
                )
        if not fresh_count:
            raise ThemeSourceUnavailable("Tushare DC 概念成分连续不可用")
        unprocessed = {
            str(item.get("code") or ""): item
            for item in previous_matching
            if str(item.get("code") or "") not in themes
        }
        self.store.replace_themes([*themes.values(), *unprocessed.values()])
        return {
            "catalog": len(rows),
            "available": len(themes),
            "fresh": fresh_count,
            "source": _TUSHARE_THEME_SOURCE,
            "trade_date": trade_date,
            "issues": ["东方财富概念接口不可用，已自动切换为 Tushare DC 概念目录。"],
        }

    def _ths_page(self, client, code: str, page: int) -> str:
        """Fetch one public THS concept page with a bounded provider-side retry."""
        url = f"http://q.10jqka.com.cn/gn/detail/code/{code}/page/{page}/"

        def fetch() -> str:
            last_error: Exception | None = None
            for attempt in range(3):
                delay = 0.5 - (time.monotonic() - self._ths_last_request)
                if delay > 0:
                    time.sleep(delay)
                self._ths_last_request = time.monotonic()
                try:
                    response = client.get(url)
                    response.raise_for_status()
                    if not response.text.strip():
                        raise RuntimeError("同花顺页面为空")
                    return response.text
                except (httpx.HTTPError, OSError, RuntimeError, ValueError) as exc:
                    last_error = exc
                    if attempt < 2:
                        time.sleep(2**attempt)
            raise RuntimeError(str(last_error or "同花顺页面请求失败"))

        return provider_call(
            "ths:concept",
            f"concept:{code}:page:{page}",
            fetch,
            empty_opens=True,
        )

    @staticmethod
    def _parse_ths_page(html: str) -> tuple[list[str], int]:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        table = soup.select_one("table.m-table")
        if table is None:
            raise ThemeSourceUnavailable("同花顺题材页面缺少成分表")
        headers = [cell.get_text(" ", strip=True) for cell in table.select("thead th")]
        try:
            code_index = headers.index("代码")
        except ValueError:
            code_index = 1
        members: list[str] = []
        for row in table.select("tbody tr"):
            cells = row.find_all("td")
            if len(cells) <= code_index:
                continue
            value = _symbol(cells[code_index].get_text(" ", strip=True))
            if value:
                members.append(value)
        info = soup.select_one(".page_info")
        match = re.search(r"\d+\s*/\s*(\d+)", info.get_text(" ", strip=True) if info else "")
        pages = max(1, int(match.group(1))) if match else 1
        return members, pages

    def _sync_ths_themes(
        self,
        progress,
        cancelled,
        previous_items: list[dict[str, Any]],
    ) -> dict[str, Any]:  # pragma: no cover - network
        """Build a resumable, all-THS catalog and publish it only after quality gates."""
        try:
            import akshare as ak
        except ModuleNotFoundError as exc:
            raise ThemeSourceUnavailable("同花顺后备源所需数据扩展未安装") from exc
        boards = akshare_call(
            "stock_board_concept_name_ths",
            ak.stock_board_concept_name_ths,
            lane="ths:concept",
        )
        if boards is None or boards.empty or not {"name", "code"}.issubset(boards.columns):
            raise ThemeSourceUnavailable("同花顺题材目录为空或缺少代码列")
        rows = [
            (str(row.get("code") or "").strip(), str(row.get("name") or "").strip())
            for _, row in boards.drop_duplicates("code").iterrows()
            if str(row.get("code") or "").strip() and str(row.get("name") or "").strip()
        ]
        directory_hash = hashlib.sha256(
            "|".join(f"{code}:{name}" for code, name in sorted(rows)).encode("utf-8")
        ).hexdigest()
        staging = self.store.begin_theme_sync(_THS_THEME_SOURCE, directory_hash, len(rows))
        run_id = str(staging["run_id"])
        themes: dict[str, dict[str, Any]] = dict(staging["items"])
        issues: list[str] = []
        required = math.ceil(len(rows) * 0.90)
        partial_required = min(required, max(75, math.ceil(len(rows) * 0.20)))

        def publish_partial(extra_issues: list[str]) -> dict[str, Any]:
            coverage = round(len(themes) / max(1, len(rows)), 4)
            audit_issues = [
                (
                    f"同花顺完整题材仅覆盖 {len(themes)}/{len(rows)}；未达到完整目录门槛 "
                    f"{required}，已按受限目录发布。"
                ),
                "东方财富与 Tushare 目录不可用；失败或分页不完整的题材未写入目录。",
                *extra_issues[:30],
            ]
            self.store.commit_theme_sync(run_id, list(themes.values()), audit_issues)
            return {
                "catalog": len(rows),
                "available": len(themes),
                "fresh": 0,
                "source": _THS_THEME_SOURCE,
                "coverage": coverage,
                "quality_status": "partial",
                "issues": audit_issues,
            }

        previous_ths = {
            str(item.get("code") or ""): item
            for item in previous_items
            if str(item.get("source") or "") == _THS_THEME_SOURCE and str(item.get("code") or "")
        }
        can_reuse_published_partial = (
            int(staging.get("attempted_count") or 0) >= len(rows)
            and partial_required <= len(previous_ths) < required
            and set(previous_ths) == set(themes)
        )
        if can_reuse_published_partial:
            audit_issues = [
                (
                    f"同花顺完整题材仅覆盖 {len(previous_ths)}/{len(rows)}；未达到完整"
                    f"目录门槛 {required}，继续使用已验证的受限目录。"
                ),
                "东方财富与 Tushare 目录不可用；失败或分页不完整的题材未写入目录。",
            ]
            self.store.reuse_published_theme_sync(run_id, audit_issues)
            return {
                "catalog": len(rows),
                "available": len(previous_ths),
                "fresh": 0,
                "source": _THS_THEME_SOURCE,
                "coverage": round(len(previous_ths) / max(1, len(rows)), 4),
                "quality_status": "partial",
                "issues": audit_issues,
            }

        # A previous complete traversal may already contain enough individually
        # verified themes for a useful cold-start snapshot.  Reuse it instead of
        # repeating thousands of pages known to require an authenticated THS
        # session.  Never replace an existing published catalog with this subset.
        if (
            not previous_items
            and int(staging.get("attempted_count") or 0) >= len(rows)
            and partial_required <= len(themes) < required
        ):
            return publish_partial([])
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36"
            ),
            "Referer": "http://q.10jqka.com.cn/gn/",
        }
        # THS is the final public fallback when the configured providers are
        # unavailable.  Do not inherit a stale machine-wide proxy here: such
        # proxies can return a 200 HTML interstitial that looks like a valid
        # response but contains no constituent table.
        with httpx.Client(
            timeout=12,
            follow_redirects=True,
            headers=headers,
            trust_env=False,
        ) as client:
            for index, (code, name) in enumerate(rows, start=1):
                if cancelled():
                    raise InterruptedError("同花顺题材扫描已取消")
                if code in themes:
                    progress(
                        39 + round(16 * index / max(1, len(rows))),
                        "恢复细分题材扫描",
                        f"同花顺 {index}/{len(rows)} · 已缓存 {len(themes)}",
                    )
                    continue
                page_count = 0
                try:
                    first_html = self._ths_page(client, code, 1)
                    first_members, page_count = self._parse_ths_page(first_html)
                    members = list(first_members)
                    progress(
                        39 + round(16 * (index - 1) / max(1, len(rows))),
                        "扫描细分题材",
                        f"同花顺 {index}/{len(rows)} · {name} · 1/{page_count} 页",
                    )
                    for page in range(2, page_count + 1):
                        if cancelled():
                            raise InterruptedError("同花顺题材扫描已取消")
                        page_members, _ = self._parse_ths_page(self._ths_page(client, code, page))
                        members.extend(page_members)
                        progress(
                            39 + round(16 * (index - 1) / max(1, len(rows))),
                            "扫描细分题材",
                            f"同花顺 {index}/{len(rows)} · {name} · {page}/{page_count} 页",
                        )
                    members = sorted(set(members))
                    if not members:
                        raise ThemeSourceUnavailable("同花顺题材成分为空")
                    payload = {
                        "code": code,
                        "name": name,
                        "members": members,
                        "aliases": [],
                        "source": _THS_THEME_SOURCE,
                    }
                    themes[code] = payload
                    self.store.save_theme_sync_item(
                        run_id,
                        code,
                        name,
                        payload=payload,
                        pages=page_count,
                    )
                except InterruptedError:
                    raise
                except (OSError, RuntimeError, TypeError, ValueError) as exc:
                    detail = (str(exc).strip() or "题材成分同步失败")[:300]
                    self.store.save_theme_sync_item(
                        run_id,
                        code,
                        name,
                        error=detail,
                        pages=page_count,
                    )
                    issues.append(f"{name}：{detail}")
                progress(
                    39 + round(16 * index / max(1, len(rows))),
                    "扫描细分题材",
                    f"同花顺 {index}/{len(rows)} · 可用 {len(themes)} · 页 {page_count}",
                )
        if len(themes) < required:
            message = f"同花顺题材覆盖不足：{len(themes)}/{len(rows)}，最低要求 {required}"
            if not previous_items and len(themes) >= partial_required:
                return publish_partial([message, *issues])
            self.store.fail_theme_sync(run_id, [message, *issues[:30]])
            raise ThemeSourceUnavailable(message)
        audit_issues = [
            "东方财富与 Tushare DC 不可用，已整套切换为同花顺题材目录。",
            *(issues[:30]),
        ]
        self.store.commit_theme_sync(run_id, list(themes.values()), audit_issues)
        return {
            "catalog": len(rows),
            "available": len(themes),
            "fresh": len(themes) - len(staging["items"]),
            "source": _THS_THEME_SOURCE,
            "coverage": round(len(themes) / max(1, len(rows)), 4),
            "quality_status": "complete",
            "issues": audit_issues,
        }

    def sync_themes(
        self, progress, cancelled, *, as_of: str = "",
    ) -> dict[str, Any]:  # pragma: no cover - 网络
        """Refresh one coherent concept taxonomy through configured fallbacks."""
        previous_items = self.store.themes()
        free_stockdb_error: ThemeSourceUnavailable | None = None
        if get_config().data.primary_provider == "free-stockdb":
            try:
                return self._sync_free_stockdb_themes(progress, cancelled)
            except InterruptedError:
                raise
            except ThemeSourceUnavailable as exc:
                free_stockdb_error = exc
                logger.warning(
                    "free-stockdb 概念目录不可用，尝试东方财富后备源：%s",
                    _compact_error(exc),
                )
        try:
            return self._sync_eastmoney_themes(
                progress,
                cancelled,
                previous_items,
            )
        except InterruptedError:
            raise
        except ThemeSourceUnavailable as eastmoney_error:
            logger.warning(
                "东方财富概念目录不可用，尝试 Tushare DC 后备源：%s",
                _compact_error(eastmoney_error),
            )
            disabled_dc = PROVIDER_HEALTH.disabled_status("tushare:dc-concept")
            if disabled_dc:
                tushare_error: BaseException = ThemeSourceUnavailable(
                    "当前 Tushare 凭据已确认无 DC 概念权限"
                )
                logger.info("Tushare DC 已按当前凭据跳过，直接尝试同花顺题材目录")
            else:
                try:
                    return self._sync_tushare_themes(
                        progress,
                        cancelled,
                        previous_items,
                        as_of=as_of,
                    )
                except InterruptedError:
                    raise
                except (RotationProviderCallError, ThemeSourceUnavailable) as exc:
                    tushare_error = exc
                    logger.warning(
                        "Tushare DC 题材目录不可用，尝试同花顺后备源：%s",
                        _compact_error(tushare_error),
                    )
            try:
                return self._sync_ths_themes(progress, cancelled, previous_items)
            except InterruptedError:
                raise
            except ThemeSourceUnavailable as ths_error:
                prefix = f"free-stockdb {str(free_stockdb_error)[:90]}；" if free_stockdb_error else ""
                raise RuntimeError(
                    "题材目录全部不可用："
                    f"{prefix}"
                    f"东方财富 {str(eastmoney_error)[:90]}；"
                    f"Tushare {str(tushare_error)[:90]}；"
                    f"同花顺 {str(ths_error)[:90]}"
                ) from ths_error

    def sync_etf_observations(
        self, progress, cancelled, *, as_of: str = "",
    ) -> dict[str, Any]:
        """Keep legacy broad history and maintain a 25-session all-market share panel."""
        from quantmaster.rotation.etf_v2 import classify_etf_profile
        from quantmaster.rotation.service import _expected_market_session

        previous = self.store.etf_observations()
        issues: list[str] = []
        acquired_at = market_now().isoformat()
        end_value = as_of or _expected_market_session()
        calendar_error: BaseException | None = None
        if not end_value:
            # The shared resolver may be unavailable in an offline/local-only
            # installation.  Derive the ceiling from an actual provider calendar
            # rather than using the wall-clock date as a synthetic session.
            probe_end = market_date().isoformat()
            probe_start = (
                pd.Timestamp(market_date()) - pd.Timedelta(days=45)
            ).date().isoformat()
            local_calendar = getattr(self.local_source, "trade_calendar", None)
            using_local_calendar = callable(local_calendar)
            calendar_reader = local_calendar
            if not using_local_calendar:
                calendar_reader = self.source.trade_calendar
            try:
                observed = calendar_reader(probe_start, probe_end)
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
                calendar_error = exc
                observed = []
            if len(observed) == 0 and using_local_calendar:
                try:
                    observed = self.source.trade_calendar(probe_start, probe_end)
                except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
                    calendar_error = exc
                    observed = []
            candidates = pd.to_datetime(pd.Index(observed), errors="coerce")
            candidates = candidates[~candidates.isna()]
            eligible = [stamp for stamp in candidates if stamp.date() <= market_date()]
            if eligible:
                end_value = max(stamp.date().isoformat() for stamp in eligible)
        # Capture the local directory before remote metadata enhancement can
        # fail, but bind its effective date to a completed-session hint (or a
        # bounded prior day), never to today's wall-clock date.
        metadata_end = pd.Timestamp(
            as_of or end_value or (market_date() - pd.Timedelta(days=1)).isoformat()
        ).normalize()
        local_basic = self._local_etf_metadata(previous, metadata_end)
        end = pd.Timestamp(end_value) if end_value else pd.NaT
        if end is pd.NaT or pd.isna(end):
            if calendar_error is not None:
                raise calendar_error
            raise RuntimeError("无法确认 ETF 观察的最近完成交易日")
        history_start = end - pd.DateOffset(years=3, days=20)
        recent_start = end - pd.Timedelta(days=45 if previous.empty else 20)
        if not local_basic.empty:
            progress(3, "读取本地 ETF 元数据", f"stockdb 证券主表 {len(local_basic)} 只")
        calendar: list[str] = []
        local_calendar = getattr(self.local_source, "trade_calendar", None)
        if callable(local_calendar):
            try:
                calendar = local_calendar(str(recent_start.date()), str(end.date()))
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
                issues.append(f"stockdb 交易日历不可用，降级 Tushare：{_compact_error(exc)}")
        if not calendar:
            calendar = self.source.trade_calendar(str(recent_start.date()), str(end.date()))
        metadata_source = "tushare:etf_basic"
        disabled_etf_basic = PROVIDER_HEALTH.disabled_status("tushare:etf_basic")
        try:
            if disabled_etf_basic:
                raise RotationProviderCallError("当前 Tushare 凭据已确认无 etf_basic 权限")
            basic = self._tushare_call(
                "etf_basic",
                7,
                list_status="L",
                fields=(
                    "ts_code,extname,cname,index_code,index_name,list_date,list_status,"
                    "exchange,mgr_name,custod_name,mgt_fee,etf_type"
                ),
            ).rename(
                columns={
                    "extname": "name",
                    "index_name": "benchmark",
                    "index_code": "benchmark_code",
                }
            )
            basic["fund_type"] = basic.get("etf_type", "ETF")
            basic["invest_type"] = basic.get("etf_type", "")
            try:
                benchmark_catalog = self._tushare_call(
                    "mkt_idx_bmk",
                    5,
                    fields="ts_code,name,fullname,bmk_level,bmk_type,bmk_src,idx_type",
                ).rename(
                    columns={
                        "ts_code": "benchmark_code",
                        "bmk_level": "benchmark_level",
                        "bmk_type": "benchmark_type",
                        "bmk_src": "index_provider",
                        "idx_type": "index_type",
                    }
                )
                if "benchmark_code" in benchmark_catalog:
                    benchmark_catalog["benchmark_code"] = (
                        benchmark_catalog["benchmark_code"].astype(str).str.upper()
                    )
                    basic["benchmark_code"] = basic["benchmark_code"].astype(str).str.upper()
                    basic = basic.merge(
                        benchmark_catalog[
                            [
                                "benchmark_code",
                                "benchmark_level",
                                "benchmark_type",
                                "index_provider",
                                "index_type",
                            ]
                        ].drop_duplicates("benchmark_code", keep="last"),
                        on="benchmark_code",
                        how="left",
                    )
            except RotationProviderCallError as exc:
                issues.append(f"mkt_idx_bmk 不可用，指数分类使用显式规则：{_compact_error(exc)}")
                logger.warning(issues[-1])
        except RotationProviderCallError as exc:
            metadata_source = "tushare:fund_basic"
            try:
                basic = self._tushare_call(
                    "fund_basic",
                    7,
                    market="E",
                    status="L",
                    fields="ts_code,name,fund_type,invest_type,benchmark,list_date,market",
                )
                if disabled_etf_basic:
                    issues.append("etf_basic 已按当前凭据跳过，官方字段直接使用 fund_basic")
                    logger.info(issues[-1])
                else:
                    issues.append(
                        f"etf_basic 不可用，官方字段降级 fund_basic：{_compact_error(exc)}"
                    )
                    logger.warning(issues[-1])
            except RotationProviderCallError as fallback_exc:
                if local_basic.empty:
                    raise
                metadata_source = "free-stockdb:security-master"
                basic = local_basic.copy()
                issues.append(
                    "Tushare ETF 元数据增强不可用，继续使用 stockdb 全市场目录："
                    f"{_compact_error(fallback_exc)}"
                )
                logger.warning(issues[-1])
        basic = basic.rename(columns={"ts_code": "symbol"})
        basic["symbol"] = basic["symbol"].astype(str).str.upper()
        if "name" not in basic:
            basic["name"] = ""
        if "cname" in basic:
            basic["name"] = basic["name"].replace("", pd.NA).fillna(basic["cname"])
        if "fund_type" not in basic:
            basic["fund_type"] = ""
        basic["name"] = basic["name"].astype(str)
        basic["fund_type"] = basic["fund_type"].astype(str)
        basic = basic[
            basic["symbol"].str.endswith((".SH", ".SZ"), na=False)
            & ~basic["name"].str.contains("联接|LOF", case=False, na=False)
            & (
                basic["name"].str.contains("ETF", case=False, na=False)
                | basic["fund_type"].str.contains("ETF|交易型", case=False, na=False)
            )
        ].copy()
        if "benchmark" not in basic:
            basic["benchmark"] = ""
        if "benchmark_code" not in basic:
            basic["benchmark_code"] = ""
        if "list_date" not in basic:
            basic["list_date"] = ""
        for column in (
            "benchmark_level",
            "benchmark_type",
            "index_provider",
            "index_type",
            "mgr_name",
            "custod_name",
            "mgt_fee",
            "etf_type",
        ):
            if column not in basic:
                basic[column] = ""
        taxonomies = [
            classify_etf_profile(
                name,
                benchmark="" if pd.isna(benchmark) else str(benchmark),
                benchmark_code="" if pd.isna(code) else str(code),
                index_name=("" if pd.isna(benchmark) else str(benchmark))
                if metadata_source.endswith("etf_basic")
                else "",
                fund_type="" if pd.isna(fund_type) else str(fund_type),
                invest_type="" if pd.isna(invest_type) else str(invest_type),
                etf_type="" if pd.isna(etf_type) else str(etf_type),
                benchmark_type="" if pd.isna(benchmark_type) else str(benchmark_type),
                index_type="" if pd.isna(index_type) else str(index_type),
                metadata_source="etf_basic" if metadata_source.endswith("etf_basic") else "fund_basic",
            )
            for name, benchmark, code, fund_type, invest_type, etf_type, benchmark_type, index_type in zip(
                basic["name"],
                basic["benchmark"],
                basic["benchmark_code"],
                basic["fund_type"],
                basic.get("invest_type", pd.Series("", index=basic.index)),
                basic.get("etf_type", pd.Series("", index=basic.index)),
                basic.get("benchmark_type", pd.Series("", index=basic.index)),
                basic.get("index_type", pd.Series("", index=basic.index)),
                strict=True,
            )
        ]
        basic["category"] = [item["category"] for item in taxonomies]
        basic["asset_class"] = [item["asset_class"] for item in taxonomies]
        basic["sector_id"] = [item["sector_id"] for item in taxonomies]
        basic["sector_name"] = [item["sector_name"] for item in taxonomies]
        basic["normalized_index"] = [item["normalized_index"] for item in taxonomies]
        basic["classification_source"] = [item["classification_source"] for item in taxonomies]
        basic["classification_confidence"] = [
            item["classification_confidence"] for item in taxonomies
        ]
        if metadata_source.startswith("tushare:"):
            basic["metadata_source"] = metadata_source
            basic["updated_at"] = end.date().isoformat()
        else:
            if "metadata_source" not in basic:
                basic["metadata_source"] = "free-stockdb:security-master"
            if "updated_at" not in basic:
                basic["updated_at"] = end.date().isoformat()
        if basic.empty:
            raise RuntimeError("基金目录中没有可核查的场内 ETF")
        if not local_basic.empty and metadata_source.startswith("tushare:"):
            basic = pd.concat([local_basic, basic], ignore_index=True, sort=False)
            source_priority = basic["metadata_source"].fillna("").astype(str).map(
                lambda value: 3 if "etf_basic" in value else 2 if "fund_basic" in value else 1
            )
            basic = (
                basic.assign(_metadata_priority=source_priority)
                .sort_values(["symbol", "_metadata_priority"])
                .drop_duplicates("symbol", keep="last")
                .drop(columns="_metadata_priority")
                .reset_index(drop=True)
            )
            catalog = local_basic.set_index("symbol")
            for column in (
                "exchange",
                "asset_type",
                "status",
                "list_date",
                "delist_date",
                "effective_date",
                "directory_member_source",
                "directory_member_observed_at",
                "directory_source",
                "directory_acquired_at",
                "directory_cutoff_at",
                "directory_freshness",
                "directory_master_record_count",
                "directory_master_batch_record_count",
                "directory_master_snapshot_sha256",
                "directory_catalog_snapshot_id",
                "directory_catalog_records_sha256",
                "directory_catalog_file_sha256",
                "directory_catalog_file_size",
                "directory_catalog_file_mtime_ns",
                "directory_catalog_relative_path",
                "directory_catalog_as_of",
                "directory_catalog_expected_count",
                "directory_attestation_sha256",
                "directory_snapshot_id",
                "directory_complete",
                "directory_expected_symbols",
                "directory_observed_symbols",
                "directory_quality_reason",
            ):
                fallback = basic["symbol"].map(catalog[column])
                if column not in basic:
                    basic[column] = fallback
                    continue
                missing = basic[column].isna()
                if basic[column].dtype == object:
                    missing |= basic[column].astype(str).str.strip().isin({"", "nan", "None"})
                basic.loc[missing, column] = fallback.loc[missing]
        metadata_observed = pd.Timestamp(market_now())
        if metadata_observed.tzinfo is None:
            metadata_observed = metadata_observed.tz_localize("Asia/Shanghai")
        if not local_basic.empty and "observed_at" in local_basic:
            local_observed = pd.to_datetime(
                local_basic["observed_at"], errors="coerce", utc=True
            ).max()
            candidate_utc = metadata_observed.tz_convert("UTC")
            if pd.notna(local_observed) and candidate_utc <= local_observed:
                metadata_observed = (
                    local_observed + pd.Timedelta(microseconds=1)
                ).tz_convert("Asia/Shanghai")
        basic["observed_at"] = metadata_observed.isoformat()
        self.store.save_etf_metadata(basic)
        names = basic.set_index("symbol")["name"].to_dict()
        categories = basic.set_index("symbol")["category"].to_dict()
        benchmarks = basic.set_index("symbol")["benchmark"].fillna("").astype(str).to_dict()
        listing_dates = basic.set_index("symbol")["list_date"].to_dict()
        rows: list[pd.DataFrame] = []
        previous_latest = pd.Series(dtype="datetime64[ns]")
        if not previous.empty and {"symbol", "trade_date"}.issubset(previous.columns):
            parsed_latest = previous.copy()
            parsed_latest["trade_date"] = pd.to_datetime(parsed_latest["trade_date"], errors="coerce")
            previous_latest = parsed_latest.groupby("symbol")["trade_date"].max()
        current_symbols = sum(
            pd.notna(previous_latest.get(symbol, pd.NaT))
            and pd.Timestamp(previous_latest.get(symbol)).date() >= (end - pd.Timedelta(days=10)).date()
            for symbol in categories
        )
        bootstrap_sessions = 25 if current_symbols < len(categories) * 0.8 else 6
        dates = [pd.Timestamp(value) for value in calendar if pd.Timestamp(value) <= end][
            -bootstrap_sessions:
        ]

        previous_dates = pd.Series(dtype="datetime64[ns]")
        if not previous.empty and {"symbol", "trade_date"}.issubset(previous.columns):
            parsed_previous = previous.copy()
            parsed_previous["trade_date"] = pd.to_datetime(
                parsed_previous["trade_date"],
                errors="coerce",
            )
            previous_dates = parsed_previous.groupby("symbol")["trade_date"].min()

        backfill_targets: list[tuple[str, pd.Timestamp, pd.Timestamp]] = []
        history_symbols = {
            symbol
            for symbol, name, benchmark in zip(
                basic["symbol"], basic["name"], basic["benchmark"], strict=True
            )
            if _broad_etf_category(name, benchmark)
        }
        for symbol in history_symbols:
            listed = pd.to_datetime(listing_dates.get(symbol), errors="coerce")
            start_at = max(history_start, listed) if pd.notna(listed) else history_start
            existing_start = previous_dates.get(symbol, pd.NaT)
            stop_at = pd.Timestamp(existing_start) - pd.Timedelta(days=1) if pd.notna(existing_start) else end
            if start_at.normalize() <= stop_at.normalize():
                backfill_targets.append((symbol, start_at.normalize(), stop_at.normalize()))

        for index, (symbol, start_at, stop_at) in enumerate(backfill_targets, start=1):
            if cancelled():
                raise InterruptedError("ETF 历史份额同步已取消")
            try:
                shares = self._tushare_call(
                    "fund_share",
                    30,
                    ts_code=symbol,
                    start_date=start_at.strftime("%Y%m%d"),
                    end_date=stop_at.strftime("%Y%m%d"),
                    fields="ts_code,trade_date,fd_share",
                ).rename(columns={"ts_code": "symbol", "fd_share": "shares"})
                daily = self._tushare_call(
                    "fund_daily",
                    30,
                    ts_code=symbol,
                    start_date=start_at.strftime("%Y%m%d"),
                    end_date=stop_at.strftime("%Y%m%d"),
                    fields="ts_code,trade_date,close",
                ).rename(columns={"ts_code": "symbol"})
            except RotationProviderCallError as exc:
                issue = f"{names.get(symbol, symbol)} 历史资金数据回填失败：{_compact_error(exc)}"
                issues.append(issue)
                logger.warning(issue)
                continue
            if not {"symbol", "trade_date", "shares"}.issubset(shares.columns):
                continue
            if not {"symbol", "trade_date", "close"}.issubset(daily.columns):
                daily = pd.DataFrame(columns=["symbol", "trade_date", "close"])
            shares["shares"] = pd.to_numeric(shares["shares"], errors="coerce") * 10_000
            merged = shares.merge(
                daily[["symbol", "trade_date", "close"]],
                on=["symbol", "trade_date"],
                how="left",
            )
            merged["nav"] = float("nan")
            merged["trade_date"] = pd.to_datetime(merged["trade_date"], errors="coerce")
            merged["name"] = merged["symbol"].map(names).fillna(merged["symbol"])
            merged["category"] = merged["symbol"].map(categories)
            merged["benchmark"] = merged["symbol"].map(benchmarks).fillna("")
            merged["total_size"] = pd.NA
            merged["share_source"] = "tushare:fund_share"
            rows.append(
                merged[
                    [
                        "trade_date",
                        "symbol",
                        "name",
                        "category",
                        "benchmark",
                        "shares",
                        "nav",
                        "close",
                        "total_size",
                        "share_source",
                    ]
                ]
            )
            progress(
                55 + round(3 * index / max(1, len(backfill_targets))),
                "回填 ETF 历史",
                f"{index}/{len(backfill_targets)} · {names.get(symbol, symbol)} · "
                f"{start_at.date()}—{stop_at.date()}",
            )

        nav_available = True
        disabled_share_size = PROVIDER_HEALTH.disabled_status("tushare:etf_share_size")
        share_size_available = disabled_share_size is None
        if disabled_share_size:
            issues.append("etf_share_size 已按当前凭据跳过，最近份额直接使用 fund_share")
            logger.info(issues[-1])
        for index, trade_date in enumerate(dates, start=1):
            if cancelled():
                raise InterruptedError("ETF 份额同步已取消")
            compact = trade_date.strftime("%Y%m%d")
            shares = pd.DataFrame()
            if share_size_available:
                try:
                    shares = self._tushare_call(
                        "etf_share_size",
                        1,
                        trade_date=compact,
                        fields=("trade_date,ts_code,etf_name,total_share,total_size,nav,close,exchange"),
                    ).rename(
                        columns={
                            "ts_code": "symbol",
                            "etf_name": "reported_name",
                            "total_share": "shares",
                        }
                    )
                    if {"symbol", "shares"}.issubset(shares.columns):
                        shares["shares"] = pd.to_numeric(shares["shares"], errors="coerce") * 10_000
                        if "total_size" in shares:
                            shares["total_size"] = (
                                pd.to_numeric(shares["total_size"], errors="coerce") * 10_000
                            )
                        else:
                            shares["total_size"] = pd.NA
                        shares["share_source"] = "tushare:etf_share_size"
                    else:
                        shares = pd.DataFrame()
                except RotationProviderCallError as exc:
                    share_size_available = False
                    issues.append(f"etf_share_size 不可用，最近份额降级 fund_share：{_compact_error(exc)}")
                    logger.warning(issues[-1])
            daily = pd.DataFrame()
            if shares.empty:
                shares = self.source._call(
                    "fund_share",
                    1,
                    trade_date=compact,
                    fields="ts_code,trade_date,fd_share",
                ).rename(columns={"ts_code": "symbol", "fd_share": "shares"})
                daily = self.source._call(
                    "fund_daily",
                    1,
                    trade_date=compact,
                    fields="ts_code,trade_date,close",
                ).rename(columns={"ts_code": "symbol"})
                shares["shares"] = pd.to_numeric(shares.get("shares"), errors="coerce") * 10_000
                shares["total_size"] = pd.NA
                shares["share_source"] = "tushare:fund_share"
            if not {"symbol", "shares"}.issubset(shares.columns):
                continue
            if "close" in shares:
                daily = shares[["symbol", "close"]].copy()
            if not {"symbol", "close"}.issubset(daily.columns):
                daily = pd.DataFrame(columns=["symbol", "close"])
            nav = (
                shares[["symbol", "nav"]].copy()
                if "nav" in shares
                else pd.DataFrame(columns=["symbol", "nav"])
            )
            if nav.empty and nav_available:
                try:
                    nav = self._tushare_call(
                        "fund_nav",
                        1,
                        nav_date=compact,
                        market="E",
                        fields="ts_code,nav_date,unit_nav",
                    ).rename(columns={"ts_code": "symbol", "unit_nav": "nav"})
                    if not {"symbol", "nav"}.issubset(nav.columns):
                        nav = pd.DataFrame(columns=["symbol", "nav"])
                except RotationProviderCallError as exc:
                    nav_available = False
                    logger.warning(
                        "场内基金单位净值接口不可用，本轮 ETF 资金改用收盘价并逐只标记：%s",
                        _compact_error(exc),
                    )
            if shares.empty:
                continue
            shares = shares[shares["symbol"].isin(categories)]
            if shares.empty:
                continue
            selected_columns = ["symbol", "shares", "total_size", "share_source"]
            merged = shares[selected_columns].merge(daily[["symbol", "close"]], on="symbol", how="left")
            if not nav.empty:
                nav = nav.drop_duplicates("symbol", keep="last")
                merged = merged.merge(nav[["symbol", "nav"]], on="symbol", how="left")
            else:
                merged["nav"] = pd.NA
            merged["trade_date"] = trade_date.normalize()
            merged["acquired_at"] = acquired_at
            merged["name"] = merged["symbol"].map(names).fillna(merged["symbol"])
            merged["category"] = merged["symbol"].map(categories)
            merged["benchmark"] = merged["symbol"].map(benchmarks).fillna("")
            rows.append(
                merged[
                    [
                        "trade_date",
                        "symbol",
                        "name",
                        "category",
                        "benchmark",
                        "shares",
                        "nav",
                        "close",
                        "total_size",
                        "share_source",
                        "acquired_at",
                    ]
                ]
            )
            progress(
                58 + round(4 * index / max(1, len(dates))),
                "同步 ETF 份额",
                f"{index}/{len(dates)} · {trade_date.date()}",
            )
        if not rows and previous.empty:
            raise RuntimeError("ETF 份额接口未返回可用数据")
        if not previous.empty and "benchmark" not in previous:
            previous["benchmark"] = ""
        if not previous.empty and "total_size" not in previous:
            previous["total_size"] = pd.NA
        if not previous.empty and "share_source" not in previous:
            previous["share_source"] = "tushare:fund_share"
        if not previous.empty and "acquired_at" not in previous:
            # Clean replacement: legacy observations stay visible for current
            # diagnostics but cannot enter historical replay without evidence.
            previous["acquired_at"] = pd.NaT
        result = pd.concat(
            [*([previous] if not previous.empty else []), *rows],
            ignore_index=True,
        )
        result["category"] = [
            classify_etf_profile(
                "" if pd.isna(name) else str(name),
                benchmark="" if pd.isna(benchmark) else str(benchmark),
            )["category"]
            for name, benchmark in zip(
                result.get("name", pd.Series("", index=result.index)),
                result.get("benchmark", pd.Series("", index=result.index)),
                strict=True,
            )
        ]
        result["trade_date"] = pd.to_datetime(result["trade_date"], errors="coerce")
        evidenced_groups = result.groupby(
            ["trade_date", "symbol"], dropna=False
        )["acquired_at"].transform(lambda values: values.notna().any())
        # Once a dated observation has acquisition evidence, discard an
        # otherwise identical legacy projection whose acquisition time is
        # unknowable.  Real observations from distinct acquisition instants
        # remain append-only for point-in-time replay.
        result = result.loc[
            ~(result["acquired_at"].isna() & evidenced_groups)
        ]
        result = (
            result.dropna(subset=["trade_date", "symbol"])
            .drop_duplicates(
                ["trade_date", "symbol", "acquired_at"],
                keep="last",
            )
            .sort_values(["symbol", "trade_date", "acquired_at"])
        )
        self.store.save_etf_observations(result)
        available_dates = result["trade_date"].dropna()
        return {
            "rows": len(result),
            "symbols": int(result["symbol"].nunique()),
            "history_start": str(available_dates.min().date()) if not available_dates.empty else "",
            "history_end": str(available_dates.max().date()) if not available_dates.empty else "",
            "metadata_source": (
                metadata_source
                if local_basic.empty
                else f"free-stockdb:security-master + {metadata_source}"
            ),
            "share_source": "etf_share_size" if share_size_available else "fund_share",
            "issues": issues,
        }
