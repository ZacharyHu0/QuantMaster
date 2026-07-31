"""Rotation snapshot builder and lease-based background worker."""

from __future__ import annotations

import hashlib
import logging
import sqlite3
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.industry import load_cached_industry_map
from quantmaster.data.instruments import InstrumentStore
from quantmaster.data.storage import BarStore
from quantmaster.rotation.analytics import (
    ALGORITHM_VERSION,
    analyze_group_rotation,
    compute_market_structure,
    compute_market_temperature,
    compute_trend_matrices,
    estimate_etf_flows,
)
from quantmaster.rotation.contracts import RotationJobSpec
from quantmaster.rotation.store import (
    RotationIntegrityError,
    RotationJobStore,
    RotationStore,
)
from quantmaster.rotation.taxonomy import (
    SW2021_L1,
    merge_l2_groups,
    strict_l1_groups,
    taxonomy_payload,
)
from quantmaster.runtime.jobs import WorkerIdentity
from quantmaster.runtime.json import strict_json_dumps

logger = logging.getLogger(__name__)
Progress = Callable[[int, str, str], None]
Cancelled = Callable[[], bool]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _snapshot_id(as_of: str, columns: list[str], scope: str) -> str:
    logical = strict_json_dumps({
        "algorithm": ALGORITHM_VERSION,
        "as_of": as_of,
        "columns": sorted(columns),
        "scope": scope,
    }, sort_keys=True)
    return hashlib.sha256(logical.encode("utf-8")).hexdigest()[:20]


def _status_quality(
    status: str,
    *,
    eligible: int | None = None,
    expected: int | None = None,
    issues: list[str] | None = None,
) -> dict[str, Any]:
    eligible_count = int(eligible) if eligible is not None else None
    expected_count = int(expected) if expected is not None else None
    return {
        "status": status,
        "eligible_count": eligible_count,
        "expected_count": expected_count,
        "coverage": (
            round(eligible_count / expected_count, 4)
            if eligible_count is not None and expected_count is not None
            and expected_count > 0
            else None
        ),
        "issues": list(issues or []),
    }


class RotationDataLoader:
    """Read verified local research partitions, falling back to BarStore."""

    def __init__(self, store: RotationStore):
        self.store = store

    @staticmethod
    def _listed_instruments() -> tuple[InstrumentStore, int]:
        instruments = InstrumentStore()
        with instruments._connection() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM instruments WHERE market='CN' "
                "AND asset_type='stock' AND lower(status) IN ('l','listed')"
            ).fetchone()
        return instruments, int(row["count"] if row else 0)

    def _research_lake(self) -> tuple[pd.DataFrame, pd.DataFrame] | None:
        if not (get_config().data_root / "research_lake").is_dir():
            return None
        try:
            from quantmaster.research.contracts import ArtifactKind, AssetClass, Frequency
            from quantmaster.research.lake import ResearchLake

            lake = ResearchLake()
            partitions = lake.catalog.partitions(
                kind=ArtifactKind.RAW,
                asset_class=AssetClass.STOCK,
                frequency=Frequency.DAILY,
                dataset_id="stock_bars",
            )
            if len(partitions) < 30:
                return None
            dates = sorted({str(item["trade_date"]) for item in partitions})[-820:]
            frame = lake.read_range(
                ArtifactKind.RAW,
                AssetClass.STOCK,
                Frequency.DAILY,
                "stock_bars",
                dates[0],
                dates[-1],
                columns=["trade_date", "symbol", "close", "amount"],
            )
            if frame.empty:
                return None
            frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
            close = frame.pivot_table(
                index="trade_date", columns="symbol", values="close", aggfunc="last",
            )
            amount = frame.pivot_table(
                index="trade_date", columns="symbol", values="amount", aggfunc="last",
            )
            return close, amount
        except (ImportError, OSError, ValueError, RuntimeError):
            logger.debug("研究湖全市场日线暂不可用", exc_info=True)
            return None

    def market_matrices(
        self,
        *,
        progress: Progress,
        cancelled: Cancelled,
    ) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str], int, list[str]]:
        instruments, expected_count = self._listed_instruments()
        lake_values = self._research_lake()
        if lake_values is not None:
            close, amount = lake_values
            records = instruments.get_many(close.columns)
            selected = [
                symbol for symbol in close.columns
                if (record := records.get(str(symbol))) is not None
                and record.asset_type == "stock"
                and record.market == "CN"
                and record.status.lower() in {"l", "listed"}
            ]
            close, amount = close[selected], amount.reindex(columns=selected)
            names = {symbol: records[symbol].name or symbol for symbol in selected}
            progress(30, "读取全市场研究湖", f"已读取 {len(selected)} 只股票")
            return close, amount, names, expected_count, ["tushare:research_lake"]

        bars = BarStore()
        symbols = [
            symbol for symbol in bars.symbols()
            if symbol.endswith((".SH", ".SZ", ".BJ"))
            and len(symbol.rsplit(".", 1)[0]) == 6
        ]
        records = instruments.get_many(symbols)
        selected = [
            symbol for symbol in symbols
            if (record := records.get(symbol)) is not None
            and record.asset_type == "stock"
            and record.market == "CN"
            and record.status.lower() in {"l", "listed"}
        ]
        if not selected:
            selected = symbols
        names = {
            symbol: (records[symbol].name or symbol) if symbol in records else symbol
            for symbol in selected
        }

        def load_one(symbol: str) -> tuple[str, pd.Series, pd.Series] | None:
            frame = bars.get(symbol)
            if frame is None or frame.empty or "close" not in frame:
                return None
            frame = frame.tail(820)
            close = pd.to_numeric(frame["close"], errors="coerce").rename(symbol)
            if "amount" in frame:
                amount = pd.to_numeric(frame["amount"], errors="coerce").rename(symbol)
            elif "volume" in frame:
                amount = (
                    pd.to_numeric(frame["volume"], errors="coerce") * close
                ).rename(symbol)
            else:
                amount = pd.Series(index=close.index, dtype=float, name=symbol)
            return symbol, close, amount

        close_series: list[pd.Series] = []
        amount_series: list[pd.Series] = []
        total = len(selected)
        progress(4, "读取本地行情", f"准备校验 {total} 只股票")
        with ThreadPoolExecutor(max_workers=min(8, max(1, total))) as executor:
            futures = {executor.submit(load_one, symbol): symbol for symbol in selected}
            for completed, future in enumerate(as_completed(futures), start=1):
                if cancelled():
                    raise InterruptedError("板块联动刷新已取消")
                value = future.result()
                if value is not None:
                    _, close, amount = value
                    if close.notna().sum() >= 30:
                        close_series.append(close)
                        amount_series.append(amount)
                if completed == total or completed % 40 == 0:
                    progress(
                        4 + round(25 * completed / max(1, total)),
                        "读取本地行情",
                        f"{completed}/{total} · 可计算 {len(close_series)}",
                    )
        if not close_series:
            raise ValueError("本地没有至少 30 日的 A 股行情；请先在数据管理中同步行情")
        close = pd.concat(close_series, axis=1).sort_index()
        amount = pd.concat(amount_series, axis=1).reindex(close.index).sort_index()
        selected_names = {symbol: names.get(symbol, symbol) for symbol in close.columns}
        return close, amount, selected_names, expected_count, ["local:bar_store"]


def _deduplicate_themes(themes: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Collapse near-identical concepts at Jaccard >= .85 while retaining aliases."""
    result: dict[str, dict[str, Any]] = {}
    memberships: list[tuple[str, set[str]]] = []
    for raw in sorted(themes, key=lambda item: str(item.get("code") or "")):
        code = str(raw.get("code") or "").strip().upper()
        name = str(raw.get("name") or "").strip()
        members = {
            str(symbol).upper() for symbol in raw.get("members") or []
            if str(symbol).upper().endswith((".SH", ".SZ", ".BJ"))
        }
        if not code or not name or not members:
            continue
        duplicate = ""
        for existing_code, existing_members in memberships:
            union = members | existing_members
            if union and len(members & existing_members) / len(union) >= 0.85:
                duplicate = existing_code
                break
        if duplicate:
            result[duplicate].setdefault("aliases", []).append(name)
            continue
        result[code] = {
            "code": code,
            "name": name,
            "level": "concept",
            "parent_code": "",
            "members": sorted(members),
            "aliases": list(dict.fromkeys(str(value) for value in raw.get("aliases") or [])),
            "source": str(raw.get("source") or "eastmoney-concept"),
        }
        memberships.append((code, members))
    return result


class RotationService:
    def __init__(
        self,
        store: RotationStore | None = None,
        jobs: RotationJobStore | None = None,
    ):
        self.store = store or RotationStore()
        self.jobs = jobs or RotationJobStore()
        self.loader = RotationDataLoader(self.store)

    @staticmethod
    def _meta(
        *,
        snapshot_id: str,
        as_of: str,
        generated_at: str,
        quality: dict[str, Any],
        sources: list[str],
    ) -> dict[str, Any]:
        theme_taxonomy = next((
            source for source in sources
            if source in {"eastmoney-concept", "tushare:dc-concept"}
        ), "eastmoney-concept")
        return {
            "snapshot_id": snapshot_id,
            "as_of": as_of,
            "generated_at": generated_at,
            "algorithm_version": ALGORITHM_VERSION,
            "taxonomy_versions": {
                "industry": "SW2021",
                "theme": theme_taxonomy,
            },
            "quality": quality,
            "sources": sources,
        }

    def _envelope(
        self,
        data: dict[str, Any],
        *,
        snapshot_id: str,
        generated_at: str,
        quality: dict[str, Any],
        sources: list[str],
    ) -> dict[str, Any]:
        return {
            "meta": self._meta(
                snapshot_id=snapshot_id,
                as_of=str(data.get("as_of") or ""),
                generated_at=generated_at,
                quality=quality,
                sources=sources,
            ),
            "data": data,
        }

    def build(
        self,
        spec: RotationJobSpec,
        *,
        progress: Progress,
        cancelled: Cancelled,
    ) -> dict[str, Any]:
        scope = spec.scope
        generated_at = _utc_now()
        computed: dict[str, dict[str, Any]] = {}
        need_market = scope in {"all", "close", "market", "industries", "themes"}
        provider_warnings: list[str] = []
        provider_results: dict[str, dict[str, Any]] = {}
        if spec.source == "auto":
            from quantmaster.rotation.provider import RotationProvider

            provider = RotationProvider(self.store)
            operations: list[tuple[str, str, Callable[[], dict[str, Any]]]] = []
            if need_market:
                operations.append((
                    "market",
                    "全市场日线",
                    lambda: provider.sync_market_history(
                        progress, cancelled, rebuild=spec.mode == "rebuild",
                    ),
                ))
            if scope in {"all", "close", "industries"}:
                operations.append((
                    "industries",
                    "申万行业层级",
                    lambda: provider.sync_industry_taxonomy(progress, cancelled),
                ))
            if scope in {"all", "close", "themes"}:
                operations.append((
                    "themes",
                    "细分题材目录",
                    lambda: provider.sync_themes(progress, cancelled),
                ))
            if scope in {"all", "etf"}:
                operations.append((
                    "etf",
                    "ETF 份额",
                    lambda: provider.sync_etf_observations(progress, cancelled),
                ))
            for key, label, operation in operations:
                try:
                    provider_results[key] = operation()
                except InterruptedError:
                    raise
                except Exception as exc:  # 外部数据源边界：记录后降级到已有快照
                    logger.warning("%s 同步失败，板块联动将使用本地覆盖", label, exc_info=True)
                    provider_warnings.append(f"{label}同步失败：{str(exc)[:160]}")
        close = pd.DataFrame()
        amount = pd.DataFrame()
        names: dict[str, str] = {}
        expected_count = 0
        sources = ["local:rotation_cache"]
        if need_market:
            loader_progress = progress
            if spec.source == "auto":
                def loader_progress(value: int, phase: str, detail: str) -> None:
                    progress(
                        62 + round(max(0, min(30, value)) * 0.20), phase, detail,
                    )
            close, amount, names, expected_count, sources = self.loader.market_matrices(
                progress=loader_progress, cancelled=cancelled,
            )
            if cancelled():
                raise InterruptedError("板块联动刷新已取消")
        as_of = str(close.index[-1].date()) if not close.empty else ""
        snapshot_id = _snapshot_id(as_of, [*list(close.columns), generated_at], scope)
        compute_base = 70 if spec.source == "auto" else 34
        trend = compute_trend_matrices(close) if need_market else None

        if scope in {"all", "close", "market"}:
            progress(compute_base, "计算市场温度", "汇总四档趋势分布与证据权重")
            temperature = compute_market_temperature(
                close, amount, expected_count=expected_count, trend=trend,
            )
            temperature_quality = temperature.pop("quality")
            temperature_quality["issues"] = list(dict.fromkeys([
                *(temperature_quality.get("issues") or []), *provider_warnings,
            ]))
            computed["temperature"] = self._envelope(
                temperature,
                snapshot_id=snapshot_id,
                generated_at=generated_at,
                quality=temperature_quality,
                sources=sources,
            )
            progress(compute_base + 7, "计算市场风格", "比较强势与低位样本收益分布")
            structure = compute_market_structure(close, names=names, trend=trend)
            computed["structure"] = self._envelope(
                structure,
                snapshot_id=snapshot_id,
                generated_at=generated_at,
                quality=temperature_quality,
                sources=sources,
            )

        l1_groups: dict[str, dict[str, Any]] = {}
        l2_groups: dict[str, dict[str, Any]] = {}
        if scope in {"all", "close", "industries"}:
            progress(compute_base + 12, "聚合申万行业", "严格过滤申万 2021 层级")
            strict_l1 = dict(SW2021_L1)
            dedicated_l1 = {
                str(node.get("code")): node for node in self.store.taxonomy_nodes("L1")
                if node.get("members")
                and strict_l1.get(str(node.get("code"))) == str(node.get("name") or "")
            }
            dedicated_count = sum(
                len(node.get("members") or []) for node in dedicated_l1.values()
            )
            l1_groups = (
                dedicated_l1
                if dedicated_count >= max(1000, round(expected_count * 0.70))
                else strict_l1_groups(load_cached_industry_map())
            )
            l2_groups = merge_l2_groups(l1_groups, self.store.taxonomy_nodes("L2"))
            industries = analyze_group_rotation(
                close, {**l1_groups, **l2_groups}, names=names, amount=amount,
                trend=trend,
            )
            count = len(industries["items"])
            industry_quality = _status_quality(
                "complete" if count >= 28 else "partial" if count >= 20 else "limited",
                eligible=count,
                expected=31 + len(l2_groups),
                issues=[
                    *([] if count >= 28 else ["部分行业未达到 8 只成分与 70% 行情覆盖门槛"]),
                    *provider_warnings,
                ],
            )
            computed["industries"] = self._envelope(
                industries,
                snapshot_id=snapshot_id,
                generated_at=generated_at,
                quality=industry_quality,
                sources=[*sources, "SW2021"],
            )
            taxonomy = taxonomy_payload(l1_groups, l2_groups)
            taxonomy["as_of"] = industries["as_of"]
            computed["taxonomy"] = self._envelope(
                taxonomy,
                snapshot_id=snapshot_id,
                generated_at=generated_at,
                quality=industry_quality,
                sources=["SW2021"],
            )

        if scope in {"all", "close", "themes"}:
            progress(compute_base + 17, "扫描细分题材", "合并高度重叠的概念板块")
            stored_themes = self.store.themes()
            themes = _deduplicate_themes(stored_themes)
            theme_sources = list(dict.fromkeys(
                str(item.get("source") or "") for item in stored_themes
                if str(item.get("source") or "")
            ))
            theme_provider_issues = list(
                provider_results.get("themes", {}).get("issues") or []
            )
            if themes:
                theme_data = analyze_group_rotation(
                    close, themes, names=names, amount=amount, kind="theme", trend=trend,
                )
                count = len(theme_data["items"])
                theme_quality = _status_quality(
                    "complete" if count >= 50 else "partial",
                    eligible=count,
                    expected=len(themes),
                    issues=[
                        *([] if count >= 50 else ["概念成分目录仍在积累"]),
                        *theme_provider_issues,
                        *provider_warnings,
                    ],
                )
            else:
                theme_data = {
                    "as_of": as_of,
                    "kind": "theme",
                    "items": [],
                    "details": {},
                    "summary": {"group_count": 0, "stages": {}},
                    "definition": {
                        "minimum_members": 8,
                        "minimum_coverage": 0.70,
                        "theme_score": "55% 生命周期 + 45% 宽度",
                    },
                }
                theme_quality = _status_quality(
                    "cold",
                    issues=["尚未建立细分题材成分目录", *provider_warnings],
                )
            computed["themes"] = self._envelope(
                theme_data,
                snapshot_id=snapshot_id,
                generated_at=generated_at,
                quality=theme_quality,
                sources=list(dict.fromkeys([*sources, *theme_sources])),
            )

        if scope in {"all", "etf"}:
            progress(compute_base + 21, "估算宽基资金", "按份额变化与净值计算申赎资金")
            etf_data = estimate_etf_flows(self.store.etf_observations())
            etf_ready = etf_data["summary"].get("status") == "ready"
            close_fallback_count = int(
                etf_data["summary"].get("close_fallback_count") or 0
            )
            etf_quality = _status_quality(
                "partial" if etf_ready and close_fallback_count else (
                    "complete" if etf_ready else "cold"
                ),
                eligible=len(etf_data["items"]),
                expected=len(etf_data["items"]),
                issues=[
                    *([] if etf_ready else ["等待 09:05 后的 ETF 份额快照"]),
                    *(
                        [f"{close_fallback_count} 只宽基 ETF 缺少单位净值，已使用收盘价"]
                        if close_fallback_count else []
                    ),
                    *provider_warnings,
                ],
            )
            etf_snapshot = _snapshot_id(
                etf_data.get("as_of") or as_of,
                [
                    *(str(item.get("symbol") or "") for item in etf_data["items"]),
                    generated_at,
                ],
                "etf",
            )
            price_sources = {
                str(item.get("price_source") or "") for item in etf_data["items"]
            }
            etf_sources = ["tushare:fund_share"]
            if "nav" in price_sources:
                etf_sources.append("tushare:fund_nav")
            if "close" in price_sources:
                etf_sources.append("tushare:fund_daily")
            etf_sources.append("local:rotation_cache")
            computed["etf_flows"] = self._envelope(
                etf_data,
                snapshot_id=etf_snapshot,
                generated_at=generated_at,
                quality=etf_quality,
                sources=etf_sources,
            )

        if cancelled():
            raise InterruptedError("板块联动刷新已取消")
        progress(96, "提交分析快照", "原子更新页面所需视图")
        self.store.save_snapshots(computed)
        return {
            "snapshot_id": snapshot_id,
            "as_of": as_of,
            "updated": sorted(computed),
            "tracked_count": len(close.columns),
            "expected_count": expected_count,
        }

    @staticmethod
    def cold(kind: str) -> dict[str, Any]:
        messages = {
            "temperature": "尚未生成市场温度快照",
            "structure": "尚未生成市场风格快照",
            "industries": "尚未生成行业周期快照",
            "themes": "尚未生成细分题材快照",
            "etf_flows": "尚未生成 ETF 资金快照",
            "taxonomy": "尚未建立申万行业目录",
        }
        return {
            "meta": {
                "snapshot_id": "",
                "as_of": "",
                "generated_at": "",
                "algorithm_version": ALGORITHM_VERSION,
            "taxonomy_versions": {
                    "industry": "SW2021", "theme": "eastmoney-concept",
                },
                "quality": _status_quality("cold", issues=[messages.get(kind, "尚无快照")]),
                "sources": [],
            },
            "data": {"as_of": "", "items": [], "message": messages.get(kind, "尚无快照")},
        }

    def snapshot(self, kind: str) -> dict[str, Any]:
        try:
            value = self.store.snapshot(kind) or self.cold(kind)
            quality = value.get("meta", {}).get("quality", {})
            expected = quality.get("expected_count")
            if expected is None or expected == 0:
                # v0.13.0 snapshots serialized an unknown denominator as 0%.  Keep
                # them readable while correcting the public meaning immediately.
                quality["coverage"] = None
            return value
        except RotationIntegrityError as exc:
            logger.error("板块联动快照完整性失败 kind=%s", kind, exc_info=True)
            value = self.cold(kind)
            value["meta"]["quality"] = _status_quality(
                "corrupt", issues=[str(exc), "请重新生成联动快照；损坏内容不会参与计算"],
            )
            value["data"]["message"] = str(exc)
            return value

    def taxonomy(self) -> dict[str, Any]:
        cached = self.store.snapshot("taxonomy")
        if cached is not None:
            return cached
        l1_groups = strict_l1_groups({})
        l2_groups = merge_l2_groups(l1_groups, self.store.taxonomy_nodes("L2"))
        data = taxonomy_payload(l1_groups, l2_groups)
        data.update({"as_of": "", "message": "目录可用；成分数量将在首次刷新后补齐"})
        return {
            "meta": self._meta(
                snapshot_id=_snapshot_id("", list(l1_groups), "taxonomy"),
                as_of="",
                generated_at="",
                quality=_status_quality(
                    "partial", issues=["尚未计算行业成分覆盖，当前只提供分类目录"],
                ),
                sources=["SW2021"],
            ),
            "data": data,
        }

    def overview(self) -> dict[str, Any]:
        temperature = self.snapshot("temperature")
        industries = self.snapshot("industries")
        themes = self.snapshot("themes")
        etf = self.snapshot("etf_flows")
        metas = [value["meta"] for value in (temperature, industries, themes, etf)]
        generated = max((str(meta.get("generated_at") or "") for meta in metas), default="")
        as_of = max((str(meta.get("as_of") or "") for meta in metas), default="")
        qualities = [str(meta.get("quality", {}).get("status") or "cold") for meta in metas]
        status = "complete" if all(value == "complete" for value in qualities) else (
            "cold" if all(value == "cold" for value in qualities) else "partial"
        )
        dimension_names = ("市场温度", "行业周期", "细分题材", "宽基资金")
        unavailable_statuses = {"cold", "corrupt", "empty"}
        available_dimensions = sum(
            value not in unavailable_statuses for value in qualities
        )
        selected_l2 = set(self.store.preferences()["l2_codes"])
        visible_industries = [
            item for item in industries["data"].get("items", [])
            if str(item.get("level")) == "L1"
            or str(item.get("code") or "").upper() in selected_l2
        ]
        data = {
            "as_of": as_of,
            "temperature": temperature["data"].get("current"),
            "industries": visible_industries[:8],
            "themes": themes["data"].get("items", [])[:8],
            "etf": etf["data"].get("summary", {}),
        }
        overview_quality = _status_quality(status, issues=(
            [
                f"当前 {available_dimensions}/4 个维度可用；"
                "各维度可能有不同快照时间，请以对应页面为准。"
            ]
            if status == "partial" else []
        ))
        overview_quality.update({
            "available_dimensions": available_dimensions,
            "total_dimensions": len(dimension_names),
            "dimension_statuses": dict(zip(dimension_names, qualities, strict=True)),
        })
        return {
            "meta": self._meta(
                snapshot_id=_snapshot_id(
                    as_of,
                    [str(meta.get("snapshot_id") or "") for meta in metas],
                    "overview",
                ),
                as_of=as_of,
                generated_at=generated,
                quality=overview_quality,
                sources=list(dict.fromkeys(
                    source for meta in metas for source in meta.get("sources") or []
                )),
            ),
            "data": data,
        }

    def detail(self, kind: str, code: str) -> dict[str, Any] | None:
        snapshot = self.snapshot(kind)
        details = snapshot.get("data", {}).get("details") or {}
        item = details.get(str(code).upper())
        if item is None:
            return None
        return {"meta": snapshot["meta"], "data": item}


class RotationWorker:
    def __init__(self, service: RotationService | None = None):
        self.service = service or RotationService()
        self.identity = WorkerIdentity.create("rotation")
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self, *, bootstrap_local: bool = False) -> None:
        if self._thread and self._thread.is_alive():
            return
        needs_local_snapshot = False
        if bootstrap_local:
            try:
                needs_local_snapshot = self.service.store.snapshot("temperature") is None
            except RotationIntegrityError:
                needs_local_snapshot = True
            now = datetime.now(ZoneInfo("Asia/Shanghai"))
            if now.weekday() < 5 and (now.hour, now.minute) >= (18, 30):
                needs_local_snapshot = False
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="quantmaster-rotation", daemon=True,
        )
        self._thread.start()
        if needs_local_snapshot:
            self.submit(RotationJobSpec(scope="close", source="local"))

    def stop(self, timeout: float = 10.0) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=max(0.1, timeout))

    @property
    def idle(self) -> bool:
        return self._thread is None or not self._thread.is_alive()

    def submit(self, spec: RotationJobSpec) -> dict[str, Any]:
        job = self.service.jobs.create(spec.model_dump(mode="json"))
        self._wake.set()
        return job

    def _scheduled(self) -> None:
        now = datetime.now(ZoneInfo("Asia/Shanghai"))
        if now.weekday() >= 5:
            return
        date_key = str(now.date())
        close_due = (now.hour, now.minute) >= (18, 30)
        etf_due = (now.hour, now.minute) >= (9, 5)
        if close_due and self.service.store.runtime_state("scheduled_close") != date_key:
            self.submit(RotationJobSpec(scope="close", source="auto"))
            self.service.store.set_runtime_state("scheduled_close", date_key)
        if etf_due and self.service.store.runtime_state("scheduled_etf") != date_key:
            self.submit(RotationJobSpec(scope="etf", source="auto"))
            self.service.store.set_runtime_state("scheduled_etf", date_key)

    def _run_job(self, job: dict[str, Any]) -> None:
        job_id = str(job["id"])
        owner = self.identity.value
        try:
            spec = RotationJobSpec.model_validate(job["spec"])
            result = self.service.build(
                spec,
                progress=lambda value, phase, detail: self.service.jobs.progress(
                    job_id, owner, value, phase, detail,
                ),
                cancelled=lambda: (
                    self._stop.is_set()
                    or self.service.jobs.is_cancel_requested(job_id, owner)
                ),
            )
            if self._stop.is_set() or self.service.jobs.is_cancel_requested(job_id, owner):
                self.service.jobs.fail(job_id, owner, "任务已安全取消", cancelled=True)
            else:
                self.service.jobs.complete(job_id, owner, result)
        except InterruptedError as exc:
            self.service.jobs.fail(job_id, owner, str(exc), cancelled=True)
        except Exception as exc:
            logger.exception("板块联动刷新失败 job_id=%s", job_id)
            self.service.jobs.fail(job_id, owner, str(exc))

    def _run(self) -> None:
        last_schedule_check = 0.0
        while not self._stop.is_set():
            now = time.monotonic()
            if now - last_schedule_check >= 45:
                try:
                    self._scheduled()
                except (OSError, sqlite3.Error):
                    logger.exception("板块联动定时检查失败")
                last_schedule_check = now
            job = self.service.jobs.claim(self.identity.value)
            if job is not None:
                self._run_job(job)
                continue
            self._wake.wait(1.0)
            self._wake.clear()


_SERVICE: RotationService | None = None
_WORKER: RotationWorker | None = None
_SINGLETON_LOCK = threading.RLock()


def get_rotation_service() -> RotationService:
    global _SERVICE
    with _SINGLETON_LOCK:
        if _SERVICE is None:
            _SERVICE = RotationService()
        return _SERVICE


def get_rotation_worker() -> RotationWorker:
    global _WORKER
    with _SINGLETON_LOCK:
        if _WORKER is None:
            _WORKER = RotationWorker(get_rotation_service())
        return _WORKER


def reset_rotation_runtime_for_tests() -> None:
    global _SERVICE, _WORKER
    with _SINGLETON_LOCK:
        if _WORKER is not None:
            _WORKER.stop(2.0)
        _WORKER = None
        _SERVICE = None
