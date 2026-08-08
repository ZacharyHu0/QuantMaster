"""Rotation snapshot builder and lease-based background worker."""

from __future__ import annotations

import copy
import hashlib
import logging
import sqlite3
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from statistics import median
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.industry import load_cached_industry_map
from quantmaster.data.instruments import InstrumentStore
from quantmaster.data.storage import BarStore
from quantmaster.rotation.analytics import (
    ALGORITHM_VERSION,
    GROUP_SCORE_WEIGHTS,
    MIN_GROUP_SCORE_WEIGHT,
    ROTATION_WINDOWS,
    analyze_group_rotation,
    compute_etf_capital_evidence,
    compute_market_structure,
    compute_market_temperature,
    compute_trend_matrices,
    estimate_etf_flows,
    map_theme_industries,
    market_temperature_reference_dates,
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
from quantmaster.trading_sessions import expected_session

logger = logging.getLogger(__name__)
Progress = Callable[[int, str, str], None]
Cancelled = Callable[[], bool]


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _market_close_epoch(as_of: str) -> float:
    value = datetime.fromisoformat(str(as_of)).replace(
        hour=15,
        minute=0,
        second=0,
        microsecond=0,
        tzinfo=ZoneInfo("Asia/Shanghai"),
    )
    return value.timestamp()


def _news_sentiment_evidence(as_of: str, *, minimum_events: int = 20) -> dict[str, Any]:
    fallback = {
        "available": False,
        "score": None,
        "as_of": str(as_of or ""),
        "note": "等待可核查资讯情绪",
        "event_count": 0,
        "signed_score": None,
        "lookback_days": 30,
    }
    if not as_of:
        return fallback
    try:
        from quantmaster.ai.crawler import NewsStore

        snapshot = NewsStore().market_sentiment(
            as_of=_market_close_epoch(as_of),
            days=30,
        )
    except (ImportError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
        logger.warning("市场温度读取本地资讯情绪失败", exc_info=True)
        return {**fallback, "note": "本地资讯情绪暂不可用"}
    event_count = int(snapshot.get("event_count") or 0)
    signed_score = float(snapshot.get("score") or 0)
    halflife_days = float(snapshot.get("halflife_days") or 0)
    if event_count < int(minimum_events):
        return {
            **fallback,
            "note": f"近 30 日合格资讯仅 {event_count} 条，至少需要 {minimum_events} 条",
            "event_count": event_count,
            "signed_score": round(signed_score, 2),
            "halflife_days": halflife_days,
        }
    score = max(0.0, min(100.0, 50.0 + signed_score / 2.0))
    label = str(snapshot.get("label") or "中性")
    return {
        "available": True,
        "score": round(score, 2),
        "as_of": str(as_of),
        "note": (
            f"{label} {signed_score:+.2f} · 近 30 日 {event_count} 条"
            f" · 半衰期 {halflife_days:g} 日"
        ),
        "event_count": event_count,
        "signed_score": round(signed_score, 2),
        "halflife_days": halflife_days,
        "lookback_days": int(snapshot.get("lookback_days") or 30),
    }


def _snapshot_id(as_of: str, columns: list[str], scope: str) -> str:
    logical = strict_json_dumps({
        "algorithm": ALGORITHM_VERSION,
        "as_of": as_of,
        "columns": sorted(columns),
        "scope": scope,
    }, sort_keys=True)
    return hashlib.sha256(logical.encode("utf-8")).hexdigest()[:20]


def _expected_market_session(now: datetime | None = None) -> str:
    """Latest completed session backed by an official or verified local calendar."""
    expectation = expected_session(now)
    return expectation.session if expectation.ready else ""


def _mark_stale(
    quality: dict[str, Any], as_of: str, expected_as_of: str,
) -> dict[str, Any]:
    value = dict(quality)
    issues = list(value.get("issues") or [])
    if (
        str(value.get("status") or "") not in {"cold", "empty", "corrupt", "loading"}
        and expected_as_of
        and (not as_of or as_of < expected_as_of)
    ):
        value["status"] = "stale"
        issue = f"行情仅到 {as_of or '未知日期'}，最近应完成交易日为 {expected_as_of}"
        if issue not in issues:
            issues.insert(0, issue)
    value["issues"] = issues
    return value


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
            lake_as_of = str(pd.Timestamp(close.index.max()).date())
            bar_metadata = BarStore().metadata_many()
            newer_bar_count = sum(
                str(meta.get("end") or "") > lake_as_of
                for symbol, meta in bar_metadata.items()
                if symbol.endswith((".SH", ".SZ", ".BJ"))
                and len(symbol.rsplit(".", 1)[0]) == 6
            )
            if newer_bar_count >= max(1000, round(expected_count * 0.70)):
                lake_values = None
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


def _load_l1_groups(store: RotationStore, expected_count: int) -> dict[str, dict[str, Any]]:
    strict_l1 = dict(SW2021_L1)
    dedicated_l1 = {
        str(node.get("code")): node for node in store.taxonomy_nodes("L1")
        if node.get("members")
        and strict_l1.get(str(node.get("code"))) == str(node.get("name") or "")
    }
    dedicated_count = sum(len(node.get("members") or []) for node in dedicated_l1.values())
    return (
        dedicated_l1
        if dedicated_count >= max(1000, round(expected_count * 0.70))
        else strict_l1_groups(load_cached_industry_map())
    )


def _signal_row(item: dict[str, Any], window: int) -> dict[str, Any]:
    signal = dict((item.get("signals") or {}).get(str(window)) or {})
    return {
        "code": str(item.get("code") or ""),
        "name": str(item.get("name") or ""),
        "level": str(item.get("level") or ""),
        "stage": str(item.get("stage") or ""),
        "stage_label": str(item.get("stage_label") or ""),
        "eligible_count": int(item.get("eligible_count") or 0),
        "member_count": int(item.get("member_count") or 0),
        "strong_ratio": item.get("strong_ratio"),
        "positive_ratio": item.get("positive_ratio"),
        "weak_ratio": item.get("weak_ratio"),
        "primary_industry": item.get("primary_industry"),
        "signal": signal,
    }


def _rank_window(items: list[dict[str, Any]], window: int) -> dict[str, Any]:
    available = [
        item for item in items
        if (item.get("signals") or {}).get(str(window), {}).get("rotation_change_pp")
        is not None
    ]

    def key(item: dict[str, Any]) -> tuple[Any, ...]:
        signal = (item.get("signals") or {}).get(str(window)) or {}
        return (
            float(signal.get("rotation_change_pp") or 0.0),
            float(signal.get("excess_return") or 0.0),
            int(item.get("eligible_count") or 0),
            str(item.get("name") or ""),
        )

    ranked = sorted(available, key=key, reverse=True)
    return {
        "available": len(ranked),
        "improving_count": sum(
            float((item.get("signals") or {}).get(str(window), {}).get("rotation_change_pp") or 0) > 0
            for item in ranked
        ),
        "retreating_count": sum(
            float((item.get("signals") or {}).get(str(window), {}).get("rotation_change_pp") or 0) < 0
            for item in ranked
        ),
        "leaders": [_signal_row(item, window) for item in ranked[:10]],
        "laggards": [_signal_row(item, window) for item in reversed(ranked[-10:])],
    }


def _resonance_rows(
    industries: list[dict[str, Any]],
    themes: list[dict[str, Any]],
    window: int,
) -> list[dict[str, Any]]:
    themes_by_industry: dict[str, list[dict[str, Any]]] = {}
    for theme in themes:
        primary = theme.get("primary_industry") or {}
        code = str(primary.get("code") or "")
        if code:
            themes_by_industry.setdefault(code, []).append(theme)
    rows: list[dict[str, Any]] = []
    for industry in industries:
        code = str(industry.get("code") or "")
        industry_signal = (industry.get("signals") or {}).get(str(window)) or {}
        industry_change = industry_signal.get("rotation_change_pp")
        linked: list[dict[str, Any]] = []
        for theme in themes_by_industry.get(code, []):
            theme_signal = (theme.get("signals") or {}).get(str(window)) or {}
            change = theme_signal.get("rotation_change_pp")
            if change is not None:
                linked.append({
                    "code": str(theme.get("code") or ""),
                    "name": str(theme.get("name") or ""),
                    "rotation_change_pp": float(change),
                })
        changes: list[float] = [float(item["rotation_change_pp"]) for item in linked]
        theme_median = round(float(median(changes)), 2) if changes else None
        if industry_change is None or len(linked) < 2:
            status = "insufficient"
        elif float(industry_change) > 0 and float(theme_median or 0) > 0:
            status = "improving"
        elif float(industry_change) < 0 and float(theme_median or 0) < 0:
            status = "retreating"
        else:
            status = "diverging"
        linked.sort(key=lambda item: (-abs(item["rotation_change_pp"]), item["name"]))
        rows.append({
            "code": code,
            "name": str(industry.get("name") or code),
            "status": status,
            "industry_change_pp": industry_change,
            "industry_excess_return": industry_signal.get("excess_return"),
            "linked_theme_count": len(linked),
            "improving_theme_count": sum(item["rotation_change_pp"] > 0 for item in linked),
            "retreating_theme_count": sum(item["rotation_change_pp"] < 0 for item in linked),
            "theme_median_change_pp": theme_median,
            "themes": linked[:3],
        })
    order = {"improving": 0, "diverging": 1, "retreating": 2, "insufficient": 3}
    rows.sort(key=lambda item: (
        order[item["status"]],
        -float(item.get("industry_change_pp") or 0.0),
        -int(item.get("linked_theme_count") or 0),
        item["name"],
    ))
    return rows


class RotationService:
    def __init__(
        self,
        store: RotationStore | None = None,
        jobs: RotationJobStore | None = None,
    ):
        self.store = store or RotationStore()
        self.jobs = jobs or RotationJobStore()
        self.loader = RotationDataLoader(self.store)
        self._overview_cache_key: tuple[str, ...] = ()
        self._overview_cache: dict[str, Any] | None = None

    @staticmethod
    def _meta(
        *,
        snapshot_id: str,
        as_of: str,
        generated_at: str,
        quality: dict[str, Any],
        sources: list[str],
        expected_as_of: str = "",
    ) -> dict[str, Any]:
        theme_taxonomy = next((
            source for source in sources
            if source in {"eastmoney-concept", "tushare:dc-concept", "ths:concept"}
        ), "eastmoney-concept")
        return {
            "snapshot_id": snapshot_id,
            "as_of": as_of,
            "actual_as_of": as_of,
            "expected_as_of": expected_as_of,
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
        expected_as_of: str = "",
    ) -> dict[str, Any]:
        return {
            "meta": self._meta(
                snapshot_id=snapshot_id,
                as_of=str(data.get("as_of") or ""),
                generated_at=generated_at,
                quality=quality,
                sources=sources,
                expected_as_of=expected_as_of,
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
        previous_snapshot_ids: dict[str, str] = {}
        for kind in ("temperature", "structure", "industries", "themes", "etf_flows", "taxonomy"):
            try:
                previous = self.store.snapshot(kind)
            except RotationIntegrityError:
                previous = None
            previous_snapshot_ids[kind] = str((previous or {}).get("meta", {}).get("snapshot_id") or "")
        need_market = scope in {"all", "close", "market", "industries", "themes"}
        provider_warnings: list[str] = []
        provider_issues: dict[str, list[str]] = {
            "market": [], "industries": [], "themes": [], "etf": [],
        }
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
                    issues = [
                        str(issue) for issue in provider_results[key].get("issues") or []
                    ]
                    provider_issues[key].extend(issues)
                    provider_warnings.extend(issues)
                except InterruptedError:
                    raise
                except Exception as exc:  # 外部数据源边界：记录后降级到已有快照
                    logger.warning("%s 同步失败，板块联动将使用本地覆盖", label, exc_info=True)
                    warning = f"{label}同步失败：{str(exc)[:160]}"
                    provider_issues[key].append(warning)
                    provider_warnings.append(warning)
        etf_observations = pd.DataFrame()
        if scope in {"all", "close", "market", "etf"}:
            try:
                etf_observations = self.store.etf_observations()
            except RotationIntegrityError:
                if scope in {"all", "etf"}:
                    raise
                logger.warning("市场温度读取 ETF 观察文件失败", exc_info=True)
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
        expected_as_of = str(
            provider_results.get("market", {}).get("expected_as_of")
            or _expected_market_session()
        ) if need_market else ""
        snapshot_id = _snapshot_id(as_of, list(close.columns), scope)
        compute_base = 70 if spec.source == "auto" else 34
        trend = compute_trend_matrices(close) if need_market else None

        if scope in {"all", "close", "market"}:
            progress(compute_base, "计算市场温度", "汇总四档趋势分布与证据权重")
            assert trend is not None
            temperature_dates = market_temperature_reference_dates(trend)
            temperature_as_of = temperature_dates.get(0, as_of)
            etf_evidence = compute_etf_capital_evidence(
                etf_observations,
                as_of=temperature_as_of,
            )
            sentiment_evidence = _news_sentiment_evidence(temperature_as_of)
            historical_evidence: dict[str, dict[str, dict[str, Any]]] = {}
            for window in ROTATION_WINDOWS:
                reference_as_of = temperature_dates.get(window)
                if not reference_as_of or reference_as_of in historical_evidence:
                    continue
                historical_evidence[reference_as_of] = {
                    "etf_capital": compute_etf_capital_evidence(
                        etf_observations,
                        as_of=reference_as_of,
                    ),
                    "sentiment": _news_sentiment_evidence(reference_as_of),
                }
            temperature = compute_market_temperature(
                close,
                amount,
                expected_count=expected_count,
                trend=trend,
                supplemental_evidence={
                    "etf_capital": etf_evidence,
                    "sentiment": sentiment_evidence,
                },
                supplemental_evidence_history=historical_evidence,
            )
            temperature_quality = temperature.pop("quality")
            temperature_quality["issues"] = list(dict.fromkeys([
                *(temperature_quality.get("issues") or []), *provider_issues["market"],
            ]))
            temperature_quality = _mark_stale(
                temperature_quality, str(temperature.get("as_of") or ""), expected_as_of,
            )
            temperature_sources = list(sources)
            all_etf_evidence = [
                etf_evidence,
                *(value["etf_capital"] for value in historical_evidence.values()),
            ]
            all_sentiment_evidence = [
                sentiment_evidence,
                *(value["sentiment"] for value in historical_evidence.values()),
            ]
            if any(value.get("available") for value in all_etf_evidence):
                temperature_sources.extend(["tushare:fund_share", "local:rotation_cache"])
                if "nav" in etf_observations and etf_observations["nav"].notna().any():
                    temperature_sources.append("tushare:fund_nav")
                if "close" in etf_observations and etf_observations["close"].notna().any():
                    temperature_sources.append("tushare:fund_daily")
            if any(value.get("available") for value in all_sentiment_evidence):
                temperature_sources.append("local:news")
            computed["temperature"] = self._envelope(
                temperature,
                snapshot_id=snapshot_id,
                generated_at=generated_at,
                quality=temperature_quality,
                sources=list(dict.fromkeys(temperature_sources)),
                expected_as_of=expected_as_of,
            )
            progress(compute_base + 7, "计算市场风格", "比较强势与低位样本收益分布")
            structure = compute_market_structure(close, names=names, trend=trend)
            computed["structure"] = self._envelope(
                structure,
                snapshot_id=snapshot_id,
                generated_at=generated_at,
                quality=temperature_quality,
                sources=sources,
                expected_as_of=expected_as_of,
            )

        l1_groups: dict[str, dict[str, Any]] = {}
        l2_groups: dict[str, dict[str, Any]] = {}
        if scope in {"all", "close", "industries"}:
            progress(compute_base + 12, "聚合申万行业", "严格过滤申万 2021 层级")
            l1_groups = _load_l1_groups(self.store, expected_count)
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
                    *provider_issues["market"],
                    *provider_issues["industries"],
                ],
            )
            industry_quality = _mark_stale(industry_quality, as_of, expected_as_of)
            computed["industries"] = self._envelope(
                industries,
                snapshot_id=snapshot_id,
                generated_at=generated_at,
                quality=industry_quality,
                sources=[*sources, "SW2021"],
                expected_as_of=expected_as_of,
            )
            taxonomy = taxonomy_payload(l1_groups, l2_groups)
            taxonomy["as_of"] = industries["as_of"]
            computed["taxonomy"] = self._envelope(
                taxonomy,
                snapshot_id=snapshot_id,
                generated_at=generated_at,
                quality=industry_quality,
                sources=["SW2021"],
                expected_as_of=expected_as_of,
            )

        if scope in {"all", "close", "themes"}:
            progress(compute_base + 17, "扫描细分题材", "合并高度重叠的概念板块")
            stored_themes = self.store.themes()
            if spec.source == "auto" and "themes" not in provider_results and not stored_themes:
                raise RuntimeError(
                    next(
                        (warning for warning in provider_warnings if "细分题材目录" in warning),
                        "细分题材三套数据源均不可用，未生成空快照",
                    )
                )
            themes = _deduplicate_themes(stored_themes)
            theme_sources = list(dict.fromkeys(
                str(item.get("source") or "") for item in stored_themes
                if str(item.get("source") or "")
            ))
            theme_provider_issues = list(
                provider_results.get("themes", {}).get("issues") or []
            )
            if themes:
                if not l1_groups:
                    l1_groups = _load_l1_groups(self.store, expected_count)
                theme_data = analyze_group_rotation(
                    close, themes, names=names, amount=amount, kind="theme", trend=trend,
                )
                industry_links = map_theme_industries(themes, l1_groups)
                for item in theme_data["items"]:
                    item.update(industry_links.get(str(item.get("code")), {}))
                for code, item in theme_data["details"].items():
                    item.update(industry_links.get(str(code), {}))
                theme_data["definition"]["industry_mapping"] = (
                    "申万一级行业真实成分交集；主行业至少 3 只且占已映射成员 25%"
                )
                count = len(theme_data["items"])
                provider_quality = str(
                    provider_results.get("themes", {}).get("quality_status") or "complete"
                )
                catalog_expected = int(
                    provider_results.get("themes", {}).get("catalog") or len(themes)
                )
                quality_issues = list(dict.fromkeys([
                    *([] if count >= 50 else ["概念成分目录仍在积累"]),
                    *theme_provider_issues,
                    *provider_issues["market"],
                    *provider_issues["themes"],
                ]))
                theme_quality = _status_quality(
                    "complete" if count >= 50 and provider_quality == "complete" else "partial",
                    eligible=count,
                    expected=catalog_expected,
                    issues=quality_issues,
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
                        "positive_states": ["strong_up", "up"],
                        "score": {
                            "weights": dict(GROUP_SCORE_WEIGHTS),
                            "minimum_available_weight": MIN_GROUP_SCORE_WEIGHT,
                            "disclaimer": "结构状态评分，不构成交易评级",
                        },
                    },
                }
                theme_quality = _status_quality(
                    "cold",
                    issues=[
                        "尚未建立细分题材成分目录",
                        *provider_issues["market"],
                        *provider_issues["themes"],
                    ],
                )
            theme_quality = _mark_stale(theme_quality, as_of, expected_as_of)
            computed["themes"] = self._envelope(
                theme_data,
                snapshot_id=snapshot_id,
                generated_at=generated_at,
                quality=theme_quality,
                sources=list(dict.fromkeys([*sources, *theme_sources])),
                expected_as_of=expected_as_of,
            )

        if scope in {"all", "etf"}:
            progress(compute_base + 21, "估算宽基资金", "按份额变化与净值计算申赎资金")
            etf_data = estimate_etf_flows(etf_observations)
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
                    *provider_issues["etf"],
                ],
            )
            etf_snapshot = _snapshot_id(
                etf_data.get("as_of") or as_of,
                [str(item.get("symbol") or "") for item in etf_data["items"]],
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
        content_digests = {
            kind: hashlib.sha256(
                strict_json_dumps(payload.get("data") or {}, sort_keys=True).encode("utf-8")
            ).hexdigest()
            for kind, payload in sorted(computed.items())
        }
        batch_id = _snapshot_id(
            as_of,
            [f"{kind}:{digest}" for kind, digest in sorted(content_digests.items())],
            "batch",
        )
        for kind, payload in computed.items():
            payload["meta"]["snapshot_id"] = _snapshot_id(
                str(payload["meta"].get("as_of") or ""),
                [content_digests[kind]],
                kind,
            )
            payload["meta"]["batch_id"] = batch_id
        progress(96, "提交分析快照", "原子更新页面所需视图")
        self.store.save_snapshots(computed)
        changed = sorted(
            kind for kind, payload in computed.items()
            if str(payload.get("meta", {}).get("snapshot_id") or "")
            != previous_snapshot_ids.get(kind, "")
        )
        non_complete = [
            kind for kind, payload in computed.items()
            if str(payload.get("meta", {}).get("quality", {}).get("status") or "")
            not in {"complete"}
        ]
        outcome = "unchanged" if not changed else (
            "partial" if provider_warnings or non_complete else "updated"
        )
        snapshot_id = _snapshot_id(
            as_of,
            [str(computed[kind]["meta"]["snapshot_id"]) for kind in sorted(computed)],
            scope,
        )
        return {
            "snapshot_id": snapshot_id,
            "as_of": as_of,
            "expected_as_of": expected_as_of,
            "fresh": not expected_as_of or as_of >= expected_as_of,
            "outcome": outcome,
            "updated": changed,
            "computed": sorted(computed),
            "warnings": list(dict.fromkeys(provider_warnings)),
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
            meta = value.setdefault("meta", {})
            quality = value.get("meta", {}).get("quality", {})
            stored_algorithm = str(meta.get("algorithm_version") or "")
            if (
                stored_algorithm != ALGORITHM_VERSION
                and str(quality.get("status") or "") not in {"cold", "corrupt", "empty"}
            ):
                quality = dict(quality)
                quality["status"] = "stale"
                quality["issues"] = list(dict.fromkeys([
                    (
                        f"快照缺少算法版本，正在升级到 {ALGORITHM_VERSION}"
                        if not stored_algorithm
                        else f"快照算法为 {stored_algorithm}，正在升级到 {ALGORITHM_VERSION}"
                    ),
                    *(quality.get("issues") or []),
                ]))
                quality["upgrade_pending"] = True
                meta["quality"] = quality
            if kind == "themes" and str(quality.get("status") or "") == "cold":
                active = next((
                    job for job in self.jobs.list(50)
                    if str(job.get("status") or "") in {"queued", "running", "cancelling"}
                    and str((job.get("spec") or {}).get("scope") or "")
                    in {"all", "close", "themes"}
                ), None)
                if active is not None:
                    quality["status"] = "loading"
                    quality["job_id"] = str(active.get("id") or "")
                    quality["progress"] = max(0, min(100, int(active.get("progress") or 0)))
                    quality["issues"] = ["细分题材目录正在后台构建"]
            if kind in {"temperature", "structure", "industries", "themes"}:
                expected_as_of = _expected_market_session()
                as_of = str(meta.get("as_of") or value.get("data", {}).get("as_of") or "")
                meta["expected_as_of"] = expected_as_of
                meta["actual_as_of"] = as_of
                quality = _mark_stale(quality, as_of, expected_as_of)
                meta["quality"] = quality
            expected = quality.get("expected_count")
            if expected is None or expected == 0:
                # v0.13.0 snapshots serialized an unknown denominator as 0%.  Keep
                # them readable while correcting the public meaning immediately.
                quality["coverage"] = None
            return value
        except RotationIntegrityError:
            logger.error("板块联动快照完整性失败 kind=%s", kind, exc_info=True)
            value = self.cold(kind)
            value["meta"]["quality"] = _status_quality(
                "corrupt", issues=[
                    "板块联动快照完整性校验失败",
                    "请重新生成联动快照；损坏内容不会参与计算",
                ],
            )
            value["data"]["message"] = "板块联动快照损坏，已停止使用"
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
        structure = self.snapshot("structure")
        industries = self.snapshot("industries")
        themes = self.snapshot("themes")
        etf = self.snapshot("etf_flows")
        snapshots = (temperature, structure, industries, themes, etf)
        metas = [value["meta"] for value in snapshots]
        cache_key = tuple(str(meta.get("snapshot_id") or "") for meta in metas)
        if self._overview_cache is not None and cache_key == self._overview_cache_key:
            return copy.deepcopy(self._overview_cache)
        generated = max((str(meta.get("generated_at") or "") for meta in metas), default="")
        as_of = max((str(meta.get("as_of") or "") for meta in metas), default="")
        market_parts = [
            str(value["meta"].get("quality", {}).get("status") or "cold")
            for value in (temperature, structure)
        ]
        market_status = (
            "complete" if all(value == "complete" for value in market_parts)
            else "cold" if all(value == "cold" for value in market_parts)
            else "partial"
        )
        qualities = [
            market_status,
            str(industries["meta"].get("quality", {}).get("status") or "cold"),
            str(themes["meta"].get("quality", {}).get("status") or "cold"),
            str(etf["meta"].get("quality", {}).get("status") or "cold"),
        ]
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
        l1_industries = [
            item for item in industries["data"].get("items", [])
            if str(item.get("level")) == "L1"
        ]
        theme_items = list(themes["data"].get("items", []))
        rankings = {
            str(window): {
                "industries": _rank_window(l1_industries, window),
                "themes": _rank_window(theme_items, window),
            }
            for window in ROTATION_WINDOWS
        }
        resonance = {
            str(window): _resonance_rows(l1_industries, theme_items, window)
            for window in ROTATION_WINDOWS
        }
        temperature_history = list(temperature["data"].get("history") or [])
        temperature_changes = {}
        for window in ROTATION_WINDOWS:
            if len(temperature_history) > window:
                latest = temperature_history[-1].get("temperature")
                previous = temperature_history[-1 - window].get("temperature")
                temperature_changes[str(window)] = (
                    round(float(latest) - float(previous), 2)
                    if latest is not None and previous is not None else None
                )
            else:
                temperature_changes[str(window)] = None

        def dimension_meta(value: dict[str, Any]) -> dict[str, Any]:
            meta = value.get("meta") or {}
            quality = meta.get("quality") or {}
            return {
                "as_of": str(meta.get("as_of") or ""),
                "status": str(quality.get("status") or "cold"),
                "eligible_count": quality.get("eligible_count"),
                "expected_count": quality.get("expected_count"),
                "coverage": quality.get("coverage"),
                "sources": list(meta.get("sources") or []),
                "issues": list(quality.get("issues") or []),
            }

        market_dimension = dimension_meta(temperature)
        market_dimension.update({
            "status": market_status,
            "structure_status": market_parts[1],
            "issues": list(dict.fromkeys([
                *market_dimension["issues"],
                *(structure.get("meta", {}).get("quality", {}).get("issues") or []),
            ])),
        })

        data = {
            "as_of": as_of,
            "temperature": temperature["data"].get("current"),
            "industries": visible_industries[:8],
            "themes": theme_items[:8],
            "etf": etf["data"].get("summary", {}),
            "windows": list(ROTATION_WINDOWS),
            "dimensions": {
                "market": market_dimension,
                "industries": dimension_meta(industries),
                "themes": dimension_meta(themes),
                "etf": dimension_meta(etf),
            },
            "market": {
                "temperature": temperature["data"].get("current"),
                "temperature_changes": temperature_changes,
                "structure": structure["data"].get("current"),
            },
            "distributions": {
                "industries": industries["data"].get("summary", {}),
                "themes": themes["data"].get("summary", {}),
            },
            "rankings": rankings,
            "resonance": resonance,
            "etf_context": {
                "summary": etf["data"].get("summary", {}),
                "benchmarks": etf["data"].get("benchmarks", []),
            },
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
        result = {
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
        self._overview_cache_key = cache_key
        self._overview_cache = copy.deepcopy(result)
        return result

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
        needs_algorithm_upgrade = False
        needs_theme_catalog = False
        if bootstrap_local:
            try:
                temperature = self.service.store.snapshot("temperature")
                needs_local_snapshot = temperature is None
                snapshots = [
                    self.service.store.snapshot(kind)
                    for kind in (
                        "temperature", "structure", "industries", "themes", "etf_flows",
                    )
                ]
                needs_algorithm_upgrade = any(
                    value is not None
                    and str(value.get("meta", {}).get("algorithm_version") or "")
                    != ALGORITHM_VERSION
                    and str(value.get("meta", {}).get("quality", {}).get("status") or "")
                    not in {"cold", "corrupt", "empty"}
                    for value in snapshots
                )
            except RotationIntegrityError:
                needs_local_snapshot = True
                needs_algorithm_upgrade = True
            now = datetime.now(ZoneInfo("Asia/Shanghai"))
            if (
                not needs_algorithm_upgrade
                and now.weekday() < 5 and (now.hour, now.minute) >= (18, 30)
            ):
                needs_local_snapshot = False
            try:
                needs_theme_catalog = not bool(self.service.store.themes())
            except (OSError, sqlite3.Error):
                needs_theme_catalog = True
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="quantmaster-rotation", daemon=True,
        )
        self._thread.start()
        if needs_algorithm_upgrade:
            self.submit(RotationJobSpec(scope="all", source="local"))
        elif needs_local_snapshot:
            self.submit(RotationJobSpec(scope="close", source="local"))
        if needs_theme_catalog:
            # Keep network bootstrap off the startup thread. create() also reuses
            # the same active logical job after a process or browser restart.
            self.submit(RotationJobSpec(scope="themes", source="auto"))

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

    def _scheduled_retry_due(self, kind: str, date_key: str) -> bool:
        value = self.service.store.runtime_state(f"scheduled_{kind}_retry")
        if not value:
            return True
        try:
            saved_date, attempt_text, next_text = value.split("|", 2)
            if saved_date != date_key:
                return True
            return int(attempt_text) < 3 and time.time() >= float(next_text)
        except (TypeError, ValueError):
            return True

    def _record_scheduled_result(
        self,
        spec: RotationJobSpec,
        *,
        succeeded: bool,
    ) -> None:
        if spec.source != "auto" or spec.scope not in {"close", "etf"}:
            return
        kind = "close" if spec.scope == "close" else "etf"
        date_key = str(datetime.now(ZoneInfo("Asia/Shanghai")).date())
        retry_key = f"scheduled_{kind}_retry"
        if succeeded:
            self.service.store.set_runtime_state(f"scheduled_{kind}", date_key)
            self.service.store.set_runtime_state(retry_key, "")
            return
        value = self.service.store.runtime_state(retry_key)
        attempt = 0
        if value:
            try:
                saved_date, attempt_text, _next_text = value.split("|", 2)
                if saved_date == date_key:
                    attempt = int(attempt_text)
            except (TypeError, ValueError):
                attempt = 0
        attempt += 1
        delays = (15 * 60, 45 * 60, 120 * 60)
        next_at = time.time() + delays[min(attempt - 1, len(delays) - 1)]
        self.service.store.set_runtime_state(
            retry_key, f"{date_key}|{attempt}|{next_at}",
        )

    def _scheduled(self) -> None:
        now = datetime.now(ZoneInfo("Asia/Shanghai"))
        if now.weekday() >= 5:
            return
        date_key = str(now.date())
        close_due = (now.hour, now.minute) >= (18, 30)
        etf_due = (now.hour, now.minute) >= (9, 5)
        if (
            close_due
            and self.service.store.runtime_state("scheduled_close") != date_key
            and self._scheduled_retry_due("close", date_key)
        ):
            self.submit(RotationJobSpec(scope="close", source="auto"))
        if (
            etf_due
            and self.service.store.runtime_state("scheduled_etf") != date_key
            and self._scheduled_retry_due("etf", date_key)
        ):
            self.submit(RotationJobSpec(scope="etf", source="auto"))

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
            if self.service.jobs.is_cancel_requested(job_id, owner):
                self.service.jobs.fail(job_id, owner, "任务已安全取消", cancelled=True)
            elif self._stop.is_set():
                self.service.jobs.release_for_handoff(job_id, owner)
                logger.info("板块联动 worker 停机，任务租约已释放 job_id=%s", job_id)
            else:
                self.service.jobs.complete(job_id, owner, result)
                schedule_succeeded = bool(
                    not result.get("warnings")
                    and result.get("fresh", True)
                    and (
                        spec.scope != "etf"
                        or result.get("outcome") in {"updated", "unchanged"}
                    )
                )
                self._record_scheduled_result(spec, succeeded=schedule_succeeded)
        except InterruptedError as exc:
            if (
                self._stop.is_set()
                and not self.service.jobs.is_cancel_requested(job_id, owner)
            ):
                self.service.jobs.release_for_handoff(job_id, owner)
                logger.info("板块联动 worker 停机，任务等待新进程接管 job_id=%s", job_id)
            else:
                self.service.jobs.fail(job_id, owner, str(exc), cancelled=True)
                if "spec" in locals():
                    self._record_scheduled_result(spec, succeeded=False)
        except Exception as exc:
            logger.exception("板块联动刷新失败 job_id=%s", job_id)
            self.service.jobs.fail(job_id, owner, str(exc))
            if "spec" in locals():
                self._record_scheduled_result(spec, succeeded=False)

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
