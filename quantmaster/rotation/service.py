"""Rotation snapshot builder and lease-based background worker."""

from __future__ import annotations

import copy
import hashlib
import logging
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from statistics import median
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.industry import load_cached_industry_map  # noqa: F401
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
from quantmaster.rotation.board_indexes import (
    BOARD_INDEX_ALGORITHM_VERSION,
    build_board_index_data,
)
from quantmaster.rotation.contracts import RotationJobSpec
from quantmaster.rotation.session import expected_market_session
from quantmaster.rotation.status import (
    data_status_payload,
    provider_status_payload,
    taxonomy_identity,
)
from quantmaster.rotation.store import (
    RotationIntegrityError,
    RotationStore,
)
from quantmaster.rotation.taxonomy import (
    SW2021_L1,
    merge_l2_groups,
    strict_l1_groups,
    taxonomy_payload,
)
from quantmaster.runtime.jobs import (
    JobContext,
    JobOutcome,
    UnifiedJobRuntime,
    UnifiedJobStore,
)
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.metrics import get_runtime_metrics
from quantmaster.stockdb_acceptance import read_stockdb_session_acceptance

logger = logging.getLogger(__name__)
Progress = Callable[[int, str, str], None]
Cancelled = Callable[[], bool]


def _ensure_rotation_provider_registered() -> None:
    """Load the bundled provider before a fresh compute child uses the seam."""

    from quantmaster.rotation.provider import _rotation_provider_factory
    from quantmaster.rotation.provider_access import register_rotation_provider

    register_rotation_provider(_rotation_provider_factory)


def _provider_health_for_sources(sources: list[str]) -> dict[str, dict[str, Any]]:
    """Return only lanes relevant to the snapshot; failures remain non-blocking here."""

    from quantmaster.data.resilience import PROVIDER_HEALTH

    families = {
        str(source).partition(":")[0].casefold()
        for source in sources if str(source).partition(":")[0]
    }
    aliases = {"eastmoney-concept": "akshare", "ths": "ths"}
    families.update(aliases.get(source, "") for source in sources)
    families.discard("")
    relevant_capabilities = {
        "eastmoney-concept", "dc-concept", "concept", "index-classify",
        "index-member", "industry",
    }
    return {
        lane: value for lane, value in PROVIDER_HEALTH.status().items()
        if lane.partition(":")[0].casefold() in families
        or lane.partition(":")[2].casefold() in relevant_capabilities
    }


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


def _knowledge_cutoff_epoch(value: str) -> float:
    try:
        cutoff = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("knowledge_cutoff 必须是 ISO-8601 时间") from exc
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    return cutoff.timestamp()


def _validate_taxonomy_evidence(
    item: dict[str, Any],
    *,
    declared: set[str],
    cutoff_epoch: float,
    as_of: str,
) -> None:
    taxonomy_id = str(item.get("taxonomy_id") or "")
    semantics = str(item.get("membership_semantics") or "")
    observed_at = float(item.get("observed_at_epoch") or 0)
    if taxonomy_id not in declared:
        raise ValueError(f"历史用途拒绝未声明 taxonomy：{taxonomy_id or 'unresolved'}")
    if semantics not in {"historical_intervals", "dated_snapshot"}:
        raise ValueError(f"历史用途拒绝 current-only taxonomy：{taxonomy_id}")
    if not observed_at or observed_at > cutoff_epoch:
        raise ValueError(f"taxonomy observation 晚于 knowledge_cutoff：{taxonomy_id}")
    effective_date = str(item.get("effective_date") or "")[:10]
    if semantics == "dated_snapshot" and effective_date != as_of:
        raise ValueError(
            f"dated taxonomy 不适用于 {as_of}：{taxonomy_id} @ {effective_date or 'unknown'}"
        )
    if semantics == "historical_intervals" and not item.get("membership_records"):
        raise ValueError(f"taxonomy 缺少成员有效期：{taxonomy_id}")


def _normalized_etf_observations(
    observations: pd.DataFrame,
    as_of: str,
) -> tuple[pd.DataFrame, pd.Timestamp | None]:
    required = {"trade_date", "symbol", "shares"}
    if observations is None or observations.empty or not required.issubset(observations.columns):
        return observations, None
    value = observations.copy()
    if "close" not in value:
        value["close"] = pd.NA
    value["trade_date"] = pd.to_datetime(
        value["trade_date"], errors="coerce",
    ).dt.normalize()
    value["symbol"] = value["symbol"].astype(str).str.upper()
    target = pd.to_datetime(as_of, errors="coerce")
    if pd.isna(target):
        target = value["trade_date"].max()
    return value, None if pd.isna(target) else pd.Timestamp(target).normalize()


def _stockdb_etf_daily(target: pd.Timestamp) -> pd.DataFrame | None:
    from quantmaster.data.free_stockdb_ingest import StockDBIngestStore

    ingest_store = StockDBIngestStore()
    candidates: list[tuple[pd.Timestamp, Any]] = []
    for snapshot in ingest_store.history(100):
        start = pd.to_datetime(snapshot.start_date, errors="coerce")
        end = pd.to_datetime(snapshot.end_date or snapshot.as_of_date, errors="coerce")
        eligible = (
            snapshot.status in {"complete", "degraded"}
            and "etf" in snapshot.assets
            and "etf_daily" in snapshot.content_hashes
            and pd.notna(end)
            and end >= target
            and (pd.isna(start) or start <= target)
        )
        if eligible:
            candidates.append((pd.Timestamp(end), snapshot))
    if not candidates:
        return None
    _, snapshot = max(candidates, key=lambda item: item[0])
    return ingest_store.load_frame(snapshot, "etf_daily")


def _etf_price_lookup(
    daily: pd.DataFrame | None,
    target: pd.Timestamp,
) -> pd.Series | None:
    required = {"symbol", "date", "close"}
    if daily is None or daily.empty or not required.issubset(daily.columns):
        return None
    prices = daily.loc[:, ["symbol", "date", "close"]].copy()
    prices["trade_date"] = pd.to_datetime(
        prices.pop("date"), errors="coerce",
    ).dt.normalize()
    prices["symbol"] = prices["symbol"].astype(str).str.upper()
    prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
    prices = prices[
        prices["trade_date"].notna()
        & prices["trade_date"].le(target)
        & prices["close"].gt(0)
    ].drop_duplicates(["trade_date", "symbol"], keep="last")
    if prices.empty:
        return None
    return prices.set_index(["trade_date", "symbol"])["close"]


def _overlay_stockdb_etf_prices(
    observations: pd.DataFrame,
    *,
    as_of: str = "",
) -> tuple[pd.DataFrame, str]:
    """Fill missing share-observation prices from the canonical StockDB ETF panel.

    ``RotationStore.etf_observations`` is the legacy share/metadata cache.  The
    validated StockDB ingest panel is the canonical daily-price evidence used by
    ETF research, so the market-temperature calculation must not treat a share
    row without a price as a missing trading day.  The overlay is read-only and
    only fills missing ``close`` values; it never rewrites the rotation cache.
    """
    value, target = _normalized_etf_observations(observations, as_of)
    if target is None:
        return value, ""
    try:
        daily = _stockdb_etf_daily(target)
    except (ImportError, OSError, RuntimeError, TypeError, ValueError):
        logger.debug("读取 StockDB ETF 日线价格叠加失败", exc_info=True)
        return value, ""
    lookup = _etf_price_lookup(daily, target)
    if lookup is None:
        return value, ""
    keys = pd.MultiIndex.from_arrays(
        [value["trade_date"], value["symbol"]],
        names=["trade_date", "symbol"],
    )
    overlay = pd.Series(lookup.reindex(keys).to_numpy(), index=value.index)
    existing = pd.to_numeric(value["close"], errors="coerce")
    fill = existing.isna() & overlay.notna()
    if not fill.any():
        return value, ""
    value.loc[fill, "close"] = overlay.loc[fill]
    return value, "local:stockdb:etf_daily"


def _expected_etf_funds(store: RotationStore) -> int | None:
    """Return the local ETF-directory denominator for evidence publication."""
    try:
        metadata = store.etf_metadata()
    except (RotationIntegrityError, OSError, RuntimeError, TypeError, ValueError):
        return None
    if metadata is None or metadata.empty or "symbol" not in metadata.columns:
        return None
    symbols = metadata["symbol"].astype(str).str.upper().str.strip()
    count = int(symbols[symbols.ne("")].nunique())
    return count or None


def _news_sentiment_evidence(
    as_of: str, *, minimum_events: int = 20, knowledge_as_of: float | None = None,
) -> dict[str, Any]:
    fallback = {
        "available": False,
        "score": None,
        "as_of": str(as_of or ""),
        "note": "等待可核查资讯情绪",
        "event_count": 0,
        "signed_score": None,
        "lookback_days": 30,
        "knowledge_as_of_epoch": None,
    }
    if not as_of:
        return fallback
    try:
        from quantmaster.ai.crawler import NewsStore

        snapshot = NewsStore().market_sentiment(
            as_of=_market_close_epoch(as_of),
            days=30,
            knowledge_as_of=knowledge_as_of,
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
            "knowledge_as_of_epoch": snapshot.get("knowledge_as_of_epoch"),
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
        "knowledge_as_of_epoch": snapshot.get("knowledge_as_of_epoch"),
    }


def _snapshot_id(as_of: str, columns: list[str], scope: str) -> str:
    logical = strict_json_dumps({
        "algorithm": ALGORITHM_VERSION,
        "as_of": as_of,
        "columns": sorted(columns),
        "scope": scope,
    }, sort_keys=True)
    return hashlib.sha256(logical.encode("utf-8")).hexdigest()[:20]


_expected_market_session = expected_market_session


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

    @staticmethod
    def _listed_symbols(instruments: InstrumentStore) -> tuple[list[str], dict[str, str]]:
        """Return the listed CN stock universe with display names."""
        with instruments._connection() as connection:
            rows = connection.execute(
                "SELECT symbol,name FROM instruments WHERE market='CN' "
                "AND asset_type='stock' AND lower(status) IN ('l','listed') "
                "ORDER BY symbol"
            ).fetchall()
        symbols: list[str] = []
        names: dict[str, str] = {}
        for row in rows:
            symbol = str(row["symbol"] or "").upper().strip()
            if not symbol:
                continue
            symbols.append(symbol)
            names[symbol] = str(row["name"] or symbol).strip() or symbol
        return symbols, names

    @staticmethod
    def _validated_stockdb_session() -> tuple[str, dict[str, Any] | None]:
        """Describe the latest accepted local StockDB session as a generation.

        An accepted v2 session is formal local evidence; ``complete`` remains
        coverage telemetry and is not a second admission gate.
        """

        acceptance = read_stockdb_session_acceptance(get_config().free_stockdb_root)
        if acceptance is None:
            return "", None
        validation = acceptance.validation
        session = acceptance.session
        stable = {
            key: validation.get(key)
            for key in (
                "target_session",
                "actual_session",
                "accepted",
                "complete",
                "observed_symbols",
                "expected_symbols",
                "symbol_ratio",
                "required_ohlcv_ratio",
            )
        }
        stable["updated_at"] = acceptance.updated_at.isoformat()
        content_id = hashlib.sha256(
            strict_json_dumps(stable, sort_keys=True).encode("utf-8")
        ).hexdigest()
        return session, {
            "source": "stockdb.validated_session",
            "partition_key": session,
            "content_id": content_id,
            "coverage_start": session,
            "coverage_end": session,
            "formal_eligible": True,
            "complete": acceptance.complete,
            "observed_at": acceptance.updated_at.isoformat(),
        }

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

    def local_input_state(self) -> dict[str, Any]:
        """Read only local catalogs to describe the currently usable panel.

        This method is deliberately forbidden from opening Parquet bars.  It is
        used before submitting a refresh to decide whether a published snapshot
        is already valid and whether an ``auto`` request actually needs a
        remote supplement.  Accepted v2 StockDB sessions are formal local
        generations even when absolute catalog coverage is below 100%.
        """

        instruments, expected_count = self._listed_instruments()
        instrument_identity = ""
        try:
            with instruments._connection() as connection:
                row = connection.execute(
                    "SELECT COUNT(*) AS count,COALESCE(MAX(observed_at),0) AS latest "
                    "FROM instruments WHERE market='CN' AND asset_type='stock' "
                    "AND lower(status) IN ('l','listed')"
                ).fetchone()
            instrument_identity = hashlib.sha256(strict_json_dumps({
                "count": int(row["count"] if row else 0),
                "latest": float(row["latest"] if row else 0),
            }, sort_keys=True).encode("utf-8")).hexdigest()
        except (OSError, sqlite3.Error, TypeError, ValueError):
            instrument_identity = "unavailable"

        lake_entries: list[dict[str, Any]] = []
        lake_as_of = ""
        if (get_config().data_root / "research_lake").is_dir():
            try:
                from quantmaster.research.contracts import ArtifactKind, AssetClass, Frequency
                from quantmaster.research.lake import ResearchLake

                partitions = ResearchLake().catalog.partitions(
                    kind=ArtifactKind.RAW,
                    asset_class=AssetClass.STOCK,
                    frequency=Frequency.DAILY,
                    dataset_id="stock_bars",
                )
                selected = sorted(partitions, key=lambda item: str(item.get("trade_date") or ""))[-820:]
                lake_entries = [
                    {
                        "source": "research_lake.stock_bars",
                        "partition_key": str(item.get("trade_date") or ""),
                        "content_id": str(item.get("content_sha256") or ""),
                        "coverage_start": str(item.get("trade_date") or ""),
                        "coverage_end": str(item.get("trade_date") or ""),
                    }
                    for item in selected
                    if str(item.get("trade_date") or "") and str(item.get("content_sha256") or "")
                ]
                lake_as_of = max((item["coverage_end"] for item in lake_entries), default="")
            except (ImportError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
                logger.debug("读取研究湖 generation 失败", exc_info=True)

        bars = BarStore()
        metadata = bars.metadata_many()
        candidate_symbols = [
            symbol for symbol in metadata
            if symbol.endswith((".SH", ".SZ", ".BJ"))
            and len(symbol.rsplit(".", 1)[0]) == 6
        ]
        records = instruments.get_many(candidate_symbols)
        selected_symbols = [
            symbol for symbol in candidate_symbols
            if (record := records.get(symbol)) is not None
            and record.asset_type == "stock"
            and record.market == "CN"
            and record.status.lower() in {"l", "listed"}
        ]
        if not selected_symbols:
            selected_symbols = candidate_symbols
        bar_entries = [
            {
                "source": "bar_store.stock_bars",
                "partition_key": symbol,
                "content_id": str(metadata[symbol].get("content_sha256") or hashlib.sha256(
                    strict_json_dumps({
                        "symbol": symbol,
                        "end": metadata[symbol].get("end"),
                        "rows": metadata[symbol].get("row_count"),
                        "updated": metadata[symbol].get("updated_at"),
                    }, sort_keys=True).encode("utf-8")
                ).hexdigest()),
                "coverage_start": str(metadata[symbol].get("start") or ""),
                "coverage_end": str(metadata[symbol].get("end") or ""),
            }
            for symbol in selected_symbols
            if str(metadata[symbol].get("end") or "")
        ]
        bar_as_of = max((item["coverage_end"] for item in bar_entries), default="")
        newer_bar_count = sum(
            item["coverage_end"] > lake_as_of for item in bar_entries
        ) if lake_as_of else 0
        use_lake = bool(lake_entries) and len(lake_entries) >= 30 and (
            newer_bar_count < max(1000, round(expected_count * 0.70))
        )
        selected_entries = lake_entries if use_lake else bar_entries
        panel_as_of = lake_as_of if use_lake else bar_as_of
        stockdb_session, stockdb_entry = self._validated_stockdb_session()
        if stockdb_entry is not None:
            selected_entries = [stockdb_entry, *selected_entries]
        generations = self.store.derived.advance_source_generations([
            *selected_entries,
            {
                "source": "instrument_catalog",
                "partition_key": "cn-listed-stock",
                "content_id": instrument_identity,
            },
        ])
        use_stockdb = bool(stockdb_entry is not None and stockdb_session > panel_as_of)
        return {
            "generations": generations,
            "as_of": stockdb_session if use_stockdb else panel_as_of,
            "source": "stockdb_formal" if use_stockdb else (
                "research_lake" if use_lake else "bar_store"
            ),
            "expected_count": expected_count,
            "available": bool(selected_entries) or stockdb_entry is not None,
        }

    @staticmethod
    def _eligible_cn_stocks(symbols: Any, records: dict[str, Any]) -> list[str]:
        return [
            str(symbol) for symbol in symbols
            if (record := records.get(str(symbol))) is not None
            and record.asset_type == "stock"
            and record.market == "CN"
            and record.status.lower() in {"l", "listed"}
        ]

    def _research_lake_matrices(
        self,
        instruments: InstrumentStore,
        expected_count: int,
        progress: Progress,
    ) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str], int, list[str]] | None:
        lake_values = self._research_lake()
        if lake_values is None:
            return None
        close, amount = lake_values
        lake_as_of = str(pd.Timestamp(close.index.max()).date())
        newer_bar_count = sum(
            str(meta.get("end") or "") > lake_as_of
            for symbol, meta in BarStore().metadata_many().items()
            if symbol.endswith((".SH", ".SZ", ".BJ"))
            and len(symbol.rsplit(".", 1)[0]) == 6
        )
        if newer_bar_count >= max(1000, round(expected_count * 0.70)):
            return None
        records = instruments.get_many(close.columns)
        selected = self._eligible_cn_stocks(close.columns, records)
        close, amount = close[selected], amount.reindex(columns=selected)
        names = {symbol: records[symbol].name or symbol for symbol in selected}
        progress(30, "读取全市场研究湖", f"已读取 {len(selected)} 只股票")
        return close, amount, names, expected_count, ["local:research_lake"]

    @staticmethod
    def _bar_series(frame: pd.DataFrame | None, symbol: str) -> tuple[pd.Series, pd.Series] | None:
        if frame is None or frame.empty or "close" not in frame:
            return None
        frame = frame.tail(820)
        close = pd.to_numeric(frame["close"], errors="coerce").rename(symbol)
        if "amount" in frame:
            amount = pd.to_numeric(frame["amount"], errors="coerce").rename(symbol)
        elif "volume" in frame:
            amount = (pd.to_numeric(frame["volume"], errors="coerce") * close).rename(symbol)
        else:
            amount = pd.Series(index=close.index, dtype=float, name=symbol)
        return (close, amount) if close.notna().sum() >= 30 else None

    def _bar_store_matrices(
        self,
        instruments: InstrumentStore,
        expected_count: int,
        progress: Progress,
        cancelled: Cancelled,
    ) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str], int, list[str]]:
        bars = BarStore()
        symbols = [
            symbol for symbol in bars.symbols()
            if symbol.endswith((".SH", ".SZ", ".BJ"))
            and len(symbol.rsplit(".", 1)[0]) == 6
        ]
        records = instruments.get_many(symbols)
        selected = self._eligible_cn_stocks(symbols, records)
        if not selected:
            selected = symbols
        names = {
            symbol: (records[symbol].name or symbol) if symbol in records else symbol
            for symbol in selected
        }

        close_series: list[pd.Series] = []
        amount_series: list[pd.Series] = []
        total = len(selected)
        progress(4, "读取本地行情", f"准备校验 {total} 只股票")
        batch = bars.read_many(
            selected,
            columns=["close", "amount", "volume"],
            max_workers=16,
            enqueue_repair=False,
        )
        for completed, symbol in enumerate(selected, start=1):
            if cancelled():
                raise InterruptedError("板块联动刷新已取消")
            series = self._bar_series(batch.frames.get(symbol), symbol)
            if series is not None:
                close, amount = series
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

    def _stockdb_matrices(
        self,
        instruments: InstrumentStore,
        expected_count: int,
        target_as_of: str,
        progress: Progress,
        cancelled: Cancelled,
        purpose: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str], int, list[str]]:
        """Build a purpose-qualified local StockDB panel."""

        symbols, names = self._listed_symbols(instruments)
        if len(symbols) < 30:
            raise ValueError("本地证券主数据中没有足够的 A 股股票")
        target = pd.Timestamp(target_as_of).normalize()
        sessions = max(60, int(get_config().data.free_stockdb_stock_history_sessions))
        # 380 calendar days safely covers 180 A-share sessions, including
        # Spring Festival and the National Day closure clusters.
        start = (target - timedelta(days=max(380, sessions * 2))).date().isoformat()
        end = target.date().isoformat()
        from quantmaster.data.free_stockdb_ingest import StockDBIngestService

        service = StockDBIngestService()

        def mapped_progress(value: int, phase: str, detail: str) -> None:
            progress(4 + min(26, round(26 * max(0, min(100, value)) / 100)), phase, detail)

        formal = purpose in {"formal_research", "historical_replay"}
        reader = (
            service.read_native_research_history
            if formal else service.read_cross_section_history
        )
        frame = reader(
            symbols, start, end, progress=mapped_progress, cancelled=cancelled,
        )
        if frame.empty or not {"date", "symbol", "close"}.issubset(frame.columns):
            raise ValueError("本地 StockDB 没有可用于市场温度预览的日频截面")
        frame = frame.copy()
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
        frame["symbol"] = frame["symbol"].astype(str).str.upper()
        frame = frame.dropna(subset=["date", "symbol"])
        frame = frame.loc[frame["date"] <= target]
        if "amount" not in frame:
            if "volume" not in frame:
                raise ValueError("本地 StockDB 截面缺少 amount/volume")
            frame["amount"] = (
                pd.to_numeric(frame["volume"], errors="coerce")
                * pd.to_numeric(frame["close"], errors="coerce")
            )
        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame["amount"] = pd.to_numeric(frame["amount"], errors="coerce")
        close = frame.pivot_table(
            index="date", columns="symbol", values="close", aggfunc="last",
        )
        amount = frame.pivot_table(
            index="date", columns="symbol", values="amount", aggfunc="last",
        )
        close = close.reindex(columns=symbols).sort_index()
        amount = amount.reindex(columns=symbols).reindex(close.index).sort_index()
        if close.empty or str(close.index.max().date()) < target_as_of:
            raise ValueError(
                f"本地 StockDB 最新截面为 {str(close.index.max().date()) if not close.empty else '未知'}，"
                f"尚未覆盖目标日 {target_as_of}"
            )
        progress(
            30,
            "读取本地 StockDB 截面",
            f"{len(close.columns)} 只 · 截至 {close.index.max().date()} · "
            f"{'已验收 qfq' if formal else '原始价格预览'}",
        )
        source = "local:stockdb:qfq-accepted" if formal else "local:stockdb:raw"
        return close, amount, names, expected_count, [source]

    def market_matrices(
        self,
        *,
        progress: Progress,
        cancelled: Cancelled,
        target_as_of: str = "",
        purpose: str = "display",
    ) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str], int, list[str]]:
        instruments, expected_count = self._listed_instruments()
        lake_result = self._research_lake_matrices(
            instruments, expected_count, progress,
        )
        if lake_result is not None:
            lake_as_of = str(pd.Timestamp(lake_result[0].index.max()).date())
            if not target_as_of or lake_as_of >= target_as_of:
                return lake_result
        if target_as_of:
            return self._stockdb_matrices(
                instruments,
                expected_count,
                target_as_of,
                progress,
                cancelled,
                purpose,
            )
        return self._bar_store_matrices(
            instruments, expected_count, progress, cancelled,
        )


def _deduplicate_themes(themes: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Preserve provider taxonomy identity; similarity is not a crosswalk."""
    result: dict[str, dict[str, Any]] = {}
    for raw in sorted(themes, key=lambda item: str(item.get("code") or "")):
        code = str(raw.get("code") or "").strip().upper()
        name = str(raw.get("name") or "").strip()
        members = {
            str(symbol).upper() for symbol in raw.get("members") or []
            if str(symbol).upper().endswith((".SH", ".SZ", ".BJ"))
        }
        if not code or not name or not members:
            continue
        result[code] = {
            "code": code,
            "name": name,
            "level": "concept",
            "parent_code": "",
            "members": sorted(members),
            "aliases": list(dict.fromkeys(str(value) for value in raw.get("aliases") or [])),
            "source": str(raw.get("source") or "eastmoney-concept"),
            "taxonomy_id": str(raw.get("taxonomy_id") or ""),
            "effective_date": str(raw.get("effective_date") or raw.get("as_of") or ""),
            "membership_semantics": str(
                raw.get("membership_semantics") or "current_snapshot"
            ),
        }
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
        if dedicated_count >= max(1, round(expected_count * 0.70))
        # A legacy name-only mapping cannot prove that it is SW2021.  Never
        # silently promote Eastmoney/StockDB names into the SWS namespace.
        else strict_l1_groups({}, taxonomy_id="sws:industry:2021")
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


@dataclass
class _RotationBuildState:
    """Mutable state shared by the explicit rotation build stages."""

    spec: RotationJobSpec
    progress: Progress
    cancelled: Cancelled
    job_id: str
    checkpoint: Callable[[str, dict[str, Any]], None] | None
    generated_at: str
    need_market: bool
    local_state: dict[str, Any]
    input_fingerprint: str
    snapshot_fingerprints: dict[str, str]
    scope_snapshot_kinds: tuple[str, ...]
    computed: dict[str, dict[str, Any]] = field(default_factory=dict)
    previous_snapshot_ids: dict[str, str] = field(default_factory=dict)
    remote_required: dict[str, bool] = field(default_factory=dict)
    provider_warnings: list[str] = field(default_factory=list)
    provider_issues: dict[str, list[str]] = field(default_factory=lambda: {
        "market": [], "industries": [], "themes": [], "etf": [],
    })
    provider_results: dict[str, dict[str, Any]] = field(default_factory=dict)
    compute_kinds: set[str] = field(default_factory=set)
    etf_observations: pd.DataFrame = field(default_factory=pd.DataFrame)
    close: pd.DataFrame = field(default_factory=pd.DataFrame)
    amount: pd.DataFrame = field(default_factory=pd.DataFrame)
    names: dict[str, str] = field(default_factory=dict)
    expected_count: int = 0
    sources: list[str] = field(default_factory=lambda: ["local:rotation_cache"])
    as_of: str = ""
    etf_price_source: str = ""
    etf_expected_funds: int | None = None
    expected_as_of: str = ""
    snapshot_id: str = ""
    compute_base: int = 34
    trend: Any = None
    l1_groups: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def scope(self) -> str:
        return self.spec.scope

    def checkpoint_node(self, node: str, payload: dict[str, Any]) -> None:
        if self.checkpoint is not None:
            self.checkpoint(node, payload)


class RotationService:
    def __init__(
        self,
        store: RotationStore | None = None,
        jobs: UnifiedJobStore | None = None,
        *,
        read_only: bool = False,
    ):
        self.read_only = bool(read_only)
        self.store = store or RotationStore(read_only=self.read_only)
        # The task ledger migrates on construction, so page readers must not
        # instantiate it merely to render a cold rotation view.
        self.jobs = jobs if jobs is not None else (
            None if self.read_only else UnifiedJobStore(self.store.root.parent / "jobs.sqlite")
        )
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
        purpose: str = "display",
        remote_fills: int = 0,
        pending: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        theme_taxonomy = next((
            source for source in sources
            if source in {
                "eastmoney-concept", "tushare:dc-concept", "ths:concept",
                "free-stockdb:concept",
            }
        ), "")
        theme_identity = taxonomy_identity(theme_taxonomy, kind="concept")
        result = {
            "snapshot_id": snapshot_id,
            "schema_version": 2,
            "as_of": as_of,
            "actual_as_of": as_of,
            "expected_as_of": expected_as_of,
            "generated_at": generated_at,
            "algorithm_version": ALGORITHM_VERSION,
            "taxonomy_versions": {
                "industry": "SW2021",
                "theme": theme_identity.get("taxonomy_id") or "unresolved",
            },
            "quality": quality,
            "sources": sources,
            "input_fingerprint": "",
            "stale": str(quality.get("status") or "") == "stale",
            "stale_reasons": list(quality.get("issues") or [])
            if str(quality.get("status") or "") == "stale" else [],
        }
        result["status"] = {
            "data": data_status_payload(
                quality=quality,
                sources=sources,
                as_of=as_of,
                expected_as_of=expected_as_of,
                purpose=purpose,
                remote_fills=remote_fills,
                pending=pending,
                taxonomy={
                    "industry": taxonomy_identity("SW2021", kind="industry"),
                    "theme": theme_identity,
                },
            ),
            "providers": provider_status_payload(_provider_health_for_sources(sources)),
        }
        return result

    @staticmethod
    def _scope_snapshot_kinds(scope: str) -> tuple[str, ...]:
        return {
            "all": (
                "temperature", "structure", "industries", "themes", "board_indexes",
                "etf_flows", "taxonomy",
            ),
            "close": (
                "temperature", "structure", "industries", "themes", "board_indexes",
                "taxonomy",
            ),
            "market": ("temperature", "structure"),
            "industries": ("industries", "board_indexes", "taxonomy"),
            "themes": ("themes", "board_indexes"),
            "indexes": ("board_indexes",),
            "etf": ("etf_flows",),
        }.get(str(scope), ())

    @staticmethod
    def _expected_for_spec(spec: RotationJobSpec) -> str:
        # Keep the no-argument call shape for lightweight tests and existing
        # local-calendar implementations; explicit replay dates still retain
        # their strict target contract.
        return (
            _expected_market_session(as_of=spec.as_of)
            if spec.as_of else _expected_market_session()
        )

    def _local_input_state(self) -> dict[str, Any]:
        try:
            return self.loader.local_input_state()
        except (OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
            logger.warning("读取本地 generation 失败；本次不会将旧快照误判为命中", exc_info=True)
            return {
                "generations": [], "as_of": "", "source": "unavailable",
                "expected_count": 0, "available": False,
            }

    def _validate_temporal_taxonomy(self, spec: RotationJobSpec) -> None:
        """Reject latest/current-only taxonomy evidence for historical consumers."""

        if spec.purpose not in {"historical_replay", "formal_research"}:
            return
        cutoff_epoch = _knowledge_cutoff_epoch(spec.knowledge_cutoff)
        declared = {
            value.strip() for value in spec.taxonomy_id.split(",") if value.strip()
        }
        evidence: list[dict[str, Any]] = []
        if spec.scope in {"all", "close", "industries"}:
            evidence.extend(self.store.taxonomy_evidence())
        if spec.scope in {"all", "close", "themes"}:
            evidence.extend(self.store.theme_evidence())
        if not evidence:
            raise ValueError("历史用途没有可用的 taxonomy observation")
        for item in evidence:
            _validate_taxonomy_evidence(
                item,
                declared=declared,
                cutoff_epoch=cutoff_epoch,
                as_of=spec.as_of,
            )

    def input_fingerprint(
        self,
        spec: RotationJobSpec,
        *,
        local_state: dict[str, Any] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Fingerprint only catalog rows, never the full market matrix."""

        state = local_state or self._local_input_state()
        node_fingerprints = self.snapshot_input_fingerprints(spec, local_state=state)
        fingerprint = self.store.derived.input_fingerprint(
            schema_version=2,
            algorithm_version=ALGORITHM_VERSION,
            parameters={
                # This is a task fingerprint.  Individual snapshot nodes use
                # the narrower fingerprints below, so an ETF metadata update
                # does not invalidate a theme matrix.
                "scope": str(spec.scope),
                "as_of": spec.as_of,
                "node_inputs": {
                    kind: node_fingerprints[kind]
                    for kind in sorted(node_fingerprints)
                },
            },
            source_generations=(),
        )
        return fingerprint, state

    def snapshot_input_fingerprints(
        self,
        spec: RotationJobSpec,
        *,
        local_state: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        """Return per-output DAG keys rather than one over-broad refresh key.

        A task may publish several snapshots, but their dependencies differ:
        an ETF share revision changes temperature/ETF flows, not industry or
        theme trend matrices.  Every read-side snapshot carries the key for
        its own dependency cut.
        """

        state = local_state or self._local_input_state()
        local_generations = list(state.get("generations") or [])
        catalog_generations = self.store.source_generations()

        def selected(*sources: str, include_market: bool = False) -> list[dict[str, Any]]:
            names = set(sources)
            values = [
                row for row in catalog_generations
                if str(row.get("source") or "") in names
            ]
            if include_market:
                values.extend(local_generations)
            # ``local_generations`` and the catalog can overlap during a
            # generation probe; a source/partition is authoritative once.
            deduplicated: dict[tuple[str, str], dict[str, Any]] = {}
            for row in values:
                key = (str(row.get("source") or ""), str(row.get("partition_key") or ""))
                if key[0] and key[1]:
                    deduplicated[key] = row
            return list(deduplicated.values())

        dependencies: dict[str, tuple[bool, tuple[str, ...]]] = {
            "temperature": (True, ("rotation.etf_observations", "rotation.etf_metadata", "news.annotations")),
            "structure": (True, ()),
            "industries": (True, ("rotation.taxonomy",)),
            "themes": (True, ("rotation.taxonomy", "rotation.themes")),
            "board_indexes": (True, ("rotation.taxonomy", "rotation.themes")),
            "taxonomy": (False, ("rotation.taxonomy",)),
            "etf_flows": (False, ("rotation.etf_observations", "rotation.etf_metadata")),
        }
        result: dict[str, str] = {}
        for kind in self._scope_snapshot_kinds(spec.scope):
            include_market, sources = dependencies[kind]
            result[kind] = self.store.derived.input_fingerprint(
                schema_version=2,
                algorithm_version=ALGORITHM_VERSION,
                parameters={"kind": kind, "as_of": spec.as_of},
                source_generations=selected(*sources, include_market=include_market),
            )
        return result

    def _published_for_input(
        self,
        spec: RotationJobSpec,
        snapshot_fingerprints: dict[str, str],
        local_state: dict[str, Any],
    ) -> bool:
        """Whether every output in scope is a valid current node for this input."""

        for kind in self._scope_snapshot_kinds(spec.scope):
            try:
                snapshot = self.store.snapshot_header(kind)
            except RotationIntegrityError:
                return False
            if snapshot is None:
                return False
            meta = snapshot.get("meta") or {}
            quality = meta.get("quality") or {}
            pending_board_indexes = (
                kind == "board_indexes"
                and int(
                    ((snapshot.get("data") or {}).get("summary") or {}).get(
                        "pending_board_count"
                    ) or 0
                ) > 0
            )
            if (
                str(meta.get("algorithm_version") or "") != ALGORITHM_VERSION
                or str(meta.get("input_fingerprint") or "")
                != str(snapshot_fingerprints.get(kind) or "")
                or str(quality.get("status") or "") in {"cold", "corrupt", "empty"}
                or pending_board_indexes
            ):
                return False
        return True

    def _remote_requirements(
        self,
        spec: RotationJobSpec,
        local_state: dict[str, Any],
    ) -> dict[str, bool]:
        """Remote is a supplement only when local evidence is absent or stale."""

        scope = spec.scope
        need_market = scope in {
            "all", "close", "market", "industries", "themes", "indexes",
        }
        expected_as_of = self._expected_for_spec(spec)
        market_missing = not bool(local_state.get("available"))
        market_stale = bool(
            expected_as_of
            and str(local_state.get("as_of") or "") < expected_as_of
        )
        theme_generations = self.store.source_generations("rotation.themes")
        themes_fresh = bool(self.store.themes()) and (
            not expected_as_of
            or any(
                str(row.get("coverage_end") or "") >= expected_as_of
                for row in theme_generations
            )
        )
        industry_pending = self.store.has_pending_theme_sync(("sws:industry:2021",))
        theme_pending = self.store.has_pending_theme_sync((
            "eastmoney-concept", "tushare:dc-concept", "ths:concept",
        ))
        return {
            "market": need_market and (market_missing or market_stale),
            "industries": scope in {"all", "close", "industries"}
            and (not bool(self.store.taxonomy_nodes()) or industry_pending),
            "themes": scope in {"all", "close", "themes"}
            and (not themes_fresh or theme_pending),
            "etf": scope in {"all", "etf"} and not self.store.etf_path.is_file(),
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
        purpose: str = "display",
        remote_fills: int = 0,
        pending: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "meta": self._meta(
                snapshot_id=snapshot_id,
                as_of=str(data.get("as_of") or ""),
                generated_at=generated_at,
                quality=quality,
                sources=sources,
                expected_as_of=expected_as_of,
                purpose=purpose,
                remote_fills=remote_fills,
                pending=pending,
            ),
            "data": data,
        }

    def _matching_snapshot_kinds(
        self,
        scope_snapshot_kinds: tuple[str, ...],
        snapshot_fingerprints: dict[str, str],
    ) -> set[str]:
        matched: set[str] = set()
        for kind in scope_snapshot_kinds:
            try:
                header = self.store.snapshot_header(kind)
            except RotationIntegrityError:
                header = None
            meta = (header or {}).get("meta") or {}
            pending_board_indexes = (
                kind == "board_indexes"
                and int(
                    (((header or {}).get("data") or {}).get("summary") or {}).get(
                        "pending_board_count"
                    ) or 0
                ) > 0
            )
            if (
                str(meta.get("algorithm_version") or "") == ALGORITHM_VERSION
                and str(meta.get("input_fingerprint") or "")
                == str(snapshot_fingerprints.get(kind) or "")
                and not pending_board_indexes
            ):
                matched.add(kind)
        return matched

    def _reuse_result(
        self,
        spec: RotationJobSpec,
        *,
        scope_snapshot_kinds: tuple[str, ...],
        need_market: bool,
        local_state: dict[str, Any],
        input_fingerprint: str,
        warnings: list[str] | None = None,
    ) -> dict[str, Any]:
        headers = [
            self.store.snapshot_header(kind) or {}
            for kind in scope_snapshot_kinds
        ]
        as_of = max(
            (str((header.get("meta") or {}).get("as_of") or "") for header in headers),
            default="",
        )
        expected_as_of = self._expected_for_spec(spec) if need_market else ""
        unique_warnings = list(dict.fromkeys(warnings or []))
        return {
            "snapshot_id": _snapshot_id(
                as_of,
                [str((header.get("meta") or {}).get("snapshot_id") or "") for header in headers],
                spec.scope,
            ),
            "as_of": as_of,
            "expected_as_of": expected_as_of,
            "fresh": not expected_as_of or as_of >= expected_as_of,
            "outcome": "partial" if unique_warnings else "unchanged",
            "updated": [],
            "computed": [],
            "warnings": unique_warnings,
            "tracked_count": int(local_state.get("expected_count") or 0),
            "expected_count": int(local_state.get("expected_count") or 0),
            "input_fingerprint": input_fingerprint,
        }

    def _reuse_incremental_result(
        self,
        spec: RotationJobSpec,
        *,
        snapshot_fingerprints: dict[str, str],
        scope_snapshot_kinds: tuple[str, ...],
        local_state: dict[str, Any],
        input_fingerprint: str,
        remote_required: dict[str, bool],
        need_market: bool,
        progress: Progress,
        checkpoint_node: Callable[[str, dict[str, Any]], None],
        job_id: str,
    ) -> dict[str, Any] | None:
        if spec.mode != "incremental":
            return None
        if not self._published_for_input(spec, snapshot_fingerprints, local_state):
            return None
        if spec.source == "auto" and any(remote_required.values()):
            return None
        self._validate_temporal_taxonomy(spec)
        progress(100, "复用已发布快照", "本地输入 generation 未变化；未读取行情或访问 provider")
        checkpoint_node("source", {
            "cache_hit": True,
            "input_fingerprint": input_fingerprint,
        })
        get_runtime_metrics().record_node(
            "rotation.refresh", job_id=job_id, input_fingerprint=input_fingerprint,
            cache_hit=True,
        )
        return self._reuse_result(
            spec,
            scope_snapshot_kinds=scope_snapshot_kinds,
            need_market=need_market,
            local_state=local_state,
            input_fingerprint=input_fingerprint,
        )

    def build(
        self,
        spec: RotationJobSpec,
        *,
        progress: Progress,
        cancelled: Cancelled,
        job_id: str = "",
        checkpoint: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        return _RotationBuildRun(
            self,
            spec,
            progress=progress,
            cancelled=cancelled,
            job_id=job_id,
            checkpoint=checkpoint,
        ).run()

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
        quality = _status_quality("cold", issues=[messages.get(kind, "尚无快照")])
        return {
            "meta": RotationService._meta(
                snapshot_id="",
                as_of="",
                generated_at="",
                quality=quality,
                sources=[],
                purpose="display",
            ),
            "data": {"as_of": "", "items": [], "message": messages.get(kind, "尚无快照")},
        }

    def _decorate_snapshot(self, kind: str, value: dict[str, Any]) -> dict[str, Any]:
        """Apply public stale/cold metadata without forcing list materialization."""

        try:
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
            if (
                not self.read_only
                and kind == "themes"
                and str(quality.get("status") or "") == "cold"
                and self.jobs is not None
            ):
                active = next((
                    job for job in self.jobs.list(50)
                    if str(job.get("status") or "") in {"queued", "running", "cancelling"}
                    and str(job.get("type") or "") == "rotation.refresh"
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
            # Provider health is live operational state.  Never freeze it into
            # a data snapshot: a recovered lane must update in place without
            # changing the independently computed data usability.
            status = meta.get("status")
            if isinstance(status, dict):
                status["providers"] = provider_status_payload(
                    _provider_health_for_sources(list(meta.get("sources") or []))
                )
            return value
        except RotationIntegrityError:
            logger.error("板块联动快照完整性失败 kind=%s", kind, exc_info=True)
            cold = self.cold(kind)
            cold["meta"]["quality"] = _status_quality(
                "corrupt", issues=[
                    "板块联动快照完整性校验失败",
                    "请重新生成联动快照；损坏内容不会参与计算",
                ],
            )
            cold["data"]["message"] = "板块联动快照损坏，已停止使用"
            return cold

    def snapshot(self, kind: str) -> dict[str, Any]:
        try:
            return self._decorate_snapshot(kind, self.store.snapshot(kind) or self.cold(kind))
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

    def snapshot_header(self, kind: str) -> dict[str, Any]:
        """Return public snapshot meta/summary without loading large lists."""

        try:
            value = self.store.snapshot_header(kind) or self.cold(kind)
            return self._decorate_snapshot(kind, value)
        except RotationIntegrityError:
            return self._decorate_snapshot(kind, self.cold(kind))

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

    def overview(self, window: int = 5) -> dict[str, Any]:
        if window not in ROTATION_WINDOWS:
            raise ValueError("轮动观察窗口仅支持 1、3、5、20 日")
        temperature = self.snapshot("temperature")
        structure = self.snapshot("structure")
        industries = self.snapshot("industries")
        themes = self.snapshot("themes")
        etf = self.snapshot("etf_flows")
        snapshots = (temperature, structure, industries, themes, etf)
        metas = [value["meta"] for value in snapshots]
        cache_key = (
            *(str(meta.get("snapshot_id") or "") for meta in metas), str(window),
        )
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
            "industries": _rank_window(l1_industries, window),
            "themes": _rank_window(theme_items, window),
        }
        resonance = _resonance_rows(l1_industries, theme_items, window)
        temperature_history = list(temperature["data"].get("history") or [])
        temperature_change = None
        if len(temperature_history) > window:
            latest = temperature_history[-1].get("temperature")
            previous = temperature_history[-1 - window].get("temperature")
            temperature_change = (
                round(float(latest) - float(previous), 2)
                if latest is not None and previous is not None else None
            )

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

        etf_summary = dict(etf["data"].get("summary") or {})
        etf_windows = etf_summary.pop("windows", {})
        etf_summary["window"] = (
            etf_windows.get(str(window), {}) if isinstance(etf_windows, dict) else {}
        )
        data = {
            "as_of": as_of,
            "window": window,
            "industries": visible_industries[:8],
            "themes": theme_items[:8],
            "windows": list(ROTATION_WINDOWS),
            "dimensions": {
                "market": market_dimension,
                "industries": dimension_meta(industries),
                "themes": dimension_meta(themes),
                "etf": dimension_meta(etf),
            },
            "market": {
                "temperature": temperature["data"].get("current"),
                "temperature_change": temperature_change,
                "structure": structure["data"].get("current"),
            },
            "distributions": {
                "industries": industries["data"].get("summary", {}),
                "themes": themes["data"].get("summary", {}),
            },
            "rankings": rankings,
            "resonance": resonance,
            "etf_summary": etf_summary,
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
        snapshot = self.snapshot_header(kind)
        item = self.store.snapshot_detail(kind, str(code).upper())
        if item is None:
            return None
        return {"meta": snapshot["meta"], "data": item}


class _RotationBuildRun:
    """Execute one refresh as explicit source, compute, and publish stages."""

    def __init__(
        self,
        service: RotationService,
        spec: RotationJobSpec,
        *,
        progress: Progress,
        cancelled: Cancelled,
        job_id: str,
        checkpoint: Callable[[str, dict[str, Any]], None] | None,
    ) -> None:
        need_market = spec.scope in {
            "all", "close", "market", "industries", "themes", "indexes",
        }
        local_state = (
            service._local_input_state()
            if need_market else {
                "generations": [], "as_of": "", "source": "not_required",
                "expected_count": 0, "available": True,
            }
        )
        input_fingerprint, local_state = service.input_fingerprint(
            spec, local_state=local_state,
        )
        self.service = service
        self.metrics = get_runtime_metrics()
        self.state = _RotationBuildState(
            spec=spec,
            progress=progress,
            cancelled=cancelled,
            job_id=job_id,
            checkpoint=checkpoint,
            generated_at=_utc_now(),
            need_market=need_market,
            local_state=local_state,
            input_fingerprint=input_fingerprint,
            snapshot_fingerprints=service.snapshot_input_fingerprints(
                spec, local_state=local_state,
            ),
            scope_snapshot_kinds=service._scope_snapshot_kinds(spec.scope),
        )
        self.state.remote_required = (
            service._remote_requirements(spec, local_state)
            if spec.source == "auto" else {}
        )

    def run(self) -> dict[str, Any]:
        state = self.state
        reused = self.service._reuse_incremental_result(
            state.spec,
            snapshot_fingerprints=state.snapshot_fingerprints,
            scope_snapshot_kinds=state.scope_snapshot_kinds,
            local_state=state.local_state,
            input_fingerprint=state.input_fingerprint,
            remote_required=state.remote_required,
            need_market=state.need_market,
            progress=state.progress,
            checkpoint_node=state.checkpoint_node,
            job_id=state.job_id,
        )
        if reused is not None:
            return reused
        self._capture_previous_snapshots()
        self._sync_remote_sources()
        state.checkpoint_node("source", {
            "input_fingerprint": state.input_fingerprint,
            "remote_operations": sorted(state.provider_results),
            "source_generations": list(state.local_state.get("generations") or []),
        })
        self.service._validate_temporal_taxonomy(state.spec)
        state.compute_kinds = set(state.scope_snapshot_kinds) - self.service._matching_snapshot_kinds(
            state.scope_snapshot_kinds, state.snapshot_fingerprints,
        )
        reused = self._reuse_after_remote_probe()
        if reused is not None:
            return reused
        self._load_compute_inputs()
        temperature_quality = self._compute_temperature()
        self._compute_structure(temperature_quality)
        self._compute_industries()
        self._compute_themes()
        self._compute_board_indexes()
        self._compute_etf_flows()
        return self._publish()

    def _capture_previous_snapshots(self) -> None:
        state = self.state
        for kind in (
            "temperature", "structure", "industries", "themes", "board_indexes",
            "etf_flows", "taxonomy",
        ):
            try:
                previous = self.service.store.snapshot_header(kind)
            except RotationIntegrityError:
                previous = None
            state.previous_snapshot_ids[kind] = str(
                (previous or {}).get("meta", {}).get("snapshot_id") or ""
            )

    def _provider_operations(
        self, provider: Any,
    ) -> list[tuple[str, str, Callable[[], dict[str, Any]]]]:
        state = self.state
        spec = state.spec
        required = state.remote_required
        operations: list[tuple[str, str, Callable[[], dict[str, Any]]]] = []
        if required["market"]:
            market_kwargs: dict[str, Any] = {"rebuild": spec.mode == "rebuild"}
            if spec.as_of:
                market_kwargs["as_of"] = spec.as_of
            operations.append((
                "market", "全市场日线",
                lambda: provider.sync_market_history(
                    state.progress, state.cancelled, **market_kwargs,
                ),
            ))
        if required["industries"]:
            operations.append((
                "industries", "申万行业层级",
                lambda: provider.sync_industry_taxonomy(
                    state.progress, state.cancelled, **(
                        {"as_of": spec.as_of} if spec.as_of else {}
                    ),
                ),
            ))
        if required["themes"]:
            theme_kwargs = {"as_of": spec.as_of} if spec.as_of else {}
            purpose_kwargs = (
                {"purpose": spec.purpose}
                if spec.as_of and spec.purpose in {"historical_replay", "formal_research"}
                else {}
            )
            operations.append((
                "themes", "细分题材目录",
                lambda: provider.sync_themes(
                    state.progress, state.cancelled, **purpose_kwargs, **theme_kwargs,
                ),
            ))
        if required["etf"]:
            etf_kwargs = {"as_of": spec.as_of} if spec.as_of else {}
            operations.append((
                "etf", "ETF 份额",
                lambda: provider.sync_etf_observations(
                    state.progress, state.cancelled, **etf_kwargs,
                ),
            ))
        return operations

    def _record_provider_success(self, key: str) -> None:
        state = self.state
        result = state.provider_results[key]
        issues = [str(issue) for issue in result.get("issues") or []]
        state.provider_warnings.extend(issues)
        source_name = {
            "industries": "rotation.taxonomy",
            "themes": "rotation.themes",
            "etf": "rotation.etf_observations",
        }.get(key, "")
        quality_status = str(result.get("quality_status") or "complete").lower()
        unavailable = {"partial", "failed", "unavailable"}
        if quality_status in unavailable:
            state.provider_issues[key].extend(issues)
        observed_as_of = str(
            result.get("as_of")
            or result.get("expected_as_of")
            or state.spec.as_of
            or self.service._expected_for_spec(state.spec)
            or ""
        )
        if source_name and quality_status not in unavailable:
            self.service.store.mark_source_coverage(source_name, observed_as_of)

    def _run_provider_operation(
        self,
        key: str,
        label: str,
        operation: Callable[[], dict[str, Any]],
    ) -> None:
        state = self.state
        started_wall = time.perf_counter()
        started_cpu = time.process_time()
        try:
            state.provider_results[key] = operation()
            self.metrics.record_node(
                f"rotation.source.{key}", job_id=state.job_id,
                input_fingerprint=state.input_fingerprint,
                wall_ms=(time.perf_counter() - started_wall) * 1000,
                cpu_ms=(time.process_time() - started_cpu) * 1000,
                remote_calls=1,
            )
            self._record_provider_success(key)
        except InterruptedError:
            raise
        except Exception as exc:  # 外部数据源边界：记录后降级到已有快照
            self.metrics.record_node(
                f"rotation.source.{key}", job_id=state.job_id,
                input_fingerprint=state.input_fingerprint, status="failed",
                wall_ms=(time.perf_counter() - started_wall) * 1000,
                cpu_ms=(time.process_time() - started_cpu) * 1000,
                remote_calls=1,
            )
            logger.info("%s 上游不可用；将按本地数据覆盖判定可用性：%s", label, exc)
            state.provider_warnings.append(f"{label}同步失败：{str(exc)[:160]}")

    def _sync_remote_sources(self) -> None:
        state = self.state
        if state.spec.source != "auto":
            return
        _ensure_rotation_provider_registered()
        from quantmaster.rotation.provider_access import rotation_provider

        operations = self._provider_operations(rotation_provider()(self.service.store))
        for key, label, operation in operations:
            self._run_provider_operation(key, label, operation)
        if not operations:
            return
        if state.need_market:
            state.local_state = self.service._local_input_state()
        state.input_fingerprint, state.local_state = self.service.input_fingerprint(
            state.spec, local_state=state.local_state,
        )
        state.snapshot_fingerprints = self.service.snapshot_input_fingerprints(
            state.spec, local_state=state.local_state,
        )

    def _reuse_after_remote_probe(self) -> dict[str, Any] | None:
        state = self.state
        if state.compute_kinds:
            return None
        state.progress(100, "复用已发布快照", "远程新鲜度探测未发现新的输入 generation")
        return self.service._reuse_result(
            state.spec,
            scope_snapshot_kinds=state.scope_snapshot_kinds,
            need_market=state.need_market,
            local_state=state.local_state,
            input_fingerprint=state.input_fingerprint,
            warnings=state.provider_warnings,
        )

    def _load_etf_observations(self) -> pd.DataFrame:
        state = self.state
        if not state.compute_kinds & {"temperature", "etf_flows"}:
            return pd.DataFrame()
        try:
            return self.service.store.etf_observations()
        except RotationIntegrityError:
            if state.scope in {"all", "etf"}:
                raise
            logger.warning("市场温度读取 ETF 观察文件失败", exc_info=True)
            return pd.DataFrame()

    def _market_loader_progress(self) -> Progress:
        state = self.state
        if state.spec.source != "auto":
            return state.progress

        def scaled(value: int, phase: str, detail: str) -> None:
            state.progress(62 + round(max(0, min(30, value)) * 0.20), phase, detail)

        return scaled

    def _load_market_matrices(self) -> None:
        state = self.state
        allow_stockdb_preview = state.spec.purpose in {"display", "current_analysis"}
        stockdb_session, stockdb_entry = self.service.loader._validated_stockdb_session()
        if stockdb_entry is not None and not allow_stockdb_preview:
            observed_at = datetime.fromisoformat(str(stockdb_entry["observed_at"]))
            if observed_at.timestamp() > _knowledge_cutoff_epoch(state.spec.knowledge_cutoff):
                raise ValueError("StockDB 验收记录晚于 knowledge_cutoff")
        target_as_of = state.spec.as_of or stockdb_session
        with self.metrics.node_timer(
            "rotation.market_matrix", job_id=state.job_id,
            input_fingerprint=state.input_fingerprint,
        ) as dimensions:
            (
                state.close, state.amount, state.names,
                state.expected_count, state.sources,
            ) = self.service.loader.market_matrices(
                progress=self._market_loader_progress(),
                cancelled=state.cancelled,
                target_as_of=target_as_of,
                purpose=state.spec.purpose,
            )
            dimensions.update(
                input_rows=len(state.close),
                output_rows=int(state.close.notna().sum().sum()),
            )
        if state.sources == ["local:stockdb:raw"]:
            issue = (
                "本地 StockDB 原始价格用于当前展示分析；"
                "复权因子链未完整验证，不能作为正式研究依据"
            )
            if issue not in state.provider_issues["market"]:
                state.provider_issues["market"].append(issue)
        if state.cancelled():
            raise InterruptedError("板块联动刷新已取消")
        state.checkpoint_node("market_panel", {
            "as_of": str(state.close.index[-1].date()) if not state.close.empty else "",
            "rows": len(state.close),
            "symbols": len(state.close.columns),
        })

    def _load_compute_inputs(self) -> None:
        state = self.state
        load_market_matrix = bool(
            state.compute_kinds & {
                "temperature", "structure", "industries", "themes", "board_indexes",
            }
        )
        state.etf_observations = self._load_etf_observations()
        state.expected_count = int(state.local_state.get("expected_count") or 0)
        if load_market_matrix:
            self._load_market_matrices()
        current_headers = [
            self.service.store.snapshot_header(kind) or {}
            for kind in state.scope_snapshot_kinds
        ]
        state.as_of = (
            str(state.close.index[-1].date()) if not state.close.empty else max(
                (
                    str((header.get("meta") or {}).get("as_of") or "")
                    for header in current_headers
                ),
                default=str(state.local_state.get("as_of") or ""),
            )
        ) or str(state.local_state.get("as_of") or "")
        if not state.etf_observations.empty:
            state.etf_observations, state.etf_price_source = _overlay_stockdb_etf_prices(
                state.etf_observations, as_of=state.as_of,
            )
        state.etf_expected_funds = _expected_etf_funds(self.service.store)
        state.expected_as_of = str(
            state.provider_results.get("market", {}).get("expected_as_of")
            or self.service._expected_for_spec(state.spec)
        ) if state.need_market else ""
        state.snapshot_id = _snapshot_id(
            state.as_of, list(state.close.columns), state.scope,
        )
        state.compute_base = 70 if state.spec.source == "auto" else 34
        state.trend = compute_trend_matrices(state.close) if load_market_matrix else None
        if state.trend is not None:
            state.checkpoint_node("trend_state", {
                "as_of": state.as_of,
                "rows": len(state.close),
                "symbols": len(state.close.columns),
                "windows": list(ROTATION_WINDOWS),
            })

    def _temperature_evidence(
        self,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[str, dict[str, Any]]]]:
        state = self.state
        temperature_dates = market_temperature_reference_dates(state.trend)
        temperature_as_of = temperature_dates.get(0, state.as_of)
        knowledge_as_of = time.time()
        etf_evidence = compute_etf_capital_evidence(
            state.etf_observations,
            as_of=temperature_as_of,
            expected_funds=state.etf_expected_funds,
        )
        sentiment_evidence = _news_sentiment_evidence(
            temperature_as_of, knowledge_as_of=knowledge_as_of,
        )
        historical: dict[str, dict[str, dict[str, Any]]] = {}
        for window in ROTATION_WINDOWS:
            reference_as_of = temperature_dates.get(window)
            if not reference_as_of or reference_as_of in historical:
                continue
            historical[reference_as_of] = {
                "etf_capital": compute_etf_capital_evidence(
                    state.etf_observations,
                    as_of=reference_as_of,
                    expected_funds=state.etf_expected_funds,
                ),
                "sentiment": _news_sentiment_evidence(
                    reference_as_of, knowledge_as_of=knowledge_as_of,
                ),
            }
        return etf_evidence, sentiment_evidence, historical

    def _mark_market_preview_quality(self, quality: dict[str, Any]) -> dict[str, Any]:
        """Expose raw StockDB current-analysis evidence as partial quality."""

        state = self.state
        if state.sources != ["local:stockdb:raw"]:
            return quality
        value = dict(quality)
        if str(value.get("status") or "") not in {"cold", "empty", "corrupt", "loading"}:
            value["status"] = "partial"
        return value

    def _temperature_sources(
        self,
        etf_evidence: dict[str, Any],
        sentiment_evidence: dict[str, Any],
        historical: dict[str, dict[str, dict[str, Any]]],
    ) -> list[str]:
        state = self.state
        sources = list(state.sources)
        all_etf = [
            etf_evidence,
            *(value["etf_capital"] for value in historical.values()),
        ]
        all_sentiment = [
            sentiment_evidence,
            *(value["sentiment"] for value in historical.values()),
        ]
        if any(value.get("available") for value in all_etf):
            sources.extend(["tushare:fund_share", "local:rotation_cache"])
            if state.etf_price_source:
                sources.append(state.etf_price_source)
            if "nav" in state.etf_observations and state.etf_observations["nav"].notna().any():
                sources.append("tushare:fund_nav")
            if "close" in state.etf_observations and state.etf_observations["close"].notna().any():
                sources.append("tushare:fund_daily")
        if any(value.get("available") for value in all_sentiment):
            sources.append("local:news")
        return list(dict.fromkeys(sources))

    def _compute_temperature(self) -> dict[str, Any] | None:
        state = self.state
        if "temperature" not in state.compute_kinds:
            return None
        state.progress(state.compute_base, "计算市场温度", "汇总四档趋势分布与证据权重")
        assert state.trend is not None
        etf_evidence, sentiment_evidence, historical = self._temperature_evidence()
        temperature = compute_market_temperature(
            state.close,
            state.amount,
            expected_count=state.expected_count,
            trend=state.trend,
            supplemental_evidence={
                "etf_capital": etf_evidence,
                "sentiment": sentiment_evidence,
            },
            supplemental_evidence_history=historical,
        )
        quality = temperature.pop("quality")
        quality["issues"] = list(dict.fromkeys([
            *(quality.get("issues") or []), *state.provider_issues["market"],
        ]))
        quality = self._mark_market_preview_quality(quality)
        quality = _mark_stale(
            quality, str(temperature.get("as_of") or ""), state.expected_as_of,
        )
        state.computed["temperature"] = self.service._envelope(
            temperature,
            snapshot_id=state.snapshot_id,
            generated_at=state.generated_at,
            quality=quality,
            sources=self._temperature_sources(
                etf_evidence, sentiment_evidence, historical,
            ),
            expected_as_of=state.expected_as_of,
            purpose=state.spec.purpose,
            remote_fills=int(bool(state.provider_results.get("market"))),
        )
        return quality

    def _compute_structure(self, temperature_quality: dict[str, Any] | None) -> None:
        state = self.state
        if "structure" not in state.compute_kinds:
            return
        state.progress(state.compute_base + 7, "计算市场风格", "比较强势与低位样本收益分布")
        assert state.trend is not None
        structure = compute_market_structure(
            state.close, names=state.names, trend=state.trend,
        )
        quality = temperature_quality
        if quality is None:
            quality = _mark_stale(
                _status_quality(
                    "complete" if len(state.close.columns) >= state.expected_count else "partial",
                    eligible=len(state.close.columns),
                    expected=state.expected_count,
                    issues=list(state.provider_issues["market"]),
                ),
                state.as_of,
                state.expected_as_of,
            )
        state.computed["structure"] = self.service._envelope(
            structure,
            snapshot_id=state.snapshot_id,
            generated_at=state.generated_at,
            quality=quality,
            sources=state.sources,
            expected_as_of=state.expected_as_of,
            purpose=state.spec.purpose,
            remote_fills=int(bool(state.provider_results.get("market"))),
        )

    def _analyze_industries(
        self, l2_groups: dict[str, dict[str, Any]],
    ) -> tuple[dict[str, Any], dict[str, Any], int]:
        state = self.state
        assert state.trend is not None
        with self.metrics.node_timer(
            "rotation.industries", job_id=state.job_id,
            input_fingerprint=state.input_fingerprint,
        ) as dimensions:
            industries = analyze_group_rotation(
                state.close,
                {**state.l1_groups, **l2_groups},
                names=state.names,
                amount=state.amount,
                trend=state.trend,
            )
            dimensions.update(
                input_rows=len(state.close), output_rows=len(industries["items"]),
            )
        count = len(industries["items"])
        quality = _status_quality(
            "complete" if count >= 28 else "partial" if count >= 20 else "limited",
            eligible=count,
            expected=31 + len(l2_groups),
            issues=[
                *([] if count >= 28 else ["部分行业未达到 8 只成分与 70% 行情覆盖门槛"]),
                *state.provider_issues["market"],
                *state.provider_issues["industries"],
            ],
        )
        quality = self._mark_market_preview_quality(quality)
        return industries, _mark_stale(quality, state.as_of, state.expected_as_of), count

    def _compute_industries(self) -> None:
        state = self.state
        needs_industries = "industries" in state.compute_kinds
        needs_taxonomy = "taxonomy" in state.compute_kinds
        if not (needs_industries or needs_taxonomy):
            return
        state.progress(state.compute_base + 12, "聚合申万行业", "严格过滤申万 2021 层级")
        state.l1_groups = _load_l1_groups(self.service.store, state.expected_count)
        l2_groups = merge_l2_groups(
            state.l1_groups, self.service.store.taxonomy_nodes("L2"),
        )
        industries: dict[str, Any] = {}
        if needs_industries:
            industries, quality, count = self._analyze_industries(l2_groups)
            state.computed["industries"] = self.service._envelope(
                industries,
                snapshot_id=state.snapshot_id,
                generated_at=state.generated_at,
                quality=quality,
                sources=[*state.sources, "SW2021"],
                expected_as_of=state.expected_as_of,
                purpose=state.spec.purpose,
                remote_fills=int(
                    state.provider_results.get("industries", {}).get("fresh") or 0
                ),
                pending=dict(
                    state.provider_results.get("industries", {}).get("pending") or {}
                ),
            )
        else:
            header = self.service.store.snapshot_header("industries") or {}
            quality = dict((header.get("meta") or {}).get("quality") or {})
            quality = quality or _status_quality("complete")
            count = 0
        if needs_taxonomy:
            taxonomy = taxonomy_payload(state.l1_groups, l2_groups)
            taxonomy["as_of"] = industries["as_of"] if needs_industries else state.as_of
            state.computed["taxonomy"] = self.service._envelope(
                taxonomy,
                snapshot_id=state.snapshot_id,
                generated_at=state.generated_at,
                quality=quality,
                sources=["SW2021"],
                expected_as_of=state.expected_as_of,
                purpose=state.spec.purpose,
                remote_fills=int(
                    state.provider_results.get("industries", {}).get("fresh") or 0
                ),
                pending=dict(
                    state.provider_results.get("industries", {}).get("pending") or {}
                ),
            )
        state.checkpoint_node("industries", {
            "as_of": state.as_of,
            "groups": count,
            "input_fingerprint": state.snapshot_fingerprints.get("industries", ""),
        })

    @staticmethod
    def _cold_theme_data(as_of: str) -> dict[str, Any]:
        return {
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

    def _analyze_themes(
        self,
        themes: dict[str, dict[str, Any]],
        provider_issues: list[str],
    ) -> tuple[dict[str, Any], dict[str, Any], int]:
        state = self.state
        if not state.l1_groups:
            state.l1_groups = _load_l1_groups(self.service.store, state.expected_count)
        assert state.trend is not None
        with self.metrics.node_timer(
            "rotation.themes", job_id=state.job_id,
            input_fingerprint=state.input_fingerprint,
        ) as dimensions:
            data = analyze_group_rotation(
                state.close, themes, names=state.names, amount=state.amount,
                kind="theme", trend=state.trend,
            )
            dimensions.update(input_rows=len(state.close), output_rows=len(data["items"]))
        industry_links = map_theme_industries(themes, state.l1_groups)
        for item in data["items"]:
            item.update(industry_links.get(str(item.get("code")), {}))
        for code, item in data["details"].items():
            item.update(industry_links.get(str(code), {}))
        data["definition"]["industry_mapping"] = (
            "申万一级行业真实成分交集；主行业至少 3 只且占已映射成员 25%"
        )
        count = len(data["items"])
        provider_quality = str(
            state.provider_results.get("themes", {}).get("quality_status") or "complete"
        )
        effective_issues = [] if provider_quality == "complete" else provider_issues
        catalog_expected = int(
            state.provider_results.get("themes", {}).get("catalog") or len(themes)
        )
        quality = _status_quality(
            "complete" if count >= 50 and provider_quality == "complete" else "partial",
            eligible=count,
            expected=catalog_expected,
            issues=list(dict.fromkeys([
                *([] if count >= 50 else ["概念成分目录仍在积累"]),
                *effective_issues,
                *state.provider_issues["market"],
                *state.provider_issues["themes"],
            ])),
        )
        return data, self._mark_market_preview_quality(quality), count

    def _compute_themes(self) -> None:
        state = self.state
        if "themes" not in state.compute_kinds:
            return
        state.progress(state.compute_base + 17, "扫描细分题材", "合并高度重叠的概念板块")
        assert state.trend is not None
        stored_themes = self.service.store.themes()
        if state.spec.source == "auto" and "themes" not in state.provider_results and not stored_themes:
            raise RuntimeError(next(
                (
                    warning for warning in state.provider_warnings
                    if "细分题材目录" in warning
                ),
                "细分题材三套数据源均不可用，未生成空快照",
            ))
        themes = _deduplicate_themes(stored_themes)
        theme_sources = list(dict.fromkeys(
            str(item.get("source") or "")
            for item in stored_themes if str(item.get("source") or "")
        ))
        provider_issues = list(
            state.provider_results.get("themes", {}).get("issues") or []
        )
        if themes:
            data, quality, count = self._analyze_themes(themes, provider_issues)
        else:
            data = self._cold_theme_data(state.as_of)
            quality = _status_quality("cold", issues=[
                "尚未建立细分题材成分目录",
                *state.provider_issues["market"],
                *state.provider_issues["themes"],
            ])
            count = 0
        quality = _mark_stale(quality, state.as_of, state.expected_as_of)
        state.computed["themes"] = self.service._envelope(
            data,
            snapshot_id=state.snapshot_id,
            generated_at=state.generated_at,
            quality=quality,
            sources=list(dict.fromkeys([*state.sources, *theme_sources])),
            expected_as_of=state.expected_as_of,
            purpose=state.spec.purpose,
            remote_fills=int(state.provider_results.get("themes", {}).get("fresh") or 0),
            pending=dict(state.provider_results.get("themes", {}).get("pending") or {}),
        )
        state.checkpoint_node("themes", {
            "as_of": state.as_of,
            "groups": count,
            "input_fingerprint": state.snapshot_fingerprints.get("themes", ""),
        })

    def _compute_board_indexes(self) -> None:
        state = self.state
        if "board_indexes" not in state.compute_kinds:
            return
        if not state.as_of:
            raise RuntimeError("没有可用于板块指数的本地交易日证据")
        state.progress(88, "准备板块指数", "读取 StockDB 当前成分目录")
        theme_items = list(
            (state.computed.get("themes", {}).get("data") or {}).get("items") or []
        )
        if not theme_items:
            _header, theme_items, _page = self.service.store.snapshot_items_page(
                "themes", page=1, page_size=500,
            )
        theme_codes = {
            str(item.get("code") or "").upper() for item in theme_items
            if str(item.get("code") or "")
        }
        selected_l2 = {
            str(code).upper() for code in self.service.store.preferences()["l2_codes"]
        }
        previous_header = self.service.store.snapshot_header("board_indexes") or {}
        previous_meta = previous_header.get("meta") or {}
        previous_quality = previous_meta.get("quality") or {}
        current_fingerprint = state.snapshot_fingerprints.get("board_indexes", "")
        resume_details: dict[str, dict[str, Any]] = {}
        previous_summary = (previous_header.get("data") or {}).get("summary") or {}
        if (
            str(previous_meta.get("input_fingerprint") or "") == current_fingerprint
            and int(previous_summary.get("pending_board_count") or 0) > 0
        ):
            previous_snapshot = self.service.store.snapshot("board_indexes") or {}
            resume_details = dict(
                (previous_snapshot.get("data") or {}).get("details") or {}
            )

        allow_checkpoint_publish = str(previous_quality.get("status") or "") != "complete"

        def publish_checkpoint(
            checkpoint_data: dict[str, Any], checkpoint_quality: dict[str, Any],
        ) -> None:
            if not allow_checkpoint_publish:
                return
            envelope = self.service._envelope(
                checkpoint_data,
                snapshot_id="",
                generated_at=state.generated_at,
                quality=checkpoint_quality,
                sources=["free-stockdb:boards", "free-stockdb:zhishu"],
                expected_as_of=state.expected_as_of,
                purpose=state.spec.purpose,
            )
            digest = hashlib.sha256(
                strict_json_dumps(checkpoint_data, sort_keys=True).encode("utf-8")
            ).hexdigest()
            checkpoint_id = _snapshot_id(
                state.as_of, [digest], "board_indexes",
            )
            envelope["meta"].update({
                "snapshot_id": checkpoint_id,
                "batch_id": checkpoint_id,
                "schema_version": 2,
                "input_fingerprint": current_fingerprint,
                "board_index_algorithm_version": BOARD_INDEX_ALGORITHM_VERSION,
            })
            self.service.store.save_snapshots({"board_indexes": envelope})
            state.checkpoint_node("board_indexes_batch", {
                "as_of": state.as_of,
                "boards": len(checkpoint_data.get("items") or []),
                "pending": int(
                    (checkpoint_data.get("summary") or {}).get("pending_board_count") or 0
                ),
                "snapshot_id": checkpoint_id,
                "input_fingerprint": current_fingerprint,
            })
        try:
            from quantmaster.data.free_stockdb_source import FreeStockDBSource

            data, quality = build_board_index_data(
                FreeStockDBSource(),
                close=state.close,
                amount=state.amount,
                names=state.names,
                as_of=state.as_of,
                selected_l2=selected_l2,
                theme_codes=theme_codes,
                progress=state.progress,
                cancelled=state.cancelled,
                checkpoint=publish_checkpoint,
                resume_details=resume_details,
            )
        except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
            if self.service.store.snapshot_header("board_indexes") is not None:
                state.provider_warnings.append(
                    f"板块指数刷新失败，继续保留旧快照：{str(exc)[:160]}"
                )
                return
            issue = f"本地 StockDB 板块指数不可用：{str(exc)[:160]}"
            data = {
                "as_of": state.as_of,
                "items": [],
                "details": {},
                "summary": {
                    "board_count": 0, "expected_board_count": 0,
                    "pending_board_count": 0, "method_count": 5,
                    "unavailable_method_count": 0,
                },
                "definition": {
                    "methods": ["equal", "float_mv", "amount", "volume", "total_mv"],
                    "membership_semantics": "current_constituents_backcast",
                    "frequency": "1d", "base": 1000.0, "sessions": 120,
                    "algorithm_version": BOARD_INDEX_ALGORITHM_VERSION,
                    "initial_seed_row": "removed_by_stockdb_zb_get",
                },
            }
            quality = {
                "status": "cold", "eligible_count": 0, "expected_count": 0,
                "coverage": None, "issues": [issue],
            }
        state.computed["board_indexes"] = self.service._envelope(
            data,
            snapshot_id=state.snapshot_id,
            generated_at=state.generated_at,
            quality=quality,
            sources=["free-stockdb:boards", "free-stockdb:zhishu"],
            expected_as_of=state.expected_as_of,
            purpose=state.spec.purpose,
        )
        state.computed["board_indexes"]["meta"][
            "board_index_algorithm_version"
        ] = BOARD_INDEX_ALGORITHM_VERSION
        state.checkpoint_node("board_indexes", {
            "as_of": state.as_of,
            "boards": len(data.get("items") or []),
            "input_fingerprint": state.snapshot_fingerprints.get("board_indexes", ""),
        })

    def _etf_sources(self, data: dict[str, Any]) -> list[str]:
        state = self.state
        price_sources = {
            str(item.get("price_source") or "") for item in data["items"]
        }
        sources = ["tushare:fund_share"]
        if "nav" in price_sources:
            sources.append("tushare:fund_nav")
        if "close" in price_sources:
            sources.append("tushare:fund_daily")
        if state.etf_price_source:
            sources.append(state.etf_price_source)
        sources.append("local:rotation_cache")
        return sources

    def _compute_etf_flows(self) -> None:
        state = self.state
        if "etf_flows" not in state.compute_kinds:
            return
        state.progress(state.compute_base + 21, "估算宽基资金", "按份额变化与净值计算申赎资金")
        data = estimate_etf_flows(state.etf_observations)
        ready = data["summary"].get("status") == "ready"
        fallback_count = int(data["summary"].get("close_fallback_count") or 0)
        quality = _status_quality(
            "partial" if ready and fallback_count else "complete" if ready else "cold",
            eligible=len(data["items"]),
            expected=len(data["items"]),
            issues=[
                *([] if ready else ["等待 09:05 后的 ETF 份额快照"]),
                *(
                    [f"{fallback_count} 只宽基 ETF 缺少单位净值，已使用收盘价"]
                    if fallback_count else []
                ),
                *state.provider_issues["etf"],
            ],
        )
        snapshot_id = _snapshot_id(
            data.get("as_of") or state.as_of,
            [str(item.get("symbol") or "") for item in data["items"]],
            "etf",
        )
        state.computed["etf_flows"] = self.service._envelope(
            data,
            snapshot_id=snapshot_id,
            generated_at=state.generated_at,
            quality=quality,
            sources=self._etf_sources(data),
            purpose=state.spec.purpose,
            remote_fills=int(bool(state.provider_results.get("etf"))),
        )
        state.checkpoint_node("etf", {
            "as_of": str(data.get("as_of") or state.as_of),
            "items": len(data.get("items") or []),
            "input_fingerprint": state.snapshot_fingerprints.get("etf_flows", ""),
        })

    def _finalize_snapshot_metadata(self) -> None:
        state = self.state
        digests = {
            kind: hashlib.sha256(
                strict_json_dumps(payload.get("data") or {}, sort_keys=True).encode("utf-8")
            ).hexdigest()
            for kind, payload in sorted(state.computed.items())
        }
        batch_id = _snapshot_id(
            state.as_of,
            [f"{kind}:{digest}" for kind, digest in sorted(digests.items())],
            "batch",
        )
        for kind, payload in state.computed.items():
            meta = payload["meta"]
            meta["snapshot_id"] = _snapshot_id(
                str(meta.get("as_of") or ""), [digests[kind]], kind,
            )
            meta["batch_id"] = batch_id
            meta["schema_version"] = 2
            meta["input_fingerprint"] = state.snapshot_fingerprints.get(
                kind, state.input_fingerprint,
            )
            quality = meta.get("quality") or {}
            meta["stale"] = str(quality.get("status") or "") == "stale"
            meta["stale_reasons"] = (
                list(quality.get("issues") or []) if meta["stale"] else []
            )

    def _published_identity(self) -> tuple[str, str]:
        state = self.state
        headers = [
            self.service.store.snapshot_header(kind) or {}
            for kind in state.scope_snapshot_kinds
        ]
        as_of = max(
            (str((header.get("meta") or {}).get("as_of") or "") for header in headers),
            default=state.as_of,
        )
        snapshot_id = _snapshot_id(
            as_of,
            [
                str((header.get("meta") or {}).get("snapshot_id") or "")
                for header in headers
            ],
            state.scope,
        )
        return snapshot_id, as_of

    def _publish(self) -> dict[str, Any]:
        state = self.state
        if state.cancelled():
            raise InterruptedError("板块联动刷新已取消")
        self._finalize_snapshot_metadata()
        state.progress(96, "提交分析快照", "原子更新页面所需视图")
        with self.metrics.node_timer(
            "rotation.publish", job_id=state.job_id,
            input_fingerprint=state.input_fingerprint,
        ) as dimensions:
            self.service.store.save_snapshots(state.computed)
            dimensions["output_rows"] = sum(
                len((payload.get("data") or {}).get("items") or [])
                for payload in state.computed.values()
            )
        state.checkpoint_node("published_snapshots", {
            "kinds": sorted(state.computed),
            "input_fingerprint": state.input_fingerprint,
        })
        changed = sorted(
            kind for kind, payload in state.computed.items()
            if str(payload.get("meta", {}).get("snapshot_id") or "")
            != state.previous_snapshot_ids.get(kind, "")
        )
        non_complete = [
            kind for kind, payload in state.computed.items()
            if str(payload.get("meta", {}).get("quality", {}).get("status") or "")
            != "complete"
        ]
        outcome = "unchanged" if not changed else "partial" if non_complete else "updated"
        snapshot_id, published_as_of = self._published_identity()
        return {
            "snapshot_id": snapshot_id,
            "as_of": published_as_of,
            "expected_as_of": state.expected_as_of,
            "fresh": not state.expected_as_of or published_as_of >= state.expected_as_of,
            "outcome": outcome,
            "updated": changed,
            "computed": sorted(state.computed),
            "warnings": list(dict.fromkeys(state.provider_warnings)),
            "tracked_count": len(state.close.columns)
            or int(state.local_state.get("expected_count") or 0),
            "expected_count": state.expected_count,
            "input_fingerprint": state.input_fingerprint,
        }


ROTATION_TASK_TYPE = "rotation.refresh"


def _record_rotation_scheduled_result(
    store: RotationStore,
    spec: RotationJobSpec,
    *,
    succeeded: bool,
) -> None:
    """Advance the scheduler marker only after an owned job has published."""

    if spec.source != "auto" or spec.scope not in {"close", "etf"}:
        return
    kind = "close" if spec.scope == "close" else "etf"
    date_key = str(
        spec.as_of
        or _expected_market_session()
        or datetime.now(ZoneInfo("Asia/Shanghai")).date()
    )
    retry_key = f"scheduled_{kind}_retry"
    if succeeded:
        store.set_runtime_state(f"scheduled_{kind}", date_key)
        store.set_runtime_state(retry_key, "")
        return
    value = store.runtime_state(retry_key)
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
    store.set_runtime_state(retry_key, f"{date_key}|{attempt}|{next_at}")


def _write_rotation_checkpoint(context: Any, node: str, payload: dict[str, Any]) -> None:
    """Persist resumable DAG boundaries without serialising large matrices."""

    context.write_checkpoint(
        f"rotation.{node}",
        context.spec_hash,
        {
            "schema_version": "1.0",
            "node": node,
            "submission_input_fingerprint": context.input_fingerprint,
            "payload": payload,
        },
    )


def _execute_rotation_refresh(
    service: RotationService,
    context: Any,
    spec_values: dict[str, Any],
) -> JobOutcome:
    """One lease-fenced rotation attempt, shared by local and spawn handlers."""

    spec = RotationJobSpec.model_validate(spec_values)
    previous = context.load_checkpoint("rotation.publish", context.spec_hash)
    if (
        isinstance(previous, dict)
        and str(previous.get("submission_input_fingerprint") or "")
        == context.input_fingerprint
        and isinstance(previous.get("payload"), dict)
        and isinstance(previous["payload"].get("result"), dict)
    ):
        result = dict(previous["payload"]["result"])
        context.progress(96, "恢复已发布快照", "复用崩溃前已完成的发布检查点")
    else:
        _write_rotation_checkpoint(context, "source", {
            "scope": spec.scope,
            "mode": spec.mode,
            "source": spec.source,
        })
        result = service.build(
            spec,
            progress=context.progress,
            cancelled=context.cancelled,
            job_id=context.job_id,
            checkpoint=lambda node, payload: _write_rotation_checkpoint(context, node, payload),
        )
        _write_rotation_checkpoint(context, "publish", {"result": result})
    artifact = context.write_artifact(
        "rotation.refresh.result",
        result,
        {
            "schema_version": "2.0",
            "lineage": {
                "snapshot_id": str(result.get("snapshot_id") or ""),
                "input_fingerprint": str(result.get("input_fingerprint") or ""),
                "algorithm_version": ALGORITHM_VERSION,
            },
        },
    )
    succeeded = bool(
        not result.get("warnings")
        and result.get("fresh", True)
        and (
            spec.scope != "etf"
            or result.get("outcome") in {"updated", "unchanged"}
        )
    )
    _record_rotation_scheduled_result(service.store, spec, succeeded=succeeded)
    detail = "；".join(str(item) for item in result.get("warnings") or [])[:1000]
    return JobOutcome("completed", detail or "板块联动刷新完成", artifact["id"])


def run_rotation_refresh_job(context: Any, spec: dict[str, Any]) -> JobOutcome:
    """Spawn-safe rotation compute entrypoint; it deliberately owns no lease."""

    return _execute_rotation_refresh(RotationService(), context, spec)


class RotationWorker:
    """Thin scheduler around the unified ledger and optional spawned compute.

    The scheduler thread only checks due times and submits canonical jobs.  It
    never calculates a matrix; ``UnifiedJobRuntime`` owns the lease and runs
    the registered entrypoint in a Windows ``spawn`` child in production.
    """

    def __init__(
        self,
        service: RotationService | None = None,
        runtime: UnifiedJobRuntime | None = None,
        *,
        isolated: bool | None = None,
    ):
        supplied_service = service is not None
        self.service = service or RotationService()
        self.runtime = runtime or UnifiedJobRuntime(self.service.jobs, max_workers=1)
        self.identity = self.runtime.identity
        self._isolated = (not supplied_service) if isolated is None else bool(isolated)
        self.runtime.register(
            ROTATION_TASK_TYPE,
            self._handle,
            process_entrypoint=(
                "quantmaster.rotation.service:run_rotation_refresh_job"
                if self._isolated else ""
            ),
        )
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self, *, bootstrap_local: bool = False) -> None:
        self.runtime.start()
        if not self.runtime.dispatch_enabled:
            # This is a Web-side client attached to an external Supervisor.
            # It may submit jobs, but it never owns the periodic scheduler.
            return
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
        if self.runtime.dispatch_enabled:
            self.runtime.pause()

    def shutdown(self, timeout: float = 10.0) -> None:
        """Permanently dispose the Supervisor-owned runtime on process exit."""

        self.stop(timeout)
        self.runtime.stop()

    @property
    def idle(self) -> bool:
        return self.runtime.idle

    def submit(self, spec: RotationJobSpec) -> dict[str, Any]:
        input_fingerprint, _state = self.service.input_fingerprint(spec)
        payload = spec.model_dump(mode="json")
        if spec.purpose == "current_analysis" and not spec.knowledge_cutoff and not spec.taxonomy_id:
            for key in ("purpose", "knowledge_cutoff", "taxonomy_id"):
                payload.pop(key, None)
        job, _created = self.runtime.submit(
            ROTATION_TASK_TYPE,
            payload,
            input_fingerprint=input_fingerprint,
            algorithm_version=ALGORITHM_VERSION,
            deadline_seconds=3600,
            max_attempts=2,
        )
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
        _record_rotation_scheduled_result(self.service.store, spec, succeeded=succeeded)

    def _scheduled(self) -> None:
        now = datetime.now(ZoneInfo("Asia/Shanghai"))
        if now.weekday() >= 5:
            return
        date_key = _expected_market_session()
        if not date_key:
            return
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

    def _handle(self, context: JobContext, spec: dict[str, Any]) -> JobOutcome:
        try:
            return _execute_rotation_refresh(self.service, context, spec)
        except (InterruptedError, OSError, RuntimeError, TypeError, ValueError):
            try:
                parsed = RotationJobSpec.model_validate(spec)
            except (TypeError, ValueError):
                parsed = None
            if parsed is not None:
                self._record_scheduled_result(parsed, succeeded=False)
            raise

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
            self._wake.wait(1.0)
            self._wake.clear()


_SERVICE: RotationService | None = None
_READ_SERVICE: RotationService | None = None
_WORKER: RotationWorker | None = None
_SINGLETON_LOCK = threading.RLock()


def get_rotation_service(*, read_only: bool = False) -> RotationService:
    global _SERVICE, _READ_SERVICE
    with _SINGLETON_LOCK:
        if read_only:
            if _READ_SERVICE is None:
                _READ_SERVICE = RotationService(read_only=True)
            return _READ_SERVICE
        if _SERVICE is None:
            _SERVICE = RotationService()
        return _SERVICE


def get_rotation_worker() -> RotationWorker:
    global _WORKER
    with _SINGLETON_LOCK:
        if _WORKER is None:
            _WORKER = RotationWorker(get_rotation_service(), isolated=True)
        return _WORKER


def reset_rotation_runtime_for_tests() -> None:
    global _SERVICE, _READ_SERVICE, _WORKER
    with _SINGLETON_LOCK:
        if _WORKER is not None:
            _WORKER.shutdown(2.0)
        _WORKER = None
        _SERVICE = None
        _READ_SERVICE = None
