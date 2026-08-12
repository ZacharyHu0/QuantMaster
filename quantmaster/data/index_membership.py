"""Reusable point-in-time index membership backed by the research lake."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from quantmaster.research.contracts import (
    ArtifactKind,
    AssetClass,
    Frequency,
    content_hash,
)
from quantmaster.research.lake import ResearchLake
from quantmaster.trading_sessions import daily_signal_cutoff

CSI800_INDEXES = ("000300.SH", "000905.SH")
CSI800_DATASET = "csi800_membership"
MAX_CSI800_SNAPSHOT_AGE_DAYS = 45
EXPECTED_INDEX_MEMBERS = {"000300.SH": 300, "000905.SH": 500}
MIN_INDEX_COMPLETENESS = 1.0
logger = logging.getLogger(__name__)


def _normalize(records: pd.DataFrame | None) -> pd.DataFrame:
    columns = [
        "trade_date", "symbol", "index_code", "weight", "source",
        "published_at", "acquired_at", "temporal_quality",
        "snapshot_id", "snapshot_member_count", "snapshot_expected_count",
    ]
    if records is None or records.empty:
        return pd.DataFrame(columns=columns)
    frame = records.copy()
    if "component_symbol" in frame:
        frame["symbol"] = frame["component_symbol"]
    for column in columns:
        if column not in frame:
            frame[column] = "tushare:index_weight" if column == "source" else pd.NA
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.normalize()
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    frame["index_code"] = frame["index_code"].astype(str).str.upper()
    frame["weight"] = pd.to_numeric(frame["weight"], errors="coerce")
    frame["published_at"] = pd.to_datetime(
        frame["published_at"], errors="coerce", utc=True,
    )
    frame["acquired_at"] = pd.to_datetime(
        frame["acquired_at"], errors="coerce", utc=True,
    )
    frame["snapshot_member_count"] = pd.to_numeric(
        frame["snapshot_member_count"], errors="coerce",
    ).astype("Int64")
    frame["snapshot_expected_count"] = pd.to_numeric(
        frame["snapshot_expected_count"], errors="coerce",
    ).astype("Int64")
    return frame[columns].dropna(
        subset=["trade_date", "symbol", "index_code"],
    ).drop_duplicates(
        subset=["trade_date", "symbol", "index_code", "acquired_at"], keep="last",
    ).sort_values(
        ["trade_date", "index_code", "acquired_at", "symbol"], kind="mergesort",
    )


def _with_temporal_evidence(
    records: pd.DataFrame, *, acquired_at: str | datetime | None = None,
) -> pd.DataFrame:
    frame = records.copy()
    observed = acquired_at or datetime.now(UTC)
    if "acquired_at" not in frame:
        frame["acquired_at"] = observed
    else:
        frame["acquired_at"] = frame["acquired_at"].where(
            frame["acquired_at"].notna(), observed,
        )
    if "published_at" not in frame:
        frame["published_at"] = pd.NaT
    published = pd.to_datetime(frame["published_at"], errors="coerce", utc=True)
    frame["published_at"] = published
    default_quality = pd.Series(
        ["published_and_acquired" if pd.notna(value) else "acquired_only" for value in published],
        index=frame.index,
    )
    if "temporal_quality" not in frame:
        frame["temporal_quality"] = default_quality
    else:
        frame["temporal_quality"] = frame["temporal_quality"].where(
            frame["temporal_quality"].astype(str).str.strip().ne(""), default_quality,
        )
    return frame


def _validated_snapshot_records(records: pd.DataFrame) -> pd.DataFrame:
    frame = _normalize(records)
    accepted: list[pd.DataFrame] = []
    for (index_code, trade_date, acquired_at), group in frame.groupby(
        ["index_code", "trade_date", "acquired_at"], dropna=False, sort=True,
    ):
        if pd.isna(acquired_at):
            logger.warning("拒绝无 acquired_at 的指数成分批次: %s/%s", index_code, trade_date)
            continue
        symbols = sorted(set(group["symbol"].dropna().astype(str)))
        declared = group["snapshot_expected_count"].dropna()
        authoritative_expected = EXPECTED_INDEX_MEMBERS.get(str(index_code))
        declared_values = set(declared.astype(int))
        if authoritative_expected is not None:
            if declared_values and declared_values != {authoritative_expected}:
                logger.warning(
                    "拒绝指数成分批次 %s/%s：上游自报期望数 %s 与权威口径 %s 不一致",
                    index_code, trade_date, sorted(declared_values), authoritative_expected,
                )
                continue
            expected = int(authoritative_expected)
        else:
            if len(declared_values) > 1:
                logger.warning("拒绝期望成分数自相矛盾的批次 %s/%s", index_code, trade_date)
                continue
            expected = next(iter(declared_values), len(symbols))
        minimum = max(1, int(expected * MIN_INDEX_COMPLETENESS + 0.999999))
        if len(symbols) != expected:
            logger.warning(
                "拒绝不完整指数成分批次 %s/%s：实得 %s，要求 %s（期望 %s）",
                index_code, trade_date, len(symbols), minimum, expected,
            )
            continue
        snapshot_id = content_hash({
            "index_code": str(index_code),
            "trade_date": pd.Timestamp(trade_date).strftime("%Y-%m-%d"),
            "acquired_at": pd.Timestamp(acquired_at).isoformat(),
            "symbols": symbols,
        })
        value = group.loc[group["symbol"].isin(symbols)].copy()
        value["snapshot_id"] = snapshot_id
        value["snapshot_member_count"] = len(symbols)
        value["snapshot_expected_count"] = expected
        accepted.append(value)
    return _normalize(pd.concat(accepted, ignore_index=True)) if accepted else _normalize(None)


def cache_csi800_records(
    records: pd.DataFrame, *, lake: ResearchLake | None = None,
    acquired_at: str | datetime | None = None,
) -> int:
    """Atomically merge monthly CSI800 snapshots into date partitions."""
    frame = _validated_snapshot_records(
        _with_temporal_evidence(records, acquired_at=acquired_at)
    )
    if frame.empty:
        return 0
    target = lake or ResearchLake()
    written = 0
    for stamp, group in frame.groupby("trade_date", sort=True):
        trade_date = pd.Timestamp(stamp).strftime("%Y-%m-%d")
        existing = target.read_partition(
            ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
            CSI800_DATASET, trade_date,
        )
        if not existing.empty:
            existing = _normalize(existing).dropna(subset=["acquired_at", "snapshot_id"])
        combined = _normalize(pd.concat((existing, group), ignore_index=True))
        storage = combined.rename(columns={"symbol": "component_symbol"}).copy()
        observed_key = storage["acquired_at"].dt.strftime("%Y%m%dT%H%M%S%fZ")
        storage["symbol"] = (
            storage["index_code"].astype(str) + ":"
            + storage["component_symbol"].astype(str) + ":" + observed_key
        )
        target.write_partition(
            ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
            CSI800_DATASET, trade_date, storage,
            input_hashes={"membership": content_hash(combined.to_dict("records"))},
            run_id=f"csi800:{trade_date}",
        )
        written += len(group)
    return written


def load_cached_csi800_records(
    start: str,
    end: str,
    *,
    pull: bool = True,
    source=None,
    lake: ResearchLake | None = None,
    max_snapshot_age_days: int = MAX_CSI800_SNAPSHOT_AGE_DAYS,
) -> pd.DataFrame:
    """Read local PIT evidence first and refresh only missing/stale indexes."""
    target = lake or ResearchLake()
    lake_records = _normalize(target.read_range(
        ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
        CSI800_DATASET, start, end,
    ))
    local = lake_records
    requested = pd.Timestamp(end).normalize()
    cutoff = pd.Timestamp(daily_signal_cutoff(requested.date())).tz_convert("UTC")
    available_at = local["published_at"].where(
        local["published_at"].notna(), local["acquired_at"],
    )
    eligible = local.loc[
        local["acquired_at"].notna()
        & (local["acquired_at"] <= cutoff)
        & available_at.notna()
        & (available_at <= cutoff)
        & local["snapshot_id"].notna()
        & local["snapshot_id"].astype(str).str.strip().ne("")
    ]
    refresh_indexes = []
    for index_code in CSI800_INDEXES:
        subset = eligible.loc[eligible["index_code"] == index_code]
        latest = subset["trade_date"].max() if not subset.empty else pd.NaT
        if pd.isna(latest) or int((requested - pd.Timestamp(latest)).days) > max_snapshot_age_days:
            refresh_indexes.append(index_code)
    if pull and refresh_indexes:
        try:
            if source is None:
                from quantmaster.data.tushare_source import TushareSource

                source = TushareSource()
            fetched = [source.index_weights(code, start, end) for code in refresh_indexes]
            nonempty = [item for item in fetched if item is not None and not item.empty]
            if nonempty:
                cache_csi800_records(pd.concat(nonempty, ignore_index=True), lake=target)
                lake_records = _normalize(target.read_range(
                    ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
                    CSI800_DATASET, start, end,
                ))
                local = lake_records
        except (ImportError, OSError, RuntimeError, TypeError, ValueError):
            logger.warning(
                "中证800远端补缺失败；保留本地证据并由后续时点/新鲜度门禁裁决",
                exc_info=True,
            )
    return local


def load_cached_csi800_members_as_of(
    as_of: str,
    *,
    pull: bool = True,
    source=None,
    lake: ResearchLake | None = None,
    max_snapshot_age_days: int = MAX_CSI800_SNAPSHOT_AGE_DAYS,
) -> dict[str, Any]:
    """Return evidenced membership known at ``as_of`` or fail closed.

    ``requested_as_of`` is the caller's decision date. ``effective_as_of`` is
    the oldest constituent snapshot required to form the union and therefore
    the conservative evidence date for the result.
    """
    try:
        target = pd.Timestamp(as_of).normalize()
    except (TypeError, ValueError) as exc:
        raise ValueError("查看日期需要使用 YYYY-MM-DD 格式") from exc
    if pd.isna(target) or max_snapshot_age_days < 0:
        raise ValueError("查看日期需要使用 YYYY-MM-DD 格式")
    start = (target - pd.DateOffset(months=12)).replace(day=1)
    frame = load_cached_csi800_records(
        start.strftime("%Y-%m-%d"), target.strftime("%Y-%m-%d"),
        pull=pull, source=source, lake=lake,
        max_snapshot_age_days=max_snapshot_age_days,
    )
    cutoff = pd.Timestamp(daily_signal_cutoff(target.date())).tz_convert("UTC")
    available_at = frame["published_at"].where(
        frame["published_at"].notna(), frame["acquired_at"],
    )
    frame = frame.loc[
        frame["acquired_at"].notna()
        & (frame["acquired_at"] <= cutoff)
        & available_at.notna()
        & (available_at <= cutoff)
        & frame["snapshot_id"].notna()
        & frame["snapshot_id"].astype(str).str.strip().ne("")
    ].copy()
    members: set[str] = set()
    snapshot_dates: dict[str, str] = {}
    snapshot_published_at: dict[str, str] = {}
    snapshot_acquired_at: dict[str, str] = {}
    snapshot_temporal_quality: dict[str, str] = {}
    snapshot_ids: dict[str, str] = {}
    index_members: dict[str, set[str]] = {}
    lag_days: dict[str, int] = {}
    for index_code in CSI800_INDEXES:
        subset = frame.loc[
            (frame["index_code"] == index_code)
            & (frame["trade_date"] <= target)
        ]
        if subset.empty:
            raise RuntimeError(
                f"{index_code} 在 {target.date()} 上海 15:00 前没有同时满足"
                " published_at/acquired_at 的点时成分证据"
            )
        latest = pd.Timestamp(subset["trade_date"].max()).normalize()
        current = subset.loc[subset["trade_date"] == latest]
        latest_acquired = current["acquired_at"].max()
        current = current.loc[current["acquired_at"] == latest_acquired]
        snapshot_values = set(current["snapshot_id"].dropna().astype(str))
        counts = set(current["snapshot_member_count"].dropna().astype(int))
        expected_counts = set(current["snapshot_expected_count"].dropna().astype(int))
        actual_count = current["symbol"].nunique()
        authoritative_expected = EXPECTED_INDEX_MEMBERS.get(index_code, actual_count)
        if (
            len(snapshot_values) != 1
            or counts != {actual_count}
            or expected_counts != {authoritative_expected}
            or actual_count != authoritative_expected
        ):
            raise RuntimeError(f"{index_code} 的指数成分快照完整性清单不一致")
        current_members = set(current["symbol"].dropna().astype(str))
        members.update(current_members)
        index_members[index_code] = current_members
        snapshot_dates[index_code] = latest.strftime("%Y-%m-%d")
        published = current["published_at"].max()
        snapshot_published_at[index_code] = published.isoformat() if pd.notna(published) else ""
        snapshot_acquired_at[index_code] = latest_acquired.isoformat()
        snapshot_temporal_quality[index_code] = str(current["temporal_quality"].iloc[-1])
        snapshot_ids[index_code] = next(iter(snapshot_values))
        lag_days[index_code] = int((target - latest).days)
    overlap = set.intersection(*(index_members[index] for index in CSI800_INDEXES))
    if overlap:
        raise RuntimeError(
            f"中证800子指数成分快照存在 {len(overlap)} 个重叠标的，不能组成互斥800母集"
        )
    expected_union = sum(EXPECTED_INDEX_MEMBERS[index] for index in CSI800_INDEXES)
    if len(members) != expected_union:
        raise RuntimeError(
            f"中证800子指数并集仅 {len(members)}/{expected_union}，完整性清单不一致"
        )
    stale = {
        index_code: days
        for index_code, days in lag_days.items()
        if days > max_snapshot_age_days
    }
    if stale:
        detail = "、".join(
            f"{code}={snapshot_dates[code]}（滞后 {days} 天）"
            for code, days in stale.items()
        )
        raise RuntimeError(
            f"中证800点时成分快照相对请求日 {target.date()} 过旧：{detail}；"
            "请先刷新 index_weight 证据"
        )
    symbols = sorted(members)
    if not symbols:
        raise RuntimeError("中证800点时成分缓存为空")
    return {
        "requested_as_of": target.strftime("%Y-%m-%d"),
        "effective_as_of": min(snapshot_dates.values()),
        "status": "ready",
        "symbols": symbols,
        "snapshot_dates": snapshot_dates,
        "snapshot_published_at": snapshot_published_at,
        "snapshot_acquired_at": snapshot_acquired_at,
        "snapshot_temporal_quality": snapshot_temporal_quality,
        "snapshot_ids": snapshot_ids,
        "lag_days": lag_days,
        "max_snapshot_age_days": int(max_snapshot_age_days),
        "dataset": CSI800_DATASET,
        "source": "research_lake:tushare:index_weight",
        "content_hash": content_hash({
            "symbols": symbols, "snapshot_dates": snapshot_dates,
            "snapshot_published_at": snapshot_published_at,
            "snapshot_acquired_at": snapshot_acquired_at,
            "snapshot_temporal_quality": snapshot_temporal_quality,
            "snapshot_ids": snapshot_ids,
        }),
    }
