"""Reusable point-in-time index membership backed by the research lake."""

from __future__ import annotations

from typing import Any

import pandas as pd

from quantmaster.research.contracts import (
    ArtifactKind,
    AssetClass,
    Frequency,
    content_hash,
)
from quantmaster.research.lake import ResearchLake

CSI800_INDEXES = ("000300.SH", "000905.SH")
CSI800_DATASET = "csi800_membership"


def _normalize(records: pd.DataFrame | None) -> pd.DataFrame:
    columns = ["trade_date", "symbol", "index_code", "weight", "source"]
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
    return frame[columns].dropna(
        subset=["trade_date", "symbol", "index_code"],
    ).drop_duplicates(
        subset=["trade_date", "symbol", "index_code"], keep="last",
    ).sort_values(["trade_date", "index_code", "symbol"], kind="mergesort")


def cache_csi800_records(records: pd.DataFrame, *, lake: ResearchLake | None = None) -> int:
    """Atomically merge monthly CSI800 snapshots into date partitions."""
    frame = _normalize(records)
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
        combined = _normalize(pd.concat((existing, group), ignore_index=True))
        storage = combined.rename(columns={"symbol": "component_symbol"}).copy()
        storage["symbol"] = (
            storage["index_code"].astype(str) + ":" + storage["component_symbol"].astype(str)
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
) -> pd.DataFrame:
    """Read cached PIT membership and optionally refresh it through Tushare."""
    target = lake or ResearchLake()
    if pull:
        if source is None:
            from quantmaster.data.tushare_source import TushareSource

            source = TushareSource()
        fetched = [source.index_weights(code, start, end) for code in CSI800_INDEXES]
        nonempty = [item for item in fetched if item is not None and not item.empty]
        if nonempty:
            cache_csi800_records(pd.concat(nonempty, ignore_index=True), lake=target)
    return _normalize(target.read_range(
        ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
        CSI800_DATASET, start, end,
    ))


def load_cached_csi800_members_as_of(
    as_of: str,
    *,
    pull: bool = True,
    source=None,
    lake: ResearchLake | None = None,
) -> dict[str, Any]:
    """Return the last known snapshot of both CSI800 component indexes."""
    target = pd.Timestamp(as_of).normalize()
    if pd.isna(target):
        raise ValueError("查看日期需要使用 YYYY-MM-DD 格式")
    start = (target - pd.DateOffset(months=12)).replace(day=1)
    frame = load_cached_csi800_records(
        start.strftime("%Y-%m-%d"), target.strftime("%Y-%m-%d"),
        pull=pull, source=source, lake=lake,
    )
    members: set[str] = set()
    snapshot_dates: dict[str, str] = {}
    for index_code in CSI800_INDEXES:
        subset = frame.loc[
            (frame["index_code"] == index_code)
            & (frame["trade_date"] <= target)
        ]
        if subset.empty:
            raise RuntimeError(f"{index_code} 在 {target.date()} 前没有已缓存点时成分")
        latest = pd.Timestamp(subset["trade_date"].max()).normalize()
        current = subset.loc[subset["trade_date"] == latest]
        members.update(current["symbol"].dropna().astype(str))
        snapshot_dates[index_code] = latest.strftime("%Y-%m-%d")
    symbols = sorted(members)
    if not symbols:
        raise RuntimeError("中证800点时成分缓存为空")
    return {
        "as_of": target.strftime("%Y-%m-%d"),
        "symbols": symbols,
        "snapshot_dates": snapshot_dates,
        "dataset": CSI800_DATASET,
        "source": "research_lake:tushare:index_weight",
        "content_hash": content_hash({
            "symbols": symbols, "snapshot_dates": snapshot_dates,
        }),
    }
