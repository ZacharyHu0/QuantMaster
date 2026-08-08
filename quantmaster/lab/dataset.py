"""Point-in-time 候选、数据就绪检查与可复现实验快照。"""

from __future__ import annotations

import hashlib
import threading
from collections import OrderedDict
from collections.abc import Iterable
from datetime import date as calendar_date
from typing import Any, Literal, cast

import numpy as np
import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.index_membership import (
    load_cached_csi800_members_as_of,
    load_cached_csi800_records,
    load_legacy_csi800_records,
)
from quantmaster.lab.models import DatasetSnapshot, content_hash

_PANEL_CACHE_LOCK = threading.RLock()
_PANEL_CACHE: OrderedDict[
    str, tuple[dict[str, pd.DataFrame], pd.DataFrame | None, dict[str, Any], int]
] = OrderedDict()
_PANEL_CACHE_BYTES = 0
_INSPECTION_CACHE: OrderedDict[
    tuple[str, str, str, str], tuple[dict[str, Any], str, tuple[int, int, int]]
] = OrderedDict()


def build_membership_mask(records: pd.DataFrame, calendar: Iterable) -> pd.DataFrame:
    """把月度指数成分快照扩展为逐交易日可用掩码。

    每个指数独立沿用最近一个已公布快照，再取指数之间的并集。这样沪深300
    和中证500在不同日期更新时，不会暂时丢失另一半候选。
    """
    dates = pd.DatetimeIndex(pd.to_datetime(list(calendar))).normalize().unique().sort_values()
    if records is None or records.empty or dates.empty:
        return pd.DataFrame(index=dates, dtype=bool)
    frame = records.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.normalize()
    if "index_code" not in frame:
        frame["index_code"] = "index"
    snapshots: dict[str, list[tuple[pd.Timestamp, tuple[str, ...]]]] = {}
    for (code, snapshot_date), group in frame.groupby(["index_code", "trade_date"], sort=True):
        snapshots.setdefault(str(code), []).append(
            (pd.Timestamp(snapshot_date), tuple(group["symbol"].dropna().astype(str)))
        )
    all_symbols = sorted(set(frame["symbol"].dropna().astype(str)))
    symbol_positions = {symbol: position for position, symbol in enumerate(all_symbols)}
    array: Any = np.zeros((len(dates), len(all_symbols)), dtype=bool)
    for index_snapshots in snapshots.values():
        for position, (snapshot_date, members) in enumerate(index_snapshots):
            row_start = int(dates.searchsorted(snapshot_date, side="left"))
            row_end = (
                int(dates.searchsorted(index_snapshots[position + 1][0], side="left"))
                if position + 1 < len(index_snapshots) else len(dates)
            )
            if row_start >= row_end or row_start >= len(dates):
                continue
            member_positions = [
                symbol_positions[symbol] for symbol in members if symbol in symbol_positions
            ]
            if member_positions:
                array[row_start:row_end, member_positions] = True
    return pd.DataFrame(array, index=dates, columns=all_symbols, dtype=bool)


def load_csi800_membership(
    start: str,
    end: str,
    *,
    calendar: Iterable | None = None,
    source=None,
) -> pd.DataFrame:
    """加载中证800历史成分；需要 Tushare 2000 积分或注入兼容数据源。"""
    if source is None:
        from quantmaster.data.tushare_source import TushareSource

        source = TushareSource()
    frame = load_cached_csi800_records(start, end, source=source)
    if frame.empty:
        raise RuntimeError("中证800历史成分为空；请检查 Tushare token 与 index_weight 权限")
    if calendar is None:
        calendar = source.trade_calendar(start, end)
    return build_membership_mask(frame, calendar)


def _cached_membership_records(start: str, end: str) -> pd.DataFrame:
    """Read enough cached PIT snapshots to establish membership at ``start``."""
    beginning = (pd.Timestamp(start) - pd.DateOffset(months=12)).strftime("%Y-%m-%d")
    lake = load_cached_csi800_records(beginning, end, pull=False)
    legacy = load_legacy_csi800_records(beginning, end)
    if legacy.empty:
        return lake
    return pd.concat((legacy, lake), ignore_index=True).drop_duplicates(
        subset=["trade_date", "symbol", "index_code"], keep="last",
    ).sort_values(["trade_date", "index_code", "symbol"], kind="stable")


def _membership_storage_identity(universe: str) -> tuple[int, int, int]:
    root = get_config().data_root
    if universe.lower() == "csi800":
        paths = [
            *root.joinpath("api_cache", "tushare").glob("index_weight-*.parquet"),
            *root.joinpath(
                "research_lake", "raw", "stock", "1d", "csi800_membership",
            ).glob("**/*.parquet"),
        ]
    else:
        paths = [root / "universe" / f"{universe}.json"]
    identities = []
    for path in paths:
        try:
            stat = path.stat()
            identities.append((stat.st_size, stat.st_mtime_ns))
        except OSError:
            continue
    return (
        len(identities), sum(size for size, _mtime in identities),
        max((mtime for _size, mtime in identities), default=0),
    )


def _bar_storage_identity(symbols: list[str], store) -> str:
    digest = hashlib.sha256()
    for symbol in symbols:
        path = store.path_for_repair(symbol)
        try:
            stat = path.stat()
            identity = f"{symbol}\0{stat.st_size}\0{stat.st_mtime_ns}\n"
        except OSError:
            identity = f"{symbol}\0missing\n"
        digest.update(identity.encode("utf-8"))
    return digest.hexdigest()


def _frame_hash(frame: pd.DataFrame) -> str:
    if frame is None or frame.empty:
        return content_hash([])
    columns = [
        name for name in ("index_code", "trade_date", "symbol", "weight")
        if name in frame.columns
    ]
    stable = frame[columns].copy()
    if "trade_date" in stable:
        stable["trade_date"] = pd.to_datetime(stable["trade_date"]).dt.strftime("%Y-%m-%d")
    stable = stable.sort_values(columns, kind="stable").reset_index(drop=True)
    values = pd.util.hash_pandas_object(stable, index=False).to_numpy().tobytes()
    return hashlib.sha256(values).hexdigest()


def _required_ranges(
    universe: str, start: str, end: str, records: pd.DataFrame,
    fixed_symbols: list[str], *, warmup_sessions: int = 120,
) -> dict[str, dict[str, str]]:
    start_date = pd.Timestamp(start).normalize()
    end_date = pd.Timestamp(end).normalize()
    if start_date > end_date:
        raise ValueError("研究开始日期不能晚于结束日期")
    warmup = pd.offsets.BDay(warmup_sessions)
    if universe.lower() != "csi800":
        return {
            symbol: {
                "start": (start_date - warmup).strftime("%Y-%m-%d"),
                "end": end_date.strftime("%Y-%m-%d"),
                "active_start": start_date.strftime("%Y-%m-%d"),
                "active_end": end_date.strftime("%Y-%m-%d"),
            }
            for symbol in fixed_symbols
        }
    calendar = pd.bdate_range(start_date, end_date)
    membership = build_membership_mask(records, calendar)
    ranges: dict[str, dict[str, str]] = {}
    for symbol in membership.columns:
        active = membership.index[membership[symbol].to_numpy(bool)]
        if active.empty:
            continue
        active_start = pd.Timestamp(active[0])
        active_end = pd.Timestamp(active[-1])
        ranges[str(symbol)] = {
            "start": (active_start - warmup).strftime("%Y-%m-%d"),
            "end": active_end.strftime("%Y-%m-%d"),
            "active_start": active_start.strftime("%Y-%m-%d"),
            "active_end": active_end.strftime("%Y-%m-%d"),
        }
    return ranges


def inspect_local_dataset(universe: str, start: str, end: str) -> dict[str, Any]:
    """Inspect local manifests without opening Parquet files or contacting providers."""
    from quantmaster.data.storage import BarStore
    from quantmaster.data.universe import load_universe

    store = BarStore()
    cache_key = (str(get_config().data_root.resolve()), universe, start, end)
    membership_identity = _membership_storage_identity(universe)
    with _PANEL_CACHE_LOCK:
        cached = _INSPECTION_CACHE.get(cache_key)
    if cached is not None:
        cached_value, cached_bars, cached_membership = cached
        if (
            cached_membership == membership_identity
            and cached_bars == _bar_storage_identity(cached_value["symbols"], store)
        ):
            with _PANEL_CACHE_LOCK:
                _INSPECTION_CACHE.move_to_end(cache_key)
            return dict(cached_value)

    end_label = pd.Timestamp(end).strftime("%Y-%m-%d")
    records = pd.DataFrame()
    if universe.lower() == "csi800":
        records = _cached_membership_records(start, end)
        fixed_symbols: list[str] = []
        membership_source = "research_lake:tushare:index_weight"
    else:
        fixed_symbols = sorted(load_universe(universe))
        membership_source = "fixed"
    ranges = _required_ranges(universe, start, end, records, fixed_symbols)
    symbols = sorted(ranges)
    metadata = store.metadata_many(symbols)
    missing: list[dict[str, Any]] = []
    coverage_gaps: list[dict[str, Any]] = []
    warmup_gaps: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        item = metadata.get(symbol) or {}
        path = store.path_for_repair(symbol)
        stat = path.stat() if path.is_file() else None
        available_start = str(item.get("coverage_start") or item.get("start") or "")
        available_end = str(item.get("coverage_end") or item.get("end") or "")
        required = ranges[symbol]
        reason = ""
        if stat is None:
            reason = "file_missing"
        elif str(item.get("last_status") or "") == "corrupt":
            reason = "catalogued_corrupt"
        elif not available_start or not available_end:
            reason = "coverage_unknown"
        elif available_start > required["active_start"]:
            coverage_gaps.append({
                "symbol": symbol, "reason": "active_start_missing",
                "required": required, "available": [available_start, available_end],
            })
        elif available_end < required["end"]:
            coverage_gaps.append({
                "symbol": symbol, "reason": "active_end_missing",
                "required": required, "available": [available_start, available_end],
            })
        elif available_start > required["start"]:
            warmup_gaps.append({
                "symbol": symbol, "reason": "warmup_partial",
                "required": required, "available": [available_start, available_end],
            })
        if reason:
            missing.append({
                "symbol": symbol, "reason": reason, "required": required,
                "available": [available_start, available_end],
            })
        rows.append({
            "symbol": symbol,
            "required": required,
            "coverage": [available_start, available_end],
            "bytes": int(stat.st_size if stat is not None else 0),
            "mtime_ns": int(stat.st_mtime_ns if stat is not None else 0),
            "content_sha256": str(item.get("content_sha256") or ""),
            "status": str(item.get("last_status") or "missing"),
        })
    requested_active_end = max(
        (value["active_end"] for value in ranges.values()), default=end_label,
    )
    current_rows = [
        row for row in rows if row["required"]["active_end"] == requested_active_end
    ]
    coverage_ends = sorted(row["coverage"][1] for row in current_rows if row["coverage"][1])
    allowed_missing = int(len(current_rows) * 0.02)
    coverage_threshold = (
        coverage_ends[min(allowed_missing, len(coverage_ends) - 1)] if coverage_ends else ""
    )
    as_of = min(requested_active_end, coverage_threshold) if coverage_threshold else ""
    state = (
        "incomplete" if not symbols or missing or coverage_gaps
        else "stale" if as_of and as_of < requested_active_end
        else "ready"
    )
    blockers: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    if universe.lower() == "csi800" and records.empty:
        blockers.append({
            "code": "DATASET_MISSING",
            "message": "本地没有可用的 CSI800 点时成分",
            "action": "配置 Tushare 后显式运行数据准备",
        })
    if missing:
        blockers.append({
            "code": "DATA_COVERAGE_INSUFFICIENT",
            "message": f"{len(missing)} 只标的缺少所需本地行情区间",
            "action": "显式运行数据准备，仅补齐列出的标的和区间",
            "context": {"missing_count": len(missing), "sample": missing[:20]},
        })
    if coverage_gaps:
        warnings.append({
            "code": "DATA_COVERAGE_INSUFFICIENT",
            "message": f"{len(coverage_gaps)} 只标的存在局部行情区间缺口",
            "action": "可使用冻结快照研究；审批或部署前需显式补齐",
            "context": {"count": len(coverage_gaps), "sample": coverage_gaps[:20]},
        })
    if warmup_gaps:
        warnings.append({
            "code": "DATA_WARMUP_PARTIAL",
            "message": f"{len(warmup_gaps)} 只标的缺少完整 120 日预热；首段样本会自动排除",
            "action": "可继续研究；如需覆盖最早日期，请显式补齐历史行情",
            "context": {"count": len(warmup_gaps), "sample": warmup_gaps[:20]},
        })
    membership_hash = (
        _frame_hash(records) if universe.lower() == "csi800" else content_hash(fixed_symbols)
    )
    result: dict[str, Any] = {
        "universe": universe,
        "start": start,
        "end": end,
        "as_of": as_of,
        "state": state,
        "symbols": symbols,
        "symbol_count": len(symbols),
        "missing": missing,
        "coverage_gaps": coverage_gaps,
        "warmup_gaps": warmup_gaps,
        "blockers": blockers,
        "warnings": warnings,
        "membership_source": membership_source,
        "membership_records": records,
        "membership_hash": membership_hash,
        "required_ranges": ranges,
        "bars": rows,
        "bytes": sum(int(row["bytes"]) for row in rows),
        "manifest_hash": content_hash({"bars": rows, "membership": membership_hash}),
    }
    with _PANEL_CACHE_LOCK:
        _INSPECTION_CACHE[cache_key] = (
            result, _bar_storage_identity(symbols, store), membership_identity,
        )
        while len(_INSPECTION_CACHE) > 8:
            _INSPECTION_CACHE.popitem(last=False)
    return dict(result)


def _assemble_panel(
    frames: dict[str, pd.DataFrame], requested_symbols: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """Assemble aligned numeric panels with one index pass and contiguous arrays."""
    symbols = list(requested_symbols or frames)
    fields = sorted({column for frame in frames.values() for column in frame.columns})
    index_values = np.unique(np.concatenate([
        frame.index.to_numpy(dtype="datetime64[ns]") for frame in frames.values()
    ]))
    index = pd.DatetimeIndex(index_values)
    values: Any = np.full(
        (len(fields), len(index), len(symbols)), np.nan, dtype=np.float64,
    )
    field_positions = {field: position for position, field in enumerate(fields)}
    for symbol_position, symbol in enumerate(symbols):
        frame = frames.get(symbol)
        if frame is None:
            continue
        date_positions = index.get_indexer(frame.index)
        for field in frame.columns:
            values[field_positions[field], date_positions, symbol_position] = frame[
                field
            ].to_numpy(dtype=np.float64)
    return {
        field: pd.DataFrame(
            values[position], index=index, columns=symbols, copy=False,
        )
        for position, field in enumerate(fields)
    }


def _panel_size(panel: dict[str, pd.DataFrame], membership: pd.DataFrame | None) -> int:
    size = sum(int(frame.memory_usage(deep=True).sum()) for frame in panel.values())
    if membership is not None:
        size += int(membership.memory_usage(deep=True).sum())
    return size


def _cache_get(
    key: str,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame | None, dict[str, Any]] | None:
    with _PANEL_CACHE_LOCK:
        value = _PANEL_CACHE.pop(key, None)
        if value is None:
            return None
        _PANEL_CACHE[key] = value
        panel, membership, snapshot, _size = value
        result = dict(snapshot)
        result["cache_hit"] = True
        return panel, membership, result


def _cache_put(
    key: str,
    panel: dict[str, pd.DataFrame],
    membership: pd.DataFrame | None,
    snapshot: dict[str, Any],
) -> None:
    global _PANEL_CACHE_BYTES
    size = _panel_size(panel, membership)
    budget = max(0, int(get_config().lab.panel_cache_mb)) * 1024 * 1024
    if not budget or size > budget:
        return
    with _PANEL_CACHE_LOCK:
        previous = _PANEL_CACHE.pop(key, None)
        if previous:
            _PANEL_CACHE_BYTES -= previous[3]
        while _PANEL_CACHE and _PANEL_CACHE_BYTES + size > budget:
            _old_key, old = _PANEL_CACHE.popitem(last=False)
            _PANEL_CACHE_BYTES -= old[3]
        _PANEL_CACHE[key] = (panel, membership, dict(snapshot), size)
        _PANEL_CACHE_BYTES += size


def clear_local_dataset_caches() -> None:
    """Clear bounded process caches; intended for deterministic local benchmarks."""
    global _PANEL_CACHE_BYTES
    with _PANEL_CACHE_LOCK:
        _PANEL_CACHE.clear()
        _INSPECTION_CACHE.clear()
        _PANEL_CACHE_BYTES = 0


def load_local_dataset(
    universe: str,
    start: str,
    end: str,
    *,
    policy: str = "prefer_local",
    progress=None,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame | None, dict[str, Any]]:
    """Materialize a research panel from the local pool with zero network access."""
    from quantmaster.data.storage import BarStore
    from quantmaster.lab.errors import LabError

    inspection = inspect_local_dataset(universe, start, end)
    if not inspection["symbols"]:
        raise LabError(
            "DATASET_MISSING", "本地研究数据为空",
            action="显式运行数据准备后重试", retryable=True, status_code=424,
        )
    if inspection["blockers"]:
        first = inspection["blockers"][0]
        raise LabError(
            str(first["code"]), str(first["message"]), action=str(first["action"]),
            retryable=True, context=dict(first.get("context") or {}), status_code=424,
        )
    key = content_hash({
        "manifest": inspection["manifest_hash"], "universe": universe,
        "start": start, "end": end, "policy": policy,
    })
    cached = _cache_get(key)
    if cached is not None:
        if progress:
            progress(52, "复用本地冻结快照")
        return cached
    if progress:
        progress(15, f"本地批量读取 {inspection['symbol_count']} 只标的")
    batch = BarStore().read_many(
        inspection["symbols"], columns=["open", "high", "low", "close", "volume", "amount"],
        ranges={
            symbol: (value["start"], value["end"])
            for symbol, value in inspection["required_ranges"].items()
        },
        max_workers=min(8, max(1, int(get_config().lab.max_workers) * 4)),
        enqueue_repair=False,
    )
    if not batch.frames:
        raise LabError(
            "DATASET_MISSING", "没有可读取的本地行情",
            action="运行数据准备或修复本地数据池", retryable=True, status_code=424,
        )
    panel = _assemble_panel(batch.frames, inspection["symbols"])
    close = panel.get("close")
    if close is None or close.empty:
        raise LabError("DATASET_MISSING", "本地行情缺少 close 字段", status_code=424)
    records = inspection["membership_records"]
    if universe.lower() == "csi800":
        membership = build_membership_mask(records, close.index)
        membership.loc[membership.index < pd.Timestamp(start)] = False
        membership.loc[membership.index > pd.Timestamp(end)] = False
    else:
        membership = pd.DataFrame(
            False, index=close.index, columns=inspection["symbols"], dtype=bool,
        )
        research_dates = (membership.index >= pd.Timestamp(start)) & (
            membership.index <= pd.Timestamp(end)
        )
        membership.loc[research_dates, :] = True
    missing_prices = 0
    membership_coverage = 1.0
    if membership is not None:
        aligned = membership.reindex(index=close.index, columns=close.columns).fillna(False)
        expected_prices = int(membership.reindex(index=close.index).fillna(False).sum().sum())
        observed_prices = int((aligned & close.notna()).sum().sum())
        missing_prices = max(0, expected_prices - observed_prices)
        membership_coverage = 1 - missing_prices / max(1, expected_prices)
    warnings: list[dict[str, Any]] = [dict(item) for item in inspection.get("warnings") or []]
    if inspection["state"] == "stale":
        warnings.append({
            "code": "DATASET_STALE", "level": "warning",
            "message": f"本地数据截至 {inspection['as_of']}，研究结果将保留该时间戳",
        })
    if membership_coverage < 0.98:
        warnings.append({
            "code": "DATA_COVERAGE_INSUFFICIENT", "level": "warning",
            "message": f"在池交易日价格覆盖率为 {membership_coverage:.2%}",
            "context": {"missing_cells": missing_prices},
        })
    if batch.failures:
        warnings.append({
            "code": "DATA_COVERAGE_INSUFFICIENT", "level": "warning",
            "message": f"{len(batch.failures)} 只标的在所需区间没有可读本地行情",
            "action": "可继续研究；审批或部署前请运行数据修复",
            "context": {"failures": dict(list(batch.failures.items())[:20])},
        })
    state = (
        "incomplete" if batch.failures or membership_coverage < 0.90
        else inspection["state"]
    )
    snapshot = DatasetSnapshot(
        universe=universe,
        start=start,
        end=end,
        symbols=tuple(batch.frames),
        membership_source=inspection["membership_source"],
        research_quality="production" if universe.lower() == "csi800" else "sandbox",
        as_of=inspection["as_of"] or pd.Timestamp(close.index.max()).strftime("%Y-%m-%d"),
        state=cast(Literal["ready", "stale", "incomplete", "corrupt"], state),
        data_policy=policy,
        production_eligible=bool(state == "ready" and membership_coverage >= 0.98),
        warnings=tuple(warnings),
        manifest={
            "bars": list(batch.manifest),
            "manifest_hash": inspection["manifest_hash"],
            "membership_hash": inspection["membership_hash"],
            "required_ranges": inspection["required_ranges"],
            "coverage": {
                "dates": len(close.index), "symbols": len(close.columns),
                "price_coverage": round(float(close.notna().sum().sum()) / max(1, close.size), 6),
                "membership_missing_prices": missing_prices,
                "membership_price_coverage": round(membership_coverage, 6),
            },
            "read_seconds": round(batch.elapsed_seconds, 6),
            "cache_key": key,
        },
    ).to_dict()
    snapshot["cache_hit"] = False
    _cache_put(key, panel, membership, snapshot)
    if progress:
        progress(52, f"本地快照已冻结 · {batch.elapsed_seconds:.2f}s")
    return panel, membership, snapshot


def load_csi800_members_as_of(as_of: str, *, source=None) -> dict[str, Any]:
    """读取目标日可知的中证800成分，分别沿用两个指数最近一期快照。"""
    return load_cached_csi800_members_as_of(as_of, source=source)


def _bar_manifest(symbols: Iterable[str]) -> dict[str, Any]:
    from quantmaster.data.storage import BarStore

    store = BarStore()
    selected = sorted(set(symbols))
    metadata = store.metadata_many(selected)
    rows = []
    for symbol in selected:
        path = store.path_for_repair(symbol)
        item = metadata.get(symbol) or {}
        coverage = (
            str(item.get("coverage_start") or item.get("start") or ""),
            str(item.get("coverage_end") or item.get("end") or ""),
        )
        rows.append({
            "symbol": symbol,
            "coverage": list(coverage) if coverage else None,
            "bytes": int(item.get("file_size") or (path.stat().st_size if path.is_file() else 0)),
            "mtime_ns": int(item.get("file_mtime_ns") or (path.stat().st_mtime_ns if path.is_file() else 0)),
            "content_sha256": str(item.get("content_sha256") or ""),
        })
    return {"bars": rows, "manifest_hash": content_hash(rows)}


def _membership_manifest(membership: pd.DataFrame | None, symbols: list[str]) -> dict[str, Any]:
    """把布尔掩码压缩成稳定、可 JSON 序列化的快照描述。"""
    if membership is None:
        return {"kind": "fixed", "symbols": symbols}
    enabled = membership.fillna(False).astype(bool)
    snapshots = []
    previous: tuple[str, ...] | None = None
    for snapshot_date, row in enabled.iterrows():
        current = tuple(str(symbol) for symbol in enabled.columns[row.to_numpy()])
        if current != previous:
            snapshots.append({
                "date": pd.Timestamp(snapshot_date).strftime("%Y-%m-%d"),
                "symbols": current,
            })
            previous = current
    return {
        "kind": "point_in_time",
        "shape": [int(enabled.shape[0]), int(enabled.shape[1])],
        "snapshots": snapshots,
    }


def create_snapshot(
    universe: str,
    start: str,
    end: str,
    *,
    panel: dict[str, pd.DataFrame] | None = None,
    membership: pd.DataFrame | None = None,
) -> DatasetSnapshot:
    """创建轻量快照；真实数据文件继续复用现有 Parquet 缓存。"""
    from quantmaster.data.universe import load_universe

    fixed_symbols = load_universe(universe) if universe.lower() != "csi800" else []
    source = "fixed"
    quality: Literal["production", "sandbox"] = "sandbox"
    if universe.lower() == "csi800":
        if membership is None:
            membership = load_csi800_membership(start, end)
        symbols = sorted(symbol for symbol in membership.columns if membership[symbol].any())
        source, quality = "tushare:index_weight", "production"
    else:
        symbols = sorted(fixed_symbols)
    close = panel.get("close") if panel else None
    coverage = {}
    if close is not None and not close.empty:
        expected = max(1, len(close.index) * max(1, len(symbols)))
        coverage = {
            "dates": len(close.index),
            "symbols": len(close.columns),
            "price_coverage": round(float(close.notna().sum().sum()) / expected, 6),
        }
    manifest = {
        **_bar_manifest(symbols),
        "coverage": coverage,
        "membership_hash": content_hash(_membership_manifest(membership, symbols)),
        "config": {
            "data_root": str(get_config().data_root.resolve()),
            "warmup_days": 120,
            "horizons": list(get_config().lab.horizons),
        },
    }
    return DatasetSnapshot(
        universe=universe,
        start=start,
        end=end,
        symbols=tuple(symbols),
        membership_source=source,
        research_quality=quality,
        manifest=manifest,
    )


def readiness() -> dict[str, Any]:
    """供 UI 展示的本机研究能力，不触发网络。"""
    import importlib.util

    cfg = get_config()
    from quantmaster.data.storage import BarStore

    cached_membership = False
    try:
        load_cached_csi800_members_as_of(calendar_date.today().isoformat(), pull=False)
        cached_membership = True
    except (OSError, RuntimeError, ValueError):
        cached_membership = False
    metadata = BarStore().metadata_many()
    return {
        "tushare": {
            "configured": bool(cfg.data.tushare_token),
            "cached_membership": cached_membership,
            "production_membership": cached_membership or bool(cfg.data.tushare_token),
        },
        "ml": {
            "torch": bool(importlib.util.find_spec("torch")),
            "sklearn": bool(importlib.util.find_spec("sklearn")),
            "onnxruntime": bool(importlib.util.find_spec("onnxruntime")),
        },
        "llm": {"configured": bool(cfg.llm.api_key), "provider": cfg.llm.provider},
        "local_data": {
            "catalogued_symbols": len(metadata),
            "bytes": sum(int(item.get("file_size") or 0) for item in metadata.values()),
            "network_required_for_research": False,
        },
    }
