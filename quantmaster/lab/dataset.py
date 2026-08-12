"""Point-in-time 候选、数据就绪检查与可复现实验快照。"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import threading
from collections import OrderedDict
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlparse

import numpy as np
import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.index_membership import (
    load_cached_csi800_members_as_of,
    load_cached_csi800_records,
)
from quantmaster.lab.models import DatasetSnapshot, content_hash
from quantmaster.trading_sessions import daily_signal_cutoff, market_date

_PANEL_CACHE_LOCK = threading.RLock()
_PANEL_CACHE: OrderedDict[
    str, tuple[dict[str, pd.DataFrame], pd.DataFrame | None, dict[str, Any], int]
] = OrderedDict()
_PANEL_CACHE_BYTES = 0
_PANEL_REQUEST_KEYS: dict[
    tuple[str, str, str, str, str],
    tuple[str, tuple[tuple[int, int], ...], tuple[int, int, int]],
] = {}
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
    if {"published_at", "acquired_at"} <= set(frame.columns):
        frame["published_at"] = pd.to_datetime(frame["published_at"], utc=True, errors="coerce")
        frame["acquired_at"] = pd.to_datetime(frame["acquired_at"], utc=True, errors="coerce")
        frame = frame.dropna(subset=["published_at", "acquired_at"])
        all_symbols = sorted(set(frame["symbol"].dropna().astype(str)))
        events = []
        for (code, effective, acquired, published), group in frame.groupby(
            ["index_code", "trade_date", "acquired_at", "published_at"], sort=True,
        ):
            effective = pd.Timestamp(effective)
            available = max(
                pd.Timestamp(acquired),
                pd.Timestamp(published),
                pd.Timestamp(daily_signal_cutoff(effective.date())).tz_convert("UTC"),
            )
            events.append((
                available, str(code), effective, pd.Timestamp(acquired),
                tuple(group["symbol"].dropna().astype(str)),
            ))
        events.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
        state: dict[str, dict[pd.Timestamp, tuple[pd.Timestamp, tuple[str, ...]]]] = {}
        rows = []
        position = 0
        for current_date in dates:
            cutoff = pd.Timestamp(daily_signal_cutoff(current_date.date())).tz_convert("UTC")
            while position < len(events) and events[position][0] <= cutoff:
                _available, code, effective, acquired, members = events[position]
                previous = state.setdefault(code, {}).get(effective)
                if previous is None or acquired >= previous[0]:
                    state[code][effective] = (acquired, members)
                position += 1
            selected: set[str] = set()
            for versions in state.values():
                eligible_dates = [effective for effective in versions if effective <= current_date]
                if eligible_dates:
                    selected.update(versions[max(eligible_dates)][1])
            rows.append([symbol in selected for symbol in all_symbols])
        return pd.DataFrame(rows, index=dates, columns=all_symbols, dtype=bool)
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
    identity = _membership_storage_identity("csi800")
    cache_root = get_config().data_root / "lab_cache" / "membership"
    cache_key = content_hash({
        "identity": identity, "beginning": beginning, "end": end,
    })[:20]
    cache_path = cache_root / f"csi800-{cache_key}.parquet"
    if cache_path.is_file():
        try:
            return pd.read_parquet(cache_path)
        except (OSError, ValueError, ImportError):
            pass
    # The shared loader merges research-lake and legacy local evidence while
    # preserving multiple acquired-at versions of the same effective date.
    result = load_cached_csi800_records(beginning, end, pull=False)
    try:
        cache_root.mkdir(parents=True, exist_ok=True)
        staged = cache_path.with_suffix(".partial.parquet")
        result.to_parquet(staged, index=False)
        os.replace(staged, cache_path)
    except (OSError, ValueError, ImportError):
        pass
    return result


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


def _fast_pool_identity() -> tuple[tuple[int, int], ...]:
    """Cheap invalidation key for an already materialized in-process panel."""
    bars_root = get_config().data_root / "bars"
    identities: list[tuple[int, int]] = []
    for path in (bars_root, bars_root / "meta.sqlite"):
        try:
            stat = path.stat()
            identities.append((stat.st_size, stat.st_mtime_ns))
        except OSError:
            identities.append((0, 0))
    return tuple(identities)


def _bar_storage_identity(symbols: list[str], store) -> str:
    digest = hashlib.sha256()
    metadata = store.metadata_many(symbols)
    for symbol in symbols:
        path = store.path_for_repair(symbol)
        try:
            stat = path.stat()
            identity = f"{symbol}\0{stat.st_size}\0{stat.st_mtime_ns}\n"
        except OSError:
            identity = f"{symbol}\0missing\n"
        digest.update(identity.encode("utf-8"))
        item = metadata.get(symbol) or {}
        digest.update(json.dumps({
            "content_sha256": item.get("content_sha256"),
            "last_status": item.get("last_status"),
            "quality_json": item.get("quality_json"),
            "source_chain_json": item.get("source_chain_json"),
        }, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    return digest.hexdigest()


def _bar_quality_for_range(
    metadata: dict[str, Any], required: dict[str, str],
) -> dict[str, Any]:
    """Resolve persisted truth only from lineage overlapping the requested interval."""
    try:
        quality = json.loads(str(metadata.get("quality_json") or "{}"))
        if not isinstance(quality, dict):
            quality = {}
    except (TypeError, ValueError, json.JSONDecodeError):
        quality = {}
    try:
        chain = json.loads(str(metadata.get("source_chain_json") or "[]"))
        if not isinstance(chain, list):
            chain = []
    except (TypeError, ValueError, json.JSONDecodeError):
        chain = []

    requested_start = str(required.get("start") or required.get("active_start") or "")[:10]
    requested_end = str(required.get("end") or required.get("active_end") or "")[:10]
    overlapping: list[dict[str, Any]] = []
    for raw in chain:
        if not isinstance(raw, dict):
            continue
        event_start = str(raw.get("affected_start") or raw.get("requested_start") or "")[:10]
        event_end = str(raw.get("affected_end") or raw.get("requested_end") or "")[:10]
        if not event_start or not event_end:
            continue
        if event_end < requested_start or event_start > requested_end:
            continue
        raw_event_quality = raw.get("quality")
        event_quality: dict[str, Any]
        if isinstance(raw_event_quality, dict):
            event_quality = raw_event_quality
        else:
            event_quality = {}
        overlapping.append({
            "source": str(raw.get("source") or ""),
            "affected_start": event_start,
            "affected_end": event_end,
            "status": str(event_quality.get("status") or raw.get("status") or ""),
            "stale": bool(event_quality.get("stale")),
            "partial": bool(event_quality.get("partial")),
            "issues": [str(value) for value in event_quality.get("issues") or ()],
        })

    rank = {"verified": 0, "degraded": 1, "unavailable": 2}
    statuses = [item["status"] for item in overlapping if item["status"] in rank]
    unknown_status = any(item["status"] not in rank for item in overlapping)
    status = (
        max(statuses, key=lambda value: rank[value])
        if statuses else str(quality.get("status") or "")
    )
    stale = bool(quality.get("stale")) or any(item["stale"] for item in overlapping)
    partial = bool(quality.get("partial")) or any(item["partial"] for item in overlapping)
    issues = list(dict.fromkeys((
        *(str(value) for value in quality.get("issues") or ()),
        *(issue for item in overlapping for issue in item["issues"]),
    )))
    if not overlapping:
        issues.append("请求区间没有可验证的分段来源链")
    if status not in rank or unknown_status:
        issues.append("请求区间缺少结构化行情质量状态")
    verified_intervals = sorted(
        (
            pd.Timestamp(item["affected_start"]),
            pd.Timestamp(item["affected_end"]),
        )
        for item in overlapping
        if item["status"] == "verified" and not item["stale"] and not item["partial"]
    )
    lineage_complete = False
    if verified_intervals:
        merged_start, merged_end = verified_intervals[0]
        if merged_start <= pd.Timestamp(requested_start):
            for interval_start, interval_end in verified_intervals[1:]:
                if interval_start > merged_end + pd.offsets.BDay(1):
                    break
                merged_end = max(merged_end, interval_end)
            lineage_complete = merged_end >= pd.Timestamp(requested_end)
    if not lineage_complete:
        issues.append("已验证来源链没有覆盖完整请求区间")
    verified = bool(
        lineage_complete and status == "verified" and not unknown_status
        and not stale and not partial
    )
    return {
        "status": status or "unavailable",
        "stale": stale,
        "partial": partial,
        "verified": verified,
        "lineage_complete": lineage_complete,
        "issues": list(dict.fromkeys(issues)),
        "source_chain": overlapping,
    }


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


def _cached_required_ranges(
    universe: str, start: str, end: str, records: pd.DataFrame,
    fixed_symbols: list[str],
) -> dict[str, dict[str, str]]:
    identity: Any = (
        _membership_storage_identity(universe)
        if universe.lower() == "csi800" else content_hash(fixed_symbols)
    )
    key = content_hash({
        "universe": universe, "start": start, "end": end, "identity": identity,
    })[:20]
    root = get_config().data_root / "lab_cache" / "membership"
    target = root / f"ranges-{key}.json"
    if target.is_file():
        try:
            value = json.loads(target.read_text(encoding="utf-8"))
            if isinstance(value, dict):
                return value
        except (OSError, ValueError, TypeError):
            pass
    ranges = _required_ranges(universe, start, end, records, fixed_symbols)
    try:
        root.mkdir(parents=True, exist_ok=True)
        staged = target.with_suffix(".partial.json")
        staged.write_text(
            json.dumps(ranges, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(staged, target)
    except OSError:
        pass
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
        fixed_symbols = sorted(load_universe(universe, as_of=end_label))
        membership_source = "fixed"
    ranges = _cached_required_ranges(universe, start, end, records, fixed_symbols)
    symbols = sorted(ranges)
    bar_identity = _bar_storage_identity(symbols, store)
    catalog_identity = []
    for catalog_path in (store.meta_db,):
        try:
            stat = catalog_path.stat()
            catalog_identity.append((stat.st_size, stat.st_mtime_ns))
        except OSError:
            continue
    persistent_key = content_hash({
        "root": str(get_config().data_root.resolve()), "universe": universe,
        "start": start, "end": end, "membership": membership_identity,
        "bars": bar_identity, "catalog": catalog_identity,
    })[:20]
    persistent_root = get_config().data_root / "lab_cache" / "inspection"
    persistent_path = persistent_root / f"{persistent_key}.json"
    if persistent_path.is_file():
        try:
            saved = json.loads(persistent_path.read_text(encoding="utf-8"))
            if isinstance(saved, dict):
                saved["membership_records"] = records
                with _PANEL_CACHE_LOCK:
                    _INSPECTION_CACHE[cache_key] = (
                        saved, bar_identity, membership_identity,
                    )
                return dict(saved)
        except (OSError, ValueError, TypeError):
            pass
    metadata = store.metadata_many(symbols)
    missing: list[dict[str, Any]] = []
    coverage_gaps: list[dict[str, Any]] = []
    warmup_gaps: list[dict[str, Any]] = []
    quality_gaps: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        item = metadata.get(symbol) or {}
        path = store.path_for_repair(symbol)
        stat = path.stat() if path.is_file() else None
        available_start = str(item.get("coverage_start") or item.get("start") or "")
        available_end = str(item.get("coverage_end") or item.get("end") or "")
        required = ranges[symbol]
        bar_quality = _bar_quality_for_range(item, required)
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
        if not bar_quality["verified"]:
            quality_gaps.append({
                "symbol": symbol,
                "required": required,
                "status": bar_quality["status"],
                "stale": bar_quality["stale"],
                "partial": bar_quality["partial"],
                "issues": bar_quality["issues"],
            })
        rows.append({
            "symbol": symbol,
            "required": required,
            "coverage": [available_start, available_end],
            "bytes": int(stat.st_size if stat is not None else 0),
            "mtime_ns": int(stat.st_mtime_ns if stat is not None else 0),
            "content_sha256": str(item.get("content_sha256") or ""),
            "status": str(item.get("last_status") or "missing"),
            "quality": bar_quality,
        })
    active_symbol_coverage = (
        (len(symbols) - len(missing) - len(coverage_gaps)) / max(1, len(symbols))
    )
    research_eligible = bool(symbols and active_symbol_coverage >= 0.90)
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
        "incomplete" if not research_eligible
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
        issue = {
            "code": "DATA_COVERAGE_INSUFFICIENT",
            "message": f"{len(missing)} 只标的缺少所需本地行情区间",
            "action": (
                "可继续研究；生产使用前请显式补齐"
                if research_eligible else "显式运行数据准备，仅补齐列出的标的和区间"
            ),
            "context": {"missing_count": len(missing), "sample": missing[:20]},
        }
        (warnings if research_eligible else blockers).append(issue)
    if coverage_gaps:
        warnings.append({
            "code": "DATA_COVERAGE_INSUFFICIENT",
            "message": f"{len(coverage_gaps)} 只标的存在局部行情区间缺口",
            "action": "可继续研究；生产资格以实际在池价格覆盖率门禁为准",
            "context": {"count": len(coverage_gaps), "sample": coverage_gaps[:20]},
        })
    if warmup_gaps:
        warnings.append({
            "code": "DATA_WARMUP_PARTIAL",
            "message": f"{len(warmup_gaps)} 只标的缺少完整 120 日预热；首段样本会自动排除",
            "action": "可继续研究；如需覆盖最早日期，请显式补齐历史行情",
            "context": {"count": len(warmup_gaps), "sample": warmup_gaps[:20]},
        })
    if quality_gaps:
        warnings.append({
            "code": "DATA_QUALITY_UNVERIFIED",
            "message": f"{len(quality_gaps)} 只标的缺少请求区间内的已验证行情证据",
            "action": "可继续沙盒研究；正式生产前需补齐单位、复权、来源与时效证据",
            "context": {"count": len(quality_gaps), "sample": quality_gaps[:20]},
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
        "active_symbol_coverage": round(active_symbol_coverage, 6),
        "research_eligible": research_eligible,
        "production_eligible": bool(
            state == "ready" and active_symbol_coverage >= 0.98 and not quality_gaps
        ),
        "symbols": symbols,
        "symbol_count": len(symbols),
        "missing": missing,
        "coverage_gaps": coverage_gaps,
        "warmup_gaps": warmup_gaps,
        "quality_gaps": quality_gaps,
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
            result, bar_identity, membership_identity,
        )
        while len(_INSPECTION_CACHE) > 8:
            _INSPECTION_CACHE.popitem(last=False)
    try:
        persistent_root.mkdir(parents=True, exist_ok=True)
        serializable = {key: value for key, value in result.items() if key != "membership_records"}
        staged = persistent_path.with_suffix(".partial.json")
        staged.write_text(
            json.dumps(serializable, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(staged, persistent_path)
    except (OSError, ValueError, TypeError):
        pass
    return dict(result)


def dataset_repair_plan(universe: str, start: str, end: str) -> dict[str, Any]:
    """Describe exact local coverage gaps and explicit online repair options."""
    inspected = inspect_local_dataset(universe, start, end)
    cfg = get_config().data
    active_symbols = {
        str(item.get("symbol") or "")
        for item in inspected.get("coverage_gaps") or []
    }
    warmup_symbols = {
        str(item.get("symbol") or "")
        for item in inspected.get("warmup_gaps") or []
    }
    missing_symbols = {
        str(item.get("symbol") or "")
        for item in inspected.get("missing") or []
    }

    def sessions(first: str, last: str) -> int:
        if not first or not last or first > last:
            return 0
        return int(
            np.busday_count(
                np.datetime64(first, "D"), np.datetime64(last, "D") + np.timedelta64(1, "D"),
            )
        )

    cells: list[dict[str, Any]] = []
    gaps: list[dict[str, Any]] = []
    missing_session_total = 0
    critical_session_total = 0
    for row in inspected.get("bars") or []:
        symbol = str(row.get("symbol") or "")
        required = dict(row.get("required") or {})
        required_start = str(required.get("start") or start)
        required_end = str(required.get("end") or end)
        active_start = str(required.get("active_start") or required_start)
        available = list(row.get("coverage") or ["", ""])
        available_start = str(available[0] or "")
        available_end = str(available[1] or "")
        health = "complete"
        if symbol in missing_symbols:
            health = "missing"
        elif symbol in active_symbols:
            health = "critical"
        elif symbol in warmup_symbols:
            health = "warmup"
        segments: list[dict[str, Any]] = []
        if health == "missing" or not available_start or not available_end:
            segments.append({"start": required_start, "end": required_end, "kind": "critical"})
        else:
            if available_start > required_start:
                left_end = (
                    pd.Timestamp(available_start) - pd.offsets.BDay(1)
                ).strftime("%Y-%m-%d")
                if required_start < active_start:
                    warmup_end = (
                        pd.Timestamp(active_start) - pd.offsets.BDay(1)
                    ).strftime("%Y-%m-%d")
                    if required_start <= min(left_end, warmup_end):
                        segments.append({
                            "start": required_start,
                            "end": min(left_end, warmup_end),
                            "kind": "warmup",
                        })
                if available_start > active_start and active_start <= left_end:
                    segments.append({
                        "start": active_start,
                        "end": min(required_end, left_end),
                        "kind": "critical",
                    })
            if available_end < required_end:
                right_start = (
                    pd.Timestamp(available_end) + pd.offsets.BDay(1)
                ).strftime("%Y-%m-%d")
                segments.append({
                    "start": max(required_start, right_start),
                    "end": required_end,
                    "kind": "critical",
                })
        missing_sessions = sum(
            sessions(str(segment["start"]), str(segment["end"]))
            for segment in segments
        )
        critical_sessions = sum(
            sessions(str(segment["start"]), str(segment["end"]))
            for segment in segments if segment["kind"] == "critical"
        )
        missing_session_total += missing_sessions
        critical_session_total += critical_sessions
        cell = {
            "symbol": symbol,
            "health": health,
            "required": [required_start, required_end],
            "available": [available_start, available_end],
            "missing_sessions": missing_sessions,
        }
        cells.append(cell)
        if segments:
            gaps.append({**cell, "segments": segments})

    membership_missing = bool(
        universe.lower() == "csi800"
        and getattr(inspected.get("membership_records"), "empty", True)
    )
    repair_symbols = len({item["symbol"] for item in gaps})
    critical_repair_symbols = len({
        item["symbol"]
        for item in gaps
        if any(segment["kind"] == "critical" for segment in item["segments"])
    })
    return {
        "universe": universe,
        "start": start,
        "end": end,
        "as_of": inspected.get("as_of", ""),
        "state": inspected.get("state", "incomplete"),
        "research_eligible": bool(inspected.get("research_eligible")),
        "production_eligible": bool(inspected.get("production_eligible")),
        "symbol_count": len(cells),
        "repair_symbol_count": repair_symbols,
        "critical_repair_symbol_count": critical_repair_symbols,
        "missing_session_count": missing_session_total,
        "critical_session_count": critical_session_total,
        "membership_missing": membership_missing,
        "counts": {
            "complete": sum(item["health"] == "complete" for item in cells),
            "critical": sum(item["health"] == "critical" for item in cells),
            "warmup": sum(item["health"] == "warmup" for item in cells),
            "missing": sum(item["health"] == "missing" for item in cells),
        },
        "cells": cells,
        "gaps": gaps,
        "providers": [
            {
                "id": "free-stockdb",
                "label": "本机 StockDB",
                "available": urlparse(str(cfg.free_stockdb_url)).hostname in {
                    "127.0.0.1", "localhost", "::1",
                },
                "can_fill_membership": False,
                "estimated_requests": repair_symbols,
                "estimated_critical_requests": critical_repair_symbols,
                "reason": (
                    "本机 StockDB 不提供 CSI800 点时成分"
                    if membership_missing else "从本机回环 StockDB 按缺口区间补齐日线"
                ),
            },
            {
                "id": "tushare",
                "label": "Tushare",
                "available": bool(cfg.tushare_token),
                "can_fill_membership": True,
                "estimated_requests": repair_symbols * 2 + (2 if membership_missing else 0),
                "estimated_critical_requests": (
                    critical_repair_symbols * 2 + (2 if membership_missing else 0)
                ),
                "reason": (
                    "可补点时成分及前复权日线"
                    if cfg.tushare_token else "需要先在设置中配置 Tushare token"
                ),
            },
        ],
    }


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
        _PANEL_REQUEST_KEYS.clear()
        _INSPECTION_CACHE.clear()
        _PANEL_CACHE_BYTES = 0


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _freeze_dataset_evidence(
    panel: dict[str, pd.DataFrame], membership: pd.DataFrame | None,
) -> dict[str, Any]:
    """Persist the exact matrices used by a Lab run as content-addressed files."""
    from quantmaster.lab.errors import LabError

    if not panel or not any(frame is not None and not frame.empty for frame in panel.values()):
        raise LabError(
            "DATASET_EVIDENCE_MISSING",
            "数据快照没有可冻结的行情矩阵",
            action="重新准备数据并保留不可变证据",
            status_code=424,
        )
    evidence_root = get_config().data_root / "lab_evidence"
    evidence_root.mkdir(parents=True, exist_ok=True)
    staged = Path(tempfile.mkdtemp(prefix=".dataset-", dir=evidence_root))
    files: list[dict[str, Any]] = []
    try:
        for field, frame in sorted(panel.items()):
            if frame is None or frame.empty:
                continue
            path = staged / f"field-{field}.parquet"
            frame.to_parquet(path)
            files.append({
                "kind": "field",
                "name": field,
                "file": path.name,
                "content_sha256": _file_sha256(path),
                "bytes": path.stat().st_size,
            })
        if membership is not None:
            path = staged / "membership.parquet"
            membership.astype(bool).to_parquet(path)
            files.append({
                "kind": "membership",
                "name": "membership",
                "file": path.name,
                "content_sha256": _file_sha256(path),
                "bytes": path.stat().st_size,
            })
        identity = content_hash({
            "schema_version": 1,
            "files": [
                {key: item[key] for key in ("kind", "name", "content_sha256", "bytes")}
                for item in files
            ],
        })
        target = evidence_root / identity
        manifest = {
            "schema_version": 1,
            "evidence_id": identity,
            "relative_root": f"lab_evidence/{identity}",
            "status": "ready",
            "files": files,
        }
        manifest_path = staged / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        if target.is_dir():
            shutil.rmtree(staged)
        else:
            try:
                os.replace(staged, target)
            except FileExistsError:
                shutil.rmtree(staged)
        manifest["manifest_sha256"] = _file_sha256(target / "manifest.json")
        return manifest
    except LabError:
        shutil.rmtree(staged, ignore_errors=True)
        raise
    except (ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
        shutil.rmtree(staged, ignore_errors=True)
        raise LabError(
            "DATASET_EVIDENCE_MISSING",
            f"不可变数据证据写入失败：{str(exc)[:200]}",
            action="检查数据目录可写空间后重新准备数据",
            retryable=True,
            status_code=424,
        ) from exc


def verify_snapshot_evidence(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Verify that every byte referenced by a stored snapshot is recoverable."""
    from quantmaster.lab.errors import LabError

    raw_payload = snapshot.get("payload")
    payload = raw_payload if isinstance(raw_payload, dict) else snapshot
    raw_manifest = payload.get("manifest")
    manifest = raw_manifest if isinstance(raw_manifest, dict) else {}
    raw_evidence = manifest.get("evidence")
    evidence = raw_evidence if isinstance(raw_evidence, dict) else {}
    if evidence.get("status") != "ready" or not evidence.get("relative_root"):
        raise LabError(
            "DATASET_EVIDENCE_MISSING",
            "数据快照没有可恢复的不可变证据",
            action="重新准备数据；禁止用当前缓存替代旧快照",
            status_code=424,
        )
    root = get_config().data_root / str(evidence["relative_root"])
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file() or _file_sha256(manifest_path) != evidence.get("manifest_sha256"):
        raise LabError(
            "DATASET_EVIDENCE_MISSING", "数据证据清单缺失或校验失败", status_code=424,
        )
    for item in evidence.get("files") or []:
        path = root / str(item.get("file") or "")
        if not path.is_file() or _file_sha256(path) != item.get("content_sha256"):
            raise LabError(
                "DATASET_EVIDENCE_MISSING",
                f"数据证据文件缺失或损坏：{item.get('name') or item.get('file')}",
                action="从受信备份恢复对应 evidence_id；不得静默改用当前行情",
                status_code=424,
            )
    return evidence


def load_snapshot_evidence(
    snapshot: dict[str, Any],
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame | None]:
    """Load the exact matrices pinned by ``_freeze_dataset_evidence``."""
    evidence = verify_snapshot_evidence(snapshot)
    root = get_config().data_root / str(evidence["relative_root"])
    panel: dict[str, pd.DataFrame] = {}
    membership = None
    for item in evidence.get("files") or []:
        frame = pd.read_parquet(root / str(item["file"]))
        if item.get("kind") == "membership":
            membership = frame.astype(bool)
        else:
            panel[str(item["name"])] = frame
    return panel, membership


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

    request_key = (
        str(get_config().data_root.resolve()), universe, start, end, policy,
    )
    pool_identity = _fast_pool_identity()
    membership_identity = _membership_storage_identity(universe)
    with _PANEL_CACHE_LOCK:
        alias = _PANEL_REQUEST_KEYS.get(request_key)
    if alias and alias[1] == pool_identity and alias[2] == membership_identity:
        cached = _cache_get(alias[0])
        if cached is not None:
            if progress:
                progress(52, "复用本地冻结快照")
            return cached

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
        max_workers=min(16, max(1, int(get_config().lab.max_workers) * 8)),
        enqueue_repair=False,
    )
    if not batch.frames:
        raise LabError(
            "DATASET_MISSING", "没有可读取的本地行情",
            action="运行数据准备或修复本地数据池", retryable=True, status_code=424,
        )
    panel = _assemble_panel(batch.frames, inspection["symbols"])
    global_start = min(
        (value["start"] for value in inspection["required_ranges"].values()),
        default=start,
    )
    panel = {field: frame.loc[global_start:end] for field, frame in panel.items()}
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
        aligned = membership.reindex(
            index=close.index, columns=close.columns, fill_value=False,
        ).to_numpy(dtype=bool, copy=False)
        expected_prices = int(aligned.sum())
        observed_prices = int((aligned & np.isfinite(close.to_numpy(copy=False))).sum())
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
    evidence = _freeze_dataset_evidence(panel, membership)
    snapshot = DatasetSnapshot(
        universe=universe,
        start=start,
        end=end,
        symbols=tuple(batch.frames),
        membership_source=inspection["membership_source"],
        research_quality=(
            "production"
            if universe.lower() == "csi800" and inspection.get("production_eligible")
            else "sandbox"
        ),
        as_of=inspection["as_of"] or pd.Timestamp(close.index.max()).strftime("%Y-%m-%d"),
        state=cast(Literal["ready", "stale", "incomplete", "corrupt"], state),
        data_policy=policy,
        production_eligible=bool(
            state == "ready"
            and inspection.get("production_eligible")
            and membership_coverage >= 0.98
        ),
        warnings=tuple(warnings),
        manifest={
            "bars": list(batch.manifest),
            "bar_quality": inspection["bars"],
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
            "evidence": evidence,
        },
    ).to_dict()
    snapshot["cache_hit"] = False
    _cache_put(key, panel, membership, snapshot)
    with _PANEL_CACHE_LOCK:
        if key in _PANEL_CACHE:
            _PANEL_REQUEST_KEYS[request_key] = (
                key, _fast_pool_identity(), membership_identity,
            )
    if progress:
        progress(52, f"本地快照已冻结 · {batch.elapsed_seconds:.2f}s")
    return panel, membership, snapshot


def load_csi800_members_as_of(as_of: str, *, source=None) -> dict[str, Any]:
    """读取目标日可知的中证800成分，分别沿用两个指数最近一期快照。"""
    # Page callers pass no source and must stay local-only.  An explicitly
    # injected source is a worker/test boundary and retains the acquisition
    # contract used to create a new immutable membership snapshot.
    return load_cached_csi800_members_as_of(
        as_of, pull=source is not None, source=source,
    )


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
    market_data_quality: dict[str, Any] | None = None,
) -> DatasetSnapshot:
    """Create a recoverable snapshot pinned to immutable matrix evidence."""
    from quantmaster.data.universe import load_universe

    fixed_symbols = (
        load_universe(universe, as_of=end) if universe.lower() != "csi800" else []
    )
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
    evidence = _freeze_dataset_evidence(panel or {}, membership)
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
        "evidence": evidence,
        "market_data_quality": dict(market_data_quality or {}),
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
        load_cached_csi800_members_as_of(market_date().isoformat(), pull=False)
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
