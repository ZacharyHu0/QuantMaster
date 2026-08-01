"""Point-in-time 候选、数据就绪检查与可复现实验快照。"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd

from quantmaster.config import get_config
from quantmaster.lab.models import DatasetSnapshot, content_hash

CSI800_INDEXES = ("000300.SH", "000905.SH")


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
    snapshots: dict[str, list[tuple[pd.Timestamp, set[str]]]] = {}
    for (code, date), group in frame.groupby(["index_code", "trade_date"], sort=True):
        snapshots.setdefault(str(code), []).append(
            (pd.Timestamp(date), set(group["symbol"].dropna().astype(str)))
        )
    all_symbols = sorted(set(frame["symbol"].dropna().astype(str)))
    mask = pd.DataFrame(False, index=dates, columns=all_symbols, dtype=bool)
    positions = {code: -1 for code in snapshots}
    active: dict[str, set[str]] = {code: set() for code in snapshots}
    for date in dates:
        for code, values in snapshots.items():
            position = positions[code]
            while position + 1 < len(values) and values[position + 1][0] <= date:
                position += 1
                active[code] = values[position][1]
            positions[code] = position
        members = set().union(*active.values()) if active else set()
        if members:
            mask.loc[date, list(members)] = True
    return mask


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
    records = [source.index_weights(code, start, end) for code in CSI800_INDEXES]
    frame = pd.concat(records, ignore_index=True) if records else pd.DataFrame()
    if frame.empty:
        raise RuntimeError("中证800历史成分为空；请检查 Tushare token 与 index_weight 权限")
    if calendar is None:
        calendar = source.trade_calendar(start, end)
    return build_membership_mask(frame, calendar)


def load_csi800_members_as_of(as_of: str, *, source=None) -> dict[str, Any]:
    """读取目标日可知的中证800成分，分别沿用两个指数最近一期快照。"""
    target = pd.Timestamp(as_of).normalize()
    if pd.isna(target):
        raise ValueError("查看日期需要使用 YYYY-MM-DD 格式")
    if source is None:
        from quantmaster.data.tushare_source import TushareSource

        source = TushareSource()
    start = (target - pd.DateOffset(months=4)).replace(day=1)
    members: set[str] = set()
    snapshot_dates: dict[str, str] = {}
    for index_code in CSI800_INDEXES:
        frame = source.index_weights(
            index_code, start.strftime("%Y-%m-%d"), target.strftime("%Y-%m-%d"))
        eligible = frame.loc[pd.to_datetime(frame.get("trade_date"), errors="coerce") <= target]
        if eligible.empty:
            raise RuntimeError(
                f"{index_code} 在 {target.date()} 前没有可用成分；请检查 Tushare token 与 index_weight 权限")
        latest = pd.to_datetime(eligible["trade_date"]).max().normalize()
        current = eligible.loc[pd.to_datetime(eligible["trade_date"]).dt.normalize() == latest]
        members.update(current["symbol"].dropna().astype(str))
        snapshot_dates[index_code] = latest.strftime("%Y-%m-%d")
    if not members:
        raise RuntimeError("中证800动态候选没有可用成分")
    return {
        "as_of": target.strftime("%Y-%m-%d"),
        "symbols": sorted(members),
        "snapshot_dates": snapshot_dates,
    }


def _bar_manifest(symbols: Iterable[str]) -> dict[str, Any]:
    from quantmaster.data.storage import BarStore

    store = BarStore()
    rows = []
    for symbol in sorted(set(symbols)):
        path = Path(store.root) / f"{symbol}.parquet"
        coverage = store.coverage(symbol)
        rows.append({
            "symbol": symbol,
            "coverage": list(coverage) if coverage else None,
            "bytes": path.stat().st_size if path.is_file() else 0,
            "mtime_ns": path.stat().st_mtime_ns if path.is_file() else 0,
        })
    return {"bars": rows, "manifest_hash": content_hash(rows)}


def _membership_manifest(membership: pd.DataFrame | None, symbols: list[str]) -> dict[str, Any]:
    """把布尔掩码压缩成稳定、可 JSON 序列化的快照描述。"""
    if membership is None:
        return {"kind": "fixed", "symbols": symbols}
    enabled = membership.fillna(False).astype(bool)
    snapshots = []
    previous: tuple[str, ...] | None = None
    for date, row in enabled.iterrows():
        current = tuple(str(symbol) for symbol in enabled.columns[row.to_numpy()])
        if current != previous:
            snapshots.append({
                "date": pd.Timestamp(date).strftime("%Y-%m-%d"),
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
    source, quality = "fixed", "sandbox"
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
    return {
        "tushare": {
            "configured": bool(cfg.data.tushare_token),
            "production_membership": bool(cfg.data.tushare_token),
        },
        "ml": {
            "torch": bool(importlib.util.find_spec("torch")),
            "sklearn": bool(importlib.util.find_spec("sklearn")),
            "onnxruntime": bool(importlib.util.find_spec("onnxruntime")),
        },
        "llm": {"configured": bool(cfg.llm.api_key), "provider": cfg.llm.provider},
    }
