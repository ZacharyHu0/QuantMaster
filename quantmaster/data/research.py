"""股票研究数据包：把信号价格、原始成交数据和 PIT 约束显式分离。"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Literal

import numpy as np
import pandas as pd

from quantmaster.lab.models import content_hash

REQUIRED_SIGNAL_FIELDS = ("open", "high", "low", "close", "volume")
REQUIRED_EXECUTION_FIELDS = (
    "raw_open", "raw_high", "raw_low", "raw_close", "adj_factor",
    "up_limit", "down_limit", "suspended",
)
_PIT_COLUMNS = (
    *REQUIRED_SIGNAL_FIELDS, "amount", *REQUIRED_EXECUTION_FIELDS,
)


class PitDataStore:
    """Production PIT inputs stored as one validated Parquet file per symbol.

    The underlying :class:`BarStore` already provides cross-process locks, content
    hashes, interrupted-write recovery and atomic replacement.  PIT data has a
    different schema from ordinary bars, but it has the same durability needs.
    """

    def __init__(self, root=None) -> None:
        from quantmaster.config import get_config
        from quantmaster.data.storage import BarStore

        self._store = BarStore(root or get_config().data_root / "pit_execution")

    def get(self, symbol: str) -> pd.DataFrame | None:
        return self._store.get(symbol)

    def put(self, symbol: str, frame: pd.DataFrame) -> None:
        self._store.put(symbol, frame)
        if not frame.empty:
            self._store.mark_checked(
                symbol, str(frame.index.min().date()), str(frame.index.max().date()),
                source="tushare:production-pit",
            )

    @staticmethod
    def reusable_for(end: str) -> bool:
        """Today's close may have changed; let the provider's 15:30 cache rule refresh it."""
        try:
            return pd.Timestamp(end).date() < date.today()
        except (TypeError, ValueError):
            return False


def _flatten_symbol(value: dict[str, pd.DataFrame], calendar: pd.DatetimeIndex) -> pd.DataFrame:
    """Make one source response persistable without losing the raw/PIT distinction."""
    frame = pd.DataFrame(index=calendar)
    for signal_field in REQUIRED_SIGNAL_FIELDS:
        if signal_field in value["signal"]:
            frame[signal_field] = value["signal"][signal_field]
    if "amount" in value["signal"]:
        frame["amount"] = value["signal"]["amount"]
    for raw_field, column in (
        ("raw_open", "open"), ("raw_high", "high"),
        ("raw_low", "low"), ("raw_close", "close"),
    ):
        frame[raw_field] = value["raw"][column]
    frame["adj_factor"] = value["adj_factor"]["adj_factor"]
    frame["up_limit"] = value["limits"]["up_limit"]
    frame["down_limit"] = value["limits"]["down_limit"]
    frame["suspended"] = value["suspended"]["suspended"].reindex(calendar)
    return frame.reindex(columns=_PIT_COLUMNS)


def _cached_missing_dates(
    frame: pd.DataFrame | None,
    calendar: pd.DatetimeIndex,
    member: pd.Series | None,
) -> pd.DatetimeIndex:
    if frame is None or not set(_PIT_COLUMNS).issubset(frame.columns):
        return calendar
    value = frame.reindex(calendar)
    active = (
        member.reindex(calendar).fillna(False).astype(bool)
        if member is not None else pd.Series(True, index=calendar)
    ) & ~value["suspended"].fillna(False).astype(bool)
    missing = value["suspended"].isna()
    fields = [field for field in _PIT_COLUMNS if field != "suspended"]
    missing |= active & value.loc[:, fields].isna().any(axis=1)
    return calendar[missing.to_numpy()]


def _unflatten_symbol(frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        "signal": frame.loc[:, [field for field in (*REQUIRED_SIGNAL_FIELDS, "amount") if field in frame]],
        "raw": frame.loc[:, ["raw_open", "raw_high", "raw_low", "raw_close"]].rename(columns={
            "raw_open": "open", "raw_high": "high", "raw_low": "low", "raw_close": "close",
        }),
        "adj_factor": frame.loc[:, ["adj_factor"]],
        "limits": frame.loc[:, ["up_limit", "down_limit"]],
        "suspended": frame.loc[:, ["suspended"]],
    }


def frame_fingerprint(frame: pd.DataFrame) -> str:
    """对二维研究输入生成包含索引、列和空值位置的稳定内容指纹。"""
    digest = hashlib.sha256()
    digest.update("\x1f".join(map(str, frame.columns)).encode("utf-8"))
    digest.update(pd.util.hash_pandas_object(frame, index=True).to_numpy().tobytes())
    return digest.hexdigest()


@dataclass
class ResearchDataBundle:
    signal: dict[str, pd.DataFrame]
    execution: dict[str, pd.DataFrame]
    membership: pd.DataFrame | None = None
    fundamentals: dict[str, pd.DataFrame] = field(default_factory=dict)
    context: dict[str, pd.DataFrame] = field(default_factory=dict)
    calendar: pd.DatetimeIndex = field(default_factory=lambda: pd.DatetimeIndex([]))
    tier: Literal["production", "sandbox"] = "sandbox"
    warnings: list[dict[str, str]] = field(default_factory=list)
    manifest: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_legacy_panel(
        cls, panel: dict[str, pd.DataFrame], *, membership: pd.DataFrame | None = None,
    ) -> ResearchDataBundle:
        close = panel["close"]
        execution = {
            "raw_open": panel.get("open", close),
            "raw_high": panel.get("high", close),
            "raw_low": panel.get("low", close),
            "raw_close": close,
            "adj_factor": pd.DataFrame(1.0, index=close.index, columns=close.columns),
            "up_limit": pd.DataFrame(np.nan, index=close.index, columns=close.columns),
            "down_limit": pd.DataFrame(np.nan, index=close.index, columns=close.columns),
            "suspended": panel.get(
                "suspended", pd.DataFrame(False, index=close.index, columns=close.columns),
            ),
        }
        return cls(
            signal={key: value for key, value in panel.items() if isinstance(value, pd.DataFrame)},
            execution=execution,
            membership=membership,
            calendar=pd.DatetimeIndex(close.index),
            tier="sandbox",
            warnings=[{
                "code": "sandbox_execution_approximation",
                "level": "warning",
                "message": "使用旧前复权行情和代码板涨跌停近似，结果不得晋升为生产候选。",
            }],
        )

    def validate(self, requested_tier: Literal["production", "sandbox"] = "production") -> dict[str, Any]:
        missing_signal = [name for name in REQUIRED_SIGNAL_FIELDS if name not in self.signal]
        missing_execution = [name for name in REQUIRED_EXECUTION_FIELDS if name not in self.execution]
        missing_membership = self.membership is None
        missing_calendar = len(self.calendar) == 0
        blockers = [f"signal:{name}" for name in missing_signal]
        blockers.extend(f"execution:{name}" for name in missing_execution)
        if missing_membership:
            blockers.append("pit_membership")
        if missing_calendar:
            blockers.append("exchange_calendar")
        if requested_tier == "production" and not missing_calendar and not missing_membership:
            membership = self.membership.reindex(
                index=self.calendar, columns=self.membership.columns,
            ).fillna(False)
            suspended = self.execution.get("suspended", pd.DataFrame()).reindex_like(
                membership,
            )
            if suspended.isna().any().any():
                blockers.append("coverage:suspended")
            eligible = membership & ~suspended.fillna(False).astype(bool)
            for name in REQUIRED_SIGNAL_FIELDS:
                frame = self.signal.get(name)
                if frame is not None:
                    aligned = frame.reindex(index=self.calendar, columns=membership.columns)
                    missing = int((eligible & aligned.isna()).sum().sum())
                    if missing:
                        blockers.append(f"coverage:signal:{name}:{missing}")
            for name in REQUIRED_EXECUTION_FIELDS:
                if name == "suspended":
                    continue
                frame = self.execution.get(name)
                if frame is not None:
                    aligned = frame.reindex(index=self.calendar, columns=membership.columns)
                    missing = int((eligible & aligned.isna()).sum().sum())
                    if missing:
                        blockers.append(f"coverage:execution:{name}:{missing}")
        if requested_tier == "production" and (self.tier != "production" or blockers):
            detail = "、".join(blockers or ["research_tier"])
            raise ValueError(f"生产研究数据门禁未通过：{detail}")
        return {
            "tier": self.tier,
            "requested_tier": requested_tier,
            "blockers": blockers,
            "warnings": list(self.warnings),
            "manifest_hash": self.manifest_hash,
        }

    def backtest_panel(self) -> dict[str, pd.DataFrame]:
        """返回兼容现有策略计算、同时携带真实成交约束的面板。"""
        panel = dict(self.signal)
        panel.update({
            "execution_open": self.execution.get("raw_open"),
            "execution_close": self.execution.get("raw_close"),
            "adj_factor": self.execution.get("adj_factor"),
            "up_limit": self.execution.get("up_limit"),
            "down_limit": self.execution.get("down_limit"),
            "suspended": self.execution.get("suspended"),
        })
        return panel

    @property
    def manifest_hash(self) -> str:
        if self.manifest.get("manifest_hash"):
            return str(self.manifest["manifest_hash"])
        close = self.signal.get("close", pd.DataFrame())
        payload = {
            "tier": self.tier,
            "dates": [
                pd.Timestamp(close.index.min()).strftime("%Y-%m-%d") if not close.empty else "",
                pd.Timestamp(close.index.max()).strftime("%Y-%m-%d") if not close.empty else "",
            ],
            "symbols": list(map(str, close.columns)),
            "signal_fields": sorted(self.signal),
            "execution_fields": sorted(self.execution),
            "fundamental_fields": sorted(self.fundamentals),
            "context_fields": sorted(self.context),
            "source_manifest": {
                key: value for key, value in self.manifest.items() if key != "manifest_hash"
            },
        }
        return content_hash(payload)


def load_research_bundle(
    symbols: list[str], start: str, end: str, *, membership: pd.DataFrame | None,
    source=None, progress=None, store: PitDataStore | None = None,
) -> ResearchDataBundle:
    """Load a production bundle, filling only PIT data absent from local storage."""
    if source is None:
        from quantmaster.data.tushare_source import TushareSource

        source = TushareSource()
    calendar = source.trade_calendar(start, end)
    store = store or PitDataStore()
    signal_fields: dict[str, dict[str, pd.Series]] = {
        name: {} for name in (*REQUIRED_SIGNAL_FIELDS, "amount")
    }
    execution_fields: dict[str, dict[str, pd.Series]] = {
        name: {} for name in REQUIRED_EXECUTION_FIELDS
    }
    failures = []
    cache_hits = 0
    downloaded = 0
    for number, symbol in enumerate(symbols, start=1):
        try:
            cached = store.get(symbol)
            member = membership[symbol] if membership is not None and symbol in membership else None
            missing_dates = (
                _cached_missing_dates(cached, calendar, member)
                if store.reusable_for(end) else calendar
            )
            if len(missing_dates) == 0:
                value = _unflatten_symbol(cached.reindex(calendar))
                cache_hits += 1
                success = True
                detail = "本地 PIT 命中"
            else:
                fetch_start = pd.Timestamp(missing_dates.min()).strftime("%Y-%m-%d")
                fetch_end = pd.Timestamp(missing_dates.max()).strftime("%Y-%m-%d")
                fetch_calendar = calendar[
                    (calendar >= pd.Timestamp(fetch_start)) & (calendar <= pd.Timestamp(fetch_end))
                ]
                value = source.research_daily(
                    symbol, fetch_start, fetch_end, calendar=fetch_calendar,
                )
                store.put(symbol, _flatten_symbol(value, fetch_calendar))
                cached = store.get(symbol)
                if _cached_missing_dates(cached, calendar, member).size:
                    raise RuntimeError(f"{symbol} PIT 缓存写入后仍存在缺口")
                value = _unflatten_symbol(cached.reindex(calendar))
                downloaded += 1
                success = True
                detail = "下载 PIT 缺口"
            for field in signal_fields:
                if field in value["signal"]:
                    signal_fields[field][symbol] = value["signal"][field]
            for field, column in (
                ("raw_open", "open"), ("raw_high", "high"),
                ("raw_low", "low"), ("raw_close", "close"),
            ):
                execution_fields[field][symbol] = value["raw"][column]
            execution_fields["adj_factor"][symbol] = value["adj_factor"]["adj_factor"]
            execution_fields["up_limit"][symbol] = value["limits"]["up_limit"]
            execution_fields["down_limit"][symbol] = value["limits"]["down_limit"]
            execution_fields["suspended"][symbol] = value["suspended"]["suspended"]
        except Exception as exc:
            failures.append({"symbol": symbol, "error": str(exc)[:300]})
            success = False
            detail = "PIT 数据失败"
        if progress:
            try:
                progress(number, len(symbols), symbol, success, detail)
            except TypeError:
                # Preserve injected progress callbacks from older integrations.
                progress(number, len(symbols), symbol, success)
    if failures:
        examples = "；".join(f"{item['symbol']}: {item['error']}" for item in failures[:5])
        raise RuntimeError(f"production 原始成交数据缺失 {len(failures)} 只：{examples}")
    signal = {
        name: pd.DataFrame(values).reindex(calendar)
        for name, values in signal_fields.items() if values
    }
    execution = {
        name: pd.DataFrame(values).reindex(calendar)
        for name, values in execution_fields.items() if values
    }
    if "suspended" in execution:
        execution["suspended"] = execution["suspended"].fillna(False).astype(bool)
    bundle = ResearchDataBundle(
        signal=signal, execution=execution, membership=membership,
        calendar=calendar, tier="production",
        manifest={
            "source": "tushare:daily+adj_factor+stk_limit+suspend_d+trade_cal",
            "symbols": len(symbols), "start": start, "end": end,
            "data_hashes": {
                "signal": {name: frame_fingerprint(frame) for name, frame in signal.items()},
                "execution": {
                    name: frame_fingerprint(frame) for name, frame in execution.items()
                },
                "membership": frame_fingerprint(membership) if membership is not None else "",
            },
        },
    )
    bundle.manifest["manifest_hash"] = bundle.manifest_hash
    # Cache statistics describe this load only and intentionally do not alter the
    # immutable input manifest hash used to reproduce a completed run.
    bundle.manifest["pit_cache"] = {
        "dataset": "pit_execution/v1", "hits": cache_hits, "downloaded": downloaded,
        "estimated_requests": downloaded * 4,
    }
    bundle.validate("production")
    return bundle
