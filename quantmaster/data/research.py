"""股票研究数据包：把信号价格、原始成交数据和 PIT 约束显式分离。"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, Literal

import numpy as np
import pandas as pd

from quantmaster.lab.models import content_hash

REQUIRED_SIGNAL_FIELDS = ("open", "high", "low", "close", "volume")
REQUIRED_EXECUTION_FIELDS = (
    "raw_open", "raw_high", "raw_low", "raw_close", "adj_factor",
    "up_limit", "down_limit", "suspended",
)


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
    source=None, progress=None,
) -> ResearchDataBundle:
    """从 Tushare 构造 production 股票研究包；任一标的失败都会显式阻断。"""
    if source is None:
        from quantmaster.data.tushare_source import TushareSource

        source = TushareSource()
    calendar = source.trade_calendar(start, end)
    signal_fields: dict[str, dict[str, pd.Series]] = {
        name: {} for name in (*REQUIRED_SIGNAL_FIELDS, "amount")
    }
    execution_fields: dict[str, dict[str, pd.Series]] = {
        name: {} for name in REQUIRED_EXECUTION_FIELDS
    }
    failures = []
    for number, symbol in enumerate(symbols, start=1):
        try:
            value = source.research_daily(symbol, start, end)
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
        if progress:
            progress(number, len(symbols), symbol, not bool(failures and failures[-1]["symbol"] == symbol))
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
    bundle.validate("production")
    return bundle
