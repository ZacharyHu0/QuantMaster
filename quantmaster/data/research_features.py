"""AutoMiner 可见的版本化研究特征注册表。"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

import pandas as pd

from quantmaster.data.research import ResearchDataBundle

PITGrade = Literal["strict", "derived", "research_only"]


@dataclass(frozen=True)
class FeatureDescriptor:
    name: str
    group: str
    description: str
    pit_grade: PITGrade
    available: bool
    coverage: float
    runtime_compatible: bool = True

    def to_dict(self) -> dict:
        return asdict(self)


def registered_features(
    bundle: ResearchDataBundle,
) -> tuple[dict[str, pd.DataFrame], list[FeatureDescriptor]]:
    """只暴露显式注册的二维时序；任意文件、账本和凭据永不进入特征包。"""
    close = bundle.signal.get("close", pd.DataFrame())
    denominator = max(1, int(close.notna().sum().sum()))
    values: dict[str, pd.DataFrame] = {}
    descriptors: list[FeatureDescriptor] = []

    def add(name: str, frame: pd.DataFrame | None, group: str, description: str,
            grade: PITGrade, runtime_compatible: bool = True) -> None:
        available = (
            isinstance(frame, pd.DataFrame) and not frame.empty
            and bool(frame.notna().any().any())
        )
        aligned = frame.reindex(index=close.index, columns=close.columns) if available else None
        coverage = float(aligned.notna().sum().sum()) / denominator if available else 0.0
        descriptors.append(FeatureDescriptor(
            name, group, description, grade, available, round(coverage, 6),
            runtime_compatible,
        ))
        if available:
            values[name] = aligned

    descriptions = {
        "open": "前复权开盘价", "high": "前复权最高价", "low": "前复权最低价",
        "close": "前复权收盘价", "volume": "成交量", "amount": "成交额",
        "turnover": "换手率", "vwap": "成交量加权均价", "returns": "日收益率",
    }
    for name in ("open", "high", "low", "close", "volume", "amount", "turnover", "vwap", "returns"):
        add(name, bundle.signal.get(name), "price_volume_v2", descriptions[name], "derived")
    for name, frame in sorted(bundle.fundamentals.items()):
        add(name, frame, "pit_fundamental_v1", f"按公告日可得的 {name}",
            "strict" if bundle.tier == "production" else "research_only")
    for name, frame in sorted(bundle.context.items()):
        add(name, frame, "market_context_v1", f"本地市场上下文 {name}", "research_only")
    for name in ("raw_open", "raw_high", "raw_low", "raw_close", "adj_factor",
                 "up_limit", "down_limit", "suspended"):
        add(name, bundle.execution.get(name), "execution_v1", f"真实成交约束 {name}",
            "strict" if bundle.tier == "production" else "research_only", False)
    if bundle.membership is not None:
        add("membership", bundle.membership.astype(float), "market_context_v1",
            "当日 PIT 指数成分", "strict")
    add("news_sentiment", bundle.signal.get("news_sentiment"), "news_v1",
        "按首次见闻时间对齐的新闻情绪", "strict")
    return values, descriptors


def feature_catalog(bundle: ResearchDataBundle) -> list[dict]:
    return [item.to_dict() for item in registered_features(bundle)[1]]
