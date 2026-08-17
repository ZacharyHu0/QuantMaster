"""Bridge versioned research-lake outputs into existing factor/backtest strategies."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from quantmaster.data.research_access import research_lake
from quantmaster.factors.artifact_contract import ArtifactKind, ArtifactRef, AssetClass, Frequency
from quantmaster.factors.base import Factor, PanelDict

_ARTIFACT_RE = re.compile(
    r"^artifact:(?P<kind>factor|factors|label|labels|risk|model|models):"
    r"(?P<asset>stock|etf|future):(?P<id>[A-Za-z][A-Za-z0-9_.-]{0,119})"
    r"@(?P<version>[0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9.-]+)?)$"
)


def parse_artifact_reference(value: str) -> ArtifactRef | None:
    match = _ARTIFACT_RE.fullmatch(value.strip())
    if not match:
        return None
    aliases = {
        "factor": ArtifactKind.FACTOR, "factors": ArtifactKind.FACTOR,
        "label": ArtifactKind.LABEL, "labels": ArtifactKind.LABEL,
        "risk": ArtifactKind.RISK,
        "model": ArtifactKind.MODEL, "models": ArtifactKind.MODEL,
    }
    return ArtifactRef(
        kind=aliases[match.group("kind")], id=match.group("id"),
        version=match.group("version"), asset_class=AssetClass(match.group("asset")),
        frequency=Frequency.DAILY,
    )


class ArtifactFactor(Factor):
    def __init__(self, reference: ArtifactRef, lake: Any | None = None):
        if reference.kind not in {ArtifactKind.FACTOR, ArtifactKind.RISK, ArtifactKind.MODEL}:
            raise ValueError("回测信号只能引用 factor/risk/model 产物")
        self.reference = reference
        self.name = f"artifact:{reference.id}@{reference.version}"
        self.description = "版本固定的 ResearchLake 产物"
        self.lake = lake or research_lake()

    def compute(self, panel: PanelDict) -> pd.DataFrame:
        close = panel.get("close")
        if close is None or close.empty:
            raise ValueError("ArtifactFactor 需要 close 面板确定日期与标的")
        start, end = str(close.index.min().date()), str(close.index.max().date())
        values = self.lake.artifact_panel(self.reference, start, end)
        if values.empty:
            raise ValueError(
                f"研究产物不存在：{self.reference.id}@{self.reference.version} "
                f"({self.reference.asset_class.value})"
            )
        values.index = pd.DatetimeIndex(pd.to_datetime(values.index), name=close.index.name)
        return values.reindex(index=close.index, columns=close.columns)
