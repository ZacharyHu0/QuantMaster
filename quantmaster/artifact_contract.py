"""Stable artifact references shared by research and factor consumers."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any

from quantmaster.research_primitives import ArtifactKind, AssetClass, Frequency

_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,119}$")
_VERSION_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9.-]+)?$")


def _enum_value(value: Any) -> Any:
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, tuple):
        return [_enum_value(item) for item in value]
    if isinstance(value, list):
        return [_enum_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _enum_value(item) for key, item in value.items()}
    return value


@dataclass(frozen=True)
class ArtifactRef:
    kind: ArtifactKind
    id: str
    version: str
    asset_class: AssetClass
    frequency: Frequency = Frequency.DAILY
    output: str = "value"

    def __post_init__(self) -> None:
        if not _ID_RE.fullmatch(self.id) or not _VERSION_RE.fullmatch(self.version):
            raise ValueError("ArtifactRef 的 id/version 非法")

    @property
    def storage_column(self) -> str:
        version = re.sub(r"[^A-Za-z0-9]+", "_", self.version).strip("_")
        return f"{self.id}__v{version}"

    def to_dict(self) -> dict[str, Any]:
        return _enum_value(asdict(self))

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ArtifactRef:
        data = dict(value)
        data["kind"] = ArtifactKind(data["kind"])
        data["asset_class"] = AssetClass(data["asset_class"])
        data["frequency"] = Frequency(data.get("frequency", "1d"))
        return cls(**data)
