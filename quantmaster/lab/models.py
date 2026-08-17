"""Quant Lab 的稳定领域类型。

这里刻意不依赖 Web 或 PyTorch：核心安装也能浏览、编辑和验证因子，
深度学习只是可选执行后端。
"""

from __future__ import annotations

import hashlib
import unicodedata
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal

from quantmaster.lab.horizons import SUPPORTED_HORIZONS
from quantmaster.runtime.json import strict_json_dumps

FactorKind = Literal["expression", "python", "learned", "latent", "composite"]
FactorStatus = Literal[
    "draft", "validating", "candidate", "approved", "production", "degraded", "archived"
]
JobStatus = Literal[
    "queued", "running", "paused", "completed", "completed_with_warnings",
    "failed", "cancelled", "interrupted",
]


class DataPolicy(StrEnum):
    """Controls whether a research operation may contact external providers."""

    LOCAL_ONLY = "local_only"
    PREFER_LOCAL = "prefer_local"
    REFRESH_MISSING = "refresh_missing"


class ResourceClass(StrEnum):
    IO = "io"
    CPU = "cpu"
    GPU = "gpu"
    EXTERNAL = "external"


ReadinessState = Literal["ready", "degraded", "blocked"]

FACTOR_STATUSES = {
    "draft", "validating", "candidate", "approved", "production", "degraded", "archived",
}
JOB_STATUSES = {
    "queued", "running", "paused", "completed", "completed_with_warnings",
    "failed", "cancelled", "interrupted",
}


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def canonical_json(value: Any) -> str:
    return strict_json_dumps(value, sort_keys=True)


def content_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def normalize_factor_name(value: str) -> str:
    """Normalize display whitespace without changing the user's visible script."""
    return " ".join(unicodedata.normalize("NFC", str(value or "")).split())


def factor_name_key(value: str) -> str:
    """Return the Unicode-aware comparison key used for factor-name uniqueness."""
    return unicodedata.normalize("NFKC", normalize_factor_name(value)).casefold()


@dataclass(frozen=True)
class FactorSpec:
    slug: str
    name: str
    kind: FactorKind = "expression"
    expression: str = ""
    description: str = ""
    category: str = "未分类"
    direction: int = 1
    required_features: tuple[str, ...] = ()
    horizons: tuple[int, ...] = SUPPORTED_HORIZONS
    rationale: str = ""
    model: dict[str, Any] = field(default_factory=dict)
    artifact: dict[str, Any] = field(default_factory=dict)
    components: tuple[dict[str, Any], ...] = ()
    tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.slug or len(self.slug) > 120:
            raise ValueError("因子标识必须为 1–120 个字符")
        normalized_name = normalize_factor_name(self.name)
        if not normalized_name or len(normalized_name) > 120:
            raise ValueError("因子名称必须为 1–120 个字符")
        object.__setattr__(self, "name", normalized_name)
        if self.kind not in {"expression", "python", "learned", "latent", "composite"}:
            raise ValueError(f"未知因子类型: {self.kind}")
        if self.kind == "expression" and not self.expression and self.slug not in {
            "ep", "bp", "dividend_yield", "small_cap", "roe", "news_sentiment",
        }:
            raise ValueError("表达式因子必须提供 expression")
        if self.kind == "python" and not self.artifact.get("manifest"):
            raise ValueError("Python 因子必须引用不可变工件清单")
        if self.direction not in {-1, 1}:
            raise ValueError("direction 只允许 -1 或 1")
        if not self.horizons or any(value not in SUPPORTED_HORIZONS for value in self.horizons):
            raise ValueError("horizons 只支持 1/3/5/7/10/20/30 日")

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        for key in ("required_features", "horizons", "components", "tags"):
            value[key] = list(value[key])
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> FactorSpec:
        data = dict(value)
        for key in ("required_features", "horizons", "components", "tags"):
            if key in data:
                data[key] = tuple(data[key] or ())
        return cls(**data)


@dataclass(frozen=True)
class DatasetSnapshot:
    universe: str
    start: str
    end: str
    symbols: tuple[str, ...]
    feature_version: str = "lab-v1"
    membership_source: str = "fixed"
    research_quality: Literal["production", "sandbox"] = "sandbox"
    as_of: str = ""
    state: Literal["ready", "stale", "incomplete", "corrupt"] = "ready"
    data_policy: str = DataPolicy.PREFER_LOCAL.value
    production_eligible: bool = False
    warnings: tuple[dict[str, Any], ...] = ()
    manifest: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["symbols"] = list(self.symbols)
        value["warnings"] = list(self.warnings)
        value["snapshot_hash"] = content_hash(value)
        return value


@dataclass(frozen=True)
class SuggestionPatch:
    base_version_id: str
    expression: str
    rationale: str
    expected_effect: str = ""
    risks: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["risks"] = list(self.risks)
        return value
