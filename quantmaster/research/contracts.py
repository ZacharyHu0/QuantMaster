"""Versioned contracts for reproducible, cross-asset research artifacts."""

from __future__ import annotations

import enum
import hashlib
import re
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from quantmaster.artifact_contract import ArtifactRef
from quantmaster.research_primitives import ArtifactKind, AssetClass, Frequency
from quantmaster.runtime.json import strict_json_dumps


class _ValueEnum(enum.StrEnum):
    def __str__(self) -> str:
        return self.value


class CapabilityState(_ValueEnum):
    AVAILABLE = "available"
    INSTALLED = "installed"
    DATA_READY = "data_ready"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"
    UNSUPPORTED_BY_SYMBOL = "unsupported_by_symbol"
    MISSING_PERMISSION = "missing_permission"
    UNCONFIGURED = "unconfigured"
    TEMPORARY_FAILURE = "temporary_failure"


class KernelBackend(_ValueEnum):
    AUTO = "auto"
    PYTHON = "python"
    RUST = "rust"


_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,119}$")
_VERSION_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9.-]+)?$")


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def canonical_json(value: Any) -> str:
    return strict_json_dumps(value, sort_keys=True, default=str)


def content_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _enum_value(value: Any) -> Any:
    if isinstance(value, enum.Enum):
        return value.value
    if isinstance(value, tuple):
        return [_enum_value(item) for item in value]
    if isinstance(value, list):
        return [_enum_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _enum_value(item) for key, item in value.items()}
    return value


@dataclass(frozen=True)
class DataRequest:
    """A point-in-time input requirement declared by a provider."""

    dataset_id: str
    columns: tuple[str, ...] = ()
    lookback_sessions: int = 0
    lookahead_sessions: int = 0
    required: bool = True

    def __post_init__(self) -> None:
        if not _ID_RE.fullmatch(self.dataset_id):
            raise ValueError(f"非法数据集标识: {self.dataset_id!r}")
        if self.lookback_sessions < 0 or self.lookahead_sessions < 0:
            raise ValueError("lookback/lookahead 不能为负数")
        if len(set(self.columns)) != len(self.columns):
            raise ValueError(f"数据请求 {self.dataset_id} 包含重复列")

    def to_dict(self) -> dict[str, Any]:
        return _enum_value(asdict(self))

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> DataRequest:
        data = dict(value)
        data["columns"] = tuple(data.get("columns") or ())
        return cls(**data)


@dataclass(frozen=True)
class ResearchSpec:
    """Immutable metadata for one factor, label, risk exposure or model output."""

    id: str
    version: str
    kind: ArtifactKind
    asset_classes: tuple[AssetClass, ...]
    frequency: Frequency = Frequency.DAILY
    name: str = ""
    aliases: tuple[str, ...] = ()
    description: str = ""
    tags: tuple[str, ...] = ()
    provider_id: str = ""
    output: str = "value"
    dependencies: tuple[DataRequest, ...] = ()
    lookback_sessions: int = 0
    lookahead_sessions: int = 0
    min_tushare_points: int = 0
    source_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not _ID_RE.fullmatch(self.id):
            raise ValueError(f"非法研究标识: {self.id!r}")
        if not _VERSION_RE.fullmatch(self.version):
            raise ValueError(f"版本必须使用 semantic versioning: {self.version!r}")
        if not self.asset_classes:
            raise ValueError("asset_classes 不能为空")
        if len(set(self.asset_classes)) != len(self.asset_classes):
            raise ValueError("asset_classes 不能重复")
        if self.lookback_sessions < 0 or self.lookahead_sessions < 0:
            raise ValueError("lookback/lookahead 不能为负数")
        if self.kind == ArtifactKind.FACTOR and self.lookahead_sessions:
            raise ValueError("因子禁止声明前看窗口")
        if self.kind == ArtifactKind.LABEL and self.lookahead_sessions <= 0:
            raise ValueError("标签必须声明正的前看窗口")
        if self.provider_id and not _ID_RE.fullmatch(self.provider_id):
            raise ValueError(f"非法 provider 标识: {self.provider_id!r}")
        if not self.output or self.output in {"trade_date", "symbol", "event_time_utc"}:
            raise ValueError(f"非法输出列: {self.output!r}")

    @property
    def storage_column(self) -> str:
        version = re.sub(r"[^A-Za-z0-9]+", "_", self.version).strip("_")
        return f"{self.id}__v{version}"

    @property
    def spec_hash(self) -> str:
        return content_hash(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return _enum_value(asdict(self))

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ResearchSpec:
        data = dict(value)
        data["kind"] = ArtifactKind(data["kind"])
        data["asset_classes"] = tuple(AssetClass(item) for item in data["asset_classes"])
        data["frequency"] = Frequency(data.get("frequency", "1d"))
        for key in ("aliases", "tags", "source_refs"):
            data[key] = tuple(data.get(key) or ())
        data["dependencies"] = tuple(
            DataRequest.from_dict(item) for item in data.get("dependencies") or ()
        )
        return cls(**data)


@dataclass(frozen=True)
class FactorProviderSpec:
    """A provider can compute several outputs from one shared input scan."""

    id: str
    outputs: tuple[ResearchSpec, ...]
    dependencies: tuple[DataRequest, ...]
    asset_classes: tuple[AssetClass, ...]
    frequency: Frequency = Frequency.DAILY
    stateful: bool = False

    def __post_init__(self) -> None:
        if not _ID_RE.fullmatch(self.id):
            raise ValueError(f"非法 provider 标识: {self.id!r}")
        if not self.outputs:
            raise ValueError("provider 至少需要一个输出")
        if len({item.id for item in self.outputs}) != len(self.outputs):
            raise ValueError(f"provider {self.id} 输出标识重复")
        for output in self.outputs:
            if output.provider_id and output.provider_id != self.id:
                raise ValueError(f"{output.id} 的 provider_id 与 {self.id} 不一致")

    @property
    def lookback_sessions(self) -> int:
        values = [item.lookback_sessions for item in self.outputs]
        values.extend(item.lookback_sessions for item in self.dependencies)
        return max(values, default=0)

    @property
    def lookahead_sessions(self) -> int:
        values = [item.lookahead_sessions for item in self.outputs]
        values.extend(item.lookahead_sessions for item in self.dependencies)
        return max(values, default=0)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "outputs": [item.to_dict() for item in self.outputs],
            "dependencies": [item.to_dict() for item in self.dependencies],
            "asset_classes": [item.value for item in self.asset_classes],
            "frequency": self.frequency.value,
            "stateful": self.stateful,
            "lookback_sessions": self.lookback_sessions,
            "lookahead_sessions": self.lookahead_sessions,
        }


@dataclass(frozen=True)
class PlanTask:
    kind: str
    dataset_id: str
    asset_class: AssetClass
    frequency: Frequency
    trade_date: str
    columns: tuple[str, ...] = ()
    provider_ids: tuple[str, ...] = ()
    reason: str = "missing"

    @property
    def key(self) -> str:
        return ":".join((self.kind, self.asset_class.value, self.frequency.value,
                         self.dataset_id, self.trade_date))

    def to_dict(self) -> dict[str, Any]:
        return _enum_value(asdict(self))

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> PlanTask:
        data = dict(value)
        data["asset_class"] = AssetClass(data["asset_class"])
        data["frequency"] = Frequency(data.get("frequency", "1d"))
        data["columns"] = tuple(data.get("columns") or ())
        data["provider_ids"] = tuple(data.get("provider_ids") or ())
        return cls(**data)


@dataclass(frozen=True)
class ExecutionPlan:
    id: str
    start: str
    end: str
    target_dates: tuple[str, ...]
    asset_classes: tuple[AssetClass, ...]
    frequency: Frequency
    datasets: tuple[str, ...]
    selected_specs: tuple[ArtifactRef, ...]
    tasks: tuple[PlanTask, ...]
    backend: KernelBackend = KernelBackend.AUTO
    warnings: tuple[str, ...] = ()
    capability_blocks: tuple[dict[str, Any], ...] = ()
    estimated_rows: int = 0
    estimated_bytes: int = 0
    created_at: str = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        start, end = date.fromisoformat(self.start), date.fromisoformat(self.end)
        if start > end:
            raise ValueError("plan.start 不能晚于 plan.end")
        if len({task.key for task in self.tasks}) != len(self.tasks):
            raise ValueError("执行计划包含重复任务")

    @property
    def plan_hash(self) -> str:
        return content_hash(self._logical_payload())

    def _logical_payload(self) -> dict[str, Any]:
        """Content that changes execution semantics, excluding runtime identity."""
        value = self.to_dict(include_hash=False)
        value.pop("id", None)
        value.pop("created_at", None)
        return value

    def to_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        value = {
            "id": self.id,
            "start": self.start,
            "end": self.end,
            "target_dates": list(self.target_dates),
            "asset_classes": [item.value for item in self.asset_classes],
            "frequency": self.frequency.value,
            "datasets": list(self.datasets),
            "selected_specs": [item.to_dict() for item in self.selected_specs],
            "tasks": [item.to_dict() for item in self.tasks],
            "backend": self.backend.value,
            "warnings": list(self.warnings),
            "capability_blocks": list(self.capability_blocks),
            "estimated_rows": self.estimated_rows,
            "estimated_bytes": self.estimated_bytes,
            "created_at": self.created_at,
        }
        if include_hash:
            value["plan_hash"] = content_hash({
                key: item for key, item in value.items()
                if key not in {"id", "created_at"}
            })
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ExecutionPlan:
        data = dict(value)
        data.pop("plan_hash", None)
        data["target_dates"] = tuple(data.get("target_dates") or ())
        data["asset_classes"] = tuple(AssetClass(item) for item in data["asset_classes"])
        data["frequency"] = Frequency(data.get("frequency", "1d"))
        data["datasets"] = tuple(data.get("datasets") or ())
        data["selected_specs"] = tuple(
            ArtifactRef.from_dict(item) for item in data.get("selected_specs") or ()
        )
        data["tasks"] = tuple(PlanTask.from_dict(item) for item in data.get("tasks") or ())
        data["backend"] = KernelBackend(data.get("backend", "auto"))
        data["warnings"] = tuple(data.get("warnings") or ())
        data["capability_blocks"] = tuple(data.get("capability_blocks") or ())
        return cls(**data)


@dataclass(frozen=True)
class RunManifest:
    run_id: str
    plan_hash: str
    status: str
    backend_requested: KernelBackend
    backend_used: KernelBackend
    started_at: str
    finished_at: str = ""
    input_partitions: tuple[dict[str, Any], ...] = ()
    output_partitions: tuple[dict[str, Any], ...] = ()
    warnings: tuple[str, ...] = ()
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        value = _enum_value(asdict(self))
        value["manifest_hash"] = content_hash(value)
        return value
