from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

ETF_SCHEMA_VERSION = "1.0"
ETF_SCORE_VERSION = "QM_ETF_RESEARCH_V1"


@dataclass(frozen=True, slots=True)
class EtfProfile:
    symbol: str
    name: str
    category: str
    benchmark: str = ""
    fund_type: str = ""
    invest_type: str = ""
    list_date: str = ""
    status: str = "listed"
    classification_evidence: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class EtfResearchItem:
    symbol: str
    name: str
    category: str
    category_rank: int | None
    score: float | None
    rankable: bool
    excluded_reason: str
    metrics: dict[str, Any]
    minute_evidence: dict[str, Any]
    shares_effective_date: str
    share_lag_sessions: int | None
    coverage: dict[str, Any]
    provenance: dict[str, Any]
    as_of_date: str
    snapshot_id: str = ""
    ingest_id: str = ""
    artifact_id: str = ""
    share_semantic_status: str = "unavailable"
    score_version: str = ETF_SCORE_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class EtfResearchSnapshot:
    snapshot_id: str
    ingest_id: str
    artifact_id: str
    as_of_date: str
    coverage: dict[str, Any]
    provenance: dict[str, Any]
    items: tuple[EtfResearchItem, ...]
    categories: tuple[str, ...]
    input_hash: str
    schema_version: str = ETF_SCHEMA_VERSION
    score_version: str = ETF_SCORE_VERSION
    generated_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat(timespec="seconds"))
    staleness: dict[str, Any] = field(
        default_factory=lambda: {
            "stale": False,
            "reason": "",
            "last_attempt_at": "",
        }
    )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> EtfResearchSnapshot:
        data = dict(value)
        data["items"] = tuple(EtfResearchItem(**item) for item in data.get("items") or ())
        data["categories"] = tuple(data.get("categories") or ())
        return cls(**data)
