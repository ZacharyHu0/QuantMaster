from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

SCHEMA_VERSION = "1.2"
SCORE_VERSION = "QM_AFTER_CLOSE_V1"
SHADOW_SCORE_VERSION = "QM_AFTER_CLOSE_V2_SHADOW"


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


@dataclass(frozen=True, slots=True)
class SectorRank:
    code: str
    name: str
    level: str
    category: str
    score: float
    rank: int
    return_5d: float | None
    return_20d: float | None
    relative_20d: float | None
    breadth_20d: float | None
    amount_change: float | None
    eligible_members: int
    total_members: int
    coverage: float | None
    candidate_symbols: tuple[str, ...] = ()
    as_of_date: str = ""
    snapshot_id: str = ""
    score_version: str = SCORE_VERSION
    provenance: dict[str, Any] = field(default_factory=dict)
    staleness: dict[str, Any] = field(default_factory=lambda: {
        "stale": False, "reason": "", "last_attempt_at": "",
    })
    sensitivity: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ResearchCandidate:
    symbol: str
    name: str
    rank: int
    score: float
    sectors: tuple[dict[str, str], ...]
    metrics: dict[str, float | None]
    reasons: tuple[str, ...]
    exclusion_rules: tuple[str, ...]
    as_of_date: str
    coverage: dict[str, Any]
    provenance: dict[str, Any]
    staleness: dict[str, Any]
    snapshot_id: str = ""
    score_version: str = SCORE_VERSION
    shadow: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AfterCloseSnapshot:
    snapshot_id: str
    as_of_date: str
    input_hash: str
    filters: dict[str, Any]
    coverage: dict[str, Any]
    provenance: dict[str, Any]
    sectors: tuple[SectorRank, ...]
    candidates: tuple[ResearchCandidate, ...]
    excluded_counts: dict[str, int]
    shadow_candidates: tuple[ResearchCandidate, ...] = ()
    validation: dict[str, Any] = field(default_factory=dict)
    ingest_id: str = ""
    artifact_id: str = ""
    score_version: str = SCORE_VERSION
    schema_version: str = SCHEMA_VERSION
    generated_at: str = field(default_factory=utc_now)
    staleness: dict[str, Any] = field(default_factory=lambda: {
        "stale": False, "reason": "", "last_attempt_at": "",
    })

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> AfterCloseSnapshot:
        data = dict(value)
        data["sectors"] = tuple(SectorRank(**item) for item in data.get("sectors") or ())
        data["candidates"] = tuple(
            ResearchCandidate(
                **{
                    **item,
                    "sectors": tuple(item.get("sectors") or ()),
                    "reasons": tuple(item.get("reasons") or ()),
                    "exclusion_rules": tuple(item.get("exclusion_rules") or ()),
                }
            )
            for item in data.get("candidates") or ()
        )
        data["shadow_candidates"] = tuple(
            ResearchCandidate(
                **{
                    **item,
                    "sectors": tuple(item.get("sectors") or ()),
                    "reasons": tuple(item.get("reasons") or ()),
                    "exclusion_rules": tuple(item.get("exclusion_rules") or ()),
                }
            )
            for item in data.get("shadow_candidates") or ()
        )
        return cls(**data)
