from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

ETF_SCHEMA_VERSION = "3.0"
ETF_RESEARCH_MODEL_VERSION = "QM_ETF_SECTOR_RADAR_V3.4"


@dataclass(frozen=True, slots=True)
class EtfProfile:
    symbol: str
    name: str
    category: str
    asset_class: str
    sector_id: str
    sector_name: str
    benchmark: str = ""
    benchmark_code: str = ""
    benchmark_type: str = ""
    benchmark_level: str = ""
    index_type: str = ""
    index_provider: str = ""
    normalized_index: str = ""
    fund_type: str = ""
    invest_type: str = ""
    manager: str = ""
    custodian: str = ""
    management_fee: float | None = None
    metadata_source: str = "fund_basic"
    classification_source: str = "quantmaster:explicit-rules"
    classification_confidence: float = 0.0
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
    asset_class: str
    sector_id: str
    sector_name: str
    normalized_index: str
    benchmark_code: str
    is_representative: bool
    representative_symbol: str
    metrics: dict[str, Any]
    funds: dict[str, Any]
    metadata: dict[str, Any]
    coverage: dict[str, Any]
    provenance: dict[str, Any]
    as_of_date: str
    snapshot_id: str = ""
    ingest_id: str = ""
    artifact_id: str = ""
    research_model_version: str = ETF_RESEARCH_MODEL_VERSION

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
    sectors: tuple[dict[str, Any], ...]
    queues: dict[str, tuple[str, ...]]
    candidate_queues: dict[str, tuple[str, ...]]
    summaries: tuple[dict[str, Any], ...]
    freshness: dict[str, Any]
    capabilities: dict[str, Any]
    evidence_hashes: dict[str, str]
    categories: tuple[str, ...]
    input_hash: str
    schema_version: str = ETF_SCHEMA_VERSION
    research_model_version: str = ETF_RESEARCH_MODEL_VERSION
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
        if data.get("schema_version") != ETF_SCHEMA_VERSION:
            raise ValueError(
                f"ETF 快照 schema {data.get('schema_version') or '未知'} 已淘汰；请重新运行 ETF 研究"
            )
        if data.get("research_model_version") != ETF_RESEARCH_MODEL_VERSION:
            raise ValueError(
                f"ETF 研究模型 {data.get('research_model_version') or '未知'} 已淘汰；请重新运行 ETF 研究"
            )
        data["items"] = tuple(EtfResearchItem(**item) for item in data.get("items") or ())
        data["sectors"] = tuple(data.get("sectors") or ())
        data["summaries"] = tuple(data.get("summaries") or ())
        data["queues"] = {str(key): tuple(items) for key, items in (data.get("queues") or {}).items()}
        data["candidate_queues"] = {
            str(key): tuple(items) for key, items in (data.get("candidate_queues") or {}).items()
        }
        data["categories"] = tuple(data.get("categories") or ())
        return cls(**data)
