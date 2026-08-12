"""Strict public contracts for the rotation domain."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from quantmaster.runtime.contracts import ContractModel


class RotationRefreshRequest(ContractModel):
    scope: Literal["all", "close", "market", "industries", "themes", "etf"] = "all"
    mode: Literal["incremental", "rebuild"] = "incremental"
    source: Literal["auto", "local"] = "auto"
    as_of: str = Field(default="", pattern=r"^$|^\d{4}-\d{2}-\d{2}$")
    purpose: Literal[
        "display", "current_analysis", "historical_replay", "formal_research",
    ] = "current_analysis"
    knowledge_cutoff: str = ""
    taxonomy_id: str = Field(default="", max_length=120)

    @model_validator(mode="after")
    def validate_temporal_contract(self):
        if self.purpose in {"historical_replay", "formal_research"}:
            if not self.as_of:
                raise ValueError("历史用途必须指定 as_of")
            if not self.knowledge_cutoff:
                raise ValueError("历史用途必须指定 knowledge_cutoff")
            if not self.taxonomy_id:
                raise ValueError("历史用途必须指定 taxonomy_id")
        return self


class RotationPreferencesUpdate(ContractModel):
    l2_codes: list[str] = Field(default_factory=list, max_length=30)


class RotationJobSpec(ContractModel):
    scope: Literal["all", "close", "market", "industries", "themes", "etf"] = "all"
    mode: Literal["incremental", "rebuild"] = "incremental"
    source: Literal["auto", "local"] = "auto"
    as_of: str = Field(default="", pattern=r"^$|^\d{4}-\d{2}-\d{2}$")
    purpose: Literal[
        "display", "current_analysis", "historical_replay", "formal_research",
    ] = "current_analysis"
    knowledge_cutoff: str = ""
    taxonomy_id: str = Field(default="", max_length=120)

    @model_validator(mode="after")
    def validate_temporal_contract(self):
        if self.purpose in {"historical_replay", "formal_research"}:
            if not self.as_of:
                raise ValueError("历史用途必须指定 as_of")
            if not self.knowledge_cutoff:
                raise ValueError("历史用途必须指定 knowledge_cutoff")
            if not self.taxonomy_id:
                raise ValueError("历史用途必须指定 taxonomy_id")
        return self
