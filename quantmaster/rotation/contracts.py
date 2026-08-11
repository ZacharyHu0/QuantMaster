"""Strict public contracts for the rotation domain."""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from quantmaster.runtime.contracts import ContractModel


class RotationRefreshRequest(ContractModel):
    scope: Literal["all", "close", "market", "industries", "themes", "etf"] = "all"
    mode: Literal["incremental", "rebuild"] = "incremental"
    source: Literal["auto", "local"] = "auto"
    as_of: str = Field(default="", pattern=r"^$|^\d{4}-\d{2}-\d{2}$")


class RotationPreferencesUpdate(ContractModel):
    l2_codes: list[str] = Field(default_factory=list, max_length=30)
    theme_limit: int = Field(default=16, ge=8, le=32)


class RotationJobSpec(ContractModel):
    scope: Literal["all", "close", "market", "industries", "themes", "etf"] = "all"
    mode: Literal["incremental", "rebuild"] = "incremental"
    source: Literal["auto", "local"] = "auto"
    as_of: str = Field(default="", pattern=r"^$|^\d{4}-\d{2}-\d{2}$")
