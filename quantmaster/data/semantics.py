"""Explicit numeric semantics at provider, cache and computation boundaries.

Values in this module are deliberately readable business dimensions.  They are
not inferred from magnitudes and are never replaced by an opaque validation tag.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any


class PriceType(StrEnum):
    RAW = "raw"
    FORWARD_ADJUSTED = "forward_adjusted"
    BACKWARD_ADJUSTED = "backward_adjusted"
    TOTAL_RETURN = "total_return"
    CONTINUOUS_FUTURES = "continuous_futures"


class RatioScale(StrEnum):
    DECIMAL = "decimal"
    PERCENT_POINTS = "percent_points"
    BASIS_POINTS = "basis_points"


class SemanticContractError(ValueError):
    """Numeric values were about to cross an incompatible semantic boundary."""


@dataclass(frozen=True)
class NumericSemantics:
    """Minimum domain identity required before bars may be merged or computed."""

    instrument: str
    observation_time: str
    price_type: PriceType
    currency: str
    price_unit: str
    volume_unit: str
    amount_unit: str
    provider: str
    provider_interface: str
    adjustment_anchor_date: str = ""
    adjustment_provider_definition: str = ""
    adjustment_published_at: str = ""
    adjustment_company_actions: str = ""
    factor_coverage: str = "not_applicable"
    base_currency: str = ""
    quote_currency: str = ""
    fx_method: str = ""
    exchange: str = ""
    contract: str = ""
    quote_unit: str = ""
    contract_multiplier: float | None = None
    tick_size: float | None = None
    settlement_field: str = ""
    roll_method: str = ""
    intended_use: str = "display"

    def __post_init__(self) -> None:
        if not self.instrument.strip() or not self.provider.strip() or not self.provider_interface.strip():
            raise SemanticContractError("数值语义必须包含 instrument/provider/provider_interface")
        if bool(self.base_currency) != bool(self.quote_currency):
            raise SemanticContractError("外汇 base_currency/quote_currency 必须成对出现")

    @property
    def merge_identity(self) -> tuple[Any, ...]:
        """All dimensions which must agree before cross-provider concatenation."""
        return (
            self.instrument, self.observation_time, self.price_type.value,
            self.currency, self.price_unit, self.volume_unit, self.amount_unit,
            self.adjustment_anchor_date, self.adjustment_provider_definition,
            self.adjustment_company_actions, self.factor_coverage,
            self.base_currency, self.quote_currency, self.fx_method,
            self.exchange, self.contract, self.quote_unit, self.contract_multiplier,
            self.tick_size, self.settlement_field, self.roll_method, self.intended_use,
        )

    def require_mergeable(self, other: NumericSemantics) -> None:
        if self.merge_identity != other.merge_identity:
            names = (
                "instrument", "observation_time", "price_type", "currency", "price_unit",
                "volume_unit", "amount_unit", "adjustment_anchor_date",
                "adjustment_provider_definition", "adjustment_company_actions",
                "factor_coverage", "base_currency", "quote_currency", "fx_method",
                "exchange", "contract", "quote_unit", "contract_multiplier", "tick_size",
                "settlement_field", "roll_method", "intended_use",
            )
            differences = [
                name
                for name, left, right in zip(
                    names, self.merge_identity, other.merge_identity, strict=True,
                )
                if left != right
            ]
            raise SemanticContractError("拒绝拼接数值语义冲突字段：" + "、".join(differences))

    def require_formal(self) -> None:
        missing = [name for name, value in (
            ("observation_time", self.observation_time), ("currency", self.currency),
            ("price_unit", self.price_unit), ("volume_unit", self.volume_unit),
        ) if not value or value == "unknown"]
        if self.base_currency and not self.fx_method:
            missing.append("fx_method")
        if self.price_type in {PriceType.FORWARD_ADJUSTED, PriceType.BACKWARD_ADJUSTED,
                               PriceType.TOTAL_RETURN}:
            if self.factor_coverage != "complete":
                missing.append("complete_factor_chain")
            for name, value in (
                ("adjustment_provider_definition", self.adjustment_provider_definition),
                ("adjustment_company_actions", self.adjustment_company_actions),
            ):
                if not value:
                    missing.append(name)
        if self.price_type == PriceType.CONTINUOUS_FUTURES:
            missing.append("tradable_contract (continuous series is research-only)")
            if not self.roll_method:
                missing.append("roll_method")
        if self.contract and self.intended_use in {"paper_trading", "portfolio"}:
            for name, value in (("contract_multiplier", self.contract_multiplier),
                                ("tick_size", self.tick_size), ("quote_unit", self.quote_unit)):
                if value in (None, ""):
                    missing.append(name)
        if missing:
            raise SemanticContractError("正式计算缺少数值语义：" + "、".join(missing))

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["price_type"] = self.price_type.value
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> NumericSemantics:
        data = dict(value)
        data["price_type"] = PriceType(data["price_type"])
        return cls(**data)
