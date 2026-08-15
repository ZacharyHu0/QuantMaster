"""Executable market boundaries shared by research and account workflows."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from quantmaster.data.base import Market, guess_market


class MarketCapability(StrEnum):
    IDENTITY = "identity"
    CALENDAR = "calendar"
    QUOTES = "quotes"
    CANDIDATE = "candidate"
    FORMAL_RESEARCH = "formal_research"
    BACKTEST = "backtest"
    PAPER_ACCOUNT = "paper_account"
    LEDGER_EXECUTION = "ledger_execution"


class MarketCapabilityError(ValueError):
    """A market or asset tried to cross an unsupported product boundary."""


@dataclass(frozen=True)
class MarketCapabilityProfile:
    market: Market
    timezone: str | None
    asset_types: frozenset[str]
    capabilities: frozenset[MarketCapability]

    def to_dict(self) -> dict[str, Any]:
        return {
            "market": self.market.value,
            "timezone": self.timezone,
            "asset_types": sorted(self.asset_types),
            "capabilities": sorted(item.value for item in self.capabilities),
        }


_REFERENCE = frozenset({MarketCapability.IDENTITY, MarketCapability.QUOTES})
_RESEARCH = frozenset({
    MarketCapability.IDENTITY,
    MarketCapability.CALENDAR,
    MarketCapability.QUOTES,
    MarketCapability.CANDIDATE,
    MarketCapability.FORMAL_RESEARCH,
})
_FULL = frozenset({
    *_RESEARCH,
    MarketCapability.BACKTEST,
    MarketCapability.PAPER_ACCOUNT,
    MarketCapability.LEDGER_EXECUTION,
})

MARKET_CAPABILITY_MATRIX: dict[Market, MarketCapabilityProfile] = {
    Market.CN: MarketCapabilityProfile(
        Market.CN,
        "Asia/Shanghai",
        frozenset({"stock", "etf", "fund"}),
        _FULL,
    ),
    Market.HK: MarketCapabilityProfile(
        Market.HK,
        "Asia/Hong_Kong",
        frozenset({"stock"}),
        _RESEARCH,
    ),
    Market.US: MarketCapabilityProfile(
        Market.US,
        "America/New_York",
        frozenset({"stock", "etf"}),
        _REFERENCE,
    ),
    Market.FUTURES: MarketCapabilityProfile(
        Market.FUTURES,
        None,
        frozenset({"future_contract", "future_continuous"}),
        _REFERENCE,
    ),
    Market.INDEX: MarketCapabilityProfile(
        Market.INDEX, None, frozenset({"index"}), _REFERENCE,
    ),
    Market.FOREX: MarketCapabilityProfile(
        Market.FOREX, None, frozenset({"forex"}), _REFERENCE,
    ),
    Market.JP: MarketCapabilityProfile(
        Market.JP, None, frozenset({"stock", "etf"}), _REFERENCE,
    ),
    Market.KR: MarketCapabilityProfile(
        Market.KR, None, frozenset({"stock", "etf"}), _REFERENCE,
    ),
}


def _field(value: object, name: str) -> str:
    if isinstance(value, Mapping):
        return str(value.get(name) or "").strip()
    return str(getattr(value, name, "") or "").strip()


def _market(value: object) -> Market:
    explicit = _field(value, "market")
    if explicit:
        try:
            return Market(explicit.lower())
        except ValueError:
            raise MarketCapabilityError(f"未知市场身份: {explicit}") from None
    symbol = _field(value, "symbol") or str(value).strip()
    try:
        return guess_market(symbol)
    except ValueError as exc:
        raise MarketCapabilityError(str(exc)) from None


def market_capability_profile(value: object) -> MarketCapabilityProfile:
    market = _market(value)
    try:
        return MARKET_CAPABILITY_MATRIX[market]
    except KeyError:
        raise MarketCapabilityError(f"未知市场能力边界: {market.value}") from None


def require_market_capability(
    value: object,
    capability: MarketCapability,
) -> MarketCapabilityProfile:
    profile = market_capability_profile(value)
    asset_type = _field(value, "asset_type").lower()
    if asset_type and asset_type not in profile.asset_types:
        raise MarketCapabilityError(
            f"{profile.market.value.upper()} 市场的 {asset_type} 不支持 {capability.value}"
        )
    if capability not in profile.capabilities:
        symbol = _field(value, "symbol") or str(value).strip()
        raise MarketCapabilityError(
            f"{profile.market.value.upper()} 市场不支持 {capability.value}: {symbol}"
        )
    return profile


def require_symbols_capability(
    symbols: Iterable[str],
    capability: MarketCapability,
) -> tuple[MarketCapabilityProfile, ...]:
    values = tuple(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols))
    if not values:
        raise MarketCapabilityError(f"{capability.value} 缺少已确认市场身份")
    return tuple(require_market_capability(symbol, capability) for symbol in values)


@dataclass(frozen=True)
class FormalResearchEligibility:
    market: Market
    timezone: str
    formal_eligible: bool
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "market": self.market.value,
            "timezone": self.timezone,
            "formal_eligible": self.formal_eligible,
            "reasons": list(self.reasons),
        }


def assess_formal_research_evidence(
    instrument: object,
    data_quality: Mapping[str, Any],
    provenance: Iterable[object],
) -> FormalResearchEligibility:
    profile = require_market_capability(instrument, MarketCapability.FORMAL_RESEARCH)
    reasons: list[str] = []
    timezone = str(data_quality.get("timezone") or "").strip()
    calendar_source = str(data_quality.get("calendar_source") or "").strip().lower()
    if data_quality.get("formal_eligible") is not True:
        reasons.append("market_data_not_formal")
    if not profile.timezone or timezone != profile.timezone:
        reasons.append("market_timezone_unverified")
    if calendar_source in {"", "unknown", "unavailable"}:
        reasons.append("market_calendar_unverified")
    if not tuple(provenance):
        reasons.append("market_provenance_missing")
    return FormalResearchEligibility(
        profile.market,
        profile.timezone or "",
        not reasons,
        tuple(dict.fromkeys(reasons)),
    )
