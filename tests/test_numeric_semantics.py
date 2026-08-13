from __future__ import annotations

import pandas as pd
import pytest

from quantmaster.data.base import BarDataQuality, normalize_bars
from quantmaster.data.semantics import (
    NumericSemantics,
    PriceType,
    RatioScale,
    SemanticContractError,
)
from quantmaster.research.providers import build_future_continuous


def semantics(**changes) -> NumericSemantics:
    values = {
        "instrument": "600000.SH",
        "observation_time": "exchange_session",
        "price_type": PriceType.RAW,
        "currency": "CNY",
        "price_unit": "CNY/share",
        "volume_unit": "share",
        "amount_unit": "CNY",
        "provider": "tushare",
        "provider_interface": "daily",
        "intended_use": "research",
    }
    values.update(changes)
    return NumericSemantics(**values)


def test_price_and_ratio_vocabularies_are_explicit():
    assert {item.value for item in PriceType} == {
        "raw", "forward_adjusted", "backward_adjusted", "total_return",
        "continuous_futures",
    }
    assert {item.value for item in RatioScale} == {
        "decimal", "percent_points", "basis_points",
    }


def test_provider_merge_rejects_price_currency_volume_and_fx_conflicts():
    base = semantics()
    for changed in (
        {"price_type": PriceType.FORWARD_ADJUSTED},
        {"currency": "USD", "price_unit": "USD/share", "amount_unit": "USD"},
        {"volume_unit": "lot"},
        {"base_currency": "USD", "quote_currency": "CNY", "fx_method": "session_close"},
    ):
        with pytest.raises(SemanticContractError, match="拒绝拼接"):
            base.require_mergeable(semantics(**changed))


def test_adjusted_and_fx_contracts_fail_closed_when_evidence_is_missing():
    adjusted = semantics(
        price_type=PriceType.FORWARD_ADJUSTED,
        factor_coverage="partial",
        adjustment_provider_definition="raw*factor/anchor",
    )
    with pytest.raises(SemanticContractError, match="complete_factor_chain"):
        adjusted.require_formal()
    fx = semantics(base_currency="USD", quote_currency="CNY")
    with pytest.raises(SemanticContractError, match="fx_method"):
        fx.require_formal()


def test_quality_formal_gate_requires_numeric_semantics():
    quality = BarDataQuality("verified", "2026-01-01", "2026-01-02")
    assert quality.formal_eligible is False
    assert BarDataQuality(
        "verified", "2026-01-01", "2026-01-02", semantics=semantics(),
    ).formal_eligible is True


def test_yahoo_adjusted_close_is_not_silently_renamed_to_close():
    frame = normalize_bars(pd.DataFrame({
        "Date": ["2026-01-02"], "Close": [100.0], "Adj Close": [80.0],
        "Open": [99.0], "High": [101.0], "Low": [98.0], "Volume": [10.0],
    }))
    assert frame.loc[pd.Timestamp("2026-01-02"), "close"] == 100.0
    assert frame.loc[pd.Timestamp("2026-01-02"), "adjusted_close"] == 80.0


def test_continuous_futures_require_specs_and_remain_research_only():
    bars = pd.DataFrame({
        "trade_date": ["2026-01-02"], "symbol": ["IF2601.CFX"],
        "close": [4000.0], "settle": [3999.0],
    })
    incomplete = pd.DataFrame({
        "trade_date": ["2026-01-02"], "symbol": ["IF.CFX"],
        "mapping_ts_code": ["IF2601.CFX"],
    })
    with pytest.raises(ValueError, match="缺少合约行情或主力映射字段"):
        build_future_continuous(bars, incomplete)
    mapping = incomplete.assign(
        exchange="CFFEX", currency="CNY", quote_unit="index_point",
        contract_multiplier=300.0, tick_size=0.2,
        roll_method="previous_overlap_settlement_ratio",
    )
    result = build_future_continuous(bars, mapping)
    assert result.loc[0, "mapping_ts_code"] == "IF2601.CFX"
    assert result.loc[0, "price_type"] == "continuous_futures"
    assert result.loc[0, "intended_use"] == "research_only_not_tradable"
    assert result.loc[0, "contract_multiplier"] == 300.0
