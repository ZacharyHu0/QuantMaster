from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from quantmaster.analysis.stock import StockAnalysisService
from quantmaster.analysis.stock_research import StockAnalysisSpec, StockResearchEngine
from quantmaster.backtest.engine import run_backtest
from quantmaster.backtest.paper_accounts import PaperStore
from quantmaster.backtest.spec import PaperAccountSpec
from quantmaster.data.base import BarDataEnvelope, BarDataQuality
from quantmaster.data.semantics import NumericSemantics, PriceType
from quantmaster.data.universe import load_universe_snapshot, save_universe
from quantmaster.market_capabilities import (
    MarketCapability,
    MarketCapabilityError,
    assess_formal_research_evidence,
    require_market_capability,
)
from quantmaster.portfolio.ledger import Ledger, TradeRecord
from quantmaster.research.contracts import AssetClass
from quantmaster.research.engine import ResearchEngine
from quantmaster.research.lake import ResearchLake


def _instrument(symbol: str, market: str, asset_type: str = "stock") -> dict[str, str]:
    return {"symbol": symbol, "market": market, "asset_type": asset_type}


def _formal_hk_envelope() -> BarDataEnvelope[pd.DataFrame]:
    dates = pd.bdate_range("2025-01-02", periods=180)
    close = np.linspace(300.0, 420.0, len(dates))
    bars = pd.DataFrame(
        {
            "open": close - 1,
            "high": close + 2,
            "low": close - 2,
            "close": close,
            "volume": np.linspace(10_000_000, 15_000_000, len(dates)),
            "amount": close * np.linspace(10_000_000, 15_000_000, len(dates)),
            "turnover": np.linspace(0.5, 1.2, len(dates)),
        },
        index=dates,
    )
    semantics = NumericSemantics(
        instrument="00700.HK",
        observation_time="exchange_session_close",
        price_type=PriceType.RAW,
        currency="HKD",
        price_unit="HKD/share",
        volume_unit="share",
        amount_unit="HKD",
        provider="fixture",
        provider_interface="daily",
        intended_use="research",
    )
    return BarDataEnvelope(
        bars,
        BarDataQuality(
            status="verified",
            requested_start=str(dates.min().date()),
            requested_end=str(dates.max().date()),
            observed_start=str(dates.min().date()),
            observed_end=str(dates.max().date()),
            coverage_ratio=1.0,
            calendar_source="hkex:fixture-v1",
            sources=("fixture",),
            timezone="Asia/Hong_Kong",
            adjustment="none",
            semantics=semantics,
        ),
        ({"source": "fixture", "contract": "hk-daily-v1"},),
    )


def _paper_spec() -> PaperAccountSpec:
    return PaperAccountSpec.model_validate(
        {
            "name": "market-boundary",
            "strategy": {
                "kind": "factor",
                "factor": "rank(close)",
                "top_n": 1,
                "rebalance": "D",
                "weighting": "equal",
                "cap_weight": 0.35,
            },
            "universe": "demo",
            "initial_capital": 100_000,
            "mode": "manual",
        }
    )


def test_market_matrix_keeps_hk_research_only_and_reference_markets_read_only() -> None:
    for instrument in (
        _instrument("600519.SH", "CN"),
        _instrument("510300.SH", "CN", "etf"),
    ):
        for capability in (
            MarketCapability.FORMAL_RESEARCH,
            MarketCapability.BACKTEST,
            MarketCapability.PAPER_ACCOUNT,
            MarketCapability.LEDGER_EXECUTION,
        ):
            require_market_capability(instrument, capability)

    hk = _instrument("00700.HK", "HK")
    assert require_market_capability(hk, MarketCapability.CANDIDATE).timezone == (
        "Asia/Hong_Kong"
    )
    require_market_capability(hk, MarketCapability.FORMAL_RESEARCH)

    for capability in (
        MarketCapability.BACKTEST,
        MarketCapability.PAPER_ACCOUNT,
        MarketCapability.LEDGER_EXECUTION,
    ):
        with pytest.raises(MarketCapabilityError, match="HK 市场不支持"):
            require_market_capability(hk, capability)
    with pytest.raises(MarketCapabilityError, match="US 市场不支持 formal_research"):
        require_market_capability(
            _instrument("AAPL.US", "US"), MarketCapability.FORMAL_RESEARCH,
        )
    with pytest.raises(MarketCapabilityError, match="FUT 市场不支持 formal_research"):
        require_market_capability(
            _instrument("GC.CONTINUOUS", "FUT", "future_continuous"),
            MarketCapability.FORMAL_RESEARCH,
        )
    with pytest.raises(MarketCapabilityError, match="未知市场身份"):
        require_market_capability(
            _instrument("MYSTERY.X", "UNKNOWN"), MarketCapability.QUOTES,
        )


def test_hk_formal_research_requires_timezone_calendar_and_provenance() -> None:
    envelope = _formal_hk_envelope()
    instrument = _instrument("00700.HK", "HK")
    quality = envelope.quality.to_dict()

    eligible = assess_formal_research_evidence(instrument, quality, envelope.provenance)
    assert eligible.formal_eligible is True
    assert eligible.to_dict() == {
        "market": "hk",
        "timezone": "Asia/Hong_Kong",
        "formal_eligible": True,
        "reasons": [],
    }

    quality["timezone"] = "unknown"
    quality["calendar_source"] = "unavailable"
    blocked = assess_formal_research_evidence(instrument, quality, ())
    assert blocked.formal_eligible is False
    assert set(blocked.reasons) == {
        "market_timezone_unverified",
        "market_calendar_unverified",
        "market_provenance_missing",
    }


def test_hk_candidate_snapshot_is_preserved_but_reference_candidates_are_rejected(
    isolated_config,
) -> None:
    save_universe("hk_research", ["00700.HK"])
    assert load_universe_snapshot("hk_research").symbols == ("00700.HK",)

    with pytest.raises(MarketCapabilityError, match="US 市场不支持 candidate"):
        save_universe("us_reference", ["AAPL.US"])


def test_hk_research_report_carries_formal_market_evidence() -> None:
    envelope = _formal_hk_envelope()
    symbol = "00700.HK"
    service = StockAnalysisService(
        resolver=lambda _query: {
            "status": "resolved",
            "instrument": {
                **_instrument(symbol, "HK"),
                "code": "00700",
                "name": "腾讯控股",
                "exchange": "HKEX",
                "currency": "HKD",
            },
        },
        history_loader=lambda *_args, **_kwargs: envelope,
        fundamental_loader=lambda *_args: {},
        news_loader=lambda *_args: [],
        capital_loader=lambda *_args: {},
        industry_loader=lambda *_args: "",
        llm_factory=None,
    )

    report = StockResearchEngine(service).run(StockAnalysisSpec("00700.HK", "quick"))

    assert report["instrument"]["symbol"] == symbol
    assert report["research"]["formal_eligible"] is True
    assert report["research"]["market_boundary"]["market"] == "hk"
    assert report["research"]["artifacts"]


@pytest.mark.parametrize("symbol", ["00700.HK", "AAPL.US", "GC.CONTINUOUS"])
def test_backtest_rejects_non_cn_execution_semantics(symbol: str) -> None:
    dates = pd.bdate_range("2026-01-05", periods=3)
    panel = {
        "open": pd.DataFrame({symbol: [10.0, 10.1, 10.2]}, index=dates),
        "close": pd.DataFrame({symbol: [10.0, 10.1, 10.2]}, index=dates),
    }
    weights = pd.DataFrame({symbol: [1.0, 1.0, 1.0]}, index=dates)

    with pytest.raises(MarketCapabilityError, match="不支持 backtest"):
        run_backtest(panel, weights)


@pytest.mark.parametrize("symbol", ["00700.HK", "AAPL.US", "GC.CONTINUOUS"])
def test_ledger_rejects_non_cn_trade_without_writing(symbol: str, tmp_path) -> None:
    ledger = Ledger(tmp_path / "ledger.sqlite")

    with pytest.raises(MarketCapabilityError, match="不支持 ledger_execution"):
        ledger.add_trade(TradeRecord("2026-01-05", symbol, "buy", 10, 100))

    assert ledger.trades().empty


def test_paper_account_rejects_hk_before_account_or_cashflow_write(tmp_path) -> None:
    account_root = tmp_path / "accounts"
    store = PaperStore(tmp_path / "paper.sqlite", account_root)

    with pytest.raises(MarketCapabilityError, match="HK 市场不支持 paper_account"):
        store.create_account(_paper_spec(), symbols=["00700.HK"])

    assert store.accounts() == []
    assert list(account_root.iterdir()) == []


def test_formal_research_lake_rejects_reference_only_futures(tmp_path) -> None:
    engine = ResearchEngine(lake=ResearchLake(tmp_path / "research"))

    with pytest.raises(MarketCapabilityError, match="FUT 市场不支持 formal_research"):
        engine.plan(
            "2024-01-02",
            "2024-01-05",
            asset_classes=(AssetClass.FUTURE,),
        )
