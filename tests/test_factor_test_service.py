"""Shared factor-test application module contracts."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from quantmaster.data.base import BarDataEnvelope, BarDataQuality
from quantmaster.data.universe import UniverseSnapshot


@pytest.mark.parametrize(
    "universe,symbols,refresh",
    [
        ("a-share", tuple(f"60000{index}.SH" for index in range(8)), False),
        ("domestic-etf", tuple(f"51030{index}.SH" for index in range(8)), True),
    ],
)
def test_factor_test_returns_the_same_pit_projection_for_supported_domestic_assets(
    panel,
    monkeypatch,
    universe,
    symbols,
    refresh,
):
    from quantmaster.factors import run_factor_test

    renamed = {field: frame.set_axis(symbols, axis="columns") for field, frame in panel.items()}
    snapshot = UniverseSnapshot(
        name=universe,
        symbols=symbols,
        observed_at="2023-07-28T15:30:00+08:00",
        effective_as_of="2023-07-28",
        content_hash=f"fixture-{universe}",
        source="fixture",
    )
    quality = BarDataQuality(
        status="verified",
        requested_start="2023-01-02",
        requested_end="2023-07-28",
        observed_start="2023-01-02",
        observed_end="2023-07-28",
        coverage_ratio=1.0,
        sources=("fixture",),
        requested_symbols=symbols,
        observed_symbols=symbols,
    )
    observed: dict[str, object] = {}

    def load_universe(name, *, as_of=None):
        observed["universe"] = (name, as_of)
        return snapshot

    def load_panel(mode, values, start, end):
        observed["panel"] = (mode, tuple(values), start, end)
        return BarDataEnvelope(renamed, quality, ({"source": "fixture"},))

    industry_evidence = {
        "status": "verified",
        "formal_eligible": True,
        "content_hash": f"industry-{universe}",
    }

    def load_industry(*, as_of=None):
        observed["industry_as_of"] = as_of
        return (
            {symbol: "行业A" if index < 4 else "行业B" for index, symbol in enumerate(symbols)},
            industry_evidence,
        )

    monkeypatch.setattr(
        "quantmaster.data.universe.load_universe_analysis_snapshot",
        load_universe,
    )
    monkeypatch.setattr(
        "quantmaster.data.read_panel",
        lambda values, start, end: load_panel("local", values, start, end),
    )
    monkeypatch.setattr(
        "quantmaster.data.refresh_panel",
        lambda values, start, end: load_panel("refresh", values, start, end),
    )
    monkeypatch.setattr(
        "quantmaster.data.industry.load_industry_analysis_context",
        load_industry,
    )

    result = run_factor_test(
        expression="mom_20d",
        universe=universe,
        start="2023-01-02",
        end="2023-07-28",
        quantiles=5,
        neutralize=True,
        refresh=refresh,
    )

    assert result["summary"] == {
        "name": "mom_20d",
        "ic_mean": 0.0169,
        "ic_std": 0.3759,
        "icir": 0.045,
        "ic_positive_ratio": 0.481,
        "long_short_annual": 0.3267,
        "monotonicity": 0.413,
        "top_quantile_turnover": 0.133,
        "quantile_annual": {
            "Q1": -0.2307,
            "Q2": -0.1205,
            "Q3": -0.4018,
            "Q4": -0.2886,
            "Q5": 0.104,
        },
    }
    assert (result["ic_series"][0], result["ic_series"][-1]) == (
        ["2023-02-03", -0.185714],
        ["2023-07-27", 0.109524],
    )
    assert (result["quantile_nav"]["Q1"][0], result["quantile_nav"]["Q1"][-1]) == (
        ["2023-01-30", 0.997445],
        ["2023-07-27", 0.8705],
    )
    assert result["neutralized"] is True
    assert result["data_quality"] == quality.to_dict()
    assert result["universe_evidence"] == snapshot.to_dict()
    assert result["industry_evidence"] == industry_evidence
    assert observed == {
        "universe": (universe, "2023-07-28"),
        "panel": ("refresh" if refresh else "local", symbols, "2023-01-02", "2023-07-28"),
        "industry_as_of": "2023-07-28",
    }


def test_factor_test_http_only_validates_and_returns_the_shared_projection(monkeypatch):
    from quantmaster.server.app import app

    projection = {
        "summary": {"name": "mom_20d", "ic_mean": 0.0312},
        "neutralized": False,
        "ic_series": [["2023-01-03", 0.1]],
        "quantile_nav": {"Q1": [["2023-01-03", 1.0]]},
        "data_quality": {"status": "verified"},
        "universe_evidence": {"name": "csi800"},
        "industry_evidence": None,
    }
    observed = []

    def run_factor_test(**kwargs):
        observed.append(kwargs)
        return projection

    monkeypatch.setattr("quantmaster.factors.run_factor_test", run_factor_test)

    client = TestClient(app)
    client.headers["X-CSRF-Token"] = client.get("/api/v1/session").json()["csrf_token"]
    response = client.post(
        "/api/v1/research/factors/test",
        json={
            "expression": "mom_20d",
            "universe": "csi800",
            "start": "2023-01-02",
            "end": "2023-07-28",
            "quantiles": 4,
            "neutralize": False,
        },
    )

    assert response.status_code == 200
    assert response.json() == projection
    assert observed == [
        {
            "expression": "mom_20d",
            "universe": "csi800",
            "start": "2023-01-02",
            "end": "2023-07-28",
            "quantiles": 4,
            "neutralize": False,
            "refresh": False,
        }
    ]
