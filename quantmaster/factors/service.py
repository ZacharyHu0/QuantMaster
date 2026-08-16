"""Factor-test application module shared by local entrypoints."""

from __future__ import annotations

import pandas as pd


def _series_to_points(series: pd.Series) -> list[list]:
    return [[str(key.date()), round(float(value), 6)] for key, value in series.dropna().items()]


def run_factor_test(
    expression: str,
    *,
    universe: str = "demo",
    start: str = "2022-01-01",
    end: str | None = None,
    quantiles: int = 5,
    neutralize: bool = False,
    refresh: bool = False,
) -> dict:
    """Evaluate one factor against PIT evidence and return its diagnostic projection."""
    from quantmaster import data as data_api
    from quantmaster.data.universe import load_universe_analysis_snapshot
    from quantmaster.factors.analysis import analyze_factor
    from quantmaster.factors.engine import compute_factor
    from quantmaster.factors.fundamental import resolve_factor
    from quantmaster.trading_sessions import default_close_data_end

    evidence_as_of = end
    resolved_end = default_close_data_end(end)
    universe_snapshot = load_universe_analysis_snapshot(universe, as_of=evidence_as_of)
    symbols = list(universe_snapshot.symbols)
    factor = resolve_factor(expression, symbols, start, resolved_end)
    market_envelope = (
        data_api.refresh_panel if refresh else data_api.read_panel
    )(symbols, start, resolved_end)
    panel = market_envelope.require_data()
    values = compute_factor(factor, panel)
    neutralized = False
    industry_evidence = None
    if neutralize:
        from quantmaster.data.industry import load_industry_analysis_context
        from quantmaster.factors.neutral import industry_neutralize

        mapping, industry_evidence = load_industry_analysis_context(as_of=evidence_as_of)
        if mapping:
            values = industry_neutralize(values, mapping)
            neutralized = True
    report = analyze_factor(values, panel["close"], name=factor.name, quantiles=quantiles)
    return {
        "summary": report.summary(),
        "neutralized": neutralized,
        "ic_series": _series_to_points(report.ic_series.rolling(20, min_periods=5).mean()),
        "quantile_nav": {
            column: _series_to_points(report.quantile_returns[column])
            for column in report.quantile_returns.columns
        },
        "data_quality": market_envelope.quality.to_dict(),
        "universe_evidence": universe_snapshot.to_dict(),
        "industry_evidence": industry_evidence,
    }
