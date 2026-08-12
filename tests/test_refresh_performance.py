"""Offline performance budgets for the generation-aware refresh kernel."""

from __future__ import annotations

import pytest

from scripts.dev.benchmark_refresh import run_benchmark


@pytest.mark.full
def test_refresh_benchmark_noop_and_vectorized_theme_budget():
    report = run_benchmark(
        scenario="all", runs=2, days=120, symbols=160, groups=240,
    )
    values = {item["scenario"]: item for item in report["results"]}

    # The fixture is intentionally smaller than a production A-share panel,
    # but it catches a regression back to per-group pandas scan loops and any
    # no-op path that invokes a provider.
    assert values["noop"]["remote_calls"] == 0
    assert values["noop"]["p95_ms"] <= 1_000
    assert values["cold"]["max_ms"] <= 8_000
