from scripts.ci import exception_policy


def test_market_overview_exception_budget_moves_without_expansion():
    assert exception_policy.BASELINE["quantmaster/server/app.py"] == 12
    assert exception_policy.BASELINE["quantmaster/market/overview.py"] == 2
    assert sum(exception_policy.BASELINE.values()) == 191
    assert exception_policy.analyze() == []
