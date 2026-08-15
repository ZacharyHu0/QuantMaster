from scripts.ci import exception_policy


def test_market_overview_exception_budget_moves_without_expansion():
    assert exception_policy.BASELINE["quantmaster/server/app.py"] == 1
    assert exception_policy.BASELINE["quantmaster/server/capabilities.py"] == 11
    assert exception_policy.BASELINE["quantmaster/market/overview.py"] == 2
    assert sum(exception_policy.BASELINE.values()) == 196
    assert exception_policy.analyze() == []
