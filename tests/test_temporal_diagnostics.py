from quantmaster.operational_diagnostics import _market_session_metrics


def test_partial_stockdb_status_has_stable_session_diagnostic() -> None:
    metrics = _market_session_metrics({
        "target_session": "2026-08-13", "updated_at": "2026-08-13T18:33:49+08:00",
        "validation": {
            "target_session": "2026-08-13", "actual_session": "2026-08-13",
            "accepted": True, "complete": False, "symbol_ratio": 0.991155,
            "missing_symbol_count": 49,
        },
    })

    assert metrics["CN"]["completion_state"] == "current_session_partial"
    assert metrics["CN"]["diagnostic_code"] == "SESSION_PARTIAL"
    assert metrics["CN"]["latest_complete_session"] == ""
    assert metrics["CN"]["missing_symbol_count"] == 49
    assert metrics["HK"]["market_timezone"] == "Asia/Hong_Kong"
    assert metrics["US"]["market_timezone"] == "America/New_York"
