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
    assert metrics["CN"]["provider_state"] == "published_time_unavailable"
    assert metrics["CN"]["ingest_state"] == "partial"
    assert metrics["CN"]["next_session"] == ""
    assert metrics["CN"]["next_session_reason"]
    assert metrics["HK"]["market_timezone"] == "Asia/Hong_Kong"
    assert metrics["US"]["market_timezone"] == "America/New_York"


def test_market_session_metrics_report_late_and_bad_timestamp_evidence() -> None:
    metrics = _market_session_metrics({
        "target_session": "2026-08-13",
        "actual_session": "2026-08-12",
        "updated_at": "2026-08-13 18:33:49",
        "validated_session": "2026-08-12",
        "validation": {
            "target_session": "2026-08-13",
            "actual_session": "2026-08-12",
            "accepted": False,
            "complete": False,
            "provider_published_at": "not-a-time",
        },
    })

    cn = metrics["CN"]
    assert cn["latest_complete_session"] == "2026-08-12"
    assert cn["provider_state"] == "waiting"
    assert cn["diagnostic_codes"] == [
        "SESSION_CLOSED_WAIT_PROVIDER", "DATA_LATE",
        "TIME_UNINTERPRETABLE", "TIME_UNZONED",
    ]
    assert cn["timestamp_diagnostics"] == [
        {"field": "provider_published_at", "diagnostic_code": "TIME_UNINTERPRETABLE"},
        {"field": "ingested_at", "diagnostic_code": "TIME_UNZONED"},
    ]


def test_market_session_metrics_keep_unverified_markets_explicit_without_status() -> None:
    metrics = _market_session_metrics({}, {})

    assert set(metrics) == {"CN", "HK", "US"}
    assert metrics["CN"]["completion_state"] == "calendar_unavailable"
    assert metrics["HK"]["next_session_reason"] == "未提供经验证的未来交易日历"
    assert metrics["US"]["late_record_count"] is None


def test_market_session_metrics_do_not_guess_date_only_timestamp_or_compact_date() -> None:
    metrics = _market_session_metrics({
        "target_session": "20260813",
        "actual_session": "20260812",
        "updated_at": "2026-08-13",
        "validation": {
            "target_session": "20260813", "actual_session": "20260812",
            "accepted": False, "complete": False,
        },
    })

    assert "DATA_LATE" not in metrics["CN"]["diagnostic_codes"]
    assert metrics["CN"]["timestamp_diagnostics"] == [{
        "field": "ingested_at", "diagnostic_code": "TIME_UNINTERPRETABLE",
    }]
