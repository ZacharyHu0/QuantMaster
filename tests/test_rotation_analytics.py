from __future__ import annotations

import numpy as np
import pandas as pd

from quantmaster.rotation.analytics import (
    _stage,
    analyze_group_rotation,
    compute_market_structure,
    compute_market_temperature,
    compute_trend_matrices,
    estimate_etf_flows,
)
from quantmaster.rotation.taxonomy import strict_l1_groups


def _close(days: int = 100, symbols: int = 24) -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-02", periods=days)
    phase = np.linspace(0, 4 * np.pi, days)[:, None]
    slopes = np.linspace(-0.0018, 0.0024, symbols)[None, :]
    noise = np.sin(phase + np.arange(symbols)[None, :] * 0.31) * 0.004
    returns = slopes + noise
    return pd.DataFrame(
        100 * np.exp(np.cumsum(returns, axis=0)),
        index=dates,
        columns=[f"{600000 + index:06d}.SH" for index in range(symbols)],
    )


def test_trend_history_does_not_change_when_future_rows_are_appended():
    close = _close(90, 12)
    earlier = compute_trend_matrices(close.iloc[:70]).score
    later = compute_trend_matrices(close).score.loc[earlier.index]
    pd.testing.assert_frame_equal(earlier, later)


def test_market_temperature_reconciles_all_state_counts():
    close = _close()
    amount = close * 1_000_000
    result = compute_market_temperature(close, amount, expected_count=len(close.columns))

    current = result["current"]
    assert 0 <= current["temperature"] <= 100
    assert sum(current["counts"].values()) == current["eligible_count"]
    assert result["quality"]["status"] == "complete"
    assert result["evidence"]["available_weight"] == 75
    assert result["history"][-1]["ma20"] is not None


def test_market_structure_exposes_distribution_and_three_day_confirmation():
    close = _close(120, 20)
    result = compute_market_structure(close)

    assert result["as_of"]
    assert {row["state"] for row in result["distribution"]} == {
        "strong_up", "up", "range", "weak",
    }
    assert result["current"]["dead_zone"] == 0.0025
    assert result["definition"]["confirmation_sessions"] == 3


def test_group_rotation_respects_coverage_and_member_reconciliation():
    close = _close(120, 24)
    symbols = list(close.columns)
    groups = {
        "801080.SI": {
            "code": "801080.SI", "name": "电子", "level": "L1",
            "members": symbols[:12],
        },
        "801750.SI": {
            "code": "801750.SI", "name": "计算机", "level": "L1",
            "members": symbols[12:],
        },
        "too-small": {"code": "too-small", "name": "样本不足", "members": symbols[:3]},
    }
    result = analyze_group_rotation(close, groups, amount=close * 2_000_000)

    assert [item["code"] for item in result["items"]] != ["too-small"]
    assert len(result["items"]) == 2
    for item in result["items"]:
        assert item["eligible_count"] <= item["member_count"]
        assert item["grade"] in {"A", "B", "C", "D"}
        assert item["stage"] in result["summary"]["stages"]
        assert len(result["details"][item["code"]]["history"]) >= 20


def test_stage_rules_are_deterministic_at_boundaries():
    assert _stage(20, 72, 1, 0) == "extreme_weak"
    assert _stage(20, 55, 1, -4) == "low_repair"
    assert _stage(30, 35, 4, -4) == "repair_spread"
    assert _stage(20, 45, -4, 4) == "clear_retreat"
    assert _stage(20, 45, -4, 0) == "retreat_watch"
    assert _stage(20, 45, 0, 0) == "unclear"


def test_strict_l1_taxonomy_drops_mixed_and_unknown_labels():
    groups = strict_l1_groups({
        "600001.SH": "电子",
        "600002.SH": "半导体",
        "600003.SH": "东方财富概念",
        "00700.HK": "电子",
    })

    assert groups["801080.SI"]["members"] == ["600001.SH"]
    assert sum(len(item["members"]) for item in groups.values()) == 1


def test_etf_flow_uses_nav_then_marks_close_fallback():
    frame = pd.DataFrame([
        {"trade_date": "2026-07-29", "symbol": "510300.SH", "shares": 100, "nav": 4.0},
        {"trade_date": "2026-07-30", "symbol": "510300.SH", "shares": 110, "nav": 4.1},
        {"trade_date": "2026-07-29", "symbol": "159915.SZ", "shares": 80, "close": 2.0},
        {"trade_date": "2026-07-30", "symbol": "159915.SZ", "shares": 75, "close": 2.2},
    ])
    result = estimate_etf_flows(frame)

    by_symbol = {item["symbol"]: item for item in result["items"]}
    assert by_symbol["510300.SH"]["flow"] == 41.0
    assert by_symbol["510300.SH"]["price_source"] == "nav"
    assert by_symbol["159915.SZ"]["flow"] == -11.0
    assert result["summary"]["nav_count"] == 1
    assert result["summary"]["close_fallback_count"] == 1
    assert result["daily"][-1]["cumulative_ma5"] is None
