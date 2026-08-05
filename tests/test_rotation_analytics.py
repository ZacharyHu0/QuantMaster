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
    map_theme_industries,
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
        assert set(item["signals"]) == {"1", "3", "5", "20"}
        for signal in item["signals"].values():
            assert signal["rotation_change_pp"] == round(
                signal["strong_change_pp"] - signal["weak_change_pp"], 2,
            )
            assert signal["member_return"] is not None
            assert signal["excess_return"] is not None
            assert 0 <= signal["advance_ratio"] <= 1
            assert signal["amount_activity"] is not None
        assert len(result["details"][item["code"]]["history"]) >= 20
    assert result["definition"]["windows"] == [1, 3, 5, 20]


def test_theme_industry_mapping_uses_member_overlap_and_auditable_thresholds():
    symbols = [f"{600000 + index:06d}.SH" for index in range(16)]
    industries = {
        "801080.SI": {"name": "电子", "level": "L1", "members": symbols[:8]},
        "801750.SI": {"name": "计算机", "level": "L1", "members": symbols[8:]},
    }
    themes = {
        "strong": {"members": [*symbols[:6], *symbols[8:10]]},
        "thin": {"members": [symbols[0], symbols[8], symbols[9], symbols[10]]},
    }

    result = map_theme_industries(themes, industries)

    assert result["strong"]["primary_industry"]["code"] == "801080.SI"
    assert result["strong"]["primary_industry"]["overlap_count"] == 6
    assert result["strong"]["industry_mapping_coverage"] == 1.0
    assert result["thin"]["primary_industry"]["code"] == "801750.SI"
    assert result["thin"]["primary_industry"]["overlap_count"] == 3


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
    assert by_symbol["510300.SH"]["flows"]["1"] == 41.0
    assert by_symbol["510300.SH"]["flows"]["3"] is None
    assert by_symbol["510300.SH"]["price_source"] == "nav"
    assert by_symbol["159915.SZ"]["flow"] == -11.0
    assert result["summary"]["nav_count"] == 1
    assert result["summary"]["close_fallback_count"] == 1
    assert result["summary"]["windows"]["1"]["net_flow"] == 30.0
    assert result["definition"]["windows"] == [1, 3, 5, 20]
    assert result["daily"][-1]["cumulative_ma5"] is None


def test_etf_flow_preserves_disclosed_benchmark_across_windows():
    rows = []
    for offset, trade_date in enumerate(pd.bdate_range("2026-07-01", periods=21)):
        rows.append({
            "trade_date": trade_date,
            "symbol": "510300.SH",
            "name": "沪深300ETF",
            "benchmark": "" if offset < 10 else "沪深300指数",
            "category": "大盘宽基",
            "shares": 100 + offset,
            "nav": 4.0,
        })

    result = estimate_etf_flows(pd.DataFrame(rows))

    assert result["items"][0]["benchmark"] == "沪深300指数"
    assert result["items"][0]["flows"]["20"] == 80.0
    assert len(result["benchmarks"]) == 1
    assert result["benchmarks"][0]["benchmark"] == "沪深300指数"
    assert result["benchmarks"][0]["flows"]["20"] == 80.0
