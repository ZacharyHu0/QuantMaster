from __future__ import annotations

import ast
import tomllib
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from quantmaster.rotation.analytics import (
    STATE_CODES,
    _amount_activity_score,
    _classify_trend_states,
    _etf_capital_parameters,
    _etf_flow_streak,
    _external_temperature_evidence_item,
    _group_grade,
    _group_window_signal,
    _midrank_percentile,
    _regime,
    _score_group_windows,
    _stage,
    _unavailable_etf_capital_evidence,
    analyze_group_rotation,
    compute_etf_capital_evidence,
    compute_market_structure,
    compute_market_temperature,
    compute_trend_matrices,
    estimate_etf_flows,
    map_theme_industries,
    market_temperature_reference_dates,
)
from quantmaster.rotation.taxonomy import strict_l1_groups


def test_quantmaster_has_no_scipy_dependency() -> None:
    project_root = Path(__file__).parents[1]
    imports = {
        name
        for source in (project_root / "quantmaster").rglob("*.py")
        for node in ast.walk(ast.parse(source.read_text(encoding="utf-8")))
        for name in (
            [alias.name for alias in node.names] if isinstance(node, ast.Import)
            else [node.module or ""] if isinstance(node, ast.ImportFrom)
            else []
        )
    }

    assert not any(name == "scipy" or name.startswith("scipy.") for name in imports)
    project = tomllib.loads((project_root / "pyproject.toml").read_text(encoding="utf-8"))
    assert not any(
        requirement.partition(";")[0].strip().lower().startswith("scipy")
        for requirement in project["project"]["dependencies"]
    )


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
    earlier = compute_trend_matrices(close.iloc[:70])
    later = compute_trend_matrices(close)
    pd.testing.assert_frame_equal(earlier.score, later.score.loc[earlier.score.index])
    pd.testing.assert_frame_equal(earlier.state, later.state.loc[earlier.state.index])


def test_trend_state_hysteresis_is_symmetric_and_resets_after_invalid_rows():
    dates = pd.bdate_range("2026-01-05", periods=7)
    score = pd.DataFrame({
        "positive": [0.37, 0.38, 0.54, 0.55, -0.29, -0.30, -0.38],
        "negative": [-0.37, -0.38, -0.20, 0.29, 0.30, 0.38, 0.55],
        "reset": [0.38, 0.20, 0.20, 0.20, 0.20, 0.20, 0.20],
    }, index=dates)
    eligible = pd.DataFrame(True, index=dates, columns=score.columns)
    eligible.loc[dates[1], "reset"] = False

    states = _classify_trend_states(score, eligible)

    assert states["positive"].tolist() == [
        STATE_CODES["range"], STATE_CODES["up"], STATE_CODES["up"],
        STATE_CODES["strong_up"], STATE_CODES["up"], STATE_CODES["range"],
        STATE_CODES["weak"],
    ]
    assert states["negative"].tolist() == [
        STATE_CODES["range"], STATE_CODES["weak"], STATE_CODES["weak"],
        STATE_CODES["weak"], STATE_CODES["range"], STATE_CODES["up"],
        STATE_CODES["strong_up"],
    ]
    assert states.loc[dates[0], "reset"] == STATE_CODES["up"]
    assert states.loc[dates[1], "reset"] == STATE_CODES["unavailable"]
    assert states.loc[dates[2], "reset"] == STATE_CODES["range"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (9.99, ("ice", "冰点/黄金坑")),
        (10.0, ("contraction", "拉锯区")),
        (24.99, ("contraction", "拉锯区")),
        (25.0, ("expansion", "强势扩散区")),
        (50.0, ("expansion", "强势扩散区")),
        (50.01, ("overheat", "过热区")),
    ],
)
def test_market_temperature_regime_boundaries_match_reference(value, expected):
    assert _regime(value) == expected


@pytest.mark.parametrize(
    ("payload", "expected_note"),
    [
        (
            {"available": False, "score": 55, "as_of": "2026-08-12", "note": "停用"},
            "停用",
        ),
        (
            {"available": True, "score": 101, "as_of": "2026-08-12", "note": "越界"},
            "越界",
        ),
        (
            {"available": True, "score": 55, "as_of": "2026-08-11", "note": "过期"},
            "证据日期 2026-08-11 与行情日 2026-08-12 不一致",
        ),
    ],
)
def test_external_temperature_evidence_rejects_unusable_payloads(payload, expected_note):
    item = _external_temperature_evidence_item(
        "sentiment",
        "情绪代理",
        10,
        "等待可核查资讯情绪",
        "2026-08-12",
        {"sentiment": payload},
    )

    assert item["score"] is None
    assert item["note"] == expected_note
    assert item["as_of"] == payload["as_of"]


def test_external_temperature_evidence_preserves_valid_payload_metadata():
    item = _external_temperature_evidence_item(
        "sentiment",
        "情绪代理",
        10,
        "等待可核查资讯情绪",
        "2026-08-12",
        {
            "sentiment": {
                "available": True,
                "score": 55.126,
                "as_of": "2026-08-12",
                "note": "可用",
                "event_count": 12,
            },
        },
    )

    assert item["score"] == 55.13
    assert item["note"] == "可用"
    assert item["event_count"] == 12


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
    assert all(
        row["strong_up"] + row["up"] + row["range"] + row["weak"]
        == row["eligible"]
        for row in result["history"]
    )
    assert result["definition"]["state_model"] == "hysteresis"
    assert result["definition"]["thresholds"] == {
        "strong_up": 0.55, "up": 0.38, "weak": -0.38,
    }
    assert result["definition"]["hysteresis"] == {
        "up_exit": -0.30, "weak_exit": 0.30,
    }
    assert result["definition"]["regimes"]["overheat"] == ">50"


def test_market_temperature_accepts_same_day_supplemental_evidence():
    close = _close()
    as_of = str(close.index[-1].date())
    result = compute_market_temperature(
        close,
        close * 1_000_000,
        supplemental_evidence={
            "etf_capital": {
                "available": True,
                "score": 18.45,
                "as_of": as_of,
                "note": "近 5 日净申购率 -3.98%",
                "reference_windows": 252,
            },
            "sentiment": {
                "available": True,
                "score": 53.95,
                "as_of": as_of,
                "note": "中性 +7.90",
                "event_count": 120,
            },
        },
    )

    items = {item["id"]: item for item in result["evidence"]["items"]}
    assert result["evidence"]["available_weight"] == 100
    assert items["etf_capital"]["score"] == 18.45
    assert items["etf_capital"]["reference_windows"] == 252
    assert items["sentiment"]["score"] == 53.95
    assert items["sentiment"]["event_count"] == 120
    assert not result["quality"]["issues"]


def test_market_temperature_change_windows_use_prior_trading_sessions():
    close = _close(120, 24)
    trend = compute_trend_matrices(close)
    references = market_temperature_reference_dates(trend)
    current_as_of = references[0]
    history_evidence = {
        references[window]: {
            "etf_capital": {
                "available": True,
                "score": 20.0 + window,
                "as_of": references[window],
                "note": f"ETF 历史 {window}",
            },
            "sentiment": {
                "available": True,
                "score": 40.0 - window,
                "as_of": references[window],
                "note": f"情绪历史 {window}",
            },
        }
        for window in (1, 3, 5, 20)
    }
    result = compute_market_temperature(
        close,
        close * 1_000_000,
        trend=trend,
        supplemental_evidence={
            "etf_capital": {
                "available": True, "score": 60.0,
                "as_of": current_as_of, "note": "ETF 当前",
            },
            "sentiment": {
                "available": True, "score": 55.0,
                "as_of": current_as_of, "note": "情绪当前",
            },
        },
        supplemental_evidence_history=history_evidence,
    )

    windows = result["change_windows"]
    assert windows["default_window"] == 5
    assert windows["supported_windows"] == [1, 3, 5, 20]
    five = windows["windows"]["5"]
    assert five["reference_as_of"] == str(close.index[-6].date())
    history = {row["date"]: row for row in result["history"]}
    assert five["temperature"]["previous"] == history[five["reference_as_of"]]["temperature"]
    assert five["temperature"]["change_pp"] == round(
        result["current"]["temperature"] - five["temperature"]["previous"], 2,
    )
    comparisons = {item["id"]: item for item in five["evidence"]["items"]}
    assert five["evidence"]["comparable_count"] == 5
    assert comparisons["etf_capital"]["previous_score"] == 25.0
    assert comparisons["etf_capital"]["change_pp"] == 35.0
    assert comparisons["sentiment"]["change_pp"] == 20.0
    assert comparisons["trend"]["change_pp"] == five["temperature"]["change_pp"]


def test_market_temperature_change_keeps_missing_history_unavailable():
    close = _close(100, 24)
    current_as_of = str(close.index[-1].date())
    result = compute_market_temperature(
        close,
        close * 1_000_000,
        supplemental_evidence={
            "etf_capital": {
                "available": True, "score": 60.0,
                "as_of": current_as_of, "note": "ETF 当前",
            },
            "sentiment": {
                "available": True, "score": 55.0,
                "as_of": current_as_of, "note": "情绪当前",
            },
        },
    )

    five = result["change_windows"]["windows"]["5"]
    comparisons = {item["id"]: item for item in five["evidence"]["items"]}
    assert five["evidence"]["comparable_count"] == 3
    assert comparisons["etf_capital"]["current_available"] is True
    assert comparisons["etf_capital"]["previous_available"] is False
    assert comparisons["etf_capital"]["change_pp"] is None
    assert "等待 ETF" in comparisons["etf_capital"]["previous_note"]
    assert comparisons["sentiment"]["change_pp"] is None


def test_market_structure_exposes_distribution_and_three_day_confirmation():
    close = _close(120, 20)
    result = compute_market_structure(close)

    assert result["as_of"]
    assert {row["state"] for row in result["distribution"]} == {
        "strong_up", "up", "range", "weak",
    }
    assert result["current"]["dead_zone"] == 0.0025
    assert result["definition"]["confirmation_sessions"] == 3
    assert sum(row["share"] for row in result["distribution"]) == pytest.approx(1.0)
    assert result["current"]["candidate_sessions"] >= 0
    assert result["current"]["confirmed_sessions"] >= 0


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
        assert item["scores"]["5"]["grade"] in {"A", "B", "C", "D"}
        assert item["positive_ratio"] >= item["strong_ratio"]
        assert item["stage"] in result["summary"]["stages"]
        assert set(item["signals"]) == {"1", "3", "5", "20"}
        assert set(item["scores"]) == {"1", "3", "5", "20"}
        assert "rotation_score" not in item
        assert "grade" not in item
        assert "score" not in item
        for signal in item["signals"].values():
            assert signal["rotation_change_pp"] == round(
                signal["positive_change_pp"] - signal["weak_change_pp"], 2,
            )
            assert signal["member_return"] is not None
            assert signal["excess_return"] is not None
            assert 0 <= signal["advance_ratio"] <= 1
            assert signal["amount_activity"] is not None
        assert len(result["details"][item["code"]]["history"]) >= 20
        history = result["details"][item["code"]]["history"]
        assert all("stage" in row and "stage_label" in row for row in history)
        assert all(row["positive_ratio"] >= row["strong_ratio"] for row in history)
        assert item["stage_sessions"] >= 1
    for movement in result["summary"]["movements"].values():
        assert sum(movement[f"{key}_count"] for key in (
            "improving", "retreating", "unchanged", "unavailable",
        )) == len(result["items"])
    assert result["definition"]["windows"] == [1, 3, 5, 20]
    assert result["definition"]["coordinates"]["x"] == "positive_ratio"
    assert result["definition"]["score"]["weights"] == {
        "trend": 40, "breadth": 20, "volume": 15,
        "relative_return": 15, "rotation": 10,
    }


def test_group_window_signal_keeps_returns_when_prior_coverage_is_too_thin():
    aggregation = SimpleNamespace(
        rows={"group": object()},
        eligible=np.array([[6.0], [10.0]]),
        strong_ratio=np.array([[20.0], [25.0]]),
        positive_ratio=np.array([[50.0], [60.0]]),
        weak_ratio=np.array([[30.0], [15.0]]),
        window_returns={1: (np.array([0.02]), np.array([0.75]))},
        window_amount_activity={1: np.array([0.10])},
    )

    signal = _group_window_signal(
        aggregation,
        window=1,
        group_index=0,
        member_count=10,
        current_position=1,
        minimum_members=8,
        minimum_coverage=0.70,
        strong_now=25.0,
        positive_now=60.0,
        weak_now=15.0,
        market_return=0.01,
    )

    assert signal == {
        "strong_change_pp": None,
        "positive_change_pp": None,
        "weak_change_pp": None,
        "rotation_change_pp": None,
        "member_return": 0.02,
        "excess_return": 0.01,
        "advance_ratio": 0.75,
        "amount_activity": 0.1,
    }


def test_group_window_signal_is_fully_unavailable_without_prior_session():
    aggregation = SimpleNamespace(rows={"group": object()})

    signal = _group_window_signal(
        aggregation,
        window=1,
        group_index=0,
        member_count=10,
        current_position=0,
        minimum_members=8,
        minimum_coverage=0.70,
        strong_now=25.0,
        positive_now=60.0,
        weak_now=15.0,
        market_return=0.01,
    )

    assert set(signal) == {
        "strong_change_pp",
        "positive_change_pp",
        "weak_change_pp",
        "rotation_change_pp",
        "member_return",
        "excess_return",
        "advance_ratio",
        "amount_activity",
    }
    assert all(value is None for value in signal.values())


def _score_item(index: int, *, positive: float = 0.0) -> dict:
    return {
        "code": f"G{index:02d}", "name": f"组 {index}", "level": "L1",
        "positive_ratio": positive,
        "signals": {
            str(window): {
                "advance_ratio": 0.0,
                "amount_activity": -0.30,
                "excess_return": float(index if window != 20 else -index) / 10_000,
                "rotation_change_pp": float(index if window != 20 else -index),
            }
            for window in (1, 3, 5, 20)
        },
    }


def test_group_score_calibration_uses_midrank_volume_bounds_and_grades():
    assert _midrank_percentile(1, [0, 1, 1, 2, 3, 4, 5, 6]) == 25.0
    assert _midrank_percentile(1, [0, 1, 2]) is None
    assert _amount_activity_score(-0.30) == 0.0
    assert _amount_activity_score(0.0) == 50.0
    assert _amount_activity_score(0.30) == 100.0
    assert _amount_activity_score(0.80) == 100.0
    assert [_group_grade(value) for value in (70, 55, 40, 39.99, None)] == [
        "A", "B", "C", "D", "",
    ]


def test_relative_leader_stays_low_when_absolute_group_state_is_weak():
    items = [_score_item(index) for index in range(8)]

    _score_group_windows(items, "industry")

    leader = items[-1]
    assert leader["scores"]["5"]["available_weight"] == 100
    assert leader["scores"]["5"]["score"] < 40
    assert leader["scores"]["5"]["grade"] == "D"
    assert leader["scores"]["1"]["score"] != leader["scores"]["20"]["score"]


def test_group_score_withholds_result_below_minimum_available_weight():
    item = _score_item(0, positive=25.0)
    for signal in item["signals"].values():
        signal.update({
            "advance_ratio": None, "amount_activity": None,
            "excess_return": None, "rotation_change_pp": None,
        })

    _score_group_windows([item], "industry")

    score = item["scores"]["5"]
    assert score["available_weight"] == 40
    assert score["score"] is None
    assert score["grade"] == ""


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
    mapping = {
        "600001.SH": "电子",
        "600002.SH": "半导体",
        "600003.SH": "东方财富概念",
        "00700.HK": "电子",
    }
    unresolved = strict_l1_groups(mapping)
    groups = strict_l1_groups(mapping, taxonomy_id="sws:industry:2021")

    assert sum(len(item["members"]) for item in unresolved.values()) == 0
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
    assert result["summary"]["windows"]["1"]["largest_inflow"]["flow"] == 41.0
    assert result["summary"]["windows"]["1"]["largest_outflow"]["flow"] == -11.0
    assert by_symbol["510300.SH"]["flow_streak_sessions"] == 1
    assert by_symbol["159915.SZ"]["flow_streak_sessions"] == -1
    assert result["definition"]["windows"] == [1, 3, 5, 20]
    assert result["daily"][-1]["cumulative_ma5"] is None


@pytest.mark.parametrize(
    ("flows", "available_dates", "expected"),
    [
        ([2.0, 3.0, 4.0], 3, 3),
        ([-2.0, -3.0, -4.0], 3, -3),
        ([2.0, -3.0, -4.0], 3, -2),
        ([2.0, 0.0, 4.0], 3, 1),
    ],
)
def test_etf_flow_streak_stops_at_reversal_zero_or_missing_session(
    flows,
    available_dates,
    expected,
):
    dates = list(pd.bdate_range("2026-08-10", periods=available_dates))
    observations = pd.DataFrame({
        "trade_date": dates[-len(flows):],
        "flow": flows,
    })

    assert _etf_flow_streak(observations, dates) == expected


def test_etf_flow_streak_stops_at_missing_session():
    dates = list(pd.bdate_range("2026-08-10", periods=3))
    observations = pd.DataFrame({
        "trade_date": [dates[0], dates[2]],
        "flow": [2.0, 4.0],
    })

    assert _etf_flow_streak(observations, dates) == 1


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
def test_etf_flow_keeps_three_year_display_window():
    dates = pd.bdate_range("2023-01-02", periods=800)
    frame = pd.DataFrame({
        "trade_date": dates,
        "symbol": "510300.SH",
        "shares": np.arange(800, dtype=float) + 1_000,
        "close": np.linspace(3.5, 4.5, 800),
    })

    result = estimate_etf_flows(frame)

    assert len(result["daily"]) == 780
    assert result["daily"][0]["date"] == str(dates[20].date())


def _etf_evidence_frame(days: int = 80, funds: int = 20) -> pd.DataFrame:
    dates = pd.bdate_range("2026-01-02", periods=days)
    rates = np.linspace(-0.01, 0.01, days - 1)
    rows = []
    for fund in range(funds):
        shares = 1_000.0 + fund
        rows.append({
            "trade_date": dates[0], "symbol": f"ETF{fund:03d}",
            "shares": shares, "nav": 1.0,
        })
        for trade_date, rate in zip(dates[1:], rates, strict=True):
            shares *= 1 + float(rate)
            rows.append({
                "trade_date": trade_date, "symbol": f"ETF{fund:03d}",
                "shares": shares, "nav": 1.0,
            })
    return pd.DataFrame(rows)


@pytest.mark.parametrize(
    ("expected_funds", "minimum_coverage", "expected"),
    [
        ("30", "0.75", (30, 0.75)),
        (-2, -0.1, (0, 0.0)),
        (12, 1.5, (12, 1.0)),
        ("invalid", "invalid", (0, 0.80)),
    ],
)
def test_etf_capital_parameters_normalize_invalid_configuration(
    expected_funds,
    minimum_coverage,
    expected,
):
    assert _etf_capital_parameters(expected_funds, minimum_coverage) == expected


def test_unavailable_etf_capital_evidence_keeps_coverage_contract():
    result = _unavailable_etf_capital_evidence(
        "覆盖不足",
        window=5,
        expected_count=30,
        coverage_threshold=0.8,
        observed_as_of="2026-08-12",
        fund_count=20,
    )

    coverage = result.pop("coverage")
    assert coverage == pytest.approx(2 / 3)
    assert result == {
        "available": False,
        "score": None,
        "as_of": "2026-08-12",
        "note": "覆盖不足",
        "window_sessions": 5,
        "reference_windows": 0,
        "fund_count": 20,
        "expected_funds": 30,
        "minimum_coverage": 0.8,
        "net_subscription_rate": None,
        "net_subscription_rate_pct": None,
    }


def test_etf_capital_evidence_is_historical_percentile_without_future_rows():
    frame = _etf_evidence_frame()
    target = pd.bdate_range("2026-01-02", periods=71)[-1]

    full = compute_etf_capital_evidence(frame, as_of=target)
    truncated = compute_etf_capital_evidence(
        frame[frame["trade_date"] <= target],
        as_of=target,
    )

    assert full == truncated
    assert full["available"] is True
    assert 90 < full["score"] <= 100
    assert full["fund_count"] == 20
    assert full["reference_windows"] >= 60
    assert full["net_subscription_rate"] > 0


def test_etf_capital_evidence_zero_flow_is_midpoint_and_requires_current_snapshot():
    frame = _etf_evidence_frame()
    frame["shares"] = 1_000.0
    target = pd.Timestamp(frame["trade_date"].max())
    evidence = compute_etf_capital_evidence(frame, as_of=target)

    assert evidence["available"] is True
    assert evidence["score"] == 50.0
    stale = compute_etf_capital_evidence(frame, as_of=target + pd.offsets.BDay())
    assert stale["available"] is False
    assert "仅到" in stale["note"]


def test_etf_capital_evidence_rejects_thin_consecutive_coverage():
    frame = _etf_evidence_frame()
    dates = sorted(pd.Timestamp(value) for value in frame["trade_date"].unique())
    missing_symbol = "ETF000"
    frame = frame[
        ~(
            (frame["symbol"] == missing_symbol)
            & (frame["trade_date"] == dates[-2])
        )
    ]
    evidence = compute_etf_capital_evidence(
        frame,
        as_of=dates[-1],
        min_funds=20,
    )

    assert evidence["available"] is False
    assert evidence["fund_count"] == 19
    assert "连续份额快照" in evidence["note"]


def test_etf_capital_evidence_rejects_thin_current_universe_coverage():
    frame = _etf_evidence_frame(funds=20)
    target = pd.Timestamp(frame["trade_date"].max())

    evidence = compute_etf_capital_evidence(
        frame,
        as_of=target,
        expected_funds=30,
        minimum_coverage=0.80,
    )

    assert evidence["available"] is False
    assert evidence["fund_count"] == 20
    assert evidence["expected_funds"] == 30
    assert evidence["coverage"] == pytest.approx(2 / 3)
    assert "低于 80% 发布门槛" in evidence["note"]
