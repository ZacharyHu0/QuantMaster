"""Vectorized, no-lookahead market and rotation analytics.

Every rolling value at date *t* uses data at or before *t*.  The functions are pure:
they neither access the network nor mutate storage, which keeps the algorithm easy to
audit and lets the API expose honest partial-coverage states.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Literal

import numpy as np
import pandas as pd

ALGORITHM_VERSION = "QM_ROTATION_V2"
ROTATION_WINDOWS = (1, 3, 5, 20)
MIN_HISTORY = 30
EPS = 1e-12

STATE_LABELS = {
    "strong_up": "强势加速",
    "up": "趋势延续",
    "range": "中位整理",
    "weak": "低位偏弱",
}

REGIMES: tuple[tuple[float, str, str], ...] = (
    (10.0, "ice", "冰点"),
    (25.0, "contraction", "收缩"),
    (50.0, "expansion", "扩散"),
    (101.0, "overheat", "过热"),
)

STAGE_LABELS = {
    "repair_spread": "修复扩散",
    "low_repair": "低位修复",
    "extreme_weak": "极弱钝化",
    "unclear": "方向未明",
    "retreat_watch": "退潮观察",
    "clear_retreat": "明确退潮",
}


@dataclass(frozen=True)
class TrendMatrices:
    close: pd.DataFrame
    returns: pd.DataFrame
    score: pd.DataFrame
    eligible: pd.DataFrame


def _number(value: Any, digits: int = 4) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return round(number, digits) if math.isfinite(number) else None


def _date(value: Any) -> str:
    return str(pd.Timestamp(value).date())


def _regime(value: float | None) -> tuple[str, str]:
    if value is None:
        return "unavailable", "暂不可用"
    for upper, code, label in REGIMES:
        if value < upper:
            return code, label
    return "overheat", "过热"


def _clean_matrix(value: pd.DataFrame, *, columns: list[str] | None = None) -> pd.DataFrame:
    frame = value.copy()
    frame.index = pd.to_datetime(frame.index, errors="coerce")
    frame = frame.loc[~frame.index.isna()]
    frame = frame[~frame.index.duplicated(keep="last")].sort_index()
    if columns is not None:
        frame = frame.reindex(columns=columns)
    return frame.apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan)


def compute_trend_matrices(close: pd.DataFrame) -> TrendMatrices:
    """Compute the existing QuantMaster trend score for a full cross-section."""
    prices = _clean_matrix(close)
    if prices.empty or not len(prices.columns):
        raise ValueError("行情矩阵为空")
    returns = prices.pct_change(fill_method=None)
    volatility = returns.rolling(20, min_periods=10).std()
    ema10 = prices.ewm(span=10, adjust=False).mean()
    ema30 = prices.ewm(span=30, adjust=False).mean()
    ma_component = (
        (ema10 / ema30 - 1.0) / (volatility * math.sqrt(10) + EPS)
    ).clip(-2, 2) / 2
    del ema10, ema30
    ema12 = prices.ewm(span=12, adjust=False).mean()
    ema26 = prices.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    del ema12, ema26
    macd_hist = macd - macd.ewm(span=9, adjust=False).mean()
    del macd
    macd_component = (macd_hist / (prices * volatility + EPS)).clip(-2, 2) / 2
    del macd_hist
    momentum = prices.pct_change(5, fill_method=None)
    momentum_component = (momentum / (volatility * math.sqrt(5) + EPS)).clip(-2, 2) / 2
    score = (
        0.45 * ma_component + 0.35 * macd_component + 0.20 * momentum_component
    ).clip(-1, 1)
    del ma_component, macd_component, momentum, momentum_component, volatility
    valid_history = prices.notna().rolling(
        MIN_HISTORY, min_periods=MIN_HISTORY,
    ).sum()
    eligible = prices.notna() & (valid_history >= MIN_HISTORY)
    score = score.where(eligible)
    return TrendMatrices(prices, returns, score, eligible & score.notna())


def _state_masks(trend: TrendMatrices) -> dict[str, pd.DataFrame]:
    score, eligible = trend.score, trend.eligible
    return {
        "strong_up": eligible & (score >= 0.55),
        "up": eligible & (score >= 0.20) & (score < 0.55),
        "range": eligible & (score > -0.20) & (score < 0.20),
        "weak": eligible & (score <= -0.20),
    }


def _quality(
    eligible: int,
    tracked: int,
    expected: int | None,
    *,
    issues: list[str] | None = None,
) -> dict[str, Any]:
    tracked_ratio = eligible / tracked if tracked else 0.0
    scope_ratio = tracked / expected if expected else None
    effective = min(tracked_ratio, scope_ratio) if scope_ratio is not None else tracked_ratio
    if not tracked:
        status = "cold"
    elif effective >= 0.90:
        status = "complete"
    elif effective >= 0.70:
        status = "partial"
    else:
        status = "limited"
    notes = list(issues or [])
    if scope_ratio is not None and scope_ratio < 0.90:
        notes.append(f"本地覆盖 {tracked}/{expected} 只在市 A 股，结果代表已覆盖样本")
    if tracked_ratio < 0.90:
        notes.append(f"最新交易日有效趋势 {eligible}/{tracked}")
    return {
        "status": status,
        "eligible_count": eligible,
        "tracked_count": tracked,
        "expected_count": expected,
        "price_coverage": round(tracked_ratio, 4) if tracked else 0.0,
        "scope_coverage": round(scope_ratio, 4) if scope_ratio is not None else None,
        "issues": list(dict.fromkeys(notes)),
    }


def compute_market_temperature(
    close: pd.DataFrame,
    amount: pd.DataFrame | None = None,
    *,
    expected_count: int | None = None,
    history: int = 760,
    trend: TrendMatrices | None = None,
) -> dict[str, Any]:
    """Return market temperature, moving averages and evidence decomposition."""
    trend = trend if trend is not None else compute_trend_matrices(close)
    masks = _state_masks(trend)
    counts = pd.DataFrame({name: mask.sum(axis=1) for name, mask in masks.items()})
    counts["eligible"] = trend.eligible.sum(axis=1)
    counts["temperature"] = (
        100.0 * (counts["strong_up"] + counts["up"]) / counts["eligible"].replace(0, np.nan)
    )
    for window in (5, 10, 20):
        counts[f"ma{window}"] = counts["temperature"].rolling(window, min_periods=1).mean()
    valid = counts[counts["eligible"] > 0]
    if valid.empty:
        raise ValueError("至少需要 30 个有效交易日才能计算市场温度")
    current_date = valid.index[-1]
    current = valid.iloc[-1]
    temperature = _number(current["temperature"], 2)
    regime, regime_label = _regime(temperature)
    history_rows = []
    for index, row in valid.tail(max(30, min(int(history), 3000))).iterrows():
        history_rows.append({
            "date": _date(index),
            "temperature": _number(row["temperature"], 2),
            "ma5": _number(row["ma5"], 2),
            "ma10": _number(row["ma10"], 2),
            "ma20": _number(row["ma20"], 2),
            "eligible": int(row["eligible"]),
            "strong_up": int(row["strong_up"]),
            "up": int(row["up"]),
            "range": int(row["range"]),
            "weak": int(row["weak"]),
        })

    advance = (trend.returns > 0).sum(axis=1) / trend.returns.notna().sum(axis=1).replace(0, np.nan)
    trend_evidence = temperature
    breadth_evidence = _number(100 * advance.loc[current_date], 2)
    volume_evidence = None
    volume_note = "本地成交额不足"
    if amount is not None and not amount.empty:
        amounts = _clean_matrix(amount, columns=list(trend.close.columns)).reindex(trend.close.index)
        total = amounts.sum(axis=1, min_count=max(1, min(10, len(amounts.columns))))
        ratio = total.rolling(5, min_periods=3).mean() / (total.rolling(20, min_periods=10).mean() + EPS)
        if pd.notna(ratio.get(current_date)):
            raw_volume_score = (float(ratio.loc[current_date]) - 0.70) / 0.60 * 100
            volume_evidence = _number(min(100.0, max(0.0, raw_volume_score)), 2)
            volume_note = f"全样本成交额 5/20 日比值 {_number(ratio.loc[current_date], 3)}"
    evidence_items = [
        {"id": "trend", "label": "趋势分布", "score": trend_evidence, "weight": 40, "note": "温度本身"},
        {
            "id": "breadth", "label": "涨跌宽度", "score": breadth_evidence,
            "weight": 20, "note": "当日上涨家数占比",
        },
        {"id": "volume", "label": "量能确认", "score": volume_evidence, "weight": 15, "note": volume_note},
        {"id": "etf_capital", "label": "ETF 资金", "score": None, "weight": 15, "note": "等待 ETF 份额快照"},
        {"id": "sentiment", "label": "情绪代理", "score": None, "weight": 10, "note": "未配置可核查情绪序列"},
    ]
    available_weight = sum(item["weight"] for item in evidence_items if item["score"] is not None)
    evidence_score = (
        sum(float(item["score"]) * item["weight"] for item in evidence_items if item["score"] is not None)
        / available_weight if available_weight else None
    )
    for item in evidence_items:
        item["available"] = item["score"] is not None
    quality = _quality(
        int(current["eligible"]), len(trend.close.columns), expected_count,
        issues=[] if available_weight == 100 else [f"证据维度覆盖权重 {available_weight}/100"],
    )
    return {
        "as_of": _date(current_date),
        "current": {
            "temperature": temperature,
            "regime": regime,
            "regime_label": regime_label,
            "eligible_count": int(current["eligible"]),
            "counts": {name: int(current[name]) for name in STATE_LABELS},
            "ratios": {
                name: _number(100 * current[name] / current["eligible"], 2)
                for name in STATE_LABELS
            },
        },
        "history": history_rows,
        "evidence": {
            "score": _number(evidence_score, 2),
            "available_weight": available_weight,
            "items": evidence_items,
        },
        "quality": quality,
        "definition": {
            "positive_states": ["strong_up", "up"],
            "thresholds": {"strong_up": 0.55, "up": 0.20, "weak": -0.20},
            "regimes": {"ice": "<10", "contraction": "10–25", "expansion": "25–50", "overheat": "≥50"},
            "minimum_history": MIN_HISTORY,
        },
    }


def _candidate(spread: Any) -> str:
    if pd.isna(spread):
        return "unavailable"
    if float(spread) > 0.0025:
        return "strong_dominant"
    if float(spread) < -0.0025:
        return "weak_rebound"
    return "balanced"


def compute_market_structure(
    close: pd.DataFrame,
    *,
    names: dict[str, str] | None = None,
    history: int = 260,
    trend: TrendMatrices | None = None,
) -> dict[str, Any]:
    """Compare return distributions across strong and weak trend cohorts."""
    names = names or {}
    trend = trend if trend is not None else compute_trend_matrices(close)
    masks = _state_masks(trend)
    strong_return = trend.returns.where(masks["strong_up"]).median(axis=1, skipna=True)
    weak_return = trend.returns.where(masks["weak"]).median(axis=1, skipna=True)
    spread = strong_return - weak_return
    candidates = spread.apply(_candidate)
    confirmed: list[str] = []
    for position, value in enumerate(candidates):
        same = position >= 2 and len(set(candidates.iloc[position - 2:position + 1])) == 1
        confirmed.append(str(value) if same and value != "unavailable" else "pending")
    sequence = pd.DataFrame({
        "strong_return": strong_return,
        "weak_return": weak_return,
        "spread": spread,
        "candidate": candidates,
        "confirmed": confirmed,
    })
    valid = sequence.dropna(subset=["spread"])
    if valid.empty:
        raise ValueError("强弱状态样本不足，无法比较市场风格")
    current_date, row = valid.index[-1], valid.iloc[-1]
    current_score = trend.score.loc[current_date].dropna()
    current_returns = trend.returns.loc[current_date]
    distributions = []
    for state, label in STATE_LABELS.items():
        members = masks[state].loc[current_date]
        values = current_returns[members].dropna()
        distributions.append({
            "state": state,
            "label": label,
            "count": int(members.sum()),
            "median_return": _number(values.median(), 4),
            "positive_ratio": _number((values > 0).mean(), 4) if len(values) else None,
        })
    leaders = []
    for symbol, score in current_score.sort_values(ascending=False).head(8).items():
        leaders.append({
            "symbol": str(symbol), "name": names.get(str(symbol), str(symbol)),
            "trend_score": _number(score, 4), "return_1d": _number(current_returns.get(symbol), 4),
        })
    laggards = []
    for symbol, score in current_score.sort_values().head(8).items():
        laggards.append({
            "symbol": str(symbol), "name": names.get(str(symbol), str(symbol)),
            "trend_score": _number(score, 4), "return_1d": _number(current_returns.get(symbol), 4),
        })
    return {
        "as_of": _date(current_date),
        "current": {
            "candidate": str(row["candidate"]),
            "confirmed": str(row["confirmed"]),
            "spread_1d": _number(row["spread"], 4),
            "spread_3d": _number(valid["spread"].tail(3).mean(), 4),
            "dead_zone": 0.0025,
        },
        "history": [
            {
                "date": _date(index),
                "strong_return": _number(value["strong_return"], 4),
                "weak_return": _number(value["weak_return"], 4),
                "spread": _number(value["spread"], 4),
                "candidate": str(value["candidate"]),
                "confirmed": str(value["confirmed"]),
            }
            for index, value in sequence.tail(max(20, min(int(history), 1000))).iterrows()
        ],
        "distribution": distributions,
        "leaders": leaders,
        "laggards": laggards,
        "definition": {"confirmation_sessions": 3, "dead_zone_percentage_points": 0.25},
    }


def _stage(strong_ratio: float, weak_ratio: float, delta_strong: float, delta_weak: float) -> str:
    if weak_ratio >= 70.0 and delta_weak > -3.0:
        return "extreme_weak"
    if weak_ratio >= 50.0 and delta_weak <= -3.0:
        return "low_repair"
    if delta_strong >= 3.0 and delta_weak <= -3.0:
        return "repair_spread"
    if delta_strong <= -3.0 and delta_weak >= 3.0:
        return "clear_retreat"
    if delta_strong <= -3.0 or delta_weak >= 3.0:
        return "retreat_watch"
    return "unclear"


def _representatives(
    symbols: list[str],
    trend: TrendMatrices,
    current_date: pd.Timestamp,
    names: dict[str, str],
    amount: pd.DataFrame | None,
) -> list[dict[str, Any]]:
    candidates = [
        symbol for symbol in symbols
        if symbol in trend.score and pd.notna(trend.score.at[current_date, symbol])
        and "ST" not in names.get(symbol, "").upper()
        and int(trend.close[symbol].notna().sum()) >= 60
    ]
    if not candidates:
        return []
    liquidity: pd.Series | None = None
    if amount is not None and not amount.empty:
        available = [symbol for symbol in candidates if symbol in amount]
        if available:
            liquidity = amount[available].reindex(trend.close.index).tail(20).median()
    ranked = sorted(
        candidates,
        key=lambda symbol: (
            float(liquidity.get(symbol, -math.inf))
            if liquidity is not None and pd.notna(liquidity.get(symbol))
            else -math.inf,
            float(trend.score.at[current_date, symbol]),
        ),
        reverse=True,
    )[:3]
    return [
        {
            "symbol": symbol,
            "name": names.get(symbol, symbol),
            "trend_score": _number(trend.score.at[current_date, symbol], 4),
            "return_1d": _number(trend.returns.at[current_date, symbol], 4),
        }
        for symbol in ranked
    ]


def _amount_activity(
    amount: pd.DataFrame | None,
    symbols: list[str],
    *,
    current_position: int,
    window: int,
    member_count: int,
) -> float | None:
    """Return the median per-stock amount expansion versus the prior 20 sessions."""
    if amount is None or amount.empty:
        return None
    recent_start = current_position - window + 1
    baseline_end = recent_start
    baseline_start = baseline_end - 20
    if recent_start < 0 or baseline_start < 0:
        return None
    recent = amount.iloc[recent_start:current_position + 1][symbols]
    baseline = amount.iloc[baseline_start:baseline_end][symbols]
    recent_min = max(1, math.ceil(window * 0.70))
    valid_recent = recent.count() >= recent_min
    valid_baseline = baseline.count() >= 14
    recent_mean = recent.mean(axis=0, skipna=True)
    baseline_mean = baseline.mean(axis=0, skipna=True)
    ratios = (recent_mean / baseline_mean - 1.0).where(
        valid_recent & valid_baseline & (baseline_mean > EPS)
    ).replace([np.inf, -np.inf], np.nan).dropna()
    if len(ratios) < 8 or len(ratios) / max(1, member_count) < 0.70:
        return None
    return _number(ratios.median(), 4)


def map_theme_industries(
    themes: dict[str, dict[str, Any]],
    industries: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Map themes to auditable SW2021 L1 overlaps without guessing from names."""
    industry_members = {
        str(code): set(item.get("members") or [])
        for code, item in industries.items()
        if str(item.get("level") or "L1") == "L1" and item.get("members")
    }
    result: dict[str, dict[str, Any]] = {}
    for code, theme in themes.items():
        members = set(theme.get("members") or [])
        overlaps: list[dict[str, Any]] = [
            {
                "code": industry_code,
                "name": str(industries[industry_code].get("name") or industry_code),
                "overlap_count": len(members & values),
            }
            for industry_code, values in industry_members.items()
            if members & values
        ]
        overlaps.sort(key=lambda item: (-item["overlap_count"], item["name"], item["code"]))
        mapped_members = set().union(*(
            members & values for values in industry_members.values()
        )) if industry_members else set()
        mapped_count = len(mapped_members)
        links: list[dict[str, Any]] = []
        for item in overlaps[:3]:
            links.append({
                **item,
                "theme_share": _number(
                    item["overlap_count"] / mapped_count if mapped_count else 0.0, 4,
                ),
            })
        primary = links[0] if links else None
        if (
            primary is None
            or int(primary["overlap_count"]) < 3
            or float(primary.get("theme_share") or 0) < 0.25
        ):
            primary = None
        result[str(code)] = {
            "industry_links": links,
            "primary_industry": primary,
            "industry_mapping_coverage": _number(
                mapped_count / len(members) if members else 0.0, 4,
            ),
        }
    return result


def analyze_group_rotation(
    close: pd.DataFrame,
    groups: dict[str, dict[str, Any]],
    *,
    names: dict[str, str] | None = None,
    amount: pd.DataFrame | None = None,
    kind: Literal["industry", "theme"] = "industry",
    minimum_members: int = 8,
    minimum_coverage: float = 0.70,
    history: int = 120,
    trend: TrendMatrices | None = None,
) -> dict[str, Any]:
    """Aggregate stock states into industry or concept lifecycle coordinates."""
    names = names or {}
    trend = trend if trend is not None else compute_trend_matrices(close)
    masks = _state_masks(trend)
    valid_dates = trend.eligible.sum(axis=1)
    valid_dates = valid_dates[valid_dates > 0]
    if valid_dates.empty:
        raise ValueError("没有可用于板块聚合的交易日")
    current_date = valid_dates.index[-1]
    current_position = int(trend.close.index.get_loc(current_date))
    window_dates = {
        window: trend.close.index[current_position - window]
        for window in ROTATION_WINDOWS
        if current_position >= window
    }
    universe_returns: dict[int, float | None] = {}
    for window, previous_date in window_dates.items():
        values = (
            trend.close.loc[current_date] / trend.close.loc[previous_date] - 1.0
        ).where(trend.eligible.loc[current_date]).replace([np.inf, -np.inf], np.nan).dropna()
        universe_returns[window] = _number(values.median(), 4) if len(values) >= 8 else None
    amount_clean = None
    if amount is not None and not amount.empty:
        amount_clean = _clean_matrix(amount, columns=list(trend.close.columns)).reindex(trend.close.index)
    items: list[dict[str, Any]] = []
    details: dict[str, Any] = {}
    for code, raw in groups.items():
        symbols = sorted(set(raw.get("members") or []).intersection(trend.close.columns))
        member_count = len(set(raw.get("members") or []))
        if member_count < minimum_members or not symbols:
            continue
        eligible_now = trend.eligible.loc[current_date, symbols]
        eligible_count = int(eligible_now.sum())
        coverage = eligible_count / member_count if member_count else 0.0
        if eligible_count < minimum_members or coverage < minimum_coverage:
            continue
        strong_now = 100.0 * float(masks["strong_up"].loc[current_date, symbols].sum()) / eligible_count
        weak_now = 100.0 * float(masks["weak"].loc[current_date, symbols].sum()) / eligible_count
        signals: dict[str, dict[str, Any]] = {}
        for window in ROTATION_WINDOWS:
            previous_date = window_dates.get(window)
            if previous_date is None:
                signals[str(window)] = {
                    "strong_change_pp": None,
                    "weak_change_pp": None,
                    "rotation_change_pp": None,
                    "member_return": None,
                    "excess_return": None,
                    "advance_ratio": None,
                    "amount_activity": None,
                }
                continue
            eligible_before = int(trend.eligible.loc[previous_date, symbols].sum())
            previous_coverage = eligible_before / member_count if member_count else 0.0
            if eligible_before >= minimum_members and previous_coverage >= minimum_coverage:
                strong_before = (
                    100.0 * float(masks["strong_up"].loc[previous_date, symbols].sum())
                    / eligible_before
                )
                weak_before = (
                    100.0 * float(masks["weak"].loc[previous_date, symbols].sum())
                    / eligible_before
                )
                strong_change = strong_now - strong_before
                weak_change = weak_now - weak_before
            else:
                strong_change = None
                weak_change = None
            period_returns = (
                trend.close.loc[current_date, symbols]
                / trend.close.loc[previous_date, symbols]
                - 1.0
            ).replace([np.inf, -np.inf], np.nan).dropna()
            return_coverage = len(period_returns) / member_count if member_count else 0.0
            if len(period_returns) >= minimum_members and return_coverage >= minimum_coverage:
                member_return = _number(period_returns.median(), 4)
                advance_ratio = _number((period_returns > 0).mean(), 4)
            else:
                member_return = None
                advance_ratio = None
            market_return = universe_returns.get(window)
            strong_change_value = _number(strong_change, 2)
            weak_change_value = _number(weak_change, 2)
            signals[str(window)] = {
                "strong_change_pp": strong_change_value,
                "weak_change_pp": weak_change_value,
                "rotation_change_pp": _number(
                    strong_change_value - weak_change_value
                    if strong_change_value is not None and weak_change_value is not None
                    else None,
                    2,
                ),
                "member_return": member_return,
                "excess_return": _number(
                    member_return - market_return
                    if member_return is not None and market_return is not None else None,
                    4,
                ),
                "advance_ratio": advance_ratio,
                "amount_activity": _amount_activity(
                    amount_clean,
                    symbols,
                    current_position=current_position,
                    window=window,
                    member_count=member_count,
                ),
            }
        three_day = signals["3"]
        delta_strong = three_day["strong_change_pp"]
        delta_weak = three_day["weak_change_pp"]
        stage = _stage(
            strong_now,
            weak_now,
            float(delta_strong or 0.0),
            float(delta_weak or 0.0),
        )
        day_returns = trend.returns.loc[current_date, symbols]
        valid_returns = int(day_returns.notna().sum())
        advance_ratio = (
            float((day_returns > 0).sum() / valid_returns) if valid_returns else 0.0
        )
        breadth_score = 100.0 * (
            0.40 * advance_ratio + 0.35 * strong_now / 100.0 + 0.25 * (1.0 - weak_now / 100.0)
        )
        lifecycle_scores = {
            "repair_spread": 82.0, "low_repair": 66.0, "extreme_weak": 24.0,
            "unclear": 48.0, "retreat_watch": 34.0, "clear_retreat": 18.0,
        }
        rotation_score = 0.55 * lifecycle_scores[stage] + 0.45 * breadth_score
        grade = (
            "A" if rotation_score >= 70 else "B" if rotation_score >= 55
            else "C" if rotation_score >= 40 else "D"
        )
        history_rows = []
        window = trend.close.index[trend.close.index <= current_date][-max(20, min(int(history), 520)):]
        for date_value in window:
            valid = int(trend.eligible.loc[date_value, symbols].sum())
            if not valid:
                continue
            history_rows.append({
                "date": _date(date_value),
                "strong_ratio": _number(100 * masks["strong_up"].loc[date_value, symbols].sum() / valid, 2),
                "weak_ratio": _number(100 * masks["weak"].loc[date_value, symbols].sum() / valid, 2),
                "eligible": valid,
            })
        item = {
            "code": str(code),
            "name": str(raw.get("name") or code),
            "level": str(raw.get("level") or ("concept" if kind == "theme" else "L1")),
            "parent_code": str(raw.get("parent_code") or ""),
            "member_count": member_count,
            "eligible_count": eligible_count,
            "coverage": round(coverage, 4),
            "strong_ratio": _number(strong_now, 2),
            "weak_ratio": _number(weak_now, 2),
            "delta_strong_3d": _number(delta_strong, 2),
            "delta_weak_3d": _number(delta_weak, 2),
            "signals": signals,
            "advance_ratio": _number(advance_ratio, 4),
            "stage": stage,
            "stage_label": STAGE_LABELS[stage],
            "rotation_score": _number(rotation_score, 2),
            "grade": grade,
            "representatives": _representatives(symbols, trend, current_date, names, amount_clean),
        }
        items.append(item)
        details[str(code)] = {**item, "history": history_rows}
    items.sort(key=lambda item: (
        -float(item["rotation_score"] or 0),
        -int(item["eligible_count"]),
        item["name"],
    ))
    return {
        "as_of": _date(current_date),
        "kind": kind,
        "items": items,
        "details": details,
        "summary": {
            "group_count": len(items),
            "stages": {
                stage: sum(1 for item in items if item["stage"] == stage)
                for stage in STAGE_LABELS
            },
        },
        "definition": {
            "minimum_members": minimum_members,
            "minimum_coverage": minimum_coverage,
            "coordinates": {"x": "strong_ratio", "y": "weak_ratio", "size": "member_count"},
            "windows": list(ROTATION_WINDOWS),
            "rotation_change": "strong_change_pp - weak_change_pp",
            "relative_return": "member return median - full-market return median",
            "amount_activity": "member median(recent N-day mean / prior 20-day mean - 1)",
            "theme_score": "55% 生命周期 + 45% 宽度" if kind == "theme" else None,
        },
    }


def estimate_etf_flows(frame: pd.DataFrame, *, history: int = 260) -> dict[str, Any]:
    """Estimate ETF subscription/redemption flow as delta shares times NAV/close."""
    required = {"trade_date", "symbol", "shares"}
    if frame is None or frame.empty or not required.issubset(frame.columns):
        return {
            "as_of": "",
            "items": [],
            "daily": [],
            "summary": {"status": "cold", "message": "等待 ETF 份额与净值快照"},
            "definition": {"formula": "份额变化 × 当日净值；净值缺失时使用收盘价并标记"},
        }
    value = frame.copy()
    value["trade_date"] = pd.to_datetime(value["trade_date"], errors="coerce")
    value["shares"] = pd.to_numeric(value["shares"], errors="coerce")
    nav = (
        pd.to_numeric(value.get("nav"), errors="coerce")
        if "nav" in value else pd.Series(np.nan, index=value.index)
    )
    close = (
        pd.to_numeric(value.get("close"), errors="coerce")
        if "close" in value else pd.Series(np.nan, index=value.index)
    )
    value["price"] = nav.fillna(close)
    value["price_source"] = np.where(nav.notna(), "nav", np.where(close.notna(), "close", "missing"))
    if "benchmark" not in value:
        value["benchmark"] = ""
    value["benchmark"] = value["benchmark"].fillna("").astype(str).str.strip()
    if "name" not in value:
        value["name"] = value["symbol"]
    if "category" not in value:
        value["category"] = "未分类"
    value = value.dropna(subset=["trade_date", "symbol", "shares", "price"])
    value = value.sort_values(["symbol", "trade_date"])
    value["share_change"] = value.groupby("symbol", sort=False)["shares"].diff()
    value["flow"] = value["share_change"] * value["price"]
    value = value.replace([np.inf, -np.inf], np.nan)
    dated = value.dropna(subset=["flow"])
    if dated.empty:
        return estimate_etf_flows(pd.DataFrame())
    as_of = dated["trade_date"].max()
    latest = dated[dated["trade_date"] == as_of].copy()
    latest = latest.sort_values("flow", ascending=False)
    available_dates = sorted(pd.Timestamp(item) for item in dated["trade_date"].dropna().unique())
    window_frames: dict[int, pd.DataFrame] = {}
    window_summaries: dict[str, dict[str, Any]] = {}
    for window in ROTATION_WINDOWS:
        selected_dates = available_dates[-window:]
        selected = (
            dated[dated["trade_date"].isin(selected_dates)]
            if len(selected_dates) == window else dated.iloc[0:0]
        )
        aggregated = selected.groupby("symbol", as_index=False)["flow"].sum()
        window_frames[window] = aggregated
        window_summaries[str(window)] = {
            "sessions": len(selected_dates),
            "net_flow": _number(aggregated["flow"].sum(), 2) if len(selected_dates) == window else None,
            "inflow_count": int((aggregated["flow"] > 0).sum()) if len(selected_dates) == window else 0,
            "outflow_count": int((aggregated["flow"] < 0).sum()) if len(selected_dates) == window else 0,
        }
    symbol_flows = {
        window: values.set_index("symbol")["flow"].to_dict()
        for window, values in window_frames.items()
    }
    items = []
    for _, row in latest.iterrows():
        items.append({
            "symbol": str(row["symbol"]),
            "name": str(row.get("name") or row["symbol"]),
            "category": str(row.get("category") or "未分类"),
            "benchmark": str(row.get("benchmark") or "未披露"),
            "flow": _number(row["flow"], 2),
            "flows": {
                str(window): _number(values.get(str(row["symbol"])), 2)
                for window, values in symbol_flows.items()
            },
            "share_change": _number(row["share_change"], 2),
            "price": _number(row["price"], 4),
            "price_source": str(row["price_source"]),
        })
    daily = dated.groupby("trade_date", as_index=False)["flow"].sum().tail(max(20, min(int(history), 1000)))
    daily["cumulative"] = daily["flow"].cumsum()
    daily["cumulative_ma5"] = daily["cumulative"].rolling(5, min_periods=5).mean()
    daily["cumulative_ma20"] = daily["cumulative"].rolling(20, min_periods=20).mean()
    benchmark_groups: list[dict[str, Any]] = []
    benchmark_values = dated.copy()
    latest_metadata = latest.drop_duplicates("symbol").set_index("symbol")
    latest_benchmarks = latest_metadata["benchmark"].fillna("").astype(str).to_dict()
    benchmark_values["benchmark_label"] = (
        benchmark_values["symbol"].astype(str).map(latest_benchmarks).fillna("")
        .replace("", "未披露")
    )
    for benchmark, group in benchmark_values.groupby("benchmark_label", sort=True):
        symbols = set(group["symbol"].astype(str))
        categories = [
            str(latest_metadata.at[symbol, "category"])
            for symbol in symbols if symbol in latest_metadata.index
        ]
        benchmark_groups.append({
            "benchmark": str(benchmark),
            "category": sorted(
                set(categories), key=lambda value: (-categories.count(value), value),
            )[0] if categories else "未分类",
            "fund_count": len(symbols),
            "flows": {
                str(window): _number(
                    sum(float(symbol_flows[window].get(symbol) or 0.0) for symbol in symbols), 2,
                ) if window_summaries[str(window)]["net_flow"] is not None else None
                for window in ROTATION_WINDOWS
            },
        })
    benchmark_groups.sort(key=lambda item: (
        -abs(float(item["flows"].get("5") or 0.0)), item["benchmark"],
    ))
    return {
        "as_of": _date(as_of),
        "items": items,
        "benchmarks": benchmark_groups,
        "daily": [
            {
                "date": _date(row["trade_date"]),
                "flow": _number(row["flow"], 2),
                "cumulative": _number(row["cumulative"], 2),
                "cumulative_ma5": _number(row["cumulative_ma5"], 2),
                "cumulative_ma20": _number(row["cumulative_ma20"], 2),
            }
            for _, row in daily.iterrows()
        ],
        "summary": {
            "status": "ready",
            "net_flow": _number(latest["flow"].sum(), 2),
            "inflow_count": int((latest["flow"] > 0).sum()),
            "outflow_count": int((latest["flow"] < 0).sum()),
            "nav_count": int((latest["price_source"] == "nav").sum()),
            "close_fallback_count": int((latest["price_source"] == "close").sum()),
            "windows": window_summaries,
        },
        "definition": {
            "formula": "份额变化 × 当日净值；净值缺失时使用收盘价并标记",
            "windows": list(ROTATION_WINDOWS),
            "benchmark": "Tushare 原始跟踪基准；缺失时不按名称猜测",
        },
    }
