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

ALGORITHM_VERSION = "QM_ROTATION_V7"
ROTATION_WINDOWS = (1, 3, 5, 20)
MIN_HISTORY = 30
EPS = 1e-12

GROUP_SCORE_WEIGHTS = {
    "trend": 40,
    "breadth": 20,
    "volume": 15,
    "relative_return": 15,
    "rotation": 10,
    }
MIN_GROUP_SCORE_WEIGHT = 60
MIN_PERCENTILE_PEERS = 8

UP_ENTER_THRESHOLD = 0.38
UP_EXIT_THRESHOLD = -0.30
WEAK_ENTER_THRESHOLD = -0.38
WEAK_EXIT_THRESHOLD = 0.30
STRONG_UP_THRESHOLD = 0.55

STATE_LABELS = {
    "strong_up": "强势加速",
    "up": "趋势延续",
    "range": "中位整理",
    "weak": "低位偏弱",
}

REGIMES: tuple[tuple[float, str, str], ...] = (
    (10.0, "ice", "冰点/黄金坑"),
    (25.0, "contraction", "拉锯区"),
    (50.0, "expansion", "强势扩散区"),
    (101.0, "overheat", "过热区"),
)

STATE_CODES = {
    "unavailable": 0,
    "strong_up": 1,
    "up": 2,
    "range": 3,
    "weak": 4,
}

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
    state: pd.DataFrame


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
    if value > 50.0:
        return "overheat", "过热区"
    for upper, code, label in REGIMES:
        if value < upper or (upper == 50.0 and value == upper):
            return code, label
    return "overheat", "过热区"


def _clean_matrix(value: pd.DataFrame, *, columns: list[str] | None = None) -> pd.DataFrame:
    frame = value.copy()
    frame.index = pd.to_datetime(frame.index, errors="coerce")
    frame = frame.loc[~frame.index.isna()]
    frame = frame[~frame.index.duplicated(keep="last")].sort_index()
    if columns is not None:
        frame = frame.reindex(columns=columns)
    return frame.apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan)


def _classify_trend_states(score: pd.DataFrame, eligible: pd.DataFrame) -> pd.DataFrame:
    """Classify mutually exclusive trend states with causal symmetric hysteresis."""
    values = score.to_numpy(dtype=float)
    valid_values = eligible.to_numpy(dtype=bool) & np.isfinite(values)
    codes = np.full(values.shape, STATE_CODES["unavailable"], dtype=np.int8)
    previous = np.full(values.shape[1], STATE_CODES["range"], dtype=np.int8)

    for position in range(values.shape[0]):
        current_values = values[position]
        valid = valid_values[position]
        previous_up = (
            (previous == STATE_CODES["strong_up"])
            | (previous == STATE_CODES["up"])
        )
        previous_weak = previous == STATE_CODES["weak"]

        up = valid & (
            (previous_up & (current_values > UP_EXIT_THRESHOLD))
            | (~previous_up & (current_values >= UP_ENTER_THRESHOLD))
        )
        weak = valid & (
            (previous_weak & (current_values < WEAK_EXIT_THRESHOLD))
            | (~previous_weak & (current_values <= WEAK_ENTER_THRESHOLD))
        )
        strong_up = up & (current_values >= STRONG_UP_THRESHOLD)

        current = np.full(values.shape[1], STATE_CODES["range"], dtype=np.int8)
        current[weak] = STATE_CODES["weak"]
        current[up] = STATE_CODES["up"]
        current[strong_up] = STATE_CODES["strong_up"]
        current[~valid] = STATE_CODES["unavailable"]
        codes[position] = current

        # An unavailable observation leaves no stale state to revive later.
        previous = current.copy()
        previous[~valid] = STATE_CODES["range"]

    return pd.DataFrame(codes, index=score.index, columns=score.columns)


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
    eligible = eligible & score.notna()
    state = _classify_trend_states(score, eligible)
    return TrendMatrices(prices, returns, score, eligible, state)


def _state_masks(trend: TrendMatrices) -> dict[str, pd.DataFrame]:
    return {
        name: trend.eligible & trend.state.eq(STATE_CODES[name])
        for name in STATE_LABELS
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
    supplemental_evidence: dict[str, dict[str, Any]] | None = None,
    supplemental_evidence_history: dict[str, dict[str, dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Return market temperature, evidence decomposition and point-in-time changes."""
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
    volume_scores = pd.Series(np.nan, index=valid.index, dtype=float)
    volume_ratios = pd.Series(np.nan, index=valid.index, dtype=float)
    if amount is not None and not amount.empty:
        amounts = _clean_matrix(amount, columns=list(trend.close.columns)).reindex(trend.close.index)
        total = amounts.sum(axis=1, min_count=max(1, min(10, len(amounts.columns))))
        ratio = total.rolling(5, min_periods=3).mean() / (total.rolling(20, min_periods=10).mean() + EPS)
        volume_ratios = ratio.reindex(valid.index)
        volume_scores = ((volume_ratios - 0.70) / 0.60 * 100).clip(lower=0, upper=100)
    current_date_text = _date(current_date)

    def external_item(
        identifier: str,
        label: str,
        weight: int,
        fallback_note: str,
        evidence_date: str,
        external: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        payload = dict(external.get(identifier) or {})
        score = _number(payload.get("score"), 2)
        evidence_as_of = str(payload.get("as_of") or "")
        note = str(payload.get("note") or fallback_note)
        if payload.get("available") is False or score is None or not 0 <= score <= 100:
            score = None
        if evidence_as_of and evidence_as_of != evidence_date:
            score = None
            note = f"证据日期 {evidence_as_of} 与行情日 {evidence_date} 不一致"
        item: dict[str, Any] = {
            "id": identifier,
            "label": label,
            "score": score,
            "weight": weight,
            "note": note,
        }
        for field_name in (
            "as_of", "window_sessions", "reference_windows", "fund_count",
            "net_subscription_rate", "net_subscription_rate_pct", "event_count",
            "signed_score", "halflife_days", "lookback_days",
        ):
            if field_name in payload:
                item[field_name] = payload[field_name]
        return item

    def evidence_at(
        evidence_date: Any,
        external: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        date_text = _date(evidence_date)
        volume_ratio = volume_ratios.get(evidence_date)
        volume_score = _number(volume_scores.get(evidence_date), 2)
        volume_note = "本地成交额不足"
        if pd.notna(volume_ratio):
            volume_note = f"全样本成交额 5/20 日比值 {_number(volume_ratio, 3)}"
        items = [
            {
                "id": "trend", "label": "趋势分布",
                "score": _number(valid.at[evidence_date, "temperature"], 2),
                "weight": 40, "note": "温度本身",
            },
            {
                "id": "breadth", "label": "涨跌宽度",
                "score": _number(100 * advance.get(evidence_date), 2),
                "weight": 20, "note": "当日上涨股票占比",
            },
            {
                "id": "volume", "label": "量能确认", "score": volume_score,
                "weight": 15, "note": volume_note,
            },
            external_item(
                "etf_capital", "ETF 资金", 15, "等待 ETF 份额快照",
                date_text, external,
            ),
            external_item(
                "sentiment", "情绪代理", 10, "等待可核查资讯情绪",
                date_text, external,
            ),
        ]
        for item in items:
            item["available"] = item["score"] is not None
        available_weight = sum(item["weight"] for item in items if item["available"])
        score = (
            sum(float(item["score"]) * item["weight"] for item in items if item["available"])
            / available_weight if available_weight else None
        )
        return {
            "score": _number(score, 2),
            "available_weight": available_weight,
            "items": items,
        }

    evidence = evidence_at(current_date, supplemental_evidence or {})
    evidence_items = evidence["items"]
    available_weight = int(evidence["available_weight"])
    evidence_history = supplemental_evidence_history or {}
    change_windows: dict[str, dict[str, Any]] = {}
    valid_dates = list(valid.index)
    for window in ROTATION_WINDOWS:
        history_note = (
            f"历史仅有 {len(valid_dates)} 个有效交易日，无法回看 {window} 日"
        )
        reference_date = valid_dates[-window - 1] if len(valid_dates) > window else None
        reference_text = _date(reference_date) if reference_date is not None else ""
        previous = (
            evidence_at(reference_date, evidence_history.get(reference_text) or {})
            if reference_date is not None else None
        )
        previous_items = {
            item["id"]: item for item in (previous or {}).get("items", [])
        }
        compared_items = []
        for current_item in evidence_items:
            previous_item = previous_items.get(current_item["id"])
            current_available = bool(current_item["available"])
            previous_available = bool(previous_item and previous_item["available"])
            comparable = current_available and previous_available
            previous_score = previous_item.get("score") if previous_item else None
            change_pp = None
            if (
                comparable
                and current_item["score"] is not None
                and previous_score is not None
            ):
                change_pp = _number(
                    float(current_item["score"]) - float(previous_score), 2,
                )
            compared_items.append({
                "id": current_item["id"],
                "label": current_item["label"],
                "weight": current_item["weight"],
                "current_score": current_item["score"],
                "previous_score": previous_score,
                "change_pp": change_pp,
                "current_available": current_available,
                "previous_available": previous_available,
                "comparable": comparable,
                "current_note": current_item["note"],
                "previous_note": (
                    str(previous_item.get("note") or "") if previous_item else history_note
                ),
            })
        previous_temperature = (
            _number(valid.at[reference_date, "temperature"], 2)
            if reference_date is not None else None
        )
        change_windows[str(window)] = {
            "window": window,
            "current_as_of": current_date_text,
            "reference_as_of": reference_text,
            "temperature": {
                "current": temperature,
                "previous": previous_temperature,
                "change_pp": _number(
                    float(temperature) - float(previous_temperature), 2,
                ) if temperature is not None and previous_temperature is not None else None,
            },
            "evidence": {
                "previous_score": (previous or {}).get("score"),
                "previous_available_weight": int(
                    (previous or {}).get("available_weight") or 0,
                ),
                "comparable_count": sum(item["comparable"] for item in compared_items),
                "total_count": len(compared_items),
                "items": compared_items,
            },
        }
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
            "score": evidence["score"],
            "available_weight": available_weight,
            "items": evidence_items,
        },
        "change_windows": {
            "default_window": 5,
            "supported_windows": list(ROTATION_WINDOWS),
            "windows": change_windows,
        },
        "quality": quality,
        "definition": {
            "positive_states": ["strong_up", "up"],
            "state_model": "hysteresis",
            "thresholds": {
                "strong_up": STRONG_UP_THRESHOLD,
                "up": UP_ENTER_THRESHOLD,
                "weak": WEAK_ENTER_THRESHOLD,
            },
            "hysteresis": {
                "up_exit": UP_EXIT_THRESHOLD,
                "weak_exit": WEAK_EXIT_THRESHOLD,
            },
            "regimes": {
                "ice": "<10",
                "contraction": "10–<25",
                "expansion": "25–50",
                "overheat": ">50",
            },
            "minimum_history": MIN_HISTORY,
            "evidence": {
                "etf_capital": "近 5 日宽基 ETF 净申购率在最近 252 个有效窗口中的中位百分位",
                "sentiment": "近 30 日质量加权资讯情绪由 [-100,100] 线性映射到 [0,100]",
            },
        },
}


def market_temperature_reference_dates(
    trend: TrendMatrices,
) -> dict[int, str]:
    """Return current and prior valid market-temperature trading dates."""
    eligible = trend.eligible.sum(axis=1)
    dates = list(eligible[eligible > 0].index)
    if not dates:
        return {}
    values = {0: _date(dates[-1])}
    for window in ROTATION_WINDOWS:
        if len(dates) > window:
            values[window] = _date(dates[-window - 1])
    return values


def _candidate(spread: Any) -> str:
    if pd.isna(spread):
        return "unavailable"
    if float(spread) > 0.0025:
        return "strong_dominant"
    if float(spread) < -0.0025:
        return "weak_rebound"
    return "balanced"


def _trailing_sessions(values: list[str], value: str, *, unavailable: str = "unavailable") -> int:
    """Count the current uninterrupted state; unavailable observations break it."""
    if not value or value == unavailable:
        return 0
    count = 0
    for candidate in reversed(values):
        if candidate != value:
            break
        count += 1
    return count


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
    eligible_count = int(trend.eligible.loc[current_date].sum())
    distributions = []
    for state, label in STATE_LABELS.items():
        members = masks[state].loc[current_date]
        values = current_returns[members].dropna()
        distributions.append({
            "state": state,
            "label": label,
            "count": int(members.sum()),
            "share": _number(float(members.sum()) / eligible_count, 4) if eligible_count else None,
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
            "candidate_sessions": _trailing_sessions(
                [str(value) for value in valid["candidate"].tolist()], str(row["candidate"]),
            ),
            "confirmed_sessions": _trailing_sessions(
                [str(value) for value in valid["confirmed"].tolist()], str(row["confirmed"]),
                unavailable="pending",
            ),
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


def _stage(
    positive_ratio: float,
    weak_ratio: float,
    delta_positive: float,
    delta_weak: float,
) -> str:
    del positive_ratio
    if weak_ratio >= 70.0 and delta_weak > -3.0:
        return "extreme_weak"
    if weak_ratio >= 50.0 and delta_weak <= -3.0:
        return "low_repair"
    if delta_positive >= 3.0 and delta_weak <= -3.0:
        return "repair_spread"
    if delta_positive <= -3.0 and delta_weak >= 3.0:
        return "clear_retreat"
    if delta_positive <= -3.0 or delta_weak >= 3.0:
        return "retreat_watch"
    return "unclear"


def _movement_row(item: dict[str, Any], window: int) -> dict[str, Any]:
    signal = dict((item.get("signals") or {}).get(str(window)) or {})
    return {
        "code": str(item.get("code") or ""),
        "name": str(item.get("name") or ""),
        "stage": str(item.get("stage") or ""),
        "stage_label": str(item.get("stage_label") or ""),
        "rotation_change_pp": signal.get("rotation_change_pp"),
        "excess_return": signal.get("excess_return"),
    }


def _movement_summary(items: list[dict[str, Any]], window: int) -> dict[str, Any]:
    """Return auditable direction counts and endpoints for one observation window."""
    available = [
        item for item in items
        if (item.get("signals") or {}).get(str(window), {}).get("rotation_change_pp") is not None
    ]
    ranked = sorted(
        available,
        key=lambda item: (
            float((item.get("signals") or {}).get(str(window), {}).get("rotation_change_pp") or 0.0),
            float((item.get("signals") or {}).get(str(window), {}).get("excess_return") or 0.0),
            str(item.get("name") or ""),
        ),
        reverse=True,
    )
    changes = [
        float((item.get("signals") or {}).get(str(window), {}).get("rotation_change_pp") or 0.0)
        for item in available
    ]
    return {
        "improving_count": sum(value > 0 for value in changes),
        "retreating_count": sum(value < 0 for value in changes),
        "unchanged_count": sum(value == 0 for value in changes),
        "unavailable_count": len(items) - len(available),
        "leader": _movement_row(ranked[0], window) if ranked else None,
        "laggard": _movement_row(ranked[-1], window) if ranked else None,
    }


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


def _midrank_percentile(value: Any, reference: list[Any]) -> float | None:
    """Return a tie-aware cross-sectional percentile without forcing thin cohorts."""
    current = _number(value, 8)
    values = [
        number for candidate in reference
        if (number := _number(candidate, 8)) is not None
    ]
    if current is None or len(values) < MIN_PERCENTILE_PEERS:
        return None
    less = sum(candidate < current for candidate in values)
    equal = sum(candidate == current for candidate in values)
    return _number(100.0 * (less + 0.5 * equal) / len(values), 2)


def _amount_activity_score(value: Any) -> float | None:
    """Map -30%..+30% activity around the prior-20-session baseline to 0..100."""
    activity = _number(value, 8)
    if activity is None:
        return None
    return _number(min(100.0, max(0.0, (activity + 0.30) / 0.60 * 100.0)), 2)


def _group_grade(score: float | None) -> str:
    if score is None:
        return ""
    return "A" if score >= 70 else "B" if score >= 55 else "C" if score >= 40 else "D"


def _score_group_windows(items: list[dict[str, Any]], kind: str) -> None:
    """Attach auditable per-window scores after all peer observations are known."""
    peer_groups: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        peer = "theme" if kind == "theme" else str(item.get("level") or "L1")
        peer_groups.setdefault(peer, []).append(item)

    for window in ROTATION_WINDOWS:
        key = str(window)
        for peers in peer_groups.values():
            excess_reference = [
                (item.get("signals") or {}).get(key, {}).get("excess_return")
                for item in peers
            ]
            rotation_reference = [
                (item.get("signals") or {}).get(key, {}).get("rotation_change_pp")
                for item in peers
            ]
            for item in peers:
                signal = dict((item.get("signals") or {}).get(key) or {})
                positive_ratio = _number(item.get("positive_ratio"), 2)
                advance_ratio = _number(signal.get("advance_ratio"), 8)
                breadth_score = _number(
                    100.0 * advance_ratio if advance_ratio is not None else None, 2,
                )
                volume_score = _amount_activity_score(signal.get("amount_activity"))
                excess_score = _midrank_percentile(
                    signal.get("excess_return"), excess_reference,
                )
                rotation_score = _midrank_percentile(
                    signal.get("rotation_change_pp"), rotation_reference,
                )
                evidence = [
                    {
                        "id": "trend",
                        "label": "趋势向上",
                        "score": positive_ratio,
                        "weight": GROUP_SCORE_WEIGHTS["trend"],
                        "note": (
                            f"强势加速 + 趋势延续占比 {positive_ratio:.2f}%"
                            if positive_ratio is not None else "当前趋势分布不可用"
                        ),
                    },
                    {
                        "id": "breadth",
                        "label": "上涨宽度",
                        "score": breadth_score,
                        "weight": GROUP_SCORE_WEIGHTS["breadth"],
                        "note": (
                            f"近 {window} 日上涨成员占比 {breadth_score:.2f}%"
                            if breadth_score is not None else f"近 {window} 日收益覆盖不足"
                        ),
                    },
                    {
                        "id": "volume",
                        "label": "量能确认",
                        "score": volume_score,
                        "weight": GROUP_SCORE_WEIGHTS["volume"],
                        "note": (
                            f"近 {window} 日量能较此前 20 日 {float(signal['amount_activity']):+.2%}"
                            if signal.get("amount_activity") is not None else "成交额覆盖不足"
                        ),
                    },
                    {
                        "id": "relative_return",
                        "label": "相对收益",
                        "score": excess_score,
                        "weight": GROUP_SCORE_WEIGHTS["relative_return"],
                        "note": (
                            f"近 {window} 日超额 {float(signal['excess_return']):+.2%} · "
                            f"同层级第 {excess_score:.2f} 百分位"
                            if excess_score is not None else "同层级超额收益样本不足"
                        ),
                    },
                    {
                        "id": "rotation",
                        "label": "轮动变化",
                        "score": rotation_score,
                        "weight": GROUP_SCORE_WEIGHTS["rotation"],
                        "note": (
                            f"趋势净改善 {float(signal['rotation_change_pp']):+.2f} pp · "
                            f"同层级第 {rotation_score:.2f} 百分位"
                            if rotation_score is not None else "同层级轮动变化样本不足"
                        ),
                    },
                ]
                for evidence_item in evidence:
                    evidence_item["available"] = evidence_item["score"] is not None
                available_weight = sum(
                    int(evidence_item["weight"])
                    for evidence_item in evidence if evidence_item["available"]
                )
                score = None
                if available_weight >= MIN_GROUP_SCORE_WEIGHT:
                    score = _number(
                        sum(
                            float(evidence_item["score"]) * int(evidence_item["weight"])
                            for evidence_item in evidence if evidence_item["available"]
                        ) / available_weight,
                        2,
                    )
                item.setdefault("scores", {})[key] = {
                    "window": window,
                    "score": score,
                    "grade": _group_grade(score),
                    "available_weight": available_weight,
                    "minimum_weight": MIN_GROUP_SCORE_WEIGHT,
                    "items": evidence,
                }

    for item in items:
        default_score = dict((item.get("scores") or {}).get("5") or {})
        item["score"] = default_score
        item["rotation_score"] = default_score.get("score")
        item["grade"] = str(default_score.get("grade") or "")


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
    positive_mask = masks["strong_up"] | masks["up"]
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
    histories: dict[str, list[dict[str, Any]]] = {}
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
        positive_now = 100.0 * float(positive_mask.loc[current_date, symbols].sum()) / eligible_count
        weak_now = 100.0 * float(masks["weak"].loc[current_date, symbols].sum()) / eligible_count
        signals: dict[str, dict[str, Any]] = {}
        for window in ROTATION_WINDOWS:
            previous_date = window_dates.get(window)
            if previous_date is None:
                signals[str(window)] = {
                    "strong_change_pp": None,
                    "positive_change_pp": None,
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
                positive_before = (
                    100.0 * float(positive_mask.loc[previous_date, symbols].sum())
                    / eligible_before
                )
                weak_before = (
                    100.0 * float(masks["weak"].loc[previous_date, symbols].sum())
                    / eligible_before
                )
                strong_change = strong_now - strong_before
                positive_change = positive_now - positive_before
                weak_change = weak_now - weak_before
            else:
                strong_change = None
                positive_change = None
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
            positive_change_value = _number(positive_change, 2)
            weak_change_value = _number(weak_change, 2)
            signals[str(window)] = {
                "strong_change_pp": strong_change_value,
                "positive_change_pp": positive_change_value,
                "weak_change_pp": weak_change_value,
                "rotation_change_pp": _number(
                    positive_change_value - weak_change_value
                    if positive_change_value is not None and weak_change_value is not None
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
        delta_positive = three_day["positive_change_pp"]
        delta_weak = three_day["weak_change_pp"]
        stage = _stage(
            positive_now,
            weak_now,
            float(delta_positive or 0.0),
            float(delta_weak or 0.0),
        )
        day_returns = trend.returns.loc[current_date, symbols]
        valid_returns = int(day_returns.notna().sum())
        advance_ratio = (
            float((day_returns > 0).sum() / valid_returns) if valid_returns else 0.0
        )
        history_rows = []
        history_length = max(20, min(int(history), 520))
        history_start = max(0, current_position - history_length + 1)
        for position in range(history_start, current_position + 1):
            date_value = trend.close.index[position]
            valid = int(trend.eligible.loc[date_value, symbols].sum())
            if not valid:
                continue
            strong_ratio = 100 * masks["strong_up"].loc[date_value, symbols].sum() / valid
            positive_ratio = 100 * positive_mask.loc[date_value, symbols].sum() / valid
            weak_ratio = 100 * masks["weak"].loc[date_value, symbols].sum() / valid
            stage_key: str | None = None
            if position >= 3:
                previous_date = trend.close.index[position - 3]
                previous_valid = int(trend.eligible.loc[previous_date, symbols].sum())
                previous_coverage = previous_valid / member_count if member_count else 0.0
                if previous_valid >= minimum_members and previous_coverage >= minimum_coverage:
                    previous_positive = (
                        100 * positive_mask.loc[previous_date, symbols].sum() / previous_valid
                    )
                    previous_weak = (
                        100 * masks["weak"].loc[previous_date, symbols].sum() / previous_valid
                    )
                    stage_key = _stage(
                        float(positive_ratio), float(weak_ratio),
                        float(positive_ratio - previous_positive),
                        float(weak_ratio - previous_weak),
                    )
            if position == current_position:
                stage_key = stage
            history_rows.append({
                "date": _date(date_value),
                "strong_ratio": _number(strong_ratio, 2),
                "positive_ratio": _number(positive_ratio, 2),
                "weak_ratio": _number(weak_ratio, 2),
                "eligible": valid,
                "stage": stage_key,
                "stage_label": STAGE_LABELS.get(stage_key or "", "待判定"),
            })
        stage_sessions = _trailing_sessions(
            [str(row["stage"] or "") for row in history_rows], stage, unavailable="",
        )
        item = {
            "code": str(code),
            "name": str(raw.get("name") or code),
            "level": str(raw.get("level") or ("concept" if kind == "theme" else "L1")),
            "parent_code": str(raw.get("parent_code") or ""),
            "member_count": member_count,
            "eligible_count": eligible_count,
            "coverage": round(coverage, 4),
            "strong_ratio": _number(strong_now, 2),
            "positive_ratio": _number(positive_now, 2),
            "weak_ratio": _number(weak_now, 2),
            "delta_positive_3d": _number(delta_positive, 2),
            "delta_weak_3d": _number(delta_weak, 2),
            "signals": signals,
            "advance_ratio": _number(advance_ratio, 4),
            "stage": stage,
            "stage_label": STAGE_LABELS[stage],
            "stage_sessions": stage_sessions,
            "representatives": _representatives(symbols, trend, current_date, names, amount_clean),
        }
        items.append(item)
        histories[str(code)] = history_rows
    _score_group_windows(items, kind)
    items.sort(key=lambda item: (
        -float(item["rotation_score"])
        if item.get("rotation_score") is not None else math.inf,
        -int(item["eligible_count"]),
        item["name"],
    ))
    details = {
        str(item["code"]): {**item, "history": histories.get(str(item["code"]), [])}
        for item in items
    }
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
            "movements": {
                str(window): _movement_summary(items, window) for window in ROTATION_WINDOWS
            },
            "persistence": {
                "median_sessions": _number(
                    pd.Series([item.get("stage_sessions") for item in items]).median(), 1,
                ) if items else None,
                "longest": [
                    {
                        "code": str(item.get("code") or ""),
                        "name": str(item.get("name") or ""),
                        "stage": str(item.get("stage") or ""),
                        "stage_label": str(item.get("stage_label") or ""),
                        "sessions": int(item.get("stage_sessions") or 0),
                    }
                    for item in sorted(
                        items,
                        key=lambda item: (-int(item.get("stage_sessions") or 0), str(item.get("name") or "")),
                    )[:5]
                ],
            },
        },
        "definition": {
            "minimum_members": minimum_members,
            "minimum_coverage": minimum_coverage,
            "coordinates": {"x": "positive_ratio", "y": "weak_ratio", "size": "member_count"},
            "windows": list(ROTATION_WINDOWS),
            "positive_states": ["strong_up", "up"],
            "rotation_change": "positive_change_pp - weak_change_pp",
            "relative_return": "member return median - full-market return median",
            "amount_activity": "member median(recent N-day mean / prior 20-day mean - 1)",
            "score": {
                "weights": dict(GROUP_SCORE_WEIGHTS),
                "minimum_available_weight": MIN_GROUP_SCORE_WEIGHT,
                "volume_mapping": "-30% -> 0, 0% -> 50, +30% -> 100; clipped",
                "relative_components": "同类同层级中位百分位；至少 8 个可用同伴",
                "grades": {"A": ">=70", "B": "55-<70", "C": "40-<55", "D": "<40"},
                "disclaimer": "结构状态评分，不构成交易评级",
            },
        },
    }


def compute_etf_capital_evidence(
    frame: pd.DataFrame,
    *,
    as_of: Any,
    window: int = 5,
    lookback: int = 252,
    min_history: int = 60,
    min_funds: int = 20,
) -> dict[str, Any]:
    """Score broad-ETF subscriptions without scale or look-ahead leakage."""
    target = pd.to_datetime(as_of, errors="coerce")
    target_text = _date(target) if pd.notna(target) else ""

    def unavailable(note: str, *, observed_as_of: str = "") -> dict[str, Any]:
        return {
            "available": False,
            "score": None,
            "as_of": observed_as_of,
            "note": note,
            "window_sessions": int(window),
            "reference_windows": 0,
            "fund_count": 0,
            "net_subscription_rate": None,
            "net_subscription_rate_pct": None,
        }

    required = {"trade_date", "symbol", "shares"}
    if pd.isna(target) or frame is None or frame.empty or not required.issubset(frame.columns):
        return unavailable("等待 ETF 份额与价格快照")
    target = pd.Timestamp(target).tz_localize(None).normalize()
    value = frame.copy()
    value["trade_date"] = pd.to_datetime(value["trade_date"], errors="coerce")
    if getattr(value["trade_date"].dt, "tz", None) is not None:
        value["trade_date"] = value["trade_date"].dt.tz_localize(None)
    value["trade_date"] = value["trade_date"].dt.normalize()
    value["symbol"] = value["symbol"].astype(str)
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
    value = value.dropna(subset=["trade_date", "symbol", "shares", "price"])
    value = value[(value["shares"] > 0) & (value["price"] > 0)]
    value = value.drop_duplicates(["trade_date", "symbol"], keep="last")
    value = value[value["trade_date"] <= target].sort_values(["symbol", "trade_date"])
    if value.empty:
        return unavailable("目标行情日前没有可用 ETF 份额快照")

    observed_dates = pd.DatetimeIndex(sorted(value["trade_date"].unique()))
    latest_observed = pd.Timestamp(observed_dates[-1])
    latest_text = _date(latest_observed)
    if target not in observed_dates:
        return unavailable(
            f"ETF 份额仅到 {latest_text}，目标行情日为 {target_text}",
            observed_as_of=latest_text,
        )

    previous_dates = {
        pd.Timestamp(observed_dates[index]): pd.Timestamp(observed_dates[index - 1])
        for index in range(1, len(observed_dates))
    }
    grouped = value.groupby("symbol", sort=False)
    value["previous_trade_date"] = grouped["trade_date"].shift()
    value["previous_shares"] = grouped["shares"].shift()
    value["expected_previous_date"] = value["trade_date"].map(previous_dates)
    consecutive = value["previous_trade_date"].eq(value["expected_previous_date"])
    eligible = value[consecutive & value["previous_shares"].gt(0)].copy()
    eligible["flow"] = (eligible["shares"] - eligible["previous_shares"]) * eligible["price"]
    eligible["prior_value"] = eligible["previous_shares"] * eligible["price"]
    eligible = eligible.replace([np.inf, -np.inf], np.nan).dropna(
        subset=["flow", "prior_value"],
    )
    daily = eligible.groupby("trade_date").agg(
        net_flow=("flow", "sum"),
        prior_value=("prior_value", "sum"),
        fund_count=("symbol", "nunique"),
    ).reindex(observed_dates)
    daily["rate"] = (
        daily["net_flow"] / daily["prior_value"].where(daily["prior_value"] > 0)
    ).where(daily["fund_count"] >= int(min_funds))
    rolling_rate = daily["rate"].rolling(int(window), min_periods=int(window)).sum()
    current_rate = rolling_rate.get(target)
    fund_count = int(daily.at[target, "fund_count"] or 0) if pd.notna(
        daily.at[target, "fund_count"]
    ) else 0
    if pd.isna(current_rate):
        return {
            **unavailable(
                f"目标行情日需至少 {min_funds} 只 ETF 的连续份额快照",
                observed_as_of=target_text,
            ),
            "fund_count": fund_count,
        }

    reference = rolling_rate.loc[:target].dropna().tail(max(1, int(lookback)))
    if len(reference) < int(min_history):
        return {
            **unavailable(
                f"ETF 资金历史仅有 {len(reference)} 个有效窗口，至少需要 {min_history} 个",
                observed_as_of=target_text,
            ),
            "reference_windows": len(reference),
            "fund_count": fund_count,
            "net_subscription_rate": _number(current_rate, 6),
            "net_subscription_rate_pct": _number(float(current_rate) * 100, 2),
        }
    less = int((reference < float(current_rate)).sum())
    equal = int((reference == float(current_rate)).sum())
    score = 100.0 * (less + 0.5 * equal) / len(reference)
    rate_pct = _number(float(current_rate) * 100, 2)
    score_value = _number(score, 2)
    return {
        "available": True,
        "score": score_value,
        "as_of": target_text,
        "note": (
            f"近 {window} 日净申购率 {rate_pct:+.2f}% · "
            f"近 {len(reference)} 个窗口第 {score_value:.2f} 百分位 · {fund_count} 只"
        ),
        "window_sessions": int(window),
        "reference_windows": len(reference),
        "fund_count": fund_count,
        "net_subscription_rate": _number(current_rate, 6),
        "net_subscription_rate_pct": rate_pct,
    }


def estimate_etf_flows(frame: pd.DataFrame, *, history: int = 780) -> dict[str, Any]:
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
    latest_metadata = latest.drop_duplicates("symbol").set_index("symbol")

    def flow_streak(symbol: str) -> int:
        observations = dated[dated["symbol"].astype(str) == symbol]
        by_date = {
            pd.Timestamp(row["trade_date"]): float(row["flow"])
            for _, row in observations.iterrows()
            if pd.notna(row["flow"])
        }
        direction = 0
        sessions = 0
        for date_value in reversed(available_dates):
            flow = by_date.get(date_value)
            if flow is None or flow == 0:
                break
            next_direction = 1 if flow > 0 else -1
            if direction and next_direction != direction:
                break
            direction = next_direction
            sessions += 1
        return direction * sessions

    flow_streaks = {
        str(symbol): flow_streak(str(symbol)) for symbol in latest_metadata.index.astype(str)
    }
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

    def flow_descriptor(row: pd.Series | None) -> dict[str, Any] | None:
        if row is None:
            return None
        symbol = str(row["symbol"])
        metadata = latest_metadata.loc[symbol] if symbol in latest_metadata.index else None
        return {
            "symbol": symbol,
            "name": str(metadata.get("name") if metadata is not None else symbol),
            "benchmark": str(metadata.get("benchmark") if metadata is not None else "未披露"),
            "flow": _number(row["flow"], 2),
        }

    for window, values in window_frames.items():
        positive = values[values["flow"] > 0].sort_values(["flow", "symbol"], ascending=[False, True])
        negative = values[values["flow"] < 0].sort_values(["flow", "symbol"], ascending=[True, True])
        window_summaries[str(window)].update({
            "largest_inflow": flow_descriptor(positive.iloc[0]) if not positive.empty else None,
            "largest_outflow": flow_descriptor(negative.iloc[0]) if not negative.empty else None,
        })
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
            "flow_streak_sessions": flow_streaks.get(str(row["symbol"]), 0),
            "price": _number(row["price"], 4),
            "price_source": str(row["price_source"]),
        })
    daily = dated.groupby("trade_date", as_index=False)["flow"].sum().tail(max(20, min(int(history), 1000)))
    daily["cumulative"] = daily["flow"].cumsum()
    daily["cumulative_ma5"] = daily["cumulative"].rolling(5, min_periods=5).mean()
    daily["cumulative_ma20"] = daily["cumulative"].rolling(20, min_periods=20).mean()
    benchmark_groups: list[dict[str, Any]] = []
    benchmark_values = dated.copy()
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
    inflow_streaks = sorted(
        (item for item in items if int(item.get("flow_streak_sessions") or 0) > 0),
        key=lambda item: (-int(item["flow_streak_sessions"]), item["name"], item["symbol"]),
    )
    outflow_streaks = sorted(
        (item for item in items if int(item.get("flow_streak_sessions") or 0) < 0),
        key=lambda item: (int(item["flow_streak_sessions"]), item["name"], item["symbol"]),
    )
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
                "streaks": {
                    "longest_inflow": {
                    "symbol": inflow_streaks[0]["symbol"], "name": inflow_streaks[0]["name"],
                    "sessions": int(inflow_streaks[0]["flow_streak_sessions"]),
                } if inflow_streaks else None,
                "longest_outflow": {
                    "symbol": outflow_streaks[0]["symbol"], "name": outflow_streaks[0]["name"],
                    "sessions": abs(int(outflow_streaks[0]["flow_streak_sessions"])),
                } if outflow_streaks else None,
            },
        },
        "definition": {
            "formula": "份额变化 × 当日净值；净值缺失时使用收盘价并标记",
            "windows": list(ROTATION_WINDOWS),
            "benchmark": "Tushare 原始跟踪基准；缺失时不按名称猜测",
        },
    }
