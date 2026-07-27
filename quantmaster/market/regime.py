"""牛熊与趋势状态模型。

这里的“未来”是基于趋势持续性、MACD、波动和市场宽度的规则型概率展望，
不是确定性预测。所有历史状态只使用当日及以前的数据，可直接用于回测。
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

EPS = 1e-12

STATE_LABELS = {
    "strong_up": "强势上行",
    "up": "上行",
    "range": "震荡",
    "down": "下行",
    "strong_down": "强势下行",
}


def _state(score: float) -> str:
    if score >= 0.55:
        return "strong_up"
    if score >= 0.20:
        return "up"
    if score > -0.20:
        return "range"
    if score > -0.55:
        return "down"
    return "strong_down"


def _number(value: Any, digits: int = 4) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return round(number, digits) if math.isfinite(number) else None


def indicator_frame(bars: pd.DataFrame) -> pd.DataFrame:
    """计算逐日 MACD、资金量代理、波动和趋势分数。"""
    if "close" not in bars:
        raise ValueError("行情缺少 close 列")
    close = pd.to_numeric(bars["close"], errors="coerce").sort_index()
    if close.dropna().empty:
        raise ValueError("close 没有有效数据")

    result = pd.DataFrame(index=close.index)
    result["close"] = close
    result["return_1d"] = close.pct_change(fill_method=None)
    result["momentum_5d"] = close.pct_change(5, fill_method=None)
    result["momentum_20d"] = close.pct_change(20, fill_method=None)
    result["ema_10"] = close.ewm(span=10, adjust=False).mean()
    result["ema_30"] = close.ewm(span=30, adjust=False).mean()
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    result["macd"] = ema12 - ema26
    result["macd_signal"] = result["macd"].ewm(span=9, adjust=False).mean()
    result["macd_hist"] = result["macd"] - result["macd_signal"]
    result["volatility_20d"] = result["return_1d"].rolling(20, min_periods=10).std()

    vol = result["volatility_20d"].replace(0, np.nan)
    ma_component = ((result["ema_10"] / result["ema_30"] - 1) /
                    (vol * math.sqrt(10) + EPS)).clip(-2, 2) / 2
    macd_component = (result["macd_hist"] / (close * vol + EPS)).clip(-2, 2) / 2
    momentum_component = (result["momentum_5d"] /
                          (vol * math.sqrt(5) + EPS)).clip(-2, 2) / 2
    result["trend_score"] = (
        0.45 * ma_component + 0.35 * macd_component + 0.20 * momentum_component
    ).clip(-1, 1)
    result["bull_score"] = ((result["trend_score"] + 1) * 50).clip(0, 100)

    for field in ("volume", "amount"):
        if field in bars:
            values = pd.to_numeric(bars[field], errors="coerce").reindex(result.index)
            result[field] = values
            result[f"{field}_ratio_5_20"] = (
                values.rolling(5, min_periods=3).mean()
                / (values.rolling(20, min_periods=10).mean() + EPS)
            )
    result["state"] = result["trend_score"].apply(
        lambda value: _state(float(value)) if pd.notna(value) else "unknown"
    )
    return result


def _current(frame: pd.DataFrame) -> dict[str, Any]:
    valid = frame[frame["trend_score"].notna()]
    if valid.empty:
        raise ValueError("至少需要约 20 条有效行情才能判断趋势")
    row = valid.iloc[-1]
    state = str(row["state"])
    return {
        "as_of": str(valid.index[-1]),
        "state": state,
        "state_label": STATE_LABELS.get(state, state),
        "bull_score": _number(row["bull_score"], 2),
        "trend_score": _number(row["trend_score"], 4),
        "close": _number(row["close"]),
        "return_1d": _number(row["return_1d"]),
        "momentum_5d": _number(row["momentum_5d"]),
        "momentum_20d": _number(row["momentum_20d"]),
        "macd": _number(row["macd"]),
        "macd_signal": _number(row["macd_signal"]),
        "macd_hist": _number(row["macd_hist"]),
        "volatility_20d": _number(row["volatility_20d"]),
        "volume_ratio": _number(row.get("volume_ratio_5_20")),
        "amount_ratio": _number(row.get("amount_ratio_5_20")),
    }


def _forecast(frame: pd.DataFrame, horizons: tuple[int, ...]) -> list[dict[str, Any]]:
    valid = frame[frame["trend_score"].notna()]
    last = valid.iloc[-1]
    score = float(last["trend_score"])
    vol = float(last["volatility_20d"]) if pd.notna(last["volatility_20d"]) else 0.02
    stability = 1.0 - min(float(valid["trend_score"].tail(10).std(ddof=0) or 0.0), 1.0)
    result = []
    for horizon in horizons:
        if not 1 <= horizon <= 7:
            raise ValueError("预测周期仅支持 1-7 个交易日")
        decay = math.exp(-(horizon - 1) / 7)
        signal = score * decay
        probability_up = 1.0 / (1.0 + math.exp(-2.2 * signal))
        expected = signal * vol * math.sqrt(horizon) * 0.55
        confidence = min(0.90, (0.45 + 0.35 * abs(score)) * (0.65 + 0.35 * stability) * decay)
        result.append({
            "horizon_days": horizon,
            "direction": "up" if probability_up >= 0.55 else (
                "down" if probability_up <= 0.45 else "range"),
            "probability_up": round(probability_up, 4),
            "expected_return": round(expected, 4),
            "confidence": round(confidence, 4),
        })
    return result


def _forecast_validation(
    frame: pd.DataFrame, horizons: tuple[int, ...]
) -> list[dict[str, Any]]:
    """用已发生的后续收益评价历史概率；末尾未知未来严格剔除。"""
    rows: list[dict[str, Any]] = []
    for horizon in horizons:
        decay = math.exp(-(horizon - 1) / 7)
        signal = frame["trend_score"] * decay
        probability = 1.0 / (1.0 + np.exp(-2.2 * signal))
        forward_return = frame["close"].shift(-horizon) / frame["close"] - 1.0
        valid = probability.notna() & forward_return.notna()
        sample_count = int(valid.sum())
        if sample_count == 0:
            rows.append({
                "horizon_days": horizon, "samples": 0,
                "direction_accuracy": None, "brier_score": None,
            })
            continue
        actual_up = (forward_return[valid] > 0).astype(float)
        predicted_up = (probability[valid] >= 0.5).astype(float)
        rows.append({
            "horizon_days": horizon,
            "samples": sample_count,
            "direction_accuracy": round(float((predicted_up == actual_up).mean()), 4),
            "brier_score": round(float(((probability[valid] - actual_up) ** 2).mean()), 4),
            "average_forward_return": round(float(forward_return[valid].mean()), 4),
        })
    return rows


def analyze_bars(
    bars: pd.DataFrame, horizons: tuple[int, ...] = (1, 3, 5, 7)
) -> dict[str, Any]:
    """返回当前、过去逐日状态和未来 1-7 日概率展望。"""
    frame = indicator_frame(bars)
    past_columns = [
        "close", "return_1d", "trend_score", "bull_score", "state",
        "macd", "macd_signal", "macd_hist", "volume_ratio_5_20", "amount_ratio_5_20",
    ]
    past = frame[[c for c in past_columns if c in frame]].copy()
    return {
        "current": _current(frame),
        "past": past,
        "future": _forecast(frame, horizons),
        "forecast_validation": _forecast_validation(frame, horizons),
        "forecast_note": "规则型概率展望，只使用截至分析日的数据，不代表确定收益。",
    }


def _market_bars(panel: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    close = panel["close"].sort_index()
    returns = close.pct_change(fill_method=None)
    equal_return = returns.mean(axis=1, skipna=True).fillna(0.0)
    bars = pd.DataFrame(index=close.index)
    bars["close"] = 100.0 * (1.0 + equal_return).cumprod()
    for field in ("volume", "amount"):
        if field in panel:
            bars[field] = panel[field].reindex_like(close).sum(axis=1, min_count=1)
    breadth = pd.DataFrame(index=close.index)
    breadth["advance_ratio"] = (returns > 0).sum(axis=1) / returns.notna().sum(axis=1).replace(0, np.nan)
    ma20 = close.rolling(20, min_periods=10).mean()
    breadth["above_ma20_ratio"] = (close > ma20).sum(axis=1) / ma20.notna().sum(axis=1).replace(0, np.nan)
    return bars, breadth


def analyze_market(
    panel: dict[str, pd.DataFrame], horizons: tuple[int, ...] = (1, 3, 5, 7)
) -> dict[str, Any]:
    """股票池等权市场状态；用宽度修正单一指数可能造成的失真。"""
    bars, breadth = _market_bars(panel)
    frame = indicator_frame(bars)
    breadth_score = (
        (breadth["advance_ratio"] - 0.5) * 1.2
        + (breadth["above_ma20_ratio"] - 0.5) * 0.8
    ).clip(-1, 1)
    frame["trend_score"] = (0.70 * frame["trend_score"] + 0.30 * breadth_score).clip(-1, 1)
    frame["bull_score"] = (frame["trend_score"] + 1) * 50
    frame["state"] = frame["trend_score"].apply(
        lambda value: _state(float(value)) if pd.notna(value) else "unknown")
    frame = frame.join(breadth)
    report = {
        "current": _current(frame),
        "past": frame,
        "future": _forecast(frame, horizons),
        "forecast_validation": _forecast_validation(frame, horizons),
        "forecast_note": "股票池等权趋势 + 市场宽度的规则型概率展望，不代表确定收益。",
    }
    last = frame.dropna(subset=["trend_score"]).iloc[-1]
    report["current"].update({
        "advance_ratio": _number(last.get("advance_ratio")),
        "above_ma20_ratio": _number(last.get("above_ma20_ratio")),
        "universe_size": int(panel["close"].shape[1]),
    })
    return report


def analyze_sectors(
    panel: dict[str, pd.DataFrame], industry_map: dict[str, str]
) -> pd.DataFrame:
    """按行业聚合当前牛熊/趋势/宽度，返回强弱排序。"""
    close = panel["close"]
    rows: list[dict[str, Any]] = []
    groups: dict[str, list[str]] = {}
    for symbol in close.columns:
        sector = industry_map.get(symbol)
        if sector:
            groups.setdefault(sector, []).append(symbol)
    for sector, symbols in groups.items():
        if len(symbols) < 2:
            continue
        sub = {name: values.loc[:, values.columns.intersection(symbols)]
               for name, values in panel.items() if isinstance(values, pd.DataFrame)}
        try:
            current = analyze_market(sub, horizons=(1,))["current"]
        except ValueError:
            continue
        rows.append({"sector": sector, "members": len(symbols), **current})
    if not rows:
        return pd.DataFrame(columns=["sector", "members", "state", "bull_score"])
    return pd.DataFrame(rows).sort_values(["bull_score", "members"], ascending=[False, False])
