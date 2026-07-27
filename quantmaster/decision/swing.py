"""面向 1-7 个交易日持有期的日间选股模型。

模型将短中期趋势、MACD、价格位置、资金量代理和波动风险组合成截面分数。
所有滚动特征只读取当日及以前数据；T 日分数供 T+1 日开盘交易。
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

EPS = 1e-12


def _rank(values: pd.DataFrame) -> pd.DataFrame:
    return values.rank(axis=1, pct=True, method="average")


def swing_score_panel(panel: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """计算每日股票截面综合分（0-100，越高越适合短周期做多）。"""
    required = {"close", "high", "low"}
    missing = required - panel.keys()
    if missing:
        raise ValueError(f"选股行情缺少字段: {sorted(missing)}")
    close = panel["close"].sort_index()
    high = panel["high"].reindex_like(close)
    low = panel["low"].reindex_like(close)
    returns = close.pct_change(fill_method=None)

    mom5 = close.pct_change(5, fill_method=None)
    mom20 = close.pct_change(20, fill_method=None)
    ema10 = close.ewm(span=10, adjust=False).mean()
    ema30 = close.ewm(span=30, adjust=False).mean()
    trend = ema10 / (ema30 + EPS) - 1.0
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    macd_hist = macd - macd.ewm(span=9, adjust=False).mean()
    low20 = low.rolling(20, min_periods=10).min()
    high20 = high.rolling(20, min_periods=10).max()
    price_position = (close - low20) / (high20 - low20 + EPS)
    volatility = returns.rolling(20, min_periods=10).std()

    money = panel.get("amount", panel.get("volume"))
    if money is None:
        money_ratio = pd.DataFrame(1.0, index=close.index, columns=close.columns)
    else:
        money = money.reindex_like(close)
        money_ratio = (money.rolling(5, min_periods=3).mean()
                       / (money.rolling(20, min_periods=10).mean() + EPS))

    score = 100 * (
        0.18 * _rank(mom5)
        + 0.17 * _rank(mom20)
        + 0.20 * _rank(trend)
        + 0.18 * _rank(macd_hist / (close + EPS))
        + 0.12 * _rank(price_position)
        + 0.10 * _rank(money_ratio.clip(upper=3.0))
        + 0.05 * (1.0 - _rank(volatility))
    )

    tradable = close.gt(1.0) & close.notna()
    if "amount" in panel:
        liquid = panel["amount"].reindex_like(close).rolling(20, min_periods=10).mean() >= 1e7
        tradable &= liquid
    return score.where(tradable).clip(0, 100)


def market_exposure(panel: dict[str, pd.DataFrame]) -> pd.Series:
    """根据股票池宽度给出逐日风险敞口：牛/震荡/熊约为 100%/65%/30%。"""
    close = panel["close"].sort_index()
    returns = close.pct_change(fill_method=None)
    advance = (returns > 0).sum(axis=1) / returns.notna().sum(axis=1).replace(0, np.nan)
    ma20 = close.rolling(20, min_periods=10).mean()
    above = (close > ma20).sum(axis=1) / ma20.notna().sum(axis=1).replace(0, np.nan)
    state_score = (advance - 0.5) + (above - 0.5)
    exposure = pd.Series(0.65, index=close.index, dtype=float)
    exposure[state_score >= 0.20] = 1.0
    exposure[state_score <= -0.20] = 0.30
    return exposure.where(state_score.notna(), 0.30)


def _safe_float(value: Any, digits: int = 4) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return round(number, digits) if math.isfinite(number) else None


def daily_selection(
    panel: dict[str, pd.DataFrame],
    top_n: int = 10,
    horizon: int = 3,
    industry_map: dict[str, str] | None = None,
    name_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """生成今日选股清单，以及 1-7 日持有建议、风控位和理由。"""
    if not 1 <= horizon <= 7:
        raise ValueError("horizon 必须在 1-7 个交易日之间")
    if top_n < 1:
        raise ValueError("top_n 必须为正整数")
    industry_map = industry_map or {}
    name_map = name_map or {}
    scores = swing_score_panel(panel)
    valid_dates = scores.dropna(how="all")
    if valid_dates.empty:
        raise ValueError("有效历史不足，至少需要约 20 个交易日且成交额满足过滤条件")
    date = valid_dates.index[-1]
    latest = valid_dates.iloc[-1].dropna().sort_values(ascending=False).head(top_n)
    close = panel["close"].reindex(scores.index)
    returns = close.pct_change(fill_method=None)
    vol = returns.rolling(20, min_periods=10).std().loc[date]
    ema10 = close.ewm(span=10, adjust=False).mean().loc[date]
    ema30 = close.ewm(span=30, adjust=False).mean().loc[date]
    macd = close.ewm(span=12, adjust=False).mean() - close.ewm(span=26, adjust=False).mean()
    macd_hist = (macd - macd.ewm(span=9, adjust=False).mean()).loc[date]
    money = panel.get("amount", panel.get("volume"))
    money_ratio = None
    if money is not None:
        money = money.reindex_like(close)
        money_ratio = (money.rolling(5, min_periods=3).mean()
                       / (money.rolling(20, min_periods=10).mean() + EPS)).loc[date]

    exposure = float(market_exposure(panel).loc[date])
    picks: list[dict[str, Any]] = []
    for symbol, raw_score in latest.items():
        score = float(raw_score)
        daily_vol = float(vol.get(symbol, np.nan))
        if not math.isfinite(daily_vol):
            daily_vol = 0.025
        stop_loss = min(0.10, max(0.03, daily_vol * math.sqrt(horizon) * 1.5))
        expected = ((score - 50.0) / 50.0) * daily_vol * math.sqrt(horizon) * 0.65
        take_profit = min(0.20, max(stop_loss * 1.6, expected * 1.8))
        confidence = min(0.90, 0.42 + abs(score - 50.0) / 100.0)
        trend_up = bool(ema10.get(symbol, np.nan) > ema30.get(symbol, np.nan))
        hist_value = float(macd_hist.get(symbol, np.nan))
        flow_value = float(money_ratio.get(symbol, np.nan)) if money_ratio is not None else np.nan
        reasons = ["短均线在长均线上方" if trend_up else "均线趋势尚未转强"]
        reasons.append("MACD 柱为正" if hist_value > 0 else "MACD 柱为负")
        if math.isfinite(flow_value):
            reasons.append("近5日资金量放大" if flow_value > 1.05 else "资金量未明显放大")
        action = "buy" if score >= 65 and exposure >= 0.65 else (
            "watch" if score >= 50 else "avoid")
        picks.append({
            "rank": len(picks) + 1,
            "symbol": symbol,
            "name": name_map.get(symbol, "名称待同步"),
            "industry": industry_map.get(symbol, "未知"),
            "score": round(score, 2),
            "action": action,
            "holding_days": horizon,
            "confidence": round(confidence, 4),
            "last_close": _safe_float(close.at[date, symbol]),
            "expected_return": round(expected, 4),
            "stop_loss": round(stop_loss, 4),
            "take_profit": round(take_profit, 4),
            "macd_hist": _safe_float(hist_value, 6),
            "money_ratio": _safe_float(flow_value),
            "reasons": reasons,
        })
    regime = "bull" if exposure >= 0.99 else ("range" if exposure >= 0.60 else "bear")
    return {
        "model_version": "swing-v1",
        "signal_date": str(date.date()) if hasattr(date, "date") else str(date),
        "holding_horizon_days": horizon,
        "market_regime": regime,
        "recommended_exposure": exposure,
        "picks": picks,
        "risk_note": "信号于收盘后生成，按 T+1 开盘执行；止损/止盈为波动率规则，不构成投资建议。",
    }
