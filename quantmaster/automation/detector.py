from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from quantmaster.automation.models import AlertEvent, stable_hash


def _percentile(value: float, history: pd.Series) -> float:
    clean = pd.to_numeric(history, errors="coerce").dropna()
    if clean.empty:
        return 0.0
    return float((clean <= value).mean() * 100)


def _closed(frame: pd.DataFrame, cutoff: pd.Timestamp | None) -> pd.DataFrame:
    result = frame.sort_index()
    if cutoff is not None:
        index = pd.to_datetime(result.index)
        result = result.loc[index <= cutoff]
    return result


@dataclass
class MarketTurnDetector:
    """把分钟行情转换为可审计的变盘候选；目标策略负责最终确认和冷却。"""

    direction: str = "neutral"
    consecutive: int = 0

    def evaluate(
        self,
        bars: dict[str, pd.DataFrame],
        breadth: pd.Series | None = None,
        *,
        cutoff: pd.Timestamp | None = None,
    ) -> AlertEvent | None:
        returns: dict[str, float] = {}
        amount_scores: list[float] = []
        as_of: list[pd.Timestamp] = []
        history_sessions: set[pd.Timestamp] = set()

        for symbol, raw in bars.items():
            frame = _closed(raw, cutoff)
            if len(frame) < 4 or "close" not in frame:
                continue
            close = pd.to_numeric(frame["close"], errors="coerce").dropna()
            if len(close) < 4:
                continue
            value = float(close.iloc[-1] / close.iloc[-4] - 1)
            returns[symbol] = value
            stamp = pd.Timestamp(close.index[-1])
            as_of.append(stamp)
            history_sessions.update(pd.to_datetime(close.index).normalize().unique())
            if "amount" in frame:
                amount = pd.to_numeric(frame["amount"], errors="coerce")
                recent = float(amount.iloc[-3:].sum())
                historical = amount.groupby(pd.to_datetime(frame.index).normalize()).apply(
                    lambda values: float(values.iloc[-3:].sum()))
                amount_scores.append(_percentile(recent, historical.iloc[:-1]))

        if len(returns) < 3 or not as_of:
            self.direction, self.consecutive = "neutral", 0
            return None
        signs = np.sign(list(returns.values()))
        positive, negative = int((signs > 0).sum()), int((signs < 0).sum())
        if max(positive, negative) < 3:
            self.direction, self.consecutive = "neutral", 0
            return None
        direction = "up" if positive > negative else "down"
        median_return = float(np.median(list(returns.values())))

        breadth_delta = 0.0
        breadth_as_of = None
        breadth_history = pd.Series(dtype=float)
        if breadth is not None:
            series = pd.to_numeric(breadth, errors="coerce").dropna().sort_index()
            if cutoff is not None:
                series = series.loc[pd.to_datetime(series.index) <= cutoff]
            if len(series) >= 4:
                breadth_delta = float((series.iloc[-1] - series.iloc[-4]) * 100)
                breadth_as_of = pd.Timestamp(series.index[-1])
                breadth_history = series.diff(3).abs().mul(100).iloc[:-1]

        if abs(median_return) < 0.004 and abs(breadth_delta) < 15:
            self.direction, self.consecutive = "neutral", 0
            return None

        return_history: list[float] = []
        for raw in bars.values():
            if "close" not in raw or len(raw) < 4:
                continue
            close = pd.to_numeric(raw["close"], errors="coerce")
            return_history.extend(
                close.pct_change(3, fill_method=None).abs().dropna().iloc[:-1].tolist()
            )
        shock_score = _percentile(abs(median_return), pd.Series(return_history))
        breadth_score = _percentile(abs(breadth_delta), breadth_history)
        amount_score = float(np.mean(amount_scores)) if amount_scores else 0.0
        score = round(0.55 * shock_score + 0.30 * breadth_score + 0.15 * amount_score, 2)

        enough_history = len(history_sessions) >= 10
        if not enough_history:
            if abs(median_return) < 0.01 and abs(breadth_delta) < 25:
                return None
            score = max(score, 50.0)

        if direction == self.direction:
            self.consecutive += 1
        else:
            self.direction, self.consecutive = direction, 1
        stamp = min(as_of + ([breadth_as_of] if breadth_as_of is not None else []))
        evidence = [
            f"{len(returns)} 个指数中 {max(positive, negative)} 个同向",
            f"15 分钟指数收益中位数 {median_return:+.2%}",
            f"上涨家数比例变化 {breadth_delta:+.1f} 个百分点",
            f"成交异常分位 {amount_score:.0f}",
        ]
        if not enough_history:
            evidence.append("分钟历史不足 10 个交易日，按极端阈值低置信度判断")
        payload: dict[str, Any] = {
            "returns": returns, "median_return": median_return,
            "breadth_delta_pp": breadth_delta, "confirmation_count": self.consecutive,
            "history_sessions": len(history_sessions), "confidence": "normal" if enough_history else "low",
        }
        bucket = stamp.floor("5min").isoformat()
        return AlertEvent(
            kind="market_turn", score=score,
            severity="critical" if score >= 95 else ("high" if score >= 80 else "medium"),
            direction=direction, data_as_of=stamp.isoformat(), evidence=evidence,
            dedupe_key=stable_hash({"kind": "market_turn", "direction": direction, "bar": bucket}),
            payload=payload,
        )


def close_regime_event(current: dict[str, Any], previous: dict[str, Any] | None,
                       data_as_of: str) -> AlertEvent | None:
    state = str(current.get("state") or "unknown")
    score = float(current.get("bull_score") or 0)
    old_state = str((previous or {}).get("state") or "unknown")
    old_score = float((previous or {}).get("bull_score") or score)
    crossed = any((old_score < line <= score) or (score < line <= old_score) for line in (40, 60))
    if previous and state == old_state and not crossed and abs(score - old_score) < 15:
        return None
    direction = "up" if score > old_score else "down" if score < old_score else "neutral"
    event_score = min(100.0, 55 + abs(score - old_score) * 2 + (20 if state != old_state else 0))
    return AlertEvent(
        kind="market_close", score=event_score,
        severity="high" if event_score >= 80 else "medium", direction=direction,
        data_as_of=data_as_of,
        evidence=[f"市场状态 {old_state} → {state}", f"牛市分数 {old_score:.1f} → {score:.1f}"],
        dedupe_key=stable_hash({"kind": "market_close", "date": data_as_of[:10], "state": state}),
        payload={"current": current, "previous": previous or {}},
    )
