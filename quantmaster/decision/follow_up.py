"""历史选股快照的 T+1 事后价格验证。"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd


def _finite_price(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def _normalize_bars(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["open", "close"])
    columns = {str(column).casefold(): column for column in frame.columns}
    values = pd.DataFrame(index=frame.index)
    for field in ("open", "close"):
        source = columns.get(field)
        values[field] = (
            pd.to_numeric(frame[source], errors="coerce")
            if source is not None
            else float("nan")
        )
    dates = pd.DatetimeIndex(pd.to_datetime(values.index, errors="coerce"))
    if dates.tz is not None:
        dates = dates.tz_localize(None)
    values.index = dates.normalize()
    values = values.loc[~values.index.isna()]
    values = values.loc[~values.index.duplicated(keep="last")]
    return values.sort_index()


def price_frames_from_panel(
    panel: Mapping[str, pd.DataFrame], symbols: Sequence[str],
) -> dict[str, pd.DataFrame]:
    """Extract per-symbol open/close frames from a field-oriented price panel."""
    frames: dict[str, pd.DataFrame] = {}
    for symbol in dict.fromkeys(symbols):
        columns: dict[str, pd.Series] = {}
        for field in ("open", "close"):
            values = panel.get(field)
            if isinstance(values, pd.DataFrame) and symbol in values.columns:
                columns[field] = values[symbol]
        if columns:
            frames[symbol] = pd.DataFrame(columns)
    return frames


def _holding_horizon(snapshot: Mapping[str, Any]) -> int:
    try:
        return max(1, int(snapshot.get("holding_horizon_days") or 1))
    except (TypeError, ValueError):
        return 1


def _signal_date(snapshot: Mapping[str, Any]) -> pd.Timestamp | None:
    try:
        value = pd.Timestamp(str(snapshot.get("signal_date") or "")).normalize()
    except (TypeError, ValueError):
        return None
    return None if pd.isna(value) else value


def _follow_up_timeline(
    normalized: Mapping[str, pd.DataFrame],
    signal_date: pd.Timestamp | None,
    horizon: int,
) -> tuple[int, pd.Timestamp | None, pd.Timestamp | None, set[pd.Timestamp]]:
    calendar: set[pd.Timestamp] = set()
    all_close_dates: set[pd.Timestamp] = set()
    if signal_date is not None:
        for frame in normalized.values():
            close_dates = frame.index[frame["close"].notna()]
            all_close_dates.update(close_dates)
            calendar.update(date for date in close_dates if date > signal_date)
    sessions = sorted(calendar)
    completed_sessions = min(len(sessions), horizon)
    entry_date = sessions[0] if sessions else None
    evaluation_date = sessions[completed_sessions - 1] if completed_sessions else None
    return completed_sessions, entry_date, evaluation_date, all_close_dates


def _follow_up_status(
    *,
    has_picks: bool,
    signal_date: pd.Timestamp | None,
    completed_sessions: int,
    horizon: int,
    has_price_data: bool,
) -> str:
    if not has_picks or signal_date is None:
        return "unavailable"
    if completed_sessions >= horizon:
        return "completed"
    if completed_sessions:
        return "in_progress"
    if has_price_data:
        return "pending"
    return "unavailable"


def _pick_outcome(
    pick: Mapping[str, Any],
    position: int,
    status: str,
    frame: pd.DataFrame | None,
    entry_date: pd.Timestamp | None,
    evaluation_date: pd.Timestamp | None,
) -> tuple[dict[str, Any], float | None]:
    symbol = str(pick.get("symbol") or "")
    outcome: dict[str, Any] = {
        "rank": int(pick.get("rank") or position),
        "symbol": symbol,
        "name": str(pick.get("name") or ""),
        "target_weight": (
            round(float(pick.get("target_weight") or 0), 6)
            if "target_weight" in pick else None
        ),
        "status": "pending" if status == "pending" else "unavailable",
        "entry_date": entry_date.date().isoformat() if entry_date is not None else None,
        "entry_price": None,
        "price_date": None,
        "price": None,
        "price_change": None,
        "return": None,
    }
    if frame is None or frame.empty or entry_date is None or evaluation_date is None:
        return outcome, None
    entry_value = frame.at[entry_date, "open"] if entry_date in frame.index else None
    entry_price = _finite_price(entry_value)
    if entry_price is None:
        outcome["status"] = "missing_entry"
        return outcome, None
    marks = frame.loc[
        (frame.index >= entry_date) & (frame.index <= evaluation_date), "close"
    ].dropna()
    if marks.empty:
        outcome.update({"status": "missing_price", "entry_price": round(entry_price, 4)})
        return outcome, None
    price = _finite_price(marks.iloc[-1])
    if price is None:
        outcome.update({"status": "missing_price", "entry_price": round(entry_price, 4)})
        return outcome, None
    price_date = pd.Timestamp(marks.index[-1])
    price_return = price / entry_price - 1
    outcome.update({
        "status": "ready",
        "entry_price": round(entry_price, 4),
        "price_date": price_date.date().isoformat(),
        "price": round(price, 4),
        "price_change": round(price - entry_price, 4),
        "return": round(price_return, 6),
    })
    return outcome, price_return


def decision_follow_up(
    snapshot: Mapping[str, Any], price_frames: Mapping[str, pd.DataFrame],
) -> dict[str, Any]:
    """Evaluate a snapshot from T+1 open to the horizon/latest session close.

    The first post-signal session is holding day one.  Once the configured number
    of sessions exists, the result is frozen at that session's close even when the
    local cache already contains later prices.
    """
    raw_picks = [pick for pick in (snapshot.get("picks") or []) if isinstance(pick, Mapping)]
    has_position_weights = any("target_weight" in pick for pick in raw_picks)
    if has_position_weights:
        picks = [
            pick for pick in raw_picks
            if float(pick.get("target_weight") or 0) > 0
        ][:3]
    else:
        picks = raw_picks[:3]
    horizon = _holding_horizon(snapshot)
    signal_date = _signal_date(snapshot)
    if has_position_weights and not picks and snapshot.get("position_state") == "flat":
        return {
            "status": "flat",
            "method": "no_position_validation",
            "horizon_days": horizon,
            "completed_sessions": 0,
            "progress": 1.0,
            "entry_date": None,
            "evaluation_date": None,
            "data_as_of_date": None,
            "average_return": None,
            "available_picks": 0,
            "winner_count": 0,
            "picks": [],
        }
    normalized = {
        str(pick.get("symbol") or ""): _normalize_bars(
            price_frames.get(str(pick.get("symbol") or ""))
        )
        for pick in picks
        if pick.get("symbol")
    }
    completed_sessions, entry_date, evaluation_date, all_close_dates = _follow_up_timeline(
        normalized, signal_date, horizon,
    )
    has_price_data = any(not frame.empty for frame in normalized.values())
    status = _follow_up_status(
        has_picks=bool(picks),
        signal_date=signal_date,
        completed_sessions=completed_sessions,
        horizon=horizon,
        has_price_data=has_price_data,
    )

    outcomes: list[dict[str, Any]] = []
    returns: list[float] = []
    return_weights: list[float] = []
    for position, pick in enumerate(picks, start=1):
        symbol = str(pick.get("symbol") or "")
        frame = normalized.get(symbol)
        outcome, price_return = _pick_outcome(
            pick, position, status, frame, entry_date, evaluation_date,
        )
        if price_return is not None:
            returns.append(price_return)
            return_weights.append(float(pick.get("target_weight") or 1.0))
        outcomes.append(outcome)

    total_weight = sum(return_weights)
    average_return = (
        sum(value * weight for value, weight in zip(returns, return_weights, strict=True))
        / total_weight
        if returns and total_weight > 0 else None
    )
    return {
        "status": status,
        "method": "t_plus_one_open_to_horizon_close" if status == "completed"
        else "t_plus_one_open_to_latest_close",
        "horizon_days": horizon,
        "completed_sessions": completed_sessions,
        "progress": round(completed_sessions / horizon, 4),
        "entry_date": entry_date.date().isoformat() if entry_date is not None else None,
        "evaluation_date": (
            evaluation_date.date().isoformat() if evaluation_date is not None else None
        ),
        "data_as_of_date": (
            max(all_close_dates).date().isoformat() if all_close_dates else None
        ),
        "average_return": round(average_return, 6) if average_return is not None else None,
        "available_picks": len(returns),
        "winner_count": sum(value > 0 for value in returns),
        "picks": outcomes,
    }


def enrich_decision_snapshots(
    snapshots: Sequence[Mapping[str, Any]],
    price_frames: Mapping[str, pd.DataFrame],
) -> list[dict[str, Any]]:
    """Attach derived follow-up data without altering immutable stored payloads."""
    enriched: list[dict[str, Any]] = []
    for snapshot in snapshots:
        value = dict(snapshot)
        value["follow_up_validation"] = decision_follow_up(snapshot, price_frames)
        enriched.append(value)
    return enriched
