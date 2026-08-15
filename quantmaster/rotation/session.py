"""Shared completed-session boundary for rotation providers and orchestration."""

from __future__ import annotations

from datetime import datetime

from quantmaster.trading_sessions import resolve_session_target


def expected_market_session(now: datetime | None = None, *, as_of: str = "") -> str:
    """Return the latest completed session backed by a verified calendar."""
    expectation = resolve_session_target(as_of, now)
    return expectation.session if expectation.ready else ""
