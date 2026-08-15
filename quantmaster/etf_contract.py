"""Transport-neutral ETF identity rules shared by research and rotation."""

from __future__ import annotations

from typing import Any


def is_exchange_etf(instrument: Any) -> bool:
    """Return whether an instrument is a listed SH/SZ exchange-traded fund."""
    if instrument.exchange not in {"SH", "SZ"}:
        return False
    if instrument.status.casefold() not in {"listed", "active", "l"}:
        return False
    text = instrument.name.upper()
    if "LOF" in text or "联接" in text:
        return False
    return instrument.asset_type == "etf" or (
        instrument.asset_type == "fund" and ("ETF" in text or "交易型" in text)
    )
