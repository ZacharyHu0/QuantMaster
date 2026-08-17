"""Provider callbacks for the trading-session foundation.

The calendar resolver owns session semantics; data and research adapters register
their evidence readers here so the foundation never imports a domain provider.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path

from quantmaster.runtime.sqlite import connect_sqlite

_official_source: Callable[[date, date], list[str]] | None = None


def register_official_calendar(source: Callable[[date, date], list[str]]) -> None:
    global _official_source
    _official_source = source


def official_calendar(start: date, end: date) -> list[str]:
    return [] if _official_source is None else list(_official_source(start, end))


def research_calendar(root: Path, start: date, end: date) -> list[str]:
    path = root / "research_lake" / "_meta" / "catalog.sqlite"
    if not path.is_file():
        return []
    with connect_sqlite(path) as connection:
        rows = connection.execute(
            "SELECT DISTINCT trade_date FROM research_partitions "
            "WHERE kind=? AND asset_class=? AND frequency=? "
            "AND trade_date>=? AND trade_date<=? ORDER BY trade_date",
            ("raw", "stock", "1d", start.isoformat(), end.isoformat()),
        ).fetchall()
    return [str(row[0]) for row in rows]
