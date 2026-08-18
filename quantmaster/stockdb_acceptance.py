"""Fail-closed acceptance contract for the user-managed local StockDB."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class StockDBSessionAcceptance:
    session: str
    complete: bool
    updated_at: datetime
    validation: dict[str, Any]


def read_stockdb_session_acceptance(
    root: str | Path,
) -> StockDBSessionAcceptance | None:
    """Read a self-consistent v2 marker without reinterpreting older formats."""

    try:
        marker = json.loads(
            (Path(root) / ".quantmaster-update.json").read_text(encoding="utf-8")
        )
        validation = marker.get("validation")
        session = date.fromisoformat(str(marker.get("validated_session") or ""))
        updated_at = datetime.fromisoformat(
            str(marker.get("updated_at") or "").replace("Z", "+00:00")
        )
    except (AttributeError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return None
    value = session.isoformat()
    if (
        marker.get("schema_version") != 2
        or not isinstance(validation, dict)
        or validation.get("accepted") is not True
        or str(marker.get("target_session") or "") != value
        or str(validation.get("target_session") or "") != value
        or str(validation.get("actual_session") or "") != value
        or updated_at.tzinfo is None
    ):
        return None
    return StockDBSessionAcceptance(
        value,
        validation.get("complete") is True,
        updated_at,
        dict(validation),
    )
