"""Strict JSON boundary helpers: RFC-compliant output with no NaN or Infinity."""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

from starlette.responses import JSONResponse


def sanitize_json(value: Any) -> Any:
    """Recursively replace non-finite floats and normalize common value objects."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        number = float(value) if value.is_finite() else math.nan
        return number if math.isfinite(number) else None
    if isinstance(value, Enum):
        return sanitize_json(value.value)
    if isinstance(value, Mapping):
        return {key: sanitize_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [sanitize_json(item) for item in value]
    if is_dataclass(value) and not isinstance(value, type):
        return sanitize_json(asdict(value))
    if isinstance(value, (date, datetime, Path)):
        return str(value)
    if hasattr(value, "model_dump"):
        return sanitize_json(value.model_dump(mode="json"))
    if hasattr(value, "item"):
        try:
            item = value.item()
        except (TypeError, ValueError):
            pass
        else:
            if item is not value:
                return sanitize_json(item)
    return value


def strict_json_dumps(
    value: Any,
    *,
    indent: int | None = None,
    sort_keys: bool = False,
    default: Callable[[Any], Any] | None = None,
) -> str:
    options: dict[str, Any] = {
        "ensure_ascii": False,
        "allow_nan": False,
        "indent": indent,
        "sort_keys": sort_keys,
        "separators": None if indent is not None else (",", ":"),
    }
    if default is not None:
        options["default"] = default
    return json.dumps(sanitize_json(value), **options)


class StrictJSONResponse(JSONResponse):
    """FastAPI response class that never emits JavaScript-only numeric tokens."""

    def render(self, content: Any) -> bytes:
        return strict_json_dumps(content).encode("utf-8")
