"""Strict JSON boundary helpers: RFC-compliant output with no NaN or Infinity."""

from __future__ import annotations

import json
import math
import threading
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

from starlette.responses import JSONResponse

_numeric_lock = threading.Lock()
_numeric_intercepts: Counter[str] = Counter()


def _nonfinite_reason(value: Any) -> str:
    try:
        float(value)
    except (TypeError, ValueError, OverflowError):
        return "parse_failed"
    return "calculation_nonfinite"


def numeric_boundary_diagnostics(*, reset: bool = False) -> dict[str, Any]:
    """Return bounded process-local evidence about non-finite JSON interceptions."""
    with _numeric_lock:
        reasons = dict(sorted(_numeric_intercepts.items()))
        total = sum(reasons.values())
        if reset:
            _numeric_intercepts.clear()
    return {"intercepted": total, "reason_counts": reasons}


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


def sanitize_api_json(value: Any) -> Any:
    """Sanitize an API payload and disclose every intercepted numeric path.

    Mapping responses gain ``numeric_missing`` only when interception happened,
    so ordinary schemas remain unchanged while a non-finite value can never be
    mistaken for an unexplained optional null.
    """
    missing: list[dict[str, str]] = []

    def visit(item: Any, path: str) -> Any:
        if isinstance(item, float) and not math.isfinite(item):
            missing.append({"path": path, "reason": _nonfinite_reason(item)})
            return None
        if isinstance(item, Decimal) and not item.is_finite():
            missing.append({"path": path, "reason": _nonfinite_reason(item)})
            return None
        if isinstance(item, Mapping):
            return {key: visit(child, f"{path}.{key}") for key, child in item.items()}
        if isinstance(item, (list, tuple, set, frozenset)):
            return [visit(child, f"{path}[{index}]") for index, child in enumerate(item)]
        normalized = sanitize_json(item)
        if normalized is not item:
            return visit(normalized, path)
        return normalized

    result = visit(value, "payload")
    if missing:
        counts = Counter(item["reason"] for item in missing)
        with _numeric_lock:
            _numeric_intercepts.update(counts)
        disclosure = {"count": len(missing), "reason_counts": dict(sorted(counts.items())), "items": missing}
        if isinstance(result, dict):
            result = {**result, "numeric_missing": disclosure}
        else:
            result = {"data": result, "numeric_missing": disclosure}
    return result


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
        return strict_json_dumps(sanitize_api_json(content)).encode("utf-8")
