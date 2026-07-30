"""Transport-neutral operation problem contract shared by workers and APIs."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from quantmaster.logging_config import redact_sensitive_text

Problem = dict[str, Any]


def _clean(value: object, limit: int = 300) -> str:
    text = redact_sensitive_text(str(value or "").strip())
    return text if len(text) <= limit else f"{text[: limit - 1]}…"


def make_problem(
    code: str,
    *,
    severity: str = "error",
    source: str = "本地服务",
    title: str,
    message: str,
    action: str,
    blocking: bool = False,
    can_continue: bool = False,
    problem_id: str | None = None,
    items: list[object] | None = None,
    **context: object,
) -> Problem:
    """Build a stable, redacted, directly displayable problem document."""
    safe_severity = severity if severity in {"info", "warning", "error"} else "error"
    problem: Problem = {
        "id": problem_id or f"{source}:{code}",
        "code": _clean(code, 80),
        "severity": safe_severity,
        "source": _clean(source, 60),
        "title": _clean(title, 120),
        "message": _clean(message),
        "action": _clean(action),
        "blocking": bool(blocking),
        "can_continue": bool(can_continue),
    }
    if items:
        problem["items"] = [_clean(item, 100) for item in items[:20]]
    for key, value in context.items():
        if value is not None:
            problem[key] = value
    revision_payload = {
        key: value for key, value in problem.items()
        if key not in {"revision", "checked_at"}
    }
    problem["revision"] = hashlib.sha256(
        json.dumps(revision_payload, ensure_ascii=False, sort_keys=True, default=str).encode()
    ).hexdigest()[:12]
    return problem


class OperationProblem(Exception):
    """Business failure carrying status and recovery semantics, without HTTP imports."""

    def __init__(
        self,
        status_code: int,
        problem: Problem,
        *,
        data_quality: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(problem.get("message") or problem.get("title") or "操作未完成")
        self.status_code = status_code
        self.problem = problem
        self.data_quality = data_quality

    def response(self, error_id: str) -> dict[str, Any]:
        result: dict[str, Any] = {
            "detail": self.problem["message"],
            "problem": self.problem,
            "error_id": error_id,
        }
        if self.data_quality is not None:
            result["data_quality"] = self.data_quality
        return result
