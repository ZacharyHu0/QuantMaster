"""Transport-neutral operation problem contract shared by workers and APIs."""

from __future__ import annotations

from typing import Any

from quantmaster.logging_config import redact_sensitive_text, redact_sensitive_value

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
    field: str | None = None,
    retryable: bool | None = None,
    retry_after: int | None = None,
    suggestion: str | None = None,
    problem_id: str | None = None,
    items: list[object] | None = None,
    event: str = "operation_problem",
    component: str | None = None,
    diagnostic_id: str | None = None,
    operation_id: str | None = None,
    item_type: str | None = None,
    item_id: str | None = None,
    attempt: int | None = None,
    next_retry_at: str | None = None,
    impact: str | None = None,
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
        # These names form the small HTTP-facing problem contract as well as
        # being useful to background workers.  Keep the recovery text in one
        # place so callers cannot accidentally expose an exception as a hint.
        "retryable": bool(can_continue) if retryable is None else bool(retryable),
        "suggestion": _clean(suggestion if suggestion is not None else action),
        "event": _clean(event, 80),
        "component": _clean(component or source, 80),
    }
    if field:
        problem["field"] = _clean(field, 160)
    if retry_after is not None:
        problem["retry_after"] = max(0, int(retry_after))
    if items:
        problem["items"] = [_clean(item, 100) for item in items[:20]]
    structured = {
        "diagnostic_id": diagnostic_id, "operation_id": operation_id,
        "item_type": item_type, "item_id": item_id, "attempt": attempt,
        "next_retry_at": next_retry_at, "impact": impact,
    }
    for key, value in {**structured, **context}.items():
        if value is not None:
            problem[key] = redact_sensitive_value(value, key=key)
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
        retryable = bool(self.problem.get("retryable", self.status_code in {429, 502, 503}))
        result: dict[str, Any] = {
            "detail": self.problem["message"],
            "problem": self.problem,
            "error_id": error_id,
            "request_id": error_id,
            "diagnostic_id": error_id,
            "code": self.problem["code"],
            "message": self.problem["message"],
            "retryable": retryable,
            "suggestion": self.problem.get("suggestion", self.problem.get("action", "")),
        }
        if self.problem.get("field"):
            result["field"] = self.problem["field"]
        if self.problem.get("retry_after") is not None:
            result["retry_after"] = self.problem["retry_after"]
        if self.data_quality is not None:
            result["data_quality"] = self.data_quality
        return result
