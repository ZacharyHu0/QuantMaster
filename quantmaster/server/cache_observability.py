"""Read-only presentation contract for cache namespace observability."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

_KNOWN_NAMESPACES = (
    ("stockdb.raw", "StockDB 原始证据"),
    ("provider.raw", "Provider 原始响应"),
    ("provider.normalized", "Provider 标准化结果"),
    ("news.raw", "资讯原文"),
    ("news.normalized", "资讯标准化"),
    ("market.bars", "行情"),
    ("industry.catalog", "行业目录"),
    ("instrument.catalog", "证券目录"),
    ("model.catalog", "模型目录"),
    ("capability.probe", "能力检测"),
    ("lab.panel", "研究面板"),
)
_COUNT_KEYS = ("fresh", "stale", "partial", "negative")


def _integer(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _text(value: Any, limit: int = 300) -> str:
    return str(value or "")[:limit]


def _items(value: Any, *, limit: int = 20) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple)):
        return []
    return [dict(item) for item in value[:limit] if isinstance(item, Mapping)]


def _source_snapshot() -> Any:
    try:
        from quantmaster.data.cache_contracts import cache_registry

        return cache_registry.snapshot()
    except (ImportError, AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return None


def _namespace_values(snapshot: Any) -> tuple[list[Mapping[str, Any]], str]:
    if snapshot is None:
        return [], ""
    if isinstance(snapshot, Mapping):
        revision = _text(snapshot.get("config_revision"), 80)
        values = snapshot.get("namespaces", snapshot)
        if isinstance(values, Mapping):
            return [
                {"namespace": name, **dict(item)}
                for name, item in values.items()
                if name not in {"config_revision", "checked_at", "summary"}
                and isinstance(item, Mapping)
            ], revision
        if isinstance(values, (list, tuple)):
            return [item for item in values if isinstance(item, Mapping)], revision
    if isinstance(snapshot, (list, tuple)):
        return [item for item in snapshot if isinstance(item, Mapping)], ""
    return [], ""


def _normalize_namespace(value: Mapping[str, Any], *, default_revision: str = "") -> dict[str, Any]:
    namespace = _text(value.get("namespace") or value.get("name"), 100)
    counts_value = value.get("counts") if isinstance(value.get("counts"), Mapping) else value
    counts = {key: _integer(counts_value.get(key)) for key in _COUNT_KEYS}
    hits, misses = _integer(value.get("hits")), _integer(value.get("misses"))
    requests = hits + misses
    refresh_value = value.get("refresh") if isinstance(value.get("refresh"), Mapping) else (
        value.get("pending") if isinstance(value.get("pending"), Mapping) else {}
    )
    completed = _integer(refresh_value.get("completed", value.get("completed")))
    total = _integer(refresh_value.get("total", value.get("total")))
    pending = _integer(refresh_value.get("pending")) if "pending" in refresh_value else max(
        0, total - completed,
    )
    negatives = []
    negative_values = value.get("negatives")
    for item in _items(negative_values):
        negatives.append({
            "reason": _text(item.get("negative_reason") or item.get("reason"), 160),
            "count": _integer(item.get("count") or 1),
            "source": _text(item.get("source"), 100),
            "observed_at": _text(item.get("observed_at"), 80),
            "expires_at": _text(item.get("expires_at"), 80),
        })
    negative_reasons = value.get("negative_reasons")
    if isinstance(negative_reasons, Mapping):
        negatives.extend({
            "reason": _text(reason, 160),
            "count": _integer(count),
            "source": "",
            "observed_at": "",
            "expires_at": "",
        } for reason, count in list(negative_reasons.items())[:20])
    issues = []
    issue_values = value.get("issues")
    if isinstance(issue_values, (list, tuple)):
        for item in issue_values[:20]:
            if isinstance(item, Mapping):
                issues.append({
                    "code": _text(item.get("diagnostic_code") or item.get("code"), 100),
                    "message": _text(item.get("message") or item.get("detail"), 300),
                })
            elif item:
                issues.append({"code": _text(item, 100), "message": ""})
    stale_consumers = value.get("stale_consumers", value.get("stale_pages", []))
    if not isinstance(stale_consumers, (list, tuple)):
        stale_consumers = []
    diagnostic_code = _text(value.get("diagnostic_code"), 100)
    observed = bool(value.get("observed", diagnostic_code != "CACHE_NAMESPACE_UNOBSERVED"))
    return {
        "namespace": namespace,
        "label": _text(value.get("label") or namespace, 100),
        "observed": observed,
        "status": _text(value.get("status") or ("observed" if observed else "unavailable"), 40),
        "diagnostic_code": diagnostic_code,
        "value_kind": _text(value.get("value_kind"), 160),
        "freshness_rule": _text(value.get("freshness_rule"), 500),
        "dependencies": [
            _text(item, 100) for item in value.get("dependencies", [])[:20]
        ] if isinstance(value.get("dependencies"), (list, tuple)) else [],
        "hits": hits,
        "misses": misses,
        "hit_rate": round(hits / requests, 4) if requests else None,
        "counts": counts,
        "oldest_at": _text(value.get("oldest_at") or value.get("oldest"), 80),
        "newest_at": _text(value.get("newest_at") or value.get("newest"), 80),
        "refresh": {"completed": completed, "total": total, "pending": pending},
        "negatives": negatives,
        "stale_consumers": [_text(item, 120) for item in stale_consumers[:20]],
        "provider_revalidation_pending": (
            _integer(value.get("provider_revalidation_pending", value.get("revalidation_pending")))
            if "provider_revalidation_pending" in value or "revalidation_pending" in value
            else None
        ),
        "config_revision": _text(value.get("config_revision") or default_revision, 80),
        "parser_revision": _text(value.get("parser_revision") or value.get("parser_version"), 80),
        "issues": issues,
    }


def collect_cache_observability(snapshot: Any = None) -> dict[str, Any]:
    """Return a bounded, non-secret cache snapshot without touching cache values."""

    source = _source_snapshot() if snapshot is None else snapshot
    values, config_revision = _namespace_values(source)
    namespaces = [
        _normalize_namespace(value, default_revision=config_revision)
        for value in values
        if _text(value.get("namespace") or value.get("name"), 100)
    ]
    existing = {item["namespace"] for item in namespaces}
    for namespace, label in _KNOWN_NAMESPACES:
        if namespace not in existing:
            namespaces.append(_normalize_namespace({
                "namespace": namespace,
                "label": label,
                "observed": False,
                "status": "unavailable",
                "diagnostic_code": "CACHE_NAMESPACE_UNOBSERVED",
                "issues": [{
                    "diagnostic_code": "CACHE_NAMESPACE_UNOBSERVED",
                    "message": "该 namespace 尚未发布运行期观测。",
                }],
            }, default_revision=config_revision))
    namespaces.sort(key=lambda item: (not item["observed"], item["label"]))
    observed = [item for item in namespaces if item["observed"]]
    hits = sum(item["hits"] for item in observed)
    misses = sum(item["misses"] for item in observed)
    requests = hits + misses
    from quantmaster.runtime.json import numeric_boundary_diagnostics

    numeric = numeric_boundary_diagnostics()
    return {
        "checked_at": datetime.now(UTC).isoformat(),
        "summary": {
            "namespace_count": len(namespaces),
            "observed_count": len(observed),
            "hit_rate": round(hits / requests, 4) if requests else None,
            "fresh": sum(item["counts"]["fresh"] for item in observed),
            "stale": sum(item["counts"]["stale"] for item in observed),
            "partial": sum(item["counts"]["partial"] for item in observed),
            "negative": sum(item["counts"]["negative"] for item in observed),
            "pending": sum(item["refresh"]["pending"] for item in observed),
            "provider_revalidation_pending": sum(
                item["provider_revalidation_pending"] or 0 for item in observed
            ),
            "numeric_intercepted": numeric["intercepted"],
        },
        "numeric_boundary": numeric,
        "namespaces": namespaces,
    }
