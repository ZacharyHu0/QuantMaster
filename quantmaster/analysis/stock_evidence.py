"""个股研究证据账本、严格序列化与内容寻址。"""

from __future__ import annotations

import hashlib
import json
import math
import threading
from collections.abc import Mapping
from datetime import datetime
from typing import Any

import pandas as pd

DIMENSION_ORDER = (
    "fundamental",
    "technical",
    "news",
    "capital",
    "sentiment",
    "macro",
)
SOURCE_LEVELS = {
    1: "官方/结构化数据",
    2: "AKShare 聚合",
    3: "可信媒体补缺",
}
EVIDENCE_SCHEMA_VERSION = "1.0"


def canonical_json(value: Any) -> str:
    return json.dumps(
        _strict_json_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def content_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _strict_json_value(value: Any, path: str = "payload") -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} 不能包含 NaN 或 Infinity")
        return value
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {
            str(key): _strict_json_value(item, f"{path}.{key}")
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [
            _strict_json_value(item, f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    try:
        converted = value.item()
    except (AttributeError, ValueError):
        return str(value)
    return _strict_json_value(converted, path)


def _date_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        parsed = pd.to_datetime(value, errors="coerce")
    except (TypeError, ValueError, OverflowError):
        parsed = None
    if parsed is not None and not pd.isna(parsed):
        return str(pd.Timestamp(parsed).date())
    return str(value)[:80]


class EvidenceLedger:
    """Build immutable, deterministic evidence IDs and a de-duplicated source table."""

    def __init__(self) -> None:
        self._items: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()

    def add(
        self,
        dimension: str,
        *,
        title: str,
        value: Any,
        source_name: str,
        source_level: int,
        url: str = "",
        published_at: str = "",
        data_as_of: str = "",
        provider: str = "",
        evidence_type: str = "structured",
        excerpt: str = "",
    ) -> dict[str, Any]:
        if dimension not in DIMENSION_ORDER:
            raise ValueError(f"未知研究维度：{dimension}")
        if source_level not in SOURCE_LEVELS:
            raise ValueError("source_level 必须为 1、2 或 3")
        normalized_url = str(url or "").strip()
        if normalized_url and not normalized_url.lower().startswith(("http://", "https://")):
            normalized_url = ""
        body = {
            "schema_version": EVIDENCE_SCHEMA_VERSION,
            "dimension": dimension,
            "type": str(evidence_type or "structured")[:50],
            "title": str(title or "未命名证据")[:300],
            "value": _strict_json_value(value),
            "excerpt": str(excerpt or "")[:1600],
            "source": {
                "name": str(source_name or "未知来源")[:200],
                "level": source_level,
                "level_label": SOURCE_LEVELS[source_level],
                "provider": str(provider or "")[:100],
                "url": normalized_url[:2048],
            },
            "published_at": _date_text(published_at),
            "data_as_of": _date_text(data_as_of),
        }
        if not normalized_url:
            raise ValueError("证据必须提供可核查的 HTTP(S) URL")
        digest = content_hash(body)
        item = {"id": f"ev_{digest[:20]}", **body, "content_hash": digest}
        with self._lock:
            self._items[item["id"]] = item
        return dict(item)

    def extend(self, items: list[dict[str, Any]]) -> None:
        for item in items:
            validated = validate_evidence(item)
            with self._lock:
                self._items[validated["id"]] = validated

    def for_dimension(self, dimension: str) -> list[dict[str, Any]]:
        with self._lock:
            values = [
                dict(item)
                for item in self._items.values()
                if item["dimension"] == dimension
            ]
        return sorted(
            values,
            key=lambda item: (
                int(item["source"]["level"]),
                item.get("published_at", ""),
                item["id"],
            ),
        )

    def all(self) -> list[dict[str, Any]]:
        return [item for key in DIMENSION_ORDER for item in self.for_dimension(key)]

    def sources(self) -> list[dict[str, Any]]:
        values: dict[str, dict[str, Any]] = {}
        for evidence in self.all():
            source = evidence["source"]
            key = content_hash(source)
            row = values.setdefault(
                key,
                {
                    "id": f"src_{key[:16]}",
                    **source,
                    "evidence_ids": [],
                },
            )
            row["evidence_ids"].append(evidence["id"])
        return sorted(values.values(), key=lambda item: (item["level"], item["name"], item["id"]))


def validate_evidence(value: dict[str, Any]) -> dict[str, Any]:
    item = _strict_json_value(value)
    if item.get("schema_version") != EVIDENCE_SCHEMA_VERSION:
        raise ValueError("证据 schema_version 不受支持")
    if item.get("dimension") not in DIMENSION_ORDER:
        raise ValueError("证据 dimension 非法")
    source = item.get("source") or {}
    if source.get("level") not in SOURCE_LEVELS:
        raise ValueError("证据来源层级非法")
    if not str(source.get("url") or "").startswith(("http://", "https://")):
        raise ValueError("证据缺少可核查 URL")
    expected_body = {
        key: item[key]
        for key in (
            "schema_version",
            "dimension",
            "type",
            "title",
            "value",
            "excerpt",
            "source",
            "published_at",
            "data_as_of",
        )
    }
    digest = content_hash(expected_body)
    if item.get("content_hash") != digest or item.get("id") != f"ev_{digest[:20]}":
        raise ValueError("证据内容哈希校验失败")
    return item
