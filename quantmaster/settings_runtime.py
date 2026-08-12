"""Durable settings revision and component-effective projections.

The YAML revision is the persisted source of truth.  This sidecar records only
non-secret apply observations; it never becomes another configuration source.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import yaml

COMPONENTS = (
    "web",
    "server",
    "runtime-worker",
    "automation",
    "lab",
    "free-stockdb",
    "llm",
    "data-clients",
    "scheduler",
)

APPLY_STRATEGIES = {
    "web": "immediate",
    "server": "restart_required",
    "runtime-worker": "immediate",
    "automation": "safe_rebuild",
    "lab": "safe_rebuild",
    "free-stockdb": "safe_rebuild",
    "llm": "safe_rebuild",
    "data-clients": "safe_rebuild",
    "scheduler": "safe_rebuild",
}

_lock = threading.RLock()


def persisted_revision(path: str | Path) -> int:
    try:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    except (FileNotFoundError, OSError, TypeError, ValueError, yaml.YAMLError):
        return 0
    try:
        return max(0, int(raw.get("_revision") or 0)) if isinstance(raw, dict) else 0
    except (TypeError, ValueError):
        return 0


def state_path(config_path: str | Path) -> Path:
    return Path(config_path).with_suffix(".runtime.json")


def _empty(revision: int = 0) -> dict[str, Any]:
    return {
        "version": 1,
        "persisted_revision": revision,
        "latest_generation": 0,
        "updated_at": "",
        "components": {},
    }


def _read(config_path: str | Path) -> dict[str, Any]:
    path = state_path(config_path)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return _empty(persisted_revision(config_path))
    if not isinstance(value, dict):
        return _empty(persisted_revision(config_path))
    value.setdefault("components", {})
    return value


def _atomic_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, raw_temp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp = Path(raw_temp)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def begin_apply(config_path: str | Path, revision: int) -> int:
    """Allocate a monotonic apply generation for one persisted revision."""
    with _lock:
        state = _read(config_path)
        latest = max(0, int(state.get("latest_generation") or 0)) + 1
        state.update(
            persisted_revision=max(int(state.get("persisted_revision") or 0), int(revision)),
            latest_generation=latest,
            updated_at=time.time(),
        )
        for component in COMPONENTS:
            previous = dict((state.get("components") or {}).get(component) or {})
            effective = int(previous.get("effective_revision") or 0)
            state["components"][component] = {
                **previous,
                "component": component,
                "target_revision": int(revision),
                "generation": latest,
                "apply_strategy": APPLY_STRATEGIES[component],
                "status": "pending" if effective < int(revision) else "effective",
                "error": "",
                "diagnostic_code": "",
                "recommendation": "等待组件确认应用",
            }
        _atomic_write(state_path(config_path), state)
        return latest


def report_component(
    config_path: str | Path,
    component: str,
    *,
    revision: int,
    generation: int,
    status: str,
    error: str = "",
    diagnostic_code: str = "",
    recommendation: str = "",
    effective_revision: int | None = None,
) -> bool:
    """Publish a component result, rejecting stale revision/generation reports."""
    if component not in COMPONENTS:
        raise ValueError(f"unknown settings component: {component}")
    with _lock:
        state = _read(config_path)
        current_persisted = persisted_revision(config_path)
        current = dict((state.get("components") or {}).get(component) or {})
        current_generation = int(current.get("generation") or 0)
        current_target = int(current.get("target_revision") or 0)
        if int(revision) < current_persisted:
            return False
        if int(revision) < current_target:
            return False
        if int(revision) == current_target and int(generation) < current_generation:
            return False
        confirmed = status == "effective"
        observed_effective = (
            int(effective_revision)
            if effective_revision is not None
            else int(revision) if status == "effective" else int(current.get("effective_revision") or 0)
        )
        state["persisted_revision"] = max(
            int(state.get("persisted_revision") or 0), current_persisted, int(revision),
        )
        state["latest_generation"] = max(
            int(state.get("latest_generation") or 0), int(generation),
        )
        state.setdefault("components", {})[component] = {
            **current,
            "component": component,
            "target_revision": int(revision),
            "effective_revision": observed_effective,
            "generation": int(generation),
            "apply_strategy": APPLY_STRATEGIES[component],
            "status": str(status),
            "confirmed": confirmed,
            "last_applied_at": time.time(),
            "error": str(error)[:500],
            "diagnostic_code": str(diagnostic_code)[:120],
            "recommendation": str(recommendation)[:500],
        }
        state["updated_at"] = time.time()
        _atomic_write(state_path(config_path), state)
        return True


def public_state(config_path: str | Path, *, worker_available: bool | None = None) -> dict[str, Any]:
    with _lock:
        state = _read(config_path)
    revision = persisted_revision(config_path)
    state["persisted_revision"] = revision
    components: dict[str, Any] = {}
    for name in COMPONENTS:
        item = dict((state.get("components") or {}).get(name) or {})
        effective = int(item.get("effective_revision") or 0)
        target = int(item.get("target_revision") or revision)
        item.update(
            component=name,
            target_revision=target,
            effective_revision=effective,
            generation=int(item.get("generation") or 0),
            apply_strategy=APPLY_STRATEGIES[name],
        )
        if worker_available is False and name not in {"web", "server"} and effective < revision:
            item.update(
                status="unconfirmed",
                confirmed=False,
                recommendation="后台 worker 离线；重连后将读取最新 revision",
            )
        else:
            item.setdefault("status", "effective" if effective == revision else "pending")
            item.setdefault("confirmed", effective == revision)
            item.setdefault("recommendation", "" if effective == revision else "等待组件确认应用")
        item.setdefault("error", "")
        item.setdefault("diagnostic_code", "")
        components[name] = item
    return {
        "persisted_revision": revision,
        "latest_generation": int(state.get("latest_generation") or 0),
        "components": components,
        "drift": sorted(
            name for name, item in components.items()
            if item["effective_revision"] != revision
        ),
    }


def diagnostic_id(prefix: str = "cfg") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"
