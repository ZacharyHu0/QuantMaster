"""Local staged-update status and external activation-job boundary."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from quantmaster.release.history import release_lookup, release_sections
from quantmaster.runtime.activation import (
    FULL_SHA,
    ActivationBlocked,
    Candidate,
    SlotRegistry,
    installed_app_root,
    lifecycle_lock,
)

OPERATION_SCHEMA = 1
OPERATION_FILE = ".activation-operation.json"
_UPDATE_FAILURES = (ActivationBlocked, OSError, subprocess.SubprocessError, ValueError, TypeError)
_STAGED_AT_FALLBACK = datetime.min.replace(tzinfo=UTC)


def operation_path(app_root: str | Path | None = None) -> Path:
    root = Path(app_root).resolve() if app_root is not None else installed_app_root()
    return root / OPERATION_FILE


def _write_json(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(dict(value), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_json(path: Path) -> dict[str, object] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return dict(value) if isinstance(value, dict) else None


def _identity(registry: SlotRegistry, build_sha: str, role: str) -> dict[str, str]:
    meta = _slot_metadata(registry, build_sha)
    if meta and str(meta.get("version") or ""):
        version = str(meta.get("version") or "")
        date = str(meta.get("release_date") or "")
    else:
        release = release_lookup(build_sha)
        version = release.get("version", "")
        date = release.get("release_date", "")
    return {
        "build_sha": build_sha,
        "slot_id": build_sha,
        "role": role,
        "version": version,
        "release_date": date,
    }


def _blocker(exc: ActivationBlocked, *, build_sha: str = "") -> dict[str, str]:
    result = {"code": exc.code, "message": exc.detail}
    if build_sha:
        result["build_sha"] = build_sha
    return result


def _slot_metadata(registry: SlotRegistry, build_sha: str) -> dict[str, object] | None:
    """Read stable, path-free version metadata written next to a slot's marker.

    The file is written at staging time (immutable) and never exposes local paths
    on the public update surface.
    """

    try:
        marker = registry.slot(build_sha) / "slot_meta.json"
    except ActivationBlocked:
        return None
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, Mapping):
        return None
    meta_version = str(payload.get("version") or "")
    meta_date = str(payload.get("release_date") or "")
    meta_title = str(payload.get("title") or "").strip()
    if meta_version or meta_date or meta_title:
        return {"version": meta_version, "release_date": meta_date, "title": meta_title}
    return None


def _candidate_release_metadata(registry: SlotRegistry, build_sha: str) -> dict[str, object]:
    """Version/date/changelog for one slot: staging meta, then the release registry."""

    meta = _slot_metadata(registry, build_sha)
    if meta and str(meta.get("version") or ""):
        pass
    else:
        meta = release_lookup(build_sha)
    sections = release_sections(build_sha)
    return {
        "version": str(meta.get("version") or ""),
        "release_date": str(meta.get("release_date") or ""),
        "title": str(meta.get("title") or ""),
        "changelog": sections[:3],
    }


def _pointer_blocker(registry: SlotRegistry, active: str) -> dict[str, str] | None:
    """Detect a torn or tampered active/launcher pointer pair."""

    try:
        lines = registry.launcher_target.read_text(encoding="ascii").splitlines()
    except FileNotFoundError:
        return None if not active else {
            "code": "launcher_target_missing",
            "message": "active registry 缺少稳定 launcher target",
        }
    except (OSError, UnicodeError):
        return {"code": "launcher_target_unreadable", "message": "稳定 launcher target 不可读"}
    if len(lines) != 1 or FULL_SHA.fullmatch(lines[0]) is None:
        return {"code": "launcher_target_invalid", "message": "稳定 launcher target 不是完整 lowercase SHA"}
    if lines[0] != active:
        return {
            "code": "activation_pointer_mismatch",
            "message": "active registry 与稳定 launcher target 不一致",
        }
    return None


def _local_main_marker_blocker(registry: SlotRegistry, build_sha: str) -> dict[str, str] | None:
    marker = registry.slot(build_sha) / ".quantmaster-stage.json"
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {
            "code": "candidate_unstaged",
            "message": "候选槽缺少可验证的 local-main staging marker",
            "build_sha": build_sha,
        }
    if not isinstance(payload, Mapping):
        return {
            "code": "candidate_invalid",
            "message": "staging marker 不是结构化 object",
            "build_sha": build_sha,
        }
    source_task = payload.get("source_task")
    source_commit = payload.get("source_task_commit")
    size = payload.get("size")
    smoke = payload.get("smoke")
    complete = (
        isinstance(source_task, str) and bool(source_task.strip())
        and isinstance(source_commit, str) and FULL_SHA.fullmatch(source_commit) is not None
        and isinstance(size, Mapping) and size.get("mode") == "onedir-measurement"
        and size.get("build_sha") == build_sha
        and size.get("within_hard_limits") is True
        and not size.get("errors") and not size.get("limit_failures")
        and isinstance(smoke, Mapping) and smoke.get("layout") == "onedir"
        and smoke.get("build_sha") == build_sha and smoke.get("slot_id") == build_sha
    )
    if complete:
        return None
    return {
        "code": "local_main_evidence_required",
        "message": "候选槽缺少完整 local-main package/smoke evidence",
        "build_sha": build_sha,
    }


def _candidate(registry: SlotRegistry, build_sha: str, *, active: str, previous: str) -> dict[str, object]:
    blockers: list[dict[str, str]] = []
    try:
        registry.validate_candidate(build_sha)
    except ActivationBlocked as exc:
        blockers.append(_blocker(exc, build_sha=build_sha))
    if not blockers:
        local_main_blocker = _local_main_marker_blocker(registry, build_sha)
        if local_main_blocker is not None:
            blockers.append(local_main_blocker)
    eligible = not blockers and build_sha != active
    metadata = _candidate_release_metadata(registry, build_sha)
    return {
        "build_sha": build_sha,
        "slot_id": build_sha,
        "eligible": eligible,
        "current": build_sha == active,
        "previous": build_sha == previous,
        "blockers": blockers,
        "version": metadata["version"],
        "release_date": metadata["release_date"],
        "title": metadata["title"],
        "changelog": metadata["changelog"],
    }


def _staged_at(registry: SlotRegistry, build_sha: str) -> datetime:
    """Read a slot's immutable UTC staging time, with a deterministic low fallback."""

    try:
        marker = registry.slot(build_sha) / ".quantmaster-stage.json"
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (ActivationBlocked, OSError, UnicodeError, json.JSONDecodeError):
        return _STAGED_AT_FALLBACK
    if not isinstance(payload, Mapping) or (
        payload.get("build_sha") != build_sha or payload.get("slot_id") != build_sha
    ):
        return _STAGED_AT_FALLBACK
    value = payload.get("staged_at")
    if not isinstance(value, str):
        return _STAGED_AT_FALLBACK
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
        return parsed.astimezone(UTC) if parsed.tzinfo is not None else _STAGED_AT_FALLBACK
    except (OverflowError, ValueError):
        return _STAGED_AT_FALLBACK


def update_status(app_root: str | Path | None = None) -> dict[str, object]:
    """Return a local-only, path-free snapshot of activation eligibility."""

    try:
        root = Path(app_root).resolve() if app_root is not None else installed_app_root()
        registry = SlotRegistry(root)
    except ActivationBlocked as exc:
        return {
            "status": "blocked",
            "active": None,
            "previous": None,
            "pending": None,
            "staged": [],
            "eligibility": {
                "status": "blocked", "eligible_sha": "", "eligible_count": 0,
                "reasons": [_blocker(exc)],
            },
            "blockers": [_blocker(exc)],
            "operation": None,
        }
    try:
        state = registry.read()
    except ActivationBlocked as exc:
        return {
            "status": "blocked",
            "active": None,
            "previous": None,
            "pending": None,
            "staged": [],
            "eligibility": {
                "status": "blocked", "eligible_sha": "", "eligible_count": 0,
                "reasons": [_blocker(exc)],
            },
            "blockers": [_blocker(exc)],
            "operation": _read_json(operation_path(registry.app_root)),
        }

    active = str(state.get("active") or "")
    previous = str(state.get("previous") or "")
    pending = str(state.get("pending") or "")
    staged_with_time: list[tuple[datetime, dict[str, object]]] = []
    blockers: list[dict[str, str]] = []
    pointer_blocker = _pointer_blocker(registry, active)
    if pointer_blocker is not None:
        blockers.append(pointer_blocker)
    if registry.slots.is_dir() and not registry.slots.is_symlink():
        for path in sorted(registry.slots.iterdir(), key=lambda item: item.name):
            if not path.is_dir() or path.is_symlink() or FULL_SHA.fullmatch(path.name) is None:
                continue
            item = _candidate(registry, path.name, active=active, previous=previous)
            staged_with_time.append((_staged_at(registry, path.name), item))
            item_blockers = item.get("blockers")
            if isinstance(item_blockers, list):
                blockers.extend(
                    blocker for blocker in item_blockers
                    if isinstance(blocker, dict) and all(isinstance(key, str) for key in blocker)
                )
    staged = [item for _, item in sorted(staged_with_time, key=lambda entry: entry[0], reverse=True)]
    eligible = [] if pointer_blocker is not None else [
        str(item["build_sha"]) for item in staged if item.get("eligible") is True
    ]
    if state.get("status") == "blocked" and state.get("last_error"):
        blockers.append({"code": "activation_blocked", "message": str(state["last_error"])})
    eligibility_status = "eligible" if eligible else "blocked" if blockers else "none_staged"
    return {
        "status": str(state.get("status") or "empty"),
        "active": _identity(registry, active, "active") if active else None,
        "previous": _identity(registry, previous, "previous") if previous else None,
        "pending": _identity(registry, pending, "pending") if pending else None,
        "active_build_sha": active,
        "previous_build_sha": previous,
        "staged": staged,
        "eligibility": {
            "status": eligibility_status,
            "eligible_sha": eligible[0] if eligible else "",
            "eligible_count": len(eligible),
            "reasons": blockers,
        },
        "blockers": blockers,
        "operation": _read_json(operation_path(registry.app_root)),
    }


def _root_pid() -> int | None:
    raw = os.environ.get("QM_WINDOWS_APP_JOB_ROOT", "").strip()
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def _helper_command(candidate: Candidate, root_pid: int | None) -> list[str]:
    if getattr(sys, "frozen", False):
        command = [str(candidate.slot / "QuantMaster.exe"), "activate", candidate.build_sha]
    else:
        command = [
            sys.executable, "-m", "quantmaster.server.cli", "activate", candidate.build_sha,
        ]
    if root_pid is not None:
        command.extend(("--root-pid", str(root_pid)))
    return command


def _creation_options() -> dict[str, object]:
    if os.name != "nt":
        return {"start_new_session": True}
    flags = int(getattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0))
    flags |= int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
    return {"creationflags": flags}


def start_activation(build_sha: str, app_root: str | Path | None = None) -> dict[str, object]:
    """Validate a local candidate and launch #61's helper outside the Web request."""

    root = Path(app_root).resolve() if app_root is not None else installed_app_root()
    registry = SlotRegistry(root)
    with lifecycle_lock(root):
        state = registry.read()
        pointer_blocker = _pointer_blocker(registry, str(state.get("active") or ""))
        if pointer_blocker is not None:
            raise ActivationBlocked(pointer_blocker["code"], pointer_blocker["message"])
        candidate = registry.validate_candidate(build_sha)
        local_main_blocker = _local_main_marker_blocker(registry, candidate.build_sha)
        if local_main_blocker is not None:
            raise ActivationBlocked(
                local_main_blocker["code"], local_main_blocker["message"], build_sha=candidate.build_sha,
            )
        if build_sha == state.get("active"):
            result: dict[str, object] = {
                "status": "already_active", "build_sha": build_sha, "slot_id": build_sha,
            }
            _write_json(operation_path(root), {
                "schema": OPERATION_SCHEMA, "status": "already_active",
                "operation_id": "", "requested_build_sha": build_sha,
                "updated_at": time.time(), "result": result,
            })
            return result
        previous = _read_json(operation_path(root))
        if previous and str(previous.get("status") or "") in {"accepted", "running"}:
            raise ActivationBlocked("activation_in_progress", "已有 activation 操作正在进行")
        operation_id = uuid.uuid4().hex
        result_path = operation_path(root)
        _write_json(result_path, {
            "schema": OPERATION_SCHEMA,
            "status": "accepted",
            "phase": "starting",
            "operation_id": operation_id,
            "requested_build_sha": candidate.build_sha,
            "started_at": time.time(),
            "updated_at": time.time(),
            "result": None,
            "blockers": [],
        })
        environment = os.environ.copy()
        environment.update({
            "QM_ACTIVATION_OPERATION_ID": operation_id,
            "QM_ACTIVATION_RESULT_PATH": str(result_path),
        })
        try:
            command = _helper_command(candidate, _root_pid())
            if os.name == "nt":
                process = subprocess.Popen(
                    command,
                    cwd=str(root),
                    env=environment,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    creationflags=int(cast(int, _creation_options()["creationflags"])),
                )
            else:
                process = subprocess.Popen(
                    command,
                    cwd=str(root),
                    env=environment,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True,
                )
        except _UPDATE_FAILURES as exc:
            blocker = _blocker(ActivationBlocked("helper_start_failed", "无法启动 activation helper"))
            _write_json(result_path, {
                "schema": OPERATION_SCHEMA, "status": "blocked", "phase": "failed",
                "operation_id": operation_id, "requested_build_sha": candidate.build_sha,
                "updated_at": time.time(), "result": None, "blockers": [blocker],
            })
            raise ActivationBlocked("helper_start_failed", "无法启动 activation helper") from exc
    return {
        "status": "accepted",
        "operation_id": operation_id,
        "requested_build_sha": candidate.build_sha,
        "progress": "starting",
        "helper_pid": int(process.pid),
    }


def mark_activation_running() -> None:
    """Move an accepted helper operation into its durable running phase."""

    raw_path = os.environ.get("QM_ACTIVATION_RESULT_PATH", "").strip()
    operation_id = os.environ.get("QM_ACTIVATION_OPERATION_ID", "").strip()
    if not raw_path or not operation_id:
        return
    path = Path(raw_path).resolve()
    current = _read_json(path)
    if not current or str(current.get("operation_id") or "") != operation_id:
        return
    _write_json(path, {
        **current,
        "status": "running",
        "phase": "stopping_current",
        "updated_at": time.time(),
    })


def write_activation_result(result: Mapping[str, object]) -> None:
    """Persist helper completion so the replacement Web process can report it."""

    raw_path = os.environ.get("QM_ACTIVATION_RESULT_PATH", "").strip()
    if not raw_path:
        return
    path = Path(raw_path).resolve()
    current = _read_json(path) or {}
    status = str(result.get("status") or "blocked")
    blockers: list[object] = []
    if status == "blocked":
        blockers.append({
            "code": str(result.get("code") or "activation_blocked"),
            "message": str(result.get("detail") or "activation 未完成"),
        })
    _write_json(path, {
        **current,
        "schema": OPERATION_SCHEMA,
        "status": status,
        "phase": "complete" if status != "blocked" else "failed",
        "updated_at": time.time(),
        "result": dict(result),
        "blockers": blockers,
    })
