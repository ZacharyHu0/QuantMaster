"""Stable launcher and local update surface contracts."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from quantmaster.runtime.activation import ActivationBlocked
from quantmaster.runtime.launcher import read_launcher_target
from quantmaster.runtime.update import (
    OPERATION_FILE,
    start_activation,
    update_status,
)

SHA_A = "a" * 40
SHA_B = "b" * 40
SHA_C = "c" * 40
SHA_D = "d" * 40


def _candidate(
    root: Path, sha: str, *, complete: bool = True, staged_at: str | None = None,
) -> None:
    slot = root / "slots" / sha
    slot.mkdir(parents=True, exist_ok=True)
    (slot / "QuantMaster.exe").write_bytes(b"candidate")
    marker = {
        "schema": 1,
        "status": "staged",
        "build_sha": sha,
        "slot_id": sha,
        "source_task": "stable-app-launcher",
        "source_task_commit": "c" * 40,
        "size": {
            "mode": "onedir-measurement" if complete else "onefile",
            "build_sha": sha,
            "within_hard_limits": True,
            "errors": [],
            "limit_failures": [],
        },
        "smoke": {
            "layout": "onedir" if complete else "onefile",
            "build_sha": sha,
            "slot_id": sha,
        },
    }
    if staged_at is not None:
        marker["staged_at"] = staged_at
    (slot / ".quantmaster-stage.json").write_text(
        json.dumps(marker), encoding="utf-8",
    )


def _state(root: Path, *, active: str, previous: str = "") -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "active.json").write_text(json.dumps({
        "schema": 1,
        "active": active,
        "previous": previous,
        "pending": "",
        "status": "stable",
        "last_error": "",
    }), encoding="utf-8")
    (root / "launcher.target").write_text(f"{active}\n", encoding="ascii")


def test_update_status_reports_exact_identity_and_local_main_eligibility(tmp_path):
    _candidate(tmp_path, SHA_A)
    _candidate(tmp_path, SHA_B)
    _state(tmp_path, active=SHA_A)

    status = update_status(tmp_path)

    assert status["active"] == {"build_sha": SHA_A, "slot_id": SHA_A, "role": "active",
                                "version": "", "release_date": ""}
    assert status["previous"] is None
    assert status["pending"] is None
    assert status["eligibility"] == {
        "status": "eligible", "eligible_sha": SHA_B, "eligible_count": 1, "reasons": [],
    }
    assert [item["build_sha"] for item in status["staged"]] == [SHA_A, SHA_B]
    assert all("path" not in item for item in status["staged"])
    assert all("version" in item and "release_date" in item and "title" in item and "changelog" in item
               for item in status["staged"])
    assert all("path" not in str(item) for item in status["staged"])


def test_update_status_orders_staged_slots_by_time_and_marks_current_and_previous(tmp_path):
    _candidate(tmp_path, SHA_A, staged_at="2026-08-16T08:00:00+00:00")
    _candidate(tmp_path, SHA_B, staged_at="2026-08-16T10:00:00+00:00")
    _candidate(tmp_path, SHA_C, staged_at="not-a-time")
    _candidate(tmp_path, SHA_D)
    _state(tmp_path, active=SHA_A, previous=SHA_C)

    status = update_status(tmp_path)

    assert [item["build_sha"] for item in status["staged"]] == [SHA_B, SHA_A, SHA_C, SHA_D]
    by_sha = {item["build_sha"]: item for item in status["staged"]}
    assert by_sha[SHA_A]["current"] is True and by_sha[SHA_A]["previous"] is False
    assert by_sha[SHA_C]["current"] is False and by_sha[SHA_C]["previous"] is True
    assert status["eligibility"]["eligible_sha"] == SHA_B


def test_update_status_fails_closed_on_pointer_mismatch(tmp_path):
    _candidate(tmp_path, SHA_A)
    _candidate(tmp_path, SHA_B)
    _state(tmp_path, active=SHA_A)
    (tmp_path / "launcher.target").write_text(f"{SHA_B}\n", encoding="ascii")

    status = update_status(tmp_path)

    assert status["eligibility"]["status"] == "blocked"
    assert any(item["code"] == "activation_pointer_mismatch" for item in status["blockers"])


def test_start_activation_accepts_only_complete_local_main_and_persists_operation(
    tmp_path, monkeypatch,
):
    _candidate(tmp_path, SHA_A)
    _candidate(tmp_path, SHA_B, complete=False)
    _state(tmp_path, active=SHA_A)

    with pytest.raises(ActivationBlocked, match="local-main"):
        start_activation(SHA_B, tmp_path)

    _candidate(tmp_path, SHA_B)
    calls = []

    class Process:
        pid = 4321

    monkeypatch.setattr(
        "quantmaster.runtime.update.subprocess.Popen",
        lambda command, **kwargs: calls.append((command, kwargs)) or Process(),
    )
    result = start_activation(SHA_B, tmp_path)

    assert result["status"] == "accepted"
    assert result["requested_build_sha"] == SHA_B
    assert calls[0][0][-2:] == ["activate", SHA_B]
    operation = json.loads((tmp_path / OPERATION_FILE).read_text(encoding="utf-8"))
    assert operation["status"] == "accepted"
    assert "tmp_path" not in json.dumps(operation)


def test_frozen_activation_helper_runs_from_verified_candidate_slot(tmp_path, monkeypatch):
    import quantmaster.runtime.update as update

    _candidate(tmp_path, SHA_A)
    _candidate(tmp_path, SHA_B)
    _state(tmp_path, active=SHA_A)
    calls = []

    class Process:
        pid = 4321

    monkeypatch.setattr(update.sys, "frozen", True, raising=False)
    monkeypatch.setattr(update.sys, "executable", "current-active-slot.exe")
    monkeypatch.setenv("QM_WINDOWS_APP_JOB_ROOT", "3141")
    monkeypatch.setattr(
        update.subprocess,
        "Popen",
        lambda command, **kwargs: calls.append((command, kwargs)) or Process(),
    )

    result = update.start_activation(SHA_B, tmp_path)

    assert result["status"] == "accepted"
    assert calls[0][0] == [
        str(tmp_path / "slots" / SHA_B / "QuantMaster.exe"),
        "activate",
        SHA_B,
        "--root-pid",
        "3141",
    ]


def test_launcher_target_is_one_validated_line(tmp_path):
    (tmp_path / "launcher.target").write_text(f"{SHA_A}\n{SHA_B}\n", encoding="ascii")
    with pytest.raises(ActivationBlocked, match="完整 lowercase SHA"):
        read_launcher_target(tmp_path)


def _slot_meta(
    root: Path, sha: str, *, version: str, release_date: str, title: str = "",
) -> None:
    (root / "slots" / sha / "slot_meta.json").write_text(
        json.dumps({"schema": 1, "build_sha": sha, "version": version,
                    "release_date": release_date, "title": title}),
        encoding="utf-8",
    )


def test_update_status_attaches_version_metadata_to_active_and_candidates(tmp_path):
    _candidate(tmp_path, SHA_A)
    _candidate(tmp_path, SHA_B)
    _slot_meta(
        tmp_path, SHA_A, version="1.16.0", release_date="2026-08-15", title="v1.16.0",
    )
    _slot_meta(
        tmp_path,
        SHA_B,
        version="1.16.1",
        release_date="2026-08-16",
        title="v1.16.1 · fix(update): keep staging metadata",
    )
    _state(tmp_path, active=SHA_A)

    status = update_status(tmp_path)

    assert status["active"]["version"] == "1.16.0"
    assert status["active"]["release_date"] == "2026-08-15"
    by_sha = {item["build_sha"]: item for item in status["staged"]}
    assert by_sha[SHA_A]["version"] == "1.16.0"
    assert by_sha[SHA_A]["release_date"] == "2026-08-15"
    assert by_sha[SHA_A]["title"] == "v1.16.0"
    assert by_sha[SHA_B]["version"] == "1.16.1"
    assert by_sha[SHA_B]["release_date"] == "2026-08-16"
    assert by_sha[SHA_B]["title"] == "v1.16.1 · fix(update): keep staging metadata"
    assert by_sha[SHA_B]["changelog"] == []
    assert str(tmp_path) not in json.dumps(status, ensure_ascii=False)


def test_release_lookup_returns_version_metadata_via_version_or_sha():
    from quantmaster.release.history import release_lookup, release_sections

    assert release_lookup("1.16.2")["version"] == "1.16.2"
    assert release_lookup("1.16.2")["release_date"] == "2026-08-16"
    sections = release_sections("1.16.2")
    assert isinstance(sections, list) and len(sections) >= 1
    assert sections[0]["title"]
    unknown = release_lookup("a" * 40)
    assert unknown == {"version": "", "release_date": "", "sha": "a" * 40}
    assert release_lookup("UPPERCASE")["version"] == ""
