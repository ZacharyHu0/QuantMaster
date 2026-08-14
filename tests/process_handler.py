"""Minimal importable handler used to exercise the Windows-spawn runtime path."""

from __future__ import annotations

import os
from pathlib import Path

from quantmaster.runtime.jobs import JobOutcome


def write_artifact(context, spec: dict) -> JobOutcome:
    from quantmaster.runtime.identity import get_application_identity

    identity = get_application_identity()
    context.progress(40, "计算子进程", "写入可验证结果")
    artifact = context.write_artifact(
        "test.process.result",
        {
            "schema_version": "1.0",
            "pid": os.getpid(),
            "value": spec["value"],
            "application_identity": {
                "build_sha": identity.build_sha,
                "slot_id": identity.slot_id,
                "runtime_generation": identity.runtime_generation,
            },
        },
        {"schema_version": "1.0", "lineage": {"fixture": "process"}},
    )
    return JobOutcome("completed", "isolated", artifact["id"])


def supervisor_probe(stop_event, bootstrap_rotation: bool) -> None:
    """Spawn-safe lightweight Supervisor target used by lifecycle tests."""

    marker = Path(os.environ["QM_TEST_SUPERVISOR_MARKER"])
    marker.write_text(f"{os.getpid()}|{int(bool(bootstrap_rotation))}", encoding="utf-8")
    while not stop_event.wait(0.05):
        pass


def supervisor_crash_once(stop_event, bootstrap_rotation: bool) -> None:
    """Exit once, then remain alive so the parent can prove recovery."""

    marker = Path(os.environ["QM_TEST_SUPERVISOR_MARKER"])
    attempt = marker.with_suffix(".attempt")
    if not attempt.exists():
        attempt.write_text("1", encoding="utf-8")
        return
    marker.write_text(f"{os.getpid()}|{int(bool(bootstrap_rotation))}", encoding="utf-8")
    while not stop_event.wait(0.05):
        pass
