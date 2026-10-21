"""Checkpointed user update plan, executed by the existing data job runtime."""

from __future__ import annotations

from typing import Any

from quantmaster.runtime.jobs import UnifiedJobRuntime, UnifiedJobStore

UPDATE_TASK = "data.update"
UPDATE_STATE = "data.update.plan"
ACTIVE = {"queued", "running", "cancelling", "interrupted"}


def project_update(store: UnifiedJobStore, job: dict[str, Any]) -> dict[str, Any]:
    state = store.checkpoint(job["id"], UPDATE_STATE, job["spec_hash"]) or {}
    return {
        **UnifiedJobRuntime.public(job), **state,
        "waiting_on": str(job.get("waiting_on") or ""), "can_retry": False,
    }


