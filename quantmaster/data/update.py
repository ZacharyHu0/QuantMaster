"""Checkpointed user update plan, executed by the existing data job runtime."""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any

from quantmaster.runtime.jobs import UnifiedJobRuntime, UnifiedJobStore

UPDATE_TASK = "data.update"
UPDATE_STATE = "data.update.plan"
ACTIVE = {"queued", "running", "cancelling", "interrupted"}


def queue_status(store: UnifiedJobStore, job: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    end = datetime.fromisoformat(job["finished_at"]).timestamp() if job.get("finished_at") else time.time()
    elapsed = max(0, round(end - datetime.fromisoformat(job["created_at"]).timestamp()))
    queued = job["status"] == "queued" or job.get("waiting_on") == "refresh_batch"
    waiting = [job] if queued else []
    if job["status"] in ACTIVE:
        waiting.extend(
            child for item in state.get("children", [])[:10]
            if (child := store.get(item["id"]))["status"] == "queued"
        )
    pools = {str(item["type"]) for item in waiting}
    if pools.intersection({"data.refresh", UPDATE_TASK}):
        pools.update({"data.refresh", UPDATE_TASK})
    blockers = {}
    for job_type in sorted(pools):
        for running in store.list(5, job_type=job_type, statuses=("running", "cancelling")):
            if running["id"] == job["id"]:
                continue
            spec = running["spec"]
            blockers[running["id"]] = {
                "id": running["id"], "type": job_type, "status": running["status"],
                "progress": running["progress"], "detail": running.get("detail", ""),
                "scope": spec.get("universe") or spec.get("scope") or job_type,
            }
    return {
        "elapsed_seconds": elapsed,
        "remaining_seconds": max(0, round(float(job["deadline_seconds"]) - elapsed)),
        "overdue": job["status"] in ACTIVE and elapsed >= float(job["deadline_seconds"]),
        "phase": ("waiting_for_slot" if blockers else
                  "queued" if queued else
                  "dependencies" if job.get("waiting_on") else job["status"]),
        "blockers": list(blockers.values())[:10],
    }


def project_update(store: UnifiedJobStore, job: dict[str, Any]) -> dict[str, Any]:
    state = store.checkpoint(job["id"], UPDATE_STATE, job["spec_hash"]) or {}
    return {
        **UnifiedJobRuntime.public(job),
        "target_session": job["spec"]["target_session"], **state,
        "waiting_on": str(job.get("waiting_on") or ""), "can_retry": False,
        "queue": queue_status(store, job, state),
    }


