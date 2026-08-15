from __future__ import annotations

import threading
import time

import pytest

from quantmaster.config import Config, set_config
from quantmaster.lab.jobs import LabJobManager
from quantmaster.lab.store import LabStore
from quantmaster.lab.worker import LabWorker


def _ready(*_args, **_kwargs):
    return {
        "runnable": True,
        "state": "ready",
        "resource_class": "cpu",
        "blockers": [],
        "warnings": [],
        "dataset": {},
    }


class _BlockingService:
    def __init__(self, store: LabStore):
        self.store = store
        self.release = threading.Event()
        self.two_started = threading.Event()
        self._lock = threading.Lock()
        self.active = 0
        self.max_active = 0
        self.calls = 0

    preflight = staticmethod(_ready)

    def recover_publications(self, limit=20):
        return {"attempted": 0, "published": 0}

    def run_job(self, job, progress=None, cancelled=None):
        with self._lock:
            self.calls += 1
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            if self.active >= 2:
                self.two_started.set()
        try:
            if not self.release.wait(timeout=5):
                raise TimeoutError("测试任务等待释放超时")
            if cancelled and cancelled():
                raise InterruptedError("测试任务已取消")
            return {"job_id": job["id"]}
        finally:
            with self._lock:
                self.active -= 1


def _config(tmp_path, *, max_workers=2):
    cfg = Config()
    cfg.data.root = str(tmp_path)
    cfg.lab.enabled = True
    cfg.lab.max_workers = max_workers
    set_config(cfg)


def _wait_until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def test_worker_runs_two_unified_jobs_without_blocking_the_queue(tmp_path, monkeypatch):
    _config(tmp_path, max_workers=2)
    service = _BlockingService(LabStore(tmp_path / "lab.sqlite"))
    worker = LabWorker(service=service)
    monkeypatch.setattr(worker, "_start_scheduler_locked", lambda: None)
    jobs = [
        worker.jobs.submit("prepare_data", {"number": number}, preflight=_ready())
        for number in range(3)
    ]

    try:
        worker.start()
        assert service.two_started.wait(timeout=3), "两个研究任务应并行启动"
        statuses = [worker.jobs.get(job["id"])["status"] for job in jobs]
        assert statuses.count("running") == 2
        assert statuses.count("queued") == 1
        assert worker.status()["max_workers"] == 2
        assert len(worker.status()["active_job_ids"]) == 2

        service.release.set()
        assert _wait_until(
            lambda: all(worker.jobs.get(job["id"])["status"] == "completed" for job in jobs)
        )
        assert service.max_active == 2
    finally:
        service.release.set()
        worker.stop()
        set_config(None)


def test_worker_preserves_io_resource_limit_inside_unified_runtime(tmp_path):
    _config(tmp_path, max_workers=2)
    service = _BlockingService(LabStore(tmp_path / "lab.sqlite"))
    io_ready = {**_ready(), "resource_class": "io"}
    service.preflight = lambda *_args, **_kwargs: dict(io_ready)
    manager = LabJobManager(service=service)
    jobs = [
        manager.submit("prepare_data", {"number": number}, preflight=io_ready)
        for number in range(2)
    ]

    try:
        assert _wait_until(
            lambda: [manager.get(job["id"])["status"] for job in jobs].count("running") == 2
        )
        time.sleep(0.2)
        assert service.max_active == 1
        assert not service.two_started.is_set()
        service.release.set()
        assert _wait_until(
            lambda: all(manager.get(job["id"])["status"] == "completed" for job in jobs)
        )
    finally:
        service.release.set()
        manager.shutdown()
        set_config(None)


def test_worker_pause_restart_resumes_same_job_under_new_attempt(tmp_path, monkeypatch):
    _config(tmp_path, max_workers=1)
    service = _BlockingService(LabStore(tmp_path / "lab.sqlite"))
    worker = LabWorker(service=service)
    monkeypatch.setattr(worker, "_start_scheduler_locked", lambda: None)
    job = worker.jobs.submit("prepare_data", {"number": 1}, preflight=_ready())

    try:
        assert _wait_until(lambda: worker.jobs.get(job["id"])["status"] == "running")
        worker.drain()
        assert worker.jobs.get(job["id"])["status"] == "interrupted"
        service.release.set()
        assert _wait_until(lambda: not worker.jobs.active_job_ids())

        worker.start()
        assert _wait_until(lambda: worker.jobs.get(job["id"])["status"] == "completed")
        current = worker.jobs.get(job["id"])
        assert current["id"] == job["id"]
        assert current["attempt"] == 2
        assert service.calls == 2
    finally:
        service.release.set()
        worker.stop()
        set_config(None)


def test_cancelled_unified_lab_job_rejects_late_worker_result(tmp_path, monkeypatch):
    _config(tmp_path, max_workers=1)
    service = _BlockingService(LabStore(tmp_path / "lab.sqlite"))
    manager = LabJobManager(service=service)
    worker = LabWorker(manager=manager)
    monkeypatch.setattr(worker, "_start_scheduler_locked", lambda: None)
    job = manager.submit("prepare_data", {"number": 1}, preflight=_ready())

    try:
        assert _wait_until(lambda: manager.get(job["id"])["status"] == "running")
        manager.cancel(job["id"])
        service.release.set()
        assert _wait_until(lambda: manager.get(job["id"])["status"] == "cancelled")
        assert service.store.worker_result(job["id"]) is None
    finally:
        service.release.set()
        worker.stop()
        set_config(None)


def test_retry_reuses_committed_worker_result_without_duplicate_compute(tmp_path):
    _config(tmp_path, max_workers=1)
    store = LabStore(tmp_path / "lab.sqlite")

    class _Service:
        def __init__(self):
            self.store = store
            self.calls = 0

        preflight = staticmethod(_ready)

        def run_job(self, job, progress=None, cancelled=None):
            self.calls += 1
            return {
                "candidates": [{"id": "candidate-v1"}],
                "warnings": [{"code": "PARTIAL", "message": "保留候选"}],
            }

    service = _Service()
    manager = LabJobManager(service=service)
    job = manager.submit("discover_genetic", {"rounds": 2}, preflight=_ready())
    try:
        assert _wait_until(lambda: manager.get(job["id"])["status"] == "completed_with_errors")
        retried = manager.retry(job["id"])
        assert retried["id"] == job["id"]
        assert _wait_until(lambda: manager.get(job["id"])["status"] == "completed_with_errors")
        completed = manager.get(job["id"])
        assert completed["attempt"] == 2
        assert completed["outcome"] == "completed_with_warnings"
        assert completed["result"]["candidates"] == [{"id": "candidate-v1"}]
        assert service.calls == 1
        assert any(
            event["type"] == "lab_result_reused" for event in manager.events(job["id"])
        )
    finally:
        manager.shutdown()
        set_config(None)


def test_scheduled_business_key_is_idempotent_without_lab_slot_table(tmp_path):
    _config(tmp_path, max_workers=1)
    store = LabStore(tmp_path / "lab.sqlite")

    class _Service:
        def __init__(self):
            self.store = store
            self.calls = 0

        preflight = staticmethod(_ready)

        def run_job(self, job, progress=None, cancelled=None):
            self.calls += 1
            return {"ok": True}

    service = _Service()
    manager = LabJobManager(service=service)
    try:
        first = manager.submit(
            "prepare_data", {"_scheduled": True}, preflight=_ready(),
            business_key="lab:daily:2026-08-01:prepare",
        )
        assert _wait_until(lambda: manager.get(first["id"])["status"] == "completed")
        second = manager.submit(
            "prepare_data", {"_scheduled": True}, preflight=_ready(),
            business_key="lab:daily:2026-08-01:prepare",
        )
        assert second["id"] == first["id"]
        assert service.calls == 1
        with store._conn() as connection:
            tables = {row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
        assert "lab_schedule_slots" not in tables
    finally:
        manager.shutdown()
        set_config(None)


def test_scheduled_domain_records_are_content_addressed_and_never_overwritten(tmp_path):
    _config(tmp_path, max_workers=1)
    store = LabStore(tmp_path / "lab.sqlite")
    study_config = {"protocol": {"horizons": [3]}, "universe": "demo"}
    run_config = {"rounds": 2, "universe": "demo"}
    try:
        first_study = store.create_study(study_config, study_id="scheduled-study")
        repeated_study = store.create_study(study_config, study_id="scheduled-study")
        first_run = store.create_mining_run(run_config, run_id="scheduled-run")
        repeated_run = store.create_mining_run(run_config, run_id="scheduled-run")

        assert repeated_study["id"] == first_study["id"]
        assert repeated_run["id"] == first_run["id"]
        with pytest.raises(ValueError, match="不同配置"):
            store.create_study(
                {**study_config, "universe": "other"}, study_id="scheduled-study",
            )
        with pytest.raises(ValueError, match="不同配置"):
            store.create_mining_run(
                {**run_config, "rounds": 3}, run_id="scheduled-run",
            )
        assert len(store.studies()) == 1
        assert len(store.mining_runs()) == 1
    finally:
        set_config(None)


def test_domain_recovery_preserves_records_owned_by_any_live_unified_job(tmp_path):
    _config(tmp_path, max_workers=1)
    store = LabStore(tmp_path / "lab.sqlite")

    class _Service:
        def __init__(self):
            self.store = store

        preflight = staticmethod(_ready)

        @staticmethod
        def run_job(job, progress=None, cancelled=None):
            return {"ok": True}

    manager = LabJobManager(service=_Service())
    runtime = manager._ensure_runtime()
    job, _created = runtime.store.submit(
        "lab.optimize",
        {
            "kind": "optimize", "params": {"study_id": "study-live"},
            "preflight": _ready(), "dataset_id": "", "resource_class": "cpu",
        },
    )
    study = store.create_study({"protocol": {"horizons": [3]}})
    store.update_study(study["id"], job_id=job["id"], status="running")
    try:
        recovered = store.recover_orphaned_records(manager.live_job_ids())
        assert recovered["studies"] == 0
        assert store.study(study["id"])["status"] == "running"

        runtime.store.cancel(job["id"])
        recovered = store.recover_orphaned_records(manager.live_job_ids())
        assert recovered["studies"] == 1
        assert store.study(study["id"])["status"] == "interrupted"
    finally:
        manager.shutdown()
        set_config(None)
