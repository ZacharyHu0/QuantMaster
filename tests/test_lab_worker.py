from __future__ import annotations

import threading
import time

from quantmaster.config import Config, set_config
from quantmaster.lab.store import LabStore
from quantmaster.lab.worker import LabWorker


class _BlockingService:
    def __init__(self, store: LabStore):
        self.store = store
        self.release = threading.Event()
        self.two_started = threading.Event()
        self._lock = threading.Lock()
        self.active = 0
        self.max_active = 0

    def run_job(self, job, progress=None, cancelled=None):
        with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            if self.active >= 2:
                self.two_started.set()
        try:
            if not self.release.wait(timeout=5):
                raise TimeoutError("测试任务等待释放超时")
            return {"job_id": job["id"]}
        finally:
            with self._lock:
                self.active -= 1


def _wait_until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def test_worker_runs_two_jobs_without_blocking_the_queue(tmp_path, monkeypatch):
    cfg = Config()
    cfg.data.root = str(tmp_path)
    cfg.lab.enabled = True
    cfg.lab.max_workers = 2
    set_config(cfg)
    store = LabStore(tmp_path / "lab.sqlite")
    jobs = [store.enqueue("prepare_data", {"number": number}) for number in range(3)]
    service = _BlockingService(store)
    worker = LabWorker(service=service, poll_seconds=0.01)
    monkeypatch.setattr(worker, "_start_scheduler_locked", lambda: None)

    try:
        worker.start()
        assert service.two_started.wait(timeout=3), "两个研究任务应并行启动"
        statuses = [store.job(job["id"])["status"] for job in jobs]
        assert statuses.count("running") == 2
        assert statuses.count("queued") == 1
        assert worker.status()["max_workers"] == 2
        assert len(worker.status()["active_job_ids"]) == 2

        service.release.set()
        assert _wait_until(
            lambda: all(store.job(job["id"])["status"] == "completed" for job in jobs)
        )
        assert service.max_active == 2
    finally:
        service.release.set()
        worker.stop()
        set_config(None)


def test_claim_limit_is_global_across_workers(tmp_path):
    store = LabStore(tmp_path / "lab.sqlite")
    first = store.enqueue("prepare_data", {"number": 1})
    second = store.enqueue("prepare_data", {"number": 2})

    assert store.claim_next("worker-a", max_running=1)["id"] == first["id"]
    assert store.claim_next("worker-b", max_running=1) is None
    store.finish_job(first["id"], result={"ok": True})
    assert store.claim_next("worker-b", max_running=1)["id"] == second["id"]


def test_reclaimed_lab_job_rejects_stale_worker_updates(tmp_path):
    store = LabStore(tmp_path / "lab.sqlite")
    job = store.enqueue("prepare_data", {"number": 1})
    assert store.claim_next("worker-a")["id"] == job["id"]
    assert store.heartbeat_job(job["id"], "worker-a")
    assert store.interrupt_stale(stale_after_seconds=30) == 0

    with store._conn() as connection:
        connection.execute(
            "UPDATE lab_jobs SET heartbeat_at='2000-01-01T00:00:00+00:00' WHERE id=?",
            (job["id"],),
        )
    assert store.interrupt_stale(stale_after_seconds=30) == 1
    assert store.claim_next("worker-b")["id"] == job["id"]
    assert not store.update_job(
        job["id"], 80, "旧进程", expected_worker="worker-a",
    )
    assert not store.finish_job(
        job["id"], result={"stale": True}, expected_worker="worker-a",
    )
    current = store.job(job["id"])
    assert current["worker"] == "worker-b"
    assert current["status"] == "running"
