"""Real SQLite replacement races at the reader's snapshot boundary."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event

import pytest
from fastapi.testclient import TestClient

from quantmaster.data.maintenance import (
    DATA_REFRESH_TASK_TYPE,
    REFRESH_CHECKPOINT,
    REFRESH_SCHEMA,
    DataRefreshManager,
)
from quantmaster.runtime.jobs import (
    INLINE_ARTIFACT_LIMIT,
    ArtifactIntegrityError,
    JobOutcome,
    UnifiedJobRuntime,
    UnifiedJobStore,
)
from quantmaster.server.app import app


@contextmanager
def _replace_after_select(monkeypatch, reader, read, replace, *, checkpoint_only=False):
    """Pause after rows are fetched and the connection closes; commit on another connection."""
    selected, resume = Event(), Event()
    connect = reader._conn

    @contextmanager
    def paused_connection():
        queries = []
        with connect() as connection:
            connection.set_trace_callback(queries.append)
            yield connection
        fragment = "AND checkpoint_key=" if checkpoint_only else "FROM runtime_job_artifacts WHERE job_id="
        if any(fragment in sql for sql in queries):
            selected.set()
            assert resume.wait(10), "replacement never completed"

    with monkeypatch.context() as patch, ThreadPoolExecutor(max_workers=1) as executor:
        patch.setattr(reader, "_conn", paused_connection)
        future = executor.submit(read)
        try:
            assert selected.wait(10), "reader never selected the artifact"
            replace()
        finally:
            resume.set()
        yield future.result(timeout=10)


@pytest.mark.parametrize("external", [False, True])
@pytest.mark.parametrize("method", ["checkpoint", "latest_artifact"])
def test_replacement_preserves_selected_artifact(monkeypatch, tmp_path, external, method):
    writer = UnifiedJobStore(tmp_path / "jobs.sqlite")
    job, _ = writer.submit("test.snapshot", {"value": 1})
    reader = UnifiedJobStore(writer.path, read_only=True)
    old = {"version": 1, "blob": "x" * (INLINE_ARTIFACT_LIMIT if external else 1)}
    new = {**old, "version": 2}
    first = writer.write_artifact(job["id"], "progress", old, checkpoint_key="progress")
    assert first["external"] is external

    def read():
        if method == "checkpoint":
            return reader.checkpoint(job["id"], "progress", job["spec_hash"])
        return reader.latest_artifact(job["id"], "progress")["payload"]

    def replace():
        writer.write_artifact(job["id"], "progress", new, checkpoint_key="progress")
        with pytest.raises(KeyError):
            writer.artifact(first["id"])

    with _replace_after_select(monkeypatch, reader, read, replace) as value:
        assert value == old
    assert read() == new
    assert writer.repairs() == []


@pytest.mark.parametrize("external", [False, True])
def test_checkpoint_integrity_and_read_only_repair(tmp_path, external):
    writer = UnifiedJobStore(tmp_path / "jobs.sqlite")
    job, _ = writer.submit("test.snapshot", {"value": 1})
    reader = UnifiedJobStore(writer.path, read_only=True)
    assert reader.checkpoint(job["id"], "missing", job["spec_hash"]) is None
    payload = {"blob": "x" * (INLINE_ARTIFACT_LIMIT if external else 1)}
    artifact = writer.write_artifact(job["id"], "progress", payload, checkpoint_key="progress")
    assert reader.checkpoint(job["id"], "progress", "wrong-spec") is None
    if external:
        row = writer.artifact(artifact["id"])
        (writer.path.parent / row["external_path"]).write_bytes(b"corrupt")
    else:
        with writer._conn() as connection:
            connection.execute(
                "UPDATE runtime_job_artifacts SET payload_json='{}' WHERE id=?", (artifact["id"],),
            )
    with pytest.raises(ArtifactIntegrityError):
        reader.artifact(artifact["id"])
    assert reader.checkpoint(job["id"], "progress", job["spec_hash"]) is None
    assert reader.latest_artifact(job["id"], "progress") is None
    assert writer.repairs() == []
    assert writer.checkpoint(job["id"], "progress", job["spec_hash"]) is None
    assert [item["artifact_id"] for item in writer.repairs()] == [artifact["id"]]
    with pytest.raises(KeyError):
        reader.checkpoint("missing-job", "progress", job["spec_hash"])


def test_checkpoint_attempt_order_spec_filter_and_corrupt_fallback(tmp_path):
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    job, _ = store.submit("test.snapshot", {"value": 1})
    store.write_artifact(job["id"], "progress", {"attempt": 1}, checkpoint_key="progress")
    store.cancel(job["id"])
    store.retry(job["id"])
    second = store.write_artifact(
        job["id"], "progress", {"attempt": 2}, checkpoint_key="progress",
    )
    assert store.checkpoint(job["id"], "progress", job["spec_hash"]) == {"attempt": 2}
    with store._conn() as connection:
        connection.execute(
            "UPDATE runtime_job_artifacts SET spec_hash='other-spec' WHERE id=?", (second["id"],),
        )
    assert store.checkpoint(job["id"], "progress", job["spec_hash"]) == {"attempt": 1}
    with store._conn() as connection:
        connection.execute(
            "UPDATE runtime_job_artifacts SET spec_hash=?,payload_json='{}' WHERE id=?",
            (job["spec_hash"], second["id"]),
        )
    assert store.checkpoint(job["id"], "progress", job["spec_hash"]) == {"attempt": 1}
    assert store.latest_artifact(job["id"], "progress")["payload"] == {"attempt": 1}
    assert [item["artifact_id"] for item in store.repairs()] == [second["id"]]


@pytest.mark.parametrize("external", [False, True])
def test_refresh_http_reads_during_checkpoint_replacement(monkeypatch, external):
    writer = UnifiedJobStore()
    reader = UnifiedJobStore(writer.path, read_only=True)
    manager = DataRefreshManager(UnifiedJobRuntime(writer, dispatch=False))
    projection = DataRefreshManager()
    monkeypatch.setattr(projection, "_read_store", lambda: reader)
    monkeypatch.setattr("quantmaster.server.jobs.data_refresh_manager", projection)
    monkeypatch.setattr(
        "quantmaster.runtime.worker.runtime_worker_status",
        lambda: {"available": True, "status": "running", "age_seconds": 0.0},
    )

    def worker_command(operation, payload, **_kwargs):
        assert operation == "data.refresh.cancel"
        return manager.cancel(payload["job_id"])

    monkeypatch.setattr("quantmaster.runtime.worker_ipc.call_worker_command", worker_command)
    job, _ = writer.submit(DATA_REFRESH_TASK_TYPE, {"refresh_schema": REFRESH_SCHEMA})
    assert writer.claim(job["id"], "test-worker")
    token = writer.get(job["id"])["lease_token"]
    state = {
        "schema_version": REFRESH_SCHEMA, "total": 20, "next_index": 0, "succeeded": 0,
        "blob": "x" * (INLINE_ARTIFACT_LIMIT if external else 1),
    }

    def progress():
        state["next_index"] += 1
        state["succeeded"] += 1
        writer.write_artifact(
            job["id"], "checkpoint", state, checkpoint_key=REFRESH_CHECKPOINT,
            owner="test-worker", lease_token=token,
        )
        writer.progress(job["id"], "test-worker", token, state["next_index"], "refresh", "")

    progress()
    client = TestClient(app)
    headers = {"X-CSRF-Token": client.get("/api/v1/session").json()["csrf_token"]}
    url = f"/api/v1/jobs/{job['id']}"
    for _ in range(3):
        for route in (url, "/api/v1/jobs?domain=data"):
            previous = state["next_index"]
            with _replace_after_select(
                monkeypatch, reader, lambda route=route: client.get(route), progress, checkpoint_only=True,
            ) as response:
                assert response.status_code == 200
                value = response.json()
                if "items" in value:
                    value = next(item for item in value["items"] if item["id"] == job["id"])
                assert value["next_index"] == previous
                assert value["succeeded"] == previous
                assert value["status"] == "running"
    with _replace_after_select(
        monkeypatch, reader, lambda: client.post(f"{url}/cancel", headers=headers), progress,
        checkpoint_only=True,
    ) as cancelled:
        assert cancelled.status_code == 200
        assert cancelled.json()["status"] == "cancelling"
    writer.finish(job["id"], "test-worker", JobOutcome("cancelled"), lease_token=token)
    terminal = client.get(url)
    assert terminal.status_code == 200
    assert terminal.json()["status"] == "cancelled"
    assert terminal.json()["next_index"] == state["next_index"]
    assert client.get("/api/v1/jobs/missing-job").status_code == 404
    assert writer.repairs() == []
