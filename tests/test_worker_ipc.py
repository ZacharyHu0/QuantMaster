"""Runtime-worker local command channel contracts."""

import pytest

from quantmaster.runtime.worker_ipc import (
    RuntimeCommandServer,
    WorkerCommandUnavailable,
    call_worker_command,
)


def test_runtime_worker_command_channel_round_trips_without_web_writes(tmp_path):
    received = []

    def handler(operation, payload):
        received.append((operation, payload))
        return {"id": "job-1", "status": "queued"}

    server = RuntimeCommandServer(handler, root=tmp_path / "runtime")
    server.start()
    try:
        result = call_worker_command(
            "data.refresh.create",
            {"scope": "market"},
            root=tmp_path / "runtime",
        )
    finally:
        server.stop()

    assert result == {"id": "job-1", "status": "queued"}
    assert received == [("data.refresh.create", {"scope": "market"})]


def test_runtime_worker_command_channel_fails_fast_when_no_worker_exists(tmp_path):
    with pytest.raises(WorkerCommandUnavailable):
        call_worker_command(
            "data.refresh.create",
            {"scope": "market"},
            timeout=0.1,
            root=tmp_path / "no-worker",
        )
