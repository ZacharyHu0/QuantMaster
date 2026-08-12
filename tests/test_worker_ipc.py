"""Runtime-worker local command channel contracts."""

import threading

import pytest

from quantmaster.runtime.maintenance import maintenance_barrier
from quantmaster.runtime.worker_ipc import (
    RuntimeCommandServer,
    WorkerCommandUnavailable,
    call_worker_command,
    worker_command_endpoint,
)


def test_worker_command_endpoint_stays_within_unix_socket_path_limit(tmp_path):
    root = tmp_path / ("deep-path-" * 20)
    assert len(worker_command_endpoint(root).encode()) <= 100


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


def test_runtime_worker_stop_completes_during_accept_loop_transition(tmp_path):
    server = RuntimeCommandServer(lambda *_args: {}, root=tmp_path / "runtime")
    server.start()
    stopper = threading.Thread(target=server.stop)
    stopper.start()
    stopper.join(timeout=2)
    assert not stopper.is_alive()
    assert not server.running


def test_runtime_worker_command_channel_fails_fast_when_no_worker_exists(tmp_path):
    with pytest.raises(WorkerCommandUnavailable):
        call_worker_command(
            "data.refresh.create",
            {"scope": "market"},
            timeout=0.1,
            root=tmp_path / "no-worker",
        )


def test_maintenance_command_token_is_held_by_worker_handler(tmp_path):
    lease = None

    def handler(operation, payload):
        nonlocal lease
        if operation == "maintenance.enter":
            lease = maintenance_barrier.enter(payload["reason"])
            return {"token": lease.token, **maintenance_barrier.status()}
        if operation == "maintenance.status":
            return {
                "valid": bool(lease and lease.token == payload["token"]),
                **maintenance_barrier.status(),
            }
        if operation == "maintenance.exit":
            maintenance_barrier.exit(lease)
            lease = None
            return {"released": True}
        return {}

    server = RuntimeCommandServer(handler, root=tmp_path / "runtime")
    server.start()
    try:
        entered = call_worker_command(
            "maintenance.enter", {"reason": "test"}, root=tmp_path / "runtime",
        )
        assert maintenance_barrier.frozen
        assert call_worker_command(
            "maintenance.status", {"token": entered["token"]}, root=tmp_path / "runtime",
        )["valid"]
        call_worker_command(
            "maintenance.exit", {"token": entered["token"]}, root=tmp_path / "runtime",
        )
        assert not maintenance_barrier.active
    finally:
        if lease is not None:
            maintenance_barrier.exit(lease)
        server.stop()
