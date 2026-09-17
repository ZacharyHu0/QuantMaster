"""Runtime-worker local command channel contracts."""

import os
import threading
import time

import pytest

from quantmaster.runtime.maintenance import maintenance_barrier
from quantmaster.runtime.worker_ipc import (
    RuntimeCommandServer,
    WorkerCommandError,
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


def test_runtime_worker_rejects_mixed_identity_before_handler_dispatch(
    tmp_path,
    monkeypatch,
):
    received = []
    monkeypatch.setenv("QM_BUILD_SHA", "a" * 40)
    monkeypatch.setenv("QM_SLOT_ID", "slot-a")
    monkeypatch.setenv("QM_RUNTIME_GENERATION", "b" * 32)
    server = RuntimeCommandServer(
        lambda operation, payload: received.append((operation, payload)) or {},
        root=tmp_path / "runtime",
    )
    server.start()
    monkeypatch.setenv("QM_RUNTIME_GENERATION", "c" * 32)
    try:
        with pytest.raises(WorkerCommandError, match="runtime_identity_mismatch") as exc_info:
            call_worker_command(
                "data.refresh.create",
                {"scope": "market"},
                root=tmp_path / "runtime",
            )
    finally:
        server.stop()

    assert exc_info.value.code == "runtime_identity_mismatch"
    assert received == []


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


def test_slow_handler_bounds_following_authentication_and_shutdown(tmp_path):
    entered, release = threading.Event(), threading.Event()

    def handler(*_args):
        entered.set()
        assert release.wait(5)
        return {}

    server = RuntimeCommandServer(handler, root=tmp_path / "runtime")
    server.start()
    try:
        with pytest.raises(WorkerCommandUnavailable):
            call_worker_command("slow", root=server.root, timeout=0.1)
        assert entered.is_set()
        for _ in range(5):
            started = time.monotonic()
            with pytest.raises(WorkerCommandUnavailable):
                call_worker_command("next", root=server.root, timeout=0.1)
            assert time.monotonic() - started < 0.5
        started = time.monotonic()
        server.stop()
        assert time.monotonic() - started < 1.5
        with pytest.raises(RuntimeError, match="still stopping"):
            server.start()
    finally:
        release.set()
        if server._thread is not None:
            server._thread.join(2)
        server.stop()


def test_stalled_unauthenticated_peer_is_reaped(tmp_path):
    from quantmaster.runtime.ipc_transport import DeadlineConnection

    server = RuntimeCommandServer(lambda *_: {}, root=tmp_path / "runtime")
    server.start()
    connection = DeadlineConnection.connect(server.endpoint, time.monotonic() + 1)
    try:
        # The idle raw client never answers the server's challenge.
        started = time.monotonic()
        with pytest.raises(WorkerCommandUnavailable):
            call_worker_command("next", root=server.root, timeout=0.1)
        assert time.monotonic() - started < 0.5
        assert call_worker_command("recovered", root=server.root, timeout=2) == {}
    finally:
        connection.close()
        server.stop()


def test_standard_authenticated_client_and_large_response_remain_compatible(tmp_path):
    from multiprocessing.connection import Client

    from quantmaster.runtime.identity import get_application_identity

    value = "large" * 30000
    server = RuntimeCommandServer(lambda *_: {"value": value}, root=tmp_path / "runtime")
    server.start()
    identity = get_application_identity()
    try:
        with Client(server.endpoint, family=server.family, authkey=server._authkey) as channel:
            channel.send({"operation": "test", "payload": {}, "application_identity": {
                "build_sha": identity.build_sha, "slot_id": identity.slot_id,
                "runtime_generation": identity.runtime_generation,
            }})
            assert channel.poll(2)
            assert channel.recv() == {"ok": True, "value": {"value": value}}
        assert call_worker_command("test", root=server.root)["value"] == value
    finally:
        server.stop()


@pytest.mark.skipif(os.name == "nt", reason="Unix byte-stream fragmentation contract")
def test_partial_unix_frame_cannot_hold_server_forever(tmp_path):
    import struct

    from quantmaster.runtime.ipc_transport import DeadlineConnection

    server = RuntimeCommandServer(lambda *_: {}, root=tmp_path / "runtime")
    server.start()
    channel = DeadlineConnection.connect(server.endpoint, time.monotonic() + 2)
    try:
        channel.authenticate(server._authkey)
        channel.connection.sendall(struct.pack("!i", 100) + b"partial")
        assert call_worker_command("recovered", root=server.root, timeout=2) == {}
    finally:
        channel.close()
        server.stop()
