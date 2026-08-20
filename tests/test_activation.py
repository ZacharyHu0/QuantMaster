"""Atomic immutable-slot activation contracts."""

import json
from pathlib import Path

import pytest

import quantmaster.runtime.activation as activation
from quantmaster.runtime.activation import (
    ActivationBlocked,
    ActivationCoordinator,
    SlotRegistry,
    SubprocessGenerationController,
)
from quantmaster.runtime.identity import ApplicationIdentity

SHA_A = "a" * 40
SHA_B = "b" * 40
SHA_C = "c" * 40


def _candidate(root: Path, sha: str, *, reversible: bool = True) -> Path:
    slot = root / "slots" / sha
    slot.mkdir(parents=True)
    (slot / "QuantMaster.exe").write_bytes(b"candidate")
    marker = {
        "schema": 1,
        "status": "staged",
        "build_sha": sha,
        "slot_id": sha,
        "size": {"within_hard_limits": True},
        "smoke": {"build_sha": sha, "slot_id": sha},
    }
    if not reversible:
        marker["migration"] = {"reversible": False}
    (slot / ".quantmaster-stage.json").write_text(
        json.dumps(marker), encoding="utf-8",
    )
    return slot


def _write_state(
    root: Path, *, active: str, previous: str = "", pending: str = "",
    status: str = "stable",
) -> None:
    app = root
    app.mkdir(parents=True, exist_ok=True)
    (app / "active.json").write_text(json.dumps({
        "schema": 1,
        "active": active,
        "previous": previous,
        "pending": pending,
        "status": status,
        "last_error": "",
    }), encoding="utf-8")


class _Generation:
    def __init__(self, sha: str) -> None:
        self.sha = sha
        self.stopped = False

    def poll(self):
        return 0 if self.stopped else None


class FakeController:
    def __init__(self, current: str = "", *, fail: str = "", fail_stop: bool = False) -> None:
        self.current = current
        self.fail = fail
        self.fail_stop = fail_stop
        self.calls: list[tuple[str, str]] = []

    def current_identity(self):
        if not self.current:
            return None
        return {"build_sha": self.current, "slot_id": self.current}

    def drain_current(self, _timeout: float) -> None:
        self.calls.append(("drain", self.current))

    def resume_current(self, _timeout: float) -> None:
        self.calls.append(("resume", self.current))

    def stop_current(self, _timeout: float) -> None:
        self.calls.append(("stop-current", self.current))
        if self.fail_stop:
            raise ActivationBlocked("current_stop_failed", "fixture failure")
        self.current = ""

    def start_generation(self, _slot: Path, identity: ApplicationIdentity):
        self.calls.append(("start", identity.build_sha))
        if identity.build_sha == self.fail:
            raise ActivationBlocked("candidate_start_failed", "fixture failure")
        self.current = identity.build_sha
        return _Generation(identity.build_sha)

    def wait_ready(self, generation: _Generation, identity: ApplicationIdentity, _timeout: float):
        self.calls.append(("ready", identity.build_sha))
        if identity.build_sha == self.fail:
            raise ActivationBlocked("candidate_not_ready", "fixture failure")
        return {
            "status": "ok",
            "core_ready": True,
            "build_sha": identity.build_sha,
            "slot_id": identity.slot_id,
            "runtime_generation": identity.runtime_generation,
        }

    def stop_generation(
        self,
        generation: _Generation,
        _identity: ApplicationIdentity,
        _timeout: float,
    ) -> None:
        self.calls.append(("stop-generation", generation.sha))
        generation.stopped = True
        if self.current == generation.sha:
            self.current = ""


def test_activation_commits_a_new_generation_and_preserves_previous(tmp_path):
    _candidate(tmp_path, SHA_A)
    _candidate(tmp_path, SHA_B)
    _write_state(tmp_path, active=SHA_A)
    registry = SlotRegistry(tmp_path)
    controller = FakeController(SHA_A)

    result = ActivationCoordinator(registry, controller).activate(SHA_B)

    assert result["status"] == "activated"
    assert registry.read() == {
        "schema": 1,
        "active": SHA_B,
        "previous": SHA_A,
        "pending": "",
        "status": "stable",
        "last_error": "",
    }
    assert (tmp_path / "launcher.target").read_text(encoding="ascii") == f"{SHA_B}\n"
    assert controller.calls == [
        ("drain", SHA_A), ("stop-current", SHA_A),
        ("start", SHA_B), ("ready", SHA_B),
    ]


def test_candidate_failure_rolls_back_previous_slot(tmp_path):
    _candidate(tmp_path, SHA_A)
    _candidate(tmp_path, SHA_B)
    _write_state(tmp_path, active=SHA_A)
    registry = SlotRegistry(tmp_path)
    controller = FakeController(SHA_A, fail=SHA_B)

    result = ActivationCoordinator(registry, controller).activate(SHA_B)

    assert result["status"] == "rolled_back"
    assert registry.read()["active"] == SHA_A
    assert registry.read()["pending"] == ""
    assert (tmp_path / "launcher.target").read_text(encoding="ascii") == f"{SHA_A}\n"
    assert ("start", SHA_A) in controller.calls


def test_candidate_cleanup_failure_blocks_without_restarting_previous(tmp_path):
    _candidate(tmp_path, SHA_A)
    _candidate(tmp_path, SHA_B)
    _write_state(tmp_path, active=SHA_A)

    class CleanupFailController(FakeController):
        def start_generation(self, _slot: Path, identity: ApplicationIdentity):
            self.calls.append(("start", identity.build_sha))
            self.current = identity.build_sha
            return _Generation(identity.build_sha)

        def wait_ready(
            self,
            _generation: _Generation,
            _identity: ApplicationIdentity,
            _timeout: float,
        ):
            raise ActivationBlocked("candidate_not_ready", "fixture candidate not ready")

        def stop_generation(
            self,
            generation: _Generation,
            identity: ApplicationIdentity,
            timeout: float,
        ) -> None:
            super().stop_generation(generation, identity, timeout)
            raise ActivationBlocked("candidate_cleanup_failed", "fixture candidate still listening")

    controller = CleanupFailController(SHA_A, fail=SHA_B)
    result = ActivationCoordinator(SlotRegistry(tmp_path), controller).activate(SHA_B)

    assert result["status"] == "blocked"
    assert controller.calls.count(("start", SHA_A)) == 0
    assert "candidate_cleanup_failed" in str(SlotRegistry(tmp_path).read()["last_error"])


def test_stop_failure_releases_drain_before_retry(tmp_path):
    _candidate(tmp_path, SHA_A)
    _candidate(tmp_path, SHA_B)
    _write_state(tmp_path, active=SHA_A)
    registry = SlotRegistry(tmp_path)
    controller = FakeController(SHA_A, fail_stop=True)

    assert ActivationCoordinator(registry, controller).activate(SHA_B)["status"] == "rolled_back"
    assert controller.calls == [
        ("drain", SHA_A), ("stop-current", SHA_A), ("resume", SHA_A),
    ]

    controller.fail_stop = False
    assert ActivationCoordinator(registry, controller).activate(SHA_B)["status"] == "activated"


def test_schema_handoff_stops_an_unrecoverable_worker_and_activates_candidate(tmp_path):
    _candidate(tmp_path, SHA_A)
    _candidate(tmp_path, SHA_B)
    _write_state(tmp_path, active=SHA_A)
    registry = SlotRegistry(tmp_path)
    registry.launcher_target.write_text(f"{SHA_A}\n", encoding="ascii")
    controller = FakeController(SHA_A)

    def fail_drain(_timeout: float) -> None:
        raise ActivationBlocked("worker_drain_unconfirmed", "fixture worker unavailable")

    controller.drain_current = fail_drain
    result = ActivationCoordinator(
        registry, controller, allow_unrecoverable_current=True,
    ).activate(SHA_B)

    assert result["status"] == "activated"
    assert registry.read()["active"] == SHA_B
    assert registry.launcher_target.read_text(encoding="ascii") == f"{SHA_B}\n"
    assert not registry.handoff_path.exists()
    assert controller.calls == [("stop-current", SHA_A), ("start", SHA_B), ("ready", SHA_B)]


def test_schema_handoff_blocks_without_relaunching_incompatible_previous_slot(tmp_path):
    _candidate(tmp_path, SHA_A)
    _candidate(tmp_path, SHA_B)
    _write_state(tmp_path, active=SHA_A)
    registry = SlotRegistry(tmp_path)
    registry.launcher_target.write_text(f"{SHA_A}\n", encoding="ascii")
    controller = FakeController(SHA_A, fail=SHA_B)

    def fail_drain(_timeout: float) -> None:
        raise ActivationBlocked("worker_drain_unconfirmed", "fixture worker unavailable")

    controller.drain_current = fail_drain
    result = ActivationCoordinator(
        registry, controller, allow_unrecoverable_current=True,
    ).activate(SHA_B)

    assert result["status"] == "blocked"
    assert registry.read()["status"] == "blocked"
    assert not registry.launcher_target.exists()
    assert registry.pending_schema_handoff(SHA_B)
    assert ("start", SHA_A) not in controller.calls


def test_interrupted_schema_handoff_retries_candidate_without_relaunching_previous(tmp_path):
    _candidate(tmp_path, SHA_A)
    _candidate(tmp_path, SHA_B)
    _write_state(tmp_path, active=SHA_A, pending=SHA_B, status="pending")
    registry = SlotRegistry(tmp_path)
    registry.begin_schema_handoff(candidate=SHA_B, previous=SHA_A)
    controller = FakeController()

    result = ActivationCoordinator(registry, controller).activate(SHA_B)

    assert result["status"] == "activated"
    assert registry.read()["active"] == SHA_B
    assert not registry.handoff_path.exists()
    assert ("start", SHA_A) not in controller.calls
    assert controller.calls == [("start", SHA_B), ("ready", SHA_B)]


def test_drain_reconciliation_failure_records_its_phase(tmp_path):
    _candidate(tmp_path, SHA_A)
    _candidate(tmp_path, SHA_B)
    _write_state(tmp_path, active=SHA_A)
    registry = SlotRegistry(tmp_path)
    controller = FakeController(SHA_A)

    def fail_drain(_timeout: float) -> None:
        raise ActivationBlocked(
            "worker_drain_unconfirmed",
            "当前 runtime-worker 排空状态未确认",
            phase="maintenance_status_reconcile",
        )

    controller.drain_current = fail_drain

    result = ActivationCoordinator(registry, controller).activate(SHA_B)

    assert result["status"] == "rolled_back"
    assert result["last_error"] == (
        "worker_drain_unconfirmed: 当前 runtime-worker 排空状态未确认 "
        "[maintenance_status_reconcile]"
    )


def test_failed_first_activation_is_retryable_without_registry_repair(tmp_path):
    _candidate(tmp_path, SHA_B)
    registry = SlotRegistry(tmp_path)
    controller = FakeController(fail=SHA_B)

    with pytest.raises(ActivationBlocked) as failure:
        ActivationCoordinator(registry, controller).activate(SHA_B)

    assert failure.value.code == "candidate_start_failed"
    assert registry.read() == {
        "schema": 1,
        "active": "",
        "previous": "",
        "pending": "",
        "status": "blocked",
        "last_error": "candidate_start_failed: fixture failure",
    }
    assert not (tmp_path / "launcher.target").exists()

    controller.fail = ""
    result = ActivationCoordinator(registry, controller).activate(SHA_B)

    assert result["status"] == "activated"
    assert registry.read()["active"] == SHA_B


def test_interrupted_first_activation_clears_pending_before_retry(tmp_path):
    _candidate(tmp_path, SHA_B)
    _write_state(tmp_path, active="", pending=SHA_B, status="pending")
    registry = SlotRegistry(tmp_path)
    controller = FakeController()

    with pytest.raises(ActivationBlocked) as failure:
        ActivationCoordinator(registry, controller).activate(SHA_B)

    assert failure.value.code == "pending_without_previous"
    assert registry.read()["status"] == "blocked"
    assert registry.read()["pending"] == ""
    assert ActivationCoordinator(registry, controller).activate(SHA_B)["status"] == "activated"


def test_interrupted_pending_is_recovered_before_retry(tmp_path):
    _candidate(tmp_path, SHA_A)
    _candidate(tmp_path, SHA_B)
    _candidate(tmp_path, SHA_C)
    _write_state(tmp_path, active=SHA_A, pending=SHA_B, status="pending")
    registry = SlotRegistry(tmp_path)
    controller = FakeController()

    result = ActivationCoordinator(registry, controller).activate(SHA_C)

    assert result["status"] == "activated"
    assert registry.read()["active"] == SHA_C
    assert controller.calls[:2] == [("start", SHA_A), ("ready", SHA_A)]
    assert ("start", SHA_C) in controller.calls


def test_unknown_or_irreversible_candidate_is_rejected_before_stopping_current(tmp_path):
    _candidate(tmp_path, SHA_A)
    _candidate(tmp_path, SHA_B, reversible=False)
    _write_state(tmp_path, active=SHA_A)
    controller = FakeController(SHA_A)

    with pytest.raises(ActivationBlocked, match="不可逆"):
        ActivationCoordinator(SlotRegistry(tmp_path), controller).activate(SHA_B)

    assert controller.calls == []
    assert SlotRegistry(tmp_path).read()["active"] == SHA_A


def test_registry_refuses_unknown_fields_and_keeps_two_slot_protection(tmp_path):
    _candidate(tmp_path, SHA_A)
    _candidate(tmp_path, SHA_B)
    _candidate(tmp_path, SHA_C)
    _write_state(tmp_path, active=SHA_A, previous=SHA_B)
    (tmp_path / "active.json").write_text(
        json.dumps({"schema": 1, "active": SHA_A, "previous": SHA_B,
                    "pending": "", "status": "stable", "last_error": "", "extra": 1}),
        encoding="utf-8",
    )
    with pytest.raises(ActivationBlocked, match="字段集合"):
        SlotRegistry(tmp_path).read()

    _write_state(tmp_path, active=SHA_A, previous=SHA_B)
    registry = SlotRegistry(tmp_path)
    assert registry.protected_slots() == {SHA_A, SHA_B}
    assert [path.name for path in registry.unreferenced_slots()] == [SHA_C]


def test_already_active_is_idempotent(tmp_path):
    _candidate(tmp_path, SHA_A)
    _write_state(tmp_path, active=SHA_A)
    controller = FakeController(SHA_A)

    result = ActivationCoordinator(SlotRegistry(tmp_path), controller).activate(SHA_A)

    assert result["status"] == "already_active"
    assert controller.calls == []


def test_packaged_worker_readiness_comes_from_candidate_http_projection(monkeypatch):
    identity = ApplicationIdentity(SHA_B, SHA_B, "d" * 32)
    controller = SubprocessGenerationController()

    def candidate_json(path: str):
        if path == "health":
            return {
                "status": "ok",
                "core_ready": True,
                "build_sha": identity.build_sha,
                "slot_id": identity.slot_id,
                "runtime_generation": identity.runtime_generation,
            }
        assert path == "settings/runtime"
        return {
            "worker": {
                "available": True,
                "pid": 42,
                "build_sha": identity.build_sha,
                "slot_id": identity.slot_id,
                "runtime_generation": identity.runtime_generation,
            },
        }

    monkeypatch.setattr(controller, "_json", candidate_json)

    health = controller.wait_ready(_Generation(SHA_B), identity, 0.2)

    assert health["build_sha"] == SHA_B


def test_packaged_controller_marks_activation_generation_as_detached(monkeypatch, tmp_path):
    slot = _candidate(tmp_path, SHA_B)
    identity = ApplicationIdentity(SHA_B, SHA_B, "d" * 32)
    captured = {}

    class Process:
        pass

    def launch(command, **kwargs):
        captured["command"] = command
        captured["environment"] = kwargs["env"]
        return Process()

    monkeypatch.setenv("QM_WINDOWS_APP_JOB_ROOT", "old-root")
    monkeypatch.setattr(activation.subprocess, "Popen", launch)

    result = SubprocessGenerationController().start_generation(slot, identity)

    assert isinstance(result, Process)
    assert captured["command"] == [str(slot / "QuantMaster.exe"), "serve"]
    assert captured["environment"][activation.DETACHED_ACTIVATION_ENV] == "1"
    assert "QM_WINDOWS_APP_JOB_ROOT" not in captured["environment"]


def test_packaged_controller_confirms_candidate_port_is_released(monkeypatch):
    identity = ApplicationIdentity(SHA_B, SHA_B, "d" * 32)
    controller = SubprocessGenerationController()
    health = {
        "build_sha": SHA_B,
        "slot_id": SHA_B,
        "process_pid": 123,
    }
    responses = iter([health, None])
    kill_calls = []
    monkeypatch.setattr(controller, "_health", lambda: next(responses))
    monkeypatch.setattr(activation.os, "name", "nt")
    monkeypatch.setattr(
        activation.subprocess,
        "run",
        lambda command, **kwargs: kill_calls.append((command, kwargs)),
    )

    class Process:
        pid = 123

        def poll(self):
            return 0

    controller.stop_generation(Process(), identity, 0.2)
    assert kill_calls[0][0] == ["taskkill", "/PID", "123", "/T", "/F"]


def test_packaged_controller_blocks_when_candidate_still_owns_port(monkeypatch):
    identity = ApplicationIdentity(SHA_B, SHA_B, "d" * 32)
    controller = SubprocessGenerationController()
    monkeypatch.setattr(activation.subprocess, "run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(controller, "_health", lambda: {
        "build_sha": SHA_B,
        "slot_id": SHA_B,
        "process_pid": 123,
    })

    class Process:
        def poll(self):
            return 0

    with pytest.raises(ActivationBlocked) as exc_info:
        controller.stop_generation(Process(), identity, 0.1)

    assert exc_info.value.code == "candidate_cleanup_failed"


def test_packaged_controller_releases_the_drain_lease(monkeypatch, tmp_path):
    identity = ApplicationIdentity(SHA_A, SHA_A, "d" * 32)
    controller = SubprocessGenerationController()
    calls = []
    worker_root = tmp_path / "active-data"

    def current_json(path):
        if path == "health":
            return {
                "build_sha": identity.build_sha,
                "slot_id": identity.slot_id,
                "runtime_generation": identity.runtime_generation,
            }
        assert path == "settings"
        return {"data": {"root": str(worker_root)}}

    monkeypatch.setattr(controller, "_json", current_json)

    def command(operation, payload, **kwargs):
        calls.append((
            operation, payload, kwargs["application_identity"], kwargs["timeout"], kwargs["root"],
        ))
        if operation == "maintenance.status":
            return {"state": "running"}
        return {"token": "lease"} if operation == "maintenance.enter" else {"released": True}

    monkeypatch.setattr("quantmaster.runtime.worker_ipc.call_worker_command", command)

    controller.drain_current(15.0)
    controller.resume_current(15.0)

    assert calls == [
        ("maintenance.status", {"token": ""}, identity, 0.5, worker_root),
        (
            "maintenance.enter",
            {"reason": "application activation", "timeout": 10.0},
            identity,
            15.0,
            worker_root,
        ),
        ("maintenance.exit", {"token": "lease"}, identity, 10.0, worker_root),
    ]


def test_packaged_controller_reuses_a_confirmed_activation_drain(monkeypatch, tmp_path):
    identity = ApplicationIdentity(SHA_A, SHA_A, "d" * 32)
    controller = SubprocessGenerationController()
    calls = []
    worker_root = tmp_path / "active-data"

    def current_json(path):
        if path == "health":
            return {
                "build_sha": identity.build_sha,
                "slot_id": identity.slot_id,
                "runtime_generation": identity.runtime_generation,
            }
        assert path == "settings"
        return {"data": {"root": str(worker_root)}}

    monkeypatch.setattr(controller, "_json", current_json)

    def command(operation, payload, **kwargs):
        calls.append((
            operation, payload, kwargs["application_identity"], kwargs["timeout"], kwargs["root"],
        ))
        assert operation == "maintenance.status"
        return {"state": "frozen", "reason": "application activation"}

    monkeypatch.setattr("quantmaster.runtime.worker_ipc.call_worker_command", command)

    controller.drain_current(15.0)
    controller.resume_current(15.0)

    assert calls == [
        ("maintenance.status", {"token": ""}, identity, 0.5, worker_root),
    ]


def test_packaged_controller_retries_an_inherited_activation_drain(monkeypatch, tmp_path):
    identity = ApplicationIdentity(SHA_A, SHA_A, "d" * 32)
    controller = SubprocessGenerationController()
    calls = []
    worker_root = tmp_path / "active-data"
    status_attempts = 0

    def current_json(path):
        if path == "health":
            return {
                "build_sha": identity.build_sha,
                "slot_id": identity.slot_id,
                "runtime_generation": identity.runtime_generation,
            }
        assert path == "settings"
        return {"data": {"root": str(worker_root)}}

    monkeypatch.setattr(controller, "_json", current_json)
    monkeypatch.setattr(activation, "WORKER_DRAIN_RETRY_DELAY_SECONDS", 0.0)

    def command(operation, payload, **kwargs):
        nonlocal status_attempts
        calls.append((
            operation, payload, kwargs["application_identity"], kwargs["timeout"], kwargs["root"],
        ))
        assert operation == "maintenance.status"
        status_attempts += 1
        if status_attempts == 1:
            raise RuntimeError("worker status reply lost")
        return {"state": "frozen", "reason": "application activation"}

    monkeypatch.setattr("quantmaster.runtime.worker_ipc.call_worker_command", command)

    controller.drain_current(15.0)

    assert calls == [
        ("maintenance.status", {"token": ""}, identity, 0.5, worker_root),
        ("maintenance.status", {"token": ""}, identity, 0.5, worker_root),
    ]


def test_packaged_controller_reconciles_an_activation_drain_after_lost_enter_reply(monkeypatch, tmp_path):
    identity = ApplicationIdentity(SHA_A, SHA_A, "d" * 32)
    controller = SubprocessGenerationController()
    calls = []
    worker_root = tmp_path / "active-data"
    status_attempts = 0

    def current_json(path):
        if path == "health":
            return {
                "build_sha": identity.build_sha,
                "slot_id": identity.slot_id,
                "runtime_generation": identity.runtime_generation,
            }
        assert path == "settings"
        return {"data": {"root": str(worker_root)}}

    monkeypatch.setattr(controller, "_json", current_json)
    monkeypatch.setattr(activation, "WORKER_DRAIN_RETRY_DELAY_SECONDS", 0.0)

    def command(operation, payload, **kwargs):
        nonlocal status_attempts
        calls.append((
            operation, payload, kwargs["application_identity"], kwargs["timeout"], kwargs["root"],
        ))
        if operation == "maintenance.status":
            status_attempts += 1
            if status_attempts == 1:
                return {"state": "running"}
            if status_attempts < 4:
                raise RuntimeError("worker status reply lost")
            return {"state": "frozen", "reason": "application activation"}
        assert operation == "maintenance.enter"
        raise RuntimeError("reply lost after maintenance entered")

    monkeypatch.setattr("quantmaster.runtime.worker_ipc.call_worker_command", command)

    controller.drain_current(15.0)

    assert calls == [
        ("maintenance.status", {"token": ""}, identity, 0.5, worker_root),
        (
            "maintenance.enter",
            {"reason": "application activation", "timeout": 10.0},
            identity,
            15.0,
            worker_root,
        ),
        ("maintenance.status", {"token": ""}, identity, 0.5, worker_root),
        ("maintenance.status", {"token": ""}, identity, 0.5, worker_root),
        ("maintenance.status", {"token": ""}, identity, 0.5, worker_root),
    ]


def test_packaged_controller_labels_an_unconfirmed_drain(monkeypatch, tmp_path):
    identity = ApplicationIdentity(SHA_A, SHA_A, "d" * 32)
    controller = SubprocessGenerationController()
    worker_root = tmp_path / "active-data"

    def current_json(path):
        if path == "health":
            return {
                "build_sha": identity.build_sha,
                "slot_id": identity.slot_id,
                "runtime_generation": identity.runtime_generation,
            }
        assert path == "settings"
        return {"data": {"root": str(worker_root)}}

    monkeypatch.setattr(controller, "_json", current_json)
    monkeypatch.setattr(activation, "WORKER_DRAIN_RECONCILE_TIMEOUT_SECONDS", 0.0)

    def command(operation, _payload, **_kwargs):
        if operation == "maintenance.status":
            return {"state": "running"}
        assert operation == "maintenance.enter"
        raise RuntimeError("maintenance enter reply lost")

    monkeypatch.setattr("quantmaster.runtime.worker_ipc.call_worker_command", command)

    with pytest.raises(ActivationBlocked) as exc_info:
        controller.drain_current(15.0)

    assert exc_info.value.code == "worker_drain_unconfirmed"
    assert exc_info.value.context == {"phase": "maintenance_status_reconcile"}


def test_packaged_controller_rejects_a_non_activation_maintenance_freeze(monkeypatch, tmp_path):
    identity = ApplicationIdentity(SHA_A, SHA_A, "d" * 32)
    controller = SubprocessGenerationController()
    calls = []
    worker_root = tmp_path / "active-data"

    def current_json(path):
        if path == "health":
            return {
                "build_sha": identity.build_sha,
                "slot_id": identity.slot_id,
                "runtime_generation": identity.runtime_generation,
            }
        assert path == "settings"
        return {"data": {"root": str(worker_root)}}

    monkeypatch.setattr(controller, "_json", current_json)

    def command(operation, payload, **kwargs):
        calls.append((operation, payload, kwargs["root"]))
        assert operation == "maintenance.status"
        return {"state": "frozen", "reason": "database migration"}

    monkeypatch.setattr("quantmaster.runtime.worker_ipc.call_worker_command", command)

    with pytest.raises(ActivationBlocked, match="非更新维护"):
        controller.drain_current(15.0)

    assert calls == [("maintenance.status", {"token": ""}, worker_root)]


def test_packaged_controller_requires_current_worker_root(monkeypatch):
    identity = ApplicationIdentity(SHA_A, SHA_A, "d" * 32)
    controller = SubprocessGenerationController()
    calls = []

    monkeypatch.setattr(controller, "_json", lambda _path: {
        "build_sha": identity.build_sha,
        "slot_id": identity.slot_id,
        "runtime_generation": identity.runtime_generation,
    })
    monkeypatch.setattr(
        "quantmaster.runtime.worker_ipc.call_worker_command",
        lambda *_args, **_kwargs: calls.append(True),
    )

    with pytest.raises(ActivationBlocked, match="数据根"):
        controller.drain_current(15.0)

    assert calls == []


def test_packaged_controller_inherits_drained_worker_root(monkeypatch, tmp_path):
    slot = _candidate(tmp_path, SHA_B)
    identity = ApplicationIdentity(SHA_B, SHA_B, "d" * 32)
    controller = SubprocessGenerationController()
    worker_root = tmp_path / "active-data"
    captured = {}

    class Process:
        pass

    def launch(_command, **kwargs):
        captured["environment"] = kwargs["env"]
        return Process()

    controller._drain_root = worker_root
    monkeypatch.setattr(activation.subprocess, "Popen", launch)

    controller.start_generation(slot, identity)

    assert captured["environment"]["QM_DATA_ROOT"] == str(worker_root)
