"""Activate a verified immutable QuantMaster slot as one application generation."""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast

from quantmaster.runtime.identity import ApplicationIdentity

FULL_SHA = re.compile(r"[0-9a-f]{40}")
GENERATION = re.compile(r"[0-9a-f]{32}")
REGISTRY_SCHEMA = 1
STAGE_SCHEMA = 1
STAGE_MARKER = ".quantmaster-stage.json"
LIFECYCLE_LOCK = ".lifecycle.lock"
LAUNCHER_TARGET = "launcher.target"
RECOVERY_HANDOFF_MARKER = ".schema-handoff.json"
DETACHED_ACTIVATION_ENV = "QM_ACTIVATION_DETACHED"
READY_TIMEOUT_SECONDS = 15.0
ROLLBACK_TIMEOUT_SECONDS = 15.0
WORKER_DRAIN_TIMEOUT_SECONDS = 10.0
WORKER_DRAIN_RECONCILE_TIMEOUT_SECONDS = 2.0
WORKER_DRAIN_STATUS_TIMEOUT_SECONDS = 0.5
WORKER_DRAIN_RETRY_DELAY_SECONDS = 0.05
_VALID_STATUSES = frozenset({"empty", "stable", "pending", "rolled_back", "blocked"})


class ActivationBlocked(RuntimeError):
    """A fail-closed activation decision with a stable reason code."""

    def __init__(self, code: str, detail: str, **context: object) -> None:
        super().__init__(detail)
        self.code = str(code)
        self.detail = str(detail)
        self.context = context


_ACTIVATION_FAILURES = (
    ActivationBlocked,
    OSError,
    subprocess.SubprocessError,
    TimeoutError,
    ValueError,
    TypeError,
    UnicodeError,
)


@dataclass(frozen=True)
class Candidate:
    build_sha: str
    slot_id: str
    slot: Path


def _require_sha(value: object, *, label: str) -> str:
    result = str(value or "")
    if FULL_SHA.fullmatch(result) is None:
        raise ActivationBlocked("invalid_sha", f"{label} 不是完整 lowercase Git SHA")
    return result


def _is_link(path: Path) -> bool:
    return path.is_symlink() or bool(getattr(path, "is_junction", lambda: False)())


def _empty_state() -> dict[str, object]:
    return {
        "schema": REGISTRY_SCHEMA,
        "active": "",
        "previous": "",
        "pending": "",
        "status": "empty",
        "last_error": "",
    }


def _validate_state(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ActivationBlocked("registry_invalid", "active.json 必须是 JSON object")
    _validate_state_shape(value)
    result = dict(value)
    _validate_state_slots(result)
    return result


def _validate_state_shape(value: Mapping[str, object]) -> None:
    expected = {"schema", "active", "previous", "pending", "status", "last_error"}
    if set(value) != expected:
        raise ActivationBlocked("registry_invalid", "active.json 字段集合不受支持")
    if value["schema"] != REGISTRY_SCHEMA:
        raise ActivationBlocked("registry_schema_unknown", "active.json schema 不受支持")
    status = value["status"]
    if status not in _VALID_STATUSES:
        raise ActivationBlocked("registry_invalid", "active.json status 不受支持")
    if not isinstance(value["last_error"], str) or len(value["last_error"]) > 500:
        raise ActivationBlocked("registry_invalid", "active.json last_error 无效")


def _validate_state_slots(result: dict[str, object]) -> None:
    for name in ("active", "previous", "pending"):
        raw = result[name]
        if raw is None:
            raw = ""
        if not isinstance(raw, str) or (raw and FULL_SHA.fullmatch(raw) is None):
            raise ActivationBlocked("registry_invalid", f"active.json {name} 不是完整 SHA")
        result[name] = raw
    if result["status"] == "pending" and not result["pending"]:
        raise ActivationBlocked("registry_invalid", "pending 状态缺少候选 SHA")
    if result["status"] != "pending" and result["pending"]:
        raise ActivationBlocked("registry_invalid", "非 pending 状态不能保留候选 SHA")
    if result["active"] and result["active"] == result["previous"]:
        raise ActivationBlocked("registry_invalid", "active 与 previous 不能相同")


def _json_bytes(value: Mapping[str, object]) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _atomic_bytes(path: Path, value: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("wb") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def lifecycle_lock(app_root: Path):
    """Hold the same lock used by staging so activation cannot race staging."""

    app_root = app_root.resolve()
    app_root.mkdir(parents=True, exist_ok=True)
    lock_path = app_root / LIFECYCLE_LOCK
    if _is_link(lock_path) or (lock_path.exists() and not lock_path.is_file()):
        raise ActivationBlocked("unsafe_lifecycle_lock", "生命周期锁不是普通文件")
    with lock_path.open("a+b") as stream:
        stream.seek(0, 2)
        if stream.tell() == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        locked = False
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(  # type: ignore[attr-defined]
                    stream.fileno(), msvcrt.LK_NBLCK, 1  # type: ignore[attr-defined]
                )
            else:
                fcntl = cast(Any, __import__("fcntl"))

                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = True
        except (BlockingIOError, OSError) as exc:
            raise ActivationBlocked("lifecycle_busy", "另一个 staging/activation 操作正在进行") from exc
        try:
            yield
        finally:
            if locked:
                if os.name == "nt":
                    import msvcrt

                    stream.seek(0)
                    msvcrt.locking(  # type: ignore[attr-defined]
                        stream.fileno(), msvcrt.LK_UNLCK, 1  # type: ignore[attr-defined]
                    )
                else:
                    fcntl = cast(Any, __import__("fcntl"))

                    fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


class SlotRegistry:
    """Strict active/previous/pending registry with atomic file commits."""

    def __init__(self, app_root: str | Path, *, launcher_target: str | Path | None = None) -> None:
        self.app_root = Path(app_root).resolve()
        self.slots = self.app_root / "slots"
        self.active_path = self.app_root / "active.json"
        self.launcher_target = Path(launcher_target).resolve() if launcher_target else (
            self.app_root / LAUNCHER_TARGET
        )
        self.handoff_path = self.app_root / RECOVERY_HANDOFF_MARKER
        if not self.launcher_target.is_relative_to(self.app_root):
            raise ActivationBlocked("unsafe_launcher_target", "launcher target 必须位于应用根目录")

    def _check_root(self) -> None:
        for path in (self.app_root, self.slots):
            if path.exists() and _is_link(path):
                raise ActivationBlocked("unsafe_slot_root", "槽根目录不能是 link/junction")

    def read(self) -> dict[str, object]:
        self._check_root()
        if not self.active_path.exists():
            return _empty_state()
        if _is_link(self.active_path) or not self.active_path.is_file():
            raise ActivationBlocked("registry_invalid", "active.json 不是普通文件")
        try:
            value = json.loads(self.active_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ActivationBlocked("registry_invalid", "active.json 不可读") from exc
        return _validate_state(value)

    def slot(self, build_sha: str) -> Path:
        sha = _require_sha(build_sha, label="候选槽")
        self._check_root()
        return self.slots / sha

    def _write_state(self, state: Mapping[str, object]) -> None:
        self.app_root.mkdir(parents=True, exist_ok=True)
        _atomic_bytes(self.active_path, _json_bytes(_validate_state(dict(state))))

    def _commit_pair(self, state: Mapping[str, object], target_sha: str) -> None:
        state_bytes = _json_bytes(_validate_state(dict(state)))
        target_bytes = (target_sha + "\n").encode("ascii")
        self.app_root.mkdir(parents=True, exist_ok=True)
        old_state = self.active_path.read_bytes() if self.active_path.exists() else None
        old_target = self.launcher_target.read_bytes() if self.launcher_target.exists() else None
        state_tmp = self.active_path.with_name(f".{self.active_path.name}.{uuid.uuid4().hex}.tmp")
        target_tmp = self.launcher_target.with_name(
            f".{self.launcher_target.name}.{uuid.uuid4().hex}.tmp"
        )
        try:
            state_tmp.write_bytes(state_bytes)
            target_tmp.write_bytes(target_bytes)
            os.replace(state_tmp, self.active_path)
            try:
                os.replace(target_tmp, self.launcher_target)
            except OSError:
                if old_state is None:
                    self.active_path.unlink(missing_ok=True)
                else:
                    _atomic_bytes(self.active_path, old_state)
                raise
        finally:
            state_tmp.unlink(missing_ok=True)
            target_tmp.unlink(missing_ok=True)
        if old_target is not None:
            # The old target is intentionally not restored after a successful commit.
            del old_target

    def validate_candidate(self, build_sha: str) -> Candidate:
        sha = _require_sha(build_sha, label="候选槽")
        slot = self.slot(sha)
        if not slot.is_dir() or _is_link(slot):
            raise ActivationBlocked("candidate_missing", "候选槽不存在或不是普通目录")
        marker = slot / STAGE_MARKER
        if not marker.is_file() or _is_link(marker):
            raise ActivationBlocked("candidate_unstaged", "候选槽缺少完整 staging marker")
        payload = _read_stage_marker(marker)
        _validate_candidate_marker(payload, sha)
        _validate_candidate_executable(slot)
        return Candidate(sha, sha, slot)

    def begin(self, candidate: str) -> dict[str, object]:
        state = self.read()
        if state["pending"]:
            raise ActivationBlocked("pending_recovery_required", "已有未完成的 activation pending")
        if candidate == state["active"]:
            return state
        pending = {
            "schema": REGISTRY_SCHEMA,
            "active": state["active"],
            "previous": state["previous"],
            "pending": candidate,
            "status": "pending",
            "last_error": "",
        }
        self._write_state(pending)
        return pending

    def commit(self, candidate: str) -> dict[str, object]:
        state = self.read()
        if state["status"] != "pending" or state["pending"] != candidate:
            raise ActivationBlocked("pending_mismatch", "候选槽不再是当前 pending activation")
        committed = {
            "schema": REGISTRY_SCHEMA,
            "active": candidate,
            "previous": state["active"],
            "pending": "",
            "status": "stable",
            "last_error": "",
        }
        self._commit_pair(committed, candidate)
        self._clear_handoff(candidate)
        return committed

    def _handoff_candidate(self) -> str:
        if not self.handoff_path.exists():
            return ""
        if _is_link(self.handoff_path) or not self.handoff_path.is_file():
            raise ActivationBlocked("recovery_handoff_invalid", "schema handoff 标记不是普通文件")
        try:
            value = json.loads(self.handoff_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ActivationBlocked("recovery_handoff_invalid", "schema handoff 标记不可读") from exc
        if (
            not isinstance(value, Mapping)
            or set(value) != {"schema", "candidate", "previous"}
            or value.get("schema") != 1
            or FULL_SHA.fullmatch(str(value.get("candidate") or "")) is None
            or (value.get("previous") and FULL_SHA.fullmatch(str(value["previous"])) is None)
        ):
            raise ActivationBlocked("recovery_handoff_invalid", "schema handoff 标记无效")
        return str(value["candidate"])

    def begin_schema_handoff(self, candidate: str, previous: str) -> None:
        if self._handoff_candidate():
            raise ActivationBlocked("recovery_handoff_pending", "已有未完成的 schema handoff")
        _atomic_bytes(self.handoff_path, _json_bytes({
            "schema": 1,
            "candidate": _require_sha(candidate, label="候选槽"),
            "previous": str(previous or ""),
        }))
        if _is_link(self.launcher_target):
            raise ActivationBlocked("unsafe_launcher_target", "launcher target 不能是 link/junction")
        self.launcher_target.unlink(missing_ok=True)

    def _clear_handoff(self, candidate: str) -> None:
        if self._handoff_candidate() == candidate:
            self.handoff_path.unlink(missing_ok=True)

    def pending_schema_handoff(self, candidate: str) -> bool:
        return self._handoff_candidate() == candidate

    def rollback(self, error: str) -> dict[str, object]:
        state = self.read()
        fallback = str(state["active"] or state["previous"] or "")
        rolled_back = {
            "schema": REGISTRY_SCHEMA,
            "active": fallback,
            "previous": state["previous"] if fallback == state["active"] else "",
            "pending": "",
            "status": "rolled_back",
            "last_error": str(error)[:500],
        }
        if fallback:
            self._commit_pair(rolled_back, fallback)
        else:
            self._write_state(rolled_back)
        return rolled_back

    def mark_blocked(self, error: str) -> dict[str, object]:
        state = self.read()
        blocked = {
            **state,
            "pending": "",
            "status": "blocked",
            "last_error": str(error)[:500],
        }
        self._write_state(blocked)
        return blocked

    def protected_slots(self) -> frozenset[str]:
        state = self.read()
        return frozenset(str(state[name]) for name in ("active", "previous", "pending") if state[name])

    def unreferenced_slots(self) -> list[Path]:
        protected = self.protected_slots()
        if not self.slots.is_dir() or _is_link(self.slots):
            return []
        return sorted(
            (path for path in self.slots.iterdir() if path.is_dir() and not _is_link(path)
             and FULL_SHA.fullmatch(path.name) and path.name not in protected),
            key=lambda path: path.name,
        )


def _read_stage_marker(marker: Path) -> Mapping[str, object]:
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ActivationBlocked("candidate_invalid", "staging marker 不可读") from exc
    if not isinstance(payload, Mapping):
        raise ActivationBlocked("candidate_invalid", "staging marker 必须是 JSON object")
    return payload


def _validate_candidate_marker(payload: Mapping[str, object], sha: str) -> None:
    if payload.get("schema") != STAGE_SCHEMA:
        raise ActivationBlocked("candidate_schema_unknown", "候选槽 schema 不受支持")
    if payload.get("status") != "staged":
        raise ActivationBlocked("candidate_invalid", "候选槽不是 staged 状态")
    if payload.get("build_sha") != sha or payload.get("slot_id") != sha:
        raise ActivationBlocked("candidate_identity_mismatch", "候选槽身份与目录 SHA 不一致")
    migration = payload.get("migration")
    if migration is True or (
        isinstance(migration, Mapping) and migration.get("reversible") is False
    ) or payload.get("schema_compatibility") == "irreversible":
        raise ActivationBlocked("irreversible_schema", "候选槽包含不可逆 schema 迁移")
    size = payload.get("size")
    if not isinstance(size, Mapping) or size.get("within_hard_limits") is not True:
        raise ActivationBlocked("package_gate_missing", "候选槽缺少通过 package hard limit 的证据")
    smoke = payload.get("smoke")
    if not isinstance(smoke, Mapping):
        raise ActivationBlocked("smoke_missing", "候选槽缺少 packaged smoke 证据")
    if smoke.get("build_sha") != sha or smoke.get("slot_id") != sha:
        raise ActivationBlocked("candidate_identity_mismatch", "packaged smoke 身份不匹配")


def _validate_candidate_executable(slot: Path) -> None:
    executable = slot / "QuantMaster.exe"
    if not executable.is_file() or _is_link(executable):
        raise ActivationBlocked("candidate_invalid", "候选槽缺少普通 QuantMaster.exe")


class GenerationController(Protocol):
    def current_identity(self) -> Mapping[str, object] | None: ...

    def drain_current(self, timeout: float) -> None: ...

    def resume_current(self, timeout: float) -> None: ...

    def stop_current(self, timeout: float) -> None: ...

    def start_generation(self, slot: Path, identity: ApplicationIdentity) -> object: ...

    def wait_ready(
        self, generation: object, identity: ApplicationIdentity, timeout: float,
    ) -> Mapping[str, object]: ...

    def stop_generation(
        self,
        generation: object,
        identity: ApplicationIdentity,
        timeout: float,
    ) -> None: ...


def _mapping_identity(value: Mapping[str, object] | None) -> tuple[str, str] | None:
    if value is None:
        return None
    return str(value.get("build_sha") or ""), str(value.get("slot_id") or "")


class SubprocessGenerationController:
    """Windows controller used by the packaged helper; tests can inject a fake controller."""

    def __init__(self, *, root_pid: int | None = None, host: str = "127.0.0.1", port: int = 8686) -> None:
        raw_pid = root_pid or os.environ.get("QM_WINDOWS_APP_JOB_ROOT", "")
        self.root_pid = int(raw_pid) if str(raw_pid).isdigit() else None
        self.host = host
        self.port = int(port)
        self._drain_identity: ApplicationIdentity | None = None
        self._drain_token = ""
        self._drain_root: Path | None = None

    def _json(self, path: str) -> Mapping[str, object] | None:
        try:
            with urllib.request.urlopen(
                f"http://{self.host}:{self.port}/api/v1/{path}", timeout=0.5,
            ) as response:
                value = json.loads(response.read().decode("utf-8"))
        except (OSError, ValueError, TypeError, urllib.error.URLError):
            return None
        return value if isinstance(value, Mapping) else None

    def _health(self) -> Mapping[str, object] | None:
        return self._json("health")

    def current_identity(self) -> Mapping[str, object] | None:
        return self._health()

    def _current_worker_root(self) -> Path:
        """Read the active generation's IPC root before replacing it."""

        settings = self._json("settings")
        data = settings.get("data") if settings is not None else None
        raw_root = data.get("root") if isinstance(data, Mapping) else None
        root = Path(raw_root) if isinstance(raw_root, str) and raw_root else None
        if root is None or not root.is_absolute():
            raise ActivationBlocked(
                "worker_root_unavailable", "无法确认当前 runtime-worker 数据根",
            )
        return root

    def _reuse_existing_activation_drain(
        self,
        identity: ApplicationIdentity,
        worker_root: Path,
        timeout: float,
        *,
        wait_for_activation_drain: bool = False,
    ) -> bool:
        """Reuse a confirmed update drain before attempting to enter maintenance.

        A lost ``maintenance.enter`` reply can leave the worker frozen while its
        serial command server finishes the first request.  Reconcile that
        short window before treating the update as unable to drain the worker.
        """

        from quantmaster.runtime.worker_ipc import call_worker_command

        deadline = time.monotonic() + min(
            max(0.05, float(timeout)), WORKER_DRAIN_RECONCILE_TIMEOUT_SECONDS,
        )
        while True:
            status: Mapping[str, object] | None
            try:
                status = call_worker_command(
                    "maintenance.status",
                    {"token": ""},
                    timeout=min(float(timeout), WORKER_DRAIN_STATUS_TIMEOUT_SECONDS),
                    root=worker_root,
                    application_identity=identity,
                )
            except (OSError, RuntimeError, ValueError, TypeError):
                status = None
            if status is not None and status.get("state") == "frozen":
                if status.get("reason") != "application activation":
                    raise ActivationBlocked(
                        "worker_maintenance_active", "当前 runtime-worker 正在非更新维护冻结",
                    )
                self._drain_identity = identity
                self._drain_token = ""
                self._drain_root = worker_root
                return True
            if status is not None and not wait_for_activation_drain:
                return False
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(WORKER_DRAIN_RETRY_DELAY_SECONDS, remaining))

    def drain_current(self, timeout: float) -> None:
        current = self.current_identity()
        if current is None:
            return
        from quantmaster.runtime.identity import ApplicationIdentity
        from quantmaster.runtime.worker_ipc import call_worker_command

        drain_timeout = min(timeout, WORKER_DRAIN_TIMEOUT_SECONDS)
        identity = ApplicationIdentity(
            str(current.get("build_sha") or ""),
            str(current.get("slot_id") or ""),
            str(current.get("runtime_generation") or ""),
        )
        worker_root = self._current_worker_root()
        if self._reuse_existing_activation_drain(identity, worker_root, timeout):
            return
        try:
            result = call_worker_command(
                "maintenance.enter",
                {"reason": "application activation", "timeout": drain_timeout},
                timeout=timeout,
                root=worker_root,
                application_identity=identity,
            )
        except (OSError, RuntimeError, ValueError, TypeError) as exc:
            if self._reuse_existing_activation_drain(
                identity, worker_root, timeout, wait_for_activation_drain=True,
            ):
                return
            raise ActivationBlocked(
                "worker_drain_unconfirmed",
                "当前 runtime-worker 排空状态未确认",
                phase="maintenance_status_reconcile",
            ) from exc
        self._drain_identity = identity
        self._drain_token = str(result.get("token") or "")
        self._drain_root = worker_root

    def resume_current(self, timeout: float) -> None:
        identity, token, worker_root = self._drain_identity, self._drain_token, self._drain_root
        if identity is None or not token:
            return
        from quantmaster.runtime.worker_ipc import call_worker_command

        try:
            call_worker_command(
                "maintenance.exit",
                {"token": token},
                timeout=min(timeout, WORKER_DRAIN_TIMEOUT_SECONDS),
                root=worker_root,
                application_identity=identity,
            )
        except (OSError, RuntimeError, ValueError, TypeError) as exc:
            raise ActivationBlocked("worker_unavailable", "当前 runtime-worker 无法恢复") from exc
        self._drain_identity = None
        self._drain_token = ""
        self._drain_root = None

    def stop_current(self, timeout: float) -> None:
        from quantmaster.runtime.windows_app import terminate_root_job

        if self.root_pid is None:
            raise ActivationBlocked("current_root_unavailable", "缺少当前应用 root Job Object PID")
        terminate_root_job(self.root_pid)
        deadline = time.monotonic() + max(0.1, timeout)
        while time.monotonic() < deadline:
            if self._health() is None:
                return
            time.sleep(0.05)
        raise ActivationBlocked("current_stop_timeout", "当前应用未在期限内停止")

    def start_generation(self, slot: Path, identity: ApplicationIdentity) -> object:
        executable = slot / "QuantMaster.exe"
        environment = os.environ.copy()
        environment.update({
            "PYINSTALLER_RESET_ENVIRONMENT": "1",
            "QM_BUILD_SHA": identity.build_sha,
            "QM_SLOT_ID": identity.slot_id,
            "QM_RUNTIME_GENERATION": identity.runtime_generation,
            # The activation helper exits after this generation is proven ready.
            # Do not let the Web lifecycle mistake that short-lived helper for a
            # user-facing stable launcher and stop the new generation with it.
            DETACHED_ACTIVATION_ENV: "1",
        })
        if self._drain_root is not None:
            environment["QM_DATA_ROOT"] = str(self._drain_root)
        environment.pop("QM_WINDOWS_APP_JOB_ROOT", None)
        try:
            return subprocess.Popen(
                [str(executable), "serve"],
                cwd=slot,
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except OSError as exc:
            raise ActivationBlocked("candidate_start_failed", "候选槽启动失败") from exc

    def _worker_ready(self, identity: ApplicationIdentity) -> bool:
        runtime = self._json("settings/runtime")
        if runtime is None or not isinstance(runtime.get("worker"), Mapping):
            return False
        value = cast(Mapping[str, object], runtime["worker"])
        return (
            value.get("available") is True
            and value.get("build_sha") == identity.build_sha
            and value.get("slot_id") == identity.slot_id
            and value.get("runtime_generation") == identity.runtime_generation
        )

    def wait_ready(
        self, generation: object, identity: ApplicationIdentity, timeout: float,
    ) -> Mapping[str, object]:
        deadline = time.monotonic() + max(0.1, timeout)
        while time.monotonic() < deadline:
            health = self._health()
            if (
                health is not None
                and health.get("status") == "ok"
                and health.get("core_ready") is True
                and health.get("build_sha") == identity.build_sha
                and health.get("slot_id") == identity.slot_id
                and health.get("runtime_generation") == identity.runtime_generation
                and self._worker_ready(identity)
            ):
                return health
            if getattr(generation, "poll", lambda: None)() is not None:
                break
            time.sleep(0.1)
        raise ActivationBlocked("candidate_not_ready", "候选槽未在 15 秒内完成 Web/runtime/compute 身份检查")

    def stop_generation(
        self,
        generation: object,
        identity: ApplicationIdentity,
        timeout: float,
    ) -> None:
        process: Any = generation
        if getattr(process, "poll", lambda: None)() is not None:
            self._wait_generation_gone(identity, timeout)
            return
        process.terminate()
        try:
            process.wait(timeout=max(0.1, timeout))
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=max(0.1, timeout))
        self._wait_generation_gone(identity, timeout)

    def _wait_generation_gone(self, identity: ApplicationIdentity, timeout: float) -> None:
        """Do not restart another slot while the candidate still owns the port."""

        deadline = time.monotonic() + max(0.1, timeout)
        taskkill_attempted = False
        while time.monotonic() < deadline:
            health = self._health()
            if _mapping_identity(health) != (identity.build_sha, identity.slot_id):
                return
            if not taskkill_attempted and os.name == "nt":
                raw_pid = health.get("process_pid") if health is not None else None
                pid = raw_pid if isinstance(raw_pid, int) else None
                if pid is not None and pid > 0:
                    try:
                        subprocess.run(
                            ["taskkill", "/PID", str(pid), "/T", "/F"],
                            check=False,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                            timeout=max(0.1, timeout),
                        )
                    except (OSError, subprocess.SubprocessError):
                        pass
                    taskkill_attempted = True
            time.sleep(0.05)
        raise ActivationBlocked(
            "candidate_cleanup_failed",
            "候选槽停止后仍占用 Web 端口，已阻止回滚重启旧槽",
        )


class ActivationCoordinator:
    """Run the A→B transition and keep registry state recoverable on failure."""

    def __init__(
        self,
        registry: SlotRegistry,
        controller: GenerationController,
        *,
        ready_timeout: float = READY_TIMEOUT_SECONDS,
        rollback_timeout: float = ROLLBACK_TIMEOUT_SECONDS,
        generation_factory: Callable[[str], ApplicationIdentity] | None = None,
        allow_unrecoverable_current: bool = False,
    ) -> None:
        self.registry = registry
        self.controller = controller
        self.ready_timeout = min(max(0.1, float(ready_timeout)), READY_TIMEOUT_SECONDS)
        self.rollback_timeout = min(max(0.1, float(rollback_timeout)), ROLLBACK_TIMEOUT_SECONDS)
        self.generation_factory = generation_factory or (
            lambda sha: ApplicationIdentity(sha, sha, uuid.uuid4().hex)
        )
        self.allow_unrecoverable_current = bool(allow_unrecoverable_current)
        self._schema_handoff = False
        self._generation_identity: ApplicationIdentity | None = None

    def _failure(self, exc: BaseException) -> str:
        if isinstance(exc, ActivationBlocked):
            phase = exc.context.get("phase")
            suffix = f" [{phase}]" if isinstance(phase, str) and phase else ""
            return f"{exc.code}: {exc.detail}{suffix}"[:500]
        return f"activation_failed: {type(exc).__name__}"[:500]

    def _start_and_wait(self, sha: str, timeout: float) -> tuple[object, Mapping[str, object]]:
        candidate = self.registry.validate_candidate(sha)
        identity = self.generation_factory(candidate.build_sha)
        self._generation_identity = identity
        generation = self.controller.start_generation(candidate.slot, identity)
        try:
            health = self.controller.wait_ready(generation, identity, timeout)
        except _ACTIVATION_FAILURES as wait_exc:
            try:
                self.controller.stop_generation(generation, identity, timeout)
            except _ACTIVATION_FAILURES as cleanup_exc:
                raise cleanup_exc from wait_exc
            raise
        return generation, health

    def _recover_interrupted(self, state: Mapping[str, object]) -> None:
        pending = str(state.get("pending") or "")
        if pending and self.registry.pending_schema_handoff(pending):
            current = self.controller.current_identity()
            if _mapping_identity(current) == (pending, pending):
                self.registry.commit(pending)
                return
            self.registry.mark_blocked(
                "interrupted_schema_handoff: 旧槽已停止；请重新激活已验证候选或从离线备份恢复"
            )
            return
        fallback = str(state.get("active") or state.get("previous") or "")
        if not fallback:
            self.registry.mark_blocked("pending_without_previous: 无可恢复的 previous 槽")
            raise ActivationBlocked("pending_without_previous", "pending activation 没有可恢复的 previous 槽")
        current = self.controller.current_identity()
        if _mapping_identity(current) != (fallback, fallback):
            generation, _health = self._start_and_wait(fallback, self.rollback_timeout)
            del generation
        self.registry.rollback("interrupted_pending")

    def _stop_previous(self, previous: str) -> bool:
        if not previous:
            return False
        if self.controller.current_identity() is None:
            return False
        try:
            self.controller.drain_current(self.ready_timeout)
        except ActivationBlocked as exc:
            if not (
                self.allow_unrecoverable_current
                and exc.code == "worker_drain_unconfirmed"
            ):
                raise
            pending = self.registry.read()
            self.registry.begin_schema_handoff(
                previous=str(pending["active"]),
                candidate=str(pending["pending"]),
            )
            self._schema_handoff = True
        self.controller.stop_current(self.ready_timeout)
        return True

    def _stop_candidate(self, generation: object | None, identity: ApplicationIdentity | None) -> None:
        if generation is None:
            return
        if identity is None:
            raise ActivationBlocked("candidate_identity_missing", "无法确认候选槽身份，拒绝清理")
        self.controller.stop_generation(generation, identity, self.rollback_timeout)

    def _rollback_transition(
        self,
        exc: BaseException,
        *,
        previous: str,
        generation: object | None,
        current_stopped: bool,
    ) -> dict[str, object]:
        failure = self._failure(exc)
        try:
            self._stop_candidate(generation, getattr(self, "_generation_identity", None))
        except _ACTIVATION_FAILURES as cleanup_exc:
            blocked = self.registry.mark_blocked(
                f"candidate_cleanup_failed: {self._failure(cleanup_exc)}; {failure}"
            )
            return {
                "status": "blocked",
                "active": blocked["active"],
                "previous": blocked["previous"],
                "last_error": blocked["last_error"],
            }
        if isinstance(exc, ActivationBlocked) and exc.code == "candidate_cleanup_failed":
            blocked = self.registry.mark_blocked(failure)
            return {
                "status": "blocked",
                "active": blocked["active"],
                "previous": blocked["previous"],
                "last_error": blocked["last_error"],
            }
        if self._schema_handoff:
            blocked = self.registry.mark_blocked(
                "schema_handoff_candidate_failed: " + failure
                + "；旧槽不会重启，请重新激活候选或从离线备份恢复"
            )
            return {
                "status": "blocked",
                "active": blocked["active"],
                "previous": blocked["previous"],
                "last_error": blocked["last_error"],
            }
        if not previous:
            self.registry.mark_blocked(failure)
            if isinstance(exc, ActivationBlocked):
                raise exc
            raise ActivationBlocked("activation_failed", failure) from exc
        try:
            if current_stopped:
                self._start_and_wait(previous, self.rollback_timeout)
            else:
                self.controller.resume_current(self.rollback_timeout)
            committed = self.registry.rollback(failure)
        except _ACTIVATION_FAILURES as rollback_exc:
            self.registry.mark_blocked(
                f"rollback_failed: {self._failure(rollback_exc)}; {failure}"
            )
            raise ActivationBlocked("rollback_failed", "previous 槽无法恢复") from rollback_exc
        return {
            "status": "rolled_back",
            "active": committed["active"],
            "previous": committed["previous"],
            "last_error": committed["last_error"],
        }

    def _run_transition(self, candidate: Candidate, previous: str) -> dict[str, object]:
        self.registry.begin(candidate.build_sha)
        current_stopped = False
        generation: object | None = None
        try:
            current_stopped = self._stop_previous(previous)
            generation, health = self._start_and_wait(candidate.build_sha, self.ready_timeout)
            committed = self.registry.commit(candidate.build_sha)
        except _ACTIVATION_FAILURES as exc:
            return self._rollback_transition(
                exc,
                previous=previous,
                generation=generation,
                current_stopped=current_stopped,
            )
        return {
            "status": "activated",
            "active": committed["active"],
            "previous": committed["previous"],
            "runtime_generation": health.get("runtime_generation", ""),
        }

    def activate(self, build_sha: str) -> dict[str, object]:
        candidate_sha = _require_sha(build_sha, label="候选槽")
        with lifecycle_lock(self.registry.app_root):
            state = self.registry.read()
            if state["pending"]:
                self._recover_interrupted(state)
                state = self.registry.read()
            candidate = self.registry.validate_candidate(candidate_sha)
            if candidate.build_sha == state["active"] and state["status"] in {"stable", "rolled_back"}:
                return {
                    "status": "already_active",
                    "active": candidate.build_sha,
                    "previous": state["previous"],
                }
            return self._run_transition(candidate, str(state["active"] or ""))


def installed_app_root() -> Path:
    local_appdata = os.environ.get("LOCALAPPDATA", "").strip()
    path = Path(local_appdata)
    if not local_appdata or not path.is_absolute():
        raise ActivationBlocked("localappdata_required", "LOCALAPPDATA 必须是绝对路径")
    return path / "QuantMaster" / "app"


def activate_installed_slot(
    build_sha: str, *, root_pid: int | None = None, ready_timeout: float = READY_TIMEOUT_SECONDS,
    recover_unavailable_current: bool = False,
) -> dict[str, object]:
    """Activate one pre-staged installed slot from the packaged helper."""

    registry = SlotRegistry(installed_app_root())
    controller = SubprocessGenerationController(root_pid=root_pid)
    return ActivationCoordinator(
        registry,
        controller,
        ready_timeout=ready_timeout,
        allow_unrecoverable_current=recover_unavailable_current,
    ).activate(build_sha)
