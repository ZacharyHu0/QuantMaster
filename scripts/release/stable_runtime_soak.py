"""Eight-hour acceptance for one immutable stable application generation."""

from __future__ import annotations

import argparse
import http.cookiejar
import json
import os
import re
import socket
import subprocess
import time
import urllib.request
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from quantmaster.runtime.activation import (
    ActivationBlocked,
    ActivationCoordinator,
    SlotRegistry,
    SubprocessGenerationController,
    installed_app_root,
)
from quantmaster.runtime.launcher import stable_launcher_path, stable_slot_executable
from quantmaster.runtime.windows_app import terminate_root_job
from scripts.dev import tasks

_FULL_SHA = re.compile(r"[0-9a-f]{40}")
_GENERATION = re.compile(r"[0-9a-f]{32}")
ROOT = Path(__file__).resolve().parents[2]
SOAK_DURATION_SECONDS = 8 * 60 * 60
HEALTH_INTERVAL_SECONDS = 60
DEVELOPMENT_INTERVAL_SECONDS = 2 * 60 * 60


def _capture_failure(errors: list[Exception], action: Callable[[], object]) -> None:
    try:
        action()
    except Exception as exc:
        errors.append(exc)


def _git_text(cwd: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-c", f"safe.directory={cwd.as_posix()}", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if completed.returncode:
        raise RuntimeError(f"git {' '.join(args)} failed: {completed.stderr.strip()}")
    return completed.stdout.strip()


@dataclass(frozen=True)
class SoakConfig:
    duration_seconds: float
    health_interval_seconds: float
    development_interval_seconds: float
    dev_slugs: tuple[str, str]
    candidate_sha: str
    evidence_path: Path


class SoakSystem(Protocol):
    def start_stable(self) -> None: ...

    def now(self) -> float: ...

    def sleep(self, seconds: float) -> None: ...

    def stable_sample(self, *, compute: bool) -> dict[str, object]: ...

    def exercise_development(
        self, slugs: tuple[str, str], cycle: int,
    ) -> dict[str, object]: ...

    def activate(self, build_sha: str) -> dict[str, object]: ...

    def force_failed_activation(self, build_sha: str) -> dict[str, object]: ...

    def shutdown(self) -> None: ...


class SoakFailure(RuntimeError):
    """The soak failed after its local evidence was persisted."""


class _SoakContractError(RuntimeError):
    def __init__(self, message: str, **context: object) -> None:
        super().__init__(message)
        self.context = context


class _FailFirstReady:
    """One-shot fault adapter; rollback readiness still uses the real controller."""

    def __init__(self, delegate: Any) -> None:
        self.delegate = delegate
        self.failed = False

    def current_identity(self):
        return self.delegate.current_identity()

    def drain_current(self, timeout: float) -> None:
        self.delegate.drain_current(timeout)

    def stop_current(self, timeout: float) -> None:
        self.delegate.stop_current(timeout)

    def start_generation(self, slot: Path, identity: object):
        return self.delegate.start_generation(slot, identity)

    def wait_ready(self, generation: object, identity: object, timeout: float):
        if not self.failed:
            self.failed = True
            raise ActivationBlocked("soak_forced_failure", "soak acceptance forced failure")
        return self.delegate.wait_ready(generation, identity, timeout)

    def stop_generation(self, generation: object, timeout: float) -> None:
        self.delegate.stop_generation(generation, timeout)


def _request_json(url: str, **kwargs: object) -> dict[str, object]:
    request = urllib.request.Request(url, **kwargs)
    with urllib.request.urlopen(request, timeout=2.0) as response:
        value = json.loads(response.read().decode("utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"non-object response from {url}")
    return value


def _request_bytes(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=2.0) as response:
        return response.read()


class WindowsSoakSystem:
    """Real Windows adapter for the stable-use acceptance runner."""

    def __init__(
        self,
        *,
        primary: Path,
        app_root: Path,
        evidence_root: Path,
        base_url: str = "http://127.0.0.1:8686/api/v1",
    ) -> None:
        self.primary = primary.resolve()
        self.app_root = app_root.resolve()
        self.evidence_root = evidence_root.resolve()
        self.base_url = base_url.rstrip("/")
        self.web_url = self.base_url.rsplit("/api/v1", 1)[0] + "/"
        self.stable_instance = self.evidence_root / "stable-instance"
        self.stable_config = self.stable_instance / "config.yaml"
        self.stable_data = self.stable_instance / "data"
        self.stable_stockdb = self.stable_instance / "free-stockdb"
        self._stable_shell: bytes | None = None
        self._stable_env: dict[str, str] | None = None
        self._owned_development: list[dict[str, object]] = []
        self._stable_process: dict[str, object] | None = None
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar())
        )

    def now(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def _stable_launcher(self) -> Path:
        try:
            socket.create_connection(("127.0.0.1", 8686), timeout=0.2).close()
        except OSError:
            pass
        else:
            raise RuntimeError("stable port 8686 is already occupied")
        launcher = stable_launcher_path(self.app_root)
        if not launcher.is_file():
            raise RuntimeError("stable launcher is unavailable")
        return launcher

    def _prepare_stable_instance(self) -> dict[str, str]:
        for directory in (self.stable_instance, self.stable_data, self.stable_stockdb):
            tasks.prepare_pytest_directory(directory)
        self.stable_config.write_text(json.dumps({
            "server": {"host": "127.0.0.1", "port": 8686},
            "data": {
                "root": str(self.stable_data),
                "free_stockdb_root": str(self.stable_stockdb),
                "free_stockdb_managed": False,
                "free_stockdb_auto_update": False,
                "free_stockdb_online_enabled": False,
                "akshare_enabled": False,
                "tushare_enabled": False,
                "yfinance_enabled": False,
                "after_close_enabled": False,
                "after_close_auto_run": False,
                "repair_enabled": False,
            },
            "automation": {"enabled": False},
            "lab": {"enabled": False},
        }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        environment = os.environ.copy()
        environment.update({
            "QM_CONFIG_PATH": str(self.stable_config),
            "QM_DATA_ROOT": str(self.stable_data),
            "QM_FREE_STOCKDB_ROOT": str(self.stable_stockdb),
            "QM_FREE_STOCKDB_MANAGED": "false",
            "QM_FREE_STOCKDB_AUTO_UPDATE": "false",
            "QM_FREE_STOCKDB_ONLINE_ENABLED": "false",
            "QM_AKSHARE_ENABLED": "false",
            "QM_TUSHARE_ENABLED": "false",
            "QM_YFINANCE_ENABLED": "false",
            "QM_AUTOMATION_ENABLED": "false",
            "QM_LAB_ENABLED": "false",
        })
        for name in ("QM_BUILD_SHA", "QM_SLOT_ID", "QM_RUNTIME_GENERATION"):
            environment.pop(name, None)
        environment.pop("PYTHONPATH", None)
        environment.pop("PYTHONHOME", None)
        return environment

    def _wait_stable_ready(self, process: Any) -> None:
        if self._stable_process is None:
            raise RuntimeError("stable launcher process was not recorded")
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            if process.poll() is not None:
                break
            try:
                health = _request_json(f"{self.base_url}/health")
                if health.get("status") == "ok" and health.get("core_ready") is True:
                    self._stable_process["web_pid"] = int(health.get("process_pid") or 0)
                    return
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                pass
            time.sleep(0.1)
        raise RuntimeError("stable launcher did not reach core readiness")

    def start_stable(self) -> None:
        launcher = self._stable_launcher()
        self._stable_env = self._prepare_stable_instance()
        log_root = self.evidence_root / "stable"
        tasks.prepare_pytest_directory(log_root)
        stdout = (log_root / "serve.stdout.log").open("w", encoding="utf-8")
        stderr = (log_root / "serve.stderr.log").open("w", encoding="utf-8")
        try:
            process = subprocess.Popen(
                ["cmd.exe", "/d", "/c", "call", str(launcher)],
                cwd=self.app_root,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                env=self._stable_env,
                creationflags=int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)),
            )
        except BaseException:
            stdout.close()
            stderr.close()
            raise
        self._stable_process = {
            "process": process, "stdout": stdout, "stderr": stderr, "web_pid": 0,
        }
        self._wait_stable_ready(process)

    def stable_sample(self, *, compute: bool) -> dict[str, object]:
        health = _request_json(f"{self.base_url}/health")
        if health.get("status") != "ok" or health.get("core_ready") is not True:
            raise RuntimeError("stable core-ready health is unavailable")
        shell = _request_bytes(self.web_url)
        if self._stable_shell is None:
            self._stable_shell = shell
        elif shell != self._stable_shell:
            raise RuntimeError("stable page changed during development activity")
        runtime = _request_json(f"{self.base_url}/settings/runtime")
        worker = runtime.get("worker")
        if not isinstance(worker, dict) or worker.get("available") is not True:
            raise RuntimeError("stable runtime-worker is unavailable")
        names = ("build_sha", "slot_id", "runtime_generation")
        identity = {name: health.get(name) for name in names}
        if any(worker.get(name) != identity[name] for name in names):
            raise RuntimeError("stable Web/runtime identity mismatch")
        compute_verified = False
        if compute:
            executable = stable_slot_executable(self.app_root)
            environment = (self._stable_env or os.environ).copy()
            for name in names:
                environment[f"QM_{name.upper()}"] = str(identity[name])
            completed = subprocess.run(
                [str(executable), "doctor", "--deep"],
                cwd=executable.parent,
                env={**environment, "PYTHONIOENCODING": "utf-8"},
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=90.0,
                check=False,
            )
            if completed.returncode:
                raise RuntimeError(f"stable compute probe failed ({completed.returncode})")
            value = json.loads(completed.stdout)
            probe = value.get("metrics", {}).get("application_identity_probe", {})
            compute_verified = all(probe.get(name) == identity[name] for name in names)
            if not compute_verified:
                raise RuntimeError("stable compute identity mismatch")
        return {
            **identity,
            "web_pid": health.get("process_pid"),
            "worker_pid": worker.get("pid"),
            "shell_bytes": len(shell),
            "write_roots": [
                str(self.stable_config), str(self.stable_data), str(self.stable_stockdb),
            ],
            "compute_verified": compute_verified,
        }

    def _http_json(
        self,
        path: str,
        *,
        method: str = "GET",
        payload: dict[str, object] | None = None,
        csrf: str = "",
    ) -> dict[str, object]:
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        headers = {"Accept": "application/json"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        if csrf:
            headers["X-CSRF-Token"] = csrf
        request = urllib.request.Request(
            f"{self.base_url}{path}", data=data, headers=headers, method=method,
        )
        with self._opener.open(request, timeout=2.0) as response:
            value = json.loads(response.read().decode("utf-8"))
        if not isinstance(value, dict):
            raise RuntimeError(f"non-object response from {path}")
        return value

    def activate(self, build_sha: str) -> dict[str, object]:
        started = time.monotonic()
        session = self._http_json("/session")
        token = str(session.get("csrf_token") or "")
        accepted = self._http_json(
            "/system/update/activate",
            method="POST",
            payload={"build_sha": build_sha},
            csrf=token,
        )
        operation_id = str(accepted.get("operation_id") or "")
        if accepted.get("status") == "already_active":
            self._stable_shell = None
            return {
                "status": "already_active", "active": build_sha,
                "elapsed_seconds": time.monotonic() - started,
            }
        if accepted.get("status") != "accepted" or not operation_id:
            raise RuntimeError("activation request was not accepted")
        deadline = started + 30.0
        while time.monotonic() < deadline:
            try:
                status = self._http_json("/system/update")
                operation = status.get("operation")
                if (
                    isinstance(operation, dict)
                    and operation.get("operation_id") == operation_id
                    and operation.get("status") in {
                        "activated", "already_active", "rolled_back", "blocked",
                    }
                ):
                    result = operation.get("result")
                    if not isinstance(result, dict):
                        result = {"status": operation.get("status")}
                    if result.get("status") not in {"activated", "already_active"}:
                        raise RuntimeError(f"activation ended as {result.get('status')}")
                    self._stable_shell = None
                    return {
                        **result,
                        "active": str(result.get("active") or build_sha),
                        "elapsed_seconds": time.monotonic() - started,
                    }
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                pass
            time.sleep(0.1)
        raise RuntimeError("activation operation timed out")

    def force_failed_activation(self, build_sha: str) -> dict[str, object]:
        current = self.stable_sample(compute=False)
        delegate = SubprocessGenerationController(root_pid=int(current["web_pid"]))
        controller = _FailFirstReady(delegate)
        started = time.monotonic()
        result = ActivationCoordinator(SlotRegistry(self.app_root), controller).activate(build_sha)
        return {**result, "elapsed_seconds": time.monotonic() - started}

    def _dev_target(self, slug: str) -> tuple[Path, Path]:
        if re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", slug) is None:
            raise RuntimeError("invalid development task slug")
        target = (self.primary / ".worktrees" / slug).resolve()
        artifacts = (self.primary / ".artifacts" / "worktrees" / slug).resolve()
        if (
            target.parent != (self.primary / ".worktrees").resolve()
            or not target.is_dir()
            or not artifacts.is_dir()
            or target not in tasks.registered_worktrees(self.primary)
            or _git_text(target, "branch", "--show-current") != f"codex/{slug}"
            or _git_text(target, "status", "--short")
        ):
            raise RuntimeError(f"development task {slug} is not a clean registered task")
        return target, artifacts

    def _start_development(self, slug: str, cycle: int) -> dict[str, object]:
        target = (self.primary / ".worktrees" / slug).resolve()
        artifacts = (self.primary / ".artifacts" / "worktrees" / slug).resolve()
        dev = artifacts / "runtime" / "dev"
        log_root = self.evidence_root / "development" / f"cycle-{cycle:02d}" / slug
        tasks.prepare_pytest_directory(log_root)
        stdout = (log_root / "serve.stdout.log").open("w", encoding="utf-8")
        stderr = (log_root / "serve.stderr.log").open("w", encoding="utf-8")
        command = [
            str(tasks.project_python(self.primary)),
            str(self.primary / "scripts" / "dev" / "tasks.py"),
            "serve",
            slug,
        ]
        try:
            process = subprocess.Popen(
                command,
                cwd=self.primary,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                creationflags=int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)),
            )
        except BaseException:
            stdout.close()
            stderr.close()
            raise
        config_path = dev / "config.yaml"
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            if process.poll() is not None:
                break
            try:
                config = json.loads(config_path.read_text(encoding="utf-8"))
                port = int(config["server"]["port"])
                health = _request_json(f"http://127.0.0.1:{port}/api/v1/health")
                if health.get("status") == "ok" and health.get("core_ready") is True:
                    return {
                        "slug": slug,
                        "cycle": cycle,
                        "port": port,
                        "web_pid": int(health["process_pid"]),
                        "process": process,
                        "stdout": stdout,
                        "stderr": stderr,
                        "target": target,
                        "artifacts": artifacts,
                        "config_path": config_path,
                    }
            except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
                pass
            time.sleep(0.1)
        stdout.flush()
        stderr.flush()
        stdout.close()
        stderr.close()
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5.0)
        raise RuntimeError(f"development task {slug} did not become ready")

    def _development_doctor(self, handle: dict[str, object]) -> dict[str, object]:
        artifacts = Path(str(handle["artifacts"]))
        dev = artifacts / "runtime" / "dev"
        environment = os.environ.copy()
        environment.update({
            "QM_CONFIG_PATH": str(handle["config_path"]),
            "QM_DATA_ROOT": str(dev / "data"),
            "QM_FREE_STOCKDB_ROOT": str(dev / "free-stockdb"),
            "QM_FREE_STOCKDB_CONTROL_PATH": str(dev / "control.sqlite"),
            "QM_FREE_STOCKDB_MANAGED": "false",
            "QM_FREE_STOCKDB_AUTO_UPDATE": "false",
            "PYTHONIOENCODING": "utf-8",
        })
        completed = subprocess.run(
            [str(tasks.project_python(self.primary)), "-m", "quantmaster.cli", "doctor", "--deep"],
            cwd=Path(str(handle["target"])),
            env=environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=90.0,
            check=False,
        )
        if completed.returncode:
            raise RuntimeError(
                f"development compute probe failed for {handle['slug']} "
                f"({completed.returncode})"
            )
        value = json.loads(completed.stdout)
        probe = value.get("metrics", {}).get("application_identity_probe", {})
        verified = (
            probe.get("build_sha") == "source"
            and probe.get("slot_id") == "source"
            and _GENERATION.fullmatch(str(probe.get("runtime_generation") or "")) is not None
        )
        if not verified:
            raise RuntimeError(f"development compute identity failed for {handle['slug']}")
        return {**probe, "compute_verified": True}

    def _stop_development(self, handle: dict[str, object]) -> None:
        process: Any = handle["process"]
        port = int(handle["port"])
        try:
            terminate_root_job(int(handle["web_pid"]))
            process.wait(timeout=15.0)
        finally:
            stdout: Any = handle["stdout"]
            stderr: Any = handle["stderr"]
            stdout.close()
            stderr.close()
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
        except OSError:
            return
        raise RuntimeError(f"development port remained bound: {port}")

    def _development_targets(
        self, slugs: tuple[str, str],
    ) -> list[tuple[str, Path, Path]]:
        if len(set(slugs)) != 2:
            raise RuntimeError("soak requires two distinct development worktrees")
        targets = [(slug, *self._dev_target(slug)) for slug in slugs]
        roots = [artifacts for _slug, _target, artifacts in targets]
        if (
            len(set(roots)) != 2
            or any(root.is_relative_to(self.app_root) for root in roots)
            or any(self.app_root.is_relative_to(root) for root in roots)
        ):
            raise RuntimeError("development writable roots overlap the stable application root")
        return targets

    def _prepare_development_changes(
        self, targets: list[tuple[str, Path, Path]], cycle: int,
    ) -> tuple[dict[Path, bytes], list[Path]]:
        originals: dict[Path, bytes] = {}
        probes: list[Path] = []
        try:
            for _slug, target, _artifacts in targets:
                project = target / "pyproject.toml"
                originals[project] = project.read_bytes()
                project.write_bytes(
                    originals[project]
                    + f"\n# stable-soak dependency probe cycle={cycle}\n".encode()
                )
                source = target / "quantmaster" / f"_soak_probe_{cycle}.py"
                static = (
                    target / "quantmaster" / "server" / "static"
                    / f"_soak_probe_{cycle}.txt"
                )
                if source.exists() or static.exists():
                    raise RuntimeError("development probe path already exists")
                source.write_text(f"CYCLE = {cycle}\n", encoding="utf-8")
                static.write_text(f"cycle={cycle}\n", encoding="utf-8")
                probes.extend((source, static))
        except Exception as exc:
            errors = self._restore_development_changes(originals, probes)
            if errors:
                raise ExceptionGroup("probe setup and restoration failed", [exc, *errors]) from exc
            raise
        return originals, probes

    @staticmethod
    def _restore_development_changes(
        originals: dict[Path, bytes], probes: list[Path],
    ) -> list[Exception]:
        errors: list[Exception] = []
        for project, content in originals.items():
            _capture_failure(errors, lambda path=project, value=content: path.write_bytes(value))
        for probe in probes:
            _capture_failure(errors, lambda path=probe: path.unlink(missing_ok=True))
        return errors

    def _finish_development(
        self,
        handles: list[dict[str, object]],
        originals: dict[Path, bytes],
        probes: list[Path],
        targets: list[tuple[str, Path, Path]],
    ) -> None:
        errors: list[Exception] = []
        for handle in reversed(handles):
            _capture_failure(errors, lambda value=handle: self._stop_development(value))
            if handle in self._owned_development:
                self._owned_development.remove(handle)
        errors.extend(self._restore_development_changes(originals, probes))
        for slug, target, _artifacts in targets:
            _capture_failure(
                errors,
                lambda name=slug, path=target: self._require_clean_development(name, path),
            )
        if errors:
            raise ExceptionGroup("development cleanup failed", errors)

    @staticmethod
    def _require_clean_development(slug: str, target: Path) -> None:
        if _git_text(target, "status", "--short"):
            raise RuntimeError(f"development task {slug} was not restored cleanly")

    def exercise_development(
        self, slugs: tuple[str, str], cycle: int,
    ) -> dict[str, object]:
        targets = self._development_targets(slugs)
        roots = [artifacts for _slug, _target, artifacts in targets]
        originals, probes = self._prepare_development_changes(targets, cycle)
        handles: list[dict[str, object]] = []
        doctors: list[dict[str, object]] = []
        try:
            for slug, _target, _artifacts in targets:
                handle = self._start_development(slug, cycle)
                handles.append(handle)
                self._owned_development.append(handle)
            doctors = [self._development_doctor(handle) for handle in handles]
            ports = [handle.get("port") for handle in handles]
            return {
                "cycle": cycle,
                "slugs": list(slugs),
                "ports": ports,
                "web_pids": [handle.get("web_pid") for handle in handles],
                "roots": [str(root) for root in roots],
                "roots_isolated": True,
                "compute_verified": all(
                    item.get("compute_verified") is True for item in doctors
                ),
                "compute": doctors,
            }
        finally:
            self._finish_development(handles, originals, probes, targets)

    def _shutdown_stable(self) -> None:
        if self._stable_process is None:
            return
        handle = self._stable_process
        self._stable_process = None
        process: Any = handle["process"]
        try:
            try:
                sample = self.stable_sample(compute=False)
                web_pid = int(sample["web_pid"])
            except (OSError, RuntimeError, ValueError, TypeError, KeyError):
                web_pid = int(handle.get("web_pid") or 0)
            if web_pid:
                terminate_root_job(web_pid)
            if process.poll() is None:
                process.wait(timeout=15.0)
        finally:
            stdout: Any = handle["stdout"]
            stderr: Any = handle["stderr"]
            stdout.close()
            stderr.close()
        try:
            socket.create_connection(("127.0.0.1", 8686), timeout=0.2).close()
        except OSError:
            return
        raise RuntimeError("stable port remained bound after shutdown")

    def shutdown(self) -> None:
        errors: list[Exception] = []
        _capture_failure(
            errors,
            lambda: self._finish_development(self._owned_development.copy(), {}, [], []),
        )
        _capture_failure(errors, self._shutdown_stable)
        if errors:
            raise ExceptionGroup("stable soak cleanup failed", errors)


def _identity(sample: dict[str, object], *, compute: bool) -> dict[str, object]:
    identity = {
        name: sample.get(name)
        for name in (
            "build_sha", "slot_id", "runtime_generation", "web_pid", "worker_pid",
        )
    }
    if (
        _FULL_SHA.fullmatch(str(identity["build_sha"] or "")) is None
        or identity["slot_id"] != identity["build_sha"]
        or _GENERATION.fullmatch(str(identity["runtime_generation"] or "")) is None
        or not isinstance(identity["web_pid"], int)
        or not isinstance(identity["worker_pid"], int)
        or identity["web_pid"] == identity["worker_pid"]
        or (compute and sample.get("compute_verified") is not True)
    ):
        raise RuntimeError("stable Web/runtime/compute identity is incomplete")
    return identity


def _write_evidence(path: Path, report: dict[str, object]) -> None:
    if not path.parent.is_dir():
        raise RuntimeError("evidence parent must be prepared by task tooling")
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _development_evidence(
    value: dict[str, object], slugs: tuple[str, str], cycle: int,
) -> dict[str, object]:
    ports = value.get("ports")
    if (
        value.get("cycle") != cycle
        or value.get("slugs") != list(slugs)
        or value.get("roots_isolated") is not True
        or value.get("compute_verified") is not True
        or not isinstance(ports, list)
        or len(ports) != 2
        or any(not isinstance(port, int) or port <= 0 for port in ports)
        or len(set(ports)) != 2
        or 8686 in ports
    ):
        raise RuntimeError("development worktree isolation evidence is incomplete")
    return value


def _transition_evidence(
    value: dict[str, object], *, status: str, active: str,
) -> dict[str, object]:
    elapsed = value.get("elapsed_seconds")
    if (
        value.get("status") != status
        or value.get("active") != active
        or not isinstance(elapsed, (int, float))
        or float(elapsed) > 15.0
    ):
        raise RuntimeError(f"{status} transition contract failed")
    return value


def _run(config: SoakConfig, system: SoakSystem) -> dict[str, object]:
    system.start_stable()
    started = system.now()
    deadline = started + config.duration_seconds
    initial_sample = system.stable_sample(compute=True)
    initial = _identity(initial_sample, compute=True)
    samples = [initial]
    development: list[dict[str, object]] = []
    next_development = started
    while system.now() < deadline:
        if system.now() >= next_development:
            cycle = len(development) + 1
            development.append(_development_evidence(
                system.exercise_development(config.dev_slugs, cycle),
                config.dev_slugs,
                cycle,
            ))
            current = _identity(system.stable_sample(compute=True), compute=True)
            if current != initial:
                raise _SoakContractError(
                    "stable generation changed during development activity",
                    expected=initial,
                    observed=current,
                )
            samples.append(current)
            next_development += config.development_interval_seconds
        wait = min(
            config.health_interval_seconds,
            max(0.0, next_development - system.now()),
            deadline - system.now(),
        )
        if wait <= 0:
            continue
        system.sleep(wait)
        current = _identity(system.stable_sample(compute=False), compute=False)
        if current != initial:
            raise _SoakContractError(
                "stable generation changed during soak",
                expected=initial,
                observed=current,
            )
        samples.append(current)
    final = _identity(system.stable_sample(compute=True), compute=True)
    if final != initial:
        raise _SoakContractError(
            "stable generation changed during soak",
            expected=initial,
            observed=final,
        )
    activation = _transition_evidence(
        system.activate(config.candidate_sha),
        status="activated",
        active=config.candidate_sha,
    )
    activated = _identity(system.stable_sample(compute=True), compute=True)
    if activated["build_sha"] != config.candidate_sha:
        raise _SoakContractError(
            "activated generation identity does not match candidate",
            expected={"build_sha": config.candidate_sha},
            observed=activated,
        )
    rollback = _transition_evidence(
        system.force_failed_activation(str(initial["build_sha"])),
        status="rolled_back",
        active=config.candidate_sha,
    )
    post_transition = _identity(system.stable_sample(compute=True), compute=True)
    if post_transition != activated:
        raise _SoakContractError(
            "forced failure did not preserve the activated generation",
            expected=activated,
            observed=post_transition,
        )
    report: dict[str, object] = {
        "schema": 1,
        "status": "passed",
        "started_at": datetime.now(UTC).isoformat(),
        "duration_seconds": config.duration_seconds,
        "config": {**asdict(config), "evidence_path": str(config.evidence_path)},
        "stable": {
            "initial": initial,
            "final": final,
            "post_transition": post_transition,
            "samples": len(samples),
            "shell_bytes": initial_sample.get("shell_bytes"),
            "write_roots": initial_sample.get("write_roots", []),
        },
        "development": development,
        "activation": activation,
        "rollback": rollback,
    }
    return report


def run_soak(config: SoakConfig, system: SoakSystem) -> dict[str, object]:
    """Observe one exact generation and always persist/clean owned state."""

    if (
        config.duration_seconds < SOAK_DURATION_SECONDS
        or config.health_interval_seconds <= 0
        or config.development_interval_seconds <= 0
        or len(set(config.dev_slugs)) != 2
        or any(tasks.SLUG_PATTERN.fullmatch(slug) is None for slug in config.dev_slugs)
        or _FULL_SHA.fullmatch(config.candidate_sha) is None
        or not config.evidence_path.is_absolute()
        or not config.evidence_path.parent.is_dir()
    ):
        raise ValueError("invalid stable-use acceptance configuration")
    report: dict[str, object]
    failure: BaseException | None = None
    started_at = datetime.now(UTC).isoformat()
    try:
        report = _run(config, system)
        report["started_at"] = started_at
    except BaseException as exc:
        failure = exc
        failure_evidence: dict[str, object] = {
            "type": type(exc).__name__,
            "message": str(exc),
            "observed_at": datetime.now(UTC).isoformat(),
        }
        if isinstance(exc, _SoakContractError):
            failure_evidence["context"] = exc.context
        report = {
            "schema": 1,
            "status": "failed",
            "started_at": started_at,
            "duration_seconds": config.duration_seconds,
            "config": {**asdict(config), "evidence_path": str(config.evidence_path)},
            "failure": failure_evidence,
        }
    try:
        system.shutdown()
    except BaseException as cleanup_exc:
        report["cleanup_failure"] = {
            "type": type(cleanup_exc).__name__, "message": str(cleanup_exc),
        }
        report["status"] = "failed"
        failure = failure or cleanup_exc
    _write_evidence(config.evidence_path, report)
    if failure is not None:
        raise SoakFailure(str(failure)) from failure
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dev-task", action="append", required=True,
        help="official isolated development task slug (exactly two)",
    )
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--evidence", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    slugs = tuple(args.dev_task)
    if (
        len(slugs) != 2
        or len(set(slugs)) != 2
        or any(tasks.SLUG_PATTERN.fullmatch(slug) is None for slug in slugs)
    ):
        parser.error("--dev-task must name exactly two distinct official task slugs")
    if _FULL_SHA.fullmatch(args.candidate_sha) is None:
        parser.error("--candidate-sha must be a full lowercase Git SHA")
    raw_evidence = Path(args.evidence).expanduser()
    if not raw_evidence.is_absolute():
        parser.error("--evidence must be an absolute task-local artifact path")
    evidence = raw_evidence.resolve()
    artifact_root = (ROOT / ".artifacts" / "worktrees").resolve()
    if not evidence.is_relative_to(artifact_root) or not evidence.parent.is_dir():
        parser.error("--evidence parent must be prepared under task artifacts")
    config = SoakConfig(
        duration_seconds=SOAK_DURATION_SECONDS,
        health_interval_seconds=HEALTH_INTERVAL_SECONDS,
        development_interval_seconds=DEVELOPMENT_INTERVAL_SECONDS,
        dev_slugs=(slugs[0], slugs[1]),
        candidate_sha=args.candidate_sha,
        evidence_path=evidence,
    )
    app_root = installed_app_root()
    registry = SlotRegistry(app_root)
    try:
        registry.validate_candidate(args.candidate_sha)
        active = str(registry.read().get("active") or "")
    except ActivationBlocked as exc:
        parser.error(f"staged candidate preflight failed: {exc}")
    if _FULL_SHA.fullmatch(active) is None or active == args.candidate_sha:
        parser.error("stable A slot must be active and distinct from candidate B")
    system = WindowsSoakSystem(
        primary=ROOT,
        app_root=app_root,
        evidence_root=evidence.parent,
    )
    report = run_soak(config, system)
    print(json.dumps({
        "status": report.get("status"),
        "duration_seconds": report.get("duration_seconds"),
        "development_cycles": len(report.get("development", [])),
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
