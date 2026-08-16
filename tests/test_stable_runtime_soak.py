from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

from scripts.release import stable_runtime_soak
from scripts.release.stable_runtime_soak import (
    SoakConfig,
    SoakFailure,
    WindowsSoakSystem,
    run_soak,
)

IDENTITY = {
    "build_sha": "a" * 40,
    "slot_id": "a" * 40,
    "runtime_generation": "b" * 32,
    "web_pid": 101,
    "worker_pid": 202,
}


@dataclass
class FakeSystem:
    elapsed: float = 0.0
    development_cycles: int = 0
    active_sha: str = "a" * 40
    activation_calls: tuple[str, ...] = ()
    sample_count: int = 0
    shutdown_calls: int = 0
    start_calls: int = 0

    def start_stable(self) -> None:
        self.start_calls += 1

    def now(self) -> float:
        return self.elapsed

    def sleep(self, seconds: float) -> None:
        self.elapsed += seconds

    def stable_sample(self, *, compute: bool) -> dict[str, object]:
        self.sample_count += 1
        identity = {
            **IDENTITY,
            "build_sha": self.active_sha,
            "slot_id": self.active_sha,
            "runtime_generation": "b" * 32 if self.active_sha == "a" * 40 else "d" * 32,
            "web_pid": 101 if self.active_sha == "a" * 40 else 303,
            "worker_pid": 202 if self.active_sha == "a" * 40 else 404,
        }
        return {**identity, "compute_verified": compute}

    def exercise_development(self, slugs: tuple[str, str], cycle: int) -> dict[str, object]:
        self.development_cycles += 1
        return {
            "cycle": cycle,
            "slugs": list(slugs),
            "ports": [18686, 18687],
            "roots_isolated": True,
            "compute_verified": True,
        }

    def activate(self, build_sha: str) -> dict[str, object]:
        self.activation_calls += (f"activate:{build_sha}",)
        self.active_sha = build_sha
        return {"status": "activated", "active": build_sha, "elapsed_seconds": 4.0}

    def force_failed_activation(self, build_sha: str) -> dict[str, object]:
        self.activation_calls += (f"fail:{build_sha}",)
        return {"status": "rolled_back", "active": self.active_sha, "elapsed_seconds": 6.0}

    def shutdown(self) -> None:
        self.shutdown_calls += 1


class DriftSystem(FakeSystem):
    def stable_sample(self, *, compute: bool) -> dict[str, object]:
        value = super().stable_sample(compute=compute)
        if self.sample_count >= 3:
            value["worker_pid"] = 999
        return value


def test_eight_hour_soak_keeps_one_stable_generation(tmp_path: Path) -> None:
    system = FakeSystem()
    report = run_soak(
        SoakConfig(
            duration_seconds=8 * 60 * 60,
            health_interval_seconds=60,
            development_interval_seconds=2 * 60 * 60,
            dev_slugs=("soak-dev-a", "soak-dev-b"),
            candidate_sha="c" * 40,
            evidence_path=tmp_path / "soak.json",
        ),
        system,
    )

    assert report["status"] == "passed"
    assert report["stable"]["initial"] == IDENTITY
    assert report["stable"]["final"] == IDENTITY
    assert report["duration_seconds"] == 8 * 60 * 60
    assert system.development_cycles == 4
    assert [item["cycle"] for item in report["development"]] == [1, 2, 3, 4]
    assert all(item["roots_isolated"] is True for item in report["development"])
    assert system.activation_calls == (
        f"activate:{'c' * 40}",
        f"fail:{'a' * 40}",
    )
    assert report["activation"]["status"] == "activated"
    assert report["rollback"]["status"] == "rolled_back"
    assert report["stable"]["post_transition"]["build_sha"] == "c" * 40
    assert system.shutdown_calls == 1
    assert system.start_calls == 1
    assert (tmp_path / "soak.json").is_file()


def test_identity_drift_writes_failure_evidence_and_cleans_owned_processes(
    tmp_path: Path,
) -> None:
    system = DriftSystem()
    evidence = tmp_path / "failed.json"

    with pytest.raises(SoakFailure, match="stable generation changed"):
        run_soak(
            SoakConfig(
                duration_seconds=8 * 60 * 60,
                health_interval_seconds=60,
                development_interval_seconds=2 * 60 * 60,
                dev_slugs=("soak-dev-a", "soak-dev-b"),
                candidate_sha="c" * 40,
                evidence_path=evidence,
            ),
            system,
        )

    assert system.shutdown_calls == 1
    report = json.loads(evidence.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["failure"]["observed_at"].endswith("+00:00")
    assert report["failure"]["context"]["expected"]["worker_pid"] == 202
    assert report["failure"]["context"]["observed"]["worker_pid"] == 999
    assert report["failure"]["context"]["observed"]["runtime_generation"] == "b" * 32


def test_windows_adapter_samples_web_runtime_and_compute_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_root = tmp_path / "app"
    executable = app_root / "slots" / ("a" * 40) / "QuantMaster.exe"
    executable.parent.mkdir(parents=True)
    executable.write_bytes(b"exe")
    (app_root / "launcher.target").write_text("a" * 40 + "\n", encoding="ascii")

    def request(path: str, **_kwargs) -> dict[str, object]:
        if path.endswith("/health"):
            return {
                "status": "ok", "core_ready": True,
                "build_sha": "a" * 40, "slot_id": "a" * 40,
                "runtime_generation": "b" * 32, "process_pid": 101,
            }
        return {"worker": {**IDENTITY, "pid": 202, "available": True}}

    monkeypatch.setattr(stable_runtime_soak, "_request_json", request)
    monkeypatch.setattr(stable_runtime_soak, "_request_bytes", lambda _url: b"stable shell")
    monkeypatch.setattr(
        stable_runtime_soak.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            [], 0, json.dumps({"metrics": {"application_identity_probe": {
                "build_sha": "a" * 40,
                "slot_id": "a" * 40,
                "runtime_generation": "b" * 32,
            }}}), "",
        ),
    )
    system = WindowsSoakSystem(
        primary=tmp_path / "primary",
        app_root=app_root,
        evidence_root=tmp_path,
    )

    assert system.stable_sample(compute=True) == {
        **IDENTITY,
        "shell_bytes": len(b"stable shell"),
        "write_roots": [
            str((tmp_path / "stable-instance" / name).resolve())
            for name in ("config.yaml", "data", "free-stockdb")
        ],
        "compute_verified": True,
    }


def test_development_cycle_restores_two_clean_worktrees(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = tmp_path / "primary"
    artifacts = primary / ".artifacts" / "worktrees"
    for slug in ("soak-dev-a", "soak-dev-b"):
        target = primary / ".worktrees" / slug
        (target / "quantmaster" / "server" / "static").mkdir(parents=True)
        (target / "pyproject.toml").write_bytes(b"[project]\nname='probe'\n")
        (artifacts / slug / "runtime" / "dev").mkdir(parents=True)

    def git_text(cwd: Path, *args: str) -> str:
        if args == ("branch", "--show-current"):
            return f"codex/{cwd.name}"
        if args == ("status", "--short"):
            return ""
        raise AssertionError(args)

    monkeypatch.setattr(stable_runtime_soak, "_git_text", git_text)
    monkeypatch.setattr(
        stable_runtime_soak.tasks,
        "registered_worktrees",
        lambda _root: {
            (primary / ".worktrees" / slug).resolve()
            for slug in ("soak-dev-a", "soak-dev-b")
        },
    )
    system = WindowsSoakSystem(
        primary=primary,
        app_root=tmp_path / "stable-app",
        evidence_root=tmp_path,
    )
    starts = []
    stops = []

    def start(slug: str, cycle: int) -> dict[str, object]:
        target = primary / ".worktrees" / slug
        assert (target / "quantmaster" / f"_soak_probe_{cycle}.py").is_file()
        assert (target / "quantmaster/server/static" / f"_soak_probe_{cycle}.txt").is_file()
        assert b"stable-soak dependency probe" in (target / "pyproject.toml").read_bytes()
        starts.append(slug)
        return {"slug": slug, "port": 18686 + len(starts), "web_pid": 300 + len(starts)}

    monkeypatch.setattr(system, "_start_development", start)
    monkeypatch.setattr(
        system,
        "_development_doctor",
        lambda handle: {"compute_verified": True, "runtime_generation": handle["slug"]},
    )
    monkeypatch.setattr(system, "_stop_development", lambda handle: stops.append(handle["slug"]))

    evidence = system.exercise_development(("soak-dev-a", "soak-dev-b"), 1)

    assert evidence["ports"] == [18687, 18688]
    assert evidence["roots_isolated"] is True
    assert evidence["compute_verified"] is True
    assert starts == ["soak-dev-a", "soak-dev-b"]
    assert stops == ["soak-dev-b", "soak-dev-a"]
    for slug in starts:
        target = primary / ".worktrees" / slug
        assert (target / "pyproject.toml").read_bytes() == b"[project]\nname='probe'\n"
        assert not list((target / "quantmaster").glob("_soak_probe_*.py"))
        assert not list((target / "quantmaster/server/static").glob("_soak_probe_*.txt"))


def test_development_probe_setup_failure_restores_earlier_worktree(
    tmp_path: Path,
) -> None:
    targets = []
    for slug in ("soak-dev-a", "soak-dev-b"):
        target = tmp_path / slug
        (target / "quantmaster/server/static").mkdir(parents=True)
        (target / "pyproject.toml").write_bytes(b"[project]\nname='probe'\n")
        targets.append((slug, target, tmp_path / f"artifacts-{slug}"))
    blocked = targets[1][1] / "quantmaster/server/static/_soak_probe_1.txt"
    blocked.write_text("occupied", encoding="utf-8")
    system = WindowsSoakSystem(
        primary=tmp_path / "primary",
        app_root=tmp_path / "app",
        evidence_root=tmp_path,
    )

    with pytest.raises(RuntimeError, match="already exists"):
        system._prepare_development_changes(targets, 1)

    assert (targets[0][1] / "pyproject.toml").read_bytes() == b"[project]\nname='probe'\n"
    assert not (targets[0][1] / "quantmaster/_soak_probe_1.py").exists()
    assert not (targets[0][1] / "quantmaster/server/static/_soak_probe_1.txt").exists()
    assert blocked.read_text(encoding="utf-8") == "occupied"


def test_development_process_uses_task_serve_and_exact_root_job_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = tmp_path / "primary"
    target = primary / ".worktrees" / "soak-dev-a"
    artifacts = primary / ".artifacts/worktrees/soak-dev-a"
    target.mkdir(parents=True)
    dev = artifacts / "runtime/dev"
    dev.mkdir(parents=True)
    (dev / "config.yaml").write_text(
        json.dumps({"server": {"host": "127.0.0.1", "port": 18691}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(stable_runtime_soak.tasks, "project_python", lambda _root: Path("python.exe"))
    calls = []

    class Process:
        pid = 77
        returncode = 0

        def poll(self):
            return None

        def wait(self, timeout=None):
            calls.append(("wait", timeout))
            return 0

    monkeypatch.setattr(
        stable_runtime_soak.subprocess,
        "Popen",
        lambda command, **kwargs: calls.append((command, kwargs)) or Process(),
    )
    monkeypatch.setattr(
        stable_runtime_soak,
        "_request_json",
        lambda url, **_kwargs: {
            "status": "ok", "core_ready": True, "process_pid": 303,
            "build_sha": "source", "slot_id": "source",
            "runtime_generation": "e" * 32,
        },
    )
    system = WindowsSoakSystem(
        primary=primary,
        app_root=tmp_path / "stable-app",
        evidence_root=tmp_path,
    )

    handle = system._start_development("soak-dev-a", 2)

    assert calls[0][0][-2:] == ["serve", "soak-dev-a"]
    assert handle["port"] == 18691
    assert handle["web_pid"] == 303

    monkeypatch.setattr(
        stable_runtime_soak.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            [], 0, json.dumps({"metrics": {"application_identity_probe": {
                "build_sha": "source", "slot_id": "source",
                "runtime_generation": "f" * 32,
            }}}), "",
        ),
    )
    assert system._development_doctor(handle)["compute_verified"] is True
    terminated = []
    monkeypatch.setattr(
        stable_runtime_soak,
        "terminate_root_job",
        lambda pid: terminated.append(pid),
    )

    system._stop_development(handle)

    assert terminated == [303]
    assert ("wait", 15.0) in calls


def test_activation_uses_local_http_and_forced_failure_reuses_coordinator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    system = WindowsSoakSystem(
        primary=tmp_path / "primary",
        app_root=tmp_path / "app",
        evidence_root=tmp_path,
    )
    requests = []

    def http(path: str, *, method: str = "GET", payload=None, csrf: str = ""):
        requests.append((path, method, payload, csrf))
        if path == "/session":
            return {"csrf_token": "token"}
        if method == "POST":
            return {"status": "accepted", "operation_id": "op"}
        return {
            "operation": {
                "status": "activated",
                "operation_id": "op",
                "result": {"status": "activated", "active": "c" * 40},
            }
        }

    monkeypatch.setattr(system, "_http_json", http)
    system._stable_shell = b"generation-a"
    activated = system.activate("c" * 40)

    assert activated["status"] == "activated"
    assert system._stable_shell is None
    assert requests[1] == (
        "/system/update/activate", "POST", {"build_sha": "c" * 40}, "token",
    )

    monkeypatch.setattr(
        system,
        "stable_sample",
        lambda **_kwargs: {**IDENTITY, "web_pid": 404, "compute_verified": True},
    )
    constructed = []

    class Coordinator:
        def __init__(self, registry, controller):
            constructed.append((registry, controller))

        def activate(self, build_sha):
            assert build_sha == "a" * 40
            return {"status": "rolled_back", "active": "c" * 40}

    monkeypatch.setattr(stable_runtime_soak, "SlotRegistry", lambda root: ("registry", root))
    monkeypatch.setattr(
        stable_runtime_soak,
        "SubprocessGenerationController",
        lambda root_pid: ("delegate", root_pid),
    )
    monkeypatch.setattr(stable_runtime_soak, "ActivationCoordinator", Coordinator)

    rolled_back = system.force_failed_activation("a" * 40)

    assert rolled_back["status"] == "rolled_back"
    assert constructed[0][0] == ("registry", system.app_root)
    assert constructed[0][1].delegate == ("delegate", 404)


def test_windows_adapter_owns_stable_launcher_and_exact_shutdown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_root = tmp_path / "app"
    app_root.mkdir()
    launcher = app_root / "QuantMaster Stable Launcher.cmd"
    launcher.write_text("@exit /b 0\r\n", encoding="ascii")
    calls = []

    class Process:
        pid = 505

        def poll(self):
            return None

        def wait(self, timeout=None):
            calls.append(("wait", timeout))
            return 0

        def terminate(self):
            calls.append(("terminate", self.pid))

    monkeypatch.setattr(
        stable_runtime_soak.subprocess,
        "Popen",
        lambda command, **kwargs: calls.append((command, kwargs)) or Process(),
    )
    monkeypatch.setattr(
        stable_runtime_soak,
        "_request_json",
        lambda *_args, **_kwargs: {"status": "ok", "core_ready": True},
    )
    terminated = []
    monkeypatch.setattr(
        stable_runtime_soak,
        "terminate_root_job",
        lambda pid: terminated.append(pid),
    )
    system = WindowsSoakSystem(
        primary=tmp_path / "primary",
        app_root=app_root,
        evidence_root=tmp_path,
    )

    system.start_stable()
    monkeypatch.setattr(
        system,
        "stable_sample",
        lambda **_kwargs: {**IDENTITY, "web_pid": 606, "compute_verified": False},
    )
    system.shutdown()

    assert calls[0][0][-1] == str(launcher)
    environment = calls[0][1]["env"]
    instance = (tmp_path / "stable-instance").resolve()
    assert Path(environment["QM_CONFIG_PATH"]).resolve() == instance / "config.yaml"
    assert Path(environment["QM_DATA_ROOT"]).resolve() == instance / "data"
    assert Path(environment["QM_FREE_STOCKDB_ROOT"]).resolve() == instance / "free-stockdb"
    assert json.loads((instance / "config.yaml").read_text(encoding="utf-8"))["server"] == {
        "host": "127.0.0.1", "port": 8686,
    }
    assert terminated == [606]
    assert ("wait", 15.0) in calls


def test_stable_sample_rejects_page_drift_without_hashes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    system = WindowsSoakSystem(
        primary=tmp_path / "primary",
        app_root=tmp_path / "app",
        evidence_root=tmp_path,
    )
    monkeypatch.setattr(
        stable_runtime_soak,
        "_request_json",
        lambda path, **_kwargs: (
            {
                "status": "ok", "core_ready": True,
                "build_sha": "a" * 40, "slot_id": "a" * 40,
                "runtime_generation": "b" * 32, "process_pid": 101,
            }
            if path.endswith("/health")
            else {"worker": {**IDENTITY, "pid": 202, "available": True}}
        ),
    )
    pages = iter((b"stable shell", b"development shell"))
    monkeypatch.setattr(stable_runtime_soak, "_request_bytes", lambda _url: next(pages))

    assert system.stable_sample(compute=False)["shell_bytes"] == len(b"stable shell")
    with pytest.raises(RuntimeError, match="page changed"):
        system.stable_sample(compute=False)


def test_stable_sample_requires_lightweight_core_readiness(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    system = WindowsSoakSystem(
        primary=tmp_path / "primary",
        app_root=tmp_path / "app",
        evidence_root=tmp_path,
    )

    def request(path: str, **_kwargs) -> dict[str, object]:
        if path.endswith("/health"):
            return {
                "status": "ok", "core_ready": False,
                "build_sha": "a" * 40, "slot_id": "a" * 40,
                "runtime_generation": "b" * 32, "process_pid": 101,
            }
        return {"worker": {**IDENTITY, "pid": 202, "available": True}}

    monkeypatch.setattr(stable_runtime_soak, "_request_json", request)

    with pytest.raises(RuntimeError, match="core-ready"):
        system.stable_sample(compute=False)


def test_development_target_must_be_registered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = tmp_path / "primary"
    target = primary / ".worktrees" / "soak-dev-a"
    artifacts = primary / ".artifacts" / "worktrees" / "soak-dev-a"
    target.mkdir(parents=True)
    artifacts.mkdir(parents=True)
    monkeypatch.setattr(
        stable_runtime_soak,
        "_git_text",
        lambda _cwd, *args: "codex/soak-dev-a" if args[0] == "branch" else "",
    )
    monkeypatch.setattr(stable_runtime_soak.tasks, "registered_worktrees", lambda _root: set())
    system = WindowsSoakSystem(
        primary=primary,
        app_root=tmp_path / "app",
        evidence_root=tmp_path,
    )

    with pytest.raises(RuntimeError, match="registered"):
        system._dev_target("soak-dev-a")


def test_cli_fixes_real_acceptance_budgets_and_task_local_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    primary = tmp_path / "primary"
    evidence = (
        primary / ".artifacts" / "worktrees" / "stable-use-soak"
        / "acceptance" / "soak.json"
    )
    evidence.parent.mkdir(parents=True)
    app_root = tmp_path / "installed-app"
    calls = []
    sentinel = object()
    monkeypatch.setattr(stable_runtime_soak, "ROOT", primary, raising=False)
    monkeypatch.setattr(
        stable_runtime_soak, "installed_app_root", lambda: app_root, raising=False,
    )
    class Registry:
        def __init__(self, root: Path) -> None:
            assert root == app_root

        def validate_candidate(self, build_sha: str) -> None:
            calls.append(("candidate", build_sha))

        def read(self) -> dict[str, object]:
            return {"active": "a" * 40}

    monkeypatch.setattr(stable_runtime_soak, "SlotRegistry", Registry)
    monkeypatch.setattr(
        stable_runtime_soak,
        "WindowsSoakSystem",
        lambda **kwargs: calls.append(("system", kwargs)) or sentinel,
    )
    monkeypatch.setattr(
        stable_runtime_soak,
        "run_soak",
        lambda config, system: calls.append(("run", config, system))
        or {"status": "passed", "duration_seconds": config.duration_seconds},
    )

    result = stable_runtime_soak.main([
        "--dev-task", "soak-dev-a",
        "--dev-task", "soak-dev-b",
        "--candidate-sha", "c" * 40,
        "--evidence", str(evidence),
    ])

    assert result == 0
    assert calls[0] == ("candidate", "c" * 40)
    config = calls[2][1]
    assert config.duration_seconds == 8 * 60 * 60
    assert config.health_interval_seconds == 60
    assert config.development_interval_seconds == 2 * 60 * 60
    assert config.dev_slugs == ("soak-dev-a", "soak-dev-b")
    assert calls[1] == (
        "system",
        {"primary": primary, "app_root": app_root, "evidence_root": evidence.parent},
    )
    output = capsys.readouterr().out
    assert '"status": "passed"' in output
    assert str(primary) not in output


@pytest.mark.parametrize(
    "arguments",
    [
        ["--dev-task", "same", "--dev-task", "same", "--candidate-sha", "c" * 40],
        ["--dev-task", "one", "--dev-task", "two", "--candidate-sha", "short"],
        ["--dev-task", "one", "--dev-task", "two", "--candidate-sha", "c" * 40,
         "--evidence", "relative.json"],
    ],
)
def test_cli_rejects_non_acceptance_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    arguments: list[str],
) -> None:
    primary = tmp_path / "primary"
    evidence = primary / ".artifacts/worktrees/stable-use-soak/acceptance/soak.json"
    evidence.parent.mkdir(parents=True)
    monkeypatch.setattr(stable_runtime_soak, "ROOT", primary, raising=False)
    if "--evidence" not in arguments:
        arguments = [*arguments, "--evidence", str(evidence)]

    with pytest.raises(SystemExit) as captured:
        stable_runtime_soak.main(arguments)

    assert captured.value.code == 2
