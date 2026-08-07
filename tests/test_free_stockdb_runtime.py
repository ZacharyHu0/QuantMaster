from __future__ import annotations

import json
import threading
from types import SimpleNamespace

import pandas as pd
import pytest

from quantmaster.data.free_stockdb_runtime import FreeStockDBRuntime
from quantmaster.settings import DataSettings


def test_free_stockdb_settings_validate_schedule_and_root() -> None:
    settings = DataSettings(
        free_stockdb_root="runtime/free-stockdb",
        free_stockdb_update_time="18:30",
    )
    assert settings.free_stockdb_managed is True
    assert settings.free_stockdb_auto_update is True
    assert settings.free_stockdb_update_time == "18:30"
    assert settings.free_stockdb_online_enabled is True
    assert settings.free_stockdb_online_url == "http://8.138.149.215:7899"

    with pytest.raises(ValueError):
        DataSettings(free_stockdb_update_time="25:00")


def test_managed_endpoint_uses_configured_loopback_port(monkeypatch) -> None:
    config = SimpleNamespace(data=SimpleNamespace(
        free_stockdb_url="http://localhost:7900",
    ))
    monkeypatch.setattr("quantmaster.data.free_stockdb_runtime.get_config", lambda: config)

    assert FreeStockDBRuntime._endpoint() == ("localhost", 7900)


def test_headless_copy_disables_only_unconditional_vendor_page(tmp_path) -> None:
    executable = tmp_path / "stockdb.exe"
    version_endpoint = b"https://a.123128.xyz/version?token=\x00"
    original = b"prefix\x00http://a.123128.xyz/\x00middle\x00" + version_endpoint + b"suffix"
    executable.write_bytes(original)

    managed = FreeStockDBRuntime._headless_executable(executable)
    patched = managed.read_bytes()

    assert managed != executable
    assert managed.name.startswith(".quantmaster-stockdb-headless-")
    assert b"http://a.123128.xyz/\x00" not in patched
    assert version_endpoint in patched
    assert executable.read_bytes() == original
    assert FreeStockDBRuntime._headless_executable(executable) == managed


def test_headless_copy_refuses_unknown_vendor_binary(tmp_path) -> None:
    executable = tmp_path / "stockdb.exe"
    executable.write_bytes(b"unknown vendor build")

    with pytest.raises(RuntimeError, match="期望 1 处，实际 0 处"):
        FreeStockDBRuntime._headless_executable(executable)


def test_reload_worker_attaches_without_taking_process_ownership(tmp_path, monkeypatch) -> None:
    runtime = FreeStockDBRuntime()
    monkeypatch.setattr(runtime, "_listening", lambda: True)
    monkeypatch.setenv(
        "QM_FREE_STOCKDB_CONTROL_PATH", str(tmp_path / "control.sqlite"),
    )

    assert runtime.attach_to_supervisor() is True
    status = runtime._status
    assert status["state"] == "running"
    assert status["managed"] is True
    assert status["supervised"] is True
    assert runtime._process is None
    assert runtime._thread is None


def test_managed_service_uses_headless_daemon_copy(
    tmp_path, monkeypatch,
) -> None:
    root = tmp_path / "free-stockdb"
    root.mkdir()
    executable = root / "stockdb.exe"
    executable.write_bytes(b"placeholder")
    managed_executable = root / ".quantmaster-stockdb-headless-test.exe"
    config_path = root / "stockdb.conf"
    config_path.write_text("server:\n\tport: 7899\n", encoding="utf-8")
    (root / "data").mkdir()
    runtime = FreeStockDBRuntime()
    calls = []
    listening = iter((False, True))

    class Process:
        def poll(self):
            return None

    def popen(command, **kwargs):
        calls.append((command, kwargs))
        return Process()

    monkeypatch.setattr(
        runtime, "_paths",
        lambda: (root, executable, root / "数据更新.exe"),
    )
    monkeypatch.setattr(runtime, "_listening", lambda: next(listening))
    monkeypatch.setattr(runtime, "_headless_executable", lambda _executable: managed_executable)
    monkeypatch.setattr("quantmaster.data.free_stockdb_runtime.subprocess.Popen", popen)

    assert runtime._start_service() is True
    command, kwargs = calls[0]
    assert command == [str(managed_executable), "-d", str(config_path), "-s", "start"]
    assert kwargs["cwd"] == root


def test_daemon_launcher_may_exit_before_service_starts(tmp_path, monkeypatch) -> None:
    root = tmp_path / "free-stockdb"
    root.mkdir()
    executable = root / "stockdb.exe"
    executable.write_bytes(b"placeholder")
    managed_executable = root / ".quantmaster-stockdb-headless-test.exe"
    (root / "stockdb.conf").write_text("server:\n\tport: 7899\n", encoding="utf-8")
    (root / "data").mkdir()
    runtime = FreeStockDBRuntime()
    listening = iter((False, False, True))

    class Process:
        pid = 0

        @staticmethod
        def poll():
            return 0

    monkeypatch.setattr(
        runtime, "_paths", lambda: (root, executable, root / "数据更新.exe"),
    )
    monkeypatch.setattr(runtime, "_listening", lambda: next(listening))
    monkeypatch.setattr(runtime, "_headless_executable", lambda _executable: managed_executable)
    monkeypatch.setattr(
        "quantmaster.data.free_stockdb_runtime.subprocess.Popen",
        lambda *_args, **_kwargs: Process(),
    )
    monkeypatch.setattr(runtime._stop, "wait", lambda _seconds: False)

    assert runtime._start_service() is True
    assert runtime._daemon_started is True


def test_managed_daemon_uses_native_stop_command(tmp_path, monkeypatch) -> None:
    root = tmp_path / "free-stockdb"
    root.mkdir()
    executable = root / "stockdb.exe"
    executable.write_bytes(b"placeholder")
    managed_executable = root / ".quantmaster-stockdb-headless-test.exe"
    config_path = root / "stockdb.conf"
    config_path.write_text("server:\n\tport: 7899\n", encoding="utf-8")
    runtime = FreeStockDBRuntime()
    runtime._daemon_started = True
    calls = []

    monkeypatch.setattr(
        runtime, "_paths", lambda: (root, executable, root / "数据更新.exe"),
    )
    monkeypatch.setattr(runtime, "_listening", lambda: False)
    monkeypatch.setattr(runtime, "_headless_executable", lambda _executable: managed_executable)
    monkeypatch.setattr(
        "quantmaster.data.free_stockdb_runtime.subprocess.run",
        lambda command, **kwargs: calls.append((command, kwargs)) or SimpleNamespace(returncode=0),
    )

    assert runtime._stop_service() is True
    command, kwargs = calls[0]
    assert command == [str(managed_executable), "-d", str(config_path), "-s", "stop"]
    assert kwargs["cwd"] == root
    assert runtime._daemon_started is False


def test_vendor_notice_parser_extracts_data_date_and_version() -> None:
    notice = FreeStockDBRuntime._parse_vendor_notice(
        "<div>数据更新至：2026-08-05</div>"
        "<span>[08-05] 二次加速修复</span>"
        "<p>最新版本 v2.3.1（2026-08-05 发布）</p>"
    )

    assert notice == {
        "data_date": "2026-08-05",
        "version": "2.3.1",
        "announcement": "[08-05] 二次加速修复",
    }


def test_vendor_notice_is_cached_without_opening_browser(tmp_path, monkeypatch) -> None:
    runtime = FreeStockDBRuntime()
    cache_path = tmp_path / "vendor-notice.json"
    calls = []

    class Response:
        text = (
            "数据更新至: 2026-08-06"
            "<span>[08-06] 新增私有存储</span>"
            "最新版本v3.0.0"
        )

        @staticmethod
        def raise_for_status() -> None:
            return None

    class Client:
        def __init__(self, **kwargs):
            calls.append(kwargs)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        @staticmethod
        def get(url):
            calls.append(url)
            return Response()

    monkeypatch.setattr(runtime, "_vendor_cache_path", lambda: cache_path)
    monkeypatch.setattr("quantmaster.data.free_stockdb_runtime.httpx.Client", Client)

    first = runtime.check_vendor_notice()
    second = runtime.check_vendor_notice()

    assert first["fingerprint"] == "2026-08-06|3.0.0|[08-06] 新增私有存储"
    assert second == first
    assert calls.count("https://a.123128.xyz/") == 1
    assert cache_path.is_file()


def test_managed_update_stops_updater_and_restores_service(tmp_path, monkeypatch) -> None:
    root = tmp_path / "free-stockdb"
    root.mkdir()
    updater = root / "数据更新.exe"
    updater.write_bytes(b"placeholder")
    runtime = FreeStockDBRuntime()
    events: list[str] = []

    monkeypatch.setattr(runtime, "_paths", lambda: (root, root / "stockdb.exe", updater))
    monkeypatch.setattr(runtime, "_marker_path", lambda: root / ".quantmaster-update.json")
    monkeypatch.setattr(runtime, "_listening", lambda: False)
    monkeypatch.setattr(runtime, "_stop_service", lambda: events.append("stop") or True)
    monkeypatch.setattr(runtime, "_start_service", lambda: events.append("start") or True)
    monkeypatch.setattr(runtime, "_run_updater", lambda *_args, **_kwargs: 0)
    validations = iter((
        {"target_session": "2026-08-07", "actual_session": "2026-08-06",
         "complete": False, "issues": ["stale"]},
        {"target_session": "2026-08-07", "actual_session": "2026-08-07",
         "complete": True, "issues": []},
    ))
    monkeypatch.setattr(runtime, "_validate_data", lambda _target: next(validations))
    monkeypatch.setattr(runtime, "_emit_update_event", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "quantmaster.data.resilience.PROVIDER_HEALTH.reset",
        lambda lane: events.append(f"reset:{lane}"),
    )

    assert runtime.update_now(target_session="2026-08-07") is True
    assert events == ["stop", "start", "reset:free-stockdb"]
    assert runtime._last_update_date() == "2026-08-07"


def test_manual_sidecar_update_is_non_blocking_and_coalesces(monkeypatch) -> None:
    runtime = FreeStockDBRuntime()
    started = threading.Event()
    release = threading.Event()

    def update(_trigger: str = "manual") -> bool:
        started.set()
        release.wait(2)
        return True

    monkeypatch.setattr(runtime, "update_now", update)

    assert runtime.request_update("manual") is True
    assert started.wait(1)
    assert runtime.request_update("manual") is False
    assert runtime.status()["state"] == "queued"
    release.set()


def test_runtime_status_exposes_sidecar_contract() -> None:
    status = FreeStockDBRuntime().status()

    assert status["update_capability"] in {"native_only", "unavailable"}
    assert status["sdk_engine"] in {"stock_sdk", "http-compatible"}
    assert status["service_url"].startswith("http")
    assert "last_update_at" in status
    assert "next_update_at" in status
    assert "managed" in status
    assert "target_session" in status
    assert "actual_session" in status
    assert "validated_session" in status
    assert "attempt" in status
    assert "max_attempts" in status
    assert "next_retry_at" in status
    assert "validation" in status


def test_stop_waits_for_queued_update_worker(monkeypatch) -> None:
    runtime = FreeStockDBRuntime()
    finished = threading.Event()

    def worker() -> None:
        runtime._stop.wait(1)
        finished.set()

    thread = threading.Thread(target=worker)
    runtime._update_thread = thread
    monkeypatch.setattr(runtime, "_stop_service", lambda: True)
    thread.start()

    runtime.stop()

    assert finished.is_set()
    assert not thread.is_alive()


def test_legacy_exit_marker_is_not_treated_as_validated(tmp_path, monkeypatch) -> None:
    runtime = FreeStockDBRuntime()
    marker = tmp_path / ".quantmaster-update.json"
    marker.write_text(
        json.dumps({"date": "2026-08-07", "exit_code": 0}), encoding="utf-8",
    )
    monkeypatch.setattr(runtime, "_marker_path", lambda: marker)

    assert runtime._last_update_date() == ""


def test_zero_exit_with_stale_data_schedules_bounded_retry(tmp_path, monkeypatch) -> None:
    root = tmp_path / "free-stockdb"
    root.mkdir()
    updater = root / "数据更新.exe"
    updater.write_bytes(b"placeholder")
    runtime = FreeStockDBRuntime()
    runtime._owner = True
    monkeypatch.setenv("QM_FREE_STOCKDB_CONTROL_PATH", str(root / "control.sqlite"))
    monkeypatch.setattr(runtime, "_paths", lambda: (root, root / "stockdb.exe", updater))
    monkeypatch.setattr(runtime, "_marker_path", lambda: root / ".quantmaster-update.json")
    monkeypatch.setattr(runtime, "_listening", lambda: False)
    monkeypatch.setattr(runtime, "_stop_service", lambda: True)
    monkeypatch.setattr(runtime, "_start_service", lambda: True)
    monkeypatch.setattr(runtime, "_run_updater", lambda *_args, **_kwargs: 0)
    validation = {
        "target_session": "2026-08-07", "actual_session": "2026-08-06",
        "complete": False, "issues": ["目标日截面为零"],
    }
    monkeypatch.setattr(runtime, "_validate_data", lambda _target: dict(validation))
    monkeypatch.setattr(
        "quantmaster.data.resilience.PROVIDER_HEALTH.reset", lambda _lane: None,
    )

    assert runtime.update_now(
        "schedule", target_session="2026-08-07", attempt=1,
    ) is False
    status = runtime.status()
    assert status["update_result"] == "retry_wait"
    assert status["attempt"] == 1
    assert status["max_attempts"] == 3
    assert status["next_retry_at"]
    assert runtime._retry_attempt == 2
    assert not (root / ".quantmaster-update.json").exists()


def test_final_validation_failure_emits_one_durable_event(tmp_path, monkeypatch) -> None:
    runtime = FreeStockDBRuntime()
    runtime._owner = True
    monkeypatch.setenv("QM_FREE_STOCKDB_CONTROL_PATH", str(tmp_path / "control.sqlite"))
    emitted = []
    monkeypatch.setattr(runtime, "_emit_update_event", lambda *args: emitted.append(args))
    validation = {
        "target_session": "2026-08-07", "actual_session": "2026-08-06",
        "complete": False, "issues": ["stale"],
    }

    runtime._finish_failure(
        target="2026-08-07", validation=validation, code=0,
        attempt=3, trigger="retry", message="stale",
    )

    assert runtime.status()["update_result"] == "failed"
    assert emitted == [(
        "update_failed", "2026-08-07",
        {"target_session": "2026-08-07", "validation": validation,
         "attempt": 3, "message": "stale"},
    )]


def test_supervised_worker_queues_update_for_owner(tmp_path, monkeypatch) -> None:
    control_path = tmp_path / "control.sqlite"
    monkeypatch.setenv("QM_FREE_STOCKDB_CONTROL_PATH", str(control_path))
    owner = FreeStockDBRuntime()
    owner._owner = True
    owner._set_status("running", "owned", managed=True)
    worker = FreeStockDBRuntime()
    assert worker.attach_to_supervisor() is True
    assert worker.request_update("manual") is True
    assert worker.request_update("manual") is False
    calls = []
    monkeypatch.setattr(owner, "update_now", lambda trigger: calls.append(trigger) or True)

    assert owner._process_command() is True
    assert calls == ["manual"]


def test_verified_quantmaster_orphan_can_be_reclaimed(tmp_path, monkeypatch) -> None:
    root = tmp_path / "free-stockdb"
    root.mkdir()
    executable = root / "stockdb.exe"
    executable.write_bytes(b"placeholder")
    marker = root / ".quantmaster-stockdb-owner.json"
    marker.write_text(json.dumps({
        "root": str(root),
        "process": {"pid": 22, "image": str(executable), "created": 222},
        "owner": {"pid": 11, "image": "QuantMaster.exe", "created": 111},
    }), encoding="utf-8")
    runtime = FreeStockDBRuntime()
    identities = {
        11: None,
        22: {"pid": 22, "image": str(executable), "created": 222},
    }
    terminated = []
    monkeypatch.setattr(runtime, "_root", lambda: root)
    monkeypatch.setattr(runtime, "_owner_marker_path", lambda: marker)
    monkeypatch.setattr(runtime, "_process_identity", lambda pid: identities.get(pid))
    monkeypatch.setattr(runtime, "_terminate_pid", lambda pid: terminated.append(pid) or True)
    monkeypatch.setattr(runtime, "_listening", lambda: False)

    assert runtime._recover_managed_orphan(executable) is True
    assert terminated == [22]
    assert not marker.exists()


def test_live_owner_process_is_never_reclaimed(tmp_path, monkeypatch) -> None:
    root = tmp_path / "free-stockdb"
    root.mkdir()
    executable = root / "stockdb.exe"
    executable.write_bytes(b"placeholder")
    marker = root / ".quantmaster-stockdb-owner.json"
    marker.write_text(json.dumps({
        "root": str(root),
        "process": {"pid": 22, "image": str(executable), "created": 222},
        "owner": {"pid": 11, "image": "QuantMaster.exe", "created": 111},
    }), encoding="utf-8")
    runtime = FreeStockDBRuntime()
    identities = {
        11: {"pid": 11, "image": "QuantMaster.exe", "created": 111},
        22: {"pid": 22, "image": str(executable), "created": 222},
    }
    terminated = []
    monkeypatch.setattr(runtime, "_root", lambda: root)
    monkeypatch.setattr(runtime, "_owner_marker_path", lambda: marker)
    monkeypatch.setattr(runtime, "_process_identity", lambda pid: identities.get(pid))
    monkeypatch.setattr(runtime, "_terminate_pid", lambda pid: terminated.append(pid) or True)

    assert runtime._recover_managed_orphan(executable) is False
    assert terminated == []
    assert marker.exists()


def test_full_market_validation_uses_actual_target_rows(isolated_config, monkeypatch) -> None:
    from quantmaster.data.free_stockdb_source import FreeStockDBSource
    from quantmaster.data.instruments import InstrumentStore

    instruments = [
        SimpleNamespace(symbol=f"{index:06d}.SZ", status="listed", exchange="SZ")
        for index in range(10)
    ]
    monkeypatch.setattr(InstrumentStore, "list", lambda *_args, **_kwargs: instruments)

    def cross_section(_self, symbols, _start, _end):
        return pd.DataFrame({
            "symbol": symbols,
            "date": [pd.Timestamp("2026-08-07")] * len(symbols),
            "open": [1.0] * len(symbols), "high": [1.0] * len(symbols),
            "low": [1.0] * len(symbols), "close": [1.0] * len(symbols),
            "volume": [1.0] * len(symbols),
        })

    monkeypatch.setattr(FreeStockDBSource, "daily_cross_section", cross_section)

    validation = FreeStockDBRuntime()._validate_data("2026-08-07")
    assert validation["complete"] is True
    assert validation["actual_session"] == "2026-08-07"
    assert validation["symbol_ratio"] == 1.0
