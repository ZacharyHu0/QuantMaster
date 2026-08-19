from __future__ import annotations

import json
import threading
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from quantmaster.data.free_stockdb_runtime import FreeStockDBRuntime
from quantmaster.settings import DataSettings


def test_owner_state_publishes_control_writer_lease(tmp_path, monkeypatch) -> None:
    root = tmp_path / "free-stockdb"
    control = root / ".quantmaster-control.sqlite"
    root.mkdir()
    captured: list[dict[str, object]] = []

    class Control:
        @staticmethod
        def write_state(payload):
            captured.append(dict(payload))

    runtime = FreeStockDBRuntime()
    runtime._owner = True
    monkeypatch.setattr(runtime, "_root", lambda: root)
    monkeypatch.setattr(runtime, "_control_path", lambda: control)
    monkeypatch.setattr(runtime, "_ensure_control", lambda: Control())
    monkeypatch.setattr(
        runtime, "_process_identity",
        lambda _pid: {"pid": 123, "image": str(tmp_path / "python.exe"), "created": 456},
    )

    runtime._set_status("running", "ready")

    assert captured[-1]["control_writer"] == {
        "pid": 123,
        "image": str(tmp_path / "python.exe"),
        "created": 456,
        "instance_root": str(root.resolve()),
        "control_path": str(control.resolve()),
    }


def test_apply_config_control_error_is_redacted(monkeypatch):
    internal = r"C:\private\control.sqlite Bearer secret-value"

    class BrokenControl:
        @staticmethod
        def enqueue(*_args, **_kwargs):
            raise OSError(internal)

    runtime = FreeStockDBRuntime()
    monkeypatch.setattr(runtime, "_ensure_control", lambda: BrokenControl())

    result = runtime.request_apply_config(["data.free_stockdb_url"])

    assert result == {
        "status": "degraded",
        "message": "控制命令入队失败；详细信息已写入本机日志",
    }
    assert "private" not in str(result)
    assert "secret-value" not in str(result)


def test_free_stockdb_settings_validate_schedule_and_root() -> None:
    settings = DataSettings(
        free_stockdb_root="runtime/free-stockdb",
        free_stockdb_update_time="18:30",
    )
    assert settings.free_stockdb_managed is True
    assert settings.free_stockdb_auto_update is True
    assert settings.free_stockdb_update_time == "18:30"
    assert settings.free_stockdb_online_enabled is False
    assert settings.free_stockdb_online_url == "http://8.138.149.215:7899"

    with pytest.raises(ValueError):
        DataSettings(free_stockdb_update_time="25:00")


def test_stockdb_schedule_uses_shanghai_clock_across_us_dst(isolated_config) -> None:
    isolated_config.data.free_stockdb_update_time = "18:30"
    new_york = ZoneInfo("America/New_York")

    before_us_dst = FreeStockDBRuntime._scheduled_at(
        datetime(2026, 3, 7, 5, tzinfo=new_york),
    )
    after_us_dst = FreeStockDBRuntime._scheduled_at(
        datetime(2026, 3, 9, 5, tzinfo=new_york),
    )

    assert before_us_dst.isoformat() == "2026-03-07T18:30:00+08:00"
    assert after_us_dst.isoformat() == "2026-03-09T18:30:00+08:00"
    with pytest.raises(ValueError, match="必须包含时区"):
        FreeStockDBRuntime._scheduled_at(datetime(2026, 3, 9, 17))


def test_managed_endpoint_uses_configured_loopback_port(monkeypatch) -> None:
    config = SimpleNamespace(data=SimpleNamespace(
        free_stockdb_url="http://localhost:7900",
    ))
    monkeypatch.setattr("quantmaster.data.free_stockdb_runtime.get_config", lambda: config)

    assert FreeStockDBRuntime._endpoint() == ("localhost", 7900)


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


def test_managed_service_launches_original_vendor_binary(
    tmp_path, monkeypatch,
) -> None:
    root = tmp_path / "free-stockdb"
    root.mkdir()
    executable = root / "stockdb.exe"
    executable.write_bytes(b"placeholder")
    config_path = root / "stockdb.conf"
    config_path.write_text("server:\n\tport: 7899\n", encoding="utf-8")
    (root / "data").mkdir()
    runtime = FreeStockDBRuntime()
    calls = []
    listening = iter((False, True))

    class Process:
        pid = 22

        def poll(self):
            return None

    def launch(actual_executable, actual_config, actual_root):
        calls.append((actual_executable, actual_config, actual_root))
        return Process()

    monkeypatch.setattr(
        runtime, "_paths",
        lambda: (root, executable, root / "数据更新.exe"),
    )
    monkeypatch.setattr(runtime, "_listening", lambda: next(listening))
    monkeypatch.setattr(runtime, "_launch_service_process", launch)

    assert runtime._start_service() is True
    assert calls == [(executable, config_path, root)]


def test_managed_service_keeps_stockdb_in_the_runtime_worker_process_tree(monkeypatch, tmp_path):
    executable = tmp_path / "stockdb.exe"
    config_path = tmp_path / "stockdb.conf"
    captured = {}

    def popen(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.setattr("quantmaster.data.free_stockdb_runtime.subprocess.Popen", popen)

    FreeStockDBRuntime._launch_service_process(executable, config_path, tmp_path)

    assert captured["command"] == [str(executable), str(config_path), "-s", "start"]


def test_daemon_launcher_may_exit_before_service_starts(tmp_path, monkeypatch) -> None:
    root = tmp_path / "free-stockdb"
    root.mkdir()
    executable = root / "stockdb.exe"
    executable.write_bytes(b"placeholder")
    config_path = root / "stockdb.conf"
    config_path.write_text("server:\n\tport: 7899\n", encoding="utf-8")
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
    monkeypatch.setattr(
        runtime, "_launch_service_process", lambda *_args, **_kwargs: Process(),
    )
    monkeypatch.setattr(runtime._stop, "wait", lambda _seconds: False)

    assert runtime._start_service() is True
    assert runtime._daemon_started is True


def test_managed_service_stops_tracked_vendor_process(monkeypatch) -> None:
    runtime = FreeStockDBRuntime()
    runtime._daemon_started = True
    calls: list[str] = []

    class Process:
        pid = 22

        @staticmethod
        def poll():
            return None

        @staticmethod
        def terminate():
            calls.append("terminate")

        @staticmethod
        def wait(timeout=None):
            calls.append(f"wait:{timeout}")
            return 1

        @staticmethod
        def kill():
            calls.append("kill")

    runtime._process = Process()
    monkeypatch.setattr(runtime, "_listening", lambda: False)

    assert runtime._stop_service() is True
    assert calls == ["terminate", "wait:15"]
    assert runtime._daemon_started is False


def test_bootstrap_binary_replacement_is_relaunched(tmp_path, monkeypatch) -> None:
    root = tmp_path / "free-stockdb"
    root.mkdir()
    executable = root / "stockdb.exe"
    executable.write_bytes(b"bootstrap")
    config_path = root / "stockdb.conf"
    config_path.write_text("server:\n\tport: 7899\n", encoding="utf-8")
    (root / "data").mkdir()
    runtime = FreeStockDBRuntime()
    launches = 0
    listening = iter((False, True))
    monotonic = iter((0.0, 11.0, 20.0, 21.0))

    class Process:
        pid = 22

        @staticmethod
        def poll():
            return 0

    def launch(*_args):
        nonlocal launches
        launches += 1
        if launches == 1:
            executable.write_bytes(b"platform-runtime")
        return Process()

    monkeypatch.setattr(
        runtime, "_paths", lambda: (root, executable, root / "数据更新.exe"),
    )
    monkeypatch.setattr(runtime, "_listening", lambda: next(listening))
    monkeypatch.setattr(runtime, "_launch_service_process", launch)
    monkeypatch.setattr(
        "quantmaster.data.free_stockdb_runtime._monotonic", lambda: next(monotonic),
    )

    assert runtime._start_service() is True
    assert launches == 2


def test_vendor_notice_parser_extracts_notice_date_and_version() -> None:
    notice = FreeStockDBRuntime._parse_vendor_notice(
        '<span class="tag-blue">更新至: 2026-08-17</span>'
        '<h3 class="card-title">[08-14] 全市场实时 Ticks 行情</h3>'
        "<p>最新版本 v0.3.1-online-more-power，直接解压覆盖即可。</p>"
    )

    assert notice == {
        "notice_updated_on": "2026-08-17",
        "version": "0.3.1-online-more-power",
        "announcement": "[08-14] 全市场实时 Ticks 行情",
    }


def test_notice_update_date_never_nominates_trading_target(monkeypatch) -> None:
    runtime = FreeStockDBRuntime()
    monkeypatch.setattr(
        runtime, "check_vendor_notice",
        lambda **_kwargs: {"notice_updated_on": "2026-08-13"},
    )
    monkeypatch.setattr(
        "quantmaster.trading_sessions.resolve_session_target",
        lambda: SimpleNamespace(ready=False, session="", source="unavailable"),
    )

    assert runtime._target_session() == ("", "unavailable")


def test_vendor_target_does_not_advance_marker_when_validation_fails(
    tmp_path, monkeypatch,
) -> None:
    runtime = FreeStockDBRuntime()
    marker = tmp_path / ".quantmaster-update.json"
    marker.write_text(json.dumps({
        "schema_version": 2, "validated_session": "2026-08-12",
    }), encoding="utf-8")
    monkeypatch.setattr(runtime, "_marker_path", lambda: marker)
    monkeypatch.setattr(runtime, "_target_session", lambda **_kwargs: (
        "2026-08-13", "free-stockdb-vendor",
    ))
    monkeypatch.setattr(runtime, "_validate_data", lambda _target: {
        "target_session": "2026-08-13", "actual_session": "2026-08-12",
        "accepted": False, "complete": False, "warnings": [], "issues": ["stale"],
    })
    monkeypatch.setattr(runtime, "_paths", lambda: (
        tmp_path, tmp_path / "stockdb.exe", tmp_path / "missing-updater.exe",
    ))

    assert runtime.update_now("manual") is False
    assert runtime._last_update_date() == "2026-08-12"


def test_notice_update_date_does_not_override_trusted_resolver(monkeypatch) -> None:
    runtime = FreeStockDBRuntime()
    monkeypatch.setattr(
        runtime, "check_vendor_notice",
        lambda **_kwargs: {"notice_updated_on": "2026-08-12"},
    )
    monkeypatch.setattr(
        "quantmaster.trading_sessions.resolve_session_target",
        lambda: SimpleNamespace(
            ready=True, session="2026-08-13", source="stockdb:validated",
        ),
    )

    assert runtime._target_session() == ("2026-08-13", "stockdb:validated")


def test_update_target_uses_latest_closed_official_session_over_reader_fallback(
    monkeypatch,
) -> None:
    runtime = FreeStockDBRuntime()
    monkeypatch.setattr(runtime, "check_vendor_notice", lambda **_kwargs: {})
    monkeypatch.setattr(
        "quantmaster.trading_sessions.resolve_session_target",
        lambda: SimpleNamespace(
            ready=True,
            session="2026-08-17",
            source="stockdb:validated",
            completion="previous_session_complete",
            coverage={
                "official_dates": ["2026-08-17", "2026-08-18"],
                "official_source": "tushare:SSE",
            },
        ),
    )

    assert runtime._target_session() == ("2026-08-18", "tushare:SSE")


def test_closed_official_calendar_session_nominates_update_target(monkeypatch) -> None:
    runtime = FreeStockDBRuntime()
    monkeypatch.setattr(runtime, "check_vendor_notice", lambda **_kwargs: {})
    monkeypatch.setattr(
        "quantmaster.trading_sessions.resolve_session_target",
        lambda: SimpleNamespace(
            ready=False,
            session="2026-08-18",
            source="free-stockdb-online:calendar",
            completion="current_session_closed_waiting_provider",
        ),
    )

    assert runtime._target_session() == (
        "2026-08-18", "free-stockdb-online:calendar",
    )


def test_vendor_notice_is_cached_without_opening_browser(tmp_path, monkeypatch) -> None:
    runtime = FreeStockDBRuntime()
    cache_path = tmp_path / "vendor-notice.json"
    calls = []

    class Response:
        text = (
            "更新至: 2026-08-06"
            '<h3 class="card-title">[08-06] 新增私有存储</h3>'
            "最新版本 v3.0.0"
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
    assert first["notice_updated_on"] == "2026-08-06"
    assert second == first
    assert first["url"] == "https://a.123128.xyz/"
    assert calls.count("https://a.123128.xyz/tabs/notice.html") == 1
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
         "accepted": False, "complete": False, "warnings": [], "issues": ["stale"]},
        {"target_session": "2026-08-07", "actual_session": "2026-08-07",
         "accepted": True, "complete": True, "warnings": [], "issues": []},
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


def test_vendor_data_roots_include_all_numbered_partitions_only(tmp_path) -> None:
    for name in ("data", "data1", "data2", "data10", "datax", "mydb", "数据库"):
        (tmp_path / name).mkdir(exist_ok=True)

    assert [path.name for path in FreeStockDBRuntime._data_roots(tmp_path)] == [
        "data", "data1", "data2", "data10",
    ]


def test_running_updater_closes_only_after_changed_data_is_stable_and_accepted(
    tmp_path, monkeypatch,
) -> None:
    runtime = FreeStockDBRuntime()
    process = SimpleNamespace(pid=42, returncode=None)
    process.poll = lambda: process.returncode
    fingerprints = iter((("before", 1, 1), ("after", 2, 2), ("after", 2, 2)))
    events: list[str] = []

    monkeypatch.setattr(
        "quantmaster.data.free_stockdb_runtime.subprocess.Popen",
        lambda *_args, **_kwargs: process,
    )
    monkeypatch.setattr(runtime, "_data_fingerprint", lambda _root: next(fingerprints))
    monkeypatch.setattr(runtime._stop, "wait", lambda _seconds: False)
    monkeypatch.setattr("quantmaster.data.free_stockdb_runtime._DATA_STABILITY_SECONDS", 0)
    monkeypatch.setattr("quantmaster.data.free_stockdb_runtime._DATA_QUIESCENCE_POLL_SECONDS", 0)
    monkeypatch.setattr(
        runtime,
        "_validate_data",
        lambda _target: events.append("validate") or {"accepted": True},
    )

    def close(candidate, **_kwargs):
        events.append(f"close:{candidate.pid}")
        candidate.returncode = 0
        return True

    monkeypatch.setattr(runtime, "_close_process_window", close)

    assert runtime._run_updater(
        tmp_path / "数据更新.exe", tmp_path,
        trigger="manual", target="2026-08-18",
    ) == 0
    assert events == ["validate", "close:42"]


def test_running_updater_never_closes_when_vendor_data_did_not_change(
    tmp_path, monkeypatch,
) -> None:
    runtime = FreeStockDBRuntime()
    process = SimpleNamespace(pid=42, returncode=None)
    process.poll = lambda: process.returncode
    polls = 0

    def wait(_seconds):
        nonlocal polls
        polls += 1
        if polls == 3:
            process.returncode = 0
        return False

    monkeypatch.setattr(
        "quantmaster.data.free_stockdb_runtime.subprocess.Popen",
        lambda *_args, **_kwargs: process,
    )
    monkeypatch.setattr(runtime, "_data_fingerprint", lambda _root: (("same", 1, 1),))
    monkeypatch.setattr(runtime._stop, "wait", wait)
    monkeypatch.setattr("quantmaster.data.free_stockdb_runtime._DATA_QUIESCENCE_POLL_SECONDS", 0)
    monkeypatch.setattr(
        runtime, "_validate_data", lambda _target: pytest.fail("unchanged data was validated"),
    )
    monkeypatch.setattr(
        runtime, "_close_process_window", lambda *_args, **_kwargs: pytest.fail(
            "unchanged data closed updater",
        ),
    )

    assert runtime._run_updater(
        tmp_path / "数据更新.exe", tmp_path,
        trigger="manual", target="2026-08-18",
    ) == 0


def test_failed_live_validation_waits_for_a_new_stable_data_change(
    tmp_path, monkeypatch,
) -> None:
    runtime = FreeStockDBRuntime()
    process = SimpleNamespace(pid=42, returncode=None)
    process.poll = lambda: process.returncode
    fingerprints = iter((
        (("before", 1, 1),),
        (("first", 2, 2),), (("first", 2, 2),),
        (("second", 3, 3),), (("second", 3, 3),),
    ))
    validations = iter(({"accepted": False}, {"accepted": True}))
    events: list[str] = []

    monkeypatch.setattr(
        "quantmaster.data.free_stockdb_runtime.subprocess.Popen",
        lambda *_args, **_kwargs: process,
    )
    monkeypatch.setattr(runtime, "_data_fingerprint", lambda _root: next(fingerprints))
    monkeypatch.setattr(runtime._stop, "wait", lambda _seconds: False)
    monkeypatch.setattr("quantmaster.data.free_stockdb_runtime._DATA_STABILITY_SECONDS", 0)
    monkeypatch.setattr("quantmaster.data.free_stockdb_runtime._DATA_QUIESCENCE_POLL_SECONDS", 0)
    monkeypatch.setattr(
        runtime,
        "_validate_data",
        lambda _target: events.append("validate") or next(validations),
    )

    def close(candidate, **_kwargs):
        events.append("close")
        candidate.returncode = 0
        return True

    monkeypatch.setattr(runtime, "_close_process_window", close)

    assert runtime._run_updater(
        tmp_path / "数据更新.exe", tmp_path,
        trigger="manual", target="2026-08-18",
    ) == 0
    assert events == ["validate", "validate", "close"]


def test_normal_window_close_targets_only_the_tracked_process(monkeypatch) -> None:
    process = SimpleNamespace(pid=314, returncode=None)
    process.poll = lambda: process.returncode
    posted: list[int] = []

    def wait(timeout):
        assert timeout == 3
        process.returncode = 0
        return 0

    process.wait = wait
    monkeypatch.setattr(
        FreeStockDBRuntime,
        "_post_windows_close",
        staticmethod(lambda pid: posted.append(pid) or True),
    )

    assert FreeStockDBRuntime._close_process_window(process, timeout=3) is True
    assert posted == [314]


def test_missing_updater_window_is_preserved_without_force_termination(monkeypatch) -> None:
    process = SimpleNamespace(pid=314, returncode=None)
    process.poll = lambda: process.returncode
    process.terminate = lambda: pytest.fail("normal close used terminate")
    process.kill = lambda: pytest.fail("normal close used kill")
    monkeypatch.setattr(
        FreeStockDBRuntime, "_post_windows_close", staticmethod(lambda _pid: False),
    )

    assert FreeStockDBRuntime._close_process_window(process) is False
    assert process.returncode is None


def test_successful_update_invalidates_stockdb_sdk_clients(monkeypatch) -> None:
    runtime = FreeStockDBRuntime()
    invalidated: list[bool] = []

    monkeypatch.setattr(
        "quantmaster.data.free_stockdb_source._invalidate_sdk_clients",
        lambda: invalidated.append(True),
    )
    monkeypatch.setattr(
        "quantmaster.after_close.service.reset_after_close_service", lambda: None,
    )
    monkeypatch.setattr(
        "quantmaster.rotation.etf_research.reset_etf_research_service", lambda: None,
    )
    monkeypatch.setattr(runtime, "_record_update", lambda *_args: None)
    monkeypatch.setattr(runtime, "_emit_update_event", lambda *_args: None)

    assert runtime._finish_success(
        target="2026-08-07", validation={"warnings": [], "actual_session": "2026-08-07"},
        code=0, attempt=1, trigger="manual",
    )
    assert invalidated == [True]


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
    assert status["market_timezone"] == "Asia/Shanghai"
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
        "accepted": False, "complete": False,
        "warnings": [], "issues": ["目标日截面为零"],
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


def test_partial_target_session_finishes_with_warning_instead_of_blocking(
    tmp_path, monkeypatch,
) -> None:
    runtime = FreeStockDBRuntime()
    runtime._owner = True
    monkeypatch.setenv("QM_FREE_STOCKDB_CONTROL_PATH", str(tmp_path / "control.sqlite"))
    monkeypatch.setattr(runtime, "_marker_path", lambda: tmp_path / "update.json")
    emitted = []
    monkeypatch.setattr(runtime, "_emit_update_event", lambda *args: emitted.append(args))
    validation = {
        "target_session": "2026-08-10", "actual_session": "2026-08-10",
        "observed_symbols": 5493, "required_ohlcv_ratio": 1.0,
        "accepted": True, "complete": False, "issues": [],
        "warnings": ["45 只缺口将交由后续混合数据源补齐"],
    }
    monkeypatch.setattr(runtime, "_validate_data", lambda _target: validation)

    assert runtime.update_now("manual", target_session="2026-08-10") is True

    assert runtime.status()["validated_session"] == "2026-08-10"
    assert runtime.status()["update_result"] == "success"
    assert "可继续扫描或稍后重试" in runtime.status()["message"]
    assert emitted == [(
        "market_session_partial", "2026-08-10",
        {"target_session": "2026-08-10", "validation": validation,
         "trigger": "manual"},
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


def test_control_command_contains_native_sdk_failure(tmp_path, monkeypatch) -> None:
    class NativeTimeout(Exception):
        pass

    control_path = tmp_path / "control.sqlite"
    monkeypatch.setenv("QM_FREE_STOCKDB_CONTROL_PATH", str(control_path))
    runtime = FreeStockDBRuntime()
    runtime._owner = True
    assert runtime.request_update("manual") is True
    monkeypatch.setattr(
        runtime, "update_now", lambda *_args, **_kwargs: (_ for _ in ()).throw(
            NativeTimeout("Connect timeout")
        ),
    )

    assert runtime._process_command() is True
    command = runtime._ensure_control()._conn().execute(
        "SELECT status,result_json FROM commands"
    ).fetchone()
    assert command["status"] == "completed"
    assert json.loads(command["result_json"])["status"] == "failed"


def test_scheduler_survives_unexpected_cycle_failure(
    isolated_config, tmp_path, monkeypatch,
) -> None:
    class NativeTimeout(Exception):
        pass

    isolated_config.data.free_stockdb_auto_update = False
    isolated_config.data.free_stockdb_managed = False
    monkeypatch.setenv(
        "QM_FREE_STOCKDB_CONTROL_PATH", str(tmp_path / "control.sqlite"),
    )
    runtime = FreeStockDBRuntime()
    attempts = 0

    def process_command():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise NativeTimeout("Connect timeout")
        runtime._stop.set()
        return False

    monkeypatch.setattr(runtime, "_process_command", process_command)
    monkeypatch.setattr(runtime._stop, "wait", lambda _seconds: False)

    runtime._scheduler()

    assert attempts == 2
    assert runtime.status()["state"] == "degraded"
    assert runtime.status()["update_result"] == "failed"


def test_scheduler_restarts_unavailable_managed_service(
    isolated_config, monkeypatch,
) -> None:
    isolated_config.data.free_stockdb_auto_update = False
    isolated_config.data.free_stockdb_managed = True
    runtime = FreeStockDBRuntime()
    runtime._owner = True
    starts: list[bool] = []
    cycles = 0

    def process_command() -> bool:
        nonlocal cycles
        cycles += 1
        if cycles > 1:
            runtime._stop.set()
        return False

    monkeypatch.setattr(runtime, "_process_command", process_command)
    monkeypatch.setattr(runtime, "_listening", lambda: False)
    monkeypatch.setattr(runtime, "_start_service", lambda: starts.append(True) or True)
    monkeypatch.setattr(runtime._stop, "wait", lambda _seconds: False)

    runtime._scheduler()

    assert starts == [True]


def test_supervise_service_does_not_restart_running_service(
    isolated_config, monkeypatch,
) -> None:
    isolated_config.data.free_stockdb_managed = True
    isolated_config.data.free_stockdb_auto_update = False
    runtime = FreeStockDBRuntime()
    runtime._owner = True
    starts: list[bool] = []
    cycles = 0

    def process_command() -> bool:
        nonlocal cycles
        cycles += 1
        if cycles > 1:
            runtime._stop.set()
        return False

    monkeypatch.setattr(runtime, "_listening", lambda: True)
    monkeypatch.setattr(runtime, "_start_service", lambda: starts.append(True))
    monkeypatch.setattr(runtime, "_process_command", process_command)
    monkeypatch.setattr(runtime._stop, "wait", lambda _s: False)

    runtime._scheduler()

    assert starts == []


def test_supervise_service_backoff_after_crash_loop(
    isolated_config, monkeypatch,
) -> None:
    from quantmaster.data.free_stockdb_runtime import (
        _SERVICE_CHECK_SECONDS,
        _SERVICE_RESTART_BACKOFF_BASE_SECONDS,
    )

    isolated_config.data.free_stockdb_managed = True
    isolated_config.data.free_stockdb_auto_update = False
    runtime = FreeStockDBRuntime()
    runtime._owner = True
    starts: list[bool] = []
    clock = [1000.0]
    monkeypatch.setattr(
        "quantmaster.data.free_stockdb_runtime.time.monotonic",
        lambda: clock[0],
    )
    monkeypatch.setattr(runtime, "_listening", lambda: False)
    monkeypatch.setattr(
        runtime, "_start_service", lambda: starts.append(True) or False,
    )

    def check() -> None:
        runtime._last_service_check = clock[0] - _SERVICE_CHECK_SECONDS
        runtime._supervise_service(isolated_config.data)

    check()
    assert starts == [True]
    assert runtime.status()["service_restart_backoff_seconds"] == (
        _SERVICE_RESTART_BACKOFF_BASE_SECONDS
    )

    clock[0] += _SERVICE_RESTART_BACKOFF_BASE_SECONDS - 1
    check()
    assert starts == [True]

    clock[0] += 1
    check()
    assert starts == [True, True]
    assert runtime.status()["service_restart_backoff_seconds"] == (
        _SERVICE_RESTART_BACKOFF_BASE_SECONDS * 2
    )

    clock[0] += _SERVICE_RESTART_BACKOFF_BASE_SECONDS * 2
    check()
    assert starts == [True, True, True]
    assert runtime.status()["service_restart_backoff_seconds"] == (
        _SERVICE_RESTART_BACKOFF_BASE_SECONDS * 4
    )


def test_manual_service_retry_reset_clears_backoff_and_retries(
    isolated_config, monkeypatch,
) -> None:
    from quantmaster.data.free_stockdb_runtime import _SERVICE_CHECK_SECONDS

    isolated_config.data.free_stockdb_managed = True
    isolated_config.data.free_stockdb_auto_update = False
    runtime = FreeStockDBRuntime()
    runtime._owner = True
    runtime._restart_failures = 3
    clock = [1000.0]
    runtime._last_restart_fail = clock[0] - 1
    runtime._last_service_check = clock[0]
    starts: list[bool] = []

    monkeypatch.setattr(
        "quantmaster.data.free_stockdb_runtime.time.monotonic",
        lambda: clock[0],
    )
    monkeypatch.setattr(runtime, "_listening", lambda: False)
    monkeypatch.setattr(
        runtime, "_start_service", lambda: starts.append(True) or False,
    )

    runtime._ensure_control().enqueue("reset_service_retry", "manual")
    assert runtime._process_command() is True
    assert runtime._restart_failures == 0
    assert runtime.status()["service_restart_backoff_seconds"] == 0
    assert runtime.status()["service_restart_next_at"] == ""

    runtime._last_service_check = clock[0] - _SERVICE_CHECK_SECONDS
    runtime._supervise_service(isolated_config.data)

    assert starts == [True]
    assert runtime._restart_failures == 1


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
    assert validation["accepted"] is True
    assert validation["complete"] is True
    assert validation["actual_session"] == "2026-08-07"
    assert validation["symbol_ratio"] == 1.0


def test_full_market_validation_contains_native_sdk_timeout(
    isolated_config, monkeypatch,
) -> None:
    from quantmaster.data.free_stockdb_source import FreeStockDBSource
    from quantmaster.data.instruments import InstrumentStore

    class NativeTimeout(Exception):
        pass

    monkeypatch.setattr(
        InstrumentStore,
        "list",
        lambda *_args, **_kwargs: [
            SimpleNamespace(symbol="000001.SZ", status="listed", exchange="SZ"),
        ],
    )
    monkeypatch.setattr(
        FreeStockDBSource,
        "daily_cross_section",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(NativeTimeout("Connect timeout")),
    )

    validation = FreeStockDBRuntime()._validate_data("2026-08-07")

    assert validation["accepted"] is False
    assert validation["complete"] is False
    assert validation["issues"] == [
        "读取 free-stockdb 验证截面失败：NativeTimeout: Connect timeout",
    ]


def test_full_market_validation_rejects_coverage_below_operational_floor(
    isolated_config, monkeypatch,
) -> None:
    from quantmaster.data.free_stockdb_source import FreeStockDBSource
    from quantmaster.data.instruments import InstrumentStore

    instruments = [
        SimpleNamespace(symbol=f"{index:06d}.SZ", status="listed", exchange="SZ")
        for index in range(10)
    ]
    monkeypatch.setattr(InstrumentStore, "list", lambda *_args, **_kwargs: instruments)

    def cross_section(_self, symbols, _start, _end):
        selected = symbols[:8]
        return pd.DataFrame({
            "symbol": selected,
            "date": [pd.Timestamp("2026-08-07")] * len(selected),
            "open": [1.0] * len(selected), "high": [1.0] * len(selected),
            "low": [1.0] * len(selected), "close": [1.0] * len(selected),
            "volume": [1.0] * len(selected),
        })

    monkeypatch.setattr(FreeStockDBSource, "daily_cross_section", cross_section)

    validation = FreeStockDBRuntime()._validate_data("2026-08-07")
    assert validation["accepted"] is False
    assert validation["complete"] is False
    assert validation["symbol_ratio"] == 0.8
    assert validation["missing_symbol_count"] == 2
    assert any("低于更新验收线 98%" in issue for issue in validation["issues"])
    assert any("后续混合数据源补齐" in warning for warning in validation["warnings"])


def test_full_market_validation_accepts_5493_of_5538_with_warning(
    isolated_config, monkeypatch,
) -> None:
    from quantmaster.data.free_stockdb_source import FreeStockDBSource
    from quantmaster.data.instruments import InstrumentStore

    instruments = [
        SimpleNamespace(symbol=f"{index:06d}.SZ", status="listed", exchange="SZ")
        for index in range(5538)
    ]
    monkeypatch.setattr(InstrumentStore, "list", lambda *_args, **_kwargs: instruments)

    def cross_section(_self, symbols, _start, _end):
        selected = [symbol for symbol in symbols if int(symbol[:6]) < 5493]
        return pd.DataFrame({
            "symbol": selected,
            "date": [pd.Timestamp("2026-08-10")] * len(selected),
            "open": [1.0] * len(selected), "high": [1.0] * len(selected),
            "low": [1.0] * len(selected), "close": [1.0] * len(selected),
            "volume": [1.0] * len(selected),
        })

    monkeypatch.setattr(FreeStockDBSource, "daily_cross_section", cross_section)

    validation = FreeStockDBRuntime()._validate_data("2026-08-10")

    assert validation["accepted"] is True
    assert validation["complete"] is False
    assert validation["observed_symbols"] == 5493
    assert validation["expected_trading_symbols"] == 5538
    assert validation["symbol_ratio"] > 0.99
    assert validation["missing_symbol_count"] == 45
    assert validation["issues"] == []
    assert any("45 只缺口" in warning for warning in validation["warnings"])


def test_full_market_validation_accepts_only_explicit_suspension_evidence(
    isolated_config, monkeypatch,
) -> None:
    from quantmaster.data import instrument_snapshots
    from quantmaster.data.free_stockdb_source import FreeStockDBSource
    from quantmaster.data.instruments import InstrumentStore

    instruments = [
        SimpleNamespace(symbol="000001.SZ", status="listed", exchange="SZ"),
        SimpleNamespace(symbol="000002.SZ", status="listed", exchange="SZ"),
    ]
    isolated_config.data.tushare_token = "test-token"
    monkeypatch.setattr(InstrumentStore, "list", lambda *_args, **_kwargs: instruments)
    monkeypatch.setattr(
        FreeStockDBSource,
        "daily_cross_section",
        lambda _self, _symbols, _start, _end: pd.DataFrame({
            "symbol": ["000001.SZ"],
            "date": [pd.Timestamp("2026-08-07")],
            "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0],
            "volume": [1.0],
        }),
    )
    monkeypatch.setattr(
        instrument_snapshots,
        "load_or_fetch_suspension_snapshot",
        lambda _source, _date: {
            "source": "tushare:suspend_d",
            "contract": "tushare-suspend_d-trade-date-v1",
            "symbols": ["000002.SZ"],
            "content_hash": "suspension-proof",
        },
    )

    validation = FreeStockDBRuntime()._validate_data("2026-08-07")

    assert validation["accepted"] is True
    assert validation["complete"] is True
    assert validation["catalog_symbols"] == 2
    assert validation["expected_trading_symbols"] == 1
    assert validation["observed_symbols"] == 1
    assert validation["excused_suspended_symbols"] == ["000002.SZ"]
    assert validation["suspension_evidence"]["content_hash"] == "suspension-proof"


def test_self_signed_suspension_payload_cannot_reduce_expected_denominator(
    isolated_config, monkeypatch,
) -> None:
    from quantmaster.data.free_stockdb_source import FreeStockDBSource
    from quantmaster.data.instruments import InstrumentStore
    from quantmaster.data.tushare_source import TushareSource

    isolated_config.data.tushare_token = "test-token"
    instruments = [
        SimpleNamespace(symbol="000001.SZ", status="listed", exchange="SZ"),
        SimpleNamespace(symbol="000002.SZ", status="listed", exchange="SZ"),
    ]
    monkeypatch.setattr(InstrumentStore, "list", lambda *_args, **_kwargs: instruments)
    monkeypatch.setattr(
        FreeStockDBSource,
        "daily_cross_section",
        lambda _self, _symbols, _start, _end: pd.DataFrame({
            "symbol": ["000001.SZ"], "date": [pd.Timestamp("2026-08-07")],
            "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0],
            "volume": [1.0],
        }),
    )
    monkeypatch.setattr(
        TushareSource,
        "suspension_snapshot",
        lambda _self, _date: {
            "schema_version": 2,
            "source": "user:self-signed",
            "contract": "self-signed-suspension-v1",
            "trade_date": "2026-08-07",
            "acquired_at": "2026-08-07T07:01:00+00:00",
            "rows": [], "symbols": ["000002.SZ"], "content_hash": "x" * 64,
        },
    )

    validation = FreeStockDBRuntime()._validate_data("2026-08-07")

    assert validation["accepted"] is False
    assert validation["complete"] is False
    assert validation["expected_trading_symbols"] == 2
    assert validation["excused_suspended_symbols"] == []
    assert any("低于更新验收线 98%" in issue for issue in validation["issues"])
    assert any("停牌证据不可用" in warning for warning in validation["warnings"])


def test_full_market_validation_rejects_nonfinite_and_impossible_ohlcv(
    isolated_config, monkeypatch,
) -> None:
    from quantmaster.data.free_stockdb_source import FreeStockDBSource
    from quantmaster.data.instruments import InstrumentStore

    instruments = [
        SimpleNamespace(symbol="000001.SZ", status="listed", exchange="SZ"),
        SimpleNamespace(symbol="000002.SZ", status="listed", exchange="SZ"),
    ]
    monkeypatch.setattr(InstrumentStore, "list", lambda *_args, **_kwargs: instruments)

    def cross_section(_self, symbols, _start, _end):
        return pd.DataFrame({
            "symbol": symbols,
            "date": [pd.Timestamp("2026-08-07")] * len(symbols),
            "open": [1.0, -1.0], "high": [1.0, 1.0],
            "low": [1.0, 99.0], "close": [float("inf"), 50.0],
            "volume": [1.0, -1.0],
        })

    monkeypatch.setattr(FreeStockDBSource, "daily_cross_section", cross_section)

    validation = FreeStockDBRuntime()._validate_data("2026-08-07")
    assert validation["accepted"] is False
    assert validation["complete"] is False
    assert validation["required_ohlcv_ratio"] == 0.0
    assert validation["invalid_ohlcv"]["nonfinite_rows"] == 1
