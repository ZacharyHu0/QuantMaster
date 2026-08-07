from __future__ import annotations

import threading
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

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


def test_reload_worker_attaches_without_taking_process_ownership(monkeypatch) -> None:
    runtime = FreeStockDBRuntime()
    monkeypatch.setattr(runtime, "_listening", lambda: True)

    assert runtime.attach_to_supervisor() is True
    status = runtime._status
    assert status["state"] == "running"
    assert status["managed"] is True
    assert status["supervised"] is True
    assert runtime._process is None
    assert runtime._thread is None


def test_managed_service_uses_server_mode_without_opening_vendor_page(
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
    monkeypatch.setattr("quantmaster.data.free_stockdb_runtime.subprocess.Popen", popen)

    assert runtime._start_service() is True
    command, kwargs = calls[0]
    assert command == [str(executable), str(config_path)]
    assert "-d" not in command
    assert kwargs["cwd"] == root


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
    monkeypatch.setattr(
        "quantmaster.data.resilience.PROVIDER_HEALTH.reset",
        lambda lane: events.append(f"reset:{lane}"),
    )

    assert runtime.update_now() is True
    assert events == ["stop", "start", "reset:free-stockdb"]
    assert runtime._last_update_date() == datetime.now(
        ZoneInfo("Asia/Shanghai")
    ).date().isoformat()


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
