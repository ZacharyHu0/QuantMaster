from __future__ import annotations

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

    with pytest.raises(ValueError):
        DataSettings(free_stockdb_update_time="25:00")


def test_managed_endpoint_uses_configured_loopback_port(monkeypatch) -> None:
    config = SimpleNamespace(data=SimpleNamespace(
        free_stockdb_url="http://localhost:7900",
    ))
    monkeypatch.setattr("quantmaster.data.free_stockdb_runtime.get_config", lambda: config)

    assert FreeStockDBRuntime._endpoint() == ("localhost", 7900)


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
    monkeypatch.setattr(runtime, "_run_updater", lambda *_args: 0)
    monkeypatch.setattr(
        "quantmaster.data.resilience.PROVIDER_HEALTH.reset",
        lambda lane: events.append(f"reset:{lane}"),
    )

    assert runtime.update_now() is True
    assert events == ["stop", "start", "reset:free-stockdb"]
    assert runtime._last_update_date() == datetime.now(
        ZoneInfo("Asia/Shanghai")
    ).date().isoformat()
