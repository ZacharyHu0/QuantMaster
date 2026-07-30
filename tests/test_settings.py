"""GUI 配置优先级、凭据、快照和 API 安全测试。"""

from __future__ import annotations

import json

import pytest
import yaml
from fastapi.testclient import TestClient

from quantmaster.config import load_config, set_config
from quantmaster.credentials import CredentialError
from quantmaster.server.app import app
from quantmaster.settings import ConfigManager, SettingsUpdate, document_from_config


class FakeCredentials:
    def __init__(self, available=True):
        self.available = available
        self.values = {}

    def get(self, target):
        if not self.available:
            raise CredentialError("unavailable")
        return self.values.get(target)

    def set(self, target, value):
        if not self.available:
            raise CredentialError("unavailable")
        self.values[target] = value

    def delete(self, target):
        if not self.available:
            raise CredentialError("unavailable")
        self.values.pop(target, None)


def _update(manager, **extra):
    base = document_from_config(manager.load()).model_dump()
    return SettingsUpdate.model_validate({**base, **extra})


def test_old_and_gui_managed_environment_priority(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    path.write_text("llm:\n  model: yaml-model\n", encoding="utf-8")
    monkeypatch.setenv("QM_LLM_MODEL", "env-model")
    assert load_config(path).llm.model == "env-model"

    path.write_text("managed_by_gui: true\nllm:\n  model: yaml-model\n", encoding="utf-8")
    assert load_config(path).llm.model == "yaml-model"


def test_explicit_clear_blocks_environment_secret(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    path.write_text(
        "managed_by_gui: true\nllm:\n  provider: openai\n  model: x\n"
        "_secrets:\n  llm:\n    state: cleared\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("OPENAI_API_KEY", "must-not-return")
    assert load_config(path).llm.api_key == ""


def test_secret_replace_redaction_and_snapshots(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("data:\n  root: data\n", encoding="utf-8")
    credentials = FakeCredentials()
    manager = ConfigManager(path, tmp_path / "backups", credentials)
    update = _update(manager)
    update.secrets.llm.action = "replace"
    update.secrets.llm.value = "highly-secret-value"
    result = manager.save(update)
    raw = path.read_text(encoding="utf-8")
    assert "highly-secret-value" not in raw
    assert result["snapshot_id"]
    assert manager.public()["secrets"]["llm"]["configured"] is True
    assert all("highly-secret-value" not in item.read_text(encoding="utf-8")
               for item in (tmp_path / "backups").glob("*.json"))
    set_config(None)


def test_plaintext_fallback_requires_confirmation(tmp_path):
    path = tmp_path / "config.yaml"
    manager = ConfigManager(path, tmp_path / "backups", FakeCredentials(available=False))
    update = _update(manager)
    update.secrets.tushare.action = "replace"
    update.secrets.tushare.value = "token-value"
    with pytest.raises(CredentialError, match="明确确认"):
        manager.save(update)
    update.allow_plaintext_secrets = True
    manager.save(update)
    assert yaml.safe_load(path.read_text(encoding="utf-8"))["data"]["tushare_token"] == "token-value"
    assert "token-value" not in json.dumps(manager.list_snapshots(), ensure_ascii=False)
    set_config(None)


def test_automation_settings_are_normalized_and_validated(tmp_path):
    data_root = tmp_path / "data"
    pool_dir = data_root / "universe"
    pool_dir.mkdir(parents=True)
    (pool_dir / "core_pool.json").write_text('["600519.SH"]', encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({"data": {"root": str(data_root)}}), encoding="utf-8")
    manager = ConfigManager(config_path, tmp_path / "backups", FakeCredentials())
    raw = _update(manager).model_dump()
    raw["automation"].update({
        "timezone": "Asia/Shanghai",
        "primary_universe": "core_pool",
        "watchlist": [" 600519 ", "600519.sh", "000001"],
        "sentinel_indices": ["sh000300", "399006"],
    })
    update = SettingsUpdate.model_validate(raw)
    manager.save(update)

    loaded = document_from_config(manager.load())
    assert loaded.automation.watchlist == ["600519.SH", "000001.SZ"]
    assert loaded.automation.sentinel_indices == ["000300.SH", "399006.SZ"]

    raw = loaded.model_dump()
    raw["automation"]["timezone"] = "Mars/Colony"
    with pytest.raises(ValueError, match="IANA"):
        SettingsUpdate.model_validate(raw)
    raw = loaded.model_dump()
    raw["automation"]["primary_universe"] = "../escape"
    with pytest.raises(ValueError, match="候选名称"):
        SettingsUpdate.model_validate(raw)
    raw = loaded.model_dump()
    raw["automation"]["primary_universe"] = "csi800"
    with pytest.raises(ValueError, match="只读"):
        SettingsUpdate.model_validate(raw)
    set_config(None)


@pytest.mark.parametrize("host", ["0.0.0.0", "::", "192.168.1.8", "quant.local"])
def test_server_settings_reject_non_loopback_hosts(host):
    raw = document_from_config(load_config()).model_dump()
    raw["server"]["host"] = host
    with pytest.raises(ValueError, match="回环地址"):
        SettingsUpdate.model_validate(raw)


def test_snapshot_diff_rollback_preserves_current_secret(tmp_path):
    path = tmp_path / "config.yaml"
    credentials = FakeCredentials()
    manager = ConfigManager(path, tmp_path / "backups", credentials)
    first = _update(manager)
    first.trade.lot_size = 100
    first.secrets.llm.action = "replace"
    first.secrets.llm.value = "keep-this"
    manager.save(first)
    snap = manager.create_named_snapshot("baseline")
    second = _update(manager)
    second.trade.lot_size = 200
    manager.save(second)
    assert any(row["field"] == "trade.lot_size" for row in manager.snapshot_diff(snap["id"]))
    manager.rollback(snap["id"])
    assert manager.load().trade.lot_size == 100
    assert manager.load().llm.api_key == "keep-this"
    set_config(None)


def test_news_and_lab_changes_report_hot_apply_fields(tmp_path):
    manager = ConfigManager(tmp_path / "config.yaml", tmp_path / "backups", FakeCredentials())
    update = _update(manager)
    update.news.annotation_batch_size = 7
    update.news.annotation_items_per_run = 35
    update.lab.enabled = False
    update.lab.horizons = [3, 7]
    result = manager.save(update)

    assert result["restart_required"] == []
    assert {
        "news.annotation_batch_size", "news.annotation_items_per_run",
        "lab.enabled", "lab.horizons",
    }.issubset(result["changed_fields"])
    assert manager.public()["config_revision"] == result["config_revision"]
    assert manager.load().lab.horizons == [3, 7]

    server = _update(manager)
    server.server.port += 1
    restarted = manager.save(server)
    assert restarted["restart_required"] == ["server.port"]
    set_config(None)


def test_settings_reject_missing_pool_and_invalid_lab_window(tmp_path):
    manager = ConfigManager(tmp_path / "config.yaml", tmp_path / "backups", FakeCredentials())
    raw = _update(manager).model_dump()
    raw.pop("secrets")
    raw.pop("allow_plaintext_secrets")
    raw["automation"]["primary_universe"] = "missing_pool"
    with pytest.raises(ValueError, match="不存在"):
        manager.validate(raw)

    raw = _update(manager).model_dump()
    raw["lab"]["window_end"] = raw["lab"]["window_start"]
    with pytest.raises(ValueError, match="不能相同"):
        SettingsUpdate.model_validate(raw)
    set_config(None)


def test_settings_api_requires_local_csrf_and_never_returns_secret(monkeypatch, tmp_path):
    from quantmaster.server import management

    manager = ConfigManager(tmp_path / "config.yaml", tmp_path / "backups", FakeCredentials())
    monkeypatch.setattr(management, "settings_manager", manager)
    client = TestClient(app)
    response = client.get("/api/v1/settings")
    assert response.status_code == 200
    assert "api_key" not in response.text and "tushare_token" not in response.text
    payload = {key: response.json()[key]
               for key in ("config_version", "llm", "data", "trade", "server")}
    assert client.post("/api/v1/settings/validate", json=payload).status_code == 403
    csrf = response.json()["csrf_token"]
    assert client.post("/api/v1/settings/validate", json=payload,
                       headers={"X-CSRF-Token": csrf}).status_code == 200
    invalid = {**payload, "llm": {**payload["llm"], "temperature": 99},
               "secrets": {"llm": {"action": "replace", "value": "never-echo-this"}}}
    rejected = client.put("/api/v1/settings", json=invalid, headers={"X-CSRF-Token": csrf})
    assert rejected.status_code == 422
    assert "never-echo-this" not in rejected.text
    remote = TestClient(app, client=("203.0.113.8", 50000))
    assert remote.get("/api/v1/settings").status_code == 403


def test_data_refresh_api_requires_preview_confirmation_and_supports_resume(monkeypatch):
    from quantmaster.data import maintenance

    class FakeRefreshManager:
        active = False

        def preview(self, scope, universe, start):
            return {"scope": scope, "universe": universe, "start": start or "2024-01-01",
                    "end": "2026-07-27", "total": 2, "unhealthy_sources": []}

        def create(self, scope, universe, start):
            return {"id": "job-1", "status": "running", "scope": scope}

        def latest(self):
            return {"id": "job-1", "status": "interrupted"}

        def get(self, job_id):
            return {"id": job_id, "status": "interrupted"}

        def cancel(self, job_id):
            return {"id": job_id, "status": "cancelling"}

        def resume(self, job_id):
            return {"id": job_id, "status": "running"}

    monkeypatch.setattr(maintenance, "data_refresh_manager", FakeRefreshManager())
    client = TestClient(app)
    assert client.post(
        "/api/v1/data/refresh/preview", json={"scope": "market"}).status_code == 403
    settings = client.get("/api/v1/settings").json()
    headers = {"X-CSRF-Token": settings["csrf_token"]}

    preview = client.post(
        "/api/v1/data/refresh/preview", json={"scope": "universe", "universe": "demo"},
        headers=headers,
    )
    assert preview.status_code == 200
    assert preview.json()["total"] == 2
    assert client.post(
        "/api/v1/data/refresh", json={"scope": "market"}, headers=headers,
    ).json()["status"] == "running"
    assert client.get("/api/v1/data/refresh/latest").json()["job"]["status"] == "interrupted"
    assert client.post(
        "/api/v1/data/refresh/job-1/resume", headers=headers,
    ).json()["status"] == "running"


def test_automation_channel_credentials_require_local_csrf():
    client = TestClient(app)
    rejected = client.post(
        "/api/v1/automation/channels/feishu/config",
        json={"app_id": "cli_test", "app_secret": "must-not-echo"},
    )
    assert rejected.status_code == 403
    assert "must-not-echo" not in rejected.text
    remote = TestClient(app, client=("203.0.113.8", 50000))
    assert remote.post("/api/v1/automation/channels/weixin/login").status_code == 403

    settings = client.get("/api/v1/settings").json()
    checked = client.post(
        "/api/v1/automation/channels/feishu/check",
        headers={"X-CSRF-Token": settings["csrf_token"]},
    )
    assert checked.status_code == 200
    assert set(checked.json()["stages"]) == {
        "credential", "runtime", "websocket", "event", "binding",
    }


def test_candidate_api_metadata_preview_and_reference_safe_changes(tmp_path, monkeypatch):
    from quantmaster.server import management

    data_root = tmp_path / "candidate-data"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({"data": {"root": str(data_root)}}), encoding="utf-8")
    manager = ConfigManager(config_path, tmp_path / "backups", FakeCredentials())
    monkeypatch.setattr(management, "settings_manager", manager)
    monkeypatch.setattr(
        management, "_apply_runtime", lambda result: {**result, "runtime": {}})
    monkeypatch.setattr(
        "quantmaster.lab.dataset.load_csi800_members_as_of",
        lambda as_of: {
            "as_of": as_of,
            "symbols": ["600519.SH", "000001.SZ"],
            "snapshot_dates": {"000300.SH": "2026-07-01", "000905.SH": "2026-07-02"},
        },
    )
    monkeypatch.setattr(
        "quantmaster.data.universe.index_universe",
        lambda symbol: ["688981.SH", "300750.SZ"] if symbol == "000688.SH" else [],
    )
    set_config(manager.load())
    client = TestClient(app)
    settings = client.get("/api/v1/settings").json()
    headers = {"X-CSRF-Token": settings["csrf_token"]}

    catalog_payload = client.get("/api/v1/settings/universes").json()
    catalog = catalog_payload["universes"]
    assert [(item["name"], item["kind"]) for item in catalog[:2]] == [
        ("demo", "fixed"), ("csi800", "dynamic"),
    ]
    presets = catalog_payload["index_presets"]
    assert [item["name"] for item in presets[:6]] == [
        "科创50", "科创100", "科创创业50", "半导体材料设备", "创业板指", "创业板50",
    ]
    assert all(item["preferred"] for item in presets[:6])
    assert {(item["name"], item["symbol"]) for item in presets[-3:]} == {
        ("沪深300", "000300.SH"), ("中证500", "000905.SH"),
        ("中证1000", "000852.SH"),
    }
    preset_preview = client.post(
        "/api/v1/settings/universes/preview",
        json={"kind": "index", "index_symbol": presets[0]["symbol"]}, headers=headers,
    )
    assert preset_preview.status_code == 200
    assert preset_preview.json()["symbols"] == ["688981.SH", "300750.SZ"]
    dynamic = client.get("/api/v1/settings/universes/csi800?as_of=2026-07-27")
    assert dynamic.status_code == 200
    assert dynamic.json()["snapshot_dates"]["000300.SH"] == "2026-07-01"

    preview = client.post(
        "/api/v1/settings/universes/preview",
        json={"kind": "manual", "symbols": ["600519", "600519.SH", "bad"]},
        headers=headers,
    ).json()
    assert preview["symbols"] == ["600519.SH"]
    assert preview["duplicates"][0]["symbol"] == "600519.SH"
    assert preview["errors"][0]["value"] == "bad"

    created = client.post(
        "/api/v1/settings/universes",
        json={"name": "core", "symbols": ["600519", "000001"]}, headers=headers,
    )
    assert created.status_code == 200
    detail = client.get("/api/v1/settings/universes/core").json()
    assert detail["symbols"] == ["600519.SH", "000001.SZ"]
    assert detail["members"][0]["name"] == "贵州茅台"

    update = _update(manager)
    update.automation.primary_universe = "core"
    update.lab.universe = "core"
    manager.save(update)
    renamed = client.post(
        "/api/v1/settings/universes/core/rename",
        json={"new_name": "renamed"}, headers=headers,
    )
    assert renamed.status_code == 200
    assert set(renamed.json()["updated_references"]) == {
        "automation.primary_universe", "lab.universe",
    }
    assert manager.load().automation.primary_universe == "renamed"

    blocked = client.delete("/api/v1/settings/universes/renamed", headers=headers)
    assert blocked.status_code == 409
    deleted = client.delete(
        "/api/v1/settings/universes/renamed?replacement=demo", headers=headers)
    assert deleted.status_code == 200
    assert manager.load().automation.primary_universe == "demo"
    assert manager.load().lab.universe == "demo"
