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
    manager = ConfigManager(tmp_path / "config.yaml", tmp_path / "backups", FakeCredentials())
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
    with pytest.raises(ValueError, match="股票池名称"):
        SettingsUpdate.model_validate(raw)
    set_config(None)


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


def test_settings_api_requires_local_csrf_and_never_returns_secret(monkeypatch, tmp_path):
    from quantmaster.server import management

    manager = ConfigManager(tmp_path / "config.yaml", tmp_path / "backups", FakeCredentials())
    monkeypatch.setattr(management, "settings_manager", manager)
    client = TestClient(app)
    response = client.get("/api/settings")
    assert response.status_code == 200
    assert "api_key" not in response.text and "tushare_token" not in response.text
    payload = {key: response.json()[key]
               for key in ("config_version", "llm", "data", "trade", "server")}
    assert client.post("/api/settings/validate", json=payload).status_code == 403
    csrf = response.json()["csrf_token"]
    assert client.post("/api/settings/validate", json=payload,
                       headers={"X-CSRF-Token": csrf}).status_code == 200
    invalid = {**payload, "llm": {**payload["llm"], "temperature": 99},
               "secrets": {"llm": {"action": "replace", "value": "never-echo-this"}}}
    rejected = client.put("/api/settings", json=invalid, headers={"X-CSRF-Token": csrf})
    assert rejected.status_code == 422
    assert "never-echo-this" not in rejected.text
    remote = TestClient(app, client=("203.0.113.8", 50000))
    assert remote.get("/api/settings").status_code == 403


def test_automation_channel_credentials_require_local_csrf():
    client = TestClient(app)
    rejected = client.post(
        "/api/automation/channels/feishu/config",
        json={"app_id": "cli_test", "app_secret": "must-not-echo"},
    )
    assert rejected.status_code == 403
    assert "must-not-echo" not in rejected.text
    remote = TestClient(app, client=("203.0.113.8", 50000))
    assert remote.post("/api/automation/channels/weixin/login").status_code == 403
