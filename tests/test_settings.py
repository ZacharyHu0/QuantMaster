"""GUI 配置优先级、凭据、快照和 API 安全测试。"""

from __future__ import annotations

import json
import os

import pytest
import yaml
from fastapi.testclient import TestClient

from quantmaster.config import load_config, set_config
from quantmaster.credentials import CredentialError, CredentialStore
from quantmaster.server.app import app
from quantmaster.settings import (
    AutomationSettings,
    ConfigManager,
    SettingsDocument,
    SettingsUpdate,
    document_from_config,
)


@pytest.fixture(scope="module", autouse=True)
def _stop_settings_jobs_after_module():
    yield
    from quantmaster.server.settings_jobs import shutdown_settings_jobs

    shutdown_settings_jobs()


def test_news_scan_interval_settings_defaults_and_bounds():
    settings = AutomationSettings()
    assert settings.fast_news_interval_minutes == 20
    assert settings.official_news_interval_minutes == 120
    assert settings.periodic_news_interval_minutes == 360
    with pytest.raises(ValueError):
        AutomationSettings(fast_news_interval_minutes=4)
    with pytest.raises(ValueError):
        AutomationSettings(periodic_news_interval_minutes=1441)


def test_diagnostic_vault_is_single_use_bounded_and_expires():
    from quantmaster.server.settings_jobs import _DiagnosticCredentialVault

    now = [100.0]
    vault = _DiagnosticCredentialVault(
        ttl_seconds=5, max_entries=1, clock=lambda: now[0],
    )
    document = SettingsDocument()
    reference = vault.put(document, "draft-secret")

    with pytest.raises(RuntimeError, match="队列已满"):
        vault.put(document, "other-secret")
    popped = vault.pop(reference)
    assert popped is not None and popped[1] == "draft-secret"
    assert vault.pop(reference) is None

    expired = vault.put(document, "expiring-secret")
    now[0] += 6
    assert vault.pop(expired) is None
    replacement = vault.put(document, "replacement-secret")
    assert vault.pop(replacement) is not None


def test_settings_page_exposes_news_scan_intervals():
    source = (
        __import__("pathlib").Path(__file__).parents[1]
        / "quantmaster" / "server" / "static" / "index.html"
    ).read_text(encoding="utf-8")
    assert 'name="automation.fast_news_interval_minutes"' in source
    assert 'name="automation.official_news_interval_minutes"' in source
    assert 'name="automation.periodic_news_interval_minutes"' in source


def test_model_diagnostic_cancel_is_compact_and_responsive():
    root = __import__("pathlib").Path(__file__).parents[1] / "quantmaster" / "server" / "static"
    script = (root / "settings.js").read_text(encoding="utf-8")
    styles = (root / "settings.css").read_text(encoding="utf-8")

    assert 'class="check-task-cancel"' in script
    assert 'aria-label="取消当前检测"' in script
    assert 'grid-template-columns: 6px minmax(0, 1fr) auto' in styles
    assert '.check-task-cancel { grid-column: 2; grid-row: 2;' in styles


def test_settings_page_exposes_online_provider_switches():
    source = (
        __import__("pathlib").Path(__file__).parents[1]
        / "quantmaster" / "server" / "static" / "index.html"
    ).read_text(encoding="utf-8")
    for name in (
        "data.free_stockdb_online_enabled",
        "data.akshare_enabled",
        "data.tushare_enabled",
        "data.yfinance_enabled",
    ):
        assert f'name="{name}"' in source


def test_online_provider_switches_round_trip_through_settings(tmp_path):
    manager = ConfigManager(tmp_path / "config.yaml", tmp_path / "backups", FakeCredentials())
    update = _update(manager)
    update.data.free_stockdb_online_enabled = True
    update.data.akshare_enabled = False
    update.data.tushare_enabled = False
    update.data.yfinance_enabled = False

    result = manager.save(update)
    public = manager.public()["data"]

    assert "data.free_stockdb_online_enabled" in result["changed_fields"]
    assert public["free_stockdb_online_enabled"] is True
    assert public["akshare_enabled"] is False
    assert public["tushare_enabled"] is False
    assert public["yfinance_enabled"] is False


def test_monotonic_revision_and_field_source_override(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    manager = ConfigManager(path, tmp_path / "backups", FakeCredentials())
    first = manager.save(_update(manager))
    second = manager.save(_update(manager))

    assert first["config_revision"] == 1
    assert second["config_revision"] == 2
    monkeypatch.setenv("QM_LLM_MODEL", "runtime-model")
    public = manager.public()
    assert public["config_revision"] == 2
    # The editable document remains the saved value.  Runtime precedence is
    # projected separately so an ENV override cannot silently rewrite YAML.
    assert public["llm"]["model"] == "claude-sonnet-5"
    assert public["field_sources"]["llm.model"] == {
        "source": "environment",
        "override": True,
        "environment": "QM_LLM_MODEL",
        "sensitive": False,
        "persisted": "claude-sonnet-5",
        "effective": "runtime-model",
        "drift": True,
    }


def test_null_config_path_uses_process_local_non_persistent_projection(monkeypatch):
    monkeypatch.setenv("QM_CONFIG_PATH", os.devnull)
    manager = ConfigManager()

    assert manager.path.name == f"quantmaster-null-{os.getpid()}.yaml"
    assert manager.check_state_path.name == f"quantmaster-null-{os.getpid()}.checks.json"


def test_gui_round_trip_preserves_extended_config_fields(tmp_path):
    manager = ConfigManager(tmp_path / "config.yaml", tmp_path / "backups", FakeCredentials())
    update = _update(manager)
    update.data.provider_retry_attempts = 7
    update.data.provider_retry_backoff = 1.5
    update.data.provider_retry_max_backoff = 19.0
    update.data.repair_daily_budget = 321
    update.data.repair_max_workers = 3
    update.lab.panel_cache_mb = 4096
    update.lab.feature_cache_gb = 12
    update.lab.gpu_memory_fraction = 0.65
    update.lab.walk_forward_train_days = 630
    update.lab.walk_forward_test_days = 210
    update.lab.walk_forward_step_days = 105
    update.lab.walk_forward_purge_days = 30
    update.lab.walk_forward_folds = 4
    update.lab.factor_correlation_threshold = 0.75
    manager.save(update)

    reloaded = document_from_config(manager.load())
    assert reloaded.data.provider_retry_attempts == 7
    assert reloaded.data.provider_retry_backoff == 1.5
    assert reloaded.data.provider_retry_max_backoff == 19.0
    assert reloaded.data.repair_daily_budget == 321
    assert reloaded.data.repair_max_workers == 3
    assert reloaded.lab.panel_cache_mb == 4096
    assert reloaded.lab.feature_cache_gb == 12
    assert reloaded.lab.gpu_memory_fraction == 0.65
    assert reloaded.lab.walk_forward_train_days == 630
    assert reloaded.lab.walk_forward_test_days == 210
    assert reloaded.lab.walk_forward_step_days == 105
    assert reloaded.lab.walk_forward_purge_days == 30
    assert reloaded.lab.walk_forward_folds == 4
    assert reloaded.lab.factor_correlation_threshold == 0.75


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


class FailSecondCredentials(FakeCredentials):
    def __init__(self):
        super().__init__()
        self.set_count = 0

    def set(self, target, value):
        self.set_count += 1
        if self.set_count == 2:
            raise CredentialError("second secret failed")
        super().set(target, value)


def test_secret_group_failure_cleans_staged_first_secret(tmp_path):
    path = tmp_path / "config.yaml"
    credentials = FailSecondCredentials()
    manager = ConfigManager(path, tmp_path / "backups", credentials)
    update = _update(manager)
    update.secrets.llm.action = "replace"
    update.secrets.llm.value = "llm-new-value"
    update.secrets.tushare.action = "replace"
    update.secrets.tushare.value = "tushare-new-value"

    with pytest.raises(CredentialError, match="second secret"):
        manager.save(update)

    assert not path.exists()
    assert credentials.values == {}


def test_runtime_status_read_does_not_construct_background_workers(monkeypatch):
    from quantmaster.server import management

    class SettingsProjection:
        @staticmethod
        def public():
            return {"config_revision": "fixture"}

    def forbidden(*_args, **_kwargs):
        pytest.fail("runtime status GET constructed a background worker")

    monkeypatch.setattr(management, "settings_manager", SettingsProjection())
    monkeypatch.setattr(
        "quantmaster.runtime.worker.runtime_worker_status",
        lambda: {"status": "running", "available": True},
    )
    monkeypatch.setattr("quantmaster.automation.runtime.get_runtime", forbidden)
    monkeypatch.setattr("quantmaster.lab.worker.get_worker", forbidden)
    monkeypatch.setattr(
        "quantmaster.data.free_stockdb_runtime.free_stockdb_runtime.status", forbidden,
    )

    result = management._runtime_status()

    assert result["config_revision"] == "fixture"
    assert result["worker"]["available"] is True


def _update(manager, **extra):
    base = document_from_config(manager.load()).model_dump()
    return SettingsUpdate.model_validate({**base, **extra})


def test_environment_override_remains_effective_for_gui_managed_config(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    monkeypatch.setenv("QM_LLM_MODEL", "env-model")
    monkeypatch.setenv("QM_LLM_REASONING_EFFORT", "low")
    path.write_text(
        "managed_by_gui: true\nllm:\n  model: yaml-model\n  reasoning_effort: high\n",
        encoding="utf-8",
    )
    managed = load_config(path)
    assert managed.llm.model == "env-model"
    assert managed.llm.reasoning_effort == "low"


def test_reasoning_effort_is_saved_and_validated_per_provider(tmp_path):
    manager = ConfigManager(tmp_path / "config.yaml", tmp_path / "backups", FakeCredentials())
    update = _update(manager)
    update.llm.reasoning_effort = "high"
    update.llm.max_concurrency = 3
    result = manager.save(update)

    assert "llm.reasoning_effort" in result["changed_fields"]
    assert "llm.max_concurrency" in result["changed_fields"]
    assert result["restart_required"] == []
    assert manager.public()["llm"]["reasoning_effort"] == "high"
    assert manager.public()["llm"]["max_concurrency"] == 3

    raw = _update(manager).model_dump()
    raw["news"]["annotation_reasoning_effort"] = "none"
    assert SettingsUpdate.model_validate(raw).news.annotation_reasoning_effort == "none"
    raw["llm"]["reasoning_effort"] = "none"
    with pytest.raises(ValueError, match="Anthropic"):
        SettingsUpdate.model_validate(raw)
    set_config(None)


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


@pytest.mark.parametrize("kind", [
    "llm-models", "llm-web-search", "tushare", "storage",
    "data-sources", "server", "lab",
])
def test_setting_check_results_persist_and_track_relevant_changes(tmp_path, kind):
    credentials = FakeCredentials()
    manager = ConfigManager(tmp_path / "config.yaml", tmp_path / "backups", credentials)
    document = document_from_config(manager.load())
    secrets = {
        "llm": "llm-secret-value",
        "tushare": "tushare-secret-value",
    }
    result = {
        "status": "warning",
        "message": f"{kind} 已检测",
        "latency_ms": 42,
        "checked_at": "2026-08-08T08:09:10+00:00",
        "details": {"category": "test"},
    }

    recorded = manager.record_check_result(kind, document, secrets, result)
    reloaded = ConfigManager(manager.path, manager.backup_dir, credentials)
    restored = reloaded.check_results(document, secrets)[kind]

    assert recorded["stale"] is False
    assert restored == {**result, "stale": False}
    state_text = reloaded.check_state_path.read_text(encoding="utf-8")
    assert "llm-secret-value" not in state_text
    assert "tushare-secret-value" not in state_text
    fingerprint_key = credentials.values[CredentialStore.settings_check_fingerprint_target()]
    assert fingerprint_key not in state_text

    changed = document.model_copy(deep=True)
    changed_secrets = dict(secrets)
    if kind == "llm-models":
        changed.llm.model = "another-model"
    elif kind == "llm-web-search":
        changed.llm.reasoning_effort = "high"
    elif kind == "tushare":
        changed_secrets["tushare"] = "replacement-token"
    elif kind == "storage":
        changed.data.root = str(tmp_path / "another-data-root")
    elif kind == "data-sources":
        changed.data.free_stockdb_url = "http://127.0.0.1:7999"
    elif kind == "server":
        changed.server.port += 1
    else:
        changed.lab.device = "cpu"

    assert reloaded.check_results(changed, changed_secrets)[kind]["stale"] is True


def test_setting_check_fingerprint_is_bound_to_private_credential_store_key(tmp_path):
    path = tmp_path / "config.yaml"
    backups = tmp_path / "backups"
    credentials = FakeCredentials()
    manager = ConfigManager(path, backups, credentials)
    document = document_from_config(manager.load())
    secret_values = {"llm": "candidate-api-key", "tushare": "candidate-token"}
    manager.record_check_result("tushare", document, secret_values, {"status": "success"})

    unrelated_credentials = FakeCredentials()
    reloaded = ConfigManager(path, backups, unrelated_credentials)

    assert reloaded.check_results(document, secret_values)["tushare"]["stale"] is True
    assert credentials.values[CredentialStore.settings_check_fingerprint_target()] != (
        unrelated_credentials.values[CredentialStore.settings_check_fingerprint_target()]
    )


def test_corrupt_setting_check_state_is_ignored(tmp_path):
    manager = ConfigManager(tmp_path / "config.yaml", tmp_path / "backups", FakeCredentials())
    manager.check_state_path.write_text("{not-json", encoding="utf-8")

    assert manager.public()["checks"] == {}


def test_adding_v1_keeps_compatible_gateway_secret(tmp_path):
    path = tmp_path / "config.yaml"
    credentials = FakeCredentials()
    manager = ConfigManager(path, tmp_path / "backups", credentials)
    first = _update(manager)
    first.llm.provider = "openai-compatible"
    first.llm.base_url = "https://gateway.test"
    first.secrets.llm.action = "replace"
    first.secrets.llm.value = "gateway-secret"
    manager.save(first)

    second = _update(manager)
    second.llm.base_url = "https://gateway.test/v1"
    result = manager.save(second)

    assert not any("凭据已保持为空" in warning for warning in result["warnings"])
    assert manager.load().llm.api_key == "gateway-secret"
    targets = {
        CredentialStore.llm_target("openai-compatible", endpoint)
        for endpoint in (
            "https://gateway.test",
            "https://gateway.test/v1",
            "https://gateway.test/v1/models",
            "https://gateway.test/v1/chat/completions",
        )
    }
    assert len(targets) == 1
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
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({"data": {"root": str(data_root)}}), encoding="utf-8")
    manager = ConfigManager(config_path, tmp_path / "backups", FakeCredentials())
    set_config(manager.load())
    from quantmaster.data.universe import save_universe

    save_universe("core_pool", ["600519.SH"])
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


def test_legacy_universe_is_explicit_sandbox_warning_in_settings(tmp_path):
    data_root = tmp_path / "data"
    universe_root = data_root / "universe"
    universe_root.mkdir(parents=True)
    (universe_root / "legacy_pool.json").write_text(
        json.dumps(["600519.SH"]), encoding="utf-8",
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({"data": {"root": str(data_root)}}), encoding="utf-8",
    )
    manager = ConfigManager(config_path, tmp_path / "backups", FakeCredentials())
    set_config(manager.load())
    raw = _update(manager).model_dump()
    raw["automation"]["primary_universe"] = "legacy_pool"

    result = manager.validate(SettingsUpdate.model_validate(raw))

    assert result["valid"] is True
    assert any("sandbox" in item and "legacy_pool" in item for item in result["warnings"])
    set_config(None)


def test_server_settings_reject_non_loopback_hosts():
    for host in ("0.0.0.0", "::", "192.168.1.8", "quant.local"):
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
    update.news.annotation_max_concurrency = 6
    update.news.annotation_items_per_run = 35
    update.news.annotation_timeout = 150
    update.news.annotation_model = "lightweight-model"
    update.news.annotation_reasoning_effort = "medium"
    update.lab.enabled = False
    update.lab.horizons = [3, 7]
    result = manager.save(update)

    assert result["restart_required"] == []
    assert {
        "news.annotation_batch_size", "news.annotation_max_concurrency",
        "news.annotation_items_per_run",
        "news.annotation_timeout", "news.annotation_model",
        "lab.enabled", "lab.horizons",
    }.issubset(result["changed_fields"])
    assert manager.public()["news"]["annotation_max_concurrency"] == 6
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


def test_free_stockdb_sidecar_api_requires_local_csrf_and_reports_queue(monkeypatch):
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

    local = TestClient(app)
    status = {
        "state": "queued", "phase": "queued", "message": "queued",
        "update_capability": "native_only",
    }
    monkeypatch.setattr(free_stockdb_runtime, "status", lambda: dict(status))
    monkeypatch.setattr(
        free_stockdb_runtime,
        "cached_vendor_notice",
        lambda: {"status": "ok", "data_date": "2026-08-06", "version": "3.0.0"},
    )
    accepted = iter((True, False))
    monkeypatch.setattr(
        free_stockdb_runtime, "request_update", lambda _trigger: next(accepted),
    )

    assert local.get("/api/v1/settings/free-stockdb").status_code == 200
    vendor_notice = local.get("/api/v1/settings/free-stockdb/vendor-notice")
    assert vendor_notice.status_code == 200
    assert vendor_notice.json()["data_date"] == "2026-08-06"
    assert local.post("/api/v1/settings/free-stockdb/update").status_code == 403
    settings = local.get("/api/v1/settings").json()
    headers = {"X-CSRF-Token": settings["csrf_token"]}
    queued = local.post("/api/v1/settings/free-stockdb/update", headers=headers)
    duplicate = local.post("/api/v1/settings/free-stockdb/update", headers=headers)

    assert queued.status_code == 202
    assert queued.json()["accepted"] is True
    assert duplicate.status_code == 200
    assert duplicate.json()["accepted"] is False
    remote = TestClient(app, client=("203.0.113.8", 50000))
    assert remote.get("/api/v1/settings/free-stockdb").status_code == 403
    assert remote.get("/api/v1/settings/free-stockdb/vendor-notice").status_code == 403


def test_settings_web_search_probe_route_enqueues_isolated_task(
    monkeypatch, tmp_path,
):
    from quantmaster.server import management

    captured = []
    manager = ConfigManager(tmp_path / "config.yaml", tmp_path / "backups", FakeCredentials())

    class FakeSettingsJobs:
        def submit_diagnostic(self, kind, document, *, api_key):
            captured.append((kind, document.llm.model, bool(api_key)))
            return ({
                "id": "job-settings-check", "type": "settings.diagnostic",
                "status": "queued", "progress": 0, "phase": "等待执行",
                "detail": "", "links": {},
            }, True)

        @staticmethod
        def public(job):
            return {
                "domain": "settings", **job,
                "links": {
                    "self": "/api/v1/jobs/job-settings-check",
                    "events": "/api/v1/jobs/job-settings-check/events",
                    "cancel": "/api/v1/jobs/job-settings-check/cancel",
                    "retry": "/api/v1/jobs/job-settings-check/retry",
                },
            }

    monkeypatch.setattr(
        "quantmaster.server.settings_jobs.get_settings_jobs", lambda: FakeSettingsJobs(),
    )
    monkeypatch.setattr(management, "settings_manager", manager)
    client = TestClient(app)
    settings = client.get("/api/v1/settings").json()
    payload = {"settings": {
        key: settings[key]
        for key in ("config_version", "llm", "data", "trade", "news", "server", "automation", "lab")
    }}

    assert client.post("/api/v1/settings/check/llm-web-search", json=payload).status_code == 403
    response = client.post(
        "/api/v1/settings/check/llm-web-search",
        json=payload,
        headers={"X-CSRF-Token": settings["csrf_token"]},
    )

    assert response.status_code == 202
    task = response.json()
    assert task["domain"] == "settings"
    assert task["type"] == "settings.diagnostic"
    assert task["status"] == "queued"
    assert task["links"]["cancel"].endswith("/cancel")
    assert captured == [("llm-web-search", settings["llm"]["model"], False)]
    # The request never probes a provider or persists a result synchronously.
    assert not manager.check_state_path.exists()
    set_config(None)


def test_data_source_settings_check_allows_explicit_provider_probe(monkeypatch, tmp_path):
    from quantmaster.data.resilience import remote_io_allowed
    from quantmaster.server import management

    manager = ConfigManager(tmp_path / "config.yaml", tmp_path / "backups", FakeCredentials())

    def checked_probe(_timeout, _data):
        assert remote_io_allowed() is True
        return {
            "status": "success", "message": "provider probe allowed",
            "latency_ms": 1, "checked_at": "2026-08-12T00:00:00+00:00", "details": {},
        }

    monkeypatch.setattr("quantmaster.server.settings_checks.check_data_sources", checked_probe)
    monkeypatch.setattr(management, "settings_manager", manager)
    client = TestClient(app)
    settings = client.get("/api/v1/settings").json()
    payload = {"settings": {
        key: settings[key]
        for key in ("config_version", "llm", "data", "trade", "news", "server", "automation", "lab")
    }}

    response = client.post(
        "/api/v1/settings/check/data-sources", json=payload,
        headers={"X-CSRF-Token": settings["csrf_token"]},
    )

    assert response.status_code == 200
    assert response.json()["message"] == "provider probe allowed"


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

        def list(self, limit):
            return [self.latest()]

        def get(self, job_id):
            return {"id": job_id, "status": "interrupted"}

        def cancel(self, job_id):
            return {"id": job_id, "status": "cancelling"}

        def resume(self, job_id):
            return {"id": job_id, "status": "running"}

    manager = FakeRefreshManager()
    monkeypatch.setattr(maintenance, "data_refresh_manager", manager)
    monkeypatch.setattr("quantmaster.server.jobs.data_refresh_manager", manager)

    def worker_command(operation, payload, **_kwargs):
        if operation == "data.refresh.preview":
            return manager.preview(payload["scope"], payload["universe"], payload["start"])
        if operation == "data.refresh.create":
            return manager.create(payload["scope"], payload["universe"], payload["start"])
        if operation == "data.refresh.cancel":
            return manager.cancel(payload["job_id"])
        if operation == "data.refresh.retry":
            return manager.resume(payload["job_id"])
        raise AssertionError(f"unexpected worker command: {operation}")

    monkeypatch.setattr("quantmaster.runtime.worker_ipc.call_worker_command", worker_command)
    monkeypatch.setattr(
        "quantmaster.runtime.worker.runtime_worker_status",
        lambda: {"available": True, "status": "running", "age_seconds": 0.0},
    )
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
    created = client.post(
        "/api/v1/data/refresh", json={"scope": "market"}, headers=headers,
    )
    assert created.status_code == 202
    assert created.json()["status"] == "running"
    assert created.json()["links"]["self"] == "/api/v1/jobs/job-1"
    jobs = client.get("/api/v1/jobs", params={"domain": "data", "limit": 1}).json()
    assert jobs["items"][0]["status"] == "interrupted"
    assert client.post("/api/v1/jobs/job-1/retry", headers=headers).json()["status"] == "running"


def test_data_refresh_fails_fast_when_runtime_worker_is_unavailable(monkeypatch):
    monkeypatch.setattr(
        "quantmaster.runtime.worker.runtime_worker_status",
        lambda: {
            "available": False,
            "status": "unavailable",
            "reason": "runtime-worker 心跳已过期",
        },
    )
    client = TestClient(app)
    headers = {"X-CSRF-Token": client.get("/api/v1/session").json()["csrf_token"]}

    response = client.post(
        "/api/v1/data/refresh", json={"scope": "market"}, headers=headers,
    )

    assert response.status_code == 503
    assert response.json()["problem"]["code"] == "worker_unavailable"


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
    assert created.status_code == 422
    assert "多个市场" in created.text
    created = client.post(
        "/api/v1/settings/universes",
        json={"name": "core", "symbols": ["600519.SH", "000001.SZ"]}, headers=headers,
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
