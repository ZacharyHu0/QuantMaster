from __future__ import annotations

import json
import os
from types import SimpleNamespace

from quantmaster.cli import main
from quantmaster.doctor import _api_issues, _route_paths, run_doctor
from quantmaster.runtime.identity import get_application_identity


def test_deep_doctor_checks_runtime_storage_architecture_and_api(isolated_config):
    report = run_doctor(deep=True)

    assert report["deep"]
    assert report["status"] == "ok"
    assert report["counts"]["high"] == 0
    assert report["metrics"]["sqlite_checked"] >= 2
    operations = report["metrics"]["operations"]
    assert operations["news_analysis"]["claims"]["active"] == 0
    assert operations["llm"]["waiting"] == 0
    assert operations["data_providers"]["timeout_seconds"] == 45
    capabilities = operations["data_source_capabilities"]
    assert capabilities["selected"] == "free-stockdb"
    assert capabilities["priority"]["cn"][0] == "free-stockdb"
    free_stockdb = next(
        item for item in capabilities["providers"] if item["name"] == "free-stockdb"
    )
    assert {"daily", "intraday", "eod_snapshot", "industry", "themes"} <= set(
        free_stockdb["capabilities"]
    )
    assert operations["free_stockdb_runtime"]["state"] in {
        "stopped", "missing", "running", "disabled", "degraded", "error", "updating",
    }
    assert operations["trading_calendar"]["ready"] is False
    assert "rotation_snapshots" in operations
    assert operations["database_schemas"]["news"] == {
        "status": "ok", "current": 8, "expected": 8,
    }
    assert operations["database_schemas"]["paper"] == {
        "status": "ok", "current": 5, "expected": 5,
    }


def test_deep_doctor_probes_spawned_compute_identity(isolated_config):
    expected = get_application_identity()

    report = run_doctor(deep=True)

    probe = report["metrics"]["application_identity_probe"]
    assert probe == {
        "build_sha": expected.build_sha,
        "slot_id": expected.slot_id,
        "runtime_generation": expected.runtime_generation,
        "pid": probe["pid"],
    }
    assert probe["pid"] != os.getpid()


def test_deep_doctor_reports_corrupt_sqlite_as_high_risk(isolated_config):
    (isolated_config.data_root / "authority.sqlite").write_bytes(b"not a sqlite database")

    report = run_doctor(deep=True)

    assert report["status"] == "high_risk"
    assert any(item["code"] == "sqlite_unreadable" for item in report["issues"])


def test_doctor_cli_returns_nonzero_for_non_loopback_configuration(
    isolated_config, capsys,
):
    isolated_config.server.host = "0.0.0.0"

    assert main(["doctor"]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "high_risk"
    assert payload["issues"][0]["code"] == "non_loopback_host"


def test_api_doctor_ignores_framework_router_sentinels(monkeypatch):
    from quantmaster.server.app import app

    monkeypatch.setattr(app.router, "routes", [*app.router.routes, object()])

    assert not any(item["code"] == "api_contract_missing" for item in _api_issues())


def test_api_doctor_flags_obsolete_health_route(monkeypatch):
    from quantmaster.server.app import app

    obsolete = SimpleNamespace(path="/api/v1/health/live")
    monkeypatch.setattr(app.router, "routes", [*app.router.routes, obsolete])

    issues = _api_issues()

    assert any(item["code"] == "obsolete_health_route_present" for item in issues)


def test_api_doctor_flattens_lazy_included_routers():
    nested = SimpleNamespace(routes=[SimpleNamespace(path="/jobs")])
    context = SimpleNamespace(prefix="/api/v1")
    included = SimpleNamespace(original_router=nested, include_context=context)

    assert _route_paths([included, object()]) == {"/api/v1/jobs"}


def test_operational_metrics_degrade_without_hiding_failure(monkeypatch):
    from quantmaster import operational_diagnostics

    monkeypatch.setattr(
        operational_diagnostics,
        "collect_operational_metrics",
        lambda: (_ for _ in ()).throw(RuntimeError("metrics offline")),
    )
    assert operational_diagnostics.safe_operational_metrics() == {
        "status": "degraded", "error": "运行指标收集失败，请查看本机日志",
    }


def test_component_probe_failure_does_not_expose_exception_text():
    from quantmaster.server.problems import _component_failure

    try:
        raise RuntimeError("internal path and credential detail")
    except RuntimeError:
        problem = _component_failure("测试组件")
    assert problem["message"] == "状态读取失败，请查看本机日志"
    assert "internal" not in str(problem)
