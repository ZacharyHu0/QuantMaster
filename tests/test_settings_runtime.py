from __future__ import annotations

import yaml

from quantmaster.settings_runtime import begin_apply, public_state, report_component


def _persist(path, revision: int) -> None:
    path.write_text(yaml.safe_dump({"_revision": revision}), encoding="utf-8")


def test_old_revision_and_generation_reports_cannot_overwrite_new_state(tmp_path):
    path = tmp_path / "config.yaml"
    _persist(path, 1)
    generation_one = begin_apply(path, 1)
    assert report_component(
        path, "llm", revision=1, generation=generation_one, status="effective",
    )

    _persist(path, 2)
    generation_two = begin_apply(path, 2)
    assert report_component(
        path, "llm", revision=2, generation=generation_two, status="effective",
    )
    assert not report_component(
        path, "llm", revision=1, generation=generation_one, status="failed",
        error="late old failure",
    )
    assert not report_component(
        path, "llm", revision=2, generation=generation_one, status="failed",
        error="late old generation",
    )

    state = public_state(path)
    llm = state["components"]["llm"]
    assert llm["effective_revision"] == 2
    assert llm["generation"] == generation_two
    assert llm["status"] == "effective"
    assert "late" not in llm["error"]


def test_worker_offline_marks_pending_components_unconfirmed(tmp_path):
    path = tmp_path / "config.yaml"
    _persist(path, 3)
    generation = begin_apply(path, 3)
    assert report_component(
        path, "web", revision=3, generation=generation, status="effective",
    )

    state = public_state(path, worker_available=False)

    assert state["components"]["web"]["status"] == "effective"
    assert state["components"]["server"]["status"] == "pending"
    assert state["components"]["runtime-worker"]["status"] == "unconfirmed"
    assert "runtime-worker" in state["drift"]


def test_failed_apply_keeps_new_persisted_and_old_effective_revision(tmp_path):
    path = tmp_path / "config.yaml"
    _persist(path, 4)
    generation = begin_apply(path, 4)
    assert report_component(
        path, "llm", revision=4, generation=generation, status="failed",
        effective_revision=3, error="candidate probe failed",
        diagnostic_code="llm_probe_failed", recommendation="modify or retry",
    )

    state = public_state(path)
    llm = state["components"]["llm"]
    assert state["persisted_revision"] == 4
    assert llm["target_revision"] == 4
    assert llm["effective_revision"] == 3
    assert llm["status"] == "failed"
    assert llm["diagnostic_code"] == "llm_probe_failed"


def test_restart_required_is_unconfirmed_drift(tmp_path):
    path = tmp_path / "config.yaml"
    _persist(path, 5)
    generation = begin_apply(path, 5)
    assert report_component(
        path, "server", revision=5, generation=generation,
        status="restart_required", effective_revision=4,
        recommendation="restart safely",
    )

    state = public_state(path, worker_available=False)
    server = state["components"]["server"]
    assert server["status"] == "restart_required"
    assert server["confirmed"] is False
    assert "server" in state["drift"]
