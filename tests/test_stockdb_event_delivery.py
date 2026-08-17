from __future__ import annotations

from types import SimpleNamespace

import pytest

from quantmaster.data.free_stockdb_runtime import FreeStockDBRuntime, StockDBUpdateEvent
from quantmaster.server.bootstrap import StockDBEventDelivery


class _Source:
    def __init__(self, event: StockDBUpdateEvent | None = None) -> None:
        self.event = event
        self.completed: list[str] = []

    def claim_update_event(self) -> StockDBUpdateEvent | None:
        return self.event

    def complete_update_event(self, event_key: str) -> None:
        self.completed.append(event_key)
        self.event = None


def _delivery(source, calls):
    service = SimpleNamespace(
        run_task=lambda *args, **kwargs: calls.append(("automation", args, kwargs)),
        process_event=lambda event: calls.append(("alert", event)),
    )
    return StockDBEventDelivery(
        source,
        after_close_jobs=SimpleNamespace(
            submit=lambda **kwargs: calls.append(("after_close", kwargs)),
        ),
        rotation_worker=SimpleNamespace(
            submit=lambda spec: calls.append(("rotation", spec)),
        ),
        automation_runtime=SimpleNamespace(service=service),
        paper_automation_worker=SimpleNamespace(
            requeue_market_data=lambda target: calls.append(("paper", target)) or 2,
        ),
        reset_after_close=lambda: calls.append("reset_after_close"),
        reset_etf_research=lambda: calls.append("reset_etf_research"),
    )


def test_complete_event_dispatches_all_real_consumers(isolated_config):
    isolated_config.data.after_close_enabled = True
    isolated_config.data.after_close_auto_run = True
    isolated_config.automation.enabled = True
    calls = []
    delivery = _delivery(_Source(), calls)

    delivery.deliver(StockDBUpdateEvent(
        "update_succeeded:2026-08-10",
        "update_succeeded",
        {"target_session": "2026-08-10"},
    ))

    assert calls[0:2] == ["reset_after_close", "reset_etf_research"]
    assert ("after_close", {"as_of": "2026-08-10", "force": False}) in calls
    rotation = next(
        item[1] for item in calls
        if isinstance(item, tuple) and item[0] == "rotation"
    )
    assert (rotation.scope, rotation.source, rotation.as_of) == (
        "all", "auto", "2026-08-10",
    )
    automation = next(
        item for item in calls
        if isinstance(item, tuple) and item[0] == "automation"
    )
    assert automation[1] == ("daily_close_pipeline",)
    assert automation[2]["business_key"] == "daily_close_pipeline:date:2026-08-10"
    assert ("paper", "2026-08-10") in calls


def test_partial_event_resets_domain_caches_and_only_dispatches_rotation(isolated_config):
    isolated_config.data.after_close_enabled = True
    isolated_config.data.after_close_auto_run = True
    isolated_config.automation.enabled = True
    calls = []
    delivery = _delivery(_Source(), calls)

    delivery.deliver(StockDBUpdateEvent(
        "market_session_partial:2026-08-10",
        "market_session_partial",
        {"target_session": "2026-08-10"},
    ))

    assert calls[0:2] == ["reset_after_close", "reset_etf_research"]
    assert [item[0] for item in calls if isinstance(item, tuple)] == ["rotation"]
    rotation = calls[-1][1]
    assert (rotation.scope, rotation.source, rotation.as_of) == (
        "all", "local", "2026-08-10",
    )


def test_failed_event_dispatches_alert_without_success_consumers(isolated_config):
    isolated_config.data.after_close_notify = True
    isolated_config.automation.enabled = True
    calls = []
    delivery = _delivery(_Source(), calls)

    delivery.deliver(StockDBUpdateEvent(
        "update_failed:2026-08-10",
        "update_failed",
        {
            "target_session": "2026-08-10",
            "validation": {"actual_session": "2026-08-09"},
            "attempt": 3,
            "message": "stale evidence",
        },
    ))

    assert len(calls) == 1
    kind, alert = calls[0]
    assert kind == "alert"
    assert alert.kind == "task_failure"
    assert alert.payload["target_session"] == "2026-08-10"
    assert "stale evidence" in alert.evidence


def test_delivery_failure_never_acknowledges_claimed_event(isolated_config, monkeypatch):
    event = StockDBUpdateEvent("event-1", "update_succeeded", {"target_session": "x"})
    source = _Source(event)
    delivery = _delivery(source, [])
    monkeypatch.setattr(delivery, "deliver", lambda _event: (_ for _ in ()).throw(
        RuntimeError("consumer failed"),
    ))

    with pytest.raises(RuntimeError, match="consumer failed"):
        delivery.poll_once()

    assert source.completed == []


def test_durable_event_key_is_idempotent_and_ack_removes_it_from_pending(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("QM_FREE_STOCKDB_CONTROL_PATH", str(tmp_path / "control.sqlite"))
    runtime = FreeStockDBRuntime()
    payload = {"target_session": "2026-08-10"}
    runtime._emit_update_event("update_succeeded", "2026-08-10", payload)
    runtime._emit_update_event("update_succeeded", "2026-08-10", payload)

    event = runtime.claim_update_event()

    assert event == StockDBUpdateEvent(
        "update_succeeded:2026-08-10", "update_succeeded", payload,
    )
    runtime.complete_update_event(event.event_key)
    assert runtime.claim_update_event() is None
