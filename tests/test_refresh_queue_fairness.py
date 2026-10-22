"""A busy refresh pool admits current work between durable history batches."""
from __future__ import annotations

import threading
import time
from collections import Counter

from quantmaster.data.maintenance import DataRefreshManager


def test_market_overtakes_bulk_backlog_and_history_resumes_once(isolated_config, monkeypatch):
    manager = DataRefreshManager()
    monkeypatch.setattr(manager, "MAX_PARALLEL_SYMBOLS", 2)
    monkeypatch.setattr(manager, "_stockdb_wait_reason", lambda: "")
    monkeypatch.setattr(manager, "_uses_stockdb", lambda symbol: False)
    monkeypatch.setattr(manager, "_fingerprint", lambda symbols: "local")
    monkeypatch.setattr(manager, "_publish_market_snapshot", lambda: None)
    entered, release = threading.Event(), threading.Event()
    calls = []

    def refresh(store, symbol, start, end):
        calls.append(symbol)
        if symbol.startswith("A"):
            entered.set()
            assert release.wait(10)
        return None

    monkeypatch.setattr(manager, "_refresh_guarded", refresh)
    runtime = manager._ensure_runtime()
    try:
        first = manager._submit("all_cached", "", "2026-01-01", "2026-10-21", [f"A{i}" for i in range(6)])
        assert entered.wait(5)
        second = manager._submit("all_cached", "", "2026-01-01", "2026-10-21", ["B0", "B1", "B2"])
        cancelled = manager._submit("all_cached", "", "2026-01-01", "2026-10-21", ["C0"])
        market = manager._submit("market", "", "2026-01-01", "2026-10-21", ["M0"])
        duplicate = manager._submit("market", "", "2026-01-01", "2026-10-21", ["M0"])
        assert duplicate["id"] == market["id"]
        projected = manager.get(market["id"])
        assert projected["queue"]["phase"] == "waiting_for_slot"
        assert projected["queue"]["blockers"][0]["id"] == first["id"]
        assert len(runtime._active) == 1  # No hidden FIFO of history futures.
        manager.cancel(cancelled["id"])
        started = time.monotonic()
        release.set()
        assert runtime.wait(market["id"], 8, until_terminal=True)["status"] == "completed"
        assert time.monotonic() - started < 5
        assert calls.index("M0") <= 4
        assert runtime.wait(first["id"], 12, until_terminal=True)["status"] == "completed"
        assert runtime.wait(second["id"], 12, until_terminal=True)["status"] == "completed"
        assert Counter(calls) == Counter([*[f"A{i}" for i in range(6)], "B0", "B1", "B2", "M0"])
        assert manager.get(first["id"])["attempt"] == 1
        assert manager.get(first["id"])["next_index"] == 6
        assert manager.get(cancelled["id"])["status"] == "cancelled"
    finally:
        release.set()
        runtime.stop()


def test_recovered_queue_prefers_market_over_newer_history(isolated_config, monkeypatch):
    manager = DataRefreshManager()
    monkeypatch.setattr(manager, "_owns_runtime", lambda: False)
    monkeypatch.setattr(manager, "_stockdb_wait_reason", lambda: "")
    monkeypatch.setattr(manager, "_uses_stockdb", lambda symbol: False)
    monkeypatch.setattr(manager, "_fingerprint", lambda symbols: "local")
    monkeypatch.setattr(manager, "_publish_market_snapshot", lambda: None)
    calls = []
    monkeypatch.setattr(manager, "_refresh_guarded", lambda store, symbol, *args: calls.append(symbol))
    market = manager._submit("market", "", "2026-01-01", "2026-10-21", ["M"])
    history = manager._submit("all_cached", "", "2026-01-01", "2026-10-21", ["H"])
    manager._runtime.stop()
    restored = DataRefreshManager()
    monkeypatch.setattr(restored, "_owns_runtime", lambda: True)
    monkeypatch.setattr(restored, "_stockdb_wait_reason", lambda: "")
    monkeypatch.setattr(restored, "_uses_stockdb", lambda symbol: False)
    monkeypatch.setattr(restored, "_fingerprint", lambda symbols: "local")
    monkeypatch.setattr(restored, "_publish_market_snapshot", lambda: None)
    monkeypatch.setattr(restored, "_refresh_guarded", lambda store, symbol, *args: calls.append(symbol))
    runtime = restored._ensure_runtime()
    try:
        runtime.start()
        assert runtime.wait(history["id"], 8, until_terminal=True)["status"] == "completed"
        assert runtime.wait(market["id"], 8, until_terminal=True)["status"] == "completed"
        assert calls == ["M", "H"]
    finally:
        runtime.stop()
