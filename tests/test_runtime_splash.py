from __future__ import annotations

from types import SimpleNamespace

from quantmaster.runtime import splash


def test_packaged_splash_updates_real_stage_and_closes(monkeypatch):
    calls = []
    backend = SimpleNamespace(
        is_alive=lambda: True,
        update_text=lambda value: calls.append(("update", value)),
        close=lambda: calls.append(("close", None)),
    )
    monkeypatch.setattr(splash.sys, "frozen", True, raising=False)
    monkeypatch.setenv("_PYI_SPLASH_IPC", "1234")
    monkeypatch.setitem(splash.sys.modules, "pyi_splash", backend)

    splash.update_splash("正在启动 Web 服务")
    assert splash.splash_active() is True
    splash.close_splash()

    assert calls == [("update", "正在启动 Web 服务"), ("close", None)]


def test_source_and_suppressed_splash_are_noops(monkeypatch):
    backend = SimpleNamespace(
        is_alive=lambda: (_ for _ in ()).throw(AssertionError("must not load splash")),
    )
    monkeypatch.delattr(splash.sys, "frozen", raising=False)
    monkeypatch.setitem(splash.sys.modules, "pyi_splash", backend)

    splash.update_splash("source")
    assert splash.splash_active() is False
    splash.close_splash()

    monkeypatch.setattr(splash.sys, "frozen", True, raising=False)
    monkeypatch.setenv("_PYI_SPLASH_IPC", "0")
    backend.is_alive = lambda: False
    splash.update_splash("suppressed")
    assert splash.splash_active() is False
    splash.close_splash()
