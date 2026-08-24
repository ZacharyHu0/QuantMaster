from __future__ import annotations


def test_non_windows_app_process_initialization_is_a_noop(monkeypatch):
    from quantmaster.runtime import windows_app

    monkeypatch.setattr(windows_app.os, "name", "posix")
    assert windows_app.initialize_windows_app_process(root=True) is False


def test_windows_child_gets_identity_without_creating_another_job(monkeypatch):
    from quantmaster.runtime import windows_app

    calls: list[str] = []
    monkeypatch.setattr(windows_app.os, "name", "nt")
    monkeypatch.setattr(windows_app, "_ROOT_JOB", None)
    monkeypatch.setenv(windows_app.APP_JOB_ENV, "123")
    monkeypatch.setattr(windows_app, "_set_app_user_model_id", lambda: calls.append("identity"))
    monkeypatch.setattr(windows_app, "_create_root_job", lambda: calls.append("job"))

    assert windows_app.initialize_windows_app_process(root=True) is True
    assert calls == ["identity"]


def test_windows_process_consumes_onefile_reset_instruction(monkeypatch):
    from quantmaster.runtime import windows_app

    monkeypatch.setattr(windows_app.os, "name", "nt")
    monkeypatch.setattr(windows_app, "_ROOT_JOB", None)
    monkeypatch.setenv(windows_app.APP_JOB_ENV, "123")
    monkeypatch.setenv("PYINSTALLER_RESET_ENVIRONMENT", "1")
    monkeypatch.setattr(windows_app, "_set_app_user_model_id", lambda: None)

    assert windows_app.initialize_windows_app_process(root=True) is True
    assert "PYINSTALLER_RESET_ENVIRONMENT" not in windows_app.os.environ


def test_windows_root_job_is_created_once_and_published_to_children(monkeypatch):
    from quantmaster.runtime import windows_app

    calls: list[str] = []
    handle = object()
    monkeypatch.setattr(windows_app.os, "name", "nt")
    monkeypatch.setattr(windows_app, "_ROOT_JOB", None)
    monkeypatch.delenv(windows_app.APP_JOB_ENV, raising=False)
    monkeypatch.setattr(windows_app, "_set_app_user_model_id", lambda: calls.append("identity"))
    monkeypatch.setattr(windows_app, "_create_root_job", lambda: handle)

    assert windows_app.initialize_windows_app_process(root=True) is True
    assert windows_app.initialize_windows_app_process(root=True) is True
    assert windows_app._ROOT_JOB is handle
    assert windows_app.APP_USER_MODEL_ID == "QuantMaster.Personal"
    assert windows_app.os.environ[windows_app.APP_JOB_ENV]
    assert calls == ["identity", "identity"]
