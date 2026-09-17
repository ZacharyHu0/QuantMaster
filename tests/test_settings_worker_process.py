"""Settings bootstrap must work without Web imports in a fresh interpreter."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml


def _environment(tmp_path, *, enabled=True):
    env = {key: value for key, value in os.environ.items() if not key.startswith("QM_")}
    env.update(
        QM_CONFIG_PATH=str(tmp_path / "config.yaml"),
        QM_DATA_ROOT=str(tmp_path / "data"),
        QM_FREE_STOCKDB_ROOT=str(tmp_path / "stockdb"),
        QM_WORKER_SUPERVISOR="1",
    )
    (tmp_path / "config.yaml").write_text(yaml.safe_dump({
        "_revision": 1,
        "data": {"root": str(tmp_path / "data"), "free_stockdb_root": str(tmp_path / "stockdb"),
                 "free_stockdb_managed": False, "repair_enabled": enabled,
                 "free_stockdb_auto_update": enabled},
    }), encoding="utf-8")
    return env


def test_fresh_worker_bootstrap_registers_settings(tmp_path):
    env = _environment(tmp_path)
    probe = subprocess.run(
        [sys.executable, "-c", """
import sys
from quantmaster.server import bootstrap, settings_control
assert 'quantmaster.server.management' not in sys.modules
plan = bootstrap.build_worker_plan()
assert settings_control._apply_runtime is not None, 'worker settings callback missing'
assert 'quantmaster.server.management' not in sys.modules
"""],
        cwd=Path(__file__).resolve().parents[1], env=env,
        capture_output=True, text=True, timeout=60,
    )
    assert probe.returncode == 0, probe.stdout + probe.stderr


_WORKER = """
import sys
from quantmaster.config import get_config
from quantmaster.server.bootstrap import build_worker_plan
from quantmaster.server.settings_runtime import public_state
from quantmaster.data.repair import enqueue_repair
plan = build_worker_plan()
assert 'quantmaster.server.management' not in sys.modules
# Simulate the snapshot held by an already running worker before the PUT.
get_config().data.repair_enabled = not EXPECTED
get_config().data.free_stockdb_auto_update = not EXPECTED
jobs = plan.settings_worker
pending = jobs.list()[0]
jobs.start()
try:
    result = jobs.apply_runtime.wait(pending['id'], timeout=20)
    assert result['status'] == 'completed', result
    assert get_config().data.repair_enabled is EXPECTED
    if not EXPECTED:
        assert enqueue_repair('history', 'test', reason='test', spec={}) is None
    assert get_config().data.free_stockdb_auto_update is EXPECTED
    state = public_state(plan.settings_manager.path)
    assert not state['drift'], state
    assert plan.settings_projection() == (state['persisted_revision'], state['latest_generation'])
    assert not plan.free_stockdb_runtime._owner
finally:
    jobs.stop()
"""


@pytest.mark.parametrize("enabled", [False, True])
def test_normal_put_is_consumed_by_fresh_worker_and_stockdb_owner(tmp_path, enabled):
    env = _environment(tmp_path, enabled=not enabled)
    env.pop("QM_WORKER_SUPERVISOR")
    env["QM_WEB_PROCESS"] = "1"
    web = """
import os, subprocess, sys, threading, time
from fastapi.testclient import TestClient
from quantmaster.server.app import app
from quantmaster.config import get_config
from quantmaster.server.settings_jobs import get_settings_jobs
from quantmaster.settings import document_from_config
from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime
client = TestClient(app)
csrf = client.get('/api/v1/session').json()['csrf_token']
document = document_from_config(get_config()).model_dump(mode='json')
document['data'].update(repair_enabled=ENABLED, free_stockdb_auto_update=ENABLED)
started = time.monotonic()
response = client.put('/api/v1/settings', json=document, headers={'X-CSRF-Token': csrf})
assert response.status_code == 200, response.text
assert time.monotonic() - started < 3
assert get_config().data.free_stockdb_auto_update is ENABLED
assert not get_settings_jobs().apply_runtime.dispatch_enabled
stop = threading.Event()
observed = []
def owner():
    # Exceed the previous five-second acknowledgement window.
    time.sleep(10)
    while not stop.wait(.01):
        if free_stockdb_runtime._process_command():
            observed.append(get_config().data.free_stockdb_auto_update)
thread = threading.Thread(target=owner)
thread.start()
worker_env = dict(os.environ, QM_WORKER_SUPERVISOR='1')
worker_env.pop('QM_WEB_PROCESS')
try:
    command = [sys.executable, '-c', 'EXPECTED = ' + repr(ENABLED) + '\\n' + WORKER]
    result = subprocess.run(command, env=worker_env,
                            capture_output=True, text=True, timeout=40)
    assert result.returncode == 0, result.stdout + result.stderr
    assert observed == [ENABLED], observed
finally:
    stop.set()
    thread.join(2)
"""
    result = subprocess.run(
        [sys.executable, "-c", "WORKER = " + repr(_WORKER) + "\nENABLED = " + repr(enabled) + "\n" + web],
        cwd=Path(__file__).resolve().parents[1], env=env,
        capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
