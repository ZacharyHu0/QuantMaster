from __future__ import annotations

import threading

import pytest

from quantmaster.server.stream_runtime import StreamGenerationClosed, WebStreamRuntime


def test_stream_generation_fences_submit_and_signals_owned_producer():
    runtime = WebStreamRuntime("17", max_workers=1)
    cancel = threading.Event()
    stopped = threading.Event()

    def producer() -> None:
        cancel.wait(2)
        stopped.set()

    runtime.submit(producer, request_id="request-1", cancel=cancel)
    runtime.shutdown(timeout=1)

    assert cancel.is_set()
    assert stopped.is_set()
    assert runtime.status()["state"] == "stopped"
    with pytest.raises(StreamGenerationClosed, match="generation 17"):
        runtime.submit(lambda: None, request_id="late", cancel=threading.Event())


def test_stream_shutdown_deadline_reports_diagnostic_without_global_cancellation():
    runtime = WebStreamRuntime("18", max_workers=1)
    cancel = threading.Event()
    release = threading.Event()

    runtime.submit(
        lambda: release.wait(2), request_id="request-timeout", cancel=cancel,
    )
    runtime.shutdown(timeout=0.01)

    status = runtime.status()
    assert cancel.is_set()
    assert status["timeout_issues"][0]["diagnostic_id"].startswith("QM-STREAM-")
    assert status["timeout_issues"][0]["phase"] == "draining"
    release.set()


@pytest.mark.anyio
async def test_client_disconnect_stops_progress_producer_at_emit_boundary(monkeypatch):
    from quantmaster.server import app as server_app

    started = threading.Event()
    stopped = threading.Event()
    proceed = threading.Event()

    def task(emit):
        started.set()
        emit(1, "atomic chunk complete")
        proceed.wait(1)
        try:
            emit(2, "next chunk")
        except StreamGenerationClosed:
            stopped.set()
            raise

    response = server_app._progress_stream(task, "disconnect-test")
    iterator = response.body_iterator
    assert started.wait(1)
    await iterator.__anext__()
    await iterator.aclose()
    proceed.set()
    assert stopped.wait(1)
    server_app._shutdown_web_stream_executor(1)
    monkeypatch.setattr(server_app, "_web_stream_runtime", None)
