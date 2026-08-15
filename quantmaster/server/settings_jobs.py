"""Durable runtime-apply and LLM diagnostic jobs for the settings API.

The Web process saves configuration and queues one of these jobs.  It never
waits for a service restart, provider probe, or model request.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

from quantmaster.config import get_config
from quantmaster.runtime.jobs import JobContext, JobOutcome, UnifiedJobRuntime, UnifiedJobStore
from quantmaster.server.settings_control import apply_runtime, settings_manager
from quantmaster.settings import SettingsDocument

APPLY_TASK_TYPE = "settings.apply"
DIAGNOSTIC_TASK_TYPE = "settings.diagnostic"
_TASK_TYPES = frozenset({APPLY_TASK_TYPE, DIAGNOSTIC_TASK_TYPE})


class _DiagnosticCredentialVault:
    """Keep one-shot diagnostic inputs inside the Web process.

    Windows services and reload workers may not have a logon session capable of
    writing Credential Manager entries (WinError 1312).  A diagnostic is not a
    durable secret-bearing operation: the durable job keeps only this opaque
    reference, and a process restart deliberately makes the job fail instead
    of persisting or reinterpreting the draft credential.
    """

    def __init__(
        self,
        *,
        ttl_seconds: float = 600.0,
        max_entries: int = 32,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._lock = threading.Lock()
        self._ttl_seconds = max(1.0, float(ttl_seconds))
        self._max_entries = max(1, int(max_entries))
        self._clock = clock
        self._values: dict[str, tuple[float, SettingsDocument, str]] = {}

    def _purge_expired(self, now: float) -> None:
        for reference, (expires_at, _document, _api_key) in list(self._values.items()):
            if expires_at <= now:
                self._values.pop(reference, None)

    def put(self, document: SettingsDocument, api_key: str) -> str:
        reference = uuid.uuid4().hex
        with self._lock:
            now = self._clock()
            self._purge_expired(now)
            if len(self._values) >= self._max_entries:
                raise RuntimeError("设置检测临时凭据队列已满，请稍后重试")
            self._values[reference] = (
                now + self._ttl_seconds,
                document.model_copy(deep=True),
                str(api_key or ""),
            )
        return reference

    def pop(self, reference: str) -> tuple[SettingsDocument, str] | None:
        with self._lock:
            now = self._clock()
            self._purge_expired(now)
            value = self._values.pop(reference, None)
            return (value[1], value[2]) if value is not None else None

    def discard(self, reference: str) -> bool:
        return self.pop(reference) is not None


_DIAGNOSTIC_CREDENTIALS = _DiagnosticCredentialVault()


class SettingsJobs:
    """Two serial lanes so a slow provider check cannot delay runtime apply."""

    def __init__(self) -> None:
        store = UnifiedJobStore(get_config().data_root / "jobs.sqlite")
        self.apply_runtime = UnifiedJobRuntime(store, max_workers=1)
        self.diagnostic_runtime = UnifiedJobRuntime(store, max_workers=1)
        self.apply_runtime.register(APPLY_TASK_TYPE, self._apply)
        self.diagnostic_runtime.register(DIAGNOSTIC_TASK_TYPE, self._diagnostic)

    @staticmethod
    def _apply(context: JobContext, spec: dict[str, Any]) -> JobOutcome:
        context.progress(5, "准备运行时应用", "配置已落盘，正在交由后台服务应用")
        context.ensure_active()
        # Import lazily to avoid server module construction in Web request
        # paths.  This code runs in runtime-worker only.
        from quantmaster.settings_runtime import persisted_revision

        saved = dict(spec.get("saved") or {})
        revision = int(saved.get("config_revision") or 0)
        manager = settings_manager()
        latest = persisted_revision(manager.path)
        if revision < latest:
            applied = {
                **saved,
                "apply_status": {"config": {"status": "superseded"}},
                "superseded_by": latest,
            }
        else:
            applied = apply_runtime(saved)
        context.ensure_active()
        context.progress(96, "记录运行时应用", "保存后台应用结果")
        artifact = context.write_artifact(
            "settings.apply.result",
            applied,
            {"schema_version": "1.0", "lineage": {"config_revision": spec.get("config_revision", "")}},
        )
        return JobOutcome("completed", "设置运行时应用已完成", artifact["id"])

    @staticmethod
    def _diagnostic(context: JobContext, spec: dict[str, Any]) -> JobOutcome:
        context.progress(5, "准备模型检测", "读取临时凭据引用")
        context.ensure_active()
        reference = str(spec.get("credential_reference") or "")
        try:
            secret_payload = _DIAGNOSTIC_CREDENTIALS.pop(reference) if reference else None
            if secret_payload is None:
                raise ValueError("设置检测临时凭据已失效，请重新检测")
            document, api_key = secret_payload
            kind = str(spec.get("kind") or "")
            from quantmaster.settings_checks import check_llm_web_search, list_llm_models

            if kind == "llm-models":
                result = list_llm_models(document.llm, api_key, isolated=True)
            elif kind == "llm-web-search":
                result = check_llm_web_search(document.llm, api_key, isolated=True)
            else:
                raise ValueError("未知 LLM 设置检测类型")
            # This fences a successful in-flight HTTP response before the
            # result can enter the persistent settings check projection.
            context.ensure_active()
            public = settings_manager().record_check_result(
                kind, document, {"llm": api_key, "tushare": ""}, result,
            )
            context.ensure_active()
            return JobOutcome("completed", str(public.get("message") or "设置检测完成"))
        finally:
            if reference:
                _DIAGNOSTIC_CREDENTIALS.discard(reference)

    def submit_apply(self, saved: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        spec = {
            "saved": dict(saved),
            "config_revision": str(saved.get("config_revision") or ""),
        }
        return self.apply_runtime.submit(
            APPLY_TASK_TYPE,
            spec,
            input_fingerprint=str(saved.get("config_revision") or ""),
            algorithm_version="QM_SETTINGS_APPLY_V1",
            deadline_seconds=300,
            max_attempts=2,
        )

    def submit_diagnostic(
        self,
        kind: str,
        document: SettingsDocument,
        *,
        api_key: str,
    ) -> tuple[dict[str, Any], bool]:
        if not self.diagnostic_runtime.dispatch_enabled:
            # Web generations cannot hand process-local memory to the
            # supervisor.  Transfer the draft once over the authenticated
            # local command channel; the worker puts it in its own one-shot
            # vault before the durable job is created.  No secret enters the
            # job ledger, event stream, artifact, or public response.
            from quantmaster.runtime.worker_ipc import call_worker_command

            response = call_worker_command(
                "settings.diagnostic.create",
                {
                    "kind": str(kind),
                    "document": document.model_dump(mode="json"),
                    "api_key": str(api_key or ""),
                },
                timeout=2.0,
            )
            task = response.get("task")
            if not isinstance(task, dict):
                raise RuntimeError("后台执行器返回无效的设置检测任务")
            return task, bool(response.get("created", True))

        return self._submit_diagnostic_local(kind, document, api_key=api_key)

    def _submit_diagnostic_local(
        self,
        kind: str,
        document: SettingsDocument,
        *,
        api_key: str,
    ) -> tuple[dict[str, Any], bool]:
        """Create a diagnostic after its secret has reached the worker."""

        reference = _DIAGNOSTIC_CREDENTIALS.put(document, api_key)
        # The durable spec contains only an opaque process-local reference.
        # Provider configuration and draft credentials never enter job/event/log JSON.
        spec = {
            "kind": str(kind),
            "credential_reference": reference,
        }
        try:
            return self.diagnostic_runtime.submit(
                DIAGNOSTIC_TASK_TYPE,
                spec,
                algorithm_version="QM_SETTINGS_DIAGNOSTIC_V1",
                deadline_seconds=max(30.0, min(600.0, float(document.llm.timeout) * 4)),
                max_attempts=1,
                llm_scope="global",
            )
        except Exception:
            _DIAGNOSTIC_CREDENTIALS.discard(reference)
            raise

    def cleanup_cancelled_credentials(self) -> int:
        """Clear draft references left behind when a queued task is cancelled."""
        removed = 0
        for job in self.apply_runtime.store.list(1000, job_type=DIAGNOSTIC_TASK_TYPE):
            if str(job.get("status") or "") not in {"cancelled", "interrupted"}:
                continue
            reference = str((job.get("spec") or {}).get("credential_reference") or "")
            if not reference:
                continue
            removed += int(_DIAGNOSTIC_CREDENTIALS.discard(reference))
        return removed

    def runtime_for(self, job_id: str) -> UnifiedJobRuntime:
        value = self.get(job_id)
        return self.apply_runtime if value["type"] == APPLY_TASK_TYPE else self.diagnostic_runtime

    def get(self, job_id: str) -> dict[str, Any]:
        value = self.apply_runtime.store.get(job_id)
        if str(value.get("type") or "") not in _TASK_TYPES:
            raise KeyError(job_id)
        return value

    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        values = self.apply_runtime.store.list(max(1, min(1000, int(limit) * 3)))
        return [value for value in values if str(value.get("type") or "") in _TASK_TYPES][:limit]

    def public(self, value: dict[str, Any]) -> dict[str, Any]:
        return self.apply_runtime.public(value)

    def start(self) -> None:
        self.cleanup_cancelled_credentials()
        self.apply_runtime.start()
        self.diagnostic_runtime.start()

    def pause(self) -> None:
        self.apply_runtime.pause()
        self.diagnostic_runtime.pause()

    def resume(self) -> None:
        self.apply_runtime.resume()
        self.diagnostic_runtime.resume()

    def stop(self) -> None:
        self.apply_runtime.stop()
        self.diagnostic_runtime.stop()

    @property
    def idle(self) -> bool:
        return self.apply_runtime.idle and self.diagnostic_runtime.idle


_lock = threading.Lock()
_instance: SettingsJobs | None = None


def get_settings_jobs() -> SettingsJobs:
    global _instance
    with _lock:
        if _instance is None:
            _instance = SettingsJobs()
        return _instance


def shutdown_settings_jobs() -> None:
    global _instance
    with _lock:
        value, _instance = _instance, None
    if value is not None:
        value.stop()
