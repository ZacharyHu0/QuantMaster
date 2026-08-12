"""Durable runtime-apply and LLM diagnostic jobs for the settings API.

The Web process saves configuration and queues one of these jobs.  It never
waits for a service restart, provider probe, or model request.
"""

from __future__ import annotations

import json
import threading
import uuid
from typing import Any

from quantmaster.config import get_config
from quantmaster.credentials import CredentialError, CredentialStore
from quantmaster.runtime.jobs import JobContext, JobOutcome, UnifiedJobRuntime, UnifiedJobStore
from quantmaster.settings import SettingsDocument

APPLY_TASK_TYPE = "settings.apply"
DIAGNOSTIC_TASK_TYPE = "settings.diagnostic"
_TASK_TYPES = frozenset({APPLY_TASK_TYPE, DIAGNOSTIC_TASK_TYPE})


def _temporary_credential_target(reference: str) -> str:
    return f"settings:diagnostic:{reference}"


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
        from quantmaster.server.management import _apply_runtime

        applied = _apply_runtime(dict(spec.get("saved") or {}))
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
        target = str(spec.get("credential_target") or "")
        credentials = CredentialStore()
        try:
            if not target:
                raise ValueError("设置检测缺少临时凭据引用")
            try:
                secret_payload = json.loads(credentials.get(target) or "{}")
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ValueError("设置检测临时凭据不可读取") from exc
            document = SettingsDocument.model_validate(secret_payload.get("document") or {})
            api_key = str(secret_payload.get("api_key") or "")
            kind = str(spec.get("kind") or "")
            from quantmaster.server.management import settings_manager
            from quantmaster.settings_checks import check_llm_web_search, list_llm_models

            if kind == "llm-models":
                result = list_llm_models(document.llm, api_key)
            elif kind == "llm-web-search":
                result = check_llm_web_search(document.llm, api_key)
            else:
                raise ValueError("未知 LLM 设置检测类型")
            # This fences a successful in-flight HTTP response before the
            # result can enter the persistent settings check projection.
            context.ensure_active()
            public = settings_manager.record_check_result(
                kind, document, {"llm": api_key, "tushare": ""}, result,
            )
            context.ensure_active()
            artifact = context.write_artifact(
                "settings.diagnostic.result",
                public,
                {"schema_version": "1.0", "lineage": {"kind": kind}},
            )
            return JobOutcome("completed", str(public.get("message") or "设置检测完成"), artifact["id"])
        finally:
            if target:
                try:
                    credentials.delete(target)
                except CredentialError:
                    # A failed deletion never grants the task a second chance
                    # to access a provider; its opaque target is unusable by
                    # normal configuration loading and is cleaned on retry.
                    pass

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
        reference = uuid.uuid4().hex
        target = _temporary_credential_target(reference)
        # The durable spec contains only an opaque keyring reference.  Both
        # provider configuration and a draft credential are short-lived
        # secret-store data, never job/event/log JSON.
        CredentialStore().set(target, json.dumps({
            "document": document.model_dump(), "api_key": str(api_key or ""),
        }, ensure_ascii=False, separators=(",", ":")))
        spec = {
            "kind": str(kind),
            "credential_target": target,
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
            try:
                CredentialStore().delete(target)
            except CredentialError:
                pass
            raise

    def cleanup_cancelled_credentials(self) -> int:
        """Clear draft references left behind when a queued task is cancelled."""
        removed = 0
        for job in self.apply_runtime.store.list(1000, job_type=DIAGNOSTIC_TASK_TYPE):
            if str(job.get("status") or "") not in {"cancelled", "interrupted"}:
                continue
            target = str((job.get("spec") or {}).get("credential_target") or "")
            if not target:
                continue
            try:
                CredentialStore().delete(target)
            except CredentialError:
                continue
            removed += 1
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
