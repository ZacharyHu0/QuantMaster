from __future__ import annotations

import threading
from typing import Any

from quantmaster.config import get_config
from quantmaster.rotation.etf_models import ETF_RESEARCH_MODEL_VERSION
from quantmaster.rotation.etf_research import get_etf_research_service
from quantmaster.runtime.derived import DerivedArtifactCatalog
from quantmaster.runtime.jobs import (
    JobContext,
    JobOutcome,
    UnifiedJobRuntime,
    UnifiedJobStore,
)

TASK_TYPE = "rotation.etf.scan"


def _research_tier(value: str) -> str:
    tier = str(value or "production").strip().casefold()
    if tier not in {"production", "sandbox"}:
        raise ValueError("ETF 研究 tier 仅支持 production 或 sandbox")
    return tier


class EtfResearchJobs:
    def __init__(self, runtime: UnifiedJobRuntime | None = None):
        self.runtime = runtime or UnifiedJobRuntime(
            UnifiedJobStore(get_config().data_root / "jobs.sqlite"),
            max_workers=1,
        )
        self.runtime.register(
            TASK_TYPE,
            self._handle,
            process_entrypoint="quantmaster.rotation.etf_jobs:EtfResearchJobs._handle",
        )

    @staticmethod
    def input_fingerprint(*, as_of: str, tier: str) -> str:
        """Read only compact local generations when deciding whether to scan.

        ETF research used to submit every click into a provider-first worker.
        This key covers the immutable StockDB ingest plus local share/metadata
        generations, so an already-published local input can be returned as a
        completed ``unchanged`` task before any provider or Parquet read.
        """

        try:
            target, _source = get_etf_research_service()._research_target(as_of)
            target_date = str(target.date())
            catalog = DerivedArtifactCatalog()
            generations = [
                *catalog.source_generations("stockdb.ingest.etf"),
                *catalog.source_generations("rotation.etf_observations"),
                *catalog.source_generations("rotation.etf_metadata"),
            ]
            # Never manufacture a cache hit when a pre-migration installation
            # has not populated the shared generation catalog yet.
            if not generations:
                return ""
            return catalog.input_fingerprint(
                schema_version=3,
                algorithm_version=ETF_RESEARCH_MODEL_VERSION,
                parameters={
                    "task": TASK_TYPE,
                    "tier": tier,
                    "requested_as_of": str(as_of or ""),
                    "target_as_of": target_date,
                },
                source_generations=generations,
            )
        except (OSError, RuntimeError, TypeError, ValueError):
            # Correctness beats an optimistic hit: an unavailable catalog
            # simply causes a normal submit, never reuse of unknown evidence.
            return ""

    @staticmethod
    def _local_evidence_ready(*, as_of: str) -> bool:
        """Whether local share and metadata evidence covers the target day."""

        try:
            from quantmaster.rotation.store import RotationStore

            target, _source = get_etf_research_service()._research_target(as_of)
            target_date = str(target.date())
            store = RotationStore()
            shares = store.source_generations("rotation.etf_observations")
            metadata = store.source_generations("rotation.etf_metadata")
            return bool(
                store.etf_path.is_file()
                and metadata
                and any(
                    str(item.get("coverage_end") or "")[:10] >= target_date
                    for item in shares
                )
            )
        except (ImportError, OSError, RuntimeError, TypeError, ValueError, AttributeError):
            return False

    @staticmethod
    def _handle(context: JobContext, spec: dict[str, Any]) -> JobOutcome:
        service = get_etf_research_service()
        tier = _research_tier(str(spec.get("tier") or "production"))
        warnings: list[str] = []
        try:
            if tier == "production":
                from quantmaster.rotation.provider import RotationProvider, RotationProviderCallError
                from quantmaster.rotation.store import RotationStore

                if EtfResearchJobs._local_evidence_ready(
                    as_of=str(spec.get("as_of") or ""),
                ):
                    context.progress(2, "复用本地 ETF 研究证据", "份额和元数据 generation 已覆盖目标交易日")
                else:
                    context.progress(2, "同步 ETF 研究证据", "本地份额或元数据缺失/过期")
                    try:
                        result = RotationProvider(RotationStore()).sync_etf_observations(
                            context.progress,
                            context.cancelled,
                        )
                        warnings.extend(str(value) for value in result.get("issues") or ())
                    except InterruptedError:
                        raise
                    except (
                        RotationProviderCallError,
                        OSError,
                        RuntimeError,
                        TypeError,
                        ValueError,
                    ) as exc:  # 可选证据失败允许降级；编程错误仍应显式暴露
                        warnings.append(f"元数据或份额同步失败，使用本地缓存：{str(exc)[:180]}")
            else:
                context.progress(2, "读取 ETF sandbox 证据", "仅使用已存在的本地元数据与份额")
            snapshot = service.scan(
                as_of=str(spec.get("as_of") or ""),
                tier=tier,
                progress=context.progress,
                cancelled=context.cancelled,
                refresh_warnings=warnings,
            )
        except (InterruptedError, OSError, RuntimeError, TypeError, ValueError, AttributeError) as exc:
            if tier == "production":
                service.store.record_failure(str(exc) or exc.__class__.__name__)
            raise
        artifact = context.write_artifact(
            "rotation.etf.snapshot" if tier == "production" else "rotation.etf.preview",
            snapshot.to_dict(),
            {
                "schema_version": snapshot.schema_version,
                "lineage": {
                    "snapshot_id": snapshot.snapshot_id,
                    "ingest_id": snapshot.ingest_id,
                    "artifact_id": snapshot.artifact_id,
                    "input_hash": snapshot.input_hash,
                    "tier": tier,
                    "formal_eligible": tier == "production",
                },
            },
        )
        message = (
            "ETF 研究快照已发布"
            if tier == "production"
            else "ETF 本地降级预览已生成（不可发布）"
        )
        if warnings:
            message += f"（{len(warnings)} 项证据已降级）"
        return JobOutcome("completed", message, artifact["id"])

    def submit(
        self,
        *,
        as_of: str = "",
        tier: str = "production",
    ) -> tuple[dict[str, Any], bool]:
        selected_tier = _research_tier(tier)
        spec = {"as_of": as_of, "tier": selected_tier}
        return self.runtime.submit(
            TASK_TYPE,
            spec,
            input_fingerprint=self.input_fingerprint(as_of=as_of, tier=selected_tier),
            algorithm_version=ETF_RESEARCH_MODEL_VERSION,
            deadline_seconds=3600,
            max_attempts=2,
        )

    def get(self, job_id: str) -> dict[str, Any]:
        value = self.runtime.store.get(job_id)
        if str(value.get("type") or "") != TASK_TYPE:
            raise KeyError(job_id)
        return value

    def public(self, value: dict[str, Any]) -> dict[str, Any]:
        result = self.runtime.public(value)
        tier = str((value.get("spec") or {}).get("tier") or "production")
        result.update(
            {
                "tier": tier,
                "formal_eligible": tier == "production",
                "message": str(value.get("detail") or ""),
            }
        )
        artifact_id = str(value.get("result_artifact_id") or "")
        if artifact_id:
            try:
                artifact = self.runtime.store.artifact(artifact_id)
            except (KeyError, RuntimeError, ValueError):
                artifact = None
            if artifact is not None:
                payload = artifact.get("payload") or {}
                snapshot_id = str(payload.get("snapshot_id") or "")
                result["result"] = {
                    "snapshot_id": snapshot_id,
                    "preview_id": snapshot_id if tier == "sandbox" else "",
                    "tier": tier,
                    "formal_eligible": bool(payload.get("formal_eligible")),
                    "artifact_id": artifact_id,
                }
        return result

    def cancel(self, job_id: str) -> dict[str, Any]:
        self.get(job_id)
        return self.runtime.store.cancel(job_id)

    def retry(self, job_id: str) -> dict[str, Any]:
        self.get(job_id)
        return self.runtime.retry(job_id)

    def start(self) -> None:
        self.runtime.start()

    def pause(self) -> None:
        self.runtime.pause()

    def resume(self) -> None:
        self.runtime.resume()

    def stop(self) -> None:
        self.runtime.stop()

    @property
    def idle(self) -> bool:
        return self.runtime.idle


_lock = threading.Lock()
_instance: EtfResearchJobs | None = None


def get_etf_research_jobs() -> EtfResearchJobs:
    global _instance
    with _lock:
        if _instance is None:
            _instance = EtfResearchJobs()
        return _instance


def shutdown_etf_research_jobs() -> None:
    global _instance
    with _lock:
        value, _instance = _instance, None
    if value is not None:
        value.stop()
