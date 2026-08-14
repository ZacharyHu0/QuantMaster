"""Stable, user-actionable Quant Lab failures."""

from __future__ import annotations

import errno
from typing import Any


class LabError(RuntimeError):
    """A safe public failure with a stable code and recovery action."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        action: str = "",
        retryable: bool = False,
        context: dict[str, Any] | None = None,
        status_code: int = 409,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.action = action
        self.retryable = retryable
        self.context = dict(context or {})
        self.status_code = int(status_code)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "action": self.action,
            "retryable": self.retryable,
            "context": self.context,
        }


def classify_lab_error(exc: Exception) -> LabError:
    """Normalize runtime failures without leaking paths, credentials or tracebacks."""
    if isinstance(exc, LabError):
        return exc
    text = str(exc).strip() or "Quant Lab 操作失败"
    lowered = text.lower()
    if isinstance(exc, InterruptedError):
        return LabError("CANCELLED", "任务已取消", retryable=True)
    if isinstance(exc, FileNotFoundError):
        return LabError(
            "DATASET_MISSING", "所需本地数据或工件不存在",
            action="先运行数据准备或修复缺失工件", retryable=True, status_code=424,
        )
    winerror = getattr(exc, "winerror", None)
    if (
        (isinstance(exc, OSError) and exc.errno == errno.ENOSPC)
        or winerror == 112
        or "no space left" in lowered
        or "database or disk is full" in lowered
        or "sqlite_full" in lowered
    ):
        return LabError(
            "STORAGE_SPACE_INSUFFICIENT", "存储卷空间不足",
            action="释放对应数据卷空间后从安全重试点继续",
            retryable=True,
        )
    if (
        "disk i/o error" in lowered
        or "input/output error" in lowered
        or "sqlite_ioerr" in lowered
    ):
        return LabError(
            "STORAGE_IO_ERROR", "存储设备或数据库发生 I/O 错误",
            action="检查卷、文件占用、ACL 与 SQLite WAL 状态后重试",
            retryable=True, status_code=503,
        )
    if "cuda" in lowered or "显存" in text:
        return LabError(
            "CUDA_UNAVAILABLE", "CUDA 运行时不可用或显存不足",
            action="运行 qm lab doctor，检查 CUDA PyTorch 与显存预算",
            retryable=True, status_code=424,
        )
    if "memory" in lowered or "allocate" in lowered or "内存" in text:
        return LabError(
            "MEMORY_BUDGET_EXCEEDED", "任务超过可用内存预算",
            action="缩短研究区间或降低并发后重试", retryable=True,
        )
    if "timeout" in lowered or "timed out" in lowered or "超时" in text:
        return LabError(
            "EXTERNAL_SERVICE_UNAVAILABLE", "外部服务超时",
            action="等待服务恢复后重试；本地研究不会受影响", retryable=True,
            status_code=503,
        )
    return LabError(
        "INTERNAL_ERROR", "Quant Lab 操作失败",
        action="查看任务事件和本机日志后重试", retryable=False, status_code=500,
    )
