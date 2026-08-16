"""Versioned market.stock_analysis submission and progressive report API."""

from __future__ import annotations

import sqlite3
from typing import Literal

from fastapi import APIRouter, Header, HTTPException
from pydantic import Field, field_validator

from quantmaster.analysis.stock_jobs import get_stock_analysis_jobs, read_stock_analysis
from quantmaster.runtime.contracts import ContractModel

router = APIRouter(prefix="/api/v1/market/stock-analyses", tags=["market"])


class StockAnalysisCreate(ContractModel):
    query: str = Field(..., min_length=1, max_length=80)
    mode: Literal["deep", "quick"] = "deep"

    @field_validator("query")
    @classmethod
    def validate_query(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("query 不能为空")
        return normalized


@router.get("")
def list_stock_analyses() -> dict:
    """List recent stock analysis runs."""
    try:
        jobs = get_stock_analysis_jobs().history(limit=20)
        return {"items": jobs}
    except (OSError, RuntimeError, ValueError, TypeError) as exc:
        return {"items": [], "error": str(exc)}


@router.post("", status_code=202)
def create_stock_analysis(
    value: StockAnalysisCreate,
    idempotency_key: str = Header(default="", alias="Idempotency-Key", max_length=200),
) -> dict:
    try:
        job, _ = get_stock_analysis_jobs().submit(
            value.query,
            value.mode,
            idempotency_key=idempotency_key,
        )
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from None
    return {
        "analysis_id": job["id"],
        "job_id": job["id"],
        "status": job["status"],
        "mode": value.mode,
    }


@router.get("/{analysis_id}")
def get_stock_analysis(analysis_id: str) -> dict:
    try:
        return read_stock_analysis(analysis_id)
    except (FileNotFoundError, sqlite3.Error):
        raise HTTPException(503, "个股分析快照暂不可读") from None
    except KeyError:
        raise HTTPException(404, "个股分析不存在") from None
