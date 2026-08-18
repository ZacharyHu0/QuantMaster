"""Public v1 market-temperature and rotation APIs."""

from __future__ import annotations

import csv
import hashlib
import io
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import Field

from quantmaster.rotation.contracts import (
    RotationJobSpec,
    RotationPreferencesUpdate,
    RotationRefreshRequest,
)
from quantmaster.rotation.service import get_rotation_service, get_rotation_worker
from quantmaster.rotation.store import RotationIntegrityError
from quantmaster.runtime.contracts import ContractModel
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.server.security import require_csrf

router = APIRouter(prefix="/api/v1", tags=["rotation"])


class EtfScanBody(ContractModel):
    as_of: str = Field(default="", pattern=r"^$|^\d{4}-\d{2}-\d{2}$")
    tier: Literal["production", "sandbox"] = "production"


def _pagination(
    values: list[dict[str, Any]],
    page: int,
    page_size: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    total = len(values)
    pages = max(1, (total + page_size - 1) // page_size)
    current = min(max(1, page), pages)
    start = (current - 1) * page_size
    return values[start : start + page_size], {
        "page": current,
        "page_size": page_size,
        "total": total,
        "pages": pages,
        "has_previous": current > 1,
        "has_next": current < pages,
    }


def _number(value: Any, fallback: float = float("-inf")) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _page_size(value: int | None, default: int = 50) -> int:
    selected = default if value is None else value
    if selected not in {25, 50, 100}:
        raise HTTPException(422, "page_size 仅允许 25、50 或 100")
    return selected


def _rotation_window(value: int) -> int:
    if value not in {1, 3, 5, 20}:
        raise HTTPException(422, "轮动观察窗口仅支持 1、3、5、20 日")
    return value


def _materialize_group_score(item: dict[str, Any], window: int) -> dict[str, Any]:
    """Expose exactly one selected-window score from the current snapshot contract."""
    obsolete = {"score", "rotation_score", "grade"}.intersection(item)
    scores = item.get("scores")
    selected = scores.get(str(window)) if isinstance(scores, dict) else None
    required = {"window", "score", "grade", "available_weight", "minimum_weight", "items"}
    if (
        obsolete
        or not isinstance(selected, dict)
        or not required.issubset(selected)
        or selected.get("window") != window
        or not isinstance(selected.get("items"), list)
    ):
        identity = str(item.get("code") or item.get("name") or "<unknown>")
        raise RotationIntegrityError(
            f"轮动快照项目 {identity} 缺少当前 {window} 日评分结构"
        )
    value = {key: raw for key, raw in item.items() if key != "scores"}
    value["score"] = {key: selected[key] for key in (
        "window", "score", "grade", "available_weight", "minimum_weight", "items",
    )}
    return value


_THEME_FOCUS_CRITERIA = (
    ("rotation", "轮动改善"),
    ("excess", "相对收益为正"),
    ("breadth", "上涨宽度过半"),
    ("amount", "量能活跃"),
    ("grade", "周期结构 A/B"),
)


def _theme_focus_items(
    items: list[dict[str, Any]],
    window: int,
    *,
    limit: int = 4,
) -> list[dict[str, Any]]:
    """Rank auditable theme evidence without inventing a new composite score."""
    ranked: list[dict[str, Any]] = []
    for item in items:
        current = dict((item.get("signals") or {}).get(str(window)) or {})
        reasons = []
        checks = {
            "rotation": _number(current.get("rotation_change_pp"), 0.0) > 0,
            "excess": _number(current.get("excess_return"), 0.0) > 0,
            "breadth": _number(current.get("advance_ratio"), 0.0) >= 0.5,
            "amount": _number(current.get("amount_activity"), 0.0) > 0,
            "grade": str((item.get("score") or {}).get("grade") or "") in {"A", "B"},
        }
        for criterion, label in _THEME_FOCUS_CRITERIA:
            if checks[criterion]:
                reasons.append({"id": criterion, "label": label})
        ranked.append(
            {
                **item,
                "focus": {
                    "evidence_count": len(reasons),
                    "evidence_total": len(_THEME_FOCUS_CRITERIA),
                    "reasons": reasons,
                },
            }
        )

    def sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
        current = (item.get("signals") or {}).get(str(window)) or {}
        return (
            -int((item.get("focus") or {}).get("evidence_count") or 0),
            -_number((item.get("score") or {}).get("score")),
            -_number(current.get("rotation_change_pp")),
            -_number(current.get("excess_return")),
            -_number(item.get("coverage")),
            str(item.get("name") or "").casefold(),
            str(item.get("code") or ""),
        )

    return sorted(ranked, key=sort_key)[:limit]


def _iso_time(value: Any) -> str:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), UTC).isoformat()
    return str(value or "")


def _snapshot_etag(
    request: Request,
    response: Response,
    payload: dict[str, Any],
) -> dict[str, Any] | Response:
    """Attach a deterministic read-side validator without inspecting data rows."""

    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    snapshot_id = str(meta.get("snapshot_id") or "")
    if not snapshot_id:
        return payload
    canonical_query = strict_json_dumps(
        sorted((str(key), str(value)) for key, value in request.query_params.multi_items()),
        sort_keys=True,
    )
    digest = hashlib.sha256(f"{snapshot_id}\n{canonical_query}".encode()).hexdigest()
    etag = f'"{digest}"'
    response.headers["ETag"] = etag
    response.headers["Cache-Control"] = "private, max-age=0, must-revalidate"
    requested = str(request.headers.get("if-none-match") or "")
    if requested == "*" or etag in {value.strip() for value in requested.split(",")}:
        return Response(status_code=304, headers={"ETag": etag})
    return payload


def public_rotation_job(value: dict[str, Any]) -> dict[str, Any]:
    worker = get_rotation_worker()
    public = worker.runtime.public(value)
    result: dict[str, Any] = {}
    artifact_id = str(value.get("result_artifact_id") or "")
    if artifact_id:
        try:
            artifact = worker.runtime.store.artifact(artifact_id)
        except (KeyError, RuntimeError, ValueError):
            artifact = None
        if isinstance(artifact, dict) and isinstance(artifact.get("payload"), dict):
            result = dict(artifact["payload"])
    public.update({
        "domain": "rotation",
        "as_of": str((value.get("spec") or {}).get("as_of") or ""),
        "completed_as_of": str(result.get("as_of") or result.get("actual_as_of") or ""),
        "expected_as_of": str(result.get("expected_as_of") or ""),
        "failure_reason": str(value.get("detail") or "")[:1000]
        if str(value.get("status") or "") in {"failed", "cancelled", "interrupted"} else "",
        "result": result or None,
    })
    return public


def get_rotation_job(job_id: str) -> dict[str, Any]:
    try:
        value = get_rotation_worker().runtime.store.get(job_id)
    except KeyError:
        raise KeyError(job_id) from None
    if str(value.get("type") or "") != "rotation.refresh":
        raise KeyError(job_id)
    return value


def list_rotation_jobs(limit: int) -> list[dict[str, Any]]:
    return [
        value for value in get_rotation_worker().runtime.store.list(max(limit * 4, limit))
        if str(value.get("type") or "") == "rotation.refresh"
    ][:limit]


def rotation_job_events(job_id: str, after: int, limit: int) -> list[dict[str, Any]]:
    get_rotation_job(job_id)
    return get_rotation_worker().runtime.store.events(job_id, after, limit)


def cancel_rotation_job(job_id: str) -> dict[str, Any]:
    get_rotation_job(job_id)
    return get_rotation_worker().runtime.store.cancel(job_id)


def retry_rotation_job(job_id: str) -> dict[str, Any]:
    get_rotation_job(job_id)
    value = get_rotation_worker().runtime.retry(job_id)
    get_rotation_worker().start()
    return value


@router.get("/market/temperature")
def market_temperature(request: Request, response: Response) -> Any:
    return _snapshot_etag(
        request, response, get_rotation_service(read_only=True).snapshot("temperature"),
    )


@router.get("/market/structure")
def market_structure(request: Request, response: Response) -> Any:
    return _snapshot_etag(
        request, response, get_rotation_service(read_only=True).snapshot("structure"),
    )


@router.post("/market/analytics/refresh", status_code=202)
def refresh_market_analytics(value: RotationRefreshRequest) -> dict[str, Any]:
    worker = get_rotation_worker()
    worker.start()
    job = worker.submit(RotationJobSpec.model_validate(value.model_dump(mode="json")))
    return public_rotation_job(job)


@router.get("/rotation/overview")
def rotation_overview(
    request: Request, response: Response, window: int = 5,
) -> Any:
    selected = _rotation_window(window)
    return _snapshot_etag(
        request, response, get_rotation_service(read_only=True).overview(selected),
    )


BoardIndexCategory = Literal["all", "sw1", "sw2", "theme"]
BoardIndexMethod = Literal["equal", "float_mv", "amount", "volume", "total_mv"]


def _board_index_key(category: str, code: str) -> str:
    if category not in {"sw1", "sw2", "theme"}:
        raise HTTPException(422, "板块类别仅支持 sw1、sw2 或 theme")
    return f"{category}:{code}".upper()


def _board_index_item(item: dict[str, Any], method: str, window: int) -> dict[str, Any]:
    methods = item.get("methods") if isinstance(item.get("methods"), dict) else {}
    selected = methods.get(method) if isinstance(methods.get(method), dict) else {}
    changes = selected.get("changes") if isinstance(selected.get("changes"), dict) else {}
    return {
        key: value for key, value in item.items() if key != "methods"
    } | {
        "method": method,
        "status": str(selected.get("status") or "unavailable"),
        "last": selected.get("last"),
        "change": changes.get(str(window)),
        "changes": changes,
        "sessions": int(selected.get("sessions") or 0),
        "reason": str(selected.get("reason") or ""),
    }


@router.get("/rotation/board-indexes")
def rotation_board_indexes(
    request: Request,
    response: Response,
    category: BoardIndexCategory = "all",
    method: BoardIndexMethod = "equal",
    window: int = 5,
    query: str = Query("", max_length=80),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    sort: Literal["change", "name", "coverage", "member_count"] = "change",
    order: Literal["asc", "desc"] = "desc",
) -> Any:
    """Read one compact page; StockDB calculation is never part of this route."""

    window = _rotation_window(window)
    selected_size = _page_size(page_size)
    service = get_rotation_service(read_only=True)
    header, values, _pagination_meta = service.store.snapshot_items_page(
        "board_indexes",
        query=query,
        category="" if category == "all" else category,
        page=1,
        page_size=500,
    )
    if header is None:
        raise HTTPException(503, "板块指数快照尚未发布")
    items = [_board_index_item(item, method, window) for item in values]

    def sort_key(item: dict[str, Any]) -> tuple[Any, str]:
        value = item.get(sort)
        if sort == "name":
            value = str(value or "").casefold()
        else:
            value = _number(value)
        return value, str(item.get("code") or "")

    available = [item for item in items if item.get(sort) is not None]
    unavailable = [item for item in items if item.get(sort) is None]
    available.sort(key=sort_key, reverse=order == "desc")
    items = available + unavailable
    items, pagination = _pagination(items, page, selected_size)
    data = dict(header.get("data") or {})
    data.update({
        "items": items,
        "pagination": pagination,
        "category": category,
        "method": method,
        "window": window,
    })
    return _snapshot_etag(request, response, {"meta": header["meta"], "data": data})


@router.get("/rotation/board-indexes/{category}/{code}/constituents")
def rotation_board_index_constituents(
    category: Literal["sw1", "sw2", "theme"],
    code: str,
    request: Request,
    response: Response,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    sort: Literal["change", "amount", "name"] = "change",
    order: Literal["asc", "desc"] = "desc",
) -> Any:
    service = get_rotation_service(read_only=True)
    header = service.store.snapshot_header("board_indexes")
    detail = service.store.snapshot_detail("board_indexes", _board_index_key(category, code))
    if header is None or detail is None:
        raise HTTPException(404, f"板块指数不存在或尚未发布: {category}/{code}")
    values = list(detail.get("constituents") or [])
    field = {"change": "change_pct", "amount": "amount", "name": "name"}[sort]
    available = [item for item in values if item.get(field) is not None]
    unavailable = [item for item in values if item.get(field) is None]
    available.sort(
        key=lambda item: (
            str(item.get(field) or "").casefold()
            if field == "name" else _number(item.get(field)),
            str(item.get("symbol") or ""),
        ),
        reverse=order == "desc",
    )
    values = available + unavailable
    values, pagination = _pagination(values, page, _page_size(page_size))
    return _snapshot_etag(request, response, {
        "meta": header["meta"],
        "data": {
            "code": detail.get("code"),
            "board_code": detail.get("board_code"),
            "name": detail.get("name"),
            "items": values,
            "pagination": pagination,
            "membership_semantics": detail.get("membership_semantics"),
        },
    })


@router.get("/rotation/board-indexes/{category}/{code}")
def rotation_board_index_detail(
    category: Literal["sw1", "sw2", "theme"],
    code: str,
    request: Request,
    response: Response,
    method: BoardIndexMethod = "equal",
) -> Any:
    service = get_rotation_service(read_only=True)
    header = service.store.snapshot_header("board_indexes")
    detail = service.store.snapshot_detail("board_indexes", _board_index_key(category, code))
    if header is None or detail is None:
        raise HTTPException(404, f"板块指数不存在或尚未发布: {category}/{code}")
    series = detail.get("series") if isinstance(detail.get("series"), dict) else {}
    methods = detail.get("methods") if isinstance(detail.get("methods"), dict) else {}
    data = {
        key: value for key, value in detail.items()
        if key not in {"series", "constituents", "methods"}
    }
    data.update({
        "method": method,
        "method_status": methods.get(method, {"status": "unavailable"}),
        "comparison": methods,
        "series": list(series.get(method) or []),
        "constituent_count": len(detail.get("constituents") or []),
    })
    return _snapshot_etag(request, response, {"meta": header["meta"], "data": data})


@router.get("/rotation/industries")
def rotation_industries(
    request: Request,
    response: Response,
    level: Literal["all", "L1", "L2"] = "all",
    query: str = Query("", max_length=80),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    window: int = 5,
) -> Any:
    window = _rotation_window(window)
    selected_size = _page_size(page_size)
    service = get_rotation_service(read_only=True)
    selected_l2 = set(service.store.preferences()["l2_codes"])
    _header, values, pagination = service.store.snapshot_items_page(
        "industries",
        query=query,
        level="" if level == "all" else level,
        allowed_keys=selected_l2,
        include_l1=True,
        page=page,
        page_size=selected_size,
    )
    snapshot = service.snapshot_header("industries")
    items = [_materialize_group_score(item, window) for item in values]
    data = dict(snapshot.get("data") or {})
    data.update({"items": items, "pagination": pagination, "window": window})
    return _snapshot_etag(request, response, {"meta": snapshot["meta"], "data": data})


@router.get("/rotation/industries/{code}")
def rotation_industry_detail(
    code: str, request: Request, response: Response, window: int = 5,
) -> Any:
    window = _rotation_window(window)
    result = get_rotation_service(read_only=True).detail("industries", code)
    if result is None:
        raise HTTPException(404, f"行业不存在或尚未达到覆盖门槛: {code}")
    return _snapshot_etag(request, response, {
        "meta": result["meta"], "data": _materialize_group_score(result["data"], window),
    })


@router.get("/rotation/themes")
def rotation_themes(
    request: Request,
    response: Response,
    query: str = Query("", max_length=80),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    stage: str = Query("", max_length=80),
    grade: Literal["", "A", "B", "C", "D"] = "",
    sort: Literal["change", "score", "excess", "amount", "coverage", "name"] = "change",
    order: Literal["asc", "desc"] = "desc",
    window: int = 5,
) -> Any:
    if "limit" in request.query_params:
        raise HTTPException(422, "limit 已删除；请使用 page 和 page_size")
    window = _rotation_window(window)
    service = get_rotation_service(read_only=True)
    snapshot = service.snapshot_header("themes")
    data = dict(snapshot.get("data") or {})
    _focus_header, focus_values, _focus_page = service.store.snapshot_items_page(
        "themes", sort="focus", order="desc", window=window, page=1, page_size=4,
    )
    focus_items = [_materialize_group_score(item, window) for item in focus_values]
    data.update(
        {
            "focus_items": _theme_focus_items(focus_items, window),
            "focus_definition": {
                "criteria": [{"id": criterion, "label": label} for criterion, label in _THEME_FOCUS_CRITERIA],
                "limit": 4,
                "window": window,
            },
        }
    )
    _header, values, pagination = service.store.snapshot_items_page(
        "themes", query=query, stage=stage, grade=grade, sort=sort, order=order,
        window=window, page=page, page_size=_page_size(page_size),
    )
    data.update({
        "items": [_materialize_group_score(item, window) for item in values],
        "pagination": pagination,
        "window": window,
    })
    return _snapshot_etag(request, response, {"meta": snapshot["meta"], "data": data})


@router.get("/rotation/themes/{code}")
def rotation_theme_detail(
    code: str, request: Request, response: Response, window: int = 5,
) -> Any:
    window = _rotation_window(window)
    result = get_rotation_service(read_only=True).detail("themes", code)
    if result is None:
        raise HTTPException(404, f"题材不存在或尚未达到覆盖门槛: {code}")
    return _snapshot_etag(request, response, {
        "meta": result["meta"], "data": _materialize_group_score(result["data"], window),
    })


@router.get("/rotation/etf-flows/items")
def rotation_etf_flow_items(
    request: Request,
    response: Response,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    query: str = Query("", max_length=80),
    category: str = Query("", max_length=80),
    sort: Literal["flow", "daily", "streak", "name"] = "flow",
    order: Literal["asc", "desc"] = "desc",
    window: int = 5,
) -> Any:
    window = _rotation_window(window)
    service = get_rotation_service(read_only=True)
    snapshot = service.snapshot_header("etf_flows")
    _header, values, pagination = service.store.snapshot_items_page(
        "etf_flows", query=query, category=category, sort=sort, order=order,
        window=window, page=page, page_size=_page_size(page_size),
    )
    return _snapshot_etag(request, response, {
        "meta": snapshot["meta"],
        "data": {
            "items": values,
            "pagination": pagination,
            "categories": service.store.snapshot_item_categories("etf_flows"),
        },
    })


def _compact_etf_funds(value: dict[str, Any] | None) -> dict[str, Any]:
    funds = value or {}
    keys = (
        "status",
        "effective_date",
        "lag_sessions",
        "source",
        "share_delta",
        "share_change_pct",
        "estimated_flow",
        "unchanged_sessions",
        "period_kind",
        "period_sessions",
        "period_label",
        "consecutive",
        "coverage",
        "coverage_level",
        "confirmed_members",
        "member_count",
        "directional_interpretation",
        "interpretation_note",
        "message",
    )
    return {
        key: funds.get(key)
        for key in keys
        if key in funds and funds.get(key) is not None and funds.get(key) != ""
    }


def _compact_etf_quality(value: dict[str, Any] | None) -> dict[str, Any]:
    """Keep list and overview responses light; field diagnostics live on coverage endpoint."""

    quality = value or {}
    keys = (
        "status",
        "product_count",
        "sector_count",
        "expected_symbols",
        "observed_symbols",
        "symbol_ratio",
        "required_ohlcv_ratio",
        "verified_adjustment_products",
        "official_metadata_products",
        "enhanced_metadata_products",
        "share_status_counts",
        "issues",
    )
    return {key: quality.get(key) for key in keys if key in quality}


def _etf_product_projection(item: Any, sector: dict[str, Any]) -> dict[str, Any]:
    metrics = item.metrics
    position_metric = str(sector.get("position_metric") or "position_60d")
    return {
        "symbol": item.symbol,
        "name": item.name,
        "category": item.category,
        "asset_class": item.asset_class,
        "sector_id": item.sector_id,
        "sector_name": item.sector_name,
        "normalized_index": item.normalized_index,
        "is_representative": item.is_representative,
        "representative_symbol": item.representative_symbol,
        "metrics": {
            "return_20d": metrics.get("return_20d"),
            "avg_amount_20d": metrics.get("avg_amount_20d"),
            "position_60d": metrics.get("position_60d"),
            "position_250d": metrics.get("position_250d"),
            "display_position": metrics.get(position_metric),
            "position_metric": position_metric,
        },
        "funds": _compact_etf_funds(item.funds),
        "sector_state": sector.get("state", "watch"),
        "sector_state_label": sector.get("state_label", "震荡观察"),
        "trend_strength": sector.get("trend_strength"),
        "activity_score": sector.get("activity_score"),
        "display_position": sector.get("display_position"),
        "position_label": sector.get("position_label", "60 日阶段位置"),
        "risk_badges": sector.get("risk_badges", []),
        "candidate_codes": sector.get("candidate_codes", []),
    }


@router.get("/rotation/etfs")
def rotation_etfs(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    query: str = Query("", max_length=80),
    category: str = Query("", max_length=80),
    asset: Literal["equity", "overseas_equity", "bond", "commodity", "money", "all"] = "all",
    state: Literal["leading", "low_turn", "improving", "weakening", "watch", ""] = "",
    sort: Literal["trend", "activity", "position", "amount", "return", "name"] = "trend",
    order: Literal["asc", "desc"] = "desc",
    snapshot_id: str = Query("", max_length=80),
    tier: Literal["production", "sandbox"] = "production",
) -> dict[str, Any]:
    from quantmaster.rotation.etf_research import get_etf_research_service

    service = get_etf_research_service(read_only=True)
    snapshot = service.resolve_snapshot(snapshot_id, tier=tier)
    if snapshot is None:
        return {
            "meta": {
                "tier": tier,
                "formal_eligible": False,
                "quality": {"status": "cold", "issues": ["尚未生成 ETF 研究快照"]},
            },
            "data": {"items": [], "categories": [], "pagination": _pagination([], 1, page_size)[1]},
        }
    needle = query.strip().casefold()
    sectors = {item["sector_id"]: item for item in snapshot.sectors}
    all_items = list(snapshot.items)
    categories = sorted(
        {item.category for item in all_items if asset == "all" or item.asset_class == asset}
    )
    items = all_items
    items = [
        item
        for item in items
        if (
            (
                not needle
                or needle in item.symbol.casefold()
                or needle in item.name.casefold()
                or needle in item.sector_name.casefold()
                or needle in item.normalized_index.casefold()
            )
            and (not category or item.category == category)
            and (asset == "all" or item.asset_class == asset)
            and (not state or sectors.get(item.sector_id, {}).get("state") == state)
        )
    ]

    def sort_key(item: Any) -> tuple[Any, ...]:
        metric = item.metrics
        sector = sectors.get(item.sector_id, {})
        values = {
            "trend": sector.get("trend_strength"),
            "activity": sector.get("activity_score"),
            "position": sector.get("display_position"),
            "amount": metric.get("avg_amount_20d"),
            "return": metric.get("return_20d"),
            "name": item.name,
        }
        if sort == "name":
            return (str(values[sort]).casefold(), item.symbol)
        value = values[sort]
        # Missing rows always remain behind rankable observations.
        return (value is None, _number(value, 0), item.symbol)

    if sort == "name":
        items.sort(key=sort_key, reverse=order == "desc")
    else:
        items.sort(
            key=lambda item: (
                sort_key(item)[0],
                -sort_key(item)[1] if order == "desc" else sort_key(item)[1],
                sort_key(item)[2],
            )
        )
    visible, pagination = _pagination(items, page, page_size)
    visible_projection = [
        _etf_product_projection(item, sectors.get(item.sector_id, {})) for item in visible
    ]
    return {
        "meta": {
            "snapshot_id": snapshot.snapshot_id,
            "ingest_id": snapshot.ingest_id,
            "artifact_id": snapshot.artifact_id,
            "as_of": snapshot.as_of_date,
            "quality": _compact_etf_quality(snapshot.coverage),
            "staleness": snapshot.staleness,
            "research_model_version": snapshot.research_model_version,
            "tier": snapshot.tier,
            "formal_eligible": snapshot.formal_eligible,
        },
        "data": {
            "items": visible_projection,
            "categories": categories,
            "pagination": pagination,
            "provenance": snapshot.provenance,
        },
    }


def _etf_overview_summaries(
    lookup: dict[str, dict[str, Any]], queues: dict[str, list[str]],
    candidate_queues: dict[str, list[str]], position_available: bool,
    strongest: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    strict_low = (
        lookup.get(queues.get("low_turn", [""])[0]) if queues.get("low_turn") else None
    )
    staged_low = (
        lookup.get(candidate_queues.get("stage_low_rebound", [""])[0])
        if candidate_queues.get("stage_low_rebound") else None
    )
    strict_risk = lookup.get(queues.get("risk", [""])[0]) if queues.get("risk") else None
    staged_risk = (
        lookup.get(candidate_queues.get("stage_high_activity", [""])[0])
        if candidate_queues.get("stage_high_activity") else None
    )
    weakening = lookup.get(queues.get("weakening", [""])[0]) if queues.get("weakening") else None
    summaries: list[dict[str, Any]] = []
    summaries.append({
        "kind": "strongest", "title": "趋势最强",
        "sector_id": strongest["sector_id"] if strongest else "",
        "sector_name": strongest["sector_name"] if strongest else "",
        "state": strongest["state"] if strongest else "none",
        "evaluation_status": "confirmed" if strongest else "unavailable",
        "text": (
            f"代表 {strongest['representative']['name']} · 趋势 {strongest['trend_strength']:.0f}"
            if strongest else "收益或成交额证据不足，暂无法比较"
        ),
    })
    low = strict_low or staged_low
    summaries.append({
        "kind": "low_turn", "title": "低位机会",
        "sector_id": low["sector_id"] if low else "",
        "sector_name": low["sector_name"] if low else "",
        "state": low["state"] if low else "none",
        "evaluation_status": "confirmed" if strict_low else "candidate" if staged_low else (
            "confirmed" if position_available else "unavailable"
        ),
        "text": (
            f"{low['state_label']} · 趋势 {low['trend_strength']:.0f}" if strict_low
            else "60 日阶段低位候选，尚缺长期确认" if staged_low
            else "本期未发现满足严格或阶段候选条件的板块" if position_available
            else "阶段位置证据不足，暂无法评估"
        ),
    })
    risk = strict_risk or staged_risk or weakening
    risk_status, risk_text = (
        ("confirmed", "长期高位拥挤风险已确认") if strict_risk else
        ("candidate", "60 日阶段高位活跃候选") if staged_risk else
        ("confirmed", "严格走弱，留意趋势延续") if weakening else
        ("confirmed", "本期未发现拥挤、阶段高位活跃或严格走弱板块") if position_available else
        ("unavailable", "位置证据不足，暂无法评估")
    )
    summaries.append({
        "kind": "risk", "title": "主要风险",
        "sector_id": risk["sector_id"] if risk else "",
        "sector_name": risk["sector_name"] if risk else "",
        "state": risk["state"] if risk else "none",
        "evaluation_status": risk_status, "text": risk_text,
    })
    return summaries


def _etf_map_config(asset: str, selected: list[dict[str, Any]]) -> tuple[str, int, str, bool]:
    long_coverage = (
        sum(item.get("metrics", {}).get("position_250d") is not None for item in selected)
        / len(selected) if selected else 0.0
    )
    use_long = asset != "money" and long_coverage >= 0.8
    if asset == "money":
        return "", 0, "货币 ETF 不评估高低位", use_long
    return (
        ("position_250d", 250, "250 日复权位置", use_long)
        if use_long else ("position_60d", 60, "60 日阶段位置", use_long)
    )


def _etf_map_ids(compact: list[dict[str, Any]], critical_ids: set[str]) -> list[str]:
    ranked = sorted(
        compact,
        key=lambda item: (item.get("activity_score") is not None, item.get("activity_score") or -1),
        reverse=True,
    )
    map_ids = [item["sector_id"] for item in compact if item["sector_id"] in critical_ids]
    for item in ranked:
        if len(map_ids) >= 40 and len(critical_ids) <= 40:
            break
        if item["sector_id"] not in map_ids:
            map_ids.append(item["sector_id"])
        if len(map_ids) >= max(40, len(critical_ids)):
            break
    return map_ids


def _etf_overview_payload(snapshot, asset: str) -> dict[str, Any]:
    capabilities = dict(snapshot.capabilities)
    metadata_capability = dict(capabilities.get("metadata") or {})
    denominator = dict(metadata_capability.get("denominator") or {})
    if denominator:
        members = denominator.pop("members", ())
        denominator["member_count"] = int(
            denominator.get("observed_symbols") or len(members)
        )
        metadata_capability["denominator"] = denominator
        capabilities["metadata"] = metadata_capability
    selected = [item for item in snapshot.sectors if asset == "all" or item.get("asset_class") == asset]
    selected_ids = {item["sector_id"] for item in selected}
    queues = {
        key: [sector_id for sector_id in values if sector_id in selected_ids]
        for key, values in snapshot.queues.items()
    }
    candidate_queues = {
        key: [sector_id for sector_id in values if sector_id in selected_ids]
        for key, values in snapshot.candidate_queues.items()
    }
    lookup = {item["sector_id"]: item for item in selected}
    rankable = [item for item in selected if item.get("trend_strength") is not None]
    strongest = max(rankable, key=lambda item: item["trend_strength"], default=None)
    position_available = any(item.get("display_position") is not None for item in selected)
    summaries = _etf_overview_summaries(
        lookup, queues, candidate_queues, position_available, strongest,
    )
    map_metric, map_horizon, map_label, use_long = _etf_map_config(asset, selected)

    compact: list[dict[str, Any]] = []
    for item in selected:
        metrics = item.get("metrics") or {}
        eligible_candidates = {
            code: value
            for code, value in (item.get("candidates") or {}).items()
            if value.get("eligible")
        }
        compact.append(
            {
                "sector_id": item["sector_id"],
                "sector_name": item["sector_name"],
                "category": item["category"],
                "asset_class": item["asset_class"],
                "representative": item["representative"],
                "member_count": item["member_count"],
                "index_count": item["index_count"],
                "metrics": {
                    key: metrics.get(key)
                    for key in (
                        "return_5d",
                        "return_20d",
                        "return_60d",
                        "amount_ratio_5v20",
                        "position_60d",
                        "position_250d",
                    )
                },
                "trend_strength": item.get("trend_strength"),
                "activity_score": item.get("activity_score"),
                "display_position": metrics.get(map_metric) if map_metric else None,
                "position_metric": map_metric,
                "position_horizon": map_horizon,
                "position_label": map_label,
                "position_source": (
                    "unavailable"
                    if (metrics.get(map_metric) if map_metric else None) is None
                    else str(item.get("long_position_source") or "verified_adjusted")
                    if use_long
                    else "stage_research_series"
                ),
                "state": item["state"],
                "state_label": item["state_label"],
                "risk_badges": item.get("risk_badges", []),
                "candidate_codes": list(eligible_candidates),
                "candidates": eligible_candidates,
                "funds": _compact_etf_funds(item.get("funds")),
            }
        )

    critical_ids = {
        item["sector_id"]
        for item in compact
        if item["state"] not in {"watch", "not_applicable"}
        or item["candidate_codes"]
        or item["risk_badges"]
    }
    map_ids = _etf_map_ids(compact, critical_ids)
    map_coverage = (
        sum(item.get("display_position") is not None for item in compact) / len(compact)
        if compact
        else 0.0
    )
    return {
        "freshness": snapshot.freshness,
        "capabilities": capabilities,
        "summaries": summaries,
        "sectors": compact,
        "queues": queues,
        "candidate_queues": candidate_queues,
        "map": {
            "position_metric": map_metric,
            "horizon": map_horizon,
            "label": map_label,
            "coverage": round(map_coverage, 4),
            "sector_ids": map_ids,
        },
    }


def _etf_evidence_hashes(
    service: Any, snapshot: Any, latest_input: Any, current_hashes: dict[str, str],
    generated_ns: int, content_hash: Any, frame_hash: Any, pd: Any, rotation_store: Any,
) -> dict[str, str]:
    factor_path = service.store.root / "evidence" / "adjustment_factors.parquet"
    evidence_paths = {
        "份额": getattr(rotation_store, "etf_path", None),
        "复权": factor_path,
        "元数据源": getattr(rotation_store, "etf_metadata_path", None),
    }
    fallback_labels: set[str] = set()
    for label, path in evidence_paths.items():
        if path is None:
            fallback_labels.add(label)
            continue
        try:
            stat = path.stat()
        except (FileNotFoundError, OSError):
            fallback_labels.add(label)
            continue
        previous_hash = snapshot.evidence_hashes.get(label, "")
        if previous_hash and generated_ns >= 0 and stat.st_mtime_ns <= generated_ns:
            current_hashes[label] = previous_hash
        else:
            current_hashes[label] = content_hash({
                "path": str(path), "mtime_ns": stat.st_mtime_ns, "size": stat.st_size,
            })
    if "份额" in fallback_labels:
        direct = service._direct_share_observations()
        if not direct.empty and "trade_date" in direct:
            direct = direct.copy()
            direct["trade_date"] = pd.to_datetime(direct["trade_date"], errors="coerce")
            direct = direct[direct["trade_date"].dt.date <= pd.Timestamp(latest_input.as_of_date).date()]
        current_hashes["份额"] = frame_hash(direct, (
            "symbol", "trade_date", "shares", "total_size", "nav", "close",
            "share_source", "source",
        ))
    if "复权" in fallback_labels:
        current_hashes["复权"] = snapshot.evidence_hashes.get("复权", content_hash([]))
    if "元数据源" in fallback_labels:
        current_hashes["元数据源"] = frame_hash(service._direct_metadata(), (
            "symbol", "name", "benchmark", "benchmark_code", "benchmark_type",
            "benchmark_level", "index_type", "index_provider", "fund_type",
            "invest_type", "mgt_fee", "metadata_source",
        ))
    return current_hashes


def _etf_refresh_hint(service: Any, snapshot: Any | None) -> dict[str, Any]:
    import pandas as pd

    from quantmaster.research.contracts import content_hash
    from quantmaster.rotation.etf_research import _frame_hash
    from quantmaster.rotation.store import RotationStore

    local_inputs = [
        item
        for item in service.ingest_store.history(100)
        if "etf" in item.assets and "etf_daily" in item.content_hashes
    ]
    latest_input = max(
        local_inputs,
        key=lambda item: str(item.as_of_date),
        default=None,
    )
    if latest_input is None:
        return {
            "recommended": False,
            "input_id": "",
            "input_as_of": "",
            "reason": "尚无可用于补算的本地 ETF 行情输入",
        }
    current_hashes = {"行情": content_hash(latest_input.content_hashes)}
    if snapshot is None:
        fingerprint = content_hash(
            {
                "input": latest_input.ingest_id,
                "as_of": latest_input.as_of_date,
                "evidence": current_hashes,
            }
        )
        return {
            "recommended": True,
            "input_id": fingerprint,
            "input_as_of": latest_input.as_of_date,
            "reason": "本地证据已变化（行情），进入页面时仅补算一次",
        }

    generated_at = pd.to_datetime(snapshot.generated_at, errors="coerce", utc=True)
    generated_ns = int(generated_at.value) if pd.notna(generated_at) else -1
    try:
        rotation_store = RotationStore(read_only=True)
    except (OSError, RuntimeError, TypeError, ValueError):
        rotation_store = None
    current_hashes = _etf_evidence_hashes(
        service, snapshot, latest_input, current_hashes, generated_ns,
        content_hash, _frame_hash, pd, rotation_store,
    )
    changed = [
        label
        for label, value in current_hashes.items()
        if snapshot is None or snapshot.evidence_hashes.get(label) != value
    ]
    recommended = snapshot is None or bool(changed)
    fingerprint = content_hash(
        {
            "input": latest_input.ingest_id,
            "as_of": latest_input.as_of_date,
            "evidence": current_hashes,
        }
    )
    return {
        "recommended": recommended,
        "input_id": fingerprint,
        "input_as_of": latest_input.as_of_date,
        "reason": (
            f"本地证据已变化（{'、'.join(changed)}），进入页面时仅补算一次"
            if recommended
            else "研究快照已使用最新行情、份额、复权与元数据证据"
        ),
    }


@router.get("/rotation/etfs/overview")
def rotation_etf_overview(
    asset: Literal["equity", "overseas_equity", "bond", "commodity", "money", "all"] = "equity",
    snapshot_id: str = Query("", max_length=80),
    tier: Literal["production", "sandbox"] = "production",
) -> dict[str, Any]:
    from quantmaster.rotation.etf_research import get_etf_research_service

    service = get_etf_research_service(read_only=True)
    snapshot = service.resolve_snapshot(snapshot_id, tier=tier)
    refresh = (
        _etf_refresh_hint(service, snapshot)
        if not snapshot_id and tier == "production"
        else {
            "recommended": False,
            "input_id": "",
            "input_as_of": "",
            "reason": (
                "sandbox 预览不自动发布或补算 production 快照"
                if tier == "sandbox"
                else "正在查看历史快照，不自动补算"
            ),
        }
    )
    if snapshot is None:
        return {
            "meta": {
                "tier": tier,
                "formal_eligible": False,
                "quality": {"status": "cold", "issues": ["尚未生成 ETF V2 研究快照"]},
                "refresh": refresh,
            },
            "data": {
                "freshness": {},
                "capabilities": {},
                "summaries": [],
                "sectors": [],
                "queues": {},
                "candidate_queues": {},
                "map": {
                    "position_metric": "",
                    "horizon": 0,
                    "label": "位置证据待补",
                    "coverage": 0.0,
                    "sector_ids": [],
                },
            },
        }
    return {
        "meta": {
            "snapshot_id": snapshot.snapshot_id,
            "as_of": snapshot.as_of_date,
            "research_model_version": snapshot.research_model_version,
            "tier": snapshot.tier,
            "formal_eligible": snapshot.formal_eligible,
            "staleness": snapshot.staleness,
            "quality": _compact_etf_quality(snapshot.coverage),
            "refresh": refresh,
        },
        "data": _etf_overview_payload(snapshot, asset),
    }


@router.get("/rotation/etfs/sectors/{sector_id}")
def rotation_etf_sector_detail(
    sector_id: str,
    snapshot_id: str = Query("", max_length=80),
    tier: Literal["production", "sandbox"] = "production",
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=50),
    index_key: str = Query("", max_length=160),
) -> dict[str, Any]:
    from quantmaster.rotation.etf_research import get_etf_research_service

    service = get_etf_research_service(read_only=True)
    snapshot = service.resolve_snapshot(snapshot_id, tier=tier)
    if snapshot is None:
        raise HTTPException(404, "尚无 ETF V2 研究快照")
    sector = next((item for item in snapshot.sectors if item["sector_id"] == sector_id), None)
    if sector is None:
        raise HTTPException(404, "ETF 板块不存在")
    member_items = [item for item in snapshot.items if item.sector_id == sector_id]
    members = list(member_items)
    members.sort(
        key=lambda item: (
            not item.is_representative,
            -_number(item.metrics.get("avg_amount_20d"), 0),
            item.symbol,
        )
    )
    index_groups: dict[str, list[Any]] = {}
    for item in members:
        key = item.benchmark_code or item.normalized_index or item.symbol
        index_groups.setdefault(key, []).append(item)
    groups = [
        {
            "index_key": key,
            "normalized_index": values[0].normalized_index or "指数待补",
            "benchmark_code": values[0].benchmark_code,
            "member_count": len(values),
            "representative": next(
                (
                    {"symbol": value.symbol, "name": value.name}
                    for value in values
                    if value.is_representative
                ),
                {"symbol": values[0].symbol, "name": values[0].name},
            ),
        }
        for key, values in index_groups.items()
    ]
    groups.sort(key=lambda value: (-value["member_count"], value["normalized_index"]))
    if index_key:
        members = [
            item
            for item in members
            if (item.benchmark_code or item.normalized_index or item.symbol) == index_key
        ]
    visible_members, member_pagination = _pagination(members, page, page_size)
    member_projection = [
        {
            "symbol": item.symbol,
            "name": item.name,
            "normalized_index": item.normalized_index,
            "benchmark_code": item.benchmark_code,
            "is_representative": item.is_representative,
            "metrics": {
                key: item.metrics.get(key)
                for key in (
                    "return_20d",
                    "avg_amount_20d",
                    "position_60d",
                    "position_250d",
                    "adjustment_status",
                )
            },
            "metadata": {
                key: item.metadata.get(key)
                for key in ("total_size", "management_fee", "classification_confidence")
            },
            "funds": _compact_etf_funds(item.funds),
            "coverage": item.coverage,
        }
        for item in visible_members
    ]
    fund_history: list[dict[str, Any]] = []
    observations = service._direct_share_observations()
    if not observations.empty and {"symbol", "trade_date", "shares"}.issubset(observations.columns):
        import pandas as pd

        symbols = {item.symbol for item in member_items}
        frame = observations[observations["symbol"].astype(str).str.upper().isin(symbols)].copy()
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
        frame["shares"] = pd.to_numeric(frame["shares"], errors="coerce")
        frame = frame[frame["trade_date"].dt.date <= pd.Timestamp(snapshot.as_of_date).date()]
        frame = frame.dropna(subset=["trade_date", "shares"]).sort_values(["symbol", "trade_date"])
        frame["share_delta"] = frame.groupby("symbol")["shares"].diff()
        nav = pd.to_numeric(frame.get("nav"), errors="coerce")
        close = pd.to_numeric(frame.get("close"), errors="coerce")
        frame["price"] = nav.fillna(close)
        frame["estimated_flow"] = frame["share_delta"] * frame["price"]
        grouped = (
            frame.dropna(subset=["share_delta"])
            .groupby("trade_date", as_index=False)
            .agg(
                share_delta=("share_delta", "sum"),
                estimated_flow=("estimated_flow", lambda values: values.sum(min_count=1)),
                confirmed_members=("symbol", "nunique"),
            )
        )
        fund_history = [
            {
                "date": row.trade_date.date().isoformat(),
                "share_delta": float(row.share_delta),
                "estimated_flow": (float(row.estimated_flow) if pd.notna(row.estimated_flow) else None),
                "confirmed_members": int(row.confirmed_members),
            }
            for row in grouped.tail(25).itertuples(index=False)
        ]
    return {
        "meta": {
            "snapshot_id": snapshot.snapshot_id,
            "as_of": snapshot.as_of_date,
            "staleness": snapshot.staleness,
            "tier": snapshot.tier,
            "formal_eligible": snapshot.formal_eligible,
        },
        "data": {
            **sector,
            "members": member_projection,
            "member_pagination": member_pagination,
            "index_groups": groups,
            "selected_index_key": index_key,
            "explanation": {
                "conclusion": sector["state_label"],
                "trigger_evidence": sector["evidence"],
                "risks": sector["risk_badges"],
                "candidates": sector.get("candidates", {}),
                "invalidation": sector["invalidation"],
            },
            "funds": {
                **sector["funds"],
                "history": fund_history,
                "provenance_note": "份额只解释一级市场申赎；二级市场成交不会自动改变总份额。",
            },
        },
    }


@router.get("/rotation/etfs/snapshots")
def rotation_etf_snapshots(limit: int = Query(50, ge=1, le=500)) -> dict[str, Any]:
    from quantmaster.rotation.etf_research import get_etf_research_service

    return {"items": get_etf_research_service(read_only=True).store.history(limit)}


@router.get("/rotation/etfs/snapshots/{snapshot_id}/coverage")
def rotation_etf_snapshot_coverage(
    snapshot_id: str,
    tier: Literal["production", "sandbox"] = "production",
) -> dict[str, Any]:
    from quantmaster.rotation.etf_research import get_etf_research_service

    snapshot = get_etf_research_service(read_only=True).resolve_snapshot(snapshot_id, tier=tier)
    if snapshot is None:
        raise HTTPException(404, "ETF 研究快照不存在")
    semantic_counts: dict[str, int] = {}
    excluded: dict[str, int] = {}
    for item in snapshot.items:
        share_status = str(item.funds.get("status") or "missing")
        semantic_counts[share_status] = semantic_counts.get(share_status, 0) + 1
        if item.metrics.get("adjustment_status") not in {"official", "verified_local"}:
            reason = "缺少可核查复权因子，长期位置不输出；地图使用 60 日阶段口径"
            excluded[reason] = excluded.get(reason, 0) + 1
    map_coverage = {
        asset: _etf_overview_payload(snapshot, asset).get("map", {})
        for asset in ("equity", "overseas_equity", "bond", "commodity", "money")
    }
    return {
        "meta": {
            "snapshot_id": snapshot.snapshot_id,
            "ingest_id": snapshot.ingest_id,
            "as_of": snapshot.as_of_date,
            "staleness": snapshot.staleness,
            "tier": snapshot.tier,
            "formal_eligible": snapshot.formal_eligible,
        },
        "data": {
            "coverage": snapshot.coverage,
            "share_semantic_counts": semantic_counts,
            "intraday_mode": "on_demand",
            "map_position": map_coverage,
            "excluded_reasons": excluded,
            "total_symbols": len(snapshot.items),
            "sector_count": len(snapshot.sectors),
            "evidence_hashes": snapshot.evidence_hashes,
            "freshness": snapshot.freshness,
        },
    }


@router.get("/rotation/etfs/export/{snapshot_id}")
def rotation_etf_export(
    snapshot_id: str,
    format: Literal["json", "csv"] = "json",
    tier: Literal["production", "sandbox"] = "production",
) -> Response:
    from quantmaster.rotation.etf_research import get_etf_research_service

    snapshot = get_etf_research_service(read_only=True).resolve_snapshot(snapshot_id, tier=tier)
    if snapshot is None:
        raise HTTPException(404, "ETF 研究快照不存在")
    if format == "json":
        return Response(
            strict_json_dumps(snapshot.to_dict(), indent=2),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{snapshot_id}.json"'},
        )
    output = io.StringIO(newline="")
    fields = [
        "category",
        "asset_class",
        "sector_name",
        "normalized_index",
        "symbol",
        "name",
        "as_of_date",
        "is_representative",
        "return_5d",
        "return_20d",
        "return_60d",
        "avg_amount_20d",
        "position_250d",
        "amount_ratio_5v20",
        "adjustment_status",
        "share_status",
        "share_delta",
        "share_change_pct",
        "estimated_flow",
    ]
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    for item in snapshot.items:
        writer.writerow(
            {
                "category": item.category,
                "asset_class": item.asset_class,
                "sector_name": item.sector_name,
                "normalized_index": item.normalized_index,
                "symbol": item.symbol,
                "name": item.name,
                "as_of_date": item.as_of_date,
                "is_representative": item.is_representative,
                **{key: item.metrics.get(key) for key in fields if key in item.metrics},
                "share_status": item.funds.get("status"),
                "share_delta": item.funds.get("share_delta"),
                "share_change_pct": item.funds.get("share_change_pct"),
                "estimated_flow": item.funds.get("estimated_flow"),
            }
        )
    return Response(
        output.getvalue().encode("utf-8-sig"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{snapshot_id}.csv"'},
    )


@router.post("/rotation/etfs/scan", status_code=202)
def rotation_etf_scan(body: EtfScanBody, request: Request) -> dict[str, Any]:
    require_csrf(request)
    from quantmaster.rotation.etf_jobs import get_etf_research_jobs

    job, created = get_etf_research_jobs().submit(as_of=body.as_of, tier=body.tier)
    return {**get_etf_research_jobs().public(job), "created": created}


@router.get("/rotation/etfs/{symbol}/intraday")
def rotation_etf_intraday(
    symbol: str,
    snapshot_id: str = Query("", max_length=80),
    tier: Literal["production", "sandbox"] = "production",
) -> dict[str, Any]:
    from quantmaster.rotation.etf_research import get_etf_research_service

    service = get_etf_research_service(read_only=True)
    snapshot = service.resolve_snapshot(snapshot_id, tier=tier)
    if snapshot is None:
        raise HTTPException(404, "尚无 ETF 研究快照")
    canonical = symbol.upper()
    if not any(item.symbol == canonical for item in snapshot.items):
        raise HTTPException(404, f"ETF 不在当前研究股票池: {canonical}")
    try:
        data = service.intraday(canonical, as_of_date=snapshot.as_of_date)
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise HTTPException(503, f"分钟走势暂不可用：{str(exc)[:180]}") from None
    return {
        "meta": {
            "snapshot_id": snapshot.snapshot_id,
            "as_of": snapshot.as_of_date,
            "scoring_input": False,
            "tier": snapshot.tier,
            "formal_eligible": snapshot.formal_eligible,
        },
        "data": data,
    }


@router.get("/rotation/etfs/{symbol}")
def rotation_etf_detail(
    symbol: str,
    snapshot_id: str = Query("", max_length=80),
    tier: Literal["production", "sandbox"] = "production",
) -> dict[str, Any]:
    from quantmaster.rotation.etf_research import get_etf_research_service

    service = get_etf_research_service(read_only=True)
    snapshot = service.resolve_snapshot(snapshot_id, tier=tier)
    if snapshot is None:
        raise HTTPException(404, "尚无 ETF 研究快照")
    canonical = symbol.upper()
    item = next((value for value in snapshot.items if value.symbol == canonical), None)
    if item is None:
        raise HTTPException(404, f"ETF 不在当前研究股票池: {canonical}")
    sector = next(
        (value for value in snapshot.sectors if value["sector_id"] == item.sector_id),
        {},
    )
    peer_key = item.benchmark_code or item.normalized_index
    peers = [
        value.to_dict()
        for value in snapshot.items
        if value.symbol != item.symbol
        and bool(peer_key)
        and (value.benchmark_code or value.normalized_index) == peer_key
    ]
    peers.sort(
        key=lambda value: (
            not value["is_representative"],
            -_number((value.get("metrics") or {}).get("avg_amount_20d"), 0),
            value["symbol"],
        )
    )
    try:
        history = service.product_history(
            item.symbol,
            snapshot_id=snapshot.snapshot_id,
            tier=tier,
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise HTTPException(
            409,
            f"ETF 历史证据不可复现：{str(exc)[:240]}",
        ) from None
    data = item.to_dict()
    data.update(
        {
            "sector_state": sector.get("state", "watch"),
            "sector_state_label": sector.get("state_label", "震荡观察"),
            "trend_strength": sector.get("trend_strength"),
            "activity_score": sector.get("activity_score"),
            "risk_badges": sector.get("risk_badges", []),
            "invalidation": sector.get("invalidation", ""),
            "candidates": sector.get("candidates", {}),
            "candidate_codes": sector.get("candidate_codes", []),
            "display_position": sector.get("display_position"),
            "position_label": sector.get("position_label", "60 日阶段位置"),
            "sector_history": sector.get("history", []),
            "history": history,
            "peer_products": peers,
            "display": {
                "资产类别": item.category,
                "研究板块": item.sector_name,
                "规范化指数": item.normalized_index or "—",
                "复权状态": item.metrics.get("adjustment_status") or "unavailable",
                "资金状态": item.funds.get("message") or "—",
            },
        }
    )
    return {
        "meta": {
            "snapshot_id": snapshot.snapshot_id,
            "as_of": snapshot.as_of_date,
            "staleness": snapshot.staleness,
            "tier": snapshot.tier,
            "formal_eligible": snapshot.formal_eligible,
        },
        "data": data,
    }


@router.get("/rotation/etf-flows")
def rotation_etf_flows(include_items: bool = True) -> dict[str, Any]:
    snapshot = get_rotation_service(read_only=True).snapshot("etf_flows")
    if include_items:
        return snapshot
    data = dict(snapshot.get("data") or {})
    items = list(data.pop("items", []) or [])
    data["item_total"] = len(items)
    return {"meta": snapshot["meta"], "data": data}


@router.get("/rotation/taxonomy/industries")
def rotation_taxonomy() -> dict[str, Any]:
    return get_rotation_service(read_only=True).taxonomy()


@router.get("/rotation/preferences")
def rotation_preferences() -> dict[str, Any]:
    return {"data": get_rotation_service(read_only=True).store.preferences()}


@router.put("/rotation/preferences")
def update_rotation_preferences(value: RotationPreferencesUpdate) -> dict[str, Any]:
    service = get_rotation_service()
    taxonomy = service.taxonomy()["data"]
    known_l2 = {str(item["code"]) for item in taxonomy.get("l2") or []}
    unknown = [code for code in value.l2_codes if code.upper() not in known_l2]
    if unknown:
        raise HTTPException(422, f"不是当前申万二级行业代码: {', '.join(unknown[:5])}")
    saved = service.store.save_preferences(value.model_dump(mode="json"))
    return {"data": saved}
