"""本机设置、候选、迁移与券商 CSV 导入 API。"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Body, File, Form, HTTPException, Request, Response, UploadFile
from pydantic import Field

from quantmaster.config import get_config
from quantmaster.credentials import CredentialError
from quantmaster.data.migration import MigrationError, migration_manager
from quantmaster.runtime.contracts import ContractModel
from quantmaster.server.security import (
    attach_csrf_cookie,
    is_local_request,
    issue_csrf,
    require_csrf,
    require_local,
)
from quantmaster.settings import (
    SecretMutations,
    SettingsDocument,
    SettingsUpdate,
    document_from_config,
)

router = APIRouter(prefix="/api/v1")
settings_manager = migration_manager.config_manager
_running_server: dict[str, Any] = {}
_applied_migrations: set[str] = set()


def _local(request: Request) -> bool:
    return is_local_request(request)


def _require_local(request: Request) -> None:
    require_local(request)


def _issue_csrf() -> str:
    return issue_csrf()


def _require_csrf(request: Request) -> None:
    require_csrf(request)


def capture_runtime_baseline() -> None:
    """在应用 lifespan 开始时记录真正需要重启才能改变的服务地址。"""
    cfg = get_config()
    _running_server.clear()
    _running_server.update({"host": cfg.server.host, "port": cfg.server.port})


def _runtime_status() -> dict[str, Any]:
    from quantmaster.automation.runtime import get_runtime
    from quantmaster.lab.worker import get_worker

    cfg = get_config()
    configured = {"host": cfg.server.host, "port": cfg.server.port}
    running = _running_server or configured
    restart = [f"server.{name}" for name in ("host", "port")
               if running.get(name) != configured.get(name)]
    return {
        "config_revision": settings_manager.public().get("config_revision", ""),
        "server": {
            "status": "restart_required" if restart else "applied",
            "running": dict(running), "configured": configured,
            "restart_required": restart,
        },
        "automation": get_runtime().status(),
        "lab": get_worker().status(),
    }


def _apply_runtime(result: dict[str, Any]) -> dict[str, Any]:
    """按变更字段热应用进程内服务；配置落盘成功不因联网状态回滚。"""
    from quantmaster.automation.runtime import get_runtime
    from quantmaster.lab.worker import get_worker

    changed = list(result.get("changed_fields") or [])
    apply_status: dict[str, Any] = {
        "config": {"status": "applied"},
        "automation": {"status": "unchanged"},
        "lab": {"status": "unchanged"},
        "server": {"status": "restart_required" if result.get("restart_required") else "applied"},
    }
    try:
        runtime = get_runtime()
        if "data.root" in changed:
            active = runtime.start() if get_config().automation.enabled else False
            apply_status["automation"] = {
                "status": "applied" if active else
                "disabled" if not get_config().automation.enabled else "standby"
            }
        elif any(field.startswith("automation.") for field in changed):
            apply_status["automation"] = runtime.apply_config(changed)
    except Exception as exc:  # 配置已安全保存；运行态失败降级为可操作警告。
        apply_status["automation"] = {"status": "degraded", "message": str(exc)[:300]}
        result.setdefault("warnings", []).append("自动化配置已保存，但运行时热应用失败")
    try:
        worker = get_worker()
        if "data.root" in changed:
            if get_config().lab.enabled:
                worker.start()
                apply_status["lab"] = {"status": "applied"}
            else:
                apply_status["lab"] = {"status": "disabled"}
        elif any(field.startswith("lab.") for field in changed) or "automation.timezone" in changed:
            apply_status["lab"] = worker.apply_config(changed)
    except Exception as exc:
        apply_status["lab"] = {"status": "degraded", "message": str(exc)[:300]}
        result.setdefault("warnings", []).append("Quant Lab 配置已保存，但 Worker 热应用失败")
    if "data.root" in changed:
        try:
            from quantmaster.backtest.workbench import get_backtest_worker
            from quantmaster.data.maintenance import data_refresh_manager
            from quantmaster.research.jobs import get_research_job_manager

            data_refresh_manager.start()
            get_research_job_manager().start()
            get_backtest_worker().start()
            apply_status["data_workers"] = {"status": "applied"}
        except Exception as exc:
            apply_status["data_workers"] = {
                "status": "degraded", "message": str(exc)[:300],
            }
            result.setdefault("warnings", []).append(
                "数据目录已切换，但部分后台执行器需要重启服务后恢复"
            )
    result["apply_status"] = apply_status
    result["runtime"] = _runtime_status()
    return result


@router.get("/settings")
def get_settings(request: Request, response: Response) -> dict:
    _require_local(request)
    token = _issue_csrf()
    attach_csrf_cookie(response, request, token)
    return {**settings_manager.public(), "csrf_token": token, "remote_management": False,
            "runtime": _runtime_status()}


@router.get("/settings/runtime")
def settings_runtime(request: Request) -> dict:
    _require_local(request)
    return _runtime_status()


@router.post("/settings/validate")
def validate_settings(request: Request, document: SettingsDocument) -> dict:
    _require_csrf(request)
    return settings_manager.validate(document)


@router.put("/settings")
def save_settings(request: Request, update: SettingsUpdate) -> dict:
    _require_csrf(request)
    try:
        result = _apply_runtime(settings_manager.save(update))
    except CredentialError as exc:
        raise HTTPException(409, str(exc)) from None
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None
    return {**result, "settings": settings_manager.public()}


def _check_document(body: dict[str, Any]) -> tuple[SettingsDocument, SecretMutations]:
    source = body.get("settings") if isinstance(body.get("settings"), dict) else body
    clean = {key: source[key] for key in (
        "config_version", "llm", "data", "trade", "news", "server", "automation", "lab")
             if key in source}
    if clean:
        document = SettingsDocument.model_validate(clean)
    else:
        document = SettingsDocument.model_validate({key: settings_manager.public()[key]
                                                    for key in SettingsDocument.model_fields})
    secrets_value = body.get("secrets") or source.get("secrets") or {}
    return document, SecretMutations.model_validate(secrets_value)


@router.post("/settings/check/{kind}")
def check_setting(kind: Literal[
        "llm-models", "tushare", "storage", "data-sources", "server", "lab"],
                  request: Request,
                  body: Annotated[dict[str, Any] | None, Body()] = None) -> dict:
    _require_csrf(request)
    from quantmaster.settings_checks import (
        check_data_sources,
        check_lab,
        check_server,
        check_storage,
        check_tushare,
        list_llm_models,
    )

    document, mutations = _check_document(body or {})
    current = get_config()
    if mutations.llm.action == "replace":
        llm_secret = mutations.llm.value or ""
    elif mutations.llm.action == "clear":
        llm_secret = ""
    else:
        same_llm_target = (
            document.llm.provider == current.llm.provider and
            document.llm.base_url.rstrip("/") == current.llm.base_url.rstrip("/")
        )
        # provider/base URL 改变时绝不把旧服务的密钥拿去探测新地址。
        llm_secret = current.llm.api_key if same_llm_target else ""
    if mutations.tushare.action == "replace":
        tushare_secret = mutations.tushare.value or ""
    elif mutations.tushare.action == "clear":
        tushare_secret = ""
    else:
        tushare_secret = current.data.tushare_token
    if kind == "llm-models":
        return list_llm_models(document.llm, llm_secret)
    if kind == "tushare":
        return check_tushare(tushare_secret)
    if kind == "storage":
        return check_storage(document.data)
    if kind == "data-sources":
        return check_data_sources(document.llm.timeout)
    if kind == "lab":
        return check_lab(document.lab, document.data, tushare_secret)
    return check_server(document.server)


class SnapshotCreate(ContractModel):
    name: str = Field(min_length=1, max_length=80)


@router.get("/settings/snapshots")
def list_snapshots(request: Request) -> dict:
    _require_local(request)
    return {"snapshots": settings_manager.list_snapshots()}


@router.post("/settings/snapshots")
def create_snapshot(request: Request, value: SnapshotCreate) -> dict:
    _require_csrf(request)
    try:
        return settings_manager.create_named_snapshot(value.name)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None


@router.get("/settings/snapshots/{snapshot_id}/diff")
def snapshot_diff(snapshot_id: str, request: Request) -> dict:
    _require_local(request)
    try:
        return {"diff": settings_manager.snapshot_diff(snapshot_id)}
    except (ValueError, FileNotFoundError) as exc:
        raise HTTPException(404, str(exc)) from None


@router.post("/settings/snapshots/{snapshot_id}/rollback")
def rollback_snapshot(snapshot_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        return _apply_runtime(settings_manager.rollback(snapshot_id))
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from None
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None


@router.delete("/settings/snapshots/{snapshot_id}")
def delete_snapshot(snapshot_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        settings_manager.delete_snapshot(snapshot_id)
        return {"status": "ok"}
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from None
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None


class MigrationCreate(ContractModel):
    target: str = Field(min_length=1, max_length=4096)
    mode: Literal["copy", "switch"] = "copy"


@router.post("/data/migrations")
def create_migration(request: Request, value: MigrationCreate) -> dict:
    _require_csrf(request)
    from quantmaster.automation.runtime import get_runtime
    from quantmaster.backtest.workbench import get_backtest_worker
    from quantmaster.data.maintenance import data_refresh_manager
    from quantmaster.lab.worker import get_worker
    from quantmaster.research.jobs import get_research_job_manager

    if data_refresh_manager.active:
        raise HTTPException(
            409, "行情数据库正在增量同步；请先完成或取消同步，再迁移数据目录")
    active_job = get_worker().status().get("active_job_id")
    if active_job:
        raise HTTPException(
            409, "Quant Lab 当前有研究任务在执行；任务完成后再迁移，当前任务不会被中断")
    research_active = [
        item for item in get_research_job_manager().list(200)
        if item["status"] in {"queued", "running", "cancelling"}
    ]
    if research_active:
        raise HTTPException(409, "研究数据任务仍在执行；完成或取消后再迁移数据目录")
    backtest_active = [
        item for item in get_backtest_worker().service.store.list(200)
        if item["status"] in {"queued", "running"}
    ]
    if backtest_active:
        raise HTTPException(409, "回测任务仍在执行；完成或取消后再迁移数据目录")
    if get_config().automation.enabled and get_runtime().status().get("started"):
        raise HTTPException(409, "自动化调度仍在运行；请先停用自动化再迁移数据目录")
    try:
        return migration_manager.create(value.target, value.mode)
    except MigrationError as exc:
        raise HTTPException(400, str(exc)) from None


@router.get("/data/migrations/{task_id}")
def get_migration(task_id: str, request: Request) -> dict:
    _require_local(request)
    try:
        task = migration_manager.get(task_id)
        if task.get("status") == "completed" and task_id not in _applied_migrations:
            _applied_migrations.add(task_id)
            task["apply"] = _apply_runtime({
                "status": "ok", "changed_fields": ["data.root"],
                "restart_required": [], "warnings": [],
            }).get("apply_status")
        return task
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from None


@router.post("/data/migrations/{task_id}/cancel")
def cancel_migration(task_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        return migration_manager.cancel(task_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from None


class DataRefreshRequest(ContractModel):
    scope: Literal["market", "universe", "all_cached"] = "market"
    universe: str = Field(default="", max_length=80)
    start: str = Field(default="", max_length=10)


@router.post("/data/refresh/preview")
def preview_data_refresh(request: Request, value: DataRefreshRequest) -> dict:
    _require_csrf(request)
    from quantmaster.data.maintenance import data_refresh_manager

    try:
        return data_refresh_manager.preview(value.scope, value.universe, value.start)
    except (ValueError, RuntimeError, FileNotFoundError) as exc:
        raise HTTPException(400, str(exc)) from None


@router.post("/data/refresh")
def create_data_refresh(request: Request, value: DataRefreshRequest) -> dict:
    _require_csrf(request)
    from quantmaster.data.maintenance import data_refresh_manager

    try:
        return data_refresh_manager.create(value.scope, value.universe, value.start)
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from None
    except (RuntimeError, FileNotFoundError) as exc:
        raise HTTPException(400, str(exc)) from None


@router.get("/data/refresh/latest")
def latest_data_refresh(request: Request) -> dict:
    _require_local(request)
    from quantmaster.data.maintenance import data_refresh_manager

    return {"job": data_refresh_manager.latest()}


@router.get("/data/refresh/{job_id}")
def get_data_refresh(job_id: str, request: Request) -> dict:
    _require_local(request)
    from quantmaster.data.maintenance import data_refresh_manager

    try:
        return data_refresh_manager.get(job_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from None


@router.get("/data/refresh/{job_id}/events")
def get_data_refresh_events(
    job_id: str,
    request: Request,
    after: int = 0,
    limit: int = 500,
) -> dict:
    _require_local(request)
    from quantmaster.data.maintenance import data_refresh_manager

    try:
        data_refresh_manager.get(job_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from None
    return {"items": data_refresh_manager.events(job_id, after, limit)}


@router.post("/data/refresh/{job_id}/cancel")
def cancel_data_refresh(job_id: str, request: Request) -> dict:
    _require_csrf(request)
    from quantmaster.data.maintenance import data_refresh_manager

    try:
        return data_refresh_manager.cancel(job_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from None
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from None


@router.post("/data/refresh/{job_id}/resume")
def resume_data_refresh(job_id: str, request: Request) -> dict:
    _require_csrf(request)
    from quantmaster.data.maintenance import data_refresh_manager

    try:
        return data_refresh_manager.resume(job_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from None
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from None


class UniverseBody(ContractModel):
    name: str | None = None
    symbols: list[str] = Field(default_factory=list, max_length=10_000)


class UniverseRename(ContractModel):
    new_name: str


class UniversePreview(ContractModel):
    kind: Literal["manual", "index"] = "manual"
    symbols: list[str] = Field(default_factory=list, max_length=10_000)
    index_symbol: str = "000300.SH"
    selections: dict[str, str] = Field(default_factory=dict)


class UniverseNameRefresh(ContractModel):
    symbols: list[str] = Field(default_factory=list, max_length=10_000)


class InstrumentResolveBody(ContractModel):
    queries: list[str] = Field(default_factory=list, max_length=10_000)
    selections: dict[str, str] = Field(default_factory=dict)


def _universe_references(name: str) -> list[dict[str, str]]:
    cfg = settings_manager.load()
    references = []
    if cfg.automation.primary_universe.casefold() == name.casefold():
        references.append({
            "key": "automation.primary_universe", "label": "自动化主候选",
        })
    if cfg.lab.universe.casefold() == name.casefold():
        references.append({
            "key": "lab.universe", "label": "Quant Lab 默认候选",
        })
    return references


def _fixed_universe_metadata(item: dict) -> dict:
    built_in = item["name"].casefold() == "demo"
    return {
        **item,
        "kind": "fixed",
        "source": "built_in" if built_in else "custom",
        "research_quality": "sandbox",
        "references": _universe_references(item["name"]),
    }


def _universe_members(symbols: list[str]) -> list[dict[str, Any]]:
    from quantmaster.data.instruments import InstrumentStore

    store = InstrumentStore()
    result = []
    for symbol in symbols:
        instrument = store.get(symbol)
        result.append({
            "symbol": symbol, "name": instrument.name if instrument else None,
            "market": instrument.market if instrument else None,
            "exchange": instrument.exchange if instrument else None,
            "asset_type": instrument.asset_type if instrument else None,
            "status": instrument.status if instrument else None,
            "source": instrument.source if instrument else None,
        })
    return result


@router.get("/market/instruments/search")
def instrument_search(
    request: Request, q: str = "", limit: int = 20, online: bool = True,
) -> dict:
    _require_local(request)
    from quantmaster.data.instruments import search_instruments

    return {"query": q, "items": search_instruments(q, limit=limit, online=online)}


@router.post("/market/instruments/resolve")
def instrument_resolve(request: Request, value: InstrumentResolveBody) -> dict:
    _require_csrf(request)
    from quantmaster.data.instruments import resolve_instruments

    return resolve_instruments(value.queries, selections=value.selections)


def _rewrite_universe_references(old_name: str, new_name: str) -> tuple[list[str], dict | None]:
    document = document_from_config(settings_manager.load())
    changed: list[str] = []
    if document.automation.primary_universe.casefold() == old_name.casefold():
        document.automation.primary_universe = new_name
        changed.append("automation.primary_universe")
    if document.lab.universe.casefold() == old_name.casefold():
        document.lab.universe = new_name
        changed.append("lab.universe")
    if not changed:
        return [], None
    update = SettingsUpdate.model_validate(document.model_dump())
    return changed, _apply_runtime(settings_manager.save(update))


def _validate_replacement(name: str, references: list[dict[str, str]]) -> str:
    value = str(name).strip()
    if not value:
        raise ValueError("请选择替代候选")
    if value.casefold() == "csi800":
        if any(item["key"] == "automation.primary_universe" for item in references):
            raise ValueError("csi800 只适用于 Quant Lab，不能替代自动化主候选")
        return "csi800"
    from quantmaster.data.universe import load_universe

    load_universe(value)
    return value


@router.get("/settings/universes")
def universes(request: Request) -> dict:
    _require_local(request)
    from quantmaster.data.universe import INDEX_UNIVERSE_PRESETS, list_universes

    fixed = [_fixed_universe_metadata(item) for item in list_universes()]
    dynamic = {
        "name": "csi800", "count": None, "readonly": True, "kind": "dynamic",
        "source": "tushare:index_weight", "research_quality": "production",
        "references": _universe_references("csi800"),
    }
    data_root = Path(settings_manager.load().data.root).expanduser().resolve()
    conflicts = []
    conflict = data_root / "universe" / "csi800.json"
    if conflict.is_file():
        conflicts.append({
            "name": "csi800", "path": str(conflict),
            "message": "检测到与系统动态候选同名的旧文件；文件已保留，请先改名再使用。",
        })
    ordered = ([fixed[0], dynamic, *fixed[1:]] if fixed else [dynamic])
    return {
        "universes": ordered,
        "index_presets": [dict(item) for item in INDEX_UNIVERSE_PRESETS],
        "conflicts": conflicts,
    }


@router.get("/settings/universes/{name}")
def universe_detail(name: str, request: Request, as_of: date | None = None) -> dict:
    _require_local(request)
    from quantmaster.data.universe import load_universe

    try:
        if name.casefold() == "csi800":
            chosen = as_of or date.today()
            if chosen > date.today():
                raise ValueError("查看日期不能晚于今天")
            from quantmaster.lab.dataset import load_csi800_members_as_of

            dynamic = load_csi800_members_as_of(chosen.isoformat())
            symbols = dynamic["symbols"]
            return {
                "name": "csi800", "symbols": symbols,
                "members": _universe_members(symbols), "count": len(symbols),
                "readonly": True, "kind": "dynamic", "source": "tushare:index_weight",
                "research_quality": "production", "as_of": dynamic["as_of"],
                "snapshot_dates": dynamic["snapshot_dates"],
                "references": _universe_references("csi800"),
            }
        symbols = load_universe(name)
        built_in = name.casefold() == "demo"
        return {
            "name": "demo" if built_in else name, "symbols": symbols,
            "members": _universe_members(symbols), "count": len(symbols),
            "readonly": built_in, "kind": "fixed",
            "source": "built_in" if built_in else "custom",
            "research_quality": "sandbox", "references": _universe_references(name),
        }
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from None
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(400, str(exc)) from None


@router.post("/settings/universes/preview")
def preview_universe(request: Request, value: UniversePreview) -> dict:
    _require_csrf(request)
    from quantmaster.data.instruments import resolve_instruments
    from quantmaster.data.universe import index_universe

    try:
        symbols = index_universe(value.index_symbol) if value.kind == "index" else value.symbols
        resolution = resolve_instruments(symbols, selections=value.selections)
        normalized = [item["instrument"]["symbol"] for item in resolution["resolved"]]
        errors = [
            {"value": item["query"], "message": item["message"]}
            for item in resolution["unresolved"]
        ]
        return {
            "symbols": normalized, "members": _universe_members(normalized),
            "count": len(normalized), "preview": normalized[:100],
            "duplicates": resolution["duplicates"], "errors": errors,
            "ambiguous": resolution["ambiguous"],
            "unresolved": resolution["unresolved"],
            "corrections": resolution["corrections"],
        }
    except Exception as exc:
        raise HTTPException(400, str(exc)) from None


@router.post("/settings/universes/names/refresh")
def refresh_universe_names(request: Request, value: UniverseNameRefresh) -> dict:
    _require_csrf(request)
    from quantmaster.data.names import load_stock_names
    from quantmaster.data.universe import normalize_symbols

    try:
        symbols = normalize_symbols(value.symbols)
        names = load_stock_names(symbols, refresh=True)
        return {
            "names": names,
            "missing": [symbol for symbol in symbols if symbol not in names],
        }
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None


@router.post("/settings/instruments/refresh")
def refresh_instruments(request: Request) -> dict:
    """Refresh external security catalogs only after an explicit local request."""
    _require_csrf(request)
    from quantmaster.data.instruments import refresh_instrument_master

    return refresh_instrument_master(force=True)


@router.post("/settings/universes")
def create_universe(request: Request, value: UniverseBody) -> dict:
    _require_csrf(request)
    if not value.name:
        raise HTTPException(400, "缺少候选名称")
    from quantmaster.data.universe import load_universe, save_universe

    try:
        try:
            load_universe(value.name)
        except FileNotFoundError:
            pass
        else:
            raise ValueError("候选已存在")
        _validate_universe_instruments(value.symbols)
        save_universe(value.name, value.symbols)
        return {"status": "ok", "name": value.name}
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None


@router.put("/settings/universes/{name}")
def update_universe(name: str, request: Request, value: UniverseBody) -> dict:
    _require_csrf(request)
    from quantmaster.data.universe import load_universe, save_universe

    try:
        load_universe(name)
        _validate_universe_instruments(value.symbols)
        save_universe(name, value.symbols)
        return {"status": "ok", "name": name}
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from None
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None


def _validate_universe_instruments(symbols: list[str]) -> None:
    """拒绝未知、歧义和没有可验证日线能力的标的。"""
    from quantmaster.data.instruments import resolve_instruments, validate_bar_capability

    resolution = resolve_instruments(symbols)
    if resolution["ambiguous"] or resolution["unresolved"]:
        detail = {
            "message": "候选包含尚未确认的证券",
            "ambiguous": resolution["ambiguous"],
            "unresolved": resolution["unresolved"],
        }
        raise HTTPException(422, detail)
    for item in resolution["resolved"]:
        try:
            validate_bar_capability(item["instrument"]["symbol"], verify_foreign=True)
        except ValueError as exc:
            raise HTTPException(422, {
                "message": str(exc), "symbol": item["instrument"]["symbol"],
            }) from None


@router.post("/settings/universes/{name}/rename")
def rename_universe_route(name: str, request: Request, value: UniverseRename) -> dict:
    _require_csrf(request)
    from quantmaster.data.universe import rename_universe

    renamed = False
    try:
        rename_universe(name, value.new_name)
        renamed = True
        changed, runtime = _rewrite_universe_references(name, value.new_name)
        return {
            "status": "ok", "name": value.new_name,
            "updated_references": changed, "runtime": runtime.get("runtime") if runtime else None,
        }
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from None
    except (ValueError, FileExistsError) as exc:
        if renamed:
            try:
                rename_universe(value.new_name, name)
            except Exception:
                pass
        raise HTTPException(400, str(exc)) from None


@router.delete("/settings/universes/{name}")
def delete_universe_route(name: str, request: Request, replacement: str | None = None) -> dict:
    _require_csrf(request)
    from quantmaster.data.universe import delete_universe

    try:
        if name.casefold() in {"demo", "csi800"}:
            raise ValueError("系统候选只读，请复制后再编辑")
        references = _universe_references(name)
        changed: list[str] = []
        runtime = None
        replacement_name = None
        if references:
            if not replacement:
                raise HTTPException(409, detail={
                    "message": "该候选正在使用中，请先选择替代候选。",
                    "references": references, "requires_replacement": True,
                })
            replacement_name = _validate_replacement(replacement, references)
            if replacement_name.casefold() == name.casefold():
                raise ValueError("替代候选不能与待删除候选相同")
            changed, runtime = _rewrite_universe_references(name, replacement_name)
        delete_universe(name)
        return {
            "status": "ok", "replacement": replacement_name,
            "updated_references": changed, "runtime": runtime.get("runtime") if runtime else None,
        }
    except HTTPException:
        raise
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from None
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None


async def _upload_bytes(file: UploadFile) -> bytes:
    content = await file.read(20 * 1024 * 1024 + 1)
    if len(content) > 20 * 1024 * 1024:
        raise HTTPException(413, "CSV 文件超过 20MB 限制")
    return content


@router.post("/portfolio/ledger/import/preview")
async def preview_ledger_csv(
    request: Request,
    file: Annotated[UploadFile, File()],
    mapping: Annotated[str | None, Form()] = None,
) -> dict:
    _require_csrf(request)
    from quantmaster.portfolio import Ledger
    from quantmaster.portfolio.csv_import import parse_broker_csv

    content = await _upload_bytes(file)
    ledger = Ledger()
    try:
        parsed = parse_broker_csv(content, mapping, ledger.fingerprints())
        return parsed.preview(batch_duplicate=ledger.has_import_hash(parsed.file_hash))
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None


@router.post("/portfolio/ledger/import/submit")
async def submit_ledger_csv(
    request: Request,
    file: Annotated[UploadFile, File()],
    mapping: Annotated[str | None, Form()] = None,
    strict: Annotated[bool, Form()] = True,
    include_duplicates: Annotated[bool, Form()] = False,
) -> dict:
    _require_csrf(request)
    from quantmaster.portfolio import Ledger
    from quantmaster.portfolio.csv_import import parse_broker_csv

    content = await _upload_bytes(file)
    ledger = Ledger()
    try:
        parsed = parse_broker_csv(content, mapping, ledger.fingerprints())
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None
    failed = [row.public() for row in parsed.rows if row.errors]
    if strict and failed:
        raise HTTPException(422, {"message": f"严格模式：{len(failed)} 行校验失败",
                                  "failed_rows": failed})
    if ledger.has_import_hash(parsed.file_hash) and not include_duplicates:
        raise HTTPException(409, "该文件已导入；如确需再次导入，请显式包含重复记录")
    rows = [row for row in parsed.valid_rows if include_duplicates or not row.duplicate]
    records = [row.record for row in rows if row.record]
    try:
        count = ledger.import_records(records, parsed.file_hash, file.filename or "trades.csv",
                                      parsed.encoding)
    except Exception:
        # 不返回数据库内部信息，也不把任何原始行写进日志。
        raise HTTPException(500, "数据库写入失败，全部记录已回滚") from None
    return {"status": "ok", "imported": count,
            "skipped_invalid": len(failed) if not strict else 0,
            "skipped_duplicates": len(parsed.valid_rows) - len(rows),
            "failed_rows": failed}
