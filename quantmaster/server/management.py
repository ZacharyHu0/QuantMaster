"""本机设置、股票池、迁移与券商 CSV 导入 API。"""

from __future__ import annotations

import hmac
import secrets
import time
from typing import Annotated, Any, Literal
from urllib.parse import urlparse

from fastapi import APIRouter, Body, File, Form, HTTPException, Request, Response, UploadFile
from pydantic import BaseModel, Field

from quantmaster.config import get_config
from quantmaster.credentials import CredentialError
from quantmaster.data.migration import MigrationError, migration_manager
from quantmaster.settings import (
    SecretMutations,
    SettingsDocument,
    SettingsUpdate,
)

router = APIRouter(prefix="/api")
settings_manager = migration_manager.config_manager
_csrf_tokens: dict[str, float] = {}
_CSRF_TTL = 8 * 60 * 60


def _local(request: Request) -> bool:
    host = request.client.host if request.client else ""
    return host in {"127.0.0.1", "::1", "localhost", "testclient"}


def _require_local(request: Request) -> None:
    if not _local(request):
        raise HTTPException(403, "设置中心仅允许从本机访问")


def _issue_csrf() -> str:
    now = time.time()
    for token, expires in list(_csrf_tokens.items()):
        if expires < now:
            _csrf_tokens.pop(token, None)
    token = secrets.token_urlsafe(32)
    _csrf_tokens[token] = now + _CSRF_TTL
    return token


def _require_csrf(request: Request) -> None:
    _require_local(request)
    header = request.headers.get("x-csrf-token", "")
    cookie = request.cookies.get("qm_csrf", "")
    if not header or not cookie or not hmac.compare_digest(header, cookie):
        raise HTTPException(403, "CSRF 令牌缺失或无效；请刷新设置页")
    if _csrf_tokens.get(header, 0) < time.time():
        raise HTTPException(403, "CSRF 令牌已过期；请刷新设置页")
    origin = request.headers.get("origin")
    if origin and urlparse(origin).netloc.lower() != request.headers.get("host", "").lower():
        raise HTTPException(403, "拒绝跨来源设置请求")


@router.get("/settings")
def get_settings(request: Request, response: Response) -> dict:
    _require_local(request)
    token = _issue_csrf()
    response.set_cookie("qm_csrf", token, httponly=False, samesite="strict",
                        secure=request.url.scheme == "https", max_age=_CSRF_TTL, path="/")
    return {**settings_manager.public(), "csrf_token": token, "remote_management": False}


@router.post("/settings/validate")
def validate_settings(request: Request, document: SettingsDocument) -> dict:
    _require_csrf(request)
    return settings_manager.validate(document)


@router.put("/settings")
def save_settings(request: Request, update: SettingsUpdate) -> dict:
    _require_csrf(request)
    try:
        result = settings_manager.save(update)
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
def check_setting(kind: Literal["llm-models", "tushare", "storage", "data-sources", "server"],
                  request: Request,
                  body: Annotated[dict[str, Any] | None, Body()] = None) -> dict:
    _require_csrf(request)
    from quantmaster.settings_checks import (
        check_data_sources,
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
    return check_server(document.server)


class SnapshotCreate(BaseModel):
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
        return settings_manager.rollback(snapshot_id)
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


class MigrationCreate(BaseModel):
    target: str = Field(min_length=1, max_length=4096)
    mode: Literal["copy", "switch"] = "copy"


@router.post("/settings/migration")
def create_migration(request: Request, value: MigrationCreate) -> dict:
    _require_csrf(request)
    try:
        return migration_manager.create(value.target, value.mode)
    except MigrationError as exc:
        raise HTTPException(400, str(exc)) from None


@router.get("/settings/migration/{task_id}")
def get_migration(task_id: str, request: Request) -> dict:
    _require_local(request)
    try:
        return migration_manager.get(task_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from None


@router.post("/settings/migration/{task_id}/cancel")
def cancel_migration(task_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        return migration_manager.cancel(task_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from None


class UniverseBody(BaseModel):
    name: str | None = None
    symbols: list[str] = Field(default_factory=list, max_length=10_000)


class UniverseRename(BaseModel):
    new_name: str


class UniversePreview(BaseModel):
    kind: Literal["manual", "index"] = "manual"
    symbols: list[str] = Field(default_factory=list, max_length=10_000)
    index_symbol: str = "000300.SH"


@router.get("/settings/universes")
def universes(request: Request) -> dict:
    _require_local(request)
    from quantmaster.data.universe import list_universes

    return {"universes": list_universes()}


@router.get("/settings/universes/{name}")
def universe_detail(name: str, request: Request) -> dict:
    _require_local(request)
    from quantmaster.data.universe import load_universe

    try:
        symbols = load_universe(name)
        return {"name": name, "symbols": symbols, "readonly": name == "demo"}
    except (ValueError, FileNotFoundError) as exc:
        raise HTTPException(404, str(exc)) from None


@router.post("/settings/universes/preview")
def preview_universe(request: Request, value: UniversePreview) -> dict:
    _require_csrf(request)
    from quantmaster.data.universe import index_universe, normalize_symbols

    try:
        symbols = index_universe(value.index_symbol) if value.kind == "index" else value.symbols
        normalized = normalize_symbols(symbols)
        return {"symbols": normalized, "count": len(normalized), "preview": normalized[:100]}
    except Exception as exc:
        raise HTTPException(400, str(exc)) from None


@router.post("/settings/universes")
def create_universe(request: Request, value: UniverseBody) -> dict:
    _require_csrf(request)
    if not value.name:
        raise HTTPException(400, "缺少股票池名称")
    from quantmaster.data.universe import load_universe, save_universe

    try:
        try:
            load_universe(value.name)
        except FileNotFoundError:
            pass
        else:
            raise ValueError("股票池已存在")
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
        save_universe(name, value.symbols)
        return {"status": "ok", "name": name}
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from None
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None


@router.post("/settings/universes/{name}/rename")
def rename_universe_route(name: str, request: Request, value: UniverseRename) -> dict:
    _require_csrf(request)
    from quantmaster.data.universe import rename_universe

    try:
        rename_universe(name, value.new_name)
        return {"status": "ok", "name": value.new_name}
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from None
    except (ValueError, FileExistsError) as exc:
        raise HTTPException(400, str(exc)) from None


@router.delete("/settings/universes/{name}")
def delete_universe_route(name: str, request: Request) -> dict:
    _require_csrf(request)
    from quantmaster.data.universe import delete_universe

    try:
        delete_universe(name)
        return {"status": "ok"}
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from None
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None


async def _upload_bytes(file: UploadFile) -> bytes:
    content = await file.read(20 * 1024 * 1024 + 1)
    if len(content) > 20 * 1024 * 1024:
        raise HTTPException(413, "CSV 文件超过 20MB 限制")
    return content


@router.post("/ledger/import/preview")
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


@router.post("/ledger/import/submit")
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
