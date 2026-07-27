"""GUI 设置的校验、原子保存、凭据状态与非敏感快照。"""

from __future__ import annotations

import copy
import hashlib
import ipaddress
import json
import os
import re
import tempfile
import threading
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from quantmaster.config import DEFAULT_CONFIG_PATHS, Config, load_config, set_config
from quantmaster.credentials import CredentialError, CredentialStore

CONFIG_VERSION = 1
AUTO_SNAPSHOT_LIMIT = 20


def normalize_api_base(provider: str, value: str) -> str:
    """规范 API 根地址，并兼容旧配置里粘贴的完整 endpoint。"""
    value = value.strip().rstrip("/")
    if not value:
        return ""
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("API 地址必须是完整的 http(s) URL")
    suffixes = ("/chat/completions", "/responses", "/messages", "/models")
    lowered = value.lower()
    for suffix in suffixes:
        if lowered.endswith(suffix):
            value = value[:-len(suffix)].rstrip("/")
            break
    # 常见 OpenAI-compatible 地址没有 /v1 时也允许；不能擅自补齐网关路径。
    return value


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class LLMSettings(StrictModel):
    provider: Literal["anthropic", "openai", "openai-compatible"] = "anthropic"
    model: str = Field(default="claude-sonnet-5", min_length=1, max_length=200)
    base_url: str = Field(default="", max_length=2048)
    max_tokens: int = Field(default=2048, ge=1, le=1_000_000)
    temperature: float = Field(default=0.3, ge=0.0, le=2.0)
    timeout: float = Field(default=60.0, gt=0.0, le=600.0)

    @model_validator(mode="after")
    def validate_endpoint(self):
        self.base_url = normalize_api_base(self.provider, self.base_url)
        if self.provider == "openai-compatible" and not self.base_url:
            raise ValueError("OpenAI-compatible 必须填写 API 根地址")
        return self


class DataSettings(StrictModel):
    root: str = Field(default="data", min_length=1, max_length=4096)
    cache_days: int = Field(default=1, ge=0, le=3650)
    intraday_cache_minutes: int = Field(default=5, ge=0, le=1440)
    akshare_retries: int = Field(default=3, ge=1, le=20)
    akshare_retry_backoff: float = Field(default=0.8, ge=0.0, le=60.0)
    tushare_calls_per_minute: int = Field(default=120, ge=1, le=10_000)
    tushare_cache_days: int = Field(default=1, ge=0, le=3650)
    fundamental_cache_days: int = Field(default=7, ge=0, le=3650)

    @field_validator("root")
    @classmethod
    def validate_root(cls, value: str) -> str:
        if "\x00" in value:
            raise ValueError("数据目录包含非法字符")
        return str(Path(value).expanduser())


class TradeSettings(StrictModel):
    commission_rate: float = Field(default=2.5e-4, ge=0.0, le=0.1)
    commission_min: float = Field(default=5.0, ge=0.0, le=10_000)
    stamp_tax_rate: float = Field(default=5e-4, ge=0.0, le=0.1)
    transfer_fee_rate: float = Field(default=1e-5, ge=0.0, le=0.1)
    slippage: float = Field(default=1e-3, ge=0.0, le=0.5)
    lot_size: int = Field(default=100, ge=1, le=100_000)


class ServerSettings(StrictModel):
    host: str = Field(default="127.0.0.1", min_length=1, max_length=253)
    port: int = Field(default=8686, ge=1, le=65535)

    @field_validator("host")
    @classmethod
    def validate_host(cls, value: str) -> str:
        if value in {"localhost", "0.0.0.0", "::"}:
            return value
        try:
            ipaddress.ip_address(value)
            return value
        except ValueError:
            hostname = (
                r"(?=.{1,253}$)([A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)*"
                r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
            )
            if not re.fullmatch(hostname, value):
                raise ValueError("host 不是合法的 IP 地址或主机名") from None
            return value.lower()


class NewsSettings(StrictModel):
    raw_cache_days: int = Field(default=7, ge=0, le=3650)
    annotation_enabled: bool = True
    annotation_batch_size: int = Field(default=10, ge=1, le=50)
    annotation_items_per_run: int = Field(default=100, ge=1, le=1000)
    factor_halflife_days: float = Field(default=3.0, gt=0, le=30)
    factor_min_confidence: float = Field(default=0.35, ge=0, le=1)


class AutomationSettings(StrictModel):
    enabled: bool = False
    timezone: str = Field(default="Asia/Shanghai", min_length=1, max_length=100)
    primary_universe: str = Field(default="demo", min_length=1, max_length=40)
    watchlist: list[str] = Field(default_factory=list, max_length=10_000)
    sentinel_indices: list[str] = Field(default_factory=lambda: [
        "000300.SH", "000905.SH", "000852.SH", "399006.SZ",
    ], min_length=1, max_length=12)
    weixin_api_base: str = Field(default="https://ilinkai.weixin.qq.com", max_length=2048)
    feishu_app_id: str = Field(default="", max_length=200)
    retention_days: int = Field(default=90, ge=7, le=3650)

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ValueError, ZoneInfoNotFoundError):
            raise ValueError("时区不是有效的 IANA 时区，例如 Asia/Shanghai") from None
        return value

    @field_validator("primary_universe")
    @classmethod
    def validate_primary_universe(cls, value: str) -> str:
        from quantmaster.data.universe import validate_universe_name

        normalized = validate_universe_name(value, allow_demo=True)
        return "demo" if normalized.lower() == "demo" else normalized

    @field_validator("watchlist", "sentinel_indices")
    @classmethod
    def normalize_automation_symbols(cls, value: list[str], info) -> list[str]:
        from quantmaster.data.universe import normalize_symbols

        if not value and info.field_name == "watchlist":
            return []
        return normalize_symbols(value)

    @field_validator("weixin_api_base")
    @classmethod
    def validate_weixin_api_base(cls, value: str) -> str:
        return normalize_api_base("openai-compatible", value)


class LabSettings(StrictModel):
    enabled: bool = True
    universe: str = Field(default="csi800", min_length=1, max_length=40)
    start: str = Field(default="2015-01-01", pattern=r"^\d{4}-\d{2}-\d{2}$")
    horizons: list[Literal[1, 3, 5, 7]] = Field(
        default_factory=lambda: [1, 3, 5, 7], min_length=1, max_length=4)
    weekly_days: list[int] = Field(
        default_factory=lambda: [1, 3, 5], min_length=1, max_length=7)
    window_start: str = Field(default="19:00", pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d$")
    window_end: str = Field(default="07:00", pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d$")
    daily_budget_hours: float = Field(default=10.0, gt=0, le=24)
    device: Literal["auto", "cpu", "cuda", "mps"] = "auto"
    allow_cloud_sample: bool = False

    @field_validator("universe")
    @classmethod
    def validate_universe(cls, value: str) -> str:
        if value.lower() == "csi800":
            return "csi800"
        from quantmaster.data.universe import validate_universe_name

        return validate_universe_name(value, allow_demo=True)

    @field_validator("start")
    @classmethod
    def validate_start(cls, value: str) -> str:
        try:
            parsed = date.fromisoformat(value)
        except ValueError:
            raise ValueError("研究起点必须是有效的 YYYY-MM-DD 日期") from None
        if parsed > date.today():
            raise ValueError("研究起点不能晚于今天")
        return parsed.isoformat()

    @field_validator("horizons")
    @classmethod
    def normalize_horizons(cls, value: list[int]) -> list[int]:
        return [item for item in (1, 3, 5, 7) if item in set(value)]

    @field_validator("weekly_days")
    @classmethod
    def validate_weekly_days(cls, value: list[int]) -> list[int]:
        if any(day < 1 or day > 7 for day in value):
            raise ValueError("weekly_days 使用 ISO 星期编号 1–7")
        return sorted(set(value))

    @model_validator(mode="after")
    def validate_window(self):
        if self.window_start == self.window_end:
            raise ValueError("研究窗口的开始和结束时间不能相同")
        return self


class SecretMutation(StrictModel):
    action: Literal["keep", "replace", "clear"] = "keep"
    value: str | None = Field(default=None, max_length=16_384)

    @model_validator(mode="after")
    def validate_action_value(self):
        if self.action == "replace" and not (self.value or "").strip():
            raise ValueError("替换凭据时必须提供新值")
        if self.action != "replace" and self.value:
            raise ValueError("仅 replace 操作可以携带凭据值")
        return self


class SecretMutations(StrictModel):
    llm: SecretMutation = Field(default_factory=SecretMutation)
    tushare: SecretMutation = Field(default_factory=SecretMutation)


class SettingsDocument(StrictModel):
    config_version: int = CONFIG_VERSION
    llm: LLMSettings = Field(default_factory=LLMSettings)
    data: DataSettings = Field(default_factory=DataSettings)
    trade: TradeSettings = Field(default_factory=TradeSettings)
    news: NewsSettings = Field(default_factory=NewsSettings)
    server: ServerSettings = Field(default_factory=ServerSettings)
    automation: AutomationSettings = Field(default_factory=AutomationSettings)
    lab: LabSettings = Field(default_factory=LabSettings)

    @field_validator("config_version")
    @classmethod
    def supported_version(cls, value: int) -> int:
        if value != CONFIG_VERSION:
            raise ValueError(f"不支持的配置版本: {value}")
        return value


class SettingsUpdate(SettingsDocument):
    secrets: SecretMutations = Field(default_factory=SecretMutations)
    allow_plaintext_secrets: bool = False


def document_from_config(cfg: Config) -> SettingsDocument:
    return SettingsDocument.model_validate({
        "config_version": CONFIG_VERSION,
        "llm": {k: getattr(cfg.llm, k) for k in LLMSettings.model_fields},
        "data": {k: getattr(cfg.data, k) for k in DataSettings.model_fields},
        "trade": {k: getattr(cfg.trade, k) for k in TradeSettings.model_fields},
        "news": {k: getattr(cfg.news, k) for k in NewsSettings.model_fields},
        "server": {k: getattr(cfg.server, k) for k in ServerSettings.model_fields},
        "automation": {k: getattr(cfg.automation, k) for k in AutomationSettings.model_fields},
        "lab": {k: getattr(cfg.lab, k) for k in LabSettings.model_fields},
    })


def _resolve_config_path(path: str | Path | None) -> Path:
    if path is not None:
        return Path(path).expanduser().resolve()
    for candidate in DEFAULT_CONFIG_PATHS:
        if candidate.is_file():
            return candidate.resolve()
    return DEFAULT_CONFIG_PATHS[0].resolve()


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    value = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(value, dict):
        raise ValueError("配置文件根节点必须是映射")
    return value


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def _sanitize(raw: dict[str, Any]) -> dict[str, Any]:
    safe = copy.deepcopy(raw)
    if isinstance(safe.get("llm"), dict):
        safe["llm"].pop("api_key", None)
    if isinstance(safe.get("data"), dict):
        safe["data"].pop("tushare_token", None)
    safe.pop("_secrets", None)
    safe.pop("secrets", None)
    safe.pop("allow_plaintext_secrets", None)
    return safe


def _hash_config(raw: dict[str, Any]) -> str:
    encoded = json.dumps(_sanitize(raw), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _flatten(value: Any, prefix: str = "") -> dict[str, Any]:
    if not isinstance(value, dict):
        return {prefix: value}
    out: dict[str, Any] = {}
    for key, child in value.items():
        name = f"{prefix}.{key}" if prefix else str(key)
        out.update(_flatten(child, name))
    return out


class ConfigManager:
    """版本化设置管理器；实例可在测试中注入路径和凭据后端。"""

    def __init__(
        self,
        path: str | Path | None = None,
        backup_dir: str | Path | None = None,
        credential_store: CredentialStore | None = None,
    ):
        self.path = _resolve_config_path(path)
        self.backup_dir = (Path(backup_dir).expanduser().resolve() if backup_dir else
                           (Path.home() / ".quantmaster" / "backups").resolve())
        self.credentials = credential_store or CredentialStore()
        self._lock = threading.RLock()

    def load(self) -> Config:
        cfg = load_config(self.path)
        raw = _read_yaml(self.path)
        metadata = raw.get("_secrets") or {}
        pairs = (
            ("llm", cfg.llm, "api_key",
             CredentialStore.llm_target(cfg.llm.provider, cfg.llm.base_url)),
            ("tushare", cfg.data, "tushare_token", CredentialStore.tushare_target()),
        )
        for name, owner, attr, default_target in pairs:
            item = metadata.get(name) or {}
            if item.get("state") == "keyring":
                try:
                    setattr(owner, attr, self.credentials.get(item.get("target") or default_target) or "")
                except CredentialError:
                    setattr(owner, attr, "")
        return cfg

    def public(self) -> dict[str, Any]:
        raw = _read_yaml(self.path)
        cfg = self.load()
        doc = document_from_config(cfg).model_dump()
        meta = raw.get("_secrets") or {}
        doc.update({
            "managed_by_gui": bool(raw.get("managed_by_gui")),
            "config_path": str(self.path),
            "config_revision": _hash_config(raw or doc),
            "secrets": {
                "llm": self._secret_public("llm", cfg.llm.api_key, meta),
                "tushare": self._secret_public("tushare", cfg.data.tushare_token, meta),
            },
        })
        return doc

    @staticmethod
    def _secret_public(name: str, runtime_value: str, metadata: dict) -> dict[str, Any]:
        item = metadata.get(name) or {}
        state = item.get("state") or ("environment-or-yaml" if runtime_value else "unset")
        return {"configured": bool(runtime_value), "state": state}

    def validate(self, value: SettingsDocument | dict[str, Any]) -> dict[str, Any]:
        doc = value if isinstance(value, SettingsDocument) else SettingsDocument.model_validate(value)
        warnings: list[str] = []
        if doc.llm.provider == "openai-compatible" and not doc.llm.model:
            warnings.append("兼容网关未指定模型；保存后仍需手动填写模型 ID")
        root = Path(doc.data.root).expanduser()
        if not root.is_absolute():
            warnings.append(f"数据目录将相对于启动目录解析：{root}")
        if doc.server.host not in {"127.0.0.1", "localhost", "::1"}:
            warnings.append("服务监听非本机地址时，远程设置入口会保持禁用")
        for label, universe in (("自动化主候选", doc.automation.primary_universe),
                                ("Quant Lab 默认候选", doc.lab.universe)):
            if universe.lower() in {"demo", "csi800"}:
                continue
            from quantmaster.data.universe import normalize_symbols

            universe_path = Path(doc.data.root).expanduser().resolve() / "universe" / f"{universe}.json"
            try:
                raw_symbols = json.loads(universe_path.read_text(encoding="utf-8"))
                symbols = normalize_symbols([str(item) for item in raw_symbols])
            except FileNotFoundError:
                raise ValueError(f"{label}不存在：{universe}") from None
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                raise ValueError(f"{label}文件无效：{universe}") from None
            if not symbols:
                raise ValueError(f"{label}为空：{universe}")
        if doc.lab.device != "auto":
            import importlib.util

            if importlib.util.find_spec("torch") is None and doc.lab.device in {"cuda", "mps"}:
                warnings.append(f"当前未安装 PyTorch；{doc.lab.device} 将在安装后用于新训练任务")
        start_minutes = int(doc.lab.window_start[:2]) * 60 + int(doc.lab.window_start[3:])
        end_minutes = int(doc.lab.window_end[:2]) * 60 + int(doc.lab.window_end[3:])
        window_hours = ((end_minutes - start_minutes) % (24 * 60)) / 60
        if doc.lab.daily_budget_hours > window_hours:
            warnings.append("Quant Lab 每日预算大于自动研究窗口；实际运行仍受窗口限制")
        return {"valid": True, "normalized": doc.model_dump(), "warnings": warnings}

    def save(self, update: SettingsUpdate | dict[str, Any], *, allow_root_change: bool = False) -> dict:
        # API 通常已经构造了模型，但测试、插件或桌面调用方仍可能在构造后修改字段。
        # 保存边界必须重新校验并采用规范化后的值，不能信任可变模型实例。
        source = update.model_dump() if isinstance(update, SettingsUpdate) else update
        value = SettingsUpdate.model_validate(source)
        with self._lock:
            current_raw = _read_yaml(self.path)
            current_cfg = self.load()
            current_doc = document_from_config(current_cfg)
            if (not allow_root_change and
                    Path(value.data.root).expanduser().resolve() !=
                    Path(current_doc.data.root).expanduser().resolve()):
                raise ValueError("数据根目录不能直接保存，请使用数据迁移的‘复制并切换’或‘仅切换’")

            warnings = self.validate(SettingsDocument.model_validate(
                value.model_dump(exclude={"secrets", "allow_plaintext_secrets"})))['warnings']
            payload = value.model_dump(exclude={"secrets", "allow_plaintext_secrets"})
            payload["managed_by_gui"] = True
            payload["_secrets"] = {}

            old_llm_target = CredentialStore.llm_target(
                current_cfg.llm.provider, current_cfg.llm.base_url)
            new_llm_target = CredentialStore.llm_target(value.llm.provider, value.llm.base_url)
            self._apply_secret(
                name="llm", mutation=value.secrets.llm, current_raw=current_raw,
                current_value=current_cfg.llm.api_key, old_target=old_llm_target,
                new_target=new_llm_target, payload=payload,
                section="llm", field="api_key", allow_plaintext=value.allow_plaintext_secrets,
                warnings=warnings,
            )
            tushare_target = CredentialStore.tushare_target()
            self._apply_secret(
                name="tushare", mutation=value.secrets.tushare, current_raw=current_raw,
                current_value=current_cfg.data.tushare_token, old_target=tushare_target,
                new_target=tushare_target, payload=payload,
                section="data", field="tushare_token", allow_plaintext=value.allow_plaintext_secrets,
                warnings=warnings,
            )

            if not current_raw.get("managed_by_gui"):
                self._create_snapshot(current_raw or document_from_config(current_cfg).model_dump(),
                                      kind="initial", name="首次 GUI 保存前")
            _atomic_write(self.path, yaml.safe_dump(payload, allow_unicode=True, sort_keys=False))
            set_config(self.load())
            snapshot = self._create_snapshot(payload, kind="automatic")
            old_flat = _flatten(current_doc.model_dump())
            new_flat = _flatten(value.model_dump(exclude={"secrets", "allow_plaintext_secrets"}))
            changed = sorted(
                key for key in set(old_flat) | set(new_flat) if old_flat.get(key) != new_flat.get(key)
            )
            restart = [f"server.{name}" for name in ("host", "port")
                       if getattr(current_doc.server, name) != getattr(value.server, name)]
            return {"status": "ok", "warnings": warnings,
                    "changed_fields": changed, "config_revision": _hash_config(payload),
                    "restart_required": restart, "snapshot_id": snapshot["id"]}

    def _apply_secret(
        self, *, name: str, mutation: SecretMutation, current_raw: dict[str, Any],
        current_value: str, old_target: str, new_target: str, payload: dict[str, Any],
        section: str, field: str, allow_plaintext: bool, warnings: list[str],
    ) -> None:
        old_meta = (current_raw.get("_secrets") or {}).get(name) or {}
        if mutation.action == "clear":
            for target in {old_meta.get("target"), old_target, new_target} - {None}:
                try:
                    self.credentials.delete(str(target))
                except CredentialError:
                    pass
            payload["_secrets"][name] = {"state": "cleared", "target": new_target}
            payload[section].pop(field, None)
            return

        if mutation.action == "keep" and old_target != new_target:
            payload["_secrets"][name] = {"state": "cleared", "target": new_target}
            payload[section].pop(field, None)
            warnings.append("API 提供商或地址已改变；为避免跨服务发送旧密钥，凭据已保持为空")
            return

        secret = (mutation.value or "").strip() if mutation.action == "replace" else current_value
        if not secret:
            state = old_meta.get("state") if mutation.action == "keep" else "cleared"
            payload["_secrets"][name] = {"state": state or "cleared", "target": new_target}
            payload[section].pop(field, None)
            return

        # 已经在同一目标的 keyring 中时，keep 不需要再次写入。
        if (mutation.action == "keep" and old_meta.get("state") == "keyring" and
                old_meta.get("target", new_target) == new_target):
            payload["_secrets"][name] = {"state": "keyring", "target": new_target}
            payload[section].pop(field, None)
            return
        try:
            self.credentials.set(new_target, secret)
            payload["_secrets"][name] = {"state": "keyring", "target": new_target}
            payload[section].pop(field, None)
        except CredentialError as exc:
            if not allow_plaintext:
                raise CredentialError(f"{exc}；如仍要保存，请明确确认明文 YAML 风险") from exc
            payload["_secrets"][name] = {"state": "plaintext", "target": new_target}
            payload[section][field] = secret
            warnings.append(f"{name} 凭据库不可用，已按你的确认写入明文 YAML")

    def update_data_root(self, target: str | Path) -> dict:
        """迁移完成后的受控切换；原样保留当前凭据元数据与明文状态。"""
        with self._lock:
            current = _read_yaml(self.path)
            if current:
                # 仍以当前 schema 验证非敏感字段，避免迁移把损坏配置写回。
                document_from_config(self.load())
                payload = copy.deepcopy(current)
            else:
                payload = document_from_config(self.load()).model_dump()
            payload.setdefault("data", {})["root"] = str(Path(target).expanduser().resolve())
            payload["config_version"] = CONFIG_VERSION
            payload["managed_by_gui"] = True
            self._create_snapshot(current or payload, kind="automatic", name="数据目录切换前")
            _atomic_write(self.path, yaml.safe_dump(payload, allow_unicode=True, sort_keys=False))
            set_config(self.load())
            snap = self._create_snapshot(payload, kind="automatic")
            return {"status": "ok", "warnings": [], "restart_required": [],
                    "changed_fields": ["data.root"], "config_revision": _hash_config(payload),
                    "snapshot_id": snap["id"]}

    def list_snapshots(self) -> list[dict[str, Any]]:
        if not self.backup_dir.exists():
            return []
        items = []
        for path in self.backup_dir.glob("*.json"):
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                items.append({key: raw.get(key) for key in
                              ("id", "kind", "name", "created_at", "config_hash")})
            except (OSError, ValueError):
                continue
        return sorted(items, key=lambda item: item.get("created_at") or "", reverse=True)

    def create_named_snapshot(self, name: str) -> dict[str, Any]:
        name = name.strip()
        if not name or len(name) > 80 or any(ord(ch) < 32 for ch in name):
            raise ValueError("快照名称须为 1–80 个可见字符")
        return self._create_snapshot(_read_yaml(self.path), kind="manual", name=name)

    def _create_snapshot(self, config: dict[str, Any], *, kind: str, name: str = "") -> dict:
        safe = _sanitize(config)
        digest = _hash_config(safe)
        if kind == "automatic":
            for item in self.list_snapshots():
                if item.get("kind") == "automatic" and item.get("config_hash") == digest:
                    return item
        now = datetime.now(timezone.utc)
        snap_id = f"{now.strftime('%Y%m%dT%H%M%S%fZ')}-{uuid.uuid4().hex[:8]}"
        item = {"id": snap_id, "kind": kind, "name": name,
                "created_at": now.isoformat(), "config_hash": digest, "config": safe}
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        _atomic_write(self.backup_dir / f"{snap_id}.json",
                      json.dumps(item, ensure_ascii=False, indent=2))
        if kind == "automatic":
            autos = [entry for entry in self.list_snapshots() if entry.get("kind") == "automatic"]
            for old in autos[AUTO_SNAPSHOT_LIMIT:]:
                (self.backup_dir / f"{old['id']}.json").unlink(missing_ok=True)
        return {key: item[key] for key in ("id", "kind", "name", "created_at", "config_hash")}

    def _load_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", snapshot_id):
            raise ValueError("快照 ID 非法")
        path = self.backup_dir / f"{snapshot_id}.json"
        if not path.is_file():
            raise FileNotFoundError("快照不存在")
        return json.loads(path.read_text(encoding="utf-8"))

    def snapshot_diff(self, snapshot_id: str) -> list[dict[str, Any]]:
        target = _sanitize(self._load_snapshot(snapshot_id)["config"])
        current = _sanitize(_read_yaml(self.path))
        left, right = _flatten(current), _flatten(target)
        return [{"field": key, "current": left.get(key), "target": right.get(key)}
                for key in sorted(set(left) | set(right)) if left.get(key) != right.get(key)]

    def rollback(self, snapshot_id: str) -> dict[str, Any]:
        with self._lock:
            target = _sanitize(self._load_snapshot(snapshot_id)["config"])
            # 目标必须重新通过当前 schema，旧版本快照不会绕过校验。
            doc = SettingsDocument.model_validate(
                {key: target[key] for key in SettingsDocument.model_fields if key in target}
            )
            current = _read_yaml(self.path)
            self._create_snapshot(current, kind="automatic", name="回滚前")
            merged = doc.model_dump()
            merged["managed_by_gui"] = True
            merged["_secrets"] = copy.deepcopy(current.get("_secrets") or {})
            for section, field in (("llm", "api_key"), ("data", "tushare_token")):
                if isinstance(current.get(section), dict) and field in current[section]:
                    merged[section][field] = current[section][field]
            _atomic_write(self.path, yaml.safe_dump(merged, allow_unicode=True, sort_keys=False))
            set_config(self.load())
            before = _flatten(_sanitize(current))
            after = _flatten(_sanitize(merged))
            changed = sorted(
                key for key in set(before) | set(after) if before.get(key) != after.get(key)
            )
            restart = [field for field in ("server.host", "server.port") if field in changed]
            return {"status": "ok", "snapshot_id": snapshot_id,
                    "changed_fields": changed, "config_revision": _hash_config(merged),
                    "restart_required": restart}

    def delete_snapshot(self, snapshot_id: str) -> None:
        item = self._load_snapshot(snapshot_id)
        if item.get("kind") != "manual":
            raise ValueError("仅手动快照可以删除")
        (self.backup_dir / f"{snapshot_id}.json").unlink()
