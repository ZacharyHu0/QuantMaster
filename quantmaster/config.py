"""全局配置。

旧配置保持 ``默认值 < YAML < 环境变量``；首次经 GUI 保存后写入
``managed_by_gui``，此后使用 ``默认值 < 环境变量 < YAML``，避免环境变量
悄悄覆盖用户刚在设置中心确认的值。GUI 管理的密钥再由系统凭据库或显式
明文状态覆盖，``cleared`` 状态会阻止环境变量令密钥意外恢复。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATHS = [
    Path("config.yaml"),
    Path.home() / ".quantmaster" / "config.yaml",
]


@dataclass
class LLMConfig:
    provider: str = "anthropic"          # anthropic | openai | openai-compatible
    model: str = "claude-sonnet-5"
    api_key: str = ""
    base_url: str = ""                    # openai-compatible 网关地址（DeepSeek/Qwen 等）
    max_tokens: int = 2048
    temperature: float = 0.3
    timeout: float = 60.0


@dataclass
class DataConfig:
    root: str = "data"                    # 本地数据目录（缓存/数据库）
    tushare_token: str = ""
    cache_days: int = 1                   # 日线缓存有效期（天）
    intraday_cache_minutes: int = 5       # 当日分钟线再次触网前的最短间隔
    akshare_retries: int = 3              # 单次 AKShare 请求总尝试次数
    akshare_retry_backoff: float = 0.8    # 指数退避初始秒数（0.8/1.6/...）
    tushare_calls_per_minute: int = 120   # 2000 积分档保守全局限速
    tushare_cache_days: int = 1           # Tushare 当期接口响应缓存天数
    fundamental_cache_days: int = 7       # 季度财务数据本地缓存天数


@dataclass
class TradeConfig:
    """A 股交易成本与规则参数。"""

    commission_rate: float = 2.5e-4       # 佣金 万2.5
    commission_min: float = 5.0           # 最低佣金 5 元
    stamp_tax_rate: float = 5e-4          # 印花税（卖出单边，2023-08 后 0.05%）
    transfer_fee_rate: float = 1e-5       # 过户费 0.001%
    slippage: float = 1e-3                # 滑点（成交价比例）
    lot_size: int = 100                   # A 股一手 100 股


@dataclass
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = 8686


@dataclass
class NewsConfig:
    """资讯归档、标注和消息面因子的全局参数。"""

    raw_cache_days: int = 7
    annotation_enabled: bool = True
    annotation_batch_size: int = 10
    annotation_items_per_run: int = 100
    factor_halflife_days: float = 3.0
    factor_min_confidence: float = 0.35


@dataclass
class AutomationConfig:
    """消息机器人、定时任务和重要事件推送的非敏感配置。"""

    enabled: bool = False
    timezone: str = "Asia/Shanghai"
    primary_universe: str = "demo"
    watchlist: list[str] = field(default_factory=list)
    sentinel_indices: list[str] = field(default_factory=lambda: [
        "000300.SH", "000905.SH", "000852.SH", "399006.SZ",
    ])
    weixin_api_base: str = "https://ilinkai.weixin.qq.com"
    feishu_app_id: str = ""
    retention_days: int = 90


@dataclass
class LabConfig:
    """AI Quant Lab 的研究范围、资源预算与合规边界。"""

    enabled: bool = True
    universe: str = "csi800"
    start: str = "2015-01-01"
    horizons: list[int] = field(default_factory=lambda: [1, 3, 5, 7])
    weekly_days: list[int] = field(default_factory=lambda: [1, 3, 5])
    window_start: str = "19:00"
    window_end: str = "07:00"
    daily_budget_hours: float = 10.0
    max_workers: int = 2
    device: str = "auto"
    allow_cloud_sample: bool = False
    ai_python_mining_enabled: bool = False


@dataclass
class Config:
    llm: LLMConfig = field(default_factory=LLMConfig)
    data: DataConfig = field(default_factory=DataConfig)
    trade: TradeConfig = field(default_factory=TradeConfig)
    news: NewsConfig = field(default_factory=NewsConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    automation: AutomationConfig = field(default_factory=AutomationConfig)
    lab: LabConfig = field(default_factory=LabConfig)
    config_version: int = 1
    managed_by_gui: bool = False

    @property
    def data_root(self) -> Path:
        p = Path(self.data.root)
        p.mkdir(parents=True, exist_ok=True)
        return p


def _apply_dict(obj: Any, data: dict) -> None:
    for key, value in data.items():
        if not hasattr(obj, key):
            continue
        current = getattr(obj, key)
        if isinstance(value, dict) and hasattr(current, "__dataclass_fields__"):
            _apply_dict(current, value)
        else:
            setattr(obj, key, value)


def _apply_env(cfg: Config) -> None:
    env = os.environ
    # LLM：优先使用专用变量，回落到官方 SDK 惯例变量
    cfg.llm.provider = env.get("QM_LLM_PROVIDER", cfg.llm.provider)
    cfg.llm.api_key = env.get("QM_LLM_API_KEY", cfg.llm.api_key)
    if not cfg.llm.api_key:
        if cfg.llm.provider == "anthropic":
            cfg.llm.api_key = env.get("ANTHROPIC_API_KEY", "")
        else:
            cfg.llm.api_key = env.get("OPENAI_API_KEY", "")
    cfg.llm.model = env.get("QM_LLM_MODEL", cfg.llm.model)
    cfg.llm.base_url = env.get("QM_LLM_BASE_URL", cfg.llm.base_url)
    cfg.data.tushare_token = env.get("TUSHARE_TOKEN", cfg.data.tushare_token)
    cfg.data.root = env.get("QM_DATA_ROOT", cfg.data.root)
    cfg.data.akshare_retries = int(
        env.get("QM_AKSHARE_RETRIES", cfg.data.akshare_retries))
    cfg.data.akshare_retry_backoff = float(
        env.get("QM_AKSHARE_RETRY_BACKOFF", cfg.data.akshare_retry_backoff))
    cfg.data.tushare_calls_per_minute = int(
        env.get("QM_TUSHARE_CALLS_PER_MINUTE", cfg.data.tushare_calls_per_minute))
    cfg.data.tushare_cache_days = int(
        env.get("QM_TUSHARE_CACHE_DAYS", cfg.data.tushare_cache_days))
    cfg.data.fundamental_cache_days = int(
        env.get("QM_FUNDAMENTAL_CACHE_DAYS", cfg.data.fundamental_cache_days))
    enabled = env.get("QM_AUTOMATION_ENABLED")
    if enabled is not None:
        cfg.automation.enabled = enabled.strip().lower() in {"1", "true", "yes", "on"}
    cfg.automation.weixin_api_base = env.get(
        "QM_WEIXIN_API_BASE", cfg.automation.weixin_api_base).rstrip("/")
    cfg.automation.feishu_app_id = env.get("QM_FEISHU_APP_ID", cfg.automation.feishu_app_id)
    enabled_lab = env.get("QM_LAB_ENABLED")
    if enabled_lab is not None:
        cfg.lab.enabled = enabled_lab.strip().lower() in {"1", "true", "yes", "on"}
    cfg.lab.max_workers = int(env.get("QM_LAB_MAX_WORKERS", cfg.lab.max_workers))
    cfg.lab.device = env.get("QM_LAB_DEVICE", cfg.lab.device)


def _apply_managed_secrets(cfg: Config, raw: dict) -> None:
    """解析 GUI 凭据状态；任何失败都按未配置处理，绝不回落到环境变量。"""
    from quantmaster.credentials import CredentialError, CredentialStore

    metadata = raw.get("_secrets") or {}
    store = CredentialStore()
    pairs = (
        ("llm", cfg.llm, "api_key",
         CredentialStore.llm_target(cfg.llm.provider, cfg.llm.base_url)),
        ("tushare", cfg.data, "tushare_token", CredentialStore.tushare_target()),
    )
    for name, owner, attr, default_target in pairs:
        item = metadata.get(name) or {}
        state = item.get("state")
        if state == "cleared":
            setattr(owner, attr, "")
        elif state == "keyring":
            try:
                setattr(owner, attr, store.get(item.get("target") or default_target) or "")
            except CredentialError:
                setattr(owner, attr, "")
        elif state == "plaintext":
            # 明文值已经随 YAML 应用；缺失时也不得回落到环境变量。
            section = raw.get("llm" if name == "llm" else "data") or {}
            key = "api_key" if name == "llm" else "tushare_token"
            setattr(owner, attr, str(section.get(key) or ""))


def load_config(path: str | Path | None = None) -> Config:
    cfg = Config()
    candidates = [Path(path)] if path else DEFAULT_CONFIG_PATHS
    raw: dict = {}
    for candidate in candidates:
        if candidate.is_file():
            with open(candidate, encoding="utf-8") as f:
                raw = yaml.safe_load(f) or {}
            break
    managed = bool(raw.get("managed_by_gui", False))
    if managed:
        _apply_env(cfg)
        _apply_dict(cfg, raw)
        cfg.managed_by_gui = True
        _apply_managed_secrets(cfg, raw)
    else:
        _apply_dict(cfg, raw)
        _apply_env(cfg)
    return cfg


_config: Config | None = None


def get_config() -> Config:
    global _config
    if _config is None:
        _config = load_config()
    return _config


def set_config(cfg: Config | None) -> None:
    """设置全局配置；传 None 重置（下次 get_config 时按默认路径重新加载）。"""
    global _config
    _config = cfg
