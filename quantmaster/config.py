"""全局配置。

旧配置保持 ``默认值 < YAML < 环境变量``；首次经 GUI 保存后写入
``managed_by_gui``，此后使用 ``默认值 < 环境变量 < YAML``，避免环境变量
悄悄覆盖用户刚在设置中心确认的值。GUI 管理的密钥再由系统凭据库或显式
明文状态覆盖，``cleared`` 状态会阻止环境变量令密钥意外恢复。
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATHS = [WORKSPACE_ROOT / "config.yaml"]


@dataclass
class LLMConfig:
    provider: str = "anthropic"          # anthropic | openai | openai-compatible
    model: str = "claude-sonnet-5"
    api_key: str = ""
    base_url: str = ""                    # openai-compatible 网关地址（DeepSeek/Qwen 等）
    reasoning_effort: str = "medium"      # none | minimal | low | medium | high | xhigh | max
    max_tokens: int = 2048
    temperature: float = 0.3
    timeout: float = 60.0
    max_concurrency: int = 1              # 除资讯标注外的 LLM 请求共享上限
    queue_timeout: float = 30.0           # 并发闸门 FIFO 排队最长等待秒数


@dataclass
class DataConfig:
    root: str = "data"                    # 本地数据目录（缓存/数据库）
    primary_provider: str = "free-stockdb"  # free-stockdb | akshare | tushare
    free_stockdb_sdk_path: str = ""       # 留空自动发现 runtime 根目录下的 pybao
    free_stockdb_url: str = "http://127.0.0.1:7899"
    free_stockdb_timeout: float = 3.0      # 本地服务不可用时快速降级
    free_stockdb_root: str = "runtime/free-stockdb"
    free_stockdb_managed: bool = True
    free_stockdb_auto_update: bool = True  # 到点自动停库、更新、验收并恢复服务
    free_stockdb_update_time: str = "18:30"
    free_stockdb_online_enabled: bool = False
    free_stockdb_online_url: str = "http://8.138.149.215:7899"
    free_stockdb_online_timeout: float = 4.0
    akshare_enabled: bool = True
    tushare_enabled: bool = True
    yfinance_enabled: bool = True
    free_stockdb_ingest_retain: int = 30
    free_stockdb_stock_history_sessions: int = 180
    free_stockdb_stock_initial_lookback_days: int = 300
    free_stockdb_stock_max_lookback_days: int = 540
    free_stockdb_etf_research_enabled: bool = True
    free_stockdb_etf_minutes_enabled: bool = True
    free_stockdb_experimental_tick_enabled: bool = False
    free_stockdb_experimental_fundamentals_enabled: bool = False
    free_stockdb_experimental_daily_quota: int = 20
    free_stockdb_native_acceleration_enabled: bool = False
    tushare_token: str = ""
    cache_days: int = 1                   # 日线缓存有效期（天）
    intraday_cache_minutes: int = 5       # 当日分钟线再次触网前的最短间隔
    akshare_retries: int = 3              # 单次 AKShare 请求总尝试次数
    akshare_retry_backoff: float = 0.8    # 指数退避初始秒数（0.8/1.6/...）
    provider_retry_attempts: int = 3      # 所有远程 provider 共享的瞬态故障总尝试次数
    provider_retry_backoff: float = 0.8   # 瞬态故障退避初始秒数（有上限）
    provider_retry_max_backoff: float = 8.0
    provider_timeout: float = 45.0        # 单次上游任务含排队的硬截止秒数
    tushare_calls_per_minute: int = 120   # 2000 积分档保守全局限速
    tushare_cache_days: int = 1           # Tushare 当期接口响应缓存天数
    fundamental_cache_days: int = 7       # 季度财务数据本地缓存天数
    after_close_enabled: bool = True       # free-stockdb 盘后研究扫描
    after_close_auto_run: bool = True      # 本地库更新就绪后自动运行
    after_close_include_bj: bool = True
    after_close_min_listing_sessions: int = 60
    after_close_min_avg_amount: float = 30_000_000.0
    after_close_candidate_limit: int = 30
    after_close_notify: bool = True
    repair_enabled: bool = True            # 检测到可重建数据损坏时持久排队
    repair_daily_budget: int = 100         # 每个数据源每日最多自动修复次数
    repair_max_workers: int = 2            # 全局自动修复并发上限
    repair_retry_backoff: float = 60.0     # 修复失败后的初始退避秒数
    repair_max_attempts: int = 5           # 自动修复最大尝试次数


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
    annotation_max_concurrency: int = 4
    annotation_items_per_run: int = 100
    annotation_timeout: float = 180.0
    annotation_model: str = ""
    annotation_reasoning_effort: str = "low"
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
    fast_news_interval_minutes: int = 20
    official_news_interval_minutes: int = 120
    periodic_news_interval_minutes: int = 360


@dataclass
class LabConfig:
    """AI Quant Lab 的研究范围、资源预算与合规边界。"""

    enabled: bool = True
    universe: str = "csi800"
    start: str = "2015-01-01"
    horizons: list[int] = field(default_factory=lambda: [1, 3, 5, 7, 10, 20, 30])
    weekly_days: list[int] = field(default_factory=lambda: [1, 3, 5])
    window_start: str = "19:00"
    window_end: str = "07:00"
    daily_budget_hours: float = 10.0
    max_workers: int = 2
    device: str = "auto"
    data_policy: str = "prefer_local"
    panel_cache_mb: int = 2048
    feature_cache_gb: int = 8
    gpu_memory_fraction: float = 0.80
    gpu_max_concurrent_jobs: int = 1
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
    config_path: Path | None = field(default=None, repr=False)
    workspace_root: Path = field(default=WORKSPACE_ROOT, repr=False)

    def resolve_local_path(self, value: str | Path, *, label: str) -> Path:
        """Resolve an instance path against explicit config provenance, never cwd."""

        text = os.fspath(value).strip()
        if not text or "\x00" in text:
            raise ValueError(f"{label}路径无效")
        path = Path(text).expanduser()
        if path.is_absolute():
            return path.resolve()
        anchor = self.config_path.parent if self.config_path is not None else self.workspace_root
        if not anchor.is_absolute():
            raise ValueError(f"{label}缺少绝对 workspace/config provenance")
        return (anchor / path).resolve()

    @property
    def data_root(self) -> Path:
        """Return the configured data path without changing the filesystem.

        This property is used pervasively by Web snapshot readers.  Creating
        the directory here made an apparently read-only GET mutate a cold
        installation, and let readiness accidentally report that cold state
        as healthy.  Persistent workers call :meth:`ensure_data_root` during
        their explicit startup phase instead.
        """

        return self.resolve_local_path(self.data.root, label="data root")

    @property
    def free_stockdb_root(self) -> Path:
        """Return the managed runtime root bound to this config instance."""

        return self.resolve_local_path(
            self.data.free_stockdb_root, label="free-stockdb runtime",
        )

    def ensure_data_root(self) -> Path:
        """Create the configured data root from an explicit writer context."""

        root = self.data_root
        root.mkdir(parents=True, exist_ok=True)
        return root


def _apply_dict(obj: Any, data: dict) -> None:
    for key, value in data.items():
        if key in {"config_path", "workspace_root"}:
            continue
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
    cfg.llm.reasoning_effort = env.get(
        "QM_LLM_REASONING_EFFORT", cfg.llm.reasoning_effort)
    cfg.llm.max_concurrency = int(
        env.get("QM_LLM_MAX_CONCURRENCY", cfg.llm.max_concurrency))
    cfg.llm.queue_timeout = float(
        env.get("QM_LLM_QUEUE_TIMEOUT", cfg.llm.queue_timeout))
    cfg.news.annotation_max_concurrency = int(env.get(
        "QM_NEWS_ANNOTATION_MAX_CONCURRENCY", cfg.news.annotation_max_concurrency,
    ))
    cfg.data.tushare_token = env.get("TUSHARE_TOKEN", cfg.data.tushare_token)
    cfg.data.root = env.get("QM_DATA_ROOT", cfg.data.root)
    cfg.data.primary_provider = env.get(
        "QM_DATA_PRIMARY_PROVIDER", cfg.data.primary_provider).strip().lower()
    cfg.data.free_stockdb_sdk_path = env.get(
        "QM_FREE_STOCKDB_SDK_PATH", cfg.data.free_stockdb_sdk_path).strip()
    cfg.data.free_stockdb_url = env.get(
        "QM_FREE_STOCKDB_URL", cfg.data.free_stockdb_url).strip().rstrip("/")
    cfg.data.free_stockdb_timeout = float(
        env.get("QM_FREE_STOCKDB_TIMEOUT", cfg.data.free_stockdb_timeout))
    cfg.data.free_stockdb_root = env.get(
        "QM_FREE_STOCKDB_ROOT", cfg.data.free_stockdb_root).strip()
    cfg.data.free_stockdb_managed = env.get(
        "QM_FREE_STOCKDB_MANAGED", str(cfg.data.free_stockdb_managed)
    ).strip().lower() in {"1", "true", "yes", "on"}
    cfg.data.free_stockdb_auto_update = env.get(
        "QM_FREE_STOCKDB_AUTO_UPDATE", str(cfg.data.free_stockdb_auto_update)
    ).strip().lower() in {"1", "true", "yes", "on"}
    cfg.data.free_stockdb_update_time = env.get(
        "QM_FREE_STOCKDB_UPDATE_TIME", cfg.data.free_stockdb_update_time).strip()
    cfg.data.free_stockdb_online_enabled = env.get(
        "QM_FREE_STOCKDB_ONLINE_ENABLED", str(cfg.data.free_stockdb_online_enabled)
    ).strip().lower() in {"1", "true", "yes", "on"}
    cfg.data.free_stockdb_online_url = env.get(
        "QM_FREE_STOCKDB_ONLINE_URL", cfg.data.free_stockdb_online_url).strip().rstrip("/")
    cfg.data.free_stockdb_online_timeout = float(env.get(
        "QM_FREE_STOCKDB_ONLINE_TIMEOUT", cfg.data.free_stockdb_online_timeout))
    cfg.data.akshare_enabled = env.get(
        "QM_AKSHARE_ENABLED", str(cfg.data.akshare_enabled)
    ).strip().lower() in {"1", "true", "yes", "on"}
    cfg.data.tushare_enabled = env.get(
        "QM_TUSHARE_ENABLED", str(cfg.data.tushare_enabled)
    ).strip().lower() in {"1", "true", "yes", "on"}
    cfg.data.yfinance_enabled = env.get(
        "QM_YFINANCE_ENABLED", str(cfg.data.yfinance_enabled)
    ).strip().lower() in {"1", "true", "yes", "on"}
    cfg.data.akshare_retries = int(
        env.get("QM_AKSHARE_RETRIES", cfg.data.akshare_retries))
    cfg.data.akshare_retry_backoff = float(
        env.get("QM_AKSHARE_RETRY_BACKOFF", cfg.data.akshare_retry_backoff))
    cfg.data.provider_retry_attempts = int(
        env.get("QM_PROVIDER_RETRY_ATTEMPTS", cfg.data.provider_retry_attempts))
    cfg.data.provider_retry_backoff = float(
        env.get("QM_PROVIDER_RETRY_BACKOFF", cfg.data.provider_retry_backoff))
    cfg.data.provider_retry_max_backoff = float(
        env.get("QM_PROVIDER_RETRY_MAX_BACKOFF", cfg.data.provider_retry_max_backoff))
    cfg.data.provider_timeout = float(
        env.get("QM_DATA_PROVIDER_TIMEOUT", cfg.data.provider_timeout))
    cfg.data.tushare_calls_per_minute = int(
        env.get("QM_TUSHARE_CALLS_PER_MINUTE", cfg.data.tushare_calls_per_minute))
    cfg.data.tushare_cache_days = int(
        env.get("QM_TUSHARE_CACHE_DAYS", cfg.data.tushare_cache_days))
    cfg.data.fundamental_cache_days = int(
        env.get("QM_FUNDAMENTAL_CACHE_DAYS", cfg.data.fundamental_cache_days))
    cfg.data.repair_enabled = env.get(
        "QM_DATA_REPAIR_ENABLED", str(cfg.data.repair_enabled)
    ).strip().lower() in {"1", "true", "yes", "on"}
    cfg.data.repair_daily_budget = int(
        env.get("QM_DATA_REPAIR_DAILY_BUDGET", cfg.data.repair_daily_budget))
    cfg.data.repair_max_workers = int(
        env.get("QM_DATA_REPAIR_MAX_WORKERS", cfg.data.repair_max_workers))
    cfg.data.repair_retry_backoff = float(
        env.get("QM_DATA_REPAIR_RETRY_BACKOFF", cfg.data.repair_retry_backoff))
    cfg.data.repair_max_attempts = int(
        env.get("QM_DATA_REPAIR_MAX_ATTEMPTS", cfg.data.repair_max_attempts))
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


def load_config(
    path: str | Path | None = None,
    *,
    load_secrets: bool = True,
) -> Config:
    """Load configuration, optionally leaving keyring-backed secrets untouched.

    Settings pages only need public configuration fields.  They must not open
    the platform credential manager on a GET, because a locked/unavailable
    credential backend used to make every settings render block or fail.
    """
    cfg = Config()
    explicit = path or os.environ.get("QM_CONFIG_PATH", "").strip()
    if explicit:
        configured = Path(explicit).expanduser()
        candidates = [
            configured.resolve() if configured.is_absolute()
            else (WORKSPACE_ROOT / configured).resolve()
        ]
    else:
        candidates = DEFAULT_CONFIG_PATHS
    raw: dict = {}
    selected: Path | None = None
    for candidate in candidates:
        if candidate.is_file():
            with open(candidate, encoding="utf-8") as f:
                raw = yaml.safe_load(f) or {}
            selected = candidate.resolve()
            break
    cfg.config_path = selected
    cfg.workspace_root = WORKSPACE_ROOT
    managed = bool(raw.get("managed_by_gui", False))
    if managed:
        _apply_env(cfg)
        _apply_dict(cfg, raw)
        cfg.managed_by_gui = True
        if load_secrets:
            _apply_managed_secrets(cfg, raw)
    else:
        _apply_dict(cfg, raw)
        _apply_env(cfg)
    return cfg


_config: Config | None = None
_config_generation = 0
_config_lock = threading.RLock()
_config_readiness: tuple[str, dict[str, str]] | None = None


def _cache_readiness_locked(cfg: Config) -> None:
    """Refresh the small, process-local readiness snapshot.

    This runs only when configuration is installed or explicitly changed.
    HTTP readiness probes therefore never need to touch SQLite, create a
    directory, or wait on configuration/keyring I/O.
    """

    global _config_readiness
    root = cfg.data_root
    _config_readiness = (
        str(cfg.data.root),
        {
            "status": "ready" if root.is_dir() else "not_ready",
            "data_root": str(root),
        },
    )


def get_config() -> Config:
    """Return the current config without letting a slow load overwrite a switch.

    Loading a GUI-managed config may initialize the platform credential backend.
    That can take seconds.  Previously a metrics/background thread could begin
    that load, another thread could then install an explicit data-root config,
    and the first thread would finally overwrite it with the stale default.
    The result was a page reading the wrong SQLite/cache root.  Keep slow I/O
    outside the lock, then publish its result only if the configuration
    generation is still current.
    """

    global _config
    while True:
        with _config_lock:
            if _config is not None:
                return _config
            generation = _config_generation
        loaded = load_config()
        with _config_lock:
            if _config is not None:
                return _config
            if generation == _config_generation:
                _config = loaded
                _cache_readiness_locked(loaded)
                return loaded
            # ``set_config(None)`` can deliberately invalidate a load while
            # it is in flight.  Retry against the new generation rather than
            # returning a config that has already been superseded.


def set_config(cfg: Config | None) -> None:
    """设置全局配置；传 None 重置（下次 get_config 时按默认路径重新加载）。"""
    global _config, _config_generation, _config_readiness
    with _config_lock:
        _config = cfg
        _config_generation += 1
        _config_readiness = None
        if cfg is not None:
            _cache_readiness_locked(cfg)


def get_config_readiness() -> dict[str, str]:
    """Return the cached data-root state used by ``/health/ready``.

    The initial call may establish configuration during process bootstrap.  A
    running Web generation is already configured in its lifespan, so ordinary
    readiness requests return only this in-memory snapshot.  The small root
    comparison also keeps tests and controlled runtime root switches correct
    when the active ``Config`` object is deliberately updated in place.
    """

    global _config_readiness
    while True:
        with _config_lock:
            cfg = _config
            cached = _config_readiness
            if cfg is not None and cached is not None and cached[0] == str(cfg.data.root):
                return dict(cached[1])
        # This branch is startup/configuration transition only.  It is outside
        # the lock because ``get_config`` can load GUI-managed credentials.
        cfg = get_config()
        with _config_lock:
            if _config is cfg:
                _cache_readiness_locked(cfg)
                return dict(_config_readiness[1])
