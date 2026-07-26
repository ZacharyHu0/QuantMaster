"""全局配置：默认值 < config.yaml < 环境变量。"""

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
class Config:
    llm: LLMConfig = field(default_factory=LLMConfig)
    data: DataConfig = field(default_factory=DataConfig)
    trade: TradeConfig = field(default_factory=TradeConfig)
    server: ServerConfig = field(default_factory=ServerConfig)

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
    cfg.llm.api_key = env.get("QM_LLM_API_KEY", cfg.llm.api_key)
    if not cfg.llm.api_key:
        if cfg.llm.provider == "anthropic":
            cfg.llm.api_key = env.get("ANTHROPIC_API_KEY", "")
        else:
            cfg.llm.api_key = env.get("OPENAI_API_KEY", "")
    cfg.llm.provider = env.get("QM_LLM_PROVIDER", cfg.llm.provider)
    cfg.llm.model = env.get("QM_LLM_MODEL", cfg.llm.model)
    cfg.llm.base_url = env.get("QM_LLM_BASE_URL", cfg.llm.base_url)
    cfg.data.tushare_token = env.get("TUSHARE_TOKEN", cfg.data.tushare_token)
    cfg.data.root = env.get("QM_DATA_ROOT", cfg.data.root)


def load_config(path: str | Path | None = None) -> Config:
    cfg = Config()
    candidates = [Path(path)] if path else DEFAULT_CONFIG_PATHS
    for candidate in candidates:
        if candidate.is_file():
            with open(candidate, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            _apply_dict(cfg, data)
            break
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
