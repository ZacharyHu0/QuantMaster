"""股票池管理：指数成分、自定义列表。"""

from __future__ import annotations

import json
from pathlib import Path

from quantmaster.config import get_config

# 内置示例股票池：沪深各行业代表性大盘股（便于开箱即用地跑通流程）
DEMO_UNIVERSE = [
    "600519.SH",  # 贵州茅台
    "601318.SH",  # 中国平安
    "600036.SH",  # 招商银行
    "601899.SH",  # 紫金矿业
    "600900.SH",  # 长江电力
    "688981.SH",  # 中芯国际
    "000333.SZ",  # 美的集团
    "000858.SZ",  # 五粮液
    "300750.SZ",  # 宁德时代
    "002594.SZ",  # 比亚迪
    "300059.SZ",  # 东方财富
    "002230.SZ",  # 科大讯飞
]


def _universe_dir() -> Path:
    p = get_config().data_root / "universe"
    p.mkdir(parents=True, exist_ok=True)
    return p


def save_universe(name: str, symbols: list[str]) -> None:
    (_universe_dir() / f"{name}.json").write_text(
        json.dumps(symbols, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def load_universe(name: str) -> list[str]:
    if name == "demo":
        return list(DEMO_UNIVERSE)
    path = _universe_dir() / f"{name}.json"
    if not path.exists():
        raise FileNotFoundError(f"股票池不存在: {name}（可用 save_universe 创建，或使用 'demo'）")
    return json.loads(path.read_text(encoding="utf-8"))


def index_universe(index_symbol: str = "000300.SH") -> list[str]:  # pragma: no cover - 网络
    """从指数成分构建股票池（如沪深300）。"""
    from quantmaster.data.akshare_source import AkshareSource

    return AkshareSource().index_members(index_symbol)
