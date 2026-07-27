"""行业分类：股票 -> 行业 的映射，用于因子行业中性化与持仓行业分布。

数据源：东方财富行业板块（akshare，免费）。映射本地 JSON 缓存，默认 30 天
有效（行业调整不频繁）。也可以用 save_industry_map 写入自己的映射
（如申万分类的授权数据）。
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from quantmaster.config import get_config

logger = logging.getLogger(__name__)

CACHE_TTL_DAYS = 30


def _cache_path() -> Path:
    return get_config().data_root / "industry_map.json"


def fetch_industry_map() -> dict[str, str]:  # pragma: no cover - 网络
    """从东方财富行业板块抓取全 A 股票的行业映射。约几十次请求，较慢。"""
    import akshare as ak

    boards = ak.stock_board_industry_name_em()
    mapping: dict[str, str] = {}
    for _, row in boards.iterrows():
        board = str(row["板块名称"])
        try:
            cons = ak.stock_board_industry_cons_em(symbol=board)
        except Exception as e:
            logger.warning("行业 %s 成分获取失败: %s", board, e)
            continue
        for code in cons["代码"].astype(str).str.zfill(6):
            suffix = "SH" if code.startswith(("6", "9")) else (
                "BJ" if code.startswith(("4", "8")) else "SZ")
            mapping[f"{code}.{suffix}"] = board
    return mapping


def save_industry_map(mapping: dict[str, str]) -> None:
    path = _cache_path()
    path.write_text(
        json.dumps({"updated_at": time.time(), "mapping": mapping}, ensure_ascii=False),
        encoding="utf-8",
    )


def load_industry_map(refresh: bool = False) -> dict[str, str]:
    """读取行业映射：缓存有效直接用；过期/缺失尝试触网，失败退回旧缓存或空。"""
    path = _cache_path()
    cached: dict[str, str] = {}
    fresh = False
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            cached = data.get("mapping", {})
            fresh = (time.time() - data.get("updated_at", 0)) < CACHE_TTL_DAYS * 86400
        except (json.JSONDecodeError, OSError):
            pass
    if cached and fresh and not refresh:
        return cached
    try:
        mapping = fetch_industry_map()
        if mapping:
            save_industry_map(mapping)
            return mapping
    except Exception as e:
        logger.warning("行业映射抓取失败: %s", e)
    return cached
