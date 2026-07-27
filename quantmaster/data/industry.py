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
from quantmaster.data.resilience import akshare_call

logger = logging.getLogger(__name__)

CACHE_TTL_DAYS = 30


def _cache_path() -> Path:
    return get_config().data_root / "industry_map.json"


def _block_cache_path() -> Path:
    return get_config().data_root / "industry_blocks.json"


def _load_industry_blocks() -> dict[str, dict]:
    path = _block_cache_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("blocks", {}) if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def _save_industry_blocks(blocks: dict[str, dict]) -> None:
    """成功一个板块就原子落盘，后续板块失败也不影响已取得的数据。"""
    path = _block_cache_path()
    temp = path.with_suffix(".json.tmp")
    temp.write_text(
        json.dumps({"updated_at": time.time(), "blocks": blocks}, ensure_ascii=False),
        encoding="utf-8",
    )
    temp.replace(path)


def fetch_industry_map() -> dict[str, str]:  # pragma: no cover - 网络
    """优先用 2000 积分 Tushare 申万行业，失败后回退东方财富。"""
    tushare_mapping: dict[str, str] = {}
    if get_config().data.tushare_token:
        try:
            from quantmaster.data.tushare_source import TushareSource

            tushare_mapping = TushareSource().industry_map()
            # A 股在市公司通常远超 3000；低于该值多半是个别行业请求失败。
            if len(tushare_mapping) >= 3000:
                return tushare_mapping
            logger.warning(
                "Tushare 申万行业映射仅 %s 条，继续用 AKShare 补全", len(tushare_mapping))
        except Exception as e:
            logger.warning("Tushare 申万行业映射失败，降级 AKShare: %s", e)

    import akshare as ak

    boards = akshare_call(
        "stock_board_industry_name_em", ak.stock_board_industry_name_em)
    blocks = _load_industry_blocks()
    for _, row in boards.iterrows():
        board = str(row["板块名称"])
        try:
            cons = akshare_call(
                f"stock_board_industry_cons_em({board})",
                ak.stock_board_industry_cons_em, symbol=board,
            )
        except Exception as e:
            logger.warning("行业 %s 成分获取失败: %s", board, e)
            continue
        block_mapping: dict[str, str] = {}
        for code in cons["代码"].astype(str).str.zfill(6):
            suffix = "SH" if code.startswith(("6", "9")) else (
                "BJ" if code.startswith(("4", "8")) else "SZ")
            block_mapping[f"{code}.{suffix}"] = board
        # 空响应同样视为不完整，不用它覆盖以前抓到的完整板块。
        if block_mapping:
            blocks[board] = {"updated_at": time.time(), "mapping": block_mapping}
            _save_industry_blocks(blocks)
    mapping: dict[str, str] = {}
    for block in blocks.values():
        if isinstance(block, dict):
            mapping.update(block.get("mapping", {}))
    # 对同一股票优先采用申万 2021 一级行业口径。
    return {**mapping, **tushare_mapping}


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
            # 抓取可能只成功了一部分行业。新数据优先，但绝不能用部分结果
            # 删除旧缓存中已经完整取得的股票/板块映射。
            merged = {**cached, **mapping}
            save_industry_map(merged)
            return merged
    except Exception as e:
        logger.warning("行业映射抓取失败: %s", e)
    return cached
