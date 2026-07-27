"""股票名称缓存：内置名称优先，缺失项由 AKShare 补全并长期复用。"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from pathlib import Path

from quantmaster.config import get_config
from quantmaster.data.universe import DEMO_STOCK_NAMES

logger = logging.getLogger(__name__)

CACHE_TTL_DAYS = 7


def _cache_path() -> Path:
    return get_config().data_root / "stock_names.json"


def _read_cache() -> tuple[dict[str, str], float]:
    path = _cache_path()
    if not path.exists():
        return {}, 0.0
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        names = data.get("names", {})
        return ({str(k): str(v) for k, v in names.items() if v}, float(data.get("updated_at", 0)))
    except (json.JSONDecodeError, OSError, TypeError, ValueError):
        return {}, 0.0


def save_stock_names(names: dict[str, str]) -> None:
    """原子保存名称映射，避免服务中断留下半份 JSON。"""
    path = _cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump({"updated_at": time.time(), "names": names}, handle, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def cached_stock_names(symbols: list[str]) -> dict[str, str]:
    """只读内置名称和本地缓存；候选浏览不会隐式触网。"""
    requested = list(dict.fromkeys(str(symbol) for symbol in symbols))
    cached, _ = _read_cache()
    available = {**DEMO_STOCK_NAMES, **cached}
    return {symbol: available[symbol] for symbol in requested if symbol in available}


def fetch_stock_names(symbols: list[str]) -> dict[str, str]:  # pragma: no cover - 网络
    """一次 AKShare 全市场快照补齐指定代码；底层已统一重试。"""
    from quantmaster.data.akshare_source import AkshareSource

    requested_by_code = {symbol.split(".")[0]: symbol for symbol in symbols}
    snapshot = AkshareSource().spot(symbols)
    result: dict[str, str] = {}
    for _, row in snapshot.iterrows():
        symbol = requested_by_code.get(str(row.get("code", "")).zfill(6))
        name = str(row.get("name", "")).strip()
        if symbol and name and name.lower() != "nan":
            result[symbol] = name
    return result


def load_stock_names(symbols: list[str], refresh: bool = False) -> dict[str, str]:
    """返回指定代码的名称；任何联网失败都退回内置/旧缓存。"""
    requested = list(dict.fromkeys(str(symbol) for symbol in symbols))
    cached, updated_at = _read_cache()
    available = {**DEMO_STOCK_NAMES, **cached}
    missing = [symbol for symbol in requested if symbol not in available]
    fresh = time.time() - updated_at < CACHE_TTL_DAYS * 86400
    if not refresh and not missing and (fresh or all(s in DEMO_STOCK_NAMES for s in requested)):
        return {symbol: available[symbol] for symbol in requested if symbol in available}

    targets = requested if refresh or not fresh else missing
    try:
        fetched = fetch_stock_names(targets)
        if fetched:
            cached.update(fetched)
            save_stock_names(cached)
            available.update(fetched)
    except Exception as exc:
        logger.warning("股票名称补全失败，使用本地缓存: %s", exc)
    return {symbol: available[symbol] for symbol in requested if symbol in available}
