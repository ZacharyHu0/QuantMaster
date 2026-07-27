"""证券名称兼容入口。

名称现由 ``security_master.sqlite`` 统一维护；保留这些函数以兼容旧调用方。
读取永不隐式联网，显式刷新失败时也始终退回随包快照和本地历史。
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from pathlib import Path

from quantmaster.config import get_config
from quantmaster.data.instruments import InstrumentStore

logger = logging.getLogger(__name__)


def _legacy_cache_path() -> Path:
    return get_config().data_root / "stock_names.json"


def _read_cache() -> tuple[dict[str, str], float]:
    """保留旧扩展的私有兼容接口；运行时检索仍以 SQLite 为准。"""
    path = _legacy_cache_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return ({str(key): str(value) for key, value in payload.get("names", {}).items() if value},
                float(payload.get("updated_at") or 0))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return {}, 0


def _write_legacy_cache(names: dict[str, str]) -> None:
    path = _legacy_cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump({"updated_at": time.time(), "names": names}, handle, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def cached_stock_names(symbols: list[str]) -> dict[str, str]:
    return InstrumentStore().names(symbols)


def fetch_stock_names(symbols: list[str]) -> dict[str, str]:  # pragma: no cover - 网络
    """从独立的 A 股快照补齐缺失名称，并写回证券主数据。"""
    from quantmaster.data.akshare_source import AkshareSource

    store = InstrumentStore()
    requested = {str(symbol).upper() for symbol in symbols}
    snapshot = AkshareSource().spot(list(requested))
    records = []
    for _, row in snapshot.iterrows():
        code = str(row.get("code", "")).zfill(6)
        matches = [symbol for symbol in requested if symbol.partition(".")[0] == code]
        name = str(row.get("name", "")).strip()
        for symbol in matches:
            current = store.get(symbol)
            if current and name and name.lower() != "nan":
                value = current.to_dict()
                value.update({"name": name, "source": "akshare:spot", "source_priority": 40})
                records.append(value)
    store.upsert(records, source="akshare:spot", source_priority=40)
    store.update_sync_state("akshare:spot", status="success", record_count=len(records))
    return store.names(requested)


def load_stock_names(symbols: list[str], refresh: bool = False) -> dict[str, str]:
    requested = list(dict.fromkeys(str(symbol).upper() for symbol in symbols))
    store = InstrumentStore()
    cached = store.names(requested)
    if refresh:
        try:
            cached.update(fetch_stock_names(requested))
        except Exception as exc:
            store.update_sync_state("akshare:spot", status="error", error=str(exc))
            logger.warning("证券名称刷新失败，继续使用本地主数据: %s", exc)
    _write_legacy_cache(cached)
    return cached


def save_stock_names(names: dict[str, str]) -> None:
    """把旧名称映射并入主数据；未知代码不会被虚构成可交易标的。"""
    store = InstrumentStore()
    records = []
    for symbol, name in names.items():
        current = store.get(symbol)
        if current and str(name).strip():
            value = current.to_dict()
            value.update({"name": str(name).strip(), "source": "legacy", "source_priority": 20})
            records.append(value)
    store.upsert(records, source="legacy", source_priority=20)
    cached, _ = _read_cache()
    cached.update({str(symbol).upper(): str(name) for symbol, name in names.items() if name})
    _write_legacy_cache(cached)
