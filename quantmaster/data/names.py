"""由证券主数据统一维护的名称查询与显式刷新入口。"""

from __future__ import annotations

import logging
import sqlite3

from quantmaster.config import get_config
from quantmaster.data.instruments import InstrumentStore
from quantmaster.runtime.sqlite import connect_sqlite

logger = logging.getLogger(__name__)

def cached_stock_names(symbols: list[str], *, read_only: bool = False) -> dict[str, str]:
    requested = list(dict.fromkeys(str(symbol).upper() for symbol in symbols))
    if not requested:
        return {}
    if not read_only:
        return InstrumentStore().names(requested)
    # Keep the page/read contract independent from InstrumentStore bootstrap:
    # opening an empty security master must not create it, seed it, or perform
    # a migration while a GET is being served.
    path = get_config().data_root / "security_master.sqlite"
    try:
        with connect_sqlite(path, timeout=0.25, row_factory=True, read_only=True) as connection:
            result: dict[str, str] = {}
            for offset in range(0, len(requested), 800):
                batch = requested[offset:offset + 800]
                rows = connection.execute(
                    f"SELECT symbol,name FROM instruments WHERE symbol IN "
                    f"({','.join('?' for _ in batch)})",
                    batch,
                ).fetchall()
                result.update({str(row["symbol"]): str(row["name"]) for row in rows if row["name"]})
            return result
    except (FileNotFoundError, sqlite3.OperationalError):
        return {}


def refresh_stock_names(symbols: list[str]) -> dict[str, str]:  # pragma: no cover - 网络
    """从独立的 A 股快照补齐缺失名称，并写回证券主数据。"""
    from quantmaster.data.registry import refresh_spot

    store = InstrumentStore()
    requested = {str(symbol).upper() for symbol in symbols}
    market_envelope = refresh_spot(list(requested))
    snapshot = market_envelope.require_data()
    records = []
    for _, row in snapshot.iterrows():
        code = str(row.get("code", "")).zfill(6)
        matches = [symbol for symbol in requested if symbol.partition(".")[0] == code]
        name = str(row.get("name", "")).strip()
        for symbol in matches:
            current = store.get(symbol)
            if current and name and name.lower() != "nan":
                value = current.to_dict()
                source = f"{row.get('source') or 'market'}:spot"
                value.update({"name": name, "source": source, "source_priority": 40})
                records.append(value)
    store.upsert(records, source="market:spot", source_priority=40)
    verified = (
        market_envelope.quality.status == "verified"
        and not market_envelope.quality.partial
        and not market_envelope.quality.stale
    )
    store.update_sync_state(
        "market:spot",
        status="success" if verified else "degraded",
        record_count=len(records),
        error="；".join(market_envelope.quality.issues),
    )
    return store.names(requested)


def read_stock_names(symbols: list[str]) -> dict[str, str]:
    """Return names from the local security-master snapshot only."""

    requested = list(dict.fromkeys(str(symbol).upper() for symbol in symbols))
    return cached_stock_names(requested, read_only=True)


def refresh_stock_names_if_needed(symbols: list[str]) -> dict[str, str]:
    """Worker-only name refresh; never use it from a Web read."""

    requested = list(dict.fromkeys(str(symbol).upper() for symbol in symbols))
    store = InstrumentStore()
    cached = store.names(requested)
    try:
        cached.update(refresh_stock_names(requested))
    except Exception as exc:
        store.update_sync_state("market:spot", status="error", error=str(exc))
        logger.warning("证券名称刷新失败，继续使用本地主数据: %s", exc)
    return cached


def save_stock_names(names: dict[str, str]) -> None:
    """把手工名称映射写入主数据；未知代码不会被虚构成可交易标的。"""
    store = InstrumentStore()
    records = []
    for symbol, name in names.items():
        current = store.get(symbol)
        if current and str(name).strip():
            value = current.to_dict()
            value.update({"name": str(name).strip(), "source": "manual", "source_priority": 20})
            records.append(value)
    store.upsert(records, source="manual", source_priority=20)
