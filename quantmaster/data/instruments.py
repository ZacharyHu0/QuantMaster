"""本地证券主数据、智能代码解析与离线搜索。

统一代码是 QuantMaster 内部唯一标识；数据源代码只保存在 ``provider_symbol``。
首次使用会把随安装包发布的压缩快照导入用户数据目录下的 SQLite。后续同步只
做 upsert，不删除旧名称与别名，因此上游短暂缺行不会让已经可用的标的消失。
"""

from __future__ import annotations

import gzip
import json
import logging
import re
import sqlite3
import threading
import time
import unicodedata
from collections.abc import Iterable
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime
from importlib import resources
from pathlib import Path
from typing import Any

from quantmaster.config import get_config
from quantmaster.runtime.sqlite import connect_sqlite
from quantmaster.trading_sessions import market_date

logger = logging.getLogger(__name__)

SNAPSHOT_NAME = "security_master.json.gz"
SCHEMA_VERSION = "1"
DOMESTIC_SUFFIXES = {"SH", "SZ", "BJ", "CSI"}
FOREIGN_SUFFIXES = {"HK", "US", "JP", "KR"}
SUPPORTED_ASSET_TYPES = {"stock", "etf", "index", "future", "fund"}


@dataclass(frozen=True)
class Instrument:
    symbol: str
    code: str
    name: str
    market: str
    exchange: str
    asset_type: str
    provider_symbol: str = ""
    full_name: str = ""
    en_name: str = ""
    pinyin: str = ""
    pinyin_initials: str = ""
    currency: str = ""
    status: str = "listed"
    source: str = "bundled"
    source_priority: int = 10
    list_date: str = ""
    delist_date: str = ""
    observed_at: float = 0.0
    bars_verified_at: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _normalized_alias(value: str) -> str:
    value = unicodedata.normalize("NFKC", str(value)).strip().casefold()
    return re.sub(r"[\s·]", "", value)


def _display_market(market: str) -> str:
    return {
        "CN": "中国内地", "HK": "中国香港", "US": "美国",
        "JP": "日本", "KR": "韩国", "FUT": "期货",
    }.get(market.upper(), market.upper())


def _seed_records() -> list[dict[str, Any]]:
    """快照损坏时仍能让演示候选和全球参考标的离线工作。"""
    from quantmaster.data.universe import DEMO_STOCK_NAMES
    from quantmaster.data.yfinance_source import GLOBAL_REFS

    rows: list[dict[str, Any]] = []
    for symbol, name in DEMO_STOCK_NAMES.items():
        code, suffix = symbol.split(".")
        rows.append({
            "symbol": symbol, "provider_symbol": symbol, "code": code,
            "name": name, "market": "CN", "exchange": suffix,
            "asset_type": "stock", "currency": "CNY", "source": "built_in",
        })
    for symbol, (provider, name) in GLOBAL_REFS.items():
        code, suffix = symbol.rsplit(".", 1)
        rows.append({
            "symbol": symbol, "provider_symbol": provider, "code": code,
            "name": name, "market": suffix, "exchange": suffix,
            "asset_type": "index" if provider.startswith("^") else "future",
            "source": "built_in",
        })
    rows.extend([
        {"symbol": "589160.SH", "code": "589160", "name": "广发上证科创板芯片ETF",
         "market": "CN", "exchange": "SH", "asset_type": "etf", "currency": "CNY"},
        {"symbol": "931743.CSI", "code": "931743", "name": "半导体材料设备",
         "market": "CN", "exchange": "CSI", "asset_type": "index", "currency": "CNY"},
        {"symbol": "950125.CSI", "code": "950125", "name": "科创半导体材料设备",
         "market": "CN", "exchange": "CSI", "asset_type": "index", "currency": "CNY"},
        {"symbol": "00700.HK", "code": "00700", "name": "腾讯控股",
         "en_name": "Tencent Holdings", "market": "HK", "exchange": "HKEX",
         "asset_type": "stock", "currency": "HKD", "provider_symbol": "0700.HK"},
        {"symbol": "AAPL.US", "code": "AAPL", "name": "Apple Inc.",
         "en_name": "Apple Inc.", "market": "US", "exchange": "NASDAQ",
         "asset_type": "stock", "currency": "USD", "provider_symbol": "AAPL"},
        {"symbol": "BRK.B.US", "code": "BRK.B", "name": "Berkshire Hathaway Inc.",
         "en_name": "Berkshire Hathaway Inc.", "market": "US", "exchange": "NYSE",
         "asset_type": "stock", "currency": "USD", "provider_symbol": "BRK-B"},
    ])
    return rows


class InstrumentStore:
    """线程安全的 SQLite 证券主数据仓库。"""

    def __init__(self, path: str | Path | None = None):
        self.path = Path(path) if path else get_config().data_root / "security_master.sqlite"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(
            self.path, policy="cache", timeout=20.0, row_factory=True,
        )

    @contextmanager
    def _connection(self):
        connection = self._connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._lock, self._connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS instruments (
                    symbol TEXT PRIMARY KEY,
                    provider_symbol TEXT NOT NULL DEFAULT '',
                    code TEXT NOT NULL,
                    name TEXT NOT NULL DEFAULT '',
                    full_name TEXT NOT NULL DEFAULT '',
                    en_name TEXT NOT NULL DEFAULT '',
                    pinyin TEXT NOT NULL DEFAULT '',
                    pinyin_initials TEXT NOT NULL DEFAULT '',
                    market TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    asset_type TEXT NOT NULL,
                    currency TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'listed',
                    list_date TEXT NOT NULL DEFAULT '',
                    delist_date TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT 'bundled',
                    source_priority INTEGER NOT NULL DEFAULT 10,
                    observed_at REAL NOT NULL DEFAULT 0,
                    bars_verified_at REAL NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_instruments_code ON instruments(code);
                CREATE INDEX IF NOT EXISTS idx_instruments_name ON instruments(name);
                CREATE TABLE IF NOT EXISTS aliases (
                    alias TEXT NOT NULL,
                    symbol TEXT NOT NULL REFERENCES instruments(symbol) ON DELETE CASCADE,
                    kind TEXT NOT NULL DEFAULT 'alias',
                    priority INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(alias, symbol, kind)
                );
                CREATE INDEX IF NOT EXISTS idx_aliases_prefix ON aliases(alias);
                CREATE TABLE IF NOT EXISTS sync_state (
                    source TEXT PRIMARY KEY,
                    last_success REAL NOT NULL DEFAULT 0,
                    last_attempt REAL NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'never',
                    record_count INTEGER NOT NULL DEFAULT 0,
                    error TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS resolution_history (
                    query TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    count INTEGER NOT NULL DEFAULT 1,
                    last_selected_at REAL NOT NULL,
                    PRIMARY KEY(query, symbol)
                );
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )
            connection.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?)",
                (SCHEMA_VERSION,),
            )
            count = connection.execute("SELECT COUNT(*) FROM instruments").fetchone()[0]
        if not count:
            self._import_bundled_snapshot()
        self._migrate_legacy_names()
        self._migrate_exchange_etfs()

    def _import_bundled_snapshot(self) -> None:
        records: list[dict[str, Any]] = []
        generated_at = time.time()
        try:
            asset = resources.files("quantmaster.data").joinpath(SNAPSHOT_NAME)
            with asset.open("rb") as raw, gzip.GzipFile(fileobj=raw) as compressed:
                payload = json.loads(compressed.read().decode("utf-8"))
            records = payload.get("instruments", payload) if isinstance(payload, dict) else payload
            if isinstance(payload, dict):
                generated_at = float(payload.get("generated_at") or generated_at)
        except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError) as exc:
            logger.warning("证券主数据快照不可用，使用内置最小集: %s", exc)
        imported = self.upsert(records or _seed_records(), source="bundled", source_priority=10)
        with self._connection() as connection:
            for source in ("tushare:catalog", "nasdaq:symbol_directory"):
                connection.execute(
                    """INSERT OR IGNORE INTO sync_state
                       (source,last_success,last_attempt,status,record_count,error)
                       VALUES(?,?,?,?,?,?)""",
                    (source, generated_at, generated_at, "bundled", imported, ""),
                )

    def _migrate_legacy_names(self) -> None:
        legacy = self.path.parent / "stock_names.json"
        if not legacy.is_file():
            return
        with self._connection() as connection:
            done = connection.execute(
                "SELECT value FROM meta WHERE key='legacy_names_migrated'"
            ).fetchone()
        if done:
            return
        try:
            payload = json.loads(legacy.read_text(encoding="utf-8"))
            names = payload.get("names", {}) if isinstance(payload, dict) else {}
        except (OSError, json.JSONDecodeError):
            names = {}
        with self._connection() as connection:
            for symbol, name in names.items():
                clean = str(name).strip()
                if clean:
                    connection.execute(
                        "UPDATE instruments SET name=CASE WHEN name='' THEN ? ELSE name END "
                        "WHERE symbol=?", (clean, str(symbol).upper()),
                    )
                    self._insert_alias(connection, clean, str(symbol).upper(), "legacy_name", 88)
            connection.execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES('legacy_names_migrated', ?)",
                (str(int(time.time())),),
            )

    def _migrate_exchange_etfs(self) -> None:
        """Repair old Tushare fund_basic imports that labeled listed ETFs as funds."""
        with self._connection() as connection:
            connection.execute(
                """UPDATE instruments SET asset_type='etf'
                   WHERE market='CN' AND exchange IN ('SH','SZ') AND asset_type='fund'
                     AND UPPER(name) LIKE '%ETF%' AND UPPER(name) NOT LIKE '%LOF%'
                     AND name NOT LIKE '%联接%'"""
            )

    @staticmethod
    def _coerce(record: Instrument | dict[str, Any], source: str, priority: int) -> dict[str, Any]:
        value = record.to_dict() if isinstance(record, Instrument) else dict(record)
        symbol = str(value.get("symbol", "")).strip().upper()
        if "." not in symbol:
            raise ValueError(f"证券代码缺少市场后缀: {symbol}")
        code, suffix = symbol.rsplit(".", 1)
        market = str(value.get("market") or ("CN" if suffix in DOMESTIC_SUFFIXES else suffix)).upper()
        return {
            "symbol": symbol,
            "provider_symbol": str(value.get("provider_symbol") or symbol),
            "code": str(value.get("code") or code).upper(),
            "name": str(value.get("name") or "").strip(),
            "full_name": str(value.get("full_name") or "").strip(),
            "en_name": str(value.get("en_name") or "").strip(),
            "pinyin": str(value.get("pinyin") or "").strip().lower(),
            "pinyin_initials": str(value.get("pinyin_initials") or "").strip().lower(),
            "market": market,
            "exchange": str(value.get("exchange") or suffix).upper(),
            "asset_type": str(value.get("asset_type") or "stock").lower(),
            "currency": str(value.get("currency") or "").upper(),
            "status": str(value.get("status") or "listed").lower(),
            "list_date": str(value.get("list_date") or ""),
            "delist_date": str(value.get("delist_date") or ""),
            "source": str(value.get("source") or source),
            "source_priority": int(value.get("source_priority") or priority),
            "observed_at": float(value.get("observed_at") or time.time()),
            "bars_verified_at": float(value.get("bars_verified_at") or 0),
        }

    def upsert(
        self, records: Iterable[Instrument | dict[str, Any]], *,
        source: str = "online", source_priority: int = 50,
    ) -> int:
        values = []
        for record in records:
            try:
                values.append(self._coerce(record, source, source_priority))
            except (TypeError, ValueError) as exc:
                logger.debug("忽略无效证券主数据: %s", exc)
        if not values:
            return 0
        columns = tuple(values[0])
        placeholders = ",".join("?" for _ in columns)
        updates = []
        for column in columns:
            if column == "symbol":
                continue
            if column == "source_priority":
                updates.append(
                    "source_priority=MAX(instruments.source_priority,excluded.source_priority)"
                )
            elif column == "bars_verified_at":
                updates.append(
                    "bars_verified_at=MAX(instruments.bars_verified_at,excluded.bars_verified_at)"
                )
            elif column == "observed_at":
                updates.append(
                    "observed_at=MAX(instruments.observed_at,excluded.observed_at)"
                )
            else:
                updates.append(
                    f"{column}=CASE WHEN excluded.source_priority >= instruments.source_priority "
                    f"AND excluded.{column}<>'' THEN excluded.{column} ELSE instruments.{column} END"
                )
        sql = (
            f"INSERT INTO instruments({','.join(columns)}) VALUES({placeholders}) "
            f"ON CONFLICT(symbol) DO UPDATE SET {','.join(updates)}"
        )
        with self._lock, self._connection() as connection:
            for value in values:
                connection.execute(sql, tuple(value[column] for column in columns))
                self._replace_generated_aliases(connection, value)
        return len(values)

    @staticmethod
    def _insert_alias(
        connection: sqlite3.Connection, alias: str, symbol: str, kind: str, priority: int,
    ) -> None:
        normalized = _normalized_alias(alias)
        if normalized:
            connection.execute(
                "INSERT OR IGNORE INTO aliases(alias,symbol,kind,priority) VALUES(?,?,?,?)",
                (normalized, symbol, kind, priority),
            )

    def _replace_generated_aliases(self, connection: sqlite3.Connection, value: dict[str, Any]) -> None:
        symbol, code = value["symbol"], value["code"]
        suffix = symbol.rsplit(".", 1)[-1]
        aliases: list[tuple[str, str, int]] = [
            (symbol, "canonical", 100), (value["provider_symbol"], "provider", 98),
            (code, "code", 92),
        ]
        if code.isdigit():
            aliases.append((code.lstrip("0") or "0", "short_code", 84))
        exchange_prefixes = {
            "SH": ("sh", "sse"), "SZ": ("sz", "szse"), "BJ": ("bj", "bse"),
            "CSI": ("csi",), "HK": ("hk", "hkex"), "US": ("us",),
            "JP": ("jp",), "KR": ("kr",),
        }.get(suffix, (suffix.lower(),))
        for prefix in exchange_prefixes:
            aliases.extend([
                (f"{prefix}:{code}", "qualified", 99),
                (f"{prefix}{code}", "qualified", 97),
                (f"{code}.{prefix}", "qualified", 97),
            ])
        if suffix == "SH":
            aliases.extend([(f"{code}.ss", "qualified", 99), (f"ss{code}", "qualified", 96)])
        if suffix == "HK" and code.isdigit():
            short = code.lstrip("0") or "0"
            aliases.extend([(f"hk:{short}", "qualified", 99), (f"{short}.hk", "qualified", 99)])
        exchange = value["exchange"].lower()
        if exchange and exchange not in exchange_prefixes:
            aliases.extend([
                (f"{exchange}:{code}", "qualified", 99),
                (f"{code}.{exchange}", "qualified", 97),
            ])
        for field, kind, priority in (
            (value["name"], "name", 96), (value["full_name"], "full_name", 94),
            (value["en_name"], "en_name", 94), (value["pinyin"], "pinyin", 90),
            (value["pinyin_initials"], "pinyin_initials", 88),
        ):
            if field:
                aliases.append((field, kind, priority))
        if value["en_name"]:
            primary_word = re.split(r"[^A-Za-z0-9]+", value["en_name"].strip(), maxsplit=1)[0]
            if len(primary_word) >= 3:
                aliases.append((primary_word, "en_name_short", 86))
        for alias, kind, priority in aliases:
            self._insert_alias(connection, alias, symbol, kind, priority)

    @staticmethod
    def _instrument(row: sqlite3.Row | None) -> Instrument | None:
        return Instrument(**dict(row)) if row else None

    def get(self, symbol: str) -> Instrument | None:
        canonical = str(symbol).strip().upper()
        with self._connection() as connection:
            return self._instrument(connection.execute(
                "SELECT * FROM instruments WHERE symbol=?", (canonical,)
            ).fetchone())

    def get_many(self, symbols: Iterable[str]) -> dict[str, Instrument]:
        """批量读取规范代码，避免候选加载时为每只证券重复打开数据库。"""
        requested = [str(item).strip().upper() for item in dict.fromkeys(symbols)]
        if not requested:
            return {}
        result: dict[str, Instrument] = {}
        with self._connection() as connection:
            for offset in range(0, len(requested), 800):
                batch = requested[offset:offset + 800]
                rows = connection.execute(
                    f"SELECT * FROM instruments WHERE symbol IN "
                    f"({','.join('?' for _ in batch)})", batch,
                ).fetchall()
                for row in rows:
                    instrument = self._instrument(row)
                    if instrument is not None:
                        result[instrument.symbol] = instrument
        return result

    def list(
        self, *, market: str = "", asset_type: str = "", status: str = "",
    ) -> list[Instrument]:
        """按稳定代码顺序枚举证券，供全市场本地研究任务使用。"""
        clauses: list[str] = []
        params: list[str] = []
        for column, raw in (
            ("market", market), ("asset_type", asset_type), ("status", status),
        ):
            value = str(raw).strip()
            if value:
                clauses.append(f"{column}=?")
                params.append(value.upper() if column == "market" else value.lower())
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connection() as connection:
            rows = connection.execute(
                f"SELECT * FROM instruments{where} ORDER BY symbol", params,
            ).fetchall()
        return [Instrument(**dict(row)) for row in rows]

    def names(self, symbols: Iterable[str]) -> dict[str, str]:
        requested = [str(item).upper() for item in dict.fromkeys(symbols)]
        if not requested:
            return {}
        result: dict[str, str] = {}
        with self._connection() as connection:
            for offset in range(0, len(requested), 800):
                batch = requested[offset:offset + 800]
                rows = connection.execute(
                    f"SELECT symbol,name FROM instruments WHERE symbol IN "
                    f"({','.join('?' for _ in batch)})", batch,
                ).fetchall()
                result.update({row["symbol"]: row["name"] for row in rows if row["name"]})
        return result

    @staticmethod
    def _public(instrument: Instrument, *, match_kind: str = "", score: float = 0) -> dict[str, Any]:
        data = instrument.to_dict()
        data.update({
            "market_label": _display_market(instrument.market),
            "match_kind": match_kind, "score": round(score, 3),
        })
        return data

    def search(self, query: str, *, limit: int = 20) -> list[dict[str, Any]]:
        normalized = _normalized_alias(query)
        if not normalized:
            return []
        limit = max(1, min(int(limit), 50))
        with self._connection() as connection:
            history = {
                row["symbol"]: (row["count"], row["last_selected_at"])
                for row in connection.execute(
                    "SELECT symbol,count,last_selected_at FROM resolution_history WHERE query=?",
                    (normalized,),
                )
            }
            rows = connection.execute(
                """
                SELECT i.*, a.kind AS match_kind, a.priority AS alias_priority,
                       CASE WHEN a.alias=? THEN 0 WHEN a.alias LIKE ? ESCAPE '\\' THEN 1 ELSE 2 END AS tier
                FROM aliases a JOIN instruments i ON i.symbol=a.symbol
                WHERE a.alias=? OR a.alias LIKE ? ESCAPE '\\' OR a.alias LIKE ? ESCAPE '\\'
                ORDER BY tier, a.priority DESC, i.source_priority DESC, i.symbol
                LIMIT 500
                """,
                (normalized, self._like(normalized) + "%", normalized,
                 self._like(normalized) + "%", "%" + self._like(normalized) + "%"),
            ).fetchall()
        ranked: dict[str, tuple[tuple[Any, ...], sqlite3.Row]] = {}
        for row in rows:
            selected_count, selected_at = history.get(row["symbol"], (0, 0))
            status_rank = 0 if row["status"] in {"listed", "active", "l"} else 1
            key = (
                int(row["tier"]), -int(row["alias_priority"]), status_rank,
                -int(selected_count), -float(selected_at), -int(row["source_priority"]), row["symbol"],
            )
            if row["symbol"] not in ranked or key < ranked[row["symbol"]][0]:
                ranked[row["symbol"]] = (key, row)
        result = []
        for key, row in sorted(ranked.values(), key=lambda item: item[0])[:limit]:
            instrument = Instrument(**{field: row[field] for field in Instrument.__dataclass_fields__})
            score = 100 - key[0] * 20 + int(row["alias_priority"]) / 100
            result.append(self._public(instrument, match_kind=row["match_kind"], score=score))
        return result

    @staticmethod
    def _like(value: str) -> str:
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    def resolve(self, query: str, *, selected_symbol: str = "") -> dict[str, Any]:
        raw = str(query).strip()
        normalized = _normalized_alias(raw)
        if not normalized:
            return {"query": raw, "status": "unresolved", "candidates": [], "message": "请输入代码或名称"}
        candidates = self.search(raw, limit=20)
        if selected_symbol:
            selected = str(selected_symbol).upper()
            match = next((item for item in candidates if item["symbol"] == selected), None)
            if match is None:
                instrument = self.get(selected)
                match = self._public(instrument) if instrument else None
            if match is None:
                return {"query": raw, "status": "unresolved", "candidates": candidates,
                        "message": "所选标的不在证券主数据中"}
            self.remember(raw, selected)
            return {"query": raw, "status": "resolved", "instrument": match,
                    "candidates": candidates, "corrected": selected != raw.upper()}
        exact = [item for item in candidates if item["score"] >= 100]
        if len(exact) == 1:
            return {"query": raw, "status": "resolved", "instrument": exact[0],
                    "candidates": exact, "corrected": exact[0]["symbol"] != raw.upper()}
        if len(exact) > 1:
            if re.fullmatch(r"\d{6}", normalized):
                active_stocks = [
                    item for item in exact
                    if item["asset_type"] == "stock"
                    and item["symbol"].rsplit(".", 1)[-1] in {"SH", "SZ", "BJ"}
                    and item["status"] in {"listed", "active", "l"}
                ]
                if len(active_stocks) == 1:
                    return {
                        "query": raw, "status": "resolved", "instrument": active_stocks[0],
                        "candidates": exact, "corrected": active_stocks[0]["symbol"] != raw.upper(),
                    }
            return {"query": raw, "status": "ambiguous", "candidates": exact,
                    "message": f"{raw} 对应多个市场，请选择具体标的"}
        if candidates:
            if len(normalized) < 4 or candidates[0]["score"] < 80:
                return {"query": raw, "status": "unresolved", "candidates": candidates,
                        "message": f"未找到可直接确认的 {raw}；请补充代码或市场"}
            return {"query": raw, "status": "ambiguous", "candidates": candidates,
                    "message": "找到相近标的，请确认后使用"}
        return {"query": raw, "status": "unresolved", "candidates": [],
                "message": f"未找到 {raw}；请检查代码、市场或名称"}

    def resolve_many(
        self, queries: Iterable[str], selections: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        selections = selections or {}
        resolved, ambiguous, unresolved, corrections = [], [], [], []
        seen: set[str] = set()
        duplicates: list[dict[str, str]] = []
        for query in queries:
            raw = str(query).strip()
            if not raw:
                continue
            selection = selections.get(raw) or selections.get(_normalized_alias(raw), "")
            item = self.resolve(raw, selected_symbol=selection)
            if item["status"] == "resolved":
                symbol = item["instrument"]["symbol"]
                if symbol in seen:
                    duplicates.append({"value": raw, "symbol": symbol})
                    continue
                seen.add(symbol)
                resolved.append(item)
                if item.get("corrected"):
                    corrections.append({"value": raw, "symbol": symbol})
            elif item["status"] == "ambiguous":
                ambiguous.append(item)
            else:
                unresolved.append(item)
        return {
            "status": "ok" if not ambiguous and not unresolved else "needs_confirmation",
            "resolved": resolved, "ambiguous": ambiguous, "unresolved": unresolved,
            "corrections": corrections, "duplicates": duplicates,
        }

    def remember(self, query: str, symbol: str) -> None:
        normalized = _normalized_alias(query)
        with self._connection() as connection:
            connection.execute(
                """INSERT INTO resolution_history(query,symbol,count,last_selected_at)
                   VALUES(?,?,1,?) ON CONFLICT(query,symbol) DO UPDATE SET
                   count=count+1,last_selected_at=excluded.last_selected_at""",
                (normalized, symbol.upper(), time.time()),
            )

    def mark_bars_verified(self, symbol: str, verified_at: float | None = None) -> None:
        with self._connection() as connection:
            connection.execute(
                "UPDATE instruments SET bars_verified_at=? WHERE symbol=?",
                (verified_at or time.time(), symbol.upper()),
            )

    def update_sync_state(
        self, source: str, *, status: str, record_count: int = 0, error: str = "",
    ) -> None:
        now = time.time()
        with self._connection() as connection:
            connection.execute(
                """INSERT INTO sync_state(source,last_success,last_attempt,status,record_count,error)
                   VALUES(?,?,?,?,?,?) ON CONFLICT(source) DO UPDATE SET
                   last_success=CASE WHEN excluded.status='success' THEN excluded.last_attempt
                                     ELSE sync_state.last_success END,
                   last_attempt=excluded.last_attempt,status=excluded.status,
                   record_count=excluded.record_count,error=excluded.error""",
                (source, now if status == "success" else 0, now, status, record_count, error[:500]),
            )

    def sync_due(self, source: str, max_age_seconds: float) -> bool:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT last_success FROM sync_state WHERE source=?", (source,)
            ).fetchone()
        return not row or float(row["last_success"] or 0) < time.time() - max_age_seconds

    def diagnostics(self) -> dict[str, Any]:
        with self._connection() as connection:
            total = connection.execute("SELECT COUNT(*) FROM instruments").fetchone()[0]
            groups = connection.execute(
                "SELECT market,asset_type,COUNT(*) AS count FROM instruments "
                "GROUP BY market,asset_type ORDER BY market,asset_type"
            ).fetchall()
            states = connection.execute(
                "SELECT * FROM sync_state ORDER BY source"
            ).fetchall()
            newest = connection.execute("SELECT MAX(observed_at) FROM instruments").fetchone()[0] or 0
        return {
            "status": "success" if total else "error", "path": str(self.path),
            "record_count": total, "updated_at": newest,
            "coverage": [dict(row) for row in groups],
            "sources": [dict(row) for row in states],
        }


def _online_yahoo_records(query: str, limit: int) -> list[dict[str, Any]]:
    """按需补齐日/韩等未随包发布的市场；失败不影响本地搜索。"""
    from quantmaster.data.resilience import provider_call
    from quantmaster.data.yfinance_source import _require_yfinance

    requested = re.sub(r"^(TYO|TSE|JP|KRX|KOSPI|KOSDAQ):", "", query.strip(), flags=re.I)
    key = f"lookup:{_normalized_alias(requested)}:{limit}"

    def fetch():
        result = _require_yfinance().Search(
            requested, max_results=max(5, limit), news_count=0,
            enable_fuzzy_query=False, raise_errors=True,
        )
        return result.quotes

    quotes = provider_call("yahoo-search", key, fetch)
    records = []
    for quote in quotes or []:
        provider = str(quote.get("symbol") or "").upper()
        if not provider:
            continue
        exchange = str(quote.get("exchange") or quote.get("exchDisp") or "").upper()
        if provider.endswith(".HK"):
            canonical = f"{provider[:-3].zfill(5)}.HK"
            market, currency = "HK", "HKD"
        elif provider.endswith(".T"):
            canonical = f"{provider[:-2]}.JP"
            market, currency = "JP", "JPY"
        elif provider.endswith((".KS", ".KQ")):
            canonical = f"{provider.rsplit('.', 1)[0]}.KR"
            market, currency = "KR", "KRW"
        elif provider.startswith("^"):
            canonical = f"{provider}.US"
            market, currency = "US", "USD"
        elif "." not in provider or exchange in {
            "NMS", "NGM", "NCM", "NYQ", "ASE", "PCX", "BTS", "NASDAQ", "NYSE",
        }:
            canonical = f"{provider.replace('-', '.')}.US"
            market, currency = "US", "USD"
        else:
            continue
        quote_type = str(quote.get("quoteType") or "EQUITY").upper()
        asset_type = {"ETF": "etf", "INDEX": "index", "MUTUALFUND": "fund"}.get(
            quote_type, "stock"
        )
        name = str(quote.get("longname") or quote.get("shortname") or provider).strip()
        records.append({
            "symbol": canonical, "provider_symbol": provider,
            "code": canonical.rsplit(".", 1)[0], "name": name, "en_name": name,
            "market": market, "exchange": exchange or market,
            "asset_type": asset_type, "currency": currency, "status": "listed",
            "source": "yahoo:search", "source_priority": 30,
        })
    return records


def search_instruments(
    query: str, limit: int = 20, *, online: bool = True,
) -> list[dict[str, Any]]:
    store = InstrumentStore()
    local = store.search(query, limit=limit)
    if local or not online or not str(query).strip():
        return local
    try:
        records = _online_yahoo_records(query, limit)
        store.upsert(records, source="yahoo:search", source_priority=30)
        store.update_sync_state("yahoo:search", status="success", record_count=len(records))
    except Exception as exc:  # 联网补全是可选层，本地结果永远可继续使用
        store.update_sync_state("yahoo:search", status="error", error=str(exc))
        logger.debug("Yahoo 证券检索失败: %s", exc)
        return local
    return store.search(query, limit=limit)


def resolve_instrument(
    query: str, selected_symbol: str = "", *, online: bool = True,
) -> dict[str, Any]:
    store = InstrumentStore()
    result = store.resolve(query, selected_symbol=selected_symbol)
    if result["status"] == "unresolved" and online:
        search_instruments(query, online=True)
        result = store.resolve(query, selected_symbol=selected_symbol)
    return result


def resolve_instruments(
    queries: Iterable[str], selections: dict[str, str] | None = None, *, online: bool = True,
) -> dict[str, Any]:
    values = list(queries)
    store = InstrumentStore()
    result = store.resolve_many(values, selections=selections)
    if online and result["unresolved"]:
        for item in result["unresolved"]:
            query = item["query"]
            if re.search(r"[A-Za-z]", query):
                search_instruments(query, online=True)
        result = store.resolve_many(values, selections=selections)
    return result


def instrument_diagnostics() -> dict[str, Any]:
    return InstrumentStore().diagnostics()


def refresh_authoritative_instrument_catalog(
    *, source: Any | None = None, store: InstrumentStore | None = None,
) -> dict[str, Any]:
    """Fetch, freeze, and project one authoritative Tushare catalog observation."""
    selected_store = store or InstrumentStore()
    if source is None:
        from quantmaster.data.tushare_source import TushareSource

        source = TushareSource()
    from quantmaster.data.instrument_snapshots import (
        TUSHARE_CATALOG_QUERY,
        freeze_instrument_catalog,
    )
    from quantmaster.data.resilience import bypass_endpoint_cache

    with bypass_endpoint_cache():
        fetched = source.instrument_catalog()
    try:
        records, request_outcomes = fetched
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            "Tushare 证券目录缺少逐子请求完整性证据"
        ) from exc
    snapshot = freeze_instrument_catalog(
        records,
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=request_outcomes,
    )
    observed_at = datetime.fromisoformat(snapshot.acquired_at).timestamp()
    observed_records = [
        {
            **dict(item),
            "source": "tushare:catalog",
            "observed_at": observed_at,
        }
        for item in records
    ]
    count = selected_store.upsert(
        observed_records,
        source="tushare:catalog",
        source_priority=40,
    )
    selected_store.update_sync_state(
        "tushare:catalog", status="success", record_count=count,
    )
    return {
        "status": "success",
        "record_count": count,
        "snapshot_id": snapshot.snapshot_id,
        "acquired_at": snapshot.acquired_at,
    }


def _nasdaq_directory_records() -> list[dict[str, Any]]:
    import httpx

    from quantmaster.data.resilience import provider_call

    sources = (
        ("https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt", "NASDAQ", "Symbol"),
        ("https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt", "", "ACT Symbol"),
    )
    exchange_names = {"A": "NYSE American", "N": "NYSE", "P": "NYSE Arca", "Z": "Cboe"}
    records = []
    for url, default_exchange, symbol_field in sources:
        response = provider_call(
            "nasdaq-directory", url,
            lambda target=url: httpx.get(target, timeout=30, follow_redirects=True),
        )
        response.raise_for_status()
        lines = [line.strip() for line in response.text.splitlines() if line.strip()]
        headers = lines[0].split("|")
        for line in lines[1:]:
            if line.startswith("File Creation Time"):
                continue
            row = dict(zip(headers, line.split("|"), strict=False))
            ticker = str(row.get(symbol_field) or "").strip().upper()
            name = str(row.get("Security Name") or "").strip()
            if not ticker or not name or row.get("Test Issue") == "Y":
                continue
            exchange = default_exchange or exchange_names.get(str(row.get("Exchange")), "US")
            records.append({
                "symbol": f"{ticker}.US", "provider_symbol": ticker.replace(".", "-"),
                "code": ticker, "name": name, "en_name": name, "market": "US",
                "exchange": exchange, "asset_type": "etf" if row.get("ETF") == "Y" else "stock",
                "currency": "USD", "status": "listed", "source": "nasdaq:symbol_directory",
                "source_priority": 40,
            })
    return records


def refresh_instrument_master(*, force: bool = False) -> dict[str, Any]:
    """独立刷新 Tushare 和 Nasdaq 目录；任一失败都不会影响另一个或旧快照。"""
    store = InstrumentStore()
    states: dict[str, Any] = {}
    jobs = {
        "tushare:catalog": refresh_authoritative_instrument_catalog,
        "nasdaq:symbol_directory": _nasdaq_directory_records,
    }
    for source, fetch in jobs.items():
        max_age = 86400 if source == "tushare:catalog" else 7 * 86400
        due = store.sync_due(source, max_age)
        if source == "tushare:catalog":
            from quantmaster.trading_sessions import daily_signal_cutoff, market_now

            current = market_now()
            if current >= daily_signal_cutoff(current.date()):
                try:
                    from quantmaster.data.instrument_snapshots import (
                        load_instrument_catalog_snapshot,
                    )

                    load_instrument_catalog_snapshot(
                        as_of=current.date().isoformat(), market="CN", asset_type="stock",
                    )
                except (OSError, RuntimeError, TypeError, ValueError):
                    due = True
        if not force and not due:
            states[source] = {"status": "fresh"}
            continue
        try:
            if source == "tushare:catalog":
                states[source] = fetch(store=store)
                continue
            records = fetch()
            count = store.upsert(records, source=source, source_priority=40)
            store.update_sync_state(source, status="success", record_count=count)
            states[source] = {"status": "success", "record_count": count}
        except Exception as exc:
            store.update_sync_state(source, status="error", error=str(exc))
            states[source] = {"status": "error", "message": str(exc)}
            logger.debug("证券主数据源 %s 刷新失败: %s", source, exc)
    return {"sources": states, "diagnostics": store.diagnostics()}


def validate_bar_capability(symbol: str, *, verify_foreign: bool = True) -> Instrument:
    """候选保存前验证日线能力；海外标的首次使用会读取缓存或做短区间探测。"""
    store = InstrumentStore()
    instrument = store.get(symbol)
    if instrument is None:
        raise ValueError(f"证券主数据中不存在 {symbol}")
    suffix = instrument.symbol.rsplit(".", 1)[-1]
    if instrument.asset_type not in SUPPORTED_ASSET_TYPES:
        raise ValueError(f"{symbol} 的品种类型 {instrument.asset_type} 暂不支持日线")
    if suffix in DOMESTIC_SUFFIXES or instrument.asset_type == "future":
        return instrument
    if suffix not in FOREIGN_SUFFIXES:
        raise ValueError(f"{symbol} 暂无可用日线数据路由")
    if not verify_foreign or instrument.bars_verified_at > time.time() - 30 * 86400:
        return instrument
    from datetime import timedelta

    from quantmaster.data.registry import load_history

    end = market_date()
    start = end - timedelta(days=21)
    try:
        market_envelope = load_history(
            instrument.symbol, start.isoformat(), end.isoformat(),
        )
        bars = market_envelope.require_data()
    except Exception as exc:
        raise ValueError(f"{symbol} 尚未验证到可用日线: {exc}") from None
    if bars is None or bars.empty:
        raise ValueError(f"{symbol} 尚未验证到可用日线")
    if market_envelope.quality.status != "verified":
        raise ValueError(
            f"{symbol} 日线证据未验证：" + "；".join(market_envelope.quality.issues)
        )
    store.mark_bars_verified(symbol)
    return store.get(symbol) or instrument
