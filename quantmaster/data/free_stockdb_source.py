"""free-stockdb 本地数据源适配器。

优先复用用户安装的 ``stock_sdk.StockDBClient`` 及其本地数据内容；未安装
SDK 时才兼容公开 HTTP 查询协议。QuantMaster 不分发 free-stockdb 程序、
数据库或上游同步地址。
"""

from __future__ import annotations

import bisect
import hashlib
import importlib
import importlib.util
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import httpx
import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.base import (
    OHLCV_COLUMNS,
    DataCapability,
    DataSource,
    Market,
    normalize_bars,
    validate_frequency,
)
from quantmaster.data.resilience import provider_call

_BOARD_CATEGORIES = {
    0: "概念",
    1: "申万一级",
    2: "申万二级",
    3: "申万三级",
}


def _compact_time(value: str | None, *, intraday: bool) -> str:
    if not value:
        return ""
    stamp = pd.Timestamp(value)
    return stamp.strftime("%Y%m%d%H%M%S" if intraday else "%Y%m%d")


def _canonical_cn_symbol(value: Any) -> str:
    code = str(value or "").strip().upper().partition(".")[0].zfill(6)
    if len(code) != 6 or not code.isdigit():
        return ""
    suffix = (
        "BJ" if code.startswith(("4", "8", "920"))
        else "SH" if code.startswith(("6", "9"))
        else "SZ"
    )
    return f"{code}.{suffix}"


class FreeStockDBSource(DataSource):
    """Read A-share bars and board metadata from user-managed free-stockdb."""

    name = "free-stockdb"
    markets = (Market.CN,)
    capabilities = frozenset({
        DataCapability.DAILY,
        DataCapability.INTRADAY,
        DataCapability.SPOT,
        DataCapability.INDUSTRY,
        DataCapability.THEMES,
    })

    def __init__(
        self,
        base_url: str | None = None,
        timeout: float | None = None,
        sdk_path: str | None = None,
    ):
        cfg = get_config().data
        self.base_url = (base_url or cfg.free_stockdb_url).strip().rstrip("/") + "/"
        self.timeout = float(timeout if timeout is not None else cfg.free_stockdb_timeout)
        self.sdk_path = str(
            sdk_path if sdk_path is not None else cfg.free_stockdb_sdk_path
        ).strip()
        self._trust_env = urlparse(self.base_url).hostname not in {
            "127.0.0.1", "localhost", "::1",
        }
        self._sdk_checked = False
        self._client: Any | None = None
        self._sdk_error: BaseException | None = None

    def _load_sdk_module(self):
        if not self.sdk_path:
            return importlib.import_module("stock_sdk")
        configured = Path(self.sdk_path).expanduser().resolve()
        sdk_file = configured if configured.is_file() else configured / "stock_sdk.py"
        if not sdk_file.is_file():
            raise ModuleNotFoundError(f"未找到 free-stockdb SDK：{sdk_file}")
        directory = str(sdk_file.parent)
        if directory not in sys.path:
            # stock_sdk 会继续加载同目录的 stockdb 原生模块；保留该显式用户路径。
            sys.path.insert(0, directory)
        digest = hashlib.sha256(str(sdk_file).encode("utf-8")).hexdigest()[:12]
        module_name = f"_quantmaster_free_stockdb_{digest}"
        cached = sys.modules.get(module_name)
        if cached is not None:
            return cached
        spec = importlib.util.spec_from_file_location(module_name, sdk_file)
        if spec is None or spec.loader is None:
            raise ImportError(f"无法加载 free-stockdb SDK：{sdk_file}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except (ImportError, OSError, AttributeError, RuntimeError):
            sys.modules.pop(module_name, None)
            raise
        return module

    def _sdk_client(self):
        if self._sdk_checked:
            return self._client
        self._sdk_checked = True
        try:
            module = self._load_sdk_module()
            client_class = module.StockDBClient
            parsed = urlparse(self.base_url)
            host = parsed.hostname or "127.0.0.1"
            port = parsed.port or (443 if parsed.scheme == "https" else 7899)
            self._client = client_class(host=host, port=port, password="")
        except (ImportError, OSError, AttributeError, RuntimeError, TypeError, ValueError) as exc:
            # SDK 是用户安装的可选能力；加载失败时仍允许兼容 HTTP 服务或其他源。
            self._sdk_error = exc
            self._client = None
        return self._client

    def _sdk_data(
        self,
        code: str | list[str],
        start: str | None,
        end: str | None,
        frequency: str,
        *,
        fq: str | None,
        probe: bool = False,
    ):
        client = self._sdk_client()
        if client is None:
            return None
        key = json.dumps(
            {
                "sdk": True,
                "code": code,
                "start": start,
                "end": end,
                "frequency": frequency,
                "fq": fq,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return provider_call(
            "free-stockdb",
            key,
            lambda: client.get_data(
                code=code,
                start=start or None,
                end=end or None,
                frequency=frequency,
                fq=fq,
                as_df=False,
            ),
            probe=probe,
        )

    def _request(self, params: dict[str, str], *, probe: bool = False):
        key = json.dumps(params, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

        def fetch():
            with httpx.Client(
                timeout=self.timeout,
                follow_redirects=False,
                trust_env=self._trust_env,
            ) as client:
                response = client.get(self.base_url, params=params)
                response.raise_for_status()
                return response.json()

        return provider_call("free-stockdb", key, fetch, probe=probe)

    @staticmethod
    def _records(payload) -> list[dict]:
        if isinstance(payload, dict):
            return [dict(payload)] if "close" in payload else []
        if not isinstance(payload, list):
            return []
        records: list[dict] = []
        for item in payload:
            key = ""
            value = item
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                key, value = str(item[0]), item[1]
            if isinstance(value, str):
                try:
                    value = json.loads(value)
                except json.JSONDecodeError:
                    continue
            if not isinstance(value, dict):
                continue
            record = dict(value)
            if not record.get("date") and key:
                record["date"] = key.rsplit(":", 1)[-1]
            records.append(record)
        return records

    def _query_http(
        self,
        table: str,
        code: str,
        start: str = "",
        end: str = "",
        *,
        raw_fallback: bool = True,
    ) -> list[dict]:
        if start and end and start != end:
            range_query = f"fwd:{start},{end}"
        elif start:
            range_query = f"key:{start}"
        else:
            range_query = "all:"
        modern = self._records(self._request({
            "cmd": "vals",
            "t": table,
            "k1": f"key:{code}",
            "k2": range_query,
        }))
        if modern or not raw_fallback:
            return modern
        if "*" in code:
            return []
        return self._records(self._request({"cmd": "get", "t": f"{table}:{code}:*"}))

    @staticmethod
    def _apply_qfq(records: list[dict], factors: list[dict], code: str) -> list[dict]:
        points = []
        for item in factors:
            try:
                if item.get("date") and item.get("cum") is not None:
                    points.append((str(item["date"])[:8], float(item["cum"])))
            except (TypeError, ValueError):
                continue
        points.sort(key=lambda item: item[0])
        if not points:
            return records
        dates = [item[0] for item in points]
        values = [item[1] for item in points]
        latest = values[-1]
        decimals = 3 if code.startswith(("1", "5")) else 2
        adjusted: list[dict] = []
        for value in records:
            item = dict(value)
            record_date = str(item.get("date") or "")[:8]
            index = bisect.bisect_right(dates, record_date) - 1
            current = values[index] if index >= 0 else 1.0
            if latest and current and abs(current - latest) > 1e-9:
                scale = current / latest
                for field in ("open", "high", "low", "close", "pre_close"):
                    if item.get(field) is not None:
                        item[field] = round(float(item[field]) * scale, decimals)
            adjusted.append(item)
        return adjusted

    @staticmethod
    def _frame(records: list[dict], *, intraday: bool) -> pd.DataFrame:
        if not records:
            return pd.DataFrame(
                columns=OHLCV_COLUMNS,
                index=pd.DatetimeIndex([], name="date"),
            )
        frame = pd.DataFrame(records)
        if "date" not in frame or not set(OHLCV_COLUMNS).issubset(frame.columns):
            return pd.DataFrame(
                columns=OHLCV_COLUMNS,
                index=pd.DatetimeIndex([], name="date"),
            )
        digits = frame["date"].astype(str).str.replace(r"\D", "", regex=True)
        width = 14 if intraday else 8
        frame["date"] = pd.to_datetime(
            digits.str[:width],
            format="%Y%m%d%H%M%S" if intraday else "%Y%m%d",
            errors="coerce",
        )
        frame = frame.dropna(subset=["date"])
        for column in (*OHLCV_COLUMNS, "amount", "turnover"):
            if column in frame:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        return normalize_bars(frame)

    def daily(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        code = symbol.partition(".")[0].zfill(6)
        begin = _compact_time(start, intraday=False)
        finish = _compact_time(end, intraday=False)
        payload = self._sdk_data(code, begin, finish, "1d", fq="qfq")
        if payload is None:
            records = self._query_http("日k", code, begin, finish)
            factors = self._query_http("复权", code)
            records = self._apply_qfq(records, factors, code)
        else:
            records = self._records(payload)
        return self._frame(records, intraday=False).loc[start:end]

    def intraday(
        self, symbol: str, start: str, end: str, frequency: str = "5m",
    ) -> pd.DataFrame:
        frequency = validate_frequency(frequency)
        if frequency == "1d":
            return self.daily(symbol, start, end)
        code = symbol.partition(".")[0].zfill(6)
        begin = _compact_time(start, intraday=True)
        finish = _compact_time(end, intraday=True)
        payload = self._sdk_data(code, begin, finish, frequency, fq=None)
        if payload is not None:
            return self._frame(self._records(payload), intraday=True).loc[start:end]
        records = self._query_http("分钟k", code, begin, finish)
        frame = self._frame(records, intraday=True).loc[start:end]
        if frame.empty or frequency == "1m":
            return frame
        aggregation = {
            "open": "first", "high": "max", "low": "min", "close": "last",
            "volume": "sum",
        }
        if "amount" in frame:
            aggregation["amount"] = "sum"
        return frame.resample(frequency).agg(aggregation).dropna(subset=["close"])

    @staticmethod
    def _flatten_batch(payload) -> list[dict]:
        if not isinstance(payload, dict):
            return FreeStockDBSource._records(payload)
        records: list[dict] = []
        for code, values in payload.items():
            for item in FreeStockDBSource._records(values):
                item.setdefault("code", str(code))
                records.append(item)
        return records

    def spot(self, symbols: list[str]) -> pd.DataFrame:
        requested = {symbol.partition(".")[0].zfill(6) for symbol in symbols}
        now = datetime.now(ZoneInfo("Asia/Shanghai"))
        records: list[dict] = []
        if self._sdk_client() is not None:
            for offset in range(10):
                trading_date = (now - timedelta(days=offset)).strftime("%Y%m%d")
                payload = self._sdk_data(
                    sorted(requested), trading_date, trading_date, "1d", fq=None,
                )
                records = self._flatten_batch(payload)
                if records:
                    break
        else:
            for offset in range(10):
                trading_date = (now - timedelta(days=offset)).strftime("%Y%m%d")
                records = self._query_http(
                    "日k", "*", trading_date, trading_date, raw_fallback=False,
                )
                if records:
                    break
        rows = []
        for item in records:
            code = str(item.get("code") or "").zfill(6)
            if code not in requested or item.get("close") is None:
                continue
            change = item.get("pct_chg")
            if change is None and item.get("pre_close"):
                change = (float(item["close"]) / float(item["pre_close"]) - 1) * 100
            rows.append({
                "code": code,
                "name": str(item.get("name") or ""),
                "price": float(item["close"]),
                "change_pct": float(change or 0),
            })
        return pd.DataFrame(rows, columns=["code", "name", "price", "change_pct"])

    @staticmethod
    def _board_records(rows) -> list[dict[str, Any]]:
        if not isinstance(rows, (list, tuple)):
            try:
                rows = list(rows)
            except TypeError:
                return []
        boards: list[dict[str, Any]] = []
        for row in rows:
            value = row[1] if isinstance(row, (list, tuple)) and len(row) >= 2 else row
            if not isinstance(value, dict):
                continue
            category = value.get("category")
            if isinstance(category, int) or str(category).isdigit():
                category = _BOARD_CATEGORIES.get(int(str(category)), str(category))
            symbols = [
                symbol
                for raw in value.get("symbols", []) or []
                if (symbol := _canonical_cn_symbol(raw))
            ]
            code = str(value.get("code") or "").strip().upper()
            name = str(value.get("name") or "").strip()
            if code and name and category and symbols:
                boards.append({
                    **value,
                    "code": code,
                    "name": name,
                    "category": str(category),
                    "symbols": sorted(set(symbols)),
                })
        return boards

    def boards(self, category: int | str | None = None) -> list[dict[str, Any]]:
        client = self._sdk_client()
        if client is None:
            detail = type(self._sdk_error).__name__ if self._sdk_error else "未安装"
            raise RuntimeError(f"free-stockdb 板块数据需要 stock_sdk（{detail}）")

        def fetch():
            return client.rd.get("板块*").do()

        rows = provider_call("free-stockdb", "sdk:boards", fetch)
        boards = self._board_records(rows)
        if category is None:
            return boards
        expected = (
            _BOARD_CATEGORIES.get(int(str(category)), str(category))
            if isinstance(category, int) or str(category).isdigit()
            else str(category)
        )
        return [item for item in boards if item["category"] == expected]

    def industry_map(self) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for board in self.boards(1):
            for symbol in board["symbols"]:
                mapping[symbol] = board["name"]
        return mapping

    def themes(self) -> list[dict[str, Any]]:
        return [
            {
                "code": board["code"],
                "name": board["name"],
                "members": board["symbols"],
                "aliases": [],
                "source": "free-stockdb:concept",
            }
            for board in self.boards(0)
        ]

    def probe(self) -> dict:
        client = self._sdk_client()
        if client is not None:
            today = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d")
            self._sdk_data("000001", today, today, "1d", fq=None, probe=True)
            return {"status": "ok", "engine": "stock_sdk"}
        payload = self._request({"cmd": "ping"}, probe=True)
        result = payload if isinstance(payload, dict) else {"status": "ok"}
        result.setdefault("engine", "http-compatible")
        return result
