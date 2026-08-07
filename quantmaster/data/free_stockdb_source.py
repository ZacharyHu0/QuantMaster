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
import tokenize
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
from quantmaster.data.free_stockdb_contracts import StockDBArtifactIdentity

_BOARD_CATEGORIES = {
    0: "概念",
    1: "申万一级",
    2: "申万二级",
    3: "申万三级",
}


def resolve_free_stockdb_sdk_path(value: str | None = None) -> Path | None:
    """Resolve an explicit SDK path or discover it beside the managed runtime."""
    cfg = get_config().data
    configured = str(cfg.free_stockdb_sdk_path if value is None else value).strip()
    if configured:
        path = Path(configured).expanduser()
        resolved = path.resolve() if path.is_absolute() else (Path.cwd() / path).resolve()
        return resolved if resolved.is_file() else resolved / "stock_sdk.py"
    root = Path(cfg.free_stockdb_root).expanduser()
    root = root.resolve() if root.is_absolute() else (Path.cwd() / root).resolve()
    candidate = root / "pybao" / "stock_sdk.py"
    return candidate if candidate.is_file() else None


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
        DataCapability.DAILY_BARS,
        DataCapability.DAILY,
        DataCapability.DAILY_CROSS_SECTION,
        DataCapability.INTRADAY_BARS,
        DataCapability.INTRADAY,
        DataCapability.EOD_SNAPSHOT,
        DataCapability.SECURITY_CATALOG,
        DataCapability.ADJUSTMENT_FACTORS,
        DataCapability.ETF_SHARES,
        DataCapability.INDUSTRY,
        DataCapability.THEMES,
        DataCapability.BOARD_HIERARCHY,
        DataCapability.NATIVE_INDICATORS,
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
        resolved_sdk = resolve_free_stockdb_sdk_path(sdk_path)
        self.sdk_path = str(resolved_sdk) if resolved_sdk is not None else ""
        self._trust_env = urlparse(self.base_url).hostname not in {
            "127.0.0.1", "localhost", "::1",
        }
        self._sdk_checked = False
        self._client: Any | None = None
        self._sdk_error: BaseException | None = None
        self._loaded_artifact_id = ""

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
        # The vendor updates files in place.  A path-only module key would keep
        # old code alive until QuantMaster itself restarts.
        digest = hashlib.sha256(sdk_file.read_bytes()).hexdigest()[:16]
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
            # SourceFileLoader may reuse a same-size/same-second .pyc after an
            # in-place vendor update. Compile the selected content directly so
            # the content-hash module name and executed code cannot diverge.
            with tokenize.open(sdk_file) as stream:
                code = compile(stream.read(), str(sdk_file), "exec")
            exec(code, module.__dict__)
        except (ImportError, OSError, AttributeError, RuntimeError):
            sys.modules.pop(module_name, None)
            raise
        return module

    def reset_runtime(self) -> None:
        """Discard clients after a data/runtime update; the next call re-probes."""
        self._sdk_checked = False
        self._client = None
        self._sdk_error = None
        self._loaded_artifact_id = ""

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

    def native_batch_available(self) -> bool:
        """Whether the native SDK can serve a true multi-symbol request."""
        return self._sdk_client() is not None

    def _sdk_data(
        self,
        code: str | list[str],
        start: str | None,
        end: str | None,
        frequency: str,
        *,
        fq: str | None,
        fields: str | None = None,
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
                "fields": fields,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        arguments = {
            "code": code,
            "start": start or None,
            "end": end or None,
            "frequency": frequency,
            "fq": fq,
            "as_df": False,
        }
        # Older stock_sdk builds do not expose the fields keyword.  Keep the
        # established daily/minute contract compatible while asking newer
        # builds for the richer after-close cross section explicitly.
        if fields is not None:
            arguments["fields"] = fields
        return provider_call(
            self.name,
            key,
            lambda: client.get_data(**arguments),
            probe=probe,
        )

    def sdk_version(self) -> str:
        """Return a deterministic vendor version or the SDK content fingerprint."""
        try:
            module = self._load_sdk_module()
        except (ImportError, OSError, AttributeError, RuntimeError, TypeError, ValueError):
            return ""
        namespace = getattr(module, "__dict__", {})
        value = namespace.get("__version__") or namespace.get("VERSION")
        if isinstance(value, (str, int, float)) and str(value).strip():
            return str(value).strip()
        sdk = self.artifact_identity().sdk
        digest = str(sdk.get("sha256") or "")
        return f"sha256:{digest[:16]}" if digest else ""

    def artifact_identity(
        self, *, data_session: str = "", catalog_hash: str = "", board_hash: str = "",
    ) -> StockDBArtifactIdentity:
        return StockDBArtifactIdentity.discover(
            self.sdk_path or None, get_config().data.free_stockdb_root,
            data_session=data_session, catalog_hash=catalog_hash, board_hash=board_hash,
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

        return provider_call(self.name, key, fetch, probe=probe)

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

    @classmethod
    def _records_for_fields(cls, payload, fields: str) -> list[dict]:
        """Decode SDK rows returned as positional arrays when fields is supplied."""
        names = [item.strip() for item in fields.split(",") if item.strip()]
        if not isinstance(payload, list) or not names:
            return cls._records(payload)
        records: list[dict] = []
        fallback: list[Any] = []
        for item in payload:
            if (
                isinstance(item, (list, tuple))
                and len(item) == len(names)
                and len(item) > 2
            ):
                records.append(dict(zip(names, item, strict=True)))
            else:
                fallback.append(item)
        if fallback:
            records.extend(cls._records(fallback))
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

    def daily_many(
        self, symbols: list[str], start: str, end: str,
    ) -> dict[str, pd.DataFrame]:
        """Fetch many A-share histories in one native SDK request."""
        ordered = list(dict.fromkeys(str(symbol).upper() for symbol in symbols))
        if not ordered:
            return {}
        codes = [symbol.partition(".")[0].zfill(6) for symbol in ordered]
        begin = _compact_time(start, intraday=False)
        finish = _compact_time(end, intraday=False)
        payload = self._sdk_data(codes, begin, finish, "1d", fq="qfq")
        if payload is None:
            return super().daily_many(ordered, start, end)
        if not isinstance(payload, dict):
            if len(ordered) == 1:
                return {ordered[0]: self._frame(self._records(payload), intraday=False).loc[start:end]}
            return {}
        result: dict[str, pd.DataFrame] = {}
        for symbol, code in zip(ordered, codes, strict=True):
            values = payload.get(code)
            if values is None:
                values = payload.get(symbol)
            frame = self._frame(self._records(values), intraday=False).loc[start:end]
            if not frame.empty:
                result[symbol] = frame
        return result

    def daily_cross_section(
        self, symbols: list[str], start: str, end: str,
    ) -> pd.DataFrame:
        """批量读取盘后点时截面；可选字段缺失时保留 NaN。"""
        ordered = list(dict.fromkeys(str(symbol).upper() for symbol in symbols))
        columns = [
            "symbol", "date", "open", "high", "low", "close", "volume",
            "amount", "float_mv", "total_mv", "pe_ttm", "pb", "is_st",
            "pre_close", "pct_chg", "amplitude", "turnover", "vol_ratio",
            "total_share", "float_share", "name",
        ]
        if not ordered:
            return pd.DataFrame(columns=columns)
        codes = [symbol.partition(".")[0].zfill(6) for symbol in ordered]
        begin = _compact_time(start, intraday=False)
        finish = _compact_time(end, intraday=False)
        fields = ",".join(columns[1:])
        try:
            # Cross-sectional ingestion archives point-in-time raw prices.  Research
            # prices are derived later from the separately frozen factor payload.
            payload = self._sdk_data(codes, begin, finish, "1d", fq=None, fields=fields)
        except (KeyError, TypeError):
            # Old SDK builds and strict wrappers only understand the original
            # cross-section projection.  Missing rich fields remain null.
            legacy = ",".join(columns[1:13])
            payload = self._sdk_data(codes, begin, finish, "1d", fq=None, fields=legacy)
            fields = legacy
        records: list[dict[str, Any]] = []
        if payload is None:
            for symbol, code in zip(ordered, codes, strict=True):
                for item in self._query_http("日k", code, begin, finish):
                    records.append({**item, "symbol": symbol})
        elif isinstance(payload, dict):
            for symbol, code in zip(ordered, codes, strict=True):
                values = payload.get(code, payload.get(symbol))
                for item in self._records_for_fields(values, fields):
                    records.append({**item, "symbol": symbol})
        elif len(ordered) == 1:
            records = [
                {**item, "symbol": ordered[0]}
                for item in self._records_for_fields(payload, fields)
            ]
        frame = pd.DataFrame(records)
        for column in columns:
            if column not in frame:
                frame[column] = pd.NA
        if frame.empty:
            return frame[columns]
        digits = frame["date"].astype(str).str.replace(r"\D", "", regex=True).str[:8]
        frame["date"] = pd.to_datetime(digits, format="%Y%m%d", errors="coerce")
        for column in (
            "open", "high", "low", "close", "volume", "amount",
            "float_mv", "total_mv", "pe_ttm", "pb", "pre_close", "pct_chg",
            "amplitude", "turnover", "vol_ratio", "total_share", "float_share",
        ):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        # ETF valuation sentinels mean not-applicable, never a literal zero.
        etf_mask = frame["symbol"].astype(str).str.partition(".")[0].str.startswith(("1", "5"))
        frame.loc[etf_mask & frame["pb"].eq(0), "pb"] = pd.NA
        return frame[columns].dropna(subset=["date"]).sort_values(["symbol", "date"])

    def board_hierarchy(self) -> list[dict[str, Any]]:
        levels = {"申万一级": "L1", "申万二级": "L2", "申万三级": "L3", "概念": "CONCEPT"}
        return [
            {
                **item,
                "level": levels.get(str(item.get("category") or ""), "OTHER"),
                "members": list(item.get("symbols") or []),
            }
            for item in self.boards()
        ]

    def native_indicators(
        self, names: list[str], symbols: list[str], start: str, end: str,
    ) -> dict:
        """调用 SDK 指标模块，仅供显式校验/性能路径使用。"""
        module = self._load_sdk_module()
        indicator = getattr(module, "zb", None)
        calculate = getattr(indicator, "get", None)
        if not callable(calculate):
            raise RuntimeError("free-stockdb SDK 未暴露公开 zb.get 指标接口")
        codes = [str(symbol).partition(".")[0].zfill(6) for symbol in symbols]
        return calculate(names, codes, start=_compact_time(start, intraday=False),
                         end=_compact_time(end, intraday=False), frequency="1d")

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
        return self._aggregate_cn_minutes(frame, frequency, aggregation)

    def intraday_many(
        self, symbols: list[str], start: str, end: str, frequency: str = "1m",
    ) -> pd.DataFrame:
        """Batch minute bars for coverage/evidence jobs; never implies realtime."""
        frequency = validate_frequency(frequency)
        if frequency == "1d":
            frames = self.daily_cross_section(symbols, start, end)
            return frames
        ordered = list(dict.fromkeys(str(symbol).upper() for symbol in symbols))
        if not ordered:
            return pd.DataFrame(columns=["symbol", "date", *OHLCV_COLUMNS, "amount"])
        codes = [symbol.partition(".")[0].zfill(6) for symbol in ordered]
        payload = self._sdk_data(
            codes, _compact_time(start, intraday=True), _compact_time(end, intraday=True),
            frequency, fq=None,
        )
        if not isinstance(payload, dict):
            return pd.DataFrame(columns=["symbol", "date", *OHLCV_COLUMNS, "amount"])
        frames: list[pd.DataFrame] = []
        for symbol, code in zip(ordered, codes, strict=True):
            values = payload.get(code, payload.get(symbol))
            frame = self._frame(self._records(values), intraday=True)
            if frame.empty:
                continue
            frame = frame.reset_index()
            frame.insert(0, "symbol", symbol)
            frames.append(frame)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
            columns=["symbol", "date", *OHLCV_COLUMNS, "amount"],
        )

    @staticmethod
    def _aggregate_cn_minutes(
        frame: pd.DataFrame, frequency: str, aggregation: dict[str, str],
    ) -> pd.DataFrame:
        """Aggregate within each A-share session so buckets never cross lunch."""
        minutes = int(frequency[:-1])
        values = frame.sort_index().copy()
        minute_of_day = values.index.hour * 60 + values.index.minute
        morning = (minute_of_day >= 570) & (minute_of_day <= 690)
        afternoon = (minute_of_day >= 780) & (minute_of_day <= 900)
        values = values.loc[morning | afternoon]
        if values.empty:
            return values
        minute_of_day = values.index.hour * 60 + values.index.minute
        session = pd.Series("pm", index=values.index)
        session.loc[minute_of_day <= 690] = "am"
        origin = pd.Series(780, index=values.index)
        origin.loc[session.eq("am")] = 570
        bucket = ((minute_of_day - origin) // minutes).astype(int)
        grouped = values.groupby([
            values.index.normalize(), session.to_numpy(), bucket,
        ], sort=True)
        result = grouped.agg(aggregation).dropna(subset=["close"])
        result.index = pd.DatetimeIndex([
            group.index[0] for _, group in grouped
        ], name="date")
        return result.sort_index()

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

    def eod_snapshot(self, symbols: list[str]) -> pd.DataFrame:
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
                "symbol": _canonical_cn_symbol(code),
                "name": str(item.get("name") or ""),
                "price": float(item["close"]),
                "change_pct": float(change or 0),
                "as_of_date": str(item.get("date") or trading_date)[:8],
                "realtime": False,
            })
        return pd.DataFrame(rows, columns=[
            "code", "symbol", "name", "price", "change_pct", "as_of_date", "realtime",
        ])

    def spot(self, symbols: list[str]) -> pd.DataFrame:
        """Deprecated compatibility method; this source does not advertise SPOT."""
        return self.eod_snapshot(symbols)

    def adjustment_factors(
        self, symbols: list[str], start: str, end: str,
    ) -> pd.DataFrame:
        requested = {
            str(item).upper().partition(".")[0].zfill(6): str(item).upper()
            for item in symbols
        }
        begin = _compact_time(start, intraday=False)
        finish = _compact_time(end, intraday=False)
        client = self._sdk_client()
        if client is not None:
            raw = provider_call(
                self.name, "sdk:adjustment-factors",
                lambda: client.rd.get("复权*").get("cum"),
            )
            by_symbol: dict[str, list[dict[str, Any]]] = {}
            for item in raw:
                if not isinstance(item, (list, tuple)) or len(item) < 2:
                    continue
                parts = str(item[0]).split(":")
                if len(parts) < 3 or parts[-2] not in requested:
                    continue
                try:
                    row = {
                        "symbol": requested[parts[-2]], "date": parts[-1][:8],
                        "adj_factor": float(item[1]),
                    }
                except (TypeError, ValueError):
                    continue
                if row["date"] <= finish:
                    by_symbol.setdefault(row["symbol"], []).append(row)
            rows: list[dict[str, Any]] = []
            for values in by_symbol.values():
                values.sort(key=lambda row: row["date"])
                previous = [row for row in values if row["date"] < begin]
                if previous:
                    rows.append(previous[-1])
                rows.extend(row for row in values if row["date"] >= begin)
            frame = pd.DataFrame(rows, columns=["symbol", "date", "adj_factor"])
            if not frame.empty:
                frame["date"] = pd.to_datetime(frame["date"], format="%Y%m%d", errors="coerce")
                return frame.dropna(subset=["date", "adj_factor"]).sort_values(["symbol", "date"])

        rows: list[dict[str, Any]] = []
        for symbol in dict.fromkeys(str(item).upper() for item in symbols):
            code = symbol.partition(".")[0].zfill(6)
            for item in self._query_http("复权", code, begin, finish):
                value = item.get("cum", item.get("factor"))
                rows.append({"symbol": symbol, "date": item.get("date"), "adj_factor": value})
        frame = pd.DataFrame(rows, columns=["symbol", "date", "adj_factor"])
        if not frame.empty:
            frame["date"] = pd.to_datetime(frame["date"].astype(str).str[:8], errors="coerce")
            frame["adj_factor"] = pd.to_numeric(frame["adj_factor"], errors="coerce")
            frame = frame.dropna(subset=["date", "adj_factor"])
        return frame

    @staticmethod
    def _raw_table_records(rows: Any) -> list[dict[str, Any]]:
        if not isinstance(rows, (list, tuple)):
            try:
                rows = list(rows)
            except TypeError:
                return []
        result: list[dict[str, Any]] = []
        for row in rows:
            key, value = "", row
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                key, value = str(row[0]), row[1]
            if isinstance(value, dict):
                result.append({"key": key, **value})
            else:
                result.append({"key": key, "value": value})
        return result

    def security_catalog(self) -> list[dict[str, Any]]:
        client = self._sdk_client()
        if client is None:
            raise RuntimeError("free-stockdb 证券目录需要 stock_sdk")
        rows = provider_call(self.name, "sdk:security-catalog", lambda: client.rd.get("股票代码").do())
        return self._raw_table_records(rows)

    def delisted_catalog(self) -> list[dict[str, Any]]:
        client = self._sdk_client()
        if client is None:
            raise RuntimeError("free-stockdb 退市目录需要 stock_sdk")
        rows = provider_call(self.name, "sdk:delisted-catalog", lambda: client.rd.get("退市*").do())
        return self._raw_table_records(rows)

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

        rows = provider_call(self.name, "sdk:boards", fetch)
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
            return {
                "status": "ok", "engine": "stock_sdk",
                "sdk_path": self.sdk_path, "service_url": self.base_url.rstrip("/"),
                "sdk_version": self.sdk_version(),
                "artifact": self.artifact_identity().to_dict(),
            }
        payload = self._request({"cmd": "ping"}, probe=True)
        result = payload if isinstance(payload, dict) else {"status": "ok"}
        result.setdefault("engine", "http-compatible")
        result.setdefault("sdk_path", self.sdk_path)
        result.setdefault("service_url", self.base_url.rstrip("/"))
        return result


class FreeStockDBOnlineSource(FreeStockDBSource):
    """Public trial endpoint used only after the local database misses."""

    name = "free-stockdb-online"

    def __init__(self):
        cfg = get_config().data
        super().__init__(
            base_url=cfg.free_stockdb_online_url,
            timeout=cfg.free_stockdb_online_timeout,
        )
