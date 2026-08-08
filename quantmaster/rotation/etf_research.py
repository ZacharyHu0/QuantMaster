"""All-exchange ETF research backed by local Tushare-distributed stockdb data."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
from collections import Counter
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.free_stockdb_ingest import (
    STOCKDB_INGEST_SCHEMA_VERSION,
    StockDBIngestService,
    StockDBIngestStore,
)
from quantmaster.data.free_stockdb_source import FreeStockDBSource
from quantmaster.data.instruments import Instrument, InstrumentStore
from quantmaster.research.contracts import content_hash
from quantmaster.rotation.etf_models import (
    ETF_SCORE_VERSION,
    EtfProfile,
    EtfResearchItem,
    EtfResearchSnapshot,
)

Progress = Callable[[int, str, str], None]
Cancelled = Callable[[], bool]

ETF_CATEGORIES = (
    "境内宽基",
    "行业主题",
    "策略",
    "港股及海外 QDII",
    "债券",
    "商品",
    "货币",
    "其他",
)


def _rank_percentile(
    frame: pd.DataFrame,
    column: str,
    *,
    ascending: bool = True,
) -> pd.Series:
    return (
        pd.to_numeric(frame.get(column), errors="coerce")
        .rank(pct=True, ascending=ascending, method="average")
        .fillna(0.5)
    )


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(fd)
    temp = Path(temp_name)
    try:
        temp.write_text(value, encoding="utf-8")
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def classify_etf(
    name: str,
    *,
    benchmark: str = "",
    fund_type: str = "",
    invest_type: str = "",
) -> tuple[str, tuple[str, ...]]:
    text = " ".join((name, benchmark, fund_type, invest_type)).upper()
    evidence: list[str] = []
    rules = (
        ("货币", ("货币", "现金", "添益", "保证金")),
        ("债券", ("债", "国开", "政金", "可转债")),
        ("商品", ("黄金", "商品", "豆粕", "有色", "能源化工", "原油")),
        (
            "港股及海外 QDII",
            (
                "QDII",
                "港股",
                "恒生",
                "H股",
                "中概",
                "纳指",
                "标普",
                "日经",
                "德国",
                "法国",
                "沙特",
                "印度",
                "东南亚",
                "海外",
            ),
        ),
        ("策略", ("红利", "低波", "价值", "成长", "质量", "策略", "增强", "SMART")),
        (
            "境内宽基",
            (
                "沪深300",
                "中证500",
                "中证1000",
                "中证2000",
                "A500",
                "上证50",
                "科创50",
                "创业板",
                "深证100",
                "中证A",
                "全指",
                "综指",
            ),
        ),
    )
    for category, tokens in rules:
        matched = [token for token in tokens if token in text]
        if matched:
            evidence.extend(matched[:3])
            return category, tuple(evidence)
    if "ETF" in text or "交易型" in text:
        return "行业主题", ("ETF 未命中跨资产分类",)
    return "其他", ("仅由场内 ETF 身份确认",)


def is_exchange_etf(instrument: Instrument) -> bool:
    if instrument.exchange not in {"SH", "SZ"}:
        return False
    if instrument.status.casefold() not in {"listed", "active", "l"}:
        return False
    text = instrument.name.upper()
    if "LOF" in text or "联接" in text:
        return False
    return instrument.asset_type == "etf" or (
        instrument.asset_type == "fund" and ("ETF" in text or "交易型" in text)
    )


class EtfResearchStore:
    def __init__(self, root: str | Path | None = None):
        self.root = Path(root or (get_config().data_root / "etf-research")).resolve()
        self._lock = threading.RLock()

    def publish(self, snapshot: EtfResearchSnapshot) -> EtfResearchSnapshot:
        encoded = json.dumps(snapshot.to_dict(), ensure_ascii=False, sort_keys=True, indent=2, default=str)
        with self._lock:
            target = self.root / "snapshots" / f"{snapshot.snapshot_id}.json"
            if target.exists():
                existing = EtfResearchSnapshot.from_dict(json.loads(target.read_text(encoding="utf-8")))
                identity = ("ingest_id", "input_hash", "score_version", "schema_version")
                if any(getattr(existing, key) != getattr(snapshot, key) for key in identity):
                    raise RuntimeError(f"ETF 研究快照不可变: {snapshot.snapshot_id}")
                snapshot = existing
            else:
                _atomic_text(target, encoded)
            _atomic_text(
                self.root / "latest.json",
                json.dumps(
                    {
                        "snapshot_id": snapshot.snapshot_id,
                        "last_failure": {},
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
            )
            return snapshot

    def get(self, snapshot_id: str) -> EtfResearchSnapshot | None:
        try:
            value = json.loads((self.root / "snapshots" / f"{snapshot_id}.json").read_text(encoding="utf-8"))
            return EtfResearchSnapshot.from_dict(value)
        except FileNotFoundError:
            return None

    def latest(self) -> EtfResearchSnapshot | None:
        try:
            state = json.loads((self.root / "latest.json").read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
            return None
        snapshot = self.get(str(state.get("snapshot_id") or ""))
        failure = state.get("last_failure") or {}
        if snapshot is not None and failure:
            data = snapshot.to_dict()
            data["staleness"] = {
                "stale": True,
                "reason": str(failure.get("reason") or "ETF 研究刷新失败"),
                "last_attempt_at": str(failure.get("attempted_at") or ""),
            }
            return EtfResearchSnapshot.from_dict(data)
        return snapshot

    def history(self, limit: int = 50) -> list[dict[str, Any]]:
        paths = sorted(
            (self.root / "snapshots").glob("*.json"), key=lambda item: item.stat().st_mtime_ns, reverse=True
        )
        result = []
        for path in paths[: max(1, limit)]:
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
                result.append(
                    {
                        "snapshot_id": value["snapshot_id"],
                        "ingest_id": value.get("ingest_id", ""),
                        "as_of_date": value["as_of_date"],
                        "generated_at": value["generated_at"],
                        "coverage": value.get("coverage") or {},
                        "item_count": len(value.get("items") or []),
                        "categories": value.get("categories") or [],
                    }
                )
            except (OSError, KeyError, ValueError, json.JSONDecodeError):
                continue
        return result

    def record_failure(self, reason: str) -> None:
        from datetime import UTC, datetime

        latest = self.latest()
        _atomic_text(
            self.root / "latest.json",
            json.dumps(
                {
                    "snapshot_id": latest.snapshot_id if latest else "",
                    "last_failure": {
                        "reason": reason[:500],
                        "attempted_at": datetime.now(UTC).isoformat(timespec="seconds"),
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
        )


class EtfResearchService:
    def __init__(
        self,
        *,
        source: FreeStockDBSource | None = None,
        instruments: InstrumentStore | None = None,
        ingest_store: StockDBIngestStore | None = None,
        store: EtfResearchStore | None = None,
    ):
        self.source = source or FreeStockDBSource()
        self.instruments = instruments or InstrumentStore()
        self.ingest_store = ingest_store or StockDBIngestStore()
        self.store = store or EtfResearchStore()

    def profiles(self) -> list[EtfProfile]:
        observations = self._direct_share_observations()
        metadata: dict[str, dict[str, str]] = {}
        if not observations.empty:
            for symbol, group in observations.groupby("symbol"):
                last = group.sort_values("trade_date").iloc[-1]
                metadata[str(symbol).upper()] = {
                    key: str(last.get(key) or "") for key in ("benchmark", "fund_type", "invest_type")
                }
        result = []
        for instrument in self.instruments.list(market="CN"):
            if not is_exchange_etf(instrument):
                continue
            extra = metadata.get(instrument.symbol, {})
            category, evidence = classify_etf(
                instrument.name,
                benchmark=extra.get("benchmark", ""),
                fund_type=extra.get("fund_type", ""),
                invest_type=extra.get("invest_type", ""),
            )
            result.append(
                EtfProfile(
                    symbol=instrument.symbol,
                    name=instrument.name,
                    category=category,
                    benchmark=extra.get("benchmark", ""),
                    fund_type=extra.get("fund_type", ""),
                    invest_type=extra.get("invest_type", ""),
                    list_date=instrument.list_date,
                    status=instrument.status,
                    classification_evidence=evidence,
                )
            )
        return sorted(result, key=lambda item: item.symbol)

    @staticmethod
    def _direct_share_observations() -> pd.DataFrame:
        try:
            from quantmaster.rotation.store import RotationStore

            return RotationStore().etf_observations()
        except (ImportError, OSError, RuntimeError, TypeError, ValueError):
            return pd.DataFrame()

    @staticmethod
    def _minute_metrics(frame: pd.DataFrame) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for symbol, group in frame.groupby("symbol"):
            values = group.sort_values("date").copy()
            close = pd.to_numeric(values["close"], errors="coerce")
            volume = pd.to_numeric(values["volume"], errors="coerce")
            amount = pd.to_numeric(values.get("amount"), errors="coerce")
            vwap = amount.sum() / volume.sum() if volume.sum() > 0 and amount.notna().any() else np.nan
            returns = close.pct_change().replace([np.inf, -np.inf], np.nan)
            times = pd.to_datetime(values["date"], errors="coerce")
            total_amount = amount.sum()
            first = amount[times.dt.time <= pd.Timestamp("10:30").time()].sum()
            last = amount[times.dt.time >= pd.Timestamp("14:00").time()].sum()
            result[str(symbol)] = {
                "rows": len(values),
                "complete_session": len(values) >= 240,
                "vwap_deviation": float(close.iloc[-1] / vwap - 1) if vwap and np.isfinite(vwap) else None,
                "realized_volatility": float(returns.std()) if returns.notna().any() else None,
                "intraday_drawdown": float((close / close.cummax() - 1).min())
                if close.notna().any()
                else None,
                "first_hour_amount_share": float(first / total_amount) if total_amount > 0 else None,
                "last_hour_amount_share": float(last / total_amount) if total_amount > 0 else None,
                "scoring_input": False,
            }
        return result

    @staticmethod
    def _daily_metrics(group: pd.DataFrame) -> dict[str, Any]:
        values = group.sort_values("date")
        close = pd.to_numeric(values["close"], errors="coerce").dropna()
        amount = pd.to_numeric(values.get("amount"), errors="coerce").dropna()
        shares = pd.to_numeric(values.get("total_share"), errors="coerce")
        returns = close.pct_change().replace([np.inf, -np.inf], np.nan)
        metric: dict[str, Any] = {
            "sessions": len(close),
            "close": float(close.iloc[-1]) if len(close) else None,
            "return_5d": float(close.iloc[-1] / close.iloc[-6] - 1) if len(close) >= 6 else None,
            "return_20d": float(close.iloc[-1] / close.iloc[-21] - 1) if len(close) >= 21 else None,
            "return_60d": float(close.iloc[-1] / close.iloc[-61] - 1) if len(close) >= 61 else None,
            "avg_amount_20d": float(amount.tail(20).mean()) if len(amount) else None,
            "volatility_20d": float(returns.tail(20).std()) if returns.notna().any() else None,
            "drawdown_20d": float(close.iloc[-1] / close.tail(20).max() - 1) if len(close) else None,
            "stockdb_total_share": float(shares.dropna().iloc[-1]) if shares.notna().any() else None,
            "stockdb_share_change_5d": (
                float(shares.dropna().iloc[-1] / shares.dropna().iloc[-6] - 1)
                if shares.notna().sum() >= 6 and shares.dropna().iloc[-6]
                else None
            ),
        }
        if shares.notna().sum() >= 2 and len(close) >= 2:
            share_values = shares.dropna()
            metric["stockdb_share_delta"] = float(share_values.iloc[-1] - share_values.iloc[-2])
            metric["stockdb_flow"] = float(metric["stockdb_share_delta"] * close.iloc[-2])
        return metric

    @staticmethod
    def _stockdb_share_semantics(
        rows: list[dict[str, Any]],
        direct: pd.DataFrame,
        actual: str,
    ) -> dict[str, Any]:
        comparable = 0
        matches = 0
        if direct.empty:
            return {
                "status": "unconfirmed",
                "comparable_symbols": 0,
                "matching_symbols": 0,
                "match_ratio": None,
                "reason": "没有同制品时点的直接 fund_share 观察，不能确认 stockdb 份额滞后语义",
            }
        target = date.fromisoformat(actual)
        for row in rows:
            observed = row["metrics"].get("stockdb_total_share")
            if observed is None:
                continue
            symbol = row["profile"].symbol
            values = direct.loc[direct["symbol"].astype(str).str.upper().eq(symbol)].copy()
            values = values.loc[values["trade_date"].dt.date <= target].sort_values("trade_date")
            if len(values) < 2 or values.iloc[-1]["trade_date"].date() != target:
                continue
            prior = pd.to_numeric(pd.Series([values.iloc[-2].get("shares")]), errors="coerce").iloc[0]
            if pd.isna(prior):
                continue
            comparable += 1
            if np.isclose(float(observed), float(prior), rtol=1e-6, atol=1.0):
                matches += 1
        ratio = matches / comparable if comparable else None
        validated = comparable >= 5 and ratio is not None and ratio >= 0.80
        return {
            "status": "stockdb_lag_validated" if validated else "unconfirmed",
            "comparable_symbols": comparable,
            "matching_symbols": matches,
            "match_ratio": round(ratio, 6) if ratio is not None else None,
            "reason": (
                "当前 artifact 的 stockdb total_share 与直接 fund_share 前一交易日语义一致"
                if validated
                else "可比样本不足或一致率低于 80%，不使用 stockdb 份额计算资金流"
            ),
        }

    def scan(
        self,
        *,
        as_of: str = "",
        progress: Progress | None = None,
        cancelled: Cancelled | None = None,
    ) -> EtfResearchSnapshot:
        cfg = get_config().data
        if not cfg.free_stockdb_etf_research_enabled:
            raise RuntimeError("ETF 研究已在设置中停用")
        progress = progress or (lambda *_: None)
        cancelled = cancelled or (lambda: False)
        profiles = self.profiles()
        if not profiles:
            raise RuntimeError("证券主数据中没有沪深场内 ETF")
        end = pd.Timestamp(as_of or date.today()).normalize()
        start = end - pd.DateOffset(years=3, days=20)
        symbols = [item.symbol for item in profiles]
        master_id = "etf_master_" + content_hash([item.to_dict() for item in profiles])[:24]
        data_session = StockDBIngestService._data_session(str(end.date()))
        identity = self.source.artifact_identity(data_session=data_session)
        cache_key = content_hash(
            {
                "schema_version": STOCKDB_INGEST_SCHEMA_VERSION,
                "asset": "etf",
                "artifact": identity.artifact_id,
                "master": master_id,
                "start": str(start.date()),
                "end": str(end.date()),
                "symbols": symbols,
            }
        )
        ingest = next(
            (
                item
                for item in self.ingest_store.history(100)
                if item.provenance.get("cache_key") == cache_key and "etf" in item.assets
            ),
            None,
        )
        daily = pd.DataFrame()
        minutes = pd.DataFrame()
        if ingest is not None:
            cached_input_hash = content_hash(
                {
                    "ingest_id": ingest.ingest_id,
                    "profiles": [item.to_dict() for item in profiles],
                    "score_version": ETF_SCORE_VERSION,
                }
            )
            cached_snapshot_id = (
                "etf_"
                + hashlib.sha256(
                    f"{ingest.as_of_date}:{ETF_SCORE_VERSION}:{cached_input_hash}".encode()
                ).hexdigest()[:24]
            )
            existing = self.store.get(cached_snapshot_id)
            if existing is not None:
                existing = self.store.publish(existing)
                self.ingest_store.pin(
                    existing.ingest_id,
                    "etf_research",
                    existing.snapshot_id,
                    {"as_of_date": existing.as_of_date},
                )
                progress(100, "复用 ETF 研究快照", existing.snapshot_id)
                return existing
            daily = self.ingest_store.load_frame(ingest, "etf_daily")
            minutes = self.ingest_store.load_frame(ingest, "etf_minutes")
        if daily.empty:
            frames = []
            for offset in range(0, len(symbols), 300):
                if cancelled():
                    raise InterruptedError("ETF 研究扫描已取消")
                batch = symbols[offset : offset + 300]
                frames.append(
                    self.source.daily_cross_section(
                        batch,
                        str(start.date()),
                        str(end.date()),
                    )
                )
                progress(
                    5 + int(50 * (offset + len(batch)) / len(symbols)),
                    "读取 ETF 日线",
                    f"{offset + len(batch)}/{len(symbols)}",
                )
            daily = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            if daily.empty:
                raise RuntimeError("free-stockdb 没有返回 ETF 日频截面")
            actual = pd.to_datetime(daily["date"], errors="coerce").max().date().isoformat()
            latest = daily[
                pd.to_datetime(daily["date"], errors="coerce").dt.date == date.fromisoformat(actual)
            ]
            observed = int(latest["symbol"].nunique())
            ratio = observed / len(symbols)
            required_ratio = float(
                latest[["open", "high", "low", "close", "volume"]].notna().all(axis=1).mean()
            )
            if ratio < 0.80 or required_ratio < 0.95:
                raise RuntimeError(
                    f"ETF 完整性门未通过：覆盖 {observed}/{len(symbols)}，OHLCV {required_ratio:.1%}"
                )
            if cfg.free_stockdb_etf_minutes_enabled:
                minute_frames = []
                minute_start = f"{actual} 09:30:00"
                minute_end = f"{actual} 15:00:00"
                for offset in range(0, len(symbols), 300):
                    if cancelled():
                        raise InterruptedError("ETF 分钟证据读取已取消")
                    minute_frames.append(
                        self.source.intraday_many(
                            symbols[offset : offset + 300],
                            minute_start,
                            minute_end,
                            "1m",
                        )
                    )
                minutes = pd.concat(minute_frames, ignore_index=True) if minute_frames else pd.DataFrame()
            coverage = {
                "status": "complete",
                "expected_symbols": len(symbols),
                "observed_symbols": observed,
                "symbol_ratio": round(ratio, 6),
                "required_ohlcv_ratio": round(required_ratio, 6),
                "minute_symbols": int(minutes["symbol"].nunique()) if not minutes.empty else 0,
                "minute_is_scoring_input": False,
            }
            etf_sessions = sorted(
                pd.to_datetime(
                    daily["date"],
                    errors="coerce",
                )
                .dropna()
                .dt.strftime("%Y-%m-%d")
                .unique()
                .tolist()
            )
            coverage["fields"] = StockDBIngestService.field_contracts(
                daily,
                actual,
                asset_class="etf",
                source=self.source.name,
            )
            ingest = self.ingest_store.publish_etf(
                daily=daily,
                minutes=minutes,
                profiles=[item.to_dict() for item in profiles],
                as_of_date=actual,
                artifact_id=identity.artifact_id,
                master_snapshot_id=master_id,
                start_date=str(start.date()),
                end_date=str(end.date()),
                coverage=coverage,
                provenance={
                    "cache_key": cache_key,
                    "upstream": "tushare",
                    "distribution": "free-stockdb",
                    "artifact": identity.to_dict(),
                    "ingest_schema_version": STOCKDB_INGEST_SCHEMA_VERSION,
                    "price_storage": "raw",
                },
                session_dates=etf_sessions,
                session_source="stockdb_broad_coverage",
            )
        actual = ingest.as_of_date
        progress(70, "计算 ETF 研究证据", "仅在同一资产类别内排序")
        minute_metrics = self._minute_metrics(minutes) if not minutes.empty else {}
        by_symbol = {symbol: self._daily_metrics(group) for symbol, group in daily.groupby("symbol")}
        direct = self._direct_share_observations()
        if not direct.empty:
            direct["trade_date"] = pd.to_datetime(direct["trade_date"], errors="coerce")
        rows: list[dict[str, Any]] = []
        for profile in profiles:
            metric = by_symbol.get(profile.symbol, {})
            effective_date, lag, share_source = "", None, "missing"
            semantic_status = "unavailable"
            direct_symbol = (
                direct[direct["symbol"].astype(str).str.upper().eq(profile.symbol)]
                if not direct.empty
                else pd.DataFrame()
            )
            direct_latest = (
                direct_symbol[direct_symbol["trade_date"].dt.date <= date.fromisoformat(actual)].sort_values(
                    "trade_date"
                )
                if not direct_symbol.empty
                else direct_symbol
            )
            if (
                not direct_latest.empty
                and pd.Timestamp(direct_latest.iloc[-1]["trade_date"]).date().isoformat() == actual
            ):
                latest_share = pd.to_numeric(
                    pd.Series([direct_latest.iloc[-1].get("shares")]), errors="coerce"
                ).iloc[0]
                metric["total_share"] = float(latest_share) if pd.notna(latest_share) else None
                effective_date, lag, share_source = actual, 0, "tushare:fund_share"
                semantic_status = "direct_confirmed"
                if len(direct_latest) >= 2 and metric.get("close") is not None:
                    prior = pd.to_numeric(
                        pd.Series([direct_latest.iloc[-2].get("shares")]), errors="coerce"
                    ).iloc[0]
                    if pd.notna(latest_share) and pd.notna(prior):
                        metric["flow"] = float((latest_share - prior) * metric["close"])
                if len(direct_latest) >= 6 and pd.notna(latest_share):
                    prior_5 = pd.to_numeric(
                        pd.Series([direct_latest.iloc[-6].get("shares")]),
                        errors="coerce",
                    ).iloc[0]
                    if pd.notna(prior_5) and float(prior_5) != 0:
                        metric["stockdb_share_change_5d"] = float(float(latest_share) / float(prior_5) - 1)
            elif metric.get("stockdb_total_share") is not None:
                symbol_dates = sorted(
                    pd.to_datetime(
                        daily.loc[daily["symbol"].eq(profile.symbol), "date"],
                        errors="coerce",
                    )
                    .dropna()
                    .dt.date.unique()
                )
                effective_date = str(symbol_dates[-2]) if len(symbol_dates) >= 2 else ""
                lag, share_source = 1, "tushare:via-free-stockdb"
                metric["total_share"] = metric["stockdb_total_share"]
                semantic_status = "unconfirmed"
            rows.append(
                {
                    "profile": profile,
                    "metrics": metric,
                    "effective_date": effective_date,
                    "lag": lag,
                    "share_source": share_source,
                    "semantic_status": semantic_status,
                }
            )

        semantic_validation = self._stockdb_share_semantics(rows, direct, actual)
        for row in rows:
            if (
                row["semantic_status"] == "unconfirmed"
                and semantic_validation["status"] == "stockdb_lag_validated"
            ):
                row["semantic_status"] = "stockdb_lag_validated"
                row["metrics"]["flow"] = row["metrics"].get("stockdb_flow")
            elif row["semantic_status"] not in {"direct_confirmed", "stockdb_lag_validated"}:
                row["metrics"].pop("flow", None)
                row["metrics"]["stockdb_share_change_5d"] = None

        category_frames: dict[str, pd.DataFrame] = {}
        for category in ETF_CATEGORIES:
            selected = [row for row in rows if row["profile"].category == category]
            frame = pd.DataFrame([{**row["metrics"], "symbol": row["profile"].symbol} for row in selected])
            if frame.empty:
                continue
            rankable = frame["sessions"].fillna(0).ge(21) & frame["avg_amount_20d"].notna()
            frame["rankable"] = rankable
            eligible = frame.loc[rankable].copy()
            if not eligible.empty:
                eligible["category_relative_20d"] = (
                    pd.to_numeric(eligible["return_20d"], errors="coerce")
                    - pd.to_numeric(eligible["return_20d"], errors="coerce").median()
                )
                eligible["score"] = 100 * (
                    0.25 * _rank_percentile(eligible, "return_20d")
                    + 0.15 * _rank_percentile(eligible, "return_5d")
                    + 0.20 * _rank_percentile(eligible, "avg_amount_20d")
                    + 0.15 * _rank_percentile(eligible, "drawdown_20d")
                    + 0.10 * _rank_percentile(eligible, "volatility_20d", ascending=False)
                    + 0.15 * _rank_percentile(eligible, "stockdb_share_change_5d")
                )
                eligible = eligible.sort_values(["score", "symbol"], ascending=[False, True])
                eligible["category_rank"] = range(1, len(eligible) + 1)
                frame = frame.merge(
                    eligible[["symbol", "score", "category_rank", "category_relative_20d"]],
                    on="symbol",
                    how="left",
                )
            category_frames[category] = frame.set_index("symbol")

        items = []
        input_hash = content_hash(
            {
                "ingest_id": ingest.ingest_id,
                "profiles": [item.to_dict() for item in profiles],
                "score_version": ETF_SCORE_VERSION,
            }
        )
        snapshot_id = (
            "etf_" + hashlib.sha256(f"{actual}:{ETF_SCORE_VERSION}:{input_hash}".encode()).hexdigest()[:24]
        )
        for row in rows:
            profile, metric = row["profile"], dict(row["metrics"])
            ranked = category_frames.get(profile.category)
            rank_row = (
                ranked.loc[profile.symbol] if ranked is not None and profile.symbol in ranked.index else None
            )
            rankable = bool(rank_row is not None and rank_row.get("rankable"))
            if rank_row is not None and pd.notna(rank_row.get("category_relative_20d")):
                metric["category_relative_20d"] = float(rank_row["category_relative_20d"])
            items.append(
                EtfResearchItem(
                    symbol=profile.symbol,
                    name=profile.name,
                    category=profile.category,
                    category_rank=(
                        int(rank_row["category_rank"])
                        if rankable and pd.notna(rank_row.get("category_rank"))
                        else None
                    ),
                    score=(
                        round(float(rank_row["score"]), 4)
                        if rankable and pd.notna(rank_row.get("score"))
                        else None
                    ),
                    rankable=rankable,
                    excluded_reason="" if rankable else "日线不足 21 日或流动性缺失",
                    metrics=metric,
                    minute_evidence=minute_metrics.get(
                        profile.symbol,
                        {
                            "rows": 0,
                            "complete_session": False,
                            "scoring_input": False,
                        },
                    ),
                    shares_effective_date=row["effective_date"],
                    share_lag_sessions=row["lag"],
                    coverage={
                        "daily": profile.symbol in by_symbol,
                        "minute": profile.symbol in minute_metrics,
                    },
                    provenance={
                        "price": "tushare:via-free-stockdb",
                        "shares": row["share_source"],
                        "classification": "tushare-master+quantmaster-rules",
                        "independent_cross_validation": False,
                    },
                    as_of_date=actual,
                    snapshot_id=snapshot_id,
                    ingest_id=ingest.ingest_id,
                    artifact_id=ingest.artifact_id,
                    share_semantic_status=row["semantic_status"],
                )
            )
        items.sort(
            key=lambda item: (
                ETF_CATEGORIES.index(item.category),
                item.category_rank or 10**9,
                item.symbol,
            )
        )
        snapshot = EtfResearchSnapshot(
            snapshot_id=snapshot_id,
            ingest_id=ingest.ingest_id,
            artifact_id=ingest.artifact_id,
            as_of_date=actual,
            coverage={
                **ingest.coverage,
                "rankable_symbols": sum(item.rankable for item in items),
                "share_semantics": semantic_validation,
                "share_semantic_counts": dict(Counter(item.share_semantic_status for item in items)),
            },
            provenance={
                "upstream": "tushare",
                "distribution": "free-stockdb",
                "master": "tushare:instrument-store",
                "calculation": "QuantMaster",
                "independent_cross_validation": False,
            },
            items=tuple(items),
            categories=tuple(
                category for category in ETF_CATEGORIES if any(item.category == category for item in items)
            ),
            input_hash=input_hash,
        )
        snapshot = self.store.publish(snapshot)
        self.ingest_store.pin(
            snapshot.ingest_id,
            "etf_research",
            snapshot.snapshot_id,
            {"as_of_date": snapshot.as_of_date},
        )
        progress(100, "ETF 研究完成", f"{len(items)} 只 · {sum(item.rankable for item in items)} 只可排名")
        return snapshot


_lock = threading.Lock()
_instance: EtfResearchService | None = None


def get_etf_research_service() -> EtfResearchService:
    global _instance
    with _lock:
        if _instance is None:
            _instance = EtfResearchService()
        return _instance


def reset_etf_research_service() -> None:
    global _instance
    with _lock:
        _instance = None
