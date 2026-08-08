from __future__ import annotations

import hashlib
import logging
import threading
from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import replace
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd

from quantmaster.after_close.models import (
    SCORE_VERSION,
    SHADOW_SCORE_VERSION,
    AfterCloseSnapshot,
    ResearchCandidate,
    SectorRank,
)
from quantmaster.after_close.store import AfterCloseStore
from quantmaster.config import get_config
from quantmaster.data.free_stockdb_ingest import StockDBIngestRejected, StockDBIngestService
from quantmaster.data.free_stockdb_source import FreeStockDBSource
from quantmaster.data.instruments import Instrument, InstrumentStore
from quantmaster.research.contracts import content_hash
from quantmaster.trading_sessions import expected_session

logger = logging.getLogger(__name__)
Progress = Callable[[int, str, str], None]
Cancelled = Callable[[], bool]
REQUIRED_FIELDS = ("open", "high", "low", "close", "volume")
OPTIONAL_FIELDS = (
    "amount",
    "float_mv",
    "total_mv",
    "pe_ttm",
    "pb",
    "is_st",
    "pre_close",
    "pct_chg",
    "amplitude",
    "turnover",
    "vol_ratio",
    "total_share",
    "float_share",
    "name",
)


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) else None


def _pct_rank(series: pd.Series, *, ascending: bool = True) -> pd.Series:
    return series.rank(pct=True, ascending=ascending, method="average").fillna(0.5)


def _bool(value: Any) -> bool | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"true", "1", "yes", "y", "st"}:
            return True
        if normalized in {"false", "0", "no", "n", ""}:
            return False
    return bool(value)


class DataGateRejected(RuntimeError):
    def __init__(self, reasons: list[str], coverage: dict[str, Any], as_of: str = ""):
        super().__init__("；".join(reasons))
        self.reasons = reasons
        self.coverage = coverage
        self.as_of = as_of


class AfterCloseService:
    def __init__(
        self,
        *,
        source: FreeStockDBSource | None = None,
        instruments: InstrumentStore | None = None,
        store: AfterCloseStore | None = None,
        ingest: StockDBIngestService | None = None,
    ):
        self.source = source or FreeStockDBSource()
        self.instruments = instruments or InstrumentStore()
        self.store = store or AfterCloseStore()
        self.ingest = ingest or StockDBIngestService(self.source)

    def _universe(self, include_bj: bool) -> list[Instrument]:
        active = {"listed", "active", "l"}
        return [
            item
            for item in self.instruments.list(market="CN", asset_type="stock")
            if item.status.casefold() in active
            and item.exchange in ({"SH", "SZ", "BJ"} if include_bj else {"SH", "SZ"})
        ]

    @staticmethod
    def _field_coverage(frame: pd.DataFrame, latest: pd.DataFrame) -> dict[str, Any]:
        return {
            column: {
                "available": bool(column in frame),
                "rows": int(frame[column].notna().sum()) if column in frame else 0,
                "latest_rows": int(latest[column].notna().sum()) if column in latest else 0,
                "latest_ratio": (
                    round(float(latest[column].notna().mean()), 6)
                    if column in latest and len(latest)
                    else None
                ),
            }
            for column in (*REQUIRED_FIELDS, *OPTIONAL_FIELDS)
        }

    @staticmethod
    def _consistency_checks(latest: pd.DataFrame) -> dict[str, Any]:
        def numeric(column: str) -> pd.Series:
            if column not in latest:
                return pd.Series(np.nan, index=latest.index, dtype="float64")
            return pd.to_numeric(latest[column], errors="coerce")

        def compare(
            name: str,
            left: pd.Series,
            right: pd.Series,
            tolerance: float,
        ) -> tuple[str, dict[str, Any]]:
            valid = left.notna() & right.notna() & np.isfinite(left) & np.isfinite(right)
            relative = (left - right).abs() / right.abs().clip(lower=1e-9)
            mismatch = valid & relative.gt(tolerance)
            rows = int(valid.sum())
            return name, {
                "comparable_rows": rows,
                "mismatch_rows": int(mismatch.sum()),
                "mismatch_ratio": round(float(mismatch.sum() / rows), 6) if rows else None,
                "tolerance": tolerance,
            }

        close, volume = numeric("close"), numeric("volume")
        total_share, float_share = numeric("total_share"), numeric("float_share")
        return dict(
            [
                compare("total_mv", numeric("total_mv"), close * total_share, 0.02),
                compare("float_mv", numeric("float_mv"), close * float_share, 0.02),
                compare("turnover", numeric("turnover"), volume / float_share * 100, 0.10),
                compare(
                    "pct_chg",
                    numeric("pct_chg"),
                    (close / numeric("pre_close") - 1) * 100,
                    0.02,
                ),
            ]
        )

    def _gate(
        self,
        frame: pd.DataFrame,
        boards: list[dict[str, Any]],
        expected_count: int,
    ) -> tuple[str, dict[str, Any]]:
        reasons: list[str] = []
        if frame.empty:
            raise DataGateRejected(
                ["free-stockdb 没有返回日频截面"],
                {
                    "expected_symbols": expected_count,
                    "observed_symbols": 0,
                },
            )
        as_of = pd.Timestamp(frame["date"].max()).date().isoformat()
        latest = frame.loc[pd.to_datetime(frame["date"]).dt.date == date.fromisoformat(as_of)]
        observed = int(latest["symbol"].nunique())
        symbol_ratio = observed / expected_count if expected_count else 0.0
        missing_required = [column for column in REQUIRED_FIELDS if column not in latest]
        required_ratio = (
            0.0 if missing_required else float(latest[list(REQUIRED_FIELDS)].notna().all(axis=1).mean())
        )
        levels = Counter(str(item.get("level") or "") for item in boards)
        expectation = expected_session()
        if symbol_ratio < 0.80:
            reasons.append(f"最新截面仅覆盖 {observed}/{expected_count} 只证券")
        if missing_required:
            reasons.append("日线必需字段缺失：" + "、".join(missing_required))
        if required_ratio < 0.95:
            reasons.append(f"最新截面完整 OHLCV 比例仅 {required_ratio:.1%}")
        if not boards or levels.get("L1", 0) == 0:
            reasons.append("free-stockdb 板块目录为空或缺少申万一级")
        if expectation.ready and as_of < expectation.session:
            reasons.append(f"本地库最新交易日 {as_of}，预期至少为 {expectation.session}")
        previous = self.store.latest()
        previous_observed = int((previous.coverage if previous else {}).get("observed_symbols") or 0)
        if previous_observed and observed < previous_observed * 0.85:
            reasons.append(f"证券覆盖较上一成功快照骤降 {previous_observed} → {observed}")
        consistency = self._consistency_checks(latest)
        severe = [
            name
            for name, item in consistency.items()
            if int(item.get("comparable_rows") or 0) >= 100 and float(item.get("mismatch_ratio") or 0) > 0.20
        ]
        if severe:
            reasons.append("截面一致性校验异常：" + "、".join(severe))
        coverage = {
            "status": "complete" if not reasons else "rejected",
            "expected_symbols": expected_count,
            "observed_symbols": observed,
            "symbol_ratio": round(symbol_ratio, 6),
            "required_ohlcv_ratio": round(required_ratio, 6),
            "field_coverage": self._field_coverage(frame, latest),
            "consistency": consistency,
            "board_counts": dict(sorted(levels.items())),
            "expected_session": expectation.as_dict(),
            "issues": reasons,
        }
        if reasons:
            raise DataGateRejected(reasons, coverage, as_of)
        return as_of, coverage

    @staticmethod
    def _frame_hash(frame: pd.DataFrame) -> str:
        columns = [
            column for column in ("symbol", "date", *REQUIRED_FIELDS, *OPTIONAL_FIELDS) if column in frame
        ]
        stable = frame[columns].copy()
        stable["date"] = pd.to_datetime(stable["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        stable = stable.sort_values(["symbol", "date"], kind="mergesort").reset_index(drop=True)
        values = pd.util.hash_pandas_object(stable, index=False, categorize=True)
        digest = hashlib.sha256()
        digest.update("\x1f".join(columns).encode())
        digest.update(values.to_numpy(dtype="uint64", copy=False).tobytes())
        return digest.hexdigest()

    @staticmethod
    def _stock_metrics(frame: pd.DataFrame) -> dict[str, float | int | bool | None]:
        values = frame.sort_values("date")
        close = pd.to_numeric(values["close"], errors="coerce").dropna()
        amount = pd.to_numeric(values.get("amount"), errors="coerce").dropna()
        returns = close.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
        latest = values.iloc[-1]
        ret1 = close.iloc[-1] / close.iloc[-2] - 1 if len(close) >= 2 else np.nan
        ret5 = close.iloc[-1] / close.iloc[-6] - 1 if len(close) >= 6 else np.nan
        ret20 = close.iloc[-1] / close.iloc[-21] - 1 if len(close) >= 21 else np.nan
        ma20 = close.tail(20).mean() if len(close) >= 20 else np.nan
        amount_recent = amount.tail(5).mean() if len(amount) >= 5 else np.nan
        amount_prior = amount.iloc[-20:-5].mean() if len(amount) >= 20 else np.nan
        amount_change = amount_recent / amount_prior - 1 if amount_prior and amount_prior > 0 else np.nan
        peak = close.tail(20).max() if len(close) else np.nan
        return {
            "sessions": len(close),
            "return_1d": _finite(ret1),
            "return_5d": _finite(ret5),
            "return_20d": _finite(ret20),
            "trend_20d": _finite(close.iloc[-1] / ma20 - 1 if ma20 else np.nan),
            "volatility_20d": _finite(returns.tail(20).std()),
            "drawdown_20d": _finite(close.iloc[-1] / peak - 1 if peak else np.nan),
            "avg_amount_20d": _finite(amount.tail(20).mean()) if len(amount) else None,
            "amount_change": _finite(amount_change),
            "float_mv": _finite(latest.get("float_mv")),
            "total_mv": _finite(latest.get("total_mv")),
            "pe_ttm": _finite(latest.get("pe_ttm")),
            "pb": _finite(latest.get("pb")),
            "is_st": _bool(latest.get("is_st")),
            "pre_close": _finite(latest.get("pre_close")),
            "pct_chg": _finite(latest.get("pct_chg")),
            "amplitude": _finite(latest.get("amplitude")),
            "turnover": _finite(latest.get("turnover")),
            "vol_ratio": _finite(latest.get("vol_ratio")),
            "total_share": _finite(latest.get("total_share")),
            "float_share": _finite(latest.get("float_share")),
        }

    @staticmethod
    def _board_members(board: dict[str, Any]) -> list[str]:
        return [str(item).upper() for item in (board.get("members") or board.get("symbols") or [])]

    def _score(
        self,
        frame: pd.DataFrame,
        boards: list[dict[str, Any]],
        instruments: dict[str, Instrument],
        as_of: str,
        *,
        min_sessions: int,
        min_amount: float,
        candidate_limit: int,
        coverage: dict[str, Any],
    ) -> tuple[
        list[SectorRank],
        list[ResearchCandidate],
        list[ResearchCandidate],
        dict[str, int],
        dict[str, Any],
    ]:
        metrics = {symbol: self._stock_metrics(group) for symbol, group in frame.groupby("symbol", sort=True)}
        excluded: Counter[str] = Counter()
        eligible: dict[str, dict[str, Any]] = {}
        for symbol, value in metrics.items():
            instrument = instruments.get(symbol)
            avg_amount = _finite(value.get("avg_amount_20d"))
            is_st = value.get("is_st") is True or "ST" in (instrument.name.upper() if instrument else "")
            if is_st:
                excluded["st"] += 1
            elif int(value.get("sessions") or 0) < min_sessions:
                excluded["listing_sessions"] += 1
            elif avg_amount is None:
                excluded["amount_missing"] += 1
            elif avg_amount < min_amount:
                excluded["liquidity"] += 1
            elif value.get("return_20d") is None:
                excluded["history_missing"] += 1
            else:
                eligible[symbol] = value
        metric_frame = pd.DataFrame.from_dict(eligible, orient="index")
        if metric_frame.empty:
            raise DataGateRejected(["过滤后没有可评分证券"], coverage, as_of)
        market_returns = {
            window: _finite(metric_frame[f"return_{window}d"].median()) or 0.0 for window in (1, 5, 20)
        }
        market20 = market_returns[20]
        board_rows: list[dict[str, Any]] = []
        symbol_boards: dict[str, list[dict[str, str]]] = defaultdict(list)
        for board in boards:
            members = self._board_members(board)
            selected = [symbol for symbol in members if symbol in eligible]
            total = len(set(members))
            if not selected or total == 0:
                continue
            subset = metric_frame.loc[selected]
            ret5 = _finite(subset["return_5d"].median())
            ret20 = _finite(subset["return_20d"].median())
            breadth = _finite((subset["return_20d"] > 0).mean())
            amount_change = _finite(subset["amount_change"].median())
            row = {
                "code": str(board.get("code") or ""),
                "name": str(board.get("name") or ""),
                "level": str(board.get("level") or "OTHER"),
                "category": str(board.get("category") or ""),
                "return_5d": ret5,
                "return_20d": ret20,
                "relative_20d": _finite((ret20 or 0.0) - market20),
                "breadth_20d": breadth,
                "amount_change": amount_change,
                "eligible_members": len(selected),
                "total_members": total,
                "coverage": round(len(selected) / total, 6),
                "members": selected,
                "sensitivity": {},
            }
            for method, weight_column in (
                ("equal", ""),
                ("amount_weighted", "avg_amount_20d"),
                ("float_mv_weighted", "float_mv"),
            ):
                weights = (
                    pd.Series(1.0, index=subset.index)
                    if not weight_column
                    else pd.to_numeric(subset[weight_column], errors="coerce").clip(lower=0)
                )
                method_values: dict[str, Any] = {}
                for window in (1, 5, 20):
                    returns = pd.to_numeric(subset[f"return_{window}d"], errors="coerce")
                    valid = returns.notna() & weights.notna() & weights.gt(0)
                    weighted_return = (
                        float(np.average(returns.loc[valid], weights=weights.loc[valid]))
                        if valid.any()
                        else None
                    )
                    method_values[str(window)] = {
                        "return": _finite(weighted_return),
                        "relative": _finite(
                            weighted_return - market_returns[window] if weighted_return is not None else None
                        ),
                        "breadth": _finite((returns.loc[valid] > 0).mean()) if valid.any() else None,
                        "amount_change": amount_change,
                        "coverage": round(float(valid.sum() / total), 6),
                    }
                row["sensitivity"][method] = method_values
            board_rows.append(row)
            descriptor: dict[str, str] = {
                "code": str(row["code"]),
                "name": str(row["name"]),
                "level": str(row["level"]),
            }
            for symbol in selected:
                symbol_boards[symbol].append(descriptor)
        board_frame = pd.DataFrame(board_rows)
        if board_frame.empty:
            raise DataGateRejected(["板块目录没有可用成分"], coverage, as_of)
        board_frame["score"] = 100 * (
            0.30 * _pct_rank(board_frame["return_20d"])
            + 0.20 * _pct_rank(board_frame["return_5d"])
            + 0.20 * _pct_rank(board_frame["breadth_20d"])
            + 0.15 * _pct_rank(board_frame["amount_change"])
            + 0.15 * _pct_rank(board_frame["coverage"])
        )
        board_frame = board_frame.sort_values(
            ["score", "coverage", "code"],
            ascending=[False, False, True],
            kind="mergesort",
        ).reset_index(drop=True)
        formal_ranks = {str(row["code"]): rank + 1 for rank, row in board_frame.iterrows()}
        for method in ("equal", "amount_weighted", "float_mv_weighted"):
            for window in (1, 5, 20):
                ranked = sorted(
                    board_rows,
                    key=lambda item: (
                        -float(item["sensitivity"][method][str(window)]["return"] or -1e30),
                        str(item["code"]),
                    ),
                )
                for sensitivity_rank, item in enumerate(ranked, 1):
                    result = item["sensitivity"][method][str(window)]
                    result["rank"] = sensitivity_rank
                    result["rank_delta"] = sensitivity_rank - formal_ranks[str(item["code"])]
        board_scores = dict(zip(board_frame["code"], board_frame["score"], strict=True))
        l1_top = set(board_frame.loc[board_frame["level"] == "L1", "code"].head(10))
        candidate_symbols = [
            symbol
            for symbol in metric_frame.index
            if any(item["code"] in l1_top for item in symbol_boards.get(symbol, []))
        ] or list(metric_frame.index)
        candidates_frame = metric_frame.loc[candidate_symbols].copy()
        candidates_frame["sector_score"] = [
            max((board_scores.get(item["code"], 0.0) for item in symbol_boards.get(symbol, [])), default=0.0)
            for symbol in candidates_frame.index
        ]
        candidates_frame["v1_score"] = 100 * (
            0.22 * _pct_rank(candidates_frame["return_20d"])
            + 0.15 * _pct_rank(candidates_frame["return_5d"])
            + 0.15 * _pct_rank(candidates_frame["trend_20d"])
            + 0.13 * _pct_rank(candidates_frame["amount_change"])
            + 0.10 * _pct_rank(candidates_frame["avg_amount_20d"])
            + 0.08 * _pct_rank(candidates_frame["volatility_20d"], ascending=False)
            + 0.07 * _pct_rank(candidates_frame["drawdown_20d"])
            + 0.10 * (candidates_frame["sector_score"] / 100)
        )
        directional_volume_ratio = _pct_rank(candidates_frame["vol_ratio"])
        directional_volume_ratio = directional_volume_ratio.where(
            candidates_frame["return_5d"].fillna(0).ge(0),
            1 - directional_volume_ratio,
        )
        turnover_sufficiency = (_pct_rank(candidates_frame["turnover"]) / 0.75).clip(upper=1)
        candidates_frame["volume_price_confirmation"] = (
            0.50 * _pct_rank(candidates_frame["amount_change"])
            + 0.30 * directional_volume_ratio
            + 0.20 * turnover_sufficiency
        )
        candidates_frame["v2_score"] = 100 * (
            0.18 * _pct_rank(candidates_frame["return_20d"])
            + 0.12 * _pct_rank(candidates_frame["return_5d"])
            + 0.14 * _pct_rank(candidates_frame["trend_20d"])
            + 0.10 * _pct_rank(candidates_frame["avg_amount_20d"])
            + 0.14 * candidates_frame["volume_price_confirmation"]
            + 0.08 * _pct_rank(candidates_frame["volatility_20d"], ascending=False)
            + 0.07 * _pct_rank(candidates_frame["drawdown_20d"])
            + 0.17 * (candidates_frame["sector_score"] / 100)
        )

        def ranked(score_column: str) -> pd.DataFrame:
            return (
                candidates_frame.sort_index(kind="mergesort")
                .sort_values([score_column], ascending=False, kind="mergesort")
                .head(candidate_limit)
            )

        v1_frame, v2_frame = ranked("v1_score"), ranked("v2_score")
        v1_ranks = {symbol: rank for rank, symbol in enumerate(v1_frame.index, 1)}
        v2_ranks = {symbol: rank for rank, symbol in enumerate(v2_frame.index, 1)}
        active_version = self.store.active_score_version()
        formal_frame, formal_column, formal_ranks = (
            (v2_frame, "v2_score", v2_ranks)
            if active_version == SHADOW_SCORE_VERSION
            else (v1_frame, "v1_score", v1_ranks)
        )
        shadow_frame, shadow_column, shadow_ranks = (
            (v1_frame, "v1_score", v1_ranks)
            if active_version == SHADOW_SCORE_VERSION
            else (v2_frame, "v2_score", v2_ranks)
        )
        shadow_version = SCORE_VERSION if active_version == SHADOW_SCORE_VERSION else SHADOW_SCORE_VERSION

        base_provenance = {
            "source": "free-stockdb",
            "calculation": "QuantMaster",
        }
        staleness = {"stale": False, "reason": "", "last_attempt_at": ""}

        def build_candidates(
            selected: pd.DataFrame,
            score_column: str,
            version: str,
            comparison_ranks: dict[str, int],
            comparison_column: str,
        ) -> list[ResearchCandidate]:
            results: list[ResearchCandidate] = []
            for rank, (symbol, row) in enumerate(selected.iterrows(), 1):
                reasons: list[str] = []
                if float(row["return_20d"]) > market20:
                    reasons.append(f"20 日相对全市场中位数领先 {(float(row['return_20d']) - market20):.1%}")
                if (_finite(row.get("trend_20d")) or 0) > 0:
                    reasons.append("收盘价位于 20 日均线上方")
                if (_finite(row.get("amount_change")) or 0) > 0:
                    reasons.append("近 5 日成交额高于此前 15 日")
                top_sectors = sorted(
                    symbol_boards.get(symbol, []),
                    key=lambda item: (-board_scores.get(item["code"], 0.0), item["code"]),
                )
                if top_sectors:
                    reasons.append(f"所属强势板块：{top_sectors[0]['name']}")
                comparison_rank = comparison_ranks.get(symbol)
                components = {
                    "relative_strength_20d": _finite(_pct_rank(candidates_frame["return_20d"])[symbol]),
                    "relative_strength_5d": _finite(_pct_rank(candidates_frame["return_5d"])[symbol]),
                    "trend_20d": _finite(_pct_rank(candidates_frame["trend_20d"])[symbol]),
                    "liquidity": _finite(_pct_rank(candidates_frame["avg_amount_20d"])[symbol]),
                    "volume_price_confirmation": _finite(row["volume_price_confirmation"]),
                    "inverse_volatility": _finite(
                        _pct_rank(candidates_frame["volatility_20d"], ascending=False)[symbol]
                    ),
                    "drawdown": _finite(_pct_rank(candidates_frame["drawdown_20d"])[symbol]),
                    "sector_strength": _finite(row["sector_score"] / 100),
                }
                instrument = instruments.get(symbol)
                results.append(
                    ResearchCandidate(
                        symbol=symbol,
                        name=instrument.name if instrument else "",
                        rank=rank,
                        score=round(float(row[score_column]), 4),
                        sectors=tuple(top_sectors[:8]),
                        metrics={
                            key: _finite(value)
                            for key, value in row.items()
                            if key not in {"is_st", "v1_score", "v2_score"}
                        },
                        reasons=tuple(reasons or ["满足全市场流动性与趋势筛选"]),
                        exclusion_rules=(
                            "非 ST",
                            f"至少 {min_sessions} 个交易日",
                            f"近 20 日日均成交额不低于 {min_amount:.0f} 元",
                        ),
                        as_of_date=as_of,
                        coverage={"field_coverage": coverage["field_coverage"]},
                        provenance={**base_provenance, "score_version": version},
                        staleness=staleness,
                        score_version=version,
                        shadow={
                            "score_version": (
                                SCORE_VERSION if version == SHADOW_SCORE_VERSION else SHADOW_SCORE_VERSION
                            ),
                            "score": _finite(row[comparison_column]),
                            "rank": comparison_rank,
                            "rank_delta": comparison_rank - rank if comparison_rank is not None else None,
                            "membership": "retained" if comparison_rank is not None else "removed",
                            "components": components if version == SHADOW_SCORE_VERSION else {},
                        },
                    )
                )
            return results

        candidates = build_candidates(
            formal_frame, formal_column, active_version, shadow_ranks, shadow_column
        )
        shadow_candidates = build_candidates(
            shadow_frame, shadow_column, shadow_version, formal_ranks, formal_column
        )
        candidate_set = {item.symbol for item in candidates}
        sector_results: list[SectorRank] = []
        for rank, row in board_frame.iterrows():
            sector_results.append(
                SectorRank(
                    code=row["code"],
                    name=row["name"],
                    level=row["level"],
                    category=row["category"],
                    score=round(float(row["score"]), 4),
                    rank=rank + 1,
                    return_5d=_finite(row["return_5d"]),
                    return_20d=_finite(row["return_20d"]),
                    relative_20d=_finite(row["relative_20d"]),
                    breadth_20d=_finite(row["breadth_20d"]),
                    amount_change=_finite(row["amount_change"]),
                    eligible_members=int(row["eligible_members"]),
                    total_members=int(row["total_members"]),
                    coverage=_finite(row["coverage"]),
                    candidate_symbols=tuple(symbol for symbol in row["members"] if symbol in candidate_set),
                    as_of_date=as_of,
                    provenance={**base_provenance, "score_version": SCORE_VERSION},
                    staleness=staleness,
                    sensitivity=row["sensitivity"],
                )
            )
        formal_symbols, shadow_symbols = set(formal_ranks), set(shadow_ranks)
        diagnostics = {
            "formal_score_version": active_version,
            "shadow_score_version": shadow_version,
            "added": sorted(shadow_symbols - formal_symbols),
            "removed": sorted(formal_symbols - shadow_symbols),
            "retained": len(formal_symbols & shadow_symbols),
            "feature_distributions": {
                "coverage": [float(value) for value in board_frame["coverage"].dropna()],
                "returns": [float(value) for value in metric_frame["return_20d"].dropna()],
                "amount": [float(value) for value in metric_frame["avg_amount_20d"].dropna()],
                "turnover": [float(value) for value in metric_frame["turnover"].dropna()],
                "volatility": [float(value) for value in metric_frame["volatility_20d"].dropna()],
                "float_mv": [float(value) for value in metric_frame["float_mv"].dropna()],
            },
        }
        return sector_results, candidates, shadow_candidates, dict(sorted(excluded.items())), diagnostics

    def scan(
        self,
        *,
        as_of: str = "",
        force: bool = False,
        progress: Progress | None = None,
        cancelled: Cancelled | None = None,
    ) -> AfterCloseSnapshot:
        cfg = get_config().data
        if not cfg.after_close_enabled:
            raise RuntimeError("盘后研究扫描已在设置中停用")
        if as_of and not force:
            frozen = self.store.for_date(as_of)
            if frozen is not None:
                return frozen
        progress = progress or (lambda *_: None)
        cancelled = cancelled or (lambda: False)
        instruments = self._universe(cfg.after_close_include_bj)
        if not instruments:
            raise RuntimeError("证券主数据中没有可扫描的 A 股普通股")
        symbols = [item.symbol for item in instruments]
        instrument_map = {item.symbol: item for item in instruments}
        target = date.fromisoformat(as_of) if as_of else date.today()
        start = target - timedelta(
            days=max(
                cfg.free_stockdb_stock_initial_lookback_days,
                cfg.after_close_min_listing_sessions * 3,
            )
        )
        progress(5, "读取本地数据库", f"准备 {len(symbols)} 只 A 股")
        try:
            ingest_snapshot, frame, boards, _ = self.ingest.load_or_create(
                instruments=instruments,
                start=start.isoformat(),
                end=target.isoformat(),
                validator=self._gate,
                progress=progress,
                cancelled=cancelled,
            )
            actual_as_of = ingest_snapshot.as_of_date
            coverage = dict(ingest_snapshot.coverage)
        except DataGateRejected as exc:
            self.store.record_failure(exc.reasons, as_of_date=exc.as_of, coverage=exc.coverage)
            raise
        except StockDBIngestRejected as exc:
            self.store.record_failure(
                exc.reasons,
                as_of_date=exc.as_of_date,
                coverage=exc.coverage,
            )
            raise DataGateRejected(exc.reasons, exc.coverage, exc.as_of_date) from None
        if as_of and not all(item.get("effective_date") or item.get("as_of_date") for item in boards):
            reasons = ["板块目录没有点时生效日期，不能用当前分类强制重算历史选择；请直接重放已冻结快照"]
            self.store.record_failure(reasons, as_of_date=actual_as_of, coverage=coverage)
            raise DataGateRejected(reasons, coverage, actual_as_of)
        if as_of and actual_as_of > as_of:
            frame = frame.loc[pd.to_datetime(frame["date"]).dt.date <= date.fromisoformat(as_of)]
            actual_as_of, coverage = self._gate(frame, boards, len(symbols))
        progress(62, "计算板块优先级", "聚合申万层级与概念板块")
        try:
            sectors, candidates, shadow_candidates, excluded, score_diagnostics = self._score(
                frame,
                boards,
                instrument_map,
                actual_as_of,
                min_sessions=cfg.after_close_min_listing_sessions,
                min_amount=cfg.after_close_min_avg_amount,
                candidate_limit=cfg.after_close_candidate_limit,
                coverage=coverage,
            )
        except DataGateRejected as exc:
            self.store.record_failure(
                exc.reasons,
                as_of_date=exc.as_of,
                coverage=exc.coverage,
            )
            raise
        filters = {
            "universe": "all_cn_stocks",
            "include_bj": cfg.after_close_include_bj,
            "exclude_st": True,
            "min_listing_sessions": cfg.after_close_min_listing_sessions,
            "min_avg_amount_20d": cfg.after_close_min_avg_amount,
            "candidate_limit": cfg.after_close_candidate_limit,
        }
        csi800: dict[str, Any] = {
            "status": "unavailable",
            "reason": "中证800点时成分尚未缓存",
        }
        try:
            from quantmaster.data.index_membership import load_cached_csi800_members_as_of

            membership = load_cached_csi800_members_as_of(actual_as_of, pull=True)
            csi800 = {
                "status": "available",
                "dataset": membership["dataset"],
                "source": membership["source"],
                "members": len(membership["symbols"]),
                "snapshot_dates": membership["snapshot_dates"],
                "content_hash": membership["content_hash"],
            }
        except (ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
            csi800["reason"] = str(exc)[:500]
            logger.info("中证800点时成分暂不可用，保留全市场基线: %s", exc)
        active_score_version = self.store.active_score_version()
        logical_input = {
            "as_of_date": actual_as_of,
            "symbols": sorted(frame["symbol"].astype(str).unique()),
            "frame_hash": self._frame_hash(frame),
            "frame_rows": len(frame),
            "boards": [
                {
                    "code": item.get("code"),
                    "level": item.get("level"),
                    "members": sorted(self._board_members(item)),
                }
                for item in sorted(boards, key=lambda value: str(value.get("code") or ""))
            ],
            "filters": filters,
            "score_version": active_score_version,
            "ingest_id": ingest_snapshot.ingest_id,
            "artifact_id": ingest_snapshot.artifact_id,
            "csi800": csi800,
        }
        input_hash = content_hash(logical_input)
        snapshot_id = (
            "ac_"
            + hashlib.sha256(f"{actual_as_of}:{active_score_version}:{input_hash}".encode()).hexdigest()[:24]
        )
        existing_snapshot = self.store.get(snapshot_id)
        if existing_snapshot is not None:
            self.ingest.store.pin(
                existing_snapshot.ingest_id,
                "after_close",
                existing_snapshot.snapshot_id,
                {"as_of_date": existing_snapshot.as_of_date},
            )
            self._write_research_lake(existing_snapshot, frame, boards)
            self.evaluate_pending(frame)
            progress(100, "盘后扫描完成", f"复用不可变快照 {snapshot_id}")
            return existing_snapshot
        sectors = [replace(item, snapshot_id=snapshot_id) for item in sectors]
        candidates = [replace(item, snapshot_id=snapshot_id) for item in candidates]
        shadow_candidates = [replace(item, snapshot_id=snapshot_id) for item in shadow_candidates]
        previous = self.store.latest()
        current_symbols = {item.symbol for item in candidates}
        previous_symbols = {item.symbol for item in previous.candidates} if previous else set()
        turnover = (
            len(current_symbols.symmetric_difference(previous_symbols))
            / max(1, len(current_symbols) + len(previous_symbols))
            if previous
            else None
        )
        primary_l1 = [
            next((sector["code"] for sector in item.sectors if sector["level"] == "L1"), "")
            for item in candidates
        ]
        concentration = max(Counter(primary_l1).values()) / len(primary_l1) if primary_l1 else None
        shadow_primary_l1 = [
            next((sector["code"] for sector in item.sectors if sector["level"] == "L1"), "")
            for item in shadow_candidates
        ]
        shadow_concentration = (
            max(Counter(shadow_primary_l1).values()) / len(shadow_primary_l1) if shadow_primary_l1 else None
        )
        l1_returns = [
            item.return_20d for item in sectors if item.level == "L1" and item.return_20d is not None
        ]
        market20: float = float(np.nanmedian(l1_returns)) if l1_returns else float("nan")
        snapshot = AfterCloseSnapshot(
            snapshot_id=snapshot_id,
            as_of_date=actual_as_of,
            input_hash=input_hash,
            filters=filters,
            coverage=coverage,
            provenance={
                "source": self.source.name,
                "upstream": "tushare",
                "distribution": "free-stockdb",
                "engine": ("stock_sdk" if self.source.native_batch_available() else "http-compatible"),
                "sdk_path": self.source.sdk_path,
                "sdk_version": self.source.sdk_version(),
                "ingest_id": ingest_snapshot.ingest_id,
                "artifact_id": ingest_snapshot.artifact_id,
                "master_snapshot_id": ingest_snapshot.master_snapshot_id,
                "score_version": active_score_version,
                "calculation": "QuantMaster auditable formulas",
            },
            sectors=tuple(sectors),
            candidates=tuple(candidates),
            shadow_candidates=tuple(shadow_candidates),
            excluded_counts=excluded,
            ingest_id=ingest_snapshot.ingest_id,
            artifact_id=ingest_snapshot.artifact_id,
            score_version=active_score_version,
            validation={
                "market_regime": "positive" if np.isfinite(market20) and market20 > 0 else "defensive",
                "candidate_turnover": _finite(turnover),
                "sector_concentration": _finite(concentration),
                "shadow_sector_concentration": _finite(shadow_concentration),
                "shadow_comparison": score_diagnostics,
                "feature_distributions": score_diagnostics["feature_distributions"],
                "baselines": {
                    "all_market": "available",
                    "csi800": csi800,
                },
                "promotion": "research_observation_only",
            },
        )
        progress(88, "发布不可变快照", snapshot_id)
        self.store.publish(snapshot)
        self.ingest.store.pin(
            snapshot.ingest_id,
            "after_close",
            snapshot.snapshot_id,
            {"as_of_date": snapshot.as_of_date},
        )
        self._write_research_lake(snapshot, frame, boards)
        self.evaluate_pending(frame)
        progress(100, "盘后扫描完成", f"{len(sectors)} 个板块 · {len(candidates)} 只候选")
        return snapshot

    def _write_research_lake(
        self,
        snapshot: AfterCloseSnapshot,
        frame: pd.DataFrame,
        boards: list[dict[str, Any]],
    ) -> None:
        try:
            from quantmaster.research.contracts import ArtifactKind, AssetClass, Frequency
            from quantmaster.research.lake import ResearchLake

            latest = frame.loc[
                pd.to_datetime(frame["date"]).dt.date == date.fromisoformat(snapshot.as_of_date)
            ].copy()
            latest = latest.rename(columns={"date": "trade_date"})
            latest["trade_date"] = snapshot.as_of_date
            latest["snapshot_id"] = snapshot.snapshot_id
            lake = ResearchLake()
            lake.write_partition(
                ArtifactKind.RAW,
                AssetClass.STOCK,
                Frequency.DAILY,
                "after_close_cross_section",
                snapshot.as_of_date,
                latest,
                input_hashes={
                    "after_close": snapshot.input_hash,
                    "stockdb_ingest": snapshot.ingest_id,
                    "stockdb_artifact": snapshot.artifact_id,
                },
                run_id=snapshot.snapshot_id,
            )
            rows = [
                {
                    "trade_date": snapshot.as_of_date,
                    "symbol": f"{board.get('code') or ''}:{symbol}",
                    "component_symbol": symbol,
                    "board_code": str(board.get("code") or ""),
                    "board_name": str(board.get("name") or ""),
                    "board_level": str(board.get("level") or ""),
                    "snapshot_id": snapshot.snapshot_id,
                }
                for board in boards
                for symbol in self._board_members(board)
            ]
            if rows:
                lake.write_partition(
                    ArtifactKind.RAW,
                    AssetClass.STOCK,
                    Frequency.DAILY,
                    "after_close_board_membership",
                    snapshot.as_of_date,
                    pd.DataFrame(rows),
                    input_hashes={
                        "after_close": snapshot.input_hash,
                        "stockdb_ingest": snapshot.ingest_id,
                        "stockdb_artifact": snapshot.artifact_id,
                    },
                    run_id=snapshot.snapshot_id,
                )
        except (ImportError, OSError, RuntimeError, ValueError) as exc:
            logger.warning("盘后快照写入研究湖失败，SQLite 正式快照仍有效: %s", exc)

    def evaluate_pending(self, frame: pd.DataFrame) -> None:
        if frame.empty:
            return
        by_symbol = {
            symbol: group.sort_values("date").set_index(pd.to_datetime(group["date"]))
            for symbol, group in frame.groupby("symbol")
        }
        for meta in self.store.history(100):
            snapshot = self.store.get(str(meta["snapshot_id"]))
            if snapshot is None:
                continue
            existing = {item["horizon"]: item for item in self.store.labels(snapshot.snapshot_id)}
            csi_symbols: set[str] = set()
            if snapshot.validation.get("baselines", {}).get("csi800", {}).get("status") == "available":
                try:
                    from quantmaster.data.index_membership import load_cached_csi800_members_as_of

                    csi_symbols = set(
                        load_cached_csi800_members_as_of(
                            snapshot.as_of_date,
                            pull=False,
                        )["symbols"]
                    )
                except (ImportError, OSError, RuntimeError, TypeError, ValueError):
                    csi_symbols = set()
            for horizon in (1, 3, 5, 7, 10, 20, 30):
                if (
                    horizon in existing
                    and (not csi_symbols or existing[horizon].get("csi800_mean_return") is not None)
                    and (not snapshot.shadow_candidates or existing[horizon].get("score_versions"))
                ):
                    continue
                market_returns = []
                for values in by_symbol.values():
                    future = values.loc[values.index > pd.Timestamp(snapshot.as_of_date), "close"].head(
                        horizon
                    )
                    base = values.loc[values.index <= pd.Timestamp(snapshot.as_of_date), "close"].tail(1)
                    if len(future) == horizon and not base.empty:
                        market_returns.append(float(future.iloc[-1] / base.iloc[-1] - 1))
                market_mean = _finite(np.mean(market_returns)) if market_returns else None
                csi_returns = []
                for symbol in csi_symbols:
                    values = by_symbol.get(symbol)
                    if values is None:
                        continue
                    future = values.loc[values.index > pd.Timestamp(snapshot.as_of_date), "close"].head(
                        horizon
                    )
                    base = values.loc[values.index <= pd.Timestamp(snapshot.as_of_date), "close"].tail(1)
                    if len(future) == horizon and not base.empty:
                        csi_returns.append(float(future.iloc[-1] / base.iloc[-1] - 1))
                csi_mean = _finite(np.mean(csi_returns)) if csi_returns else None

                def candidate_metrics(
                    selected: tuple[ResearchCandidate, ...],
                    *,
                    turnover: Any,
                    concentration: Any,
                    as_of_date: str = snapshot.as_of_date,
                    horizon_value: int = horizon,
                    market_value: float | None = market_mean,
                    csi_value: float | None = csi_mean,
                    csi_count: int = len(csi_returns),
                ) -> dict[str, Any] | None:
                    returns: list[float] = []
                    drawdowns: list[float] = []
                    sector_observations: dict[str, list[tuple[float, float]]] = defaultdict(list)
                    for candidate in selected:
                        values = by_symbol.get(candidate.symbol)
                        if values is None:
                            continue
                        future = values.loc[values.index > pd.Timestamp(as_of_date), "close"].head(
                            horizon_value
                        )
                        base = values.loc[values.index <= pd.Timestamp(as_of_date), "close"].tail(1)
                        if len(future) < horizon_value or base.empty:
                            continue
                        path = pd.concat((base, future)).astype(float)
                        realized_return = float(future.iloc[-1] / base.iloc[-1] - 1)
                        realized_drawdown = float((path / path.cummax() - 1).min())
                        returns.append(realized_return)
                        drawdowns.append(realized_drawdown)
                        primary_l1 = next(
                            (sector["code"] for sector in candidate.sectors if sector.get("level") == "L1"),
                            "unclassified",
                        )
                        sector_observations[primary_l1].append((realized_return, realized_drawdown))
                    if len(returns) != len(selected) or not returns:
                        return None
                    mean_return = float(np.mean(returns))
                    return {
                        "candidate_count": len(returns),
                        "mean_return": _finite(mean_return),
                        "median_return": _finite(np.median(returns)),
                        "hit_rate": _finite(np.mean(np.asarray(returns) > 0)),
                        "market_mean_return": market_value,
                        "excess_mean_return": _finite(
                            mean_return - market_value if market_value is not None else np.nan
                        ),
                        "mean_max_drawdown": _finite(np.mean(drawdowns)),
                        "capacity_avg_amount_20d": _finite(
                            np.mean(
                                [
                                    item.metrics.get("avg_amount_20d")
                                    for item in selected
                                    if item.metrics.get("avg_amount_20d") is not None
                                ]
                            )
                        ),
                        "candidate_turnover": turnover,
                        "sector_concentration": concentration,
                        "baseline": "all_market",
                        "csi800_status": "available" if csi_value is not None else "unavailable",
                        "csi800_member_returns": csi_count,
                        "csi800_mean_return": csi_value,
                        "excess_vs_csi800": _finite(
                            mean_return - csi_value if csi_value is not None else np.nan
                        ),
                        "sector_groups": {
                            sector: {
                                "candidate_count": len(values),
                                "mean_return": _finite(np.mean([item[0] for item in values])),
                                "hit_rate": _finite(np.mean([item[0] > 0 for item in values])),
                                "mean_max_drawdown": _finite(np.mean([item[1] for item in values])),
                                "market_mean_return": market_value,
                                "excess_mean_return": _finite(
                                    np.mean([item[0] for item in values]) - market_value
                                    if market_value is not None
                                    else np.nan
                                ),
                            }
                            for sector, values in sorted(sector_observations.items())
                        },
                    }

                formal = candidate_metrics(
                    snapshot.candidates,
                    turnover=snapshot.validation.get("candidate_turnover"),
                    concentration=snapshot.validation.get("sector_concentration"),
                )
                if formal is None:
                    continue
                shadow = (
                    candidate_metrics(
                        snapshot.shadow_candidates,
                        turnover=None,
                        concentration=snapshot.validation.get("shadow_sector_concentration"),
                    )
                    if snapshot.shadow_candidates
                    else None
                )
                score_versions = {snapshot.score_version: formal}
                if shadow is not None:
                    score_versions[snapshot.shadow_candidates[0].score_version] = shadow
                self.store.save_labels(
                    snapshot.snapshot_id,
                    horizon,
                    {
                        **formal,
                        "score_versions": score_versions,
                    },
                )


_lock = threading.Lock()
_instance: AfterCloseService | None = None


def get_after_close_service() -> AfterCloseService:
    global _instance
    with _lock:
        if _instance is None:
            _instance = AfterCloseService()
        return _instance


def reset_after_close_service() -> None:
    global _instance
    with _lock:
        _instance = None


def reset_after_close_service_for_tests() -> None:
    reset_after_close_service()
