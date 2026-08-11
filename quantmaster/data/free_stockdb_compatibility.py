"""Artifact-bound validation for optional free-stockdb native indicators."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.free_stockdb_contracts import StockDBCompatibilityProfile

METHODS = ("MA", "EMA", "MACD", "RSI", "ATR", "BOLL")


def quantmaster_indicators(bars: pd.DataFrame) -> pd.DataFrame:
    frame = bars.copy().sort_index()
    if "date" in frame:
        frame = frame.set_index(pd.to_datetime(frame.pop("date")))
    close = pd.to_numeric(frame["close"], errors="coerce")
    high = pd.to_numeric(frame.get("high", close), errors="coerce")
    low = pd.to_numeric(frame.get("low", close), errors="coerce")
    result = pd.DataFrame(index=close.index)
    result["MA"] = close.rolling(20, min_periods=20).mean()
    result["EMA"] = close.ewm(span=20, adjust=False).mean()
    ema12, ema26 = close.ewm(span=12, adjust=False).mean(), close.ewm(span=26, adjust=False).mean()
    result["MACD"] = ema12 - ema26
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    result["RSI"] = 100 - 100 / (1 + gain / loss.replace(0, np.nan))
    previous = close.shift(1)
    true_range = pd.concat((high - low, (high - previous).abs(), (low - previous).abs()), axis=1).max(axis=1)
    result["ATR"] = true_range.rolling(14, min_periods=10).mean()
    result["BOLL"] = close.rolling(20, min_periods=20).mean()
    return result


class StockDBCompatibilityStore:
    def __init__(self, root: str | Path | None = None, *, read_only: bool = False):
        self.root = Path(root or (get_config().data_root / "free-stockdb-compatibility"))
        self.read_only = bool(read_only)
        # Diagnostic and page readers must never create a data root merely by
        # checking whether a compatibility profile exists.  The worker owns
        # directory creation and publication.
        if not self.read_only:
            self.root.mkdir(parents=True, exist_ok=True)

    def path(self, artifact_id: str) -> Path:
        return self.root / f"{artifact_id}.json"

    def get(self, artifact_id: str) -> StockDBCompatibilityProfile | None:
        try:
            profile = StockDBCompatibilityProfile.from_dict(
                json.loads(self.path(artifact_id).read_text(encoding="utf-8"))
            )
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            return None
        return profile if profile.artifact_id == artifact_id else None

    def publish(self, profile: StockDBCompatibilityProfile) -> StockDBCompatibilityProfile:
        if self.read_only:
            raise RuntimeError("只读兼容性存储不能发布验收结果")
        target = self.path(profile.artifact_id)
        fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=self.root)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(profile.to_dict(), stream, ensure_ascii=False, sort_keys=True, indent=2)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
        finally:
            Path(temporary).unlink(missing_ok=True)
        return profile

    def admitted(self, artifact_id: str, method: str) -> bool:
        profile = self.get(artifact_id)
        return bool(
            profile
            and profile.status in {"compatible", "partial"}
            and profile.methods.get(method.upper(), {}).get("passed")
        )


def compare_indicator_frames(
    expected: pd.DataFrame,
    actual: pd.DataFrame,
    *,
    rtol: float = 1e-5,
    atol: float = 1e-7,
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for method in METHODS:
        if method not in expected or method not in actual:
            results[method] = {"passed": False, "reason": "column_missing"}
            continue
        left = pd.to_numeric(expected[method], errors="coerce")
        right = pd.to_numeric(actual[method], errors="coerce").reindex(left.index)
        missing_match = left.isna().equals(right.isna())
        valid = left.notna() & right.notna()
        numeric_match = (
            bool(np.allclose(left.loc[valid], right.loc[valid], rtol=rtol, atol=atol))
            if valid.any()
            else False
        )
        results[method] = {
            "passed": missing_match and numeric_match,
            "rows": int(valid.sum()),
            "missing_positions_match": missing_match,
            "numeric_match": numeric_match,
            "rtol": rtol,
            "atol": atol,
        }
    return results


def native_payload_frame(payload: Any, symbol: str) -> pd.DataFrame:
    code = symbol.partition(".")[0].zfill(6)
    records = payload.get(code, payload.get(symbol, [])) if isinstance(payload, dict) else payload
    frame = pd.DataFrame(records or [])
    if frame.empty or "date" not in frame:
        return pd.DataFrame(columns=METHODS)
    digits = frame.pop("date").astype(str).str.replace(r"\D", "", regex=True).str[:8]
    frame.index = pd.to_datetime(digits, format="%Y%m%d", errors="coerce")
    mapping = {
        "MA": "ma20",
        "EMA": "ema20",
        "MACD": "dif",
        "RSI": "rsi",
        "ATR": "atr",
        "BOLL": "mid",
    }
    return pd.DataFrame(
        {method: pd.to_numeric(frame.get(column), errors="coerce") for method, column in mapping.items()},
        index=frame.index,
    ).sort_index()


def validate_runtime_profile(
    source: Any,
    samples: list[dict[str, Any]],
    *,
    store: StockDBCompatibilityStore | None = None,
) -> StockDBCompatibilityProfile:
    artifact_id = source.artifact_identity().artifact_id
    comparisons: list[dict[str, dict[str, Any]]] = []
    evidence: list[dict[str, Any]] = []
    for sample in samples:
        symbol = str(sample["symbol"])
        start, end = str(sample["start"]), str(sample["end"])
        bars = source.daily(symbol, start, end)
        payload = source.native_indicators(list(METHODS), [symbol], start, end)
        expected = quantmaster_indicators(bars)
        actual = native_payload_frame(payload, symbol)
        comparison = compare_indicator_frames(expected, actual, rtol=1e-4, atol=1e-3)
        comparisons.append(comparison)
        evidence.append(
            {
                **sample,
                "rows": len(bars),
                "methods": {key: value["passed"] for key, value in comparison.items()},
            }
        )
    return publish_validation(artifact_id, comparisons, evidence, store=store)


def publish_validation(
    artifact_id: str,
    comparisons: list[dict[str, dict[str, Any]]],
    samples: list[dict[str, Any]],
    *,
    store: StockDBCompatibilityStore | None = None,
) -> StockDBCompatibilityProfile:
    methods: dict[str, dict[str, Any]] = {}
    for method in METHODS:
        checks = [item.get(method, {}) for item in comparisons]
        methods[method] = {
            "passed": bool(checks) and all(bool(item.get("passed")) for item in checks),
            "sample_count": len(checks),
            "checks": checks,
        }
    passed = sum(bool(item["passed"]) for item in methods.values())
    profile = StockDBCompatibilityProfile(
        artifact_id=artifact_id,
        status="compatible" if passed == len(methods) else "partial" if passed else "incompatible",
        methods=methods,
        samples=tuple(samples),
    )
    return (store or StockDBCompatibilityStore()).publish(profile)
