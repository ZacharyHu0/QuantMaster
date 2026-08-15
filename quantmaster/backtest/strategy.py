"""策略层：把「因子/规则」转成回测引擎需要的目标权重矩阵。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd

from quantmaster.factors.base import Factor, PanelDict
from quantmaster.factors.engine import compute_factor
from quantmaster.signal_contract import SignalBundle


class Strategy(ABC):
    """策略基类：输出 date × symbol 的目标权重（T 日收盘决定，T+1 开盘执行）。"""

    name: str = "strategy"

    @abstractmethod
    def target_weights(self, panel: PanelDict) -> pd.DataFrame:
        ...

    def signal_bundle(
        self,
        panel: PanelDict,
        *,
        eligibility_mask: pd.DataFrame | None = None,
    ) -> SignalBundle:
        weights = self.target_weights(panel)
        if eligibility_mask is not None:
            active = weights.notna().any(axis=1)
            mask = eligibility_mask.reindex_like(weights).fillna(False).astype(bool)
            weights = weights.where(mask, 0.0)
            weights.loc[~active] = float("nan")
        return SignalBundle(weights=weights)


def rebalance_mask(dates: pd.DatetimeIndex, freq: str = "W") -> pd.Series:
    """调仓日掩码。freq: D=每日, W=每周最后交易日, M=每月最后交易日。"""
    s = pd.Series(True, index=dates)
    if freq.upper() == "D":
        return s
    period = dates.to_period("W" if freq.upper() == "W" else "M")
    is_last = pd.Series(period, index=dates).ne(pd.Series(period, index=dates).shift(-1))
    is_last.iloc[-1] = True
    return is_last


class FactorStrategy(Strategy):
    """因子选股：调仓日按因子值从高到低取前 top_n 只，等权买入。

    cap_weight 限制单票最大权重；因子值缺失的股票不参与排名。
    """

    def __init__(
        self,
        factor: Factor,
        top_n: int = 5,
        rebalance: str = "W",
        cap_weight: float = 0.35,
        standardize: bool = True,
    ):
        self.factor = factor
        self.top_n = top_n
        self.rebalance = rebalance
        self.cap_weight = cap_weight
        self.standardize = standardize
        self.name = f"factor_{factor.name}_top{top_n}_{rebalance}"

    def target_weights(self, panel: PanelDict) -> pd.DataFrame:
        values = compute_factor(self.factor, panel, standardize=self.standardize)
        close = panel["close"]
        values = values.reindex(index=close.index, columns=close.columns)

        ranks = values.rank(axis=1, ascending=False)
        selected = (ranks <= self.top_n).astype(float).where(values.notna(), 0.0)
        counts = selected.sum(axis=1).replace(0, float("nan"))
        weights = selected.div(counts, axis=0).clip(upper=self.cap_weight).fillna(0.0)

        mask = rebalance_mask(close.index, self.rebalance)
        weights = weights.where(mask, other=float("nan"))   # 非调仓日不发信号
        return weights


class MultiFactorStrategy(Strategy):
    """多因子选股：多个因子合成一个综合分后取前 top_n。

    weighting:
        "equal"  各因子标准化后等权相加（稳健默认）
        "ic"     滚动 RankIC 动态加权（ic_weighted_combine，权重已 shift
                 防未来函数；负 IC 因子自动反向）
    """

    def __init__(
        self,
        factors: list[Factor],
        top_n: int = 5,
        rebalance: str = "W",
        cap_weight: float = 0.35,
        weighting: str = "equal",
        ic_lookback: int = 60,
    ):
        if not factors:
            raise ValueError("factors 不能为空")
        if weighting not in ("equal", "ic"):
            raise ValueError(f"weighting 只支持 equal/ic，实际: {weighting!r}")
        self.factors = factors
        self.top_n = top_n
        self.rebalance = rebalance
        self.cap_weight = cap_weight
        self.weighting = weighting
        self.ic_lookback = ic_lookback
        names = "+".join(f.name for f in factors)[:60]
        self.name = f"multi[{names}]_{weighting}_top{top_n}_{rebalance}"

    def target_weights(self, panel: PanelDict) -> pd.DataFrame:
        from quantmaster.factors.composite import ic_weighted_combine
        from quantmaster.factors.engine import combine_factors, compute_factors

        values = compute_factors(self.factors, panel, standardize=True)
        if self.weighting == "ic":
            combined, _ = ic_weighted_combine(values, panel["close"],
                                              lookback=self.ic_lookback)
        else:
            combined = combine_factors(values)

        close = panel["close"]
        combined = combined.reindex(index=close.index, columns=close.columns)
        ranks = combined.rank(axis=1, ascending=False)
        selected = (ranks <= self.top_n).astype(float).where(combined.notna(), 0.0)
        counts = selected.sum(axis=1).replace(0, float("nan"))
        weights = selected.div(counts, axis=0).clip(upper=self.cap_weight).fillna(0.0)

        mask = rebalance_mask(close.index, self.rebalance)
        return weights.where(mask, other=float("nan"))


class BuyAndHold(Strategy):
    """基准：首日等权买入并持有。"""

    name = "buy_and_hold"

    def target_weights(self, panel: PanelDict) -> pd.DataFrame:
        close = panel["close"]
        weights = pd.DataFrame(float("nan"), index=close.index, columns=close.columns)
        first = close.notna().any(axis=1).idxmax()
        n = close.loc[first].notna().sum()
        weights.loc[first] = close.loc[first].notna().astype(float) / max(n, 1)
        return weights


class LabVersionStrategy(Strategy):
    """固定 Quant Lab 版本；学习模型历史回测只读取滚动 OOF 预测。"""

    def __init__(
        self, version: dict[str, Any], *, horizon: int, top_n: int,
        rebalance_days: int, cap_weight: float,
    ):
        self.version = version
        self.horizon = horizon
        self.top_n = top_n
        self.rebalance_days = rebalance_days
        self.cap_weight = cap_weight
        self.name = f"lab_{version.get('slug', version.get('id', 'version'))}_{horizon}d"
        self._model_metadata: dict[str, Any] = {}

    def _learned_scores(self, panel: PanelDict) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        import hashlib
        import json

        from quantmaster.config import get_config

        model = (self.version.get("spec") or {}).get("model") or {}
        root = Path(get_config().data_root).resolve()
        manifest_path = (root / str(model.get("manifest") or "")).resolve()
        if not manifest_path.is_relative_to(root) or not manifest_path.is_file():
            raise FileNotFoundError("学习模型 manifest 不存在或越出数据目录")
        manifest_bytes = manifest_path.read_bytes()
        expected_manifest = str(model.get("manifest_sha256") or "")
        if expected_manifest and hashlib.sha256(manifest_bytes).hexdigest() != expected_manifest:
            raise ValueError("学习模型 manifest 完整性校验失败")
        manifest = json.loads(manifest_bytes.decode("utf-8"))
        if manifest.get("schema_version") != 2:
            raise ValueError("LabVersionStrategy 只接受带滚动 OOF 的 schema v2 模型")
        prediction_path = (root / str(manifest.get("prediction_artifact") or "")).resolve()
        if not prediction_path.is_relative_to(root) or not prediction_path.is_file():
            raise FileNotFoundError("滚动 OOF 预测工件不存在")
        digest = hashlib.sha256(prediction_path.read_bytes()).hexdigest()
        if digest != manifest.get("prediction_sha256"):
            raise ValueError("滚动 OOF 预测工件完整性校验失败")
        rows = pd.read_parquet(prediction_path)
        rows = rows.loc[pd.to_numeric(rows["horizon"], errors="coerce") == self.horizon]
        if rows.empty:
            raise ValueError(f"OOF 工件没有 {self.horizon} 日预测")

        def pivot(column: str) -> pd.DataFrame:
            result = rows.pivot(index="date", columns="symbol", values=column)
            result.index = pd.to_datetime(result.index)
            return result.rename_axis(None, axis=1)

        scores = pivot("expected_excess")
        contributions = {
            "expected_return": pivot("expected_return"),
            "probability_up": pivot("probability_up"),
            "probability_net_positive": pivot("probability_net_positive"),
            "uncertainty": pivot("q90") - pivot("q10"),
        }
        self._model_metadata = {
            "research_quality": manifest.get("research_quality"),
            "snapshot_hash": manifest.get("snapshot_hash"),
            "protocol": manifest.get("protocol"),
        }
        return scores, contributions

    def signal_bundle(
        self,
        panel: PanelDict,
        *,
        eligibility_mask: pd.DataFrame | None = None,
    ) -> SignalBundle:
        spec = self.version.get("spec") or {}
        close = panel["close"]
        contributions: dict[str, pd.DataFrame] = {}
        if spec.get("kind") == "learned":
            scores, contributions = self._learned_scores(panel)
        else:
            from quantmaster.factors import compute_factor
            from quantmaster.factors.fundamental import resolve_factor

            expression = spec.get("expression") or self.version.get("slug")
            factor = resolve_factor(
                expression, list(close.columns), str(close.index.min().date()),
                str(close.index.max().date()),
            )
            scores = compute_factor(factor, panel)
        scores = scores.reindex(index=close.index, columns=close.columns)
        ranks = scores.rank(axis=1, ascending=False)
        selected = (ranks <= self.top_n).astype(float).where(scores.notna(), 0.0)
        counts = selected.sum(axis=1).replace(0, float("nan"))
        weights = selected.div(counts, axis=0).clip(upper=self.cap_weight).fillna(0.0)
        mask = pd.Series(False, index=close.index)
        mask.iloc[::self.rebalance_days] = True
        weights = weights.where(mask, other=float("nan"))
        if eligibility_mask is not None:
            active = weights.notna().any(axis=1)
            allowed = eligibility_mask.reindex_like(weights).fillna(False).astype(bool)
            weights = weights.where(allowed, 0.0)
            weights.loc[~active] = float("nan")
        confidence = contributions.get("probability_net_positive")
        uncertainty = contributions.get("uncertainty")
        degraded = (
            uncertainty.gt(uncertainty.quantile(0.95, axis=1), axis=0)
            if uncertainty is not None else None
        )
        return SignalBundle(
            weights=weights, scores=scores, confidence=confidence, degraded=degraded,
            contributions=contributions,
            metadata={
                "version_id": self.version.get("id"), "horizon": self.horizon,
                "prediction_source": "rolling_oof", **self._model_metadata,
            },
        )

    def target_weights(self, panel: PanelDict) -> pd.DataFrame:
        return self.signal_bundle(panel).weights
