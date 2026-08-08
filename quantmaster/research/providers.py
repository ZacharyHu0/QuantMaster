"""Reference factor, label, risk-model and futures-continuous computations."""

from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd

from quantmaster.research.kernel import Kernel


def _price_column(frame: pd.DataFrame) -> str:
    for column in ("research_price", "settle_adj", "close_adj", "settle", "close"):
        if column in frame and pd.to_numeric(frame[column], errors="coerce").notna().any():
            return column
    raise ValueError("行情缺少可用研究价格")


def _pivot(frame: pd.DataFrame, column: str) -> pd.DataFrame:
    value = frame[["trade_date", "symbol", column]].copy()
    value["trade_date"] = pd.to_datetime(value["trade_date"])
    value[column] = pd.to_numeric(value[column], errors="coerce")
    return value.pivot(index="trade_date", columns="symbol", values=column).sort_index()


def _long(panel: pd.DataFrame, name: str) -> pd.DataFrame:
    value = panel.rename_axis(index="trade_date", columns="symbol").reset_index()
    return value.melt(id_vars="trade_date", var_name="symbol", value_name=name)


def compute_core_factors(bars: pd.DataFrame, kernel: Kernel | None = None) -> pd.DataFrame:
    """Compute the six portable factor outputs in one shared scan."""
    if bars.empty:
        return pd.DataFrame(columns=["trade_date", "symbol"])
    kernel = kernel or Kernel()
    price = _pivot(bars, _price_column(bars))
    returns = price.pct_change(fill_method=None)
    volume = _pivot(bars, "volume").reindex_like(price)
    amount = (
        _pivot(bars, "amount").reindex_like(price)
        if "amount" in bars else price * volume
    )
    volume_change = np.log1p(volume.clip(lower=0)).diff()
    raw = {
        "cross_momentum_20d": price / price.shift(20) - 1.0,
        "cross_reversal_5d": -(price / price.shift(5) - 1.0),
        "cross_realized_vol_20d": pd.DataFrame(
            kernel.rolling_std(returns.to_numpy(), 20), index=price.index, columns=price.columns,
        ),
        "cross_volume_ratio_20d": volume / volume.rolling(20, min_periods=10).mean(),
        "cross_price_volume_corr_20d": pd.DataFrame(
            kernel.rolling_corr(returns.to_numpy(), volume_change.to_numpy(), 20),
            index=price.index, columns=price.columns,
        ),
        "cross_amihud_20d": (
            returns.abs() / amount.replace(0, np.nan)
        ).rolling(20, min_periods=10).mean() * 1_000_000,
    }
    result: pd.DataFrame | None = None
    for name, panel in raw.items():
        standardized = pd.DataFrame(
            kernel.robust_standardize(panel.to_numpy(), 5.0),
            index=panel.index, columns=panel.columns,
        )
        table = _long(standardized, name)
        result = table if result is None else result.merge(
            table, on=["trade_date", "symbol"], how="outer", validate="one_to_one",
        )
    assert result is not None
    return result.sort_values(["trade_date", "symbol"]).reset_index(drop=True)


def compute_forward_labels(
    bars: pd.DataFrame, horizons: Iterable[int] = (1, 3, 5, 7, 10, 20, 30),
) -> pd.DataFrame:
    if bars.empty:
        return pd.DataFrame(columns=["trade_date", "symbol"])
    price = _pivot(bars, _price_column(bars))
    result: pd.DataFrame | None = None
    for horizon in horizons:
        name = f"fwd_return_{int(horizon)}d"
        table = _long(price.shift(-int(horizon)) / price - 1.0, name)
        result = table if result is None else result.merge(
            table, on=["trade_date", "symbol"], how="outer", validate="one_to_one",
        )
    assert result is not None
    return result.sort_values(["trade_date", "symbol"]).reset_index(drop=True)


def _industry_fill(values: pd.Series, industries: pd.Series | None) -> pd.Series:
    result = values.copy()
    if not result.notna().any():
        return result
    if industries is not None and industries.notna().any():
        result = result.fillna(result.groupby(industries).transform("median"))
    return result.fillna(result.median())


def _weighted_standardize(values: pd.Series, weights: pd.Series) -> pd.Series:
    result = pd.Series(np.nan, index=values.index, dtype="float64")
    finite = values.notna() & weights.notna() & weights.gt(0)
    if not finite.any():
        return result
    clean = values.loc[finite].astype(float)
    median = clean.median()
    mad = (clean - median).abs().median() * 1.4826
    if mad > 0:
        clean = clean.clip(median - 5 * mad, median + 5 * mad)
    clean_weights = weights.loc[finite].astype(float)
    mean = np.average(clean, weights=clean_weights)
    variance = np.average((clean - mean) ** 2, weights=clean_weights)
    result.loc[finite] = (clean - mean) / np.sqrt(variance) if variance > 0 else 0.0
    return result


def compute_qm_style_v1(
    bars: pd.DataFrame,
    daily_basic: pd.DataFrame,
) -> pd.DataFrame:
    """Five transparent style descriptors; this is deliberately not branded CNE6."""
    required = {"trade_date", "symbol", "total_mv", "pb"}
    if not required.issubset(daily_basic):
        raise ValueError(f"QM_STYLE_V1 缺少字段: {sorted(required - set(daily_basic))}")
    price_column = _price_column(bars)
    market = bars[["trade_date", "symbol", price_column]].copy()
    market["trade_date"] = pd.to_datetime(market["trade_date"])
    market[price_column] = pd.to_numeric(market[price_column], errors="coerce")
    market = market.sort_values(["symbol", "trade_date"])
    log_price = np.log(market[price_column].where(market[price_column] > 0))
    market["MOMENTUM_raw"] = (
        log_price.groupby(market["symbol"]).shift(21)
        - log_price.groupby(market["symbol"]).shift(252)
    )
    returns = log_price.groupby(market["symbol"]).diff()
    market["VOLATILITY_raw"] = returns.groupby(market["symbol"]).transform(
        lambda value: value.rolling(60, min_periods=40).std()
    )
    basic = daily_basic.copy()
    basic["trade_date"] = pd.to_datetime(basic["trade_date"])
    value = market.merge(basic, on=["trade_date", "symbol"], how="left", validate="one_to_one")
    total_mv = pd.to_numeric(value["total_mv"], errors="coerce")
    pb = pd.to_numeric(value["pb"], errors="coerce")
    value["SIZE_raw"] = np.log(total_mv.where(total_mv > 0))
    value["VALUE_raw"] = -np.log(pb.where(pb > 0))
    if "turnover_rate_f" in value:
        turnover = pd.to_numeric(value["turnover_rate_f"], errors="coerce")
    elif "turnover_rate" in value:
        turnover = pd.to_numeric(value["turnover_rate"], errors="coerce")
    else:
        raise ValueError("QM_STYLE_V1 缺少 turnover_rate_f/turnover_rate")
    if "turnover_rate" in value:
        turnover = turnover.fillna(pd.to_numeric(value["turnover_rate"], errors="coerce"))
    value["LIQUIDITY_raw"] = turnover.groupby(value["symbol"]).transform(
        lambda item: item.rolling(20, min_periods=12).mean()
    )
    weights = np.sqrt(total_mv.where(total_mv > 0))
    for exposure in ("SIZE", "VALUE", "MOMENTUM", "VOLATILITY", "LIQUIDITY"):
        raw = f"{exposure}_raw"
        normalized = pd.Series(np.nan, index=value.index, dtype="float64")
        for _date, indices in value.groupby("trade_date", sort=False).groups.items():
            index = list(indices)
            industries = value.loc[index, "industry"] if "industry" in value else None
            filled = _industry_fill(value.loc[index, raw], industries)
            normalized.loc[index] = _weighted_standardize(filled, weights.loc[index])
        value[exposure] = normalized
    columns = ["trade_date", "symbol"]
    for exposure in ("SIZE", "VALUE", "MOMENTUM", "VOLATILITY", "LIQUIDITY"):
        columns.extend((f"{exposure}_raw", exposure))
    return value[columns].sort_values(["trade_date", "symbol"]).reset_index(drop=True)


def build_future_continuous(
    contract_bars: pd.DataFrame,
    mapping: pd.DataFrame,
) -> pd.DataFrame:
    """Build forward ratio-adjusted research series while retaining the tradable contract."""
    required_bars = {"trade_date", "symbol", "close"}
    required_mapping = {"trade_date", "symbol", "mapping_ts_code"}
    if not required_bars.issubset(contract_bars) or not required_mapping.issubset(mapping):
        raise ValueError("期货连续序列缺少合约行情或主力映射字段")
    bars = contract_bars.copy()
    maps = mapping.copy()
    bars["trade_date"] = pd.to_datetime(bars["trade_date"])
    maps["trade_date"] = pd.to_datetime(maps["trade_date"])
    bars["symbol"] = bars["symbol"].astype(str).str.upper()
    maps["mapping_ts_code"] = maps["mapping_ts_code"].astype(str).str.upper()
    bars_lookup = bars.set_index(["trade_date", "symbol"])
    rows = []
    for continuous_symbol, group in maps.groupby("symbol"):
        group = group.sort_values("trade_date")
        scale = 1.0
        previous_contract = ""
        previous_date: pd.Timestamp | None = None
        for item in group.itertuples(index=False):
            trade_date = pd.Timestamp(item.trade_date)
            contract = str(item.mapping_ts_code)
            key = (trade_date, contract)
            if key not in bars_lookup.index:
                continue
            roll = bool(previous_contract and previous_contract != contract)
            if roll and previous_date is not None:
                old_key, new_key = (previous_date, previous_contract), (previous_date, contract)
                if old_key in bars_lookup.index and new_key in bars_lookup.index:
                    old = bars_lookup.loc[old_key]
                    new = bars_lookup.loc[new_key]
                    old_price = float(old.get("settle", old.get("close", np.nan)))
                    new_price = float(new.get("settle", new.get("close", np.nan)))
                    if np.isfinite(old_price) and np.isfinite(new_price) and new_price != 0:
                        scale *= old_price / new_price
            selected = bars_lookup.loc[key]
            if isinstance(selected, pd.DataFrame):
                selected = selected.iloc[-1]
            row = selected.to_dict()
            row.update({
                "trade_date": trade_date,
                "symbol": str(continuous_symbol).upper(),
                "mapping_ts_code": contract,
                "roll_flag": roll,
                "continuous_adj_factor": scale,
            })
            for column in ("open", "high", "low", "close", "settle", "pre_settle"):
                if column in row and pd.notna(row[column]):
                    row[f"{column}_adj"] = float(row[column]) * scale
            row["research_price"] = row.get("settle_adj", row.get("close_adj"))
            rows.append(row)
            previous_contract, previous_date = contract, trade_date
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(["symbol", "trade_date"]).reset_index(drop=True)
