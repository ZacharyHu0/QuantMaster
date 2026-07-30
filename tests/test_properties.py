from __future__ import annotations

import json
import math
import uuid

import numpy as np
import pandas as pd
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from quantmaster.portfolio.ledger import Ledger, TradeRecord
from quantmaster.runtime.json import strict_json_dumps


@given(
    st.recursive(
        st.one_of(
            st.none(),
            st.booleans(),
            st.integers(),
            st.floats(allow_nan=True, allow_infinity=True),
            st.text(max_size=20),
        ),
        lambda children: st.one_of(
            st.lists(children, max_size=8),
            st.dictionaries(st.text(min_size=1, max_size=8), children, max_size=8),
        ),
        max_leaves=30,
    )
)
@settings(max_examples=100, deadline=None)
def test_strict_json_is_always_rfc_parseable(value):
    encoded = strict_json_dumps(value)
    decoded = json.loads(encoded, parse_constant=lambda token: (_ for _ in ()).throw(AssertionError(token)))
    assert decoded is not ...


@given(
    st.lists(
        st.tuples(
            st.integers(min_value=-200, max_value=200).filter(bool),
            st.floats(min_value=0.01, max_value=10000, allow_nan=False, allow_infinity=False),
        ),
        min_size=1,
        max_size=20,
    )
)
@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_fifo_ledger_preserves_inventory_and_finite_known_pnl(tmp_path, operations):
    ledger = Ledger(tmp_path / f"property-{uuid.uuid4().hex}.sqlite")
    balance = 0
    day = pd.Timestamp("2024-01-01")
    for raw_shares, price in operations:
        day += pd.Timedelta(days=1)
        if raw_shares > 0:
            shares = raw_shares
            side = "buy"
            balance += shares
        elif balance:
            shares = min(abs(raw_shares), balance)
            side = "sell"
            balance -= shares
        else:
            continue
        ledger.add_trade(
            TradeRecord(
                date=str(day.date()),
                symbol="600000.SH",
                side=side,
                price=float(price),
                shares=float(shares),
                fee=0.0,
            )
        )
    positions = ledger.positions()
    if not positions:
        assert balance == 0
        return
    position = positions[0]
    assert position.shares == balance
    assert position.cost_basis_complete
    assert position.unknown_cost_shares == 0
    assert math.isfinite(position.realized_pnl)
    assert math.isfinite(position.avg_cost)


@given(
    st.lists(
        st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False),
        min_size=1,
        max_size=200,
    )
)
@settings(
    max_examples=50,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_finite_numeric_inputs_remain_finite_through_parquet(tmp_path, values):
    path = tmp_path / f"values-{uuid.uuid4().hex}.parquet"
    frame = pd.DataFrame({"value": np.asarray(values, dtype=float)})
    frame.to_parquet(path, index=False)
    restored = pd.read_parquet(path)
    assert np.isfinite(restored["value"]).all()
