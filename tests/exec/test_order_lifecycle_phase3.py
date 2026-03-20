from __future__ import annotations

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Fill
from bt.exec.lifecycle import OrderLifecycleState, accumulate_fills, can_transition, fill_identity_key, validate_transition


def test_valid_and_invalid_lifecycle_transitions() -> None:
    assert can_transition(None, OrderLifecycleState.SUBMITTED)
    assert can_transition(OrderLifecycleState.ACKNOWLEDGED, OrderLifecycleState.PARTIALLY_FILLED)
    invalid = validate_transition(OrderLifecycleState.FILLED, OrderLifecycleState.ACKNOWLEDGED)
    assert not invalid.valid


def test_partial_fill_aggregation_and_terminal_detection() -> None:
    ts = pd.Timestamp("2026-01-01T00:00:00Z")
    fills = [
        Fill(order_id="o1", ts=ts, symbol="BTCUSDT", side=Side.BUY, qty=0.4, price=100.0, fee=0.0, slippage=0.0),
        Fill(order_id="o1", ts=ts, symbol="BTCUSDT", side=Side.BUY, qty=0.6, price=101.0, fee=0.0, slippage=0.0),
    ]
    agg = accumulate_fills(order_id="o1", requested_qty=1.0, fills=fills)
    assert agg.cumulative_qty == 1.0
    assert agg.is_terminal


def test_fill_identity_key_is_deterministic() -> None:
    fill = Fill(order_id="o1", ts=pd.Timestamp("2026-01-01T00:00:00Z"), symbol="BTCUSDT", side=Side.BUY, qty=1.0, price=100.0, fee=0.0, slippage=0.0)
    assert fill_identity_key(fill) == fill_identity_key(fill)
