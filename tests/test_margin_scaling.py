from __future__ import annotations

import pandas as pd
import pytest

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.risk.risk_engine import RiskEngine


def test_margin_scaling_applies_and_approves() -> None:
    engine = RiskEngine(
        max_positions=5,
                taker_fee_bps=10.0,
        slippage_k_proxy=0.001,
        margin_buffer_tier=2,
        config={"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "qty_rounding": "none", "stop": {}}},
    )
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = Bar(ts=ts, symbol="BTC", open=100.0, high=101.0, low=100.0, close=100.0, volume=1.0)
    signal = Signal(
        ts=ts,
        symbol="BTC",
        side=Side.BUY,
        signal_type="unit",
        confidence=1.0,
        metadata={"stop_price": 99.0},
    )

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000.0,
        free_margin=150.0,
        open_positions=0,
        max_leverage=2.0,
        current_qty=0.0,
    )

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.metadata["scaled_by_margin"] is True

    expected_max_affordable_qty = (150.0 * 0.99) / (101.0 * (0.5 + 0.001 + 0.001 + (2.0 / 101.0)))
    assert abs(order_intent.qty) == pytest.approx(expected_max_affordable_qty)
    assert order_intent.metadata["margin_required"] <= 150.0


def test_signal_episode_does_not_scale_entries_by_portfolio_free_margin() -> None:
    engine = RiskEngine(
        max_positions=5,
        taker_fee_bps=10.0,
        slippage_k_proxy=0.001,
        margin_buffer_tier=2,
        config={
            "initial_cash": 10_000.0,
            "research": {"portfolio_constraints_applied": False, "research_mode": "signal_episode"},
            "risk": {
                "mode": "r_fixed",
                "r_per_trade": 0.01,
                "qty_rounding": "none",
                "stop": {},
                "signal_episode_sizing_equity": "initial_cash",
                "max_notional_pct_equity": 10.0,
            },
        },
    )
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = Bar(ts=ts, symbol="BTC", open=100.0, high=101.0, low=100.0, close=100.0, volume=1.0)
    signal = Signal(
        ts=ts,
        symbol="BTC",
        side=Side.BUY,
        signal_type="unit",
        confidence=1.0,
        metadata={"stop_price": 99.0},
    )

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=1_000.0,
        free_margin=1e-9,
        open_positions=999,
        max_leverage=1.0,
        current_qty=0.0,
    )

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.qty == pytest.approx(100.0)
    assert order_intent.metadata["risk_budget"] == pytest.approx(100.0)
    assert order_intent.metadata["risk_amount"] == pytest.approx(100.0)
    assert order_intent.metadata["risk_utilization_pct"] == pytest.approx(1.0)
    assert order_intent.metadata["scaled_by_margin"] is False


def test_near_zero_actual_stop_risk_is_rejected() -> None:
    engine = RiskEngine(
        max_positions=5,
        config={"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "qty_rounding": "none", "stop": {}, "max_notional_pct_equity": 1e-14}},
    )
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = Bar(ts=ts, symbol="BTC", open=100.0, high=100.0, low=100.0, close=100.0, volume=1.0)
    signal = Signal(
        ts=ts,
        symbol="BTC",
        side=Side.BUY,
        signal_type="unit",
        confidence=1.0,
        metadata={"stop_price": 99.0},
    )

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000.0,
        free_margin=10_000.0,
        open_positions=0,
        max_leverage=1.0,
        current_qty=0.0,
    )

    assert order_intent is None
    assert reason == "risk_rejected:min_risk_utilization_violation"


def test_margin_scaling_reserves_maintenance_headroom() -> None:
    engine = RiskEngine(
        max_positions=5,
        taker_fee_bps=0.0,
        slippage_k_proxy=0.0,
        margin_buffer_tier=1,
        config={
            "risk": {
                "mode": "r_fixed",
                "r_per_trade": 1.0,
                "qty_rounding": "none",
                "stop": {},
                "maintenance_free_margin_pct": 0.01,
                "max_notional_pct_equity": 100.0,
            }
        },
    )
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = Bar(ts=ts, symbol="BTC", open=100.0, high=100.0, low=100.0, close=100.0, volume=1.0)
    signal = Signal(
        ts=ts,
        symbol="BTC",
        side=Side.BUY,
        signal_type="unit",
        confidence=1.0,
        metadata={"stop_price": 99.0},
    )

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=100_000.0,
        free_margin=100_000.0,
        open_positions=0,
        max_leverage=1.0,
        current_qty=0.0,
    )

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.metadata["scaled_by_margin"] is True
    assert order_intent.metadata["maintenance_free_margin_pct"] == pytest.approx(0.01)
    assert order_intent.metadata["max_total_required"] == pytest.approx(99_000.0)
    assert order_intent.metadata["total_required"] <= order_intent.metadata["max_total_required"]
