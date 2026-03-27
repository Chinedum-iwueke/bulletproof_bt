from __future__ import annotations

import pandas as pd
import pytest

from bt.core.enums import OrderType, Side
from bt.core.types import Bar, Signal
from bt.risk.risk_engine import RiskEngine


def _bar(*, ts: pd.Timestamp, symbol: str, high: float, low: float, close: float) -> Bar:
    return Bar(
        ts=ts,
        symbol=symbol,
        open=low,
        high=high,
        low=low,
        close=close,
        volume=1.0,
    )


def _signal(*, ts: pd.Timestamp, symbol: str, side: Side | None, stop_price: float | None = None) -> Signal:
    metadata: dict[str, float] = {}
    if stop_price is not None:
        metadata["stop_price"] = stop_price
    return Signal(
        ts=ts,
        symbol=symbol,
        side=side,
        signal_type="unit",
        confidence=1.0,
        metadata=metadata,
    )


def _risk_config(*, mode: str = "r_fixed", r_per_trade: float = 0.01, qty_rounding: str = "none", min_stop_distance: float | None = None, min_stop_distance_pct: float = 0.001, max_notional_pct_equity: float = 1.0) -> dict[str, object]:
    risk_cfg: dict[str, object] = {"mode": mode, "r_per_trade": r_per_trade, "qty_rounding": qty_rounding, "stop": {}, "min_stop_distance_pct": min_stop_distance_pct, "max_notional_pct_equity": max_notional_pct_equity}
    if min_stop_distance is not None:
        risk_cfg["min_stop_distance"] = min_stop_distance
    return {"risk": risk_cfg}


def test_signal_to_order_intent_approves_and_sizes() -> None:
    engine = RiskEngine(max_positions=5, config=_risk_config())
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=110, low=100, close=105)
    signal = _signal(ts=ts, symbol="BTC", side=Side.BUY, stop_price=95.0)

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000,
        free_margin=10_000,
        open_positions=0,
        max_leverage=2.0,
        current_qty=0.0,
    )

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.metadata["scaled_by_margin"] is False
    assert order_intent.qty == pytest.approx(10.0)

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.order_type == OrderType.MARKET
    assert order_intent.limit_price is None
    assert order_intent.qty == pytest.approx(10.0)


def test_signal_to_order_intent_rejects_max_positions() -> None:
    engine = RiskEngine(max_positions=1, config=_risk_config())
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=110, low=100, close=105)
    signal = _signal(ts=ts, symbol="BTC", side=Side.BUY, stop_price=95.0)

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000,
        free_margin=10_000,
        open_positions=1,
        max_leverage=2.0,
        current_qty=0.0,
    )

    assert order_intent is None
    assert reason.startswith("risk_rejected:")


def test_signal_to_order_intent_applies_notional_cap() -> None:
    engine = RiskEngine(max_positions=5, max_notional_per_symbol=500, config=_risk_config(r_per_trade=0.5))
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=110, low=100, close=105)
    signal = _signal(ts=ts, symbol="BTC", side=Side.BUY, stop_price=95.0)

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000,
        free_margin=10_000,
        open_positions=0,
        max_leverage=2.0,
        current_qty=0.0,
    )

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.metadata["cap_applied"] is True
    assert order_intent.metadata["notional_est"] <= 500 + 1e-9


def test_signal_to_order_intent_rejects_no_side() -> None:
    engine = RiskEngine(max_positions=5, config=_risk_config())
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=110, low=100, close=105)
    signal = _signal(ts=ts, symbol="BTC", side=None)

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000,
        free_margin=10_000,
        open_positions=0,
        max_leverage=2.0,
        current_qty=0.0,
    )

    assert order_intent is None
    assert "no_side" in reason


def test_signal_to_order_intent_scales_when_margin_insufficient() -> None:
    engine = RiskEngine(max_positions=5, config=_risk_config())
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=100.5, low=100.0, close=100.0)
    signal = _signal(ts=ts, symbol="BTC", side=Side.BUY, stop_price=95.0)

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000,
        free_margin=50,
        open_positions=0,
        max_leverage=2.0,
        current_qty=0.0,
    )

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.metadata["scaled_by_margin"] is True


def test_signal_to_order_intent_allows_when_margin_sufficient() -> None:
    engine = RiskEngine(max_positions=5, config=_risk_config())
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=110, low=100, close=105)
    signal = _signal(ts=ts, symbol="BTC", side=Side.BUY, stop_price=95.0)

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000,
        free_margin=10_000,
        open_positions=0,
        max_leverage=2.0,
        current_qty=0.0,
    )


def test_signal_to_order_intent_rejects_pyramiding() -> None:
    engine = RiskEngine(max_positions=5, config=_risk_config())
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=110, low=100, close=105)
    signal = _signal(ts=ts, symbol="BTC", side=Side.BUY, stop_price=95.0)

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000,
        free_margin=10_000,
        open_positions=1,
        max_leverage=2.0,
        current_qty=5.0,
    )

    assert order_intent is None
    assert "already_in_position" in reason


def test_signal_to_order_intent_flip_generates_net_sell_order() -> None:
    engine = RiskEngine(max_positions=5, config=_risk_config())
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=110, low=100, close=105)
    signal = _signal(ts=ts, symbol="BTC", side=Side.SELL, stop_price=110.0)

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000,
        free_margin=10_000,
        open_positions=1,
        max_leverage=2.0,
        current_qty=5.0,
    )

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.qty < 0
    assert abs(order_intent.qty) > 5.0


def test_signal_to_order_intent_flip_scales_for_margin() -> None:
    engine = RiskEngine(max_positions=5, config=_risk_config())
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=110, low=100, close=100)
    signal = _signal(ts=ts, symbol="BTC", side=Side.SELL, stop_price=110.0)

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000,
        free_margin=300,
        open_positions=1,
        max_leverage=2.0,
        current_qty=5.0,
    )

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.metadata["scaled_by_margin"] is True


def test_signal_to_order_intent_rejects_when_stop_distance_pct_too_small() -> None:
    engine = RiskEngine(max_positions=5, config=_risk_config(min_stop_distance_pct=0.2))
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=110, low=100, close=105)
    signal = _signal(ts=ts, symbol="BTC", side=Side.BUY, stop_price=95.0)

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000,
        free_margin=10_000,
        open_positions=0,
        max_leverage=2.0,
        current_qty=0.0,
    )

    assert order_intent is None
    assert reason == "risk_rejected:min_stop_distance_violation"


def test_signal_to_order_intent_caps_notional_by_pct_equity() -> None:
    engine = RiskEngine(max_positions=5, config=_risk_config(r_per_trade=0.5, max_notional_pct_equity=0.05))
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=110, low=100, close=100)
    signal = _signal(ts=ts, symbol="BTC", side=Side.SELL, stop_price=110.0)

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000,
        free_margin=10_000,
        open_positions=0,
        max_leverage=3.0,
        current_qty=0.0,
    )

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.qty == pytest.approx(-5.0)
    assert order_intent.metadata["cap_applied"] is True
    assert order_intent.metadata["cap_reason"] == "max_notional_pct_equity"
    assert order_intent.metadata["max_notional"] == pytest.approx(500.0)
    assert order_intent.metadata["notional_est"] == pytest.approx(500.0)


def test_signal_to_order_intent_uses_fixed_bps_fee_and_slippage_buffers() -> None:
    engine = RiskEngine(
        max_positions=5,
        maker_fee_bps=1.0,
        taker_fee_bps=2.0,
        slippage_k_proxy=0.0,
        config={
            "model": "fixed_bps",
            "fixed_bps": 5.0,
            "risk": {"mode": "r_fixed", "r_per_trade": 0.01, "qty_rounding": "none", "stop": {}},
        },
    )
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=100.0, low=99.0, close=100.0)
    signal = _signal(ts=ts, symbol="BTC", side=Side.BUY, stop_price=99.0)

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
    assert order_intent.metadata["margin_fee_buffer"] > 0.0
    assert order_intent.metadata["margin_slippage_buffer"] > 0.0

    total_required = (
        order_intent.metadata["margin_required"]
        + order_intent.metadata["margin_fee_buffer"]
        + order_intent.metadata["margin_slippage_buffer"]
        + order_intent.metadata["margin_adverse_move_buffer"]
    )
    assert total_required <= order_intent.metadata["free_margin"]


def test_signal_to_order_intent_includes_canonical_margin_fields_and_non_negative_post_margin() -> None:
    engine = RiskEngine(max_positions=5, margin_buffer_tier=3, config=_risk_config(r_per_trade=2.0))
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=105.0, low=95.0, close=100.0)
    signal = _signal(ts=ts, symbol="BTC", side=Side.BUY, stop_price=99.0)

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000.0,
        free_margin=2_500.0,
        open_positions=0,
        max_leverage=2.0,
        current_qty=0.0,
    )

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.metadata["scaled_by_margin"] is True
    assert order_intent.metadata["mark_price_used_for_margin"] == pytest.approx(105.0)
    assert order_intent.metadata["maintenance_required"] == pytest.approx(order_intent.metadata["margin_required"])
    assert order_intent.metadata["free_margin_post"] >= 0.0

def test_signal_to_order_intent_supports_size_factor_above_one_when_allowed() -> None:
    engine = RiskEngine(max_positions=5, config=_risk_config())
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=110, low=100, close=105)
    signal = Signal(
        ts=ts,
        symbol="BTC",
        side=Side.BUY,
        signal_type="unit",
        confidence=1.0,
        metadata={"stop_price": 95.0, "size_factor_t": 1.5, "size_factor_min": 0.25, "size_factor_max": 1.5},
    )

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000,
        free_margin=10_000,
        open_positions=0,
        max_leverage=2.0,
        current_qty=0.0,
    )

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.qty == pytest.approx(15.0)
    assert order_intent.metadata["size_factor_t"] == pytest.approx(1.5)
    assert order_intent.metadata["size_factor_min"] == pytest.approx(0.25)
    assert order_intent.metadata["size_factor_max"] == pytest.approx(1.5)
