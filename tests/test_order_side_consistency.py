from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from bt.api import run_backtest
from bt.core.enums import OrderState, OrderType, Side
from bt.core.types import Bar, Order, Signal
from bt.logging.jsonl import JsonlWriter
from bt.orders.side import side_from_signed_qty, validate_order_side_consistency
from bt.risk.risk_engine import RiskEngine


def _engine() -> RiskEngine:
    return RiskEngine(
        max_positions=5,
        config={
            "risk": {
                "mode": "r_fixed",
                "r_per_trade": 0.01,
                "qty_rounding": "none",
                "stop": {},
                "stop_resolution": "safe",
                "allow_legacy_proxy": True,
            }
        },
    )


def test_side_from_signed_qty_mapping() -> None:
    assert side_from_signed_qty(1.0) == Side.BUY
    assert side_from_signed_qty(-1.0) == Side.SELL
    with pytest.raises(ValueError):
        side_from_signed_qty(0.0)


def test_validate_order_side_consistency_rejects_mismatch() -> None:
    with pytest.raises(ValueError, match="side/qty sign mismatch"):
        validate_order_side_consistency(
            side=Side.BUY,
            qty=2.0,
            signed_qty=-2.0,
            where="unit-test",
        )


def test_validate_order_side_consistency_rejects_zero_qty() -> None:
    with pytest.raises(ValueError, match="must be non-zero"):
        validate_order_side_consistency(
            side=Side.BUY,
            qty=0.0,
            where="unit-test",
        )


def test_entry_side_and_qty_are_canonical() -> None:
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    engine = _engine()
    bar = Bar(ts=ts, symbol="BTC", open=100.0, high=110.0, low=90.0, close=100.0, volume=1.0)

    buy_signal = Signal(ts=ts, symbol="BTC", side=Side.BUY, signal_type="unit", confidence=1.0, metadata={})
    buy_intent, _ = engine.signal_to_order_intent(
        ts=ts,
        signal=buy_signal,
        bar=bar,
        equity=10_000.0,
        free_margin=10_000.0,
        open_positions=0,
        max_leverage=2.0,
        current_qty=0.0,
    )
    assert buy_intent is not None
    assert buy_intent.qty > 0
    assert buy_intent.side == Side.BUY

    sell_signal = Signal(ts=ts, symbol="BTC", side=Side.SELL, signal_type="unit", confidence=1.0, metadata={})
    sell_intent, _ = engine.signal_to_order_intent(
        ts=ts,
        signal=sell_signal,
        bar=bar,
        equity=10_000.0,
        free_margin=10_000.0,
        open_positions=0,
        max_leverage=2.0,
        current_qty=0.0,
    )
    assert sell_intent is not None
    assert sell_intent.qty < 0
    assert sell_intent.side == Side.SELL


def test_exit_side_and_qty_are_canonical() -> None:
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    engine = _engine()
    bar = Bar(ts=ts, symbol="BTC", open=100.0, high=110.0, low=90.0, close=100.0, volume=1.0)

    close_long = Signal(ts=ts, symbol="BTC", side=Side.SELL, signal_type="coinflip_exit", confidence=1.0, metadata={"close_only": True})
    intent_long, _ = engine.signal_to_order_intent(
        ts=ts,
        signal=close_long,
        bar=bar,
        equity=10_000.0,
        free_margin=0.0,
        open_positions=1,
        max_leverage=2.0,
        current_qty=3.0,
    )
    assert intent_long is not None
    assert intent_long.qty < 0
    assert intent_long.side == Side.SELL

    close_short = Signal(ts=ts, symbol="BTC", side=Side.BUY, signal_type="coinflip_exit", confidence=1.0, metadata={"close_only": True})
    intent_short, _ = engine.signal_to_order_intent(
        ts=ts,
        signal=close_short,
        bar=bar,
        equity=10_000.0,
        free_margin=0.0,
        open_positions=1,
        max_leverage=2.0,
        current_qty=-2.0,
    )
    assert intent_short is not None
    assert intent_short.qty > 0
    assert intent_short.side == Side.BUY


def test_jsonl_writer_enforces_order_invariants(tmp_path: Path) -> None:
    writer = JsonlWriter(tmp_path / "decisions.jsonl")
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bad_order = Order(
        id="o-1",
        ts_submitted=ts,
        symbol="BTC",
        side=Side.BUY,
        qty=1.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        state=OrderState.NEW,
        metadata={},
    )
    bad_signal = Signal(ts=ts, symbol="BTC", side=Side.SELL, signal_type="unit", confidence=1.0, metadata={})
    with pytest.raises(ValueError, match="disagrees"):
        writer.write({"ts": ts, "order_qty": 1.0, "order": bad_order, "signal": bad_signal})
    writer.close()


def test_integration_logged_orders_obey_side_qty_invariants(tmp_path: Path) -> None:
    run_dir = Path(
        run_backtest(
            config_path="configs/engine.yaml",
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "out"),
            run_name="side-consistency",
        )
    )
    decisions_path = run_dir / "decisions.jsonl"
    for line in decisions_path.read_text(encoding="utf-8").splitlines():
        rec = json.loads(line)
        if not rec.get("approved") or "order" not in rec:
            continue
        order = rec["order"]
        qty = float(order["qty"])
        assert qty > 0
        signed_qty = float(rec["order_qty"])
        assert signed_qty != 0
        side = order["side"]
        expected = "BUY" if signed_qty > 0 else "SELL"
        assert side == expected
