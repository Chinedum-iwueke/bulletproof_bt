from __future__ import annotations

import pandas as pd
import pytest

from bt.core.enums import OrderType, PositionState, Side
from bt.core.types import OrderIntent, Position
from bt.exec.services.kill_switch import KillSwitch
from bt.exec.services.live_controls import CanaryGuard, load_canary_policy


def test_canary_policy_validation_rejects_bad_qty() -> None:
    with pytest.raises(ValueError):
        load_canary_policy(
            {
                "live_controls": {"enabled": True, "canary_mode": True},
                "canary": {"max_order_qty": 0},
            }
        )


def test_canary_blocks_symbol_and_notional() -> None:
    ts = pd.Timestamp("2026-01-01T00:00:00Z")
    guard = CanaryGuard(
        load_canary_policy(
            {
                "live_controls": {"enabled": True, "canary_mode": True},
                "canary": {
                    "allowed_symbols": ["BTCUSDT"],
                    "max_order_qty": 0.01,
                    "max_notional_usd": 100.0,
                },
            }
        )
    )
    reason = guard.validate_intent(
        intent=OrderIntent(ts=ts, symbol="ETHUSDT", side=Side.BUY, qty=0.1, order_type=OrderType.MARKET, limit_price=None, reason="test"),
        open_orders=[],
        positions=[],
        current_price=2000.0,
    )
    assert reason == "symbol_not_allowed:ETHUSDT"

    reason2 = guard.validate_intent(
        intent=OrderIntent(ts=ts, symbol="BTCUSDT", side=Side.BUY, qty=0.01, order_type=OrderType.MARKET, limit_price=None, reason="test"),
        open_orders=[],
        positions=[
            Position(
                symbol="BTCUSDT",
                state=PositionState.OPEN,
                side=Side.BUY,
                qty=1.0,
                avg_entry_price=100.0,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                mae_price=None,
                mfe_price=None,
                opened_ts=ts,
                closed_ts=None,
            )
        ],
        current_price=20000.0,
    )
    assert reason2 == "max_notional_usd_exceeded"


def test_kill_switch_freezes_after_transport_threshold() -> None:
    ks = KillSwitch()
    now = pd.Timestamp("2026-01-01T00:00:00Z")
    ks.record_transport_error(ts=now, max_consecutive_transport_errors=2)
    assert not ks.state().freeze_new_orders
    ks.record_transport_error(ts=now, max_consecutive_transport_errors=2)
    assert ks.state().freeze_new_orders
    assert ks.state().reason == "max_consecutive_transport_errors"
