from __future__ import annotations

import pandas as pd
import pytest

from bt.core.enums import OrderType, Side
from bt.core.types import OrderIntent
from bt.exec.adapters.simulated import SimulatedBrokerAdapter
from bt.exec.services.execution_router import ExecutionRouter
from bt.exec.services.portfolio_runner import PortfolioRunner
from bt.institutional.realtime_risk import RealtimeRiskError
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel
from bt.portfolio.portfolio import Portfolio


def test_external_broker_mode_cannot_submit_without_risk005_receipt() -> None:
    adapter = SimulatedBrokerAdapter(
        execution_model=ExecutionModel(
            fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=0.0),
            slippage_model=SlippageModel(
                k=0.0,
                atr_pct_cap=0.0,
                impact_cap=0.0,
                fixed_bps=0.0,
            ),
            spread_mode="none",
            spread_bps=0.0,
            spread_pips=None,
            intrabar_mode="worst_case",
            delay_bars=0,
            instrument=None,
        )
    )
    adapter.start()
    router = ExecutionRouter(
        run_id="run-1",
        mode="live_broker",
        adapter=adapter,
        portfolio_runner=PortfolioRunner(portfolio=Portfolio(initial_cash=10_000.0)),
        store=None,
        save_processed_event_ids=False,
    )
    intent = OrderIntent(
        ts=pd.Timestamp("2026-09-12T12:00:00Z"),
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=0.01,
        order_type=OrderType.MARKET,
        limit_price=None,
        reason="must fail closed",
    )
    with pytest.raises(RealtimeRiskError, match="exact RISK-005"):
        router.submit_order(order_seq=1, intent=intent, ts=intent.ts)
    assert adapter.fetch_open_orders() == []
