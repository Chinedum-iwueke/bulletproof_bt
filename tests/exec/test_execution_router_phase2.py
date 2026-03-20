from __future__ import annotations

import pandas as pd

from bt.core.enums import OrderType, Side
from bt.core.types import Bar, OrderIntent
from bt.exec.adapters.simulated import SimulatedBrokerAdapter
from bt.exec.services.execution_router import ExecutionRouter
from bt.exec.services.portfolio_runner import PortfolioRunner
from bt.exec.state.sqlite_store import SQLiteExecutionStateStore
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel
from bt.portfolio.portfolio import Portfolio


def _bar(ts: str, close: float) -> Bar:
    return Bar(ts=pd.Timestamp(ts), symbol="BTCUSDT", open=close, high=close + 1, low=close - 1, close=close, volume=1.0)


def test_execution_router_submit_and_dedupe_fill(tmp_path) -> None:
    adapter = SimulatedBrokerAdapter(
        execution_model=ExecutionModel(
            fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=0.0),
            slippage_model=SlippageModel(k=0.0, atr_pct_cap=0.0, impact_cap=0.0, fixed_bps=0.0),
            spread_mode="none",
            spread_bps=0.0,
            spread_pips=None,
            intrabar_mode="worst_case",
            delay_bars=0,
            instrument=None,
        )
    )
    adapter.start()
    store = SQLiteExecutionStateStore(path=str(tmp_path / "state.sqlite"))
    portfolio_runner = PortfolioRunner(portfolio=Portfolio(initial_cash=10_000.0))
    router = ExecutionRouter(
        run_id="run-1",
        mode="paper_simulated",
        adapter=adapter,
        portfolio_runner=portfolio_runner,
        store=store,
        save_processed_event_ids=True,
    )
    intent = OrderIntent(
        ts=pd.Timestamp("2026-01-01T00:00:00Z"),
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=1.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        reason="test",
    )
    router.submit_order(order_seq=1, intent=intent, ts=intent.ts)
    fills_1 = router.process_bar(ts=intent.ts, bars_by_symbol={"BTCUSDT": _bar("2026-01-01T00:00:00Z", 100.0)})
    fills_2 = router.process_bar(ts=intent.ts, bars_by_symbol={"BTCUSDT": _bar("2026-01-01T00:00:00Z", 100.0)})

    assert len(fills_1) == 1
    assert fills_2 == []
