from __future__ import annotations

import pandas as pd
import pytest

from bt.exec.adapters.base import BrokerOrderRequest
from bt.exec.adapters.simulated import SimulatedBrokerAdapter
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel
from bt.core.types import Bar


def test_simulated_adapter_submit_and_fill_flow() -> None:
    adapter = SimulatedBrokerAdapter(
        execution_model=ExecutionModel(
            fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=0.0),
            slippage_model=SlippageModel(k=0.0, atr_pct_cap=0.2, impact_cap=0.05, fixed_bps=0.0),
            delay_bars=0,
        )
    )
    adapter.start()
    adapter.submit_order(BrokerOrderRequest(client_order_id='c1', symbol='BTCUSDT', side='buy', qty=1.0, order_type='market', limit_price=None))
    ts = pd.Timestamp('2026-01-01T00:00:00Z')
    bar = Bar(ts=ts, symbol='BTCUSDT', open=100, high=101, low=99, close=100, volume=1)
    events = adapter.process_bar(ts=ts, bars_by_symbol={'BTCUSDT': bar})
    assert len(events) == 1


def test_simulated_adapter_cancel_amend_are_explicitly_unsupported() -> None:
    adapter = SimulatedBrokerAdapter(
        execution_model=ExecutionModel(
            fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=0.0),
            slippage_model=SlippageModel(k=0.0, atr_pct_cap=0.2, impact_cap=0.05, fixed_bps=0.0),
            delay_bars=0,
        )
    )
    with pytest.raises(NotImplementedError):
        adapter.cancel_order(request=None)  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError):
        adapter.amend_order(request=None)  # type: ignore[arg-type]
