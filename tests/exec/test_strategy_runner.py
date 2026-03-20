from __future__ import annotations

from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.exec.services.strategy_runner import StrategyRunner
from bt.strategy.base import Strategy


class ProbeStrategy(Strategy):
    def __init__(self) -> None:
        self.calls: list[tuple[pd.Timestamp, set[str], Mapping[str, Any]]] = []

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        self.calls.append((ts, tradeable, ctx))
        return [Signal(ts=ts, symbol='BTCUSDT', side=Side.BUY, signal_type='entry', confidence=0.9, metadata={})]


def test_strategy_runner_preserves_contract_shape() -> None:
    strategy = ProbeStrategy()
    runner = StrategyRunner(strategy=strategy)
    ts = pd.Timestamp('2026-01-01T00:00:00Z')
    bars = {'BTCUSDT': Bar(ts=ts, symbol='BTCUSDT', open=100, high=101, low=99, close=100, volume=1)}
    out = runner.run(ts=ts, bars_by_symbol=bars, tradeable={'BTCUSDT'}, ctx={'tradeable': {'BTCUSDT'}})
    assert len(out) == 1
    assert strategy.calls[0][0] == ts
    assert strategy.calls[0][1] == {'BTCUSDT'}
