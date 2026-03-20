from __future__ import annotations

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.exec.services.risk_runner import RiskRunner
from bt.risk.risk_engine import RiskEngine


def test_risk_runner_emits_approval_or_reject_reason() -> None:
    risk = RiskEngine(
        max_positions=5,
        config={'risk': {'mode': 'equity_pct', 'r_per_trade': 0.01, 'max_leverage': 2.0, 'stop_resolution': 'safe', 'allow_legacy_proxy': True}},
    )
    runner = RiskRunner(risk_engine=risk)
    ts = pd.Timestamp('2026-01-01T00:00:00Z')
    bar = Bar(ts=ts, symbol='BTCUSDT', open=100, high=101, low=99, close=100, volume=1)
    signal = Signal(ts=ts, symbol='BTCUSDT', side=Side.BUY, signal_type='entry', confidence=0.9, metadata={})
    decision = runner.evaluate(ts=ts, signal=signal, bar=bar, equity=100000, free_margin=100000, open_positions=0, max_leverage=2.0, current_qty=0.0)
    assert isinstance(decision.reason, str)
    assert decision.approved in {True, False}
