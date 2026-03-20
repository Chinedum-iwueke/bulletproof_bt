from __future__ import annotations

import pandas as pd

from bt.exec.runtime.bar_gate import ClosedBarGate


def test_closed_bar_gate_warmup_and_monotonicity() -> None:
    gate = ClosedBarGate(close_bar_only=True, warmup_bars=1)
    assert gate.is_eligible(ts=pd.Timestamp('2026-01-01T00:00:00Z')) is False
    assert gate.is_eligible(ts=pd.Timestamp('2026-01-01T00:01:00Z')) is True
    assert gate.is_eligible(ts=pd.Timestamp('2026-01-01T00:01:00Z')) is False
