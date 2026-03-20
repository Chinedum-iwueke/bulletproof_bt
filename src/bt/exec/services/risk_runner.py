from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from bt.core.types import Bar, OrderIntent, Signal
from bt.risk.risk_engine import RiskEngine


@dataclass(frozen=True)
class RiskDecision:
    signal: Signal
    approved: bool
    reason: str
    order_intent: OrderIntent | None


class RiskRunner:
    def __init__(self, *, risk_engine: RiskEngine) -> None:
        self._risk = risk_engine

    def evaluate(
        self,
        *,
        ts: pd.Timestamp,
        signal: Signal,
        bar: Bar,
        equity: float,
        free_margin: float,
        open_positions: int,
        max_leverage: float,
        current_qty: float,
    ) -> RiskDecision:
        order_intent, reason = self._risk.signal_to_order_intent(
            ts=ts,
            signal=signal,
            bar=bar,
            equity=equity,
            free_margin=free_margin,
            open_positions=open_positions,
            max_leverage=max_leverage,
            current_qty=current_qty,
        )
        return RiskDecision(signal=signal, approved=order_intent is not None, reason=reason, order_intent=order_intent)
