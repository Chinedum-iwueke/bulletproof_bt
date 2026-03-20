from __future__ import annotations

from typing import Any, Mapping

import pandas as pd

from bt.core.types import Bar, Signal
from bt.portfolio.portfolio import Portfolio
from bt.strategy.base import Strategy
from bt.strategy.context_view import StrategyContextView


class StrategyRunner:
    def __init__(self, *, strategy: Strategy) -> None:
        self._strategy = strategy

    def run(self, *, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        return self._strategy.on_bars(ts, bars_by_symbol, tradeable, StrategyContextView(ctx))


def build_positions_context(portfolio: Portfolio) -> dict[str, dict[str, Any]]:
    payload: dict[str, dict[str, Any]] = {}
    for symbol, position in portfolio.position_book.all_positions().items():
        qty = float(position.qty)
        side = position.side.value if position.side is not None and qty != 0 else None
        entry_price = float(position.avg_entry_price) if side is not None else None
        payload[symbol] = {"side": side, "qty": qty, "entry_price": entry_price, "notional": abs(qty) * float(entry_price or 0.0)}
    return payload
