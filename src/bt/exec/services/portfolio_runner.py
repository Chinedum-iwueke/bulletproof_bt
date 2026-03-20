from __future__ import annotations

from bt.core.types import Bar, Fill
from bt.portfolio.portfolio import Portfolio


class PortfolioRunner:
    def __init__(self, *, portfolio: Portfolio) -> None:
        self._portfolio = portfolio

    @property
    def portfolio(self) -> Portfolio:
        return self._portfolio

    def apply_fills(self, fills: list[Fill]) -> None:
        self._portfolio.apply_fills(fills)

    def mark_to_market(self, bars_by_symbol: dict[str, Bar]) -> None:
        self._portfolio.mark_to_market(bars_by_symbol)
