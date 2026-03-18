"""Portfolio accounting with margin and PnL tracking."""
from __future__ import annotations

from dataclasses import replace
from bt.core.types import Bar, Fill, Trade
from bt.core.enums import PositionState, Side
from bt.portfolio.position import PositionBook


class Portfolio:
    def __init__(
        self,
        *,
        initial_cash: float,
        max_leverage: float = 2.0,
    ) -> None:
        self.initial_cash = initial_cash
        self.max_leverage = max_leverage
        self.cash = initial_cash
        self.equity = initial_cash
        self.realized_pnl = 0.0
        self.unrealized_pnl = 0.0
        self.used_margin = 0.0
        self.free_margin = initial_cash
        self._position_book = PositionBook()
        self._mark_prices: dict[str, float] = {}
        self._last_fills: list[Fill] = []

    @property
    def position_book(self) -> PositionBook:
        return self._position_book

    def apply_fills(self, fills: list[Fill]) -> list[Trade]:
        """Apply fills to position book and cash/margin. Return trades closed."""
        trades: list[Trade] = []
        self._last_fills = list(fills)[-5:]
        for fill in fills:
            self.cash -= fill.fee
            if fill.symbol not in self._mark_prices:
                self._mark_prices[fill.symbol] = fill.price
            position, trade = self._position_book.apply_fill(fill)
            if trade is not None:
                self.realized_pnl += trade.pnl
                trades.append(trade)
            if position.state == PositionState.CLOSED:
                self._mark_prices.pop(fill.symbol, None)
            self._recalculate_state()
        return trades

    def mark_to_market(self, bars_by_symbol: dict[str, Bar]) -> None:
        """
        Update unrealized_pnl and equity using latest close prices.
        bars_by_symbol contains only symbols with bars at current timestamp.
        If a symbol has no bar, leave its last mark unchanged (no interpolation).
        """
        for symbol, bar in bars_by_symbol.items():
            self._mark_prices[symbol] = bar.close
            self._position_book.update_path_with_bar(symbol, high=float(bar.high), low=float(bar.low), ts=bar.ts)
        self._recalculate_state()

    def _recalculate_state(self) -> None:
        self._update_unrealized_pnl()
        self.equity = self.cash + self.realized_pnl + self.unrealized_pnl
        self.used_margin = self._calculate_used_margin()
        self.free_margin = self.equity - self.used_margin

    def _update_unrealized_pnl(self) -> None:
        total_unrealized = 0.0
        for symbol, position in self._position_book.all_positions().items():
            if position.state in {PositionState.FLAT, PositionState.CLOSED}:
                continue
            mark_price = self._mark_prices.get(symbol)
            if mark_price is None:
                continue
            if position.side == Side.BUY:
                unrealized = (mark_price - position.avg_entry_price) * position.qty
            else:
                unrealized = (position.avg_entry_price - mark_price) * position.qty
            total_unrealized += unrealized
            self._position_book._positions[symbol] = replace(
                position,
                unrealized_pnl=unrealized,
            )
        self.unrealized_pnl = total_unrealized

    def _calculate_used_margin(self) -> float:
        total_margin = 0.0
        for symbol, position in self._position_book.all_positions().items():
            if position.state in {PositionState.FLAT, PositionState.CLOSED}:
                continue
            mark_price = self._mark_prices.get(symbol)
            if mark_price is None:
                continue
            notional = abs(position.qty) * mark_price
            total_margin += notional / self.max_leverage
        return total_margin
