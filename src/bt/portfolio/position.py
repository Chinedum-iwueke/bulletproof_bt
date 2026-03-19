"""Position state management and MAE/MFE tracking."""
from __future__ import annotations

from dataclasses import replace
from typing import Optional

import pandas as pd

from bt.core.enums import PositionState, Side
from bt.core.types import Fill, Position, Trade
from bt.portfolio.constants import QTY_EPSILON


class PositionBook:
    def __init__(self) -> None:
        self._positions: dict[str, Position] = {}
        self._position_costs: dict[str, tuple[float, float]] = {}
        self._position_metadata: dict[str, dict[str, object]] = {}
        self._position_path_state: dict[str, dict[str, object]] = {}

    def get(self, symbol: str) -> Position:
        """Return current Position for symbol (create FLAT if missing)."""
        if symbol not in self._positions:
            self._positions[symbol] = Position(
                symbol=symbol,
                state=PositionState.FLAT,
                side=None,
                qty=0.0,
                avg_entry_price=0.0,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                mae_price=None,
                mfe_price=None,
                opened_ts=None,
                closed_ts=None,
            )
        return self._positions[symbol]

    def all_positions(self) -> dict[str, Position]:
        return dict(self._positions)

    def open_positions_count(self) -> int:
        """Count positions that are OPEN/OPENING/REDUCING (not FLAT/CLOSED)."""
        open_states = {PositionState.OPEN, PositionState.OPENING, PositionState.REDUCING}
        return sum(1 for position in self._positions.values() if position.state in open_states)

    def apply_fill(self, fill: Fill) -> tuple[Position, Optional[Trade]]:
        """
        Apply a Fill to the symbol position.
        Returns: (updated_position, trade_if_closed_else_None)
        """
        position = self.get(fill.symbol)
        trade: Optional[Trade] = None
        fees_paid, slippage_paid = self._position_costs.get(fill.symbol, (0.0, 0.0))

        if position.state in {PositionState.FLAT, PositionState.CLOSED} or self._normalize_qty(position.qty) == 0.0:
            position = self._open_new_position(fill)
            self._positions[fill.symbol] = position
            if position.qty == 0.0:
                self._position_costs.pop(fill.symbol, None)
                self._position_metadata.pop(fill.symbol, None)
                self._position_path_state.pop(fill.symbol, None)
            else:
                self._position_costs[fill.symbol] = (fill.fee, fill.slippage)
                self._position_metadata[fill.symbol] = self._extract_risk_metadata(
                    fill.metadata,
                    entry_qty=position.qty,
                )
                state = self._init_path_state(position, fill.ts)
                state["entry_stop_distance"] = self._position_metadata[fill.symbol].get("entry_stop_distance")
                self._position_path_state[fill.symbol] = state
            return position, None

        if position.side == fill.side:
            new_qty = self._normalize_qty(position.qty + fill.qty)
            new_avg = (
                position.avg_entry_price * position.qty + fill.price * fill.qty
            ) / new_qty
            mae_price, mfe_price = self._update_mae_mfe(position, fill.price)
            position = replace(
                position,
                qty=new_qty,
                avg_entry_price=new_avg,
                state=PositionState.OPEN,
                mae_price=mae_price,
                mfe_price=mfe_price,
            )
            self._positions[fill.symbol] = position
            self._position_costs[fill.symbol] = (
                fees_paid + fill.fee,
                slippage_paid + fill.slippage,
            )
            return position, None

        reduce_qty = min(position.qty, fill.qty)
        realized_pnl = self._realized_pnl(position, fill.price, reduce_qty)
        mae_price, mfe_price = self._update_mae_mfe(position, fill.price)
        remaining_qty = self._normalize_qty(position.qty - reduce_qty)
        fill_qty = max(abs(fill.qty), QTY_EPSILON)
        close_ratio = reduce_qty / fill_qty
        closing_fee = fill.fee * close_ratio
        closing_slippage = fill.slippage * close_ratio
        total_fees = fees_paid + closing_fee
        total_slippage = slippage_paid + closing_slippage

        if fill.qty > position.qty:
            trade_metadata = dict(self._position_metadata.get(fill.symbol, {}))
            if isinstance(fill.metadata, dict):
                trade_metadata.update(fill.metadata)
            trade_metadata.update(self._export_path_state(fill.symbol))
            trade = self._build_trade(
                position=position,
                exit_price=fill.price,
                exit_ts=fill.ts,
                qty=reduce_qty,
                pnl=realized_pnl,
                fees=total_fees,
                slippage=total_slippage,
                mae_price=mae_price,
                mfe_price=mfe_price,
                metadata=trade_metadata,
            )
            position = self._open_new_position(fill, qty=self._normalize_qty(fill.qty - reduce_qty))
            if position.qty == 0.0:
                self._position_costs.pop(fill.symbol, None)
                self._position_metadata.pop(fill.symbol, None)
                self._position_path_state.pop(fill.symbol, None)
            else:
                self._position_costs[fill.symbol] = (
                    fill.fee - closing_fee,
                    fill.slippage - closing_slippage,
                )
                self._position_metadata[fill.symbol] = self._extract_risk_metadata(
                    fill.metadata,
                    entry_qty=position.qty,
                )
                state = self._init_path_state(position, fill.ts)
                state["entry_stop_distance"] = self._position_metadata[fill.symbol].get("entry_stop_distance")
                self._position_path_state[fill.symbol] = state
        elif remaining_qty == 0.0:
            trade_metadata = dict(self._position_metadata.get(fill.symbol, {}))
            if isinstance(fill.metadata, dict):
                trade_metadata.update(fill.metadata)
            trade_metadata.update(self._export_path_state(fill.symbol))
            trade = self._build_trade(
                position=position,
                exit_price=fill.price,
                exit_ts=fill.ts,
                qty=reduce_qty,
                pnl=realized_pnl,
                fees=total_fees,
                slippage=total_slippage,
                mae_price=mae_price,
                mfe_price=mfe_price,
                metadata=trade_metadata,
            )
            position = replace(
                position,
                state=PositionState.CLOSED,
                side=None,
                qty=0.0,
                avg_entry_price=0.0,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                mae_price=None,
                mfe_price=None,
                opened_ts=None,
                closed_ts=fill.ts,
            )
            self._position_costs.pop(fill.symbol, None)
            self._position_metadata.pop(fill.symbol, None)
            self._position_path_state.pop(fill.symbol, None)
            self._positions.pop(fill.symbol, None)
        else:
            position = replace(
                position,
                qty=remaining_qty,
                realized_pnl=position.realized_pnl + realized_pnl,
                state=PositionState.REDUCING,
                mae_price=mae_price,
                mfe_price=mfe_price,
            )
            self._position_costs[fill.symbol] = (total_fees, total_slippage)

        if position.qty == 0.0:
            self._positions.pop(fill.symbol, None)
        else:
            self._positions[fill.symbol] = position
        return position, trade

    def update_path_with_bar(self, symbol: str, *, high: float, low: float, ts: pd.Timestamp) -> None:
        position = self._positions.get(symbol)
        if position is None or position.side is None:
            return
        state = self._position_path_state.get(symbol)
        if state is None:
            state = self._init_path_state(position, ts)
            self._position_path_state[symbol] = state

        stop_distance_raw = state.get("entry_stop_distance")
        try:
            stop_distance = float(stop_distance_raw) if stop_distance_raw is not None else None
        except (TypeError, ValueError):
            stop_distance = None
        entry_price = float(position.avg_entry_price)

        if position.side == Side.BUY:
            favorable_price = float(high)
            adverse_price = float(low)
            prev_fav = float(state.get("favorable_price", entry_price))
            prev_adv = float(state.get("adverse_price", entry_price))
            if favorable_price > prev_fav:
                state["favorable_price"] = favorable_price
                state["favorable_ts"] = ts
            if adverse_price < prev_adv:
                state["adverse_price"] = adverse_price
                state["adverse_ts"] = ts
            if stop_distance and stop_distance > 0:
                curr_profit_r = (favorable_price - entry_price) / stop_distance
                curr_loss_r = (entry_price - adverse_price) / stop_distance
                state["max_unrealized_profit_r"] = max(float(state.get("max_unrealized_profit_r", 0.0)), curr_profit_r)
                state["max_unrealized_loss_r"] = max(float(state.get("max_unrealized_loss_r", 0.0)), curr_loss_r)
        else:
            favorable_price = float(low)
            adverse_price = float(high)
            prev_fav = float(state.get("favorable_price", entry_price))
            prev_adv = float(state.get("adverse_price", entry_price))
            if favorable_price < prev_fav:
                state["favorable_price"] = favorable_price
                state["favorable_ts"] = ts
            if adverse_price > prev_adv:
                state["adverse_price"] = adverse_price
                state["adverse_ts"] = ts
            if stop_distance and stop_distance > 0:
                curr_profit_r = (entry_price - favorable_price) / stop_distance
                curr_loss_r = (adverse_price - entry_price) / stop_distance
                state["max_unrealized_profit_r"] = max(float(state.get("max_unrealized_profit_r", 0.0)), curr_profit_r)
                state["max_unrealized_loss_r"] = max(float(state.get("max_unrealized_loss_r", 0.0)), curr_loss_r)

    @staticmethod
    def _init_path_state(position: Position, ts: pd.Timestamp) -> dict[str, object]:
        entry_price = float(position.avg_entry_price)
        return {
            "favorable_price": entry_price,
            "adverse_price": entry_price,
            "favorable_ts": ts,
            "adverse_ts": ts,
            "entry_stop_distance": None,
            "max_unrealized_profit_r": 0.0,
            "max_unrealized_loss_r": 0.0,
        }

    def _export_path_state(self, symbol: str) -> dict[str, object]:
        state = self._position_path_state.get(symbol, {})
        return {
            "path_favorable_price": state.get("favorable_price"),
            "path_adverse_price": state.get("adverse_price"),
            "path_favorable_ts": state.get("favorable_ts"),
            "path_adverse_ts": state.get("adverse_ts"),
            "max_unrealized_profit_r": state.get("max_unrealized_profit_r"),
            "max_unrealized_loss_r": state.get("max_unrealized_loss_r"),
        }

    def _open_new_position(self, fill: Fill, qty: Optional[float] = None) -> Position:
        open_qty = self._normalize_qty(fill.qty if qty is None else qty)
        if open_qty == 0.0:
            return Position(
                symbol=fill.symbol,
                state=PositionState.FLAT,
                side=None,
                qty=0.0,
                avg_entry_price=0.0,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                mae_price=None,
                mfe_price=None,
                opened_ts=None,
                closed_ts=fill.ts,
            )
        return Position(
            symbol=fill.symbol,
            state=PositionState.OPEN,
            side=fill.side,
            qty=open_qty,
            avg_entry_price=fill.price,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            mae_price=fill.price,
            mfe_price=fill.price,
            opened_ts=fill.ts,
            closed_ts=None,
        )

    @staticmethod
    def _realized_pnl(position: Position, price: float, qty: float) -> float:
        if position.side == Side.BUY:
            return (price - position.avg_entry_price) * qty
        return (position.avg_entry_price - price) * qty

    @staticmethod
    def _update_mae_mfe(position: Position, price: float) -> tuple[float, float]:
        if position.mae_price is None or position.mfe_price is None:
            return price, price
        mae_price = min(position.mae_price, price)
        mfe_price = max(position.mfe_price, price)
        return mae_price, mfe_price


    @staticmethod
    def _normalize_qty(qty: float) -> float:
        if abs(qty) < QTY_EPSILON:
            return 0.0
        return round(qty, 12)

    @staticmethod
    def _extract_risk_metadata(
        metadata: object,
        *,
        entry_qty: float,
    ) -> dict[str, object]:
        if not isinstance(metadata, dict):
            return {}
        extracted: dict[str, object] = {}
        for key in (
            "risk_amount",
            "stop_distance",
            "stop_source",
            "stop_price",
            "entry_stop_price",
            "entry_stop_distance",
            "setup_side",
            "extension_to_entry_delay_signal_bars",
            "max_extension_z_before_entry",
            "z_vwap_at_entry",
            "vwap_distance_at_entry_atr_units",
            "extension_armed",
            "extension_ts",
            "extension_z_extreme",
            "reentry_confirmed",
            "reentry_ts",
            "entry_reason",
            "q_comp",
            "comp_gate_t",
            "session_vwap",
            "z_vwap_t",
            "z_ext",
            "z_reentry",
            "signal_timeframe",
            "exit_monitoring_timeframe",
            "vwap_mode",
        ):
            if key in metadata:
                extracted[key] = metadata.get(key)

        normalized_entry_qty = abs(float(entry_qty))
        extracted["entry_qty"] = normalized_entry_qty
        extracted["entry_stop_distance"] = extracted.get("stop_distance")

        stop_distance_raw = extracted.get("entry_stop_distance")
        try:
            stop_distance = float(stop_distance_raw) if stop_distance_raw is not None else None
        except (TypeError, ValueError):
            stop_distance = None

        if stop_distance is not None and stop_distance > 0 and normalized_entry_qty > 0:
            extracted["risk_amount"] = normalized_entry_qty * stop_distance
            extracted["entry_stop_distance"] = stop_distance
        if "entry_stop_price" not in extracted and "stop_price" in extracted:
            extracted["entry_stop_price"] = extracted.get("stop_price")

        return extracted

    @staticmethod
    def _build_trade(
        *,
        position: Position,
        exit_price: float,
        exit_ts: pd.Timestamp,
        qty: float,
        pnl: float,
        fees: float,
        slippage: float,
        mae_price: float,
        mfe_price: float,
        metadata: dict[str, object],
    ) -> Trade:
        return Trade(
            symbol=position.symbol,
            side=position.side or Side.BUY,
            entry_ts=position.opened_ts or exit_ts,
            exit_ts=exit_ts,
            entry_price=position.avg_entry_price,
            exit_price=exit_price,
            qty=qty,
            pnl=pnl,
            fees=fees,
            slippage=slippage,
            mae_price=mae_price,
            mfe_price=mfe_price,
            metadata=dict(metadata),
        )
