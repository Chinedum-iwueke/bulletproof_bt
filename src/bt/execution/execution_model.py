"""Execution model placeholder."""
from __future__ import annotations

from dataclasses import replace
from typing import Optional

import pandas as pd

from bt.core.enums import OrderState, OrderType, Side
from bt.core.types import Bar, Fill, Order
from bt.execution.commission import CommissionSpec, compute_commission
from bt.execution.fees import FeeModel
from bt.execution.intrabar import IntrabarMode, IntrabarSpec, market_fill_price
from bt.execution.slippage import SlippageModel
from bt.execution.spread import SpreadMode, apply_instrument_spread
from bt.instruments.spec import InstrumentSpec


class ExecutionModel:
    def __init__(
        self,
        *,
        fee_model: FeeModel,
        slippage_model: SlippageModel,
        spread_mode: SpreadMode = "none",
        spread_bps: float = 0.0,
        spread_pips: float | None = None,
        intrabar_mode: IntrabarMode = "worst_case",
        delay_bars: int = 1,
        instrument: InstrumentSpec | None = None,
        commission: CommissionSpec | None = None,
    ) -> None:
        if delay_bars < 0:
            raise ValueError("delay_bars must be >= 0")
        if spread_bps < 0:
            raise ValueError("spread_bps must be >= 0")
        if spread_mode not in {"none", "fixed_bps", "bar_range_proxy", "fixed_pips"}:
            raise ValueError(f"Unsupported spread_mode: {spread_mode}")
        if spread_pips is not None and float(spread_pips) <= 0:
            raise ValueError("spread_pips must be > 0 when provided")

        self._fee_model = fee_model
        self._slippage_model = slippage_model
        self._spread_mode = spread_mode
        self._spread_bps = spread_bps
        self._spread_pips = spread_pips
        self._intrabar_spec = IntrabarSpec(mode=intrabar_mode)
        self._delay_bars = delay_bars
        self._instrument = instrument
        self._commission = commission or CommissionSpec(mode="none")

    def process(
        self,
        *,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        open_orders: list[Order],
    ) -> tuple[list[Order], list[Fill]]:
        """
        Process open orders at timestamp ts.
        Returns: (updated_orders, fills_emitted)
        """
        updated_orders: list[Order] = []
        fills: list[Fill] = []
        for order in open_orders:
            if order.order_type != OrderType.MARKET:
                raise NotImplementedError("Only MARKET orders are supported in v1.")

            updated_order = order
            if updated_order.state == OrderState.NEW:
                updated_order = replace(updated_order, state=OrderState.SUBMITTED)

            metadata = dict(updated_order.metadata)
            if "delay_remaining" not in metadata:
                metadata["delay_remaining"] = self._delay_bars

            bar: Optional[Bar] = bars_by_symbol.get(updated_order.symbol)
            if bar is None:
                updated_orders.append(replace(updated_order, metadata=metadata))
                continue

            if metadata["delay_remaining"] > 0:
                metadata["delay_remaining"] = max(metadata["delay_remaining"] - 1, 0)
                updated_orders.append(replace(updated_order, metadata=metadata))
                continue

            fill_price = market_fill_price(side=updated_order.side, bar=bar, intrabar_spec=self._intrabar_spec)
            spread_adjusted_fill_price = apply_instrument_spread(
                price=fill_price,
                side=updated_order.side.value,
                spread={
                    "mode": self._spread_mode,
                    "spread_bps": self._spread_bps,
                    "spread_pips": self._spread_pips,
                    "bar_high": bar.high,
                    "bar_low": bar.low,
                },
                instrument=self._instrument,
            )
            spread_cost = abs(updated_order.qty) * abs(spread_adjusted_fill_price - fill_price)
            fill_price = spread_adjusted_fill_price

            slippage_quote = self._slippage_model.estimate_slippage(qty=updated_order.qty, bar=bar)
            slip_px = slippage_quote / max(abs(updated_order.qty), 1e-12)
            if updated_order.side == Side.BUY:
                fill_price += slip_px
            elif updated_order.side == Side.SELL:
                fill_price -= slip_px
            else:
                raise ValueError(f"Unsupported side: {updated_order.side}")

            fill_qty = self._risk_clipped_entry_qty(
                order=updated_order,
                fill_price=fill_price,
                metadata=metadata,
            )
            if fill_qty <= 0:
                updated_orders.append(
                    replace(
                        updated_order,
                        state=OrderState.REJECTED,
                        metadata={**metadata, "risk_fill_rejected": True, "delay_remaining": 0},
                    )
                )
                continue

            if abs(fill_qty - float(updated_order.qty)) > 1e-12:
                metadata["risk_fill_qty_clipped"] = True
                metadata["risk_fill_original_qty"] = float(updated_order.qty)
                metadata["risk_fill_clipped_qty"] = float(fill_qty)
                metadata["risk_fill_price"] = float(fill_price)
                updated_order = replace(updated_order, qty=fill_qty, metadata=metadata)
                slippage_quote = self._slippage_model.estimate_slippage(qty=updated_order.qty, bar=bar)

            notional = abs(updated_order.qty) * fill_price
            exchange_fee = self._fee_model.fee_for_notional(notional=notional, is_maker=False)
            commission_fee = compute_commission(
                instrument=self._instrument,
                qty=updated_order.qty,
                commission=self._commission,
            )
            fee = exchange_fee + commission_fee

            fill_metadata = dict(updated_order.metadata)
            fill_metadata.update(
                {
                    "actual_fill_notional": notional,
                    "intrabar_mode": self._intrabar_spec.mode,
                    "delay_bars": self._delay_bars,
                    "spread_mode": self._spread_mode,
                    "spread_bps": self._spread_bps,
                    "spread_pips": self._spread_pips,
                    "spread_cost": spread_cost,
                    "exchange_fee": exchange_fee,
                    "commission_fee": commission_fee,
                    "commission_mode": self._commission.mode,
                }
            )

            fills.append(
                Fill(
                    order_id=updated_order.id,
                    ts=ts,
                    symbol=updated_order.symbol,
                    side=updated_order.side,
                    qty=updated_order.qty,
                    price=fill_price,
                    fee=fee,
                    slippage=slippage_quote,
                    metadata=fill_metadata,
                )
            )

            updated_orders.append(
                replace(
                    updated_order,
                    state=OrderState.FILLED,
                    metadata={**metadata, "delay_remaining": 0},
                )
            )

        return updated_orders, fills

    @staticmethod
    def _risk_clipped_entry_qty(*, order: Order, fill_price: float, metadata: dict[str, object]) -> float:
        """Clip entry quantity at the actual fill price so stop risk cannot exceed budget.

        Risk sizing happens when the signal is approved, but market orders may fill
        later and under a worse intrabar assumption. The actual fill price is the
        first moment when exact stop distance is known, so clipping here is causal:
        it uses no future bars and preserves the user's risk budget.
        """
        if bool(metadata.get("close_only") or metadata.get("reduce_only") or metadata.get("is_exit")):
            return float(order.qty)

        try:
            risk_budget = float(metadata.get("risk_budget", metadata.get("risk_amount")))
            stop_price = float(metadata.get("entry_stop_price", metadata.get("stop_price")))
        except (TypeError, ValueError):
            return float(order.qty)
        if risk_budget <= 0 or stop_price <= 0 or fill_price <= 0:
            return float(order.qty)

        try:
            risk_value_per_price_unit = float(metadata.get("risk_value_per_price_unit", 1.0))
        except (TypeError, ValueError):
            risk_value_per_price_unit = 1.0
        if risk_value_per_price_unit <= 0:
            risk_value_per_price_unit = 1.0

        entry_stop_distance = abs(float(fill_price) - stop_price)
        if entry_stop_distance <= 0:
            return float(order.qty)

        max_entry_qty = risk_budget / (entry_stop_distance * risk_value_per_price_unit)
        current_qty = 0.0
        try:
            current_qty = abs(float(metadata.get("current_qty", 0.0) or 0.0))
        except (TypeError, ValueError):
            current_qty = 0.0
        is_flip = bool(metadata.get("flip"))
        order_qty_abs = abs(float(order.qty))
        if is_flip and current_qty > 0:
            requested_entry_qty = max(order_qty_abs - current_qty, 0.0)
            clipped_entry_qty = min(requested_entry_qty, max_entry_qty)
            return current_qty + clipped_entry_qty
        return min(order_qty_abs, max_entry_qty)
