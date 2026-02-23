"""Canonical order-side mapping helpers."""
from __future__ import annotations

from typing import Any

from bt.core.enums import Side


def side_from_signed_qty(qty: float) -> Side:
    """Map signed quantity to side."""
    numeric_qty = float(qty)
    if numeric_qty > 0:
        return Side.BUY
    if numeric_qty < 0:
        return Side.SELL
    raise ValueError("Order quantity must be non-zero")


def signed_qty_from_side(side: Side, abs_qty: float) -> float:
    """Return signed quantity for a side and absolute size."""
    qty = float(abs_qty)
    if qty <= 0:
        raise ValueError("abs_qty must be > 0")
    if side == Side.BUY:
        return qty
    if side == Side.SELL:
        return -qty
    raise ValueError(f"Unsupported side: {side}")


def validate_order_side_consistency(
    *,
    side: Side,
    qty: float,
    signed_qty: float | None = None,
    signal_side: Side | None = None,
    reduce_only: bool = False,
    where: str,
) -> None:
    """Validate side/sign invariants right before persistence."""
    abs_qty = float(qty)
    if abs_qty == 0:
        raise ValueError(f"{where}: order.qty must be non-zero")

    effective_signed_qty = float(signed_qty) if signed_qty is not None else signed_qty_from_side(side, abs_qty)
    expected_side = side_from_signed_qty(effective_signed_qty)
    if side != expected_side:
        raise ValueError(
            f"{where}: side/qty sign mismatch (side={side.name}, signed_qty={effective_signed_qty})"
        )

    if signed_qty is not None and abs(abs(effective_signed_qty) - abs_qty) > 1e-12:
        raise ValueError(
            f"{where}: signed order_qty magnitude mismatch "
            f"(order.qty={abs_qty}, order_qty={effective_signed_qty})"
        )

    if signal_side is not None and signal_side != side:
        raise ValueError(
            f"{where}: signal.side ({signal_side.name}) disagrees with order.side ({side.name})"
        )

    if reduce_only and signed_qty is None:
        # Cannot prove reduce direction without position snapshot, but ensure shape is valid.
        return


def coerce_side(value: Any) -> Side | None:
    if isinstance(value, Side):
        return value
    if isinstance(value, str):
        normalized = value.strip().upper()
        if normalized in Side.__members__:
            return Side[normalized]
    return None
