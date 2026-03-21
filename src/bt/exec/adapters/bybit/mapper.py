from __future__ import annotations

from typing import Any

import pandas as pd

from bt.core.enums import OrderState, OrderType, PositionState, Side
from bt.core.types import Fill, Order, Position
from bt.exec.adapters.base import BalanceSnapshot


def _parse_ts_ms(value: object) -> pd.Timestamp:
    as_int = int(str(value or "0"))
    return pd.Timestamp(as_int, unit="ms", tz="UTC")


def _to_side(value: object) -> Side:
    raw = str(value).lower()
    return Side.BUY if raw == "buy" else Side.SELL


def _to_order_type(value: object) -> OrderType:
    raw = str(value).lower()
    return OrderType.LIMIT if raw == "limit" else OrderType.MARKET


def _to_order_state(value: object) -> OrderState:
    mapping = {
        "new": OrderState.NEW,
        "created": OrderState.SUBMITTED,
        "partiallyfilled": OrderState.PARTIALLY_FILLED,
        "filled": OrderState.FILLED,
        "cancelled": OrderState.CANCELLED,
        "rejected": OrderState.REJECTED,
    }
    return mapping.get(str(value).replace(" ", "").lower(), OrderState.SUBMITTED)


def map_orders(payload: dict[str, Any]) -> list[Order]:
    rows = payload.get("list") if isinstance(payload.get("list"), list) else []
    orders: list[Order] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        orders.append(
            Order(
                id=str(row.get("orderId", "")),
                ts_submitted=_parse_ts_ms(row.get("createdTime")),
                symbol=str(row.get("symbol", "")),
                side=_to_side(row.get("side", "Buy")),
                qty=float(row.get("qty", 0.0) or 0.0),
                order_type=_to_order_type(row.get("orderType", "Market")),
                limit_price=(None if row.get("price") in {None, "", "0"} else float(row.get("price"))),
                state=_to_order_state(row.get("orderStatus", "Created")),
                metadata={"client_order_id": str(row.get("orderLinkId", ""))},
            )
        )
    return orders


def map_positions(payload: dict[str, Any]) -> list[Position]:
    rows = payload.get("list") if isinstance(payload.get("list"), list) else []
    result: list[Position] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        qty = float(row.get("size", 0.0) or 0.0)
        side_value = str(row.get("side", "")).lower()
        side = None if qty == 0 else (Side.BUY if side_value == "buy" else Side.SELL)
        result.append(
            Position(
                symbol=str(row.get("symbol", "")),
                state=PositionState.FLAT if qty == 0 else PositionState.OPEN,
                side=side,
                qty=qty,
                avg_entry_price=float(row.get("avgPrice", 0.0) or 0.0),
                realized_pnl=float(row.get("cumRealisedPnl", 0.0) or 0.0),
                unrealized_pnl=float(row.get("unrealisedPnl", 0.0) or 0.0),
                mae_price=None,
                mfe_price=None,
                opened_ts=None,
                closed_ts=None,
            )
        )
    return result


def map_balances(payload: dict[str, Any]) -> BalanceSnapshot:
    rows = payload.get("list") if isinstance(payload.get("list"), list) else []
    balances: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        for coin in row.get("coin", []):
            if not isinstance(coin, dict):
                continue
            balances[str(coin.get("coin", ""))] = float(coin.get("walletBalance", 0.0) or 0.0)
    return BalanceSnapshot(ts=pd.Timestamp.now(tz="UTC"), balances=balances)


def map_fills(payload: dict[str, Any]) -> list[Fill]:
    rows = payload.get("list") if isinstance(payload.get("list"), list) else []
    fills: list[Fill] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        fills.append(
            Fill(
                order_id=str(row.get("orderId", "")),
                ts=_parse_ts_ms(row.get("execTime")),
                symbol=str(row.get("symbol", "")),
                side=_to_side(row.get("side", "Buy")),
                qty=float(row.get("execQty", 0.0) or 0.0),
                price=float(row.get("execPrice", 0.0) or 0.0),
                fee=float(row.get("execFee", 0.0) or 0.0),
                slippage=0.0,
                metadata={"exec_id": str(row.get("execId", ""))},
            )
        )
    return fills
