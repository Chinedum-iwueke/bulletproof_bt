from __future__ import annotations

from typing import Any

import pandas as pd

from bt.core.enums import OrderState, OrderType, PositionState, Side
from bt.core.types import Fill, Order, Position
from bt.exec.adapters.base import BalanceSnapshot
from bt.exec.events.broker_events import BrokerBalanceSnapshotEvent, BrokerOrderAcknowledgedEvent, BrokerOrderCancelledEvent, BrokerOrderFilledEvent, BrokerOrderPartiallyFilledEvent, BrokerPositionSnapshotEvent


def _ts(value: object) -> pd.Timestamp:
    return pd.Timestamp(int(value or 0), unit="ms", tz="UTC")


def _side(value: object) -> Side:
    return Side.BUY if str(value).upper() == "BUY" else Side.SELL


def _type(value: object) -> OrderType:
    return OrderType.LIMIT if str(value).upper() == "LIMIT" else OrderType.MARKET


def _state(value: object) -> OrderState:
    return {
        "NEW": OrderState.SUBMITTED,
        "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
        "FILLED": OrderState.FILLED,
        "CANCELED": OrderState.CANCELLED,
        "EXPIRED": OrderState.CANCELLED,
        "REJECTED": OrderState.REJECTED,
    }.get(str(value).upper(), OrderState.SUBMITTED)


def map_orders(payload: Any) -> list[Order]:
    rows = payload if isinstance(payload, list) else [payload] if isinstance(payload, dict) else []
    return [
        Order(
            id=str(row.get("orderId", "")),
            ts_submitted=_ts(row.get("time", row.get("updateTime", 0))),
            symbol=str(row.get("symbol", "")),
            side=_side(row.get("side")),
            qty=float(row.get("origQty", 0) or 0),
            order_type=_type(row.get("type")),
            limit_price=None if str(row.get("price", "0")) in {"", "0", "0.0"} else float(row["price"]),
            state=_state(row.get("status")),
            metadata={"client_order_id": str(row.get("clientOrderId", ""))},
        )
        for row in rows if isinstance(row, dict)
    ]


def map_positions(payload: Any) -> list[Position]:
    rows = payload if isinstance(payload, list) else []
    output: list[Position] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        signed_qty = float(row.get("positionAmt", 0) or 0)
        qty = abs(signed_qty)
        output.append(Position(
            symbol=str(row.get("symbol", "")),
            state=PositionState.FLAT if qty == 0 else PositionState.OPEN,
            side=None if qty == 0 else (Side.BUY if signed_qty > 0 else Side.SELL),
            qty=qty,
            avg_entry_price=float(row.get("entryPrice", 0) or 0),
            realized_pnl=0.0,
            unrealized_pnl=float(row.get("unRealizedProfit", 0) or 0),
            mae_price=None, mfe_price=None, opened_ts=None, closed_ts=None,
        ))
    return output


def map_balances(payload: Any) -> BalanceSnapshot:
    rows = payload if isinstance(payload, list) else []
    return BalanceSnapshot(
        ts=pd.Timestamp.now(tz="UTC"),
        balances={str(row.get("asset", "")): float(row.get("balance", 0) or 0) for row in rows if isinstance(row, dict)},
        metadata={"available": {str(row.get("asset", "")): float(row.get("availableBalance", 0) or 0) for row in rows if isinstance(row, dict)}},
    )


def map_spot_balances(payload: Any) -> BalanceSnapshot:
    rows = payload.get("balances", []) if isinstance(payload, dict) else []
    return BalanceSnapshot(
        ts=pd.Timestamp.now(tz="UTC"),
        balances={str(row.get("asset", "")): float(row.get("free", 0) or 0) + float(row.get("locked", 0) or 0) for row in rows if isinstance(row, dict)},
        metadata={"free": {str(row.get("asset", "")): float(row.get("free", 0) or 0) for row in rows if isinstance(row, dict)}},
    )


def map_fills(payload: Any) -> list[Fill]:
    rows = payload if isinstance(payload, list) else []
    return [
        Fill(
            order_id=str(row.get("orderId", "")),
            ts=_ts(row.get("time", 0)),
            symbol=str(row.get("symbol", "")),
            side=_side(row.get("side")),
            qty=float(row.get("qty", 0) or 0),
            price=float(row.get("price", 0) or 0),
            fee=float(row.get("commission", 0) or 0),
            slippage=0.0,
            metadata={"trade_id": str(row.get("id", "")), "commission_asset": str(row.get("commissionAsset", ""))},
        )
        for row in rows if isinstance(row, dict)
    ]


def map_private_message(*, ts: pd.Timestamp, payload: dict[str, Any]) -> list[object]:
    event_type = str(payload.get("e", ""))
    output: list[object] = []
    if event_type in {"executionReport", "ORDER_TRADE_UPDATE"}:
        row = payload if event_type == "executionReport" else payload.get("o", {})
        if not isinstance(row, dict):
            return output
        status = str(row.get("X", row.get("status", "NEW"))).upper()
        order_id = str(row.get("i", row.get("orderId", "")))
        symbol = str(row.get("s", row.get("symbol", "")))
        side = str(row.get("S", row.get("side", "BUY")))
        order_type = str(row.get("o", row.get("orderType", "MARKET")))
        last_qty = float(row.get("l", 0) or 0)
        cumulative_qty = float(row.get("z", 0) or 0)
        original_qty = float(row.get("q", row.get("origQty", 0)) or 0)
        price = float(row.get("L", row.get("ap", row.get("price", 0))) or 0)
        event_id = f"binance-{event_type}-{order_id}-{int(ts.value)}-{status}"
        if last_qty > 0:
            fill = Fill(order_id=order_id, ts=_ts(row.get("T", payload.get("E", int(ts.timestamp()*1000)))), symbol=symbol, side=_side(side), qty=last_qty, price=price, fee=float(row.get("n", 0) or 0), slippage=0.0, metadata={"trade_id":str(row.get("t", "")),"commission_asset":str(row.get("N", ""))})
            leaves = max(0.0, original_qty-cumulative_qty)
            output.append(BrokerOrderFilledEvent(ts=ts,broker_event_id=event_id,fill=fill) if status=="FILLED" or leaves==0 else BrokerOrderPartiallyFilledEvent(ts=ts,broker_event_id=event_id,fill=fill,leaves_qty=leaves))
        elif status in {"CANCELED","EXPIRED"}:
            output.append(BrokerOrderCancelledEvent(ts=ts,broker_event_id=event_id,order_id=order_id,reason=status.lower()))
        else:
            mapped = map_orders({"orderId":order_id,"time":payload.get("E",0),"symbol":symbol,"side":side,"origQty":original_qty,"type":order_type,"price":row.get("p",0),"status":status,"clientOrderId":row.get("c","")})
            if mapped:
                output.append(BrokerOrderAcknowledgedEvent(ts=ts,broker_event_id=event_id,order=mapped[0],metadata={"status":status}))
    elif event_type in {"outboundAccountPosition","ACCOUNT_UPDATE"}:
        if event_type=="outboundAccountPosition":
            balances={str(item.get("a","")):float(item.get("f",0) or 0)+float(item.get("l",0) or 0) for item in payload.get("B",[]) if isinstance(item,dict)}
            output.append(BrokerBalanceSnapshotEvent(ts=ts,broker_event_id=f"binance-balance-{int(ts.value)}",balance_snapshot=BalanceSnapshot(ts=ts,balances=balances)))
        else:
            account=payload.get("a",{}) if isinstance(payload.get("a"),dict) else {}
            balances={str(item.get("a","")):float(item.get("wb",0) or 0) for item in account.get("B",[]) if isinstance(item,dict)}
            positions=map_positions([{"symbol":item.get("s"),"positionAmt":item.get("pa"),"entryPrice":item.get("ep"),"unRealizedProfit":item.get("up",0)} for item in account.get("P",[]) if isinstance(item,dict)])
            output.extend([BrokerBalanceSnapshotEvent(ts=ts,broker_event_id=f"binance-balance-{int(ts.value)}",balance_snapshot=BalanceSnapshot(ts=ts,balances=balances)),BrokerPositionSnapshotEvent(ts=ts,broker_event_id=f"binance-position-{int(ts.value)}",positions=positions)])
    return output
