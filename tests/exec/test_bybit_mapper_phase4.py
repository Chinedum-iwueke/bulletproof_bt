from __future__ import annotations

from bt.exec.adapters.bybit.mapper import map_balances, map_fills, map_orders, map_positions


def test_map_balances_positions_orders_fills() -> None:
    balances = map_balances({"list": [{"coin": [{"coin": "USDT", "walletBalance": "123.4"}]}]})
    assert balances.balances["USDT"] == 123.4

    positions = map_positions({"list": [{"symbol": "BTCUSDT", "side": "Buy", "size": "1", "avgPrice": "100", "cumRealisedPnl": "1", "unrealisedPnl": "2"}]})
    assert positions[0].symbol == "BTCUSDT"

    orders = map_orders({"list": [{"orderId": "o1", "createdTime": "1700000000000", "symbol": "BTCUSDT", "side": "Buy", "qty": "1", "orderType": "Limit", "price": "100", "orderStatus": "New"}]})
    assert orders[0].id == "o1"
    assert "orderId" not in orders[0].metadata

    fills = map_fills({"list": [{"orderId": "o1", "execTime": "1700000000000", "symbol": "BTCUSDT", "side": "Buy", "execQty": "1", "execPrice": "100", "execFee": "0.1", "execId": "e1"}]})
    assert fills[0].metadata["exec_id"] == "e1"
