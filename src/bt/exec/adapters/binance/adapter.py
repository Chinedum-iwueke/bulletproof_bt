from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from bt.core.types import Fill, Order, Position
from bt.exec.adapters.base import AdapterHealth, AdapterHealthStatus, BalanceSnapshot, BrokerOrderAmendRequest, BrokerOrderCancelRequest, BrokerOrderRequest
from bt.exec.adapters.binance.client_rest import BinanceRESTClient
from bt.exec.adapters.binance.client_ws_private import BinancePrivateWSClient
from bt.exec.adapters.binance.config import BinanceBrokerConfig
from bt.exec.adapters.binance.errors import BinanceAdapterError
from bt.exec.adapters.binance.mapper import map_balances, map_fills, map_orders, map_positions, map_private_message, map_spot_balances


@dataclass(frozen=True)
class BinanceInstrumentSpec:
    symbol: str
    tick_size: float
    lot_size: float
    min_notional: float


class BinanceBrokerAdapter:
    """Spot and USD-M perpetual adapter with REST-authoritative reconciliation.

    Private-stream projection is deliberately not faked. `private_stream_ready`
    remains false until a real user-data stream is attached by the host runtime.
    """

    def __init__(self, *, config: BinanceBrokerConfig, rest_client: BinanceRESTClient, ws_private: BinancePrivateWSClient | None = None) -> None:
        self._config = config
        self._rest = rest_client
        self._ws_private = ws_private
        self._started = False
        self._live_mutations_enabled = False
        self._instrument_cache: dict[str, BinanceInstrumentSpec] = {}

    def set_live_mutations_enabled(self, enabled: bool) -> None:
        self._live_mutations_enabled = enabled

    def start(self) -> None:
        self._started = True
        if self._ws_private is not None and self._config.ws_enabled:
            self._ws_private.start()

    def stop(self) -> None:
        self._started = False
        if self._ws_private is not None:
            self._ws_private.stop()

    def private_stream_ready(self) -> bool:
        return self._ws_private is not None and self._ws_private.ready()

    def iter_events(self) -> list[object]:
        output: list[object] = []
        if self._ws_private is not None:
            for message in self._ws_private.drain_messages():
                output.extend(map_private_message(ts=message.ts,payload=message.payload))
        return output

    def _require_mutations_allowed(self) -> None:
        if self._config.environment == "demo":
            return
        if self._config.environment == "live" and self._live_mutations_enabled:
            return
        raise BinanceAdapterError("Binance live mutation is blocked until live startup/canary controls pass")

    def submit_order(self, request: BrokerOrderRequest) -> str:
        self._require_mutations_allowed()
        params: dict[str, object] = {
            "symbol": request.symbol,
            "side": request.side.upper(),
            "type": request.order_type.upper(),
            "quantity": request.qty,
            "newClientOrderId": request.client_order_id,
            "newOrderRespType": "RESULT",
        }
        if self._config.product_type == "perpetual":
            params["reduceOnly"] = str(request.reduce_only).lower()
        if request.order_type.lower() == "limit":
            params["price"] = request.limit_price
            params["timeInForce"] = request.time_in_force or "GTC"
        result = self._rest.signed_post(self._path("order"), params=params).result
        order_id = str(result.get("orderId", "")) if isinstance(result, dict) else ""
        if not order_id:
            raise BinanceAdapterError("Binance submit response missing orderId")
        return order_id

    def cancel_order(self, request: BrokerOrderCancelRequest) -> None:
        self._require_mutations_allowed()
        self._rest.signed_delete(self._path("order"), params={
            "symbol": request.symbol or self._config.symbols[0],
            "orderId": request.order_id,
            "origClientOrderId": request.client_order_id,
        })

    def amend_order(self, request: BrokerOrderAmendRequest) -> None:
        self._require_mutations_allowed()
        if self._config.product_type == "spot":
            params: dict[str, object] = {
                "symbol": str(request.metadata.get("symbol", self._config.symbols[0])),
                "newQty": request.new_qty,
            }
            if request.order_id:
                params["orderId"] = request.order_id
            elif request.client_order_id:
                params["origClientOrderId"] = request.client_order_id
            else:
                raise BinanceAdapterError(
                    "Binance spot amend requires order_id or client_order_id"
                )
            self._rest.signed_put("/api/v3/order/amend/keepPriority", params=params)
            return
        self._rest.signed_put("/fapi/v1/order", params={
            "symbol": str(request.metadata.get("symbol", self._config.symbols[0])),
            "orderId": request.order_id,
            "origClientOrderId": request.client_order_id,
            "quantity": request.new_qty,
            "price": request.new_limit_price,
            "side": request.metadata.get("side"),
        })

    def fetch_open_orders(self) -> list[Order]:
        return map_orders(self._rest.signed_get(self._path("open_orders")).result)

    def fetch_completed_orders(self, limit: int = 200) -> list[Order]:
        rows: list[Order] = []
        for symbol in self._config.symbols:
            rows.extend(map_orders(self._rest.signed_get(self._path("all_orders"), params={"symbol": symbol, "limit": limit}).result))
        return rows

    def fetch_positions(self) -> list[Position]:
        if self._config.product_type == "spot":
            return []
        positions = map_positions(self._rest.signed_get("/fapi/v2/positionRisk").result)
        return [position for position in positions if position.symbol in self._config.symbols]

    def fetch_balances(self) -> BalanceSnapshot:
        if self._config.product_type == "spot":
            return map_spot_balances(self._rest.signed_get("/api/v3/account").result)
        return map_balances(self._rest.signed_get("/fapi/v2/balance").result)

    def fetch_recent_fills_or_executions(self, limit: int = 200) -> list[Fill]:
        rows: list[Fill] = []
        for symbol in self._config.symbols:
            rows.extend(map_fills(self._rest.signed_get(self._path("trades"), params={"symbol": symbol, "limit": limit}).result))
        return rows

    def get_instrument(self, symbol: str) -> BinanceInstrumentSpec | None:
        if symbol in self._instrument_cache:
            return self._instrument_cache[symbol]
        payload = self._rest.public_get(self._path("exchange_info")).result
        symbols = payload.get("symbols", []) if isinstance(payload, dict) else []
        row = next((item for item in symbols if isinstance(item, dict) and item.get("symbol") == symbol), None)
        if not isinstance(row, dict):
            return None
        filters = {str(item.get("filterType")): item for item in row.get("filters", []) if isinstance(item, dict)}
        spec = BinanceInstrumentSpec(
            symbol=symbol,
            tick_size=float(filters.get("PRICE_FILTER", {}).get("tickSize", 0) or 0),
            lot_size=float(filters.get("LOT_SIZE", {}).get("stepSize", 0) or 0),
            min_notional=float(filters.get("MIN_NOTIONAL", {}).get("notional", 0) or 0),
        )
        self._instrument_cache[symbol] = spec
        return spec

    def get_health(self) -> AdapterHealth:
        return AdapterHealth(
            source="binance",
            ts=pd.Timestamp.now(tz="UTC"),
            status=AdapterHealthStatus.HEALTHY if self._started else AdapterHealthStatus.DEGRADED,
            metadata={
                "environment": self._config.environment,
                "product_type": self._config.product_type,
                "reconciliation_source": "rest",
                "private_stream": "healthy" if self.private_stream_ready() else "not_ready",
                "private_stream_health": None if self._ws_private is None else self._ws_private.health().metadata,
                "rate_limit_status": self._rest.latest_rate_limit_status(),
                "live_mutations_enabled": self._live_mutations_enabled,
            },
        )

    def _path(self, name: str) -> str:
        spot = {"order": "/api/v3/order", "open_orders": "/api/v3/openOrders", "all_orders": "/api/v3/allOrders", "trades": "/api/v3/myTrades", "exchange_info": "/api/v3/exchangeInfo"}
        perpetual = {"order": "/fapi/v1/order", "open_orders": "/fapi/v1/openOrders", "all_orders": "/fapi/v1/allOrders", "trades": "/fapi/v1/userTrades", "exchange_info": "/fapi/v1/exchangeInfo"}
        return (spot if self._config.product_type == "spot" else perpetual)[name]
