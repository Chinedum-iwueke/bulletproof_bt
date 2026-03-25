from __future__ import annotations

from typing import Any

import pandas as pd

from bt.core.types import Fill, Order, Position
from bt.exec.adapters.base import (
    AdapterHealth,
    AdapterHealthStatus,
    BalanceSnapshot,
    BrokerOrderAmendRequest,
    BrokerOrderCancelRequest,
    BrokerOrderRequest,
)
from bt.exec.adapters.bybit.client_rest import BybitRESTClient
from bt.exec.adapters.bybit.client_ws_private import BybitPrivateWSClient
from bt.exec.adapters.bybit.client_ws_public import BybitPublicWSClient
from bt.exec.adapters.bybit.config import BybitBrokerConfig
from bt.exec.adapters.bybit.errors import BybitAdapterError
from bt.exec.adapters.bybit.instrument_cache import BybitInstrumentCache
from bt.exec.adapters.bybit.mapper import (
    map_balances,
    map_fills,
    map_orders,
    map_positions,
    map_private_execution_event,
    map_private_order_event,
    map_private_position_snapshot,
    map_private_wallet_snapshot,
)


class BybitBrokerAdapter:
    def __init__(self, *, config: BybitBrokerConfig, rest_client: BybitRESTClient, ws_public: BybitPublicWSClient, ws_private: BybitPrivateWSClient) -> None:
        self._config = config
        self._rest = rest_client
        self._ws_public = ws_public
        self._ws_private = ws_private
        self._instrument_cache = BybitInstrumentCache(rest_client=rest_client, category=config.category)
        self._started = False
        self._live_mutations_enabled = False

    def set_live_mutations_enabled(self, enabled: bool) -> None:
        self._live_mutations_enabled = enabled

    def start(self) -> None:
        self._started = True
        if self._config.ws.enabled:
            self._ws_public.start()
            self._ws_private.start()

    def stop(self) -> None:
        self._started = False
        self._ws_public.stop()
        self._ws_private.stop()

    def private_stream_ready(self) -> bool:
        return self._ws_private.health().status == AdapterHealthStatus.HEALTHY

    def iter_events(self) -> list[object]:
        output: list[object] = []
        output.extend(self._ws_public.drain_messages())
        for msg in self._ws_private.drain_messages():
            if msg.topic == "order":
                rows = msg.payload.get("data") if isinstance(msg.payload.get("data"), list) else []
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    mapped = map_private_order_event(ts=msg.ts, row=row)
                    if mapped is not None:
                        output.append(mapped)
            elif msg.topic == "execution":
                rows = msg.payload.get("data") if isinstance(msg.payload.get("data"), list) else []
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    output.append(map_private_execution_event(ts=msg.ts, row=row))
            elif msg.topic == "position":
                output.append(map_private_position_snapshot(ts=msg.ts, payload=msg.payload))
            elif msg.topic == "wallet":
                output.append(map_private_wallet_snapshot(ts=msg.ts, payload=msg.payload))
        return output

    def submit_order(self, request: BrokerOrderRequest) -> str:
        self._require_mutations_allowed()
        payload: dict[str, object] = {
            "category": self._config.category,
            "symbol": request.symbol,
            "side": request.side.capitalize(),
            "orderType": request.order_type.capitalize(),
            "qty": str(request.qty),
            "orderLinkId": request.client_order_id,
            "timeInForce": request.time_in_force or ("GTC" if request.order_type.lower() == "limit" else "IOC"),
            "reduceOnly": request.reduce_only,
        }
        if request.limit_price is not None:
            payload["price"] = str(request.limit_price)
        response = self._rest.post_private("/v5/order/create", payload=payload)
        order_id = str(response.result.get("orderId", "")).strip()
        if not order_id:
            raise BybitAdapterError("Bybit submit response missing orderId")
        self._ws_private.push_test_message(
            "order",
            {
                "data": [
                    {
                        "orderId": order_id,
                        "orderLinkId": request.client_order_id,
                        "symbol": request.symbol,
                        "side": request.side.capitalize(),
                        "qty": str(request.qty),
                        "orderType": request.order_type.capitalize(),
                        "price": None if request.limit_price is None else str(request.limit_price),
                        "orderStatus": "Created",
                        "createdTime": str(int(response.time_utc.timestamp() * 1000)),
                    }
                ]
            },
        )
        return order_id

    def cancel_order(self, request: BrokerOrderCancelRequest) -> None:
        self._require_mutations_allowed()
        response = self._rest.post_private(
            "/v5/order/cancel",
            payload={
                "category": self._config.category,
                "symbol": request.symbol or self._config.symbols[0],
                "orderId": request.order_id,
                "orderLinkId": request.client_order_id,
            },
        )
        order_id = str(response.result.get("orderId", request.order_id or "")).strip()
        if order_id:
            self._ws_private.push_test_message(
                "order",
                {"data": [{"orderId": order_id, "orderStatus": "Cancelled", "createdTime": str(int(response.time_utc.timestamp() * 1000))}]},
            )

    def amend_order(self, request: BrokerOrderAmendRequest) -> None:
        self._require_mutations_allowed()
        self._rest.post_private(
            "/v5/order/amend",
            payload={
                "category": self._config.category,
                "orderId": request.order_id,
                "orderLinkId": request.client_order_id,
                "qty": (None if request.new_qty is None else str(request.new_qty)),
                "price": (None if request.new_limit_price is None else str(request.new_limit_price)),
            },
        )

    def _require_mutations_allowed(self) -> None:
        if self._config.environment == "demo":
            return
        if self._config.environment == "live" and self._live_mutations_enabled:
            return
        raise BybitAdapterError("Bybit live mutation is blocked until live startup/canary controls pass")

    def _fetch(self, endpoint: str, **params: object) -> dict[str, Any]:
        merged = {"category": self._config.category, **params}
        return self._rest.get_private(endpoint, params=merged).result

    def fetch_open_orders(self) -> list[Order]:
        return map_orders(self._fetch("/v5/order/realtime", symbol=self._config.symbols[0], openOnly=0))

    def fetch_completed_orders(self, limit: int = 200) -> list[Order]:
        return map_orders(self._fetch("/v5/order/history", symbol=self._config.symbols[0], limit=limit))

    def fetch_positions(self) -> list[Position]:
        return map_positions(self._fetch("/v5/position/list", symbol=self._config.symbols[0]))

    def fetch_balances(self) -> BalanceSnapshot:
        return map_balances(self._fetch("/v5/account/wallet-balance", accountType="UNIFIED"))

    def fetch_recent_fills_or_executions(self, limit: int = 200) -> list[Fill]:
        return map_fills(self._fetch("/v5/execution/list", symbol=self._config.symbols[0], limit=limit))

    def get_instrument(self, symbol: str):
        return self._instrument_cache.get(symbol)

    def get_health(self) -> AdapterHealth:
        public_health = self._ws_public.health()
        private_health = self._ws_private.health()
        status = AdapterHealthStatus.HEALTHY
        if not self._started:
            status = AdapterHealthStatus.DEGRADED
        if private_health.status == AdapterHealthStatus.UNHEALTHY:
            status = AdapterHealthStatus.UNHEALTHY
        return AdapterHealth(
            source="bybit",
            ts=pd.Timestamp.now(tz="UTC"),
            status=status,
            metadata={
                "environment": self._config.environment,
                "public_ws": public_health.status.value,
                "private_ws": private_health.status.value,
                "last_private_message_ts": (
                    None if self._ws_private.last_message_ts() is None else self._ws_private.last_message_ts().isoformat()
                ),
                "last_private_auth_ts": (
                    None
                    if self._ws_private.last_auth_success_ts() is None
                    else self._ws_private.last_auth_success_ts().isoformat()
                ),
                "rate_limit_status": self._rest.latest_rate_limit_status(),
                "live_mutations_enabled": self._live_mutations_enabled,
            },
        )
