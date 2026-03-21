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
from bt.exec.adapters.bybit.mapper import map_balances, map_fills, map_orders, map_positions


class BybitBrokerAdapter:
    def __init__(self, *, config: BybitBrokerConfig, rest_client: BybitRESTClient, ws_public: BybitPublicWSClient, ws_private: BybitPrivateWSClient) -> None:
        self._config = config
        self._rest = rest_client
        self._ws_public = ws_public
        self._ws_private = ws_private
        self._instrument_cache = BybitInstrumentCache(rest_client=rest_client, category=config.category)
        self._started = False

    def start(self) -> None:
        self._started = True
        if self._config.ws.enabled:
            self._ws_public.start()
            self._ws_private.start()

    def stop(self) -> None:
        self._started = False
        self._ws_public.stop()
        self._ws_private.stop()

    def iter_events(self) -> list[object]:
        output: list[object] = []
        output.extend(self._ws_public.drain_messages())
        output.extend(self._ws_private.drain_messages())
        return output

    def submit_order(self, request: BrokerOrderRequest) -> str:
        _ = request
        raise BybitAdapterError("Bybit adapter is read-only in Phase 4; submit_order unsupported")

    def cancel_order(self, request: BrokerOrderCancelRequest) -> None:
        _ = request
        raise BybitAdapterError("Bybit adapter is read-only in Phase 4; cancel_order unsupported")

    def amend_order(self, request: BrokerOrderAmendRequest) -> None:
        _ = request
        raise BybitAdapterError("Bybit adapter is read-only in Phase 4; amend_order unsupported")

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
            },
        )
