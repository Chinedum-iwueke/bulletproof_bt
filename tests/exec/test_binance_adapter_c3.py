from __future__ import annotations

import json
import urllib.parse

import pytest

from bt.exec.adapters.base import BrokerOrderCancelRequest, BrokerOrderRequest
from bt.exec.adapters.binance import BinanceBrokerAdapter, BinanceRESTClient, resolve_binance_config
from bt.exec.adapters.binance.errors import BinanceAdapterError, BinanceConfigError


class _Response:
    def __init__(self, payload: object) -> None:
        self._payload = payload
        self.headers = {"X-MBX-USED-WEIGHT-1M": "7"}

    def read(self) -> bytes:
        return json.dumps(self._payload).encode()


def _adapter(environment: str = "demo", calls: list[dict[str, object]] | None = None, product_type: str = "perpetual") -> BinanceBrokerAdapter:
    cfg = resolve_binance_config({"broker": {"venue": "binance", "environment": environment, "product_type": product_type, "symbols": ["BTCUSDT"]}})

    def opener(request, _timeout):
        parsed = urllib.parse.urlparse(request.full_url)
        params = urllib.parse.parse_qs(parsed.query)
        if calls is not None:
            calls.append({"method": request.get_method(), "path": parsed.path, "params": params})
        if parsed.path == "/fapi/v1/order" and request.get_method() == "POST":
            return _Response({"orderId": 991, "status": "NEW"})
        if parsed.path in {"/fapi/v1/exchangeInfo", "/api/v3/exchangeInfo"}:
            return _Response({"symbols": [{"symbol": "BTCUSDT", "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                {"filterType": "MIN_NOTIONAL", "notional": "5"},
            ]}]})
        return _Response([])

    return BinanceBrokerAdapter(
        config=cfg,
        rest_client=BinanceRESTClient(
            base_url=cfg.rest_base_url, api_key="key", api_secret="secret",
            recv_window_ms=5000, timeout_ms=1000, max_retries=0,
            retry_backoff_ms=0, environment=environment, time_provider=lambda: 1700000000000,
            opener=opener,
        ),
    )


def test_binance_demo_signed_order_and_reconciliation_contract() -> None:
    calls: list[dict[str, object]] = []
    adapter = _adapter(calls=calls)
    adapter.start()
    order_id = adapter.submit_order(BrokerOrderRequest(
        client_order_id="ir-deploy-0001", symbol="BTCUSDT", side="buy", qty=0.001,
        order_type="market", limit_price=None,
    ))
    assert order_id == "991"
    adapter.cancel_order(BrokerOrderCancelRequest(order_id=order_id, client_order_id=None, symbol="BTCUSDT"))
    assert adapter.fetch_open_orders() == []
    assert adapter.fetch_positions() == []
    assert adapter.fetch_recent_fills_or_executions() == []
    assert adapter.fetch_balances().balances == {}
    spec = adapter.get_instrument("BTCUSDT")
    assert spec is not None and spec.tick_size == 0.1 and spec.lot_size == 0.001
    signed = next(call for call in calls if call["method"] == "POST")
    params = signed["params"]
    assert params["newClientOrderId"] == ["ir-deploy-0001"]
    assert params["signature"] and params["timestamp"] == ["1700000000000"]


def test_binance_live_mutations_fail_closed_until_enabled() -> None:
    adapter = _adapter(environment="live")
    request = BrokerOrderRequest(client_order_id="x", symbol="BTCUSDT", side="buy", qty=0.001, order_type="market", limit_price=None)
    with pytest.raises(BinanceAdapterError):
        adapter.submit_order(request)
    adapter.set_live_mutations_enabled(True)
    assert adapter.submit_order(request) == "991"


def test_binance_environment_endpoints_and_credentials_cannot_cross() -> None:
    with pytest.raises(BinanceConfigError):
        resolve_binance_config({"broker": {
            "venue": "binance", "environment": "demo", "symbols": ["BTCUSDT"],
            "endpoints": {"rest_base_url": "https://fapi.binance.com"},
        }})


def test_binance_spot_uses_spot_endpoints_and_has_no_derivatives_position() -> None:
    calls: list[dict[str, object]] = []
    adapter = _adapter(calls=calls, product_type="spot")
    adapter.start()
    adapter.fetch_open_orders()
    adapter.fetch_balances()
    assert adapter.fetch_positions() == []
    adapter.get_instrument("BTCUSDT")
    paths = {str(call["path"]) for call in calls}
    assert "/api/v3/openOrders" in paths
    assert "/api/v3/account" in paths
    assert "/api/v3/exchangeInfo" in paths
    assert not any(path.startswith("/fapi/") for path in paths)
    with pytest.raises(BinanceConfigError):
        resolve_binance_config({"broker": {
            "venue": "binance", "environment": "live", "symbols": ["BTCUSDT"],
            "auth": {"api_key_env": "BINANCE_DEMO_API_KEY", "api_secret_env": "BINANCE_DEMO_API_SECRET"},
        }})
