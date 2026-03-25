from __future__ import annotations

import json
import urllib.parse

import pytest

from bt.exec.adapters.base import BrokerOrderAmendRequest, BrokerOrderCancelRequest, BrokerOrderRequest
from bt.exec.adapters.bybit.adapter import BybitBrokerAdapter
from bt.exec.adapters.bybit.client_rest import BybitRESTClient
from bt.exec.adapters.bybit.client_ws_private import BybitPrivateWSClient
from bt.exec.adapters.bybit.client_ws_public import BybitPublicWSClient
from bt.exec.adapters.bybit.config import resolve_bybit_config
from bt.exec.adapters.bybit.errors import BybitAdapterError
from bt.exec.events.broker_events import BrokerOrderAcknowledgedEvent


class _Resp:
    def __init__(self, payload):
        self._payload = payload
        self.headers = {}

    def read(self):
        return json.dumps(self._payload).encode("utf-8")


def _make_adapter(*, environment: str = "demo", calls: list[dict[str, object]] | None = None) -> BybitBrokerAdapter:
    cfg = resolve_bybit_config(
        {
            "broker": {
                "venue": "bybit",
                "environment": environment,
                "category": "linear",
                "symbols": ["BTCUSDT"],
                "auth": {"api_key_env": "BYBIT_API_KEY", "api_secret_env": "BYBIT_API_SECRET"},
            }
        }
    )

    def _opener(req, _timeout):
        body = req.data.decode("utf-8") if req.data else ""
        if calls is not None:
            calls.append({"method": req.get_method(), "path": urllib.parse.urlparse(req.full_url).path, "body": body})
        payload = {"retCode": 0, "retMsg": "OK", "result": {"list": []}}
        if req.get_method() == "POST" and req.full_url.endswith("/v5/order/create"):
            payload["result"] = {"orderId": "by-123"}
        return _Resp(payload)

    rest = BybitRESTClient(
        base_url=cfg.rest_base_url,
        api_key="key",
        api_secret="secret",
        recv_window_ms=cfg.recv_window_ms,
        timeout_ms=cfg.request_timeout_ms,
        max_retries=cfg.max_retries,
        retry_backoff_ms=cfg.retry_backoff_ms,
        opener=_opener,
    )
    return BybitBrokerAdapter(
        config=cfg,
        rest_client=rest,
        ws_public=BybitPublicWSClient(url=cfg.public_ws_url, topics=cfg.ws.public_topics, symbols=cfg.symbols),
        ws_private=BybitPrivateWSClient(url=cfg.private_ws_url, topics=cfg.ws.private_topics, api_key="k", api_secret="s"),
    )


def test_demo_submit_cancel_amend_payloads_and_ack_not_final() -> None:
    calls: list[dict[str, object]] = []
    adapter = _make_adapter(calls=calls)
    adapter.start()
    order_id = adapter.submit_order(
        BrokerOrderRequest(
            client_order_id="cid-1",
            symbol="BTCUSDT",
            side="buy",
            qty=1.2,
            order_type="limit",
            limit_price=101000.0,
        )
    )
    assert order_id == "by-123"
    events = adapter.iter_events()
    assert any(isinstance(evt, BrokerOrderAcknowledgedEvent) for evt in events)
    assert not any(getattr(getattr(evt, "order", None), "state", None).value == "filled" for evt in events if hasattr(evt, "order"))
    adapter.cancel_order(BrokerOrderCancelRequest(order_id="by-123", client_order_id=None, symbol="BTCUSDT"))
    adapter.amend_order(BrokerOrderAmendRequest(order_id="by-123", client_order_id=None, new_qty=2.0, new_limit_price=102000.0))
    adapter.stop()

    paths = [str(c["path"]) for c in calls]
    assert "/v5/order/create" in paths
    assert "/v5/order/cancel" in paths
    assert "/v5/order/amend" in paths
    create_body = json.loads(next(str(c["body"]) for c in calls if str(c["path"]) == "/v5/order/create"))
    assert create_body["orderLinkId"] == "cid-1"
    assert create_body["symbol"] == "BTCUSDT"


def test_live_environment_mutations_blocked() -> None:
    adapter = _make_adapter(environment="live")
    with pytest.raises(BybitAdapterError):
        adapter.submit_order(BrokerOrderRequest(client_order_id="x", symbol="BTCUSDT", side="buy", qty=1.0, order_type="market", limit_price=None))


def test_live_environment_mutations_allowed_only_after_enable() -> None:
    adapter = _make_adapter(environment="live")
    adapter.set_live_mutations_enabled(True)
    adapter.start()
    order_id = adapter.submit_order(
        BrokerOrderRequest(client_order_id="x", symbol="BTCUSDT", side="buy", qty=1.0, order_type="market", limit_price=None)
    )
    assert order_id == "by-123"
