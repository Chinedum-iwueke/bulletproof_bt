from __future__ import annotations

import pytest

from bt.exec.adapters.bybit.adapter import BybitBrokerAdapter
from bt.exec.adapters.bybit.client_rest import BybitRESTClient
from bt.exec.adapters.bybit.client_ws_private import BybitPrivateWSClient
from bt.exec.adapters.bybit.client_ws_public import BybitPublicWSClient
from bt.exec.adapters.bybit.config import resolve_bybit_config
from bt.exec.adapters.bybit.errors import BybitAdapterError


class _Resp:
    def __init__(self, payload):
        self._payload = payload
        self.headers = {}

    def read(self):
        import json

        return json.dumps(self._payload).encode("utf-8")


def _opener(_req, _timeout):
    return _Resp({"retCode": 0, "retMsg": "OK", "result": {"list": []}})


def _adapter() -> BybitBrokerAdapter:
    cfg = resolve_bybit_config(
        {
            "broker": {
                "venue": "bybit",
                "environment": "demo",
                "category": "linear",
                "symbols": ["BTCUSDT"],
                "auth": {"api_key_env": "BYBIT_API_KEY", "api_secret_env": "BYBIT_API_SECRET"},
            }
        }
    )
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


def test_read_only_mutations_unsupported() -> None:
    adapter = _adapter()
    with pytest.raises(BybitAdapterError):
        adapter.submit_order(request=None)  # type: ignore[arg-type]


def test_health_and_fetches() -> None:
    adapter = _adapter()
    adapter.start()
    assert adapter.get_health().source == "bybit"
    assert adapter.fetch_open_orders() == []
    adapter.stop()
