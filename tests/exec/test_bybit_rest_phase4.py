from __future__ import annotations

import json
import urllib.error

import pytest

from bt.exec.adapters.bybit.client_rest import BybitRESTClient
from bt.exec.adapters.bybit.errors import BybitTransportError


class _Resp:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload
        self.headers = {"X-Bapi-Limit-Status": "119"}

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


def test_headers_signature_and_timestamp() -> None:
    client = BybitRESTClient(
        base_url="https://api.bybit.com",
        api_key="k",
        api_secret="s",
        recv_window_ms=5000,
        timeout_ms=2000,
        max_retries=1,
        retry_backoff_ms=1,
        time_provider=lambda: 1700000000000,
        opener=lambda _req, _timeout: _Resp({"retCode": 0, "retMsg": "OK", "result": {"list": []}}),
    )
    headers = client._headers(timestamp_ms=1700000000000, query_or_body="category=linear")
    assert headers["X-BAPI-API-KEY"] == "k"
    assert headers["X-BAPI-TIMESTAMP"] == "1700000000000"
    assert len(headers["X-BAPI-SIGN"]) == 64


def test_retry_then_success() -> None:
    calls = {"n": 0}

    def _opener(_req, _timeout):
        calls["n"] += 1
        if calls["n"] == 1:
            raise urllib.error.URLError("boom")
        return _Resp({"retCode": 0, "retMsg": "OK", "result": {"list": []}})

    client = BybitRESTClient(
        base_url="https://api.bybit.com",
        api_key="k",
        api_secret="s",
        recv_window_ms=5000,
        timeout_ms=2000,
        max_retries=2,
        retry_backoff_ms=1,
        opener=_opener,
        sleeper=lambda _x: None,
    )
    out = client.get_private("/v5/order/realtime", params={"category": "linear"})
    assert out.ret_code == 0
    assert calls["n"] == 2


def test_retry_exhausted_raises_transport() -> None:
    client = BybitRESTClient(
        base_url="https://api.bybit.com",
        api_key="k",
        api_secret="s",
        recv_window_ms=5000,
        timeout_ms=2000,
        max_retries=1,
        retry_backoff_ms=1,
        opener=lambda _req, _timeout: (_ for _ in ()).throw(urllib.error.URLError("boom")),
        sleeper=lambda _x: None,
    )
    with pytest.raises(BybitTransportError):
        client.get_private("/v5/order/realtime", params={"category": "linear"})
