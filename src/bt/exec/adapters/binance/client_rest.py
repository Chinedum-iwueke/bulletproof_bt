from __future__ import annotations

import hashlib
import hmac
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable

import pandas as pd

from bt.exec.adapters.binance.errors import BinanceAPIError, BinanceAuthError, BinanceTransportError


@dataclass(frozen=True)
class BinanceRESTResponse:
    endpoint: str
    result: Any
    time_utc: pd.Timestamp
    used_weight: str | None


class BinanceRESTClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        api_secret: str,
        recv_window_ms: int,
        timeout_ms: int,
        max_retries: int,
        retry_backoff_ms: int,
        environment: str,
        time_provider: Callable[[], int] | None = None,
        sleeper: Callable[[float], None] | None = None,
        opener: Callable[[urllib.request.Request, float], Any] | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._api_secret = api_secret
        self._recv_window_ms = recv_window_ms
        self._timeout_seconds = timeout_ms / 1000.0
        self._max_retries = max_retries if environment != "live" else min(max_retries, 2)
        self._retry_backoff_ms = retry_backoff_ms
        self._time_provider = time_provider or (lambda: int(time.time() * 1000))
        self._sleeper = sleeper or time.sleep
        self._opener = opener or (lambda request, timeout: urllib.request.urlopen(request, timeout=timeout))
        self._last_used_weight: str | None = None

    def latest_rate_limit_status(self) -> str | None:
        return self._last_used_weight

    def public_get(self, endpoint: str, *, params: dict[str, object] | None = None) -> BinanceRESTResponse:
        return self._request("GET", endpoint, params=params or {}, signed=False)

    def api_post(self, endpoint: str, *, params: dict[str, object] | None = None) -> BinanceRESTResponse:
        return self._request("POST", endpoint, params=params or {}, signed=False)

    def api_put(self, endpoint: str, *, params: dict[str, object] | None = None) -> BinanceRESTResponse:
        return self._request("PUT", endpoint, params=params or {}, signed=False)

    def api_delete(self, endpoint: str, *, params: dict[str, object] | None = None) -> BinanceRESTResponse:
        return self._request("DELETE", endpoint, params=params or {}, signed=False)

    def signed_get(self, endpoint: str, *, params: dict[str, object] | None = None) -> BinanceRESTResponse:
        return self._request("GET", endpoint, params=params or {}, signed=True)

    def signed_post(self, endpoint: str, *, params: dict[str, object]) -> BinanceRESTResponse:
        return self._request("POST", endpoint, params=params, signed=True)

    def signed_put(self, endpoint: str, *, params: dict[str, object]) -> BinanceRESTResponse:
        return self._request("PUT", endpoint, params=params, signed=True)

    def signed_delete(self, endpoint: str, *, params: dict[str, object]) -> BinanceRESTResponse:
        return self._request("DELETE", endpoint, params=params, signed=True)

    def _request(self, method: str, endpoint: str, *, params: dict[str, object], signed: bool) -> BinanceRESTResponse:
        attempt = 0
        while True:
            request_params = {key: value for key, value in params.items() if value is not None}
            if signed:
                request_params["timestamp"] = self._time_provider()
                request_params["recvWindow"] = self._recv_window_ms
            query = urllib.parse.urlencode(request_params)
            if signed:
                signature = hmac.new(self._api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
                query = f"{query}&signature={signature}"
            request = urllib.request.Request(
                url=f"{self._base_url}{endpoint}{'?' + query if query else ''}",
                method=method,
                headers={"X-MBX-APIKEY": self._api_key, "Content-Type": "application/x-www-form-urlencoded"},
            )
            try:
                response = self._opener(request, self._timeout_seconds)
                payload = json.loads(response.read().decode("utf-8"))
                self._last_used_weight = response.headers.get("X-MBX-USED-WEIGHT-1M")
                if isinstance(payload, dict) and int(payload.get("code", 0) or 0) < 0:
                    code = int(payload["code"])
                    message = str(payload.get("msg", "unknown"))
                    if code in {-2014, -2015, -1022}:
                        raise BinanceAuthError(f"Binance auth failed code={code}: {message}")
                    raise BinanceAPIError(code=code, message=message, endpoint=endpoint)
                return BinanceRESTResponse(endpoint=endpoint, result=payload, time_utc=pd.Timestamp.now(tz="UTC"), used_weight=self._last_used_weight)
            except (BinanceAuthError, BinanceAPIError):
                raise
            except urllib.error.HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")
                if exc.code in {401, 403}:
                    raise BinanceAuthError(f"Binance HTTP auth error {exc.code}: {body[:300]}") from exc
                if attempt >= self._max_retries:
                    raise BinanceTransportError(f"Binance HTTP error {exc.code} for {endpoint}: {body[:300]}") from exc
            except Exception as exc:
                if attempt >= self._max_retries:
                    raise BinanceTransportError(f"Binance transport error for {endpoint}: {exc}") from exc
            attempt += 1
            self._sleeper((self._retry_backoff_ms * (2 ** (attempt - 1))) / 1000.0)
