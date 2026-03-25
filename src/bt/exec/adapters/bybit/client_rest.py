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

from bt.exec.adapters.bybit.errors import BybitAPIError, BybitAuthError, BybitTransportError


@dataclass(frozen=True)
class BybitRESTResponse:
    endpoint: str
    ret_code: int
    ret_msg: str
    result: dict[str, Any]
    time_utc: pd.Timestamp
    rate_limit_status: str | None


class BybitRESTClient:
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
        environment: str = "demo",
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
        self._opener = opener or (lambda req, timeout: urllib.request.urlopen(req, timeout=timeout))
        self._last_rate_limit_status: str | None = None

    def latest_rate_limit_status(self) -> str | None:
        return self._last_rate_limit_status

    def _signature_payload(self, *, timestamp_ms: int, query_or_body: str) -> str:
        return f"{timestamp_ms}{self._api_key}{self._recv_window_ms}{query_or_body}"

    def _sign(self, payload: str) -> str:
        return hmac.new(self._api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()

    def _headers(self, *, timestamp_ms: int, query_or_body: str) -> dict[str, str]:
        signature = self._sign(self._signature_payload(timestamp_ms=timestamp_ms, query_or_body=query_or_body))
        return {
            "X-BAPI-API-KEY": self._api_key,
            "X-BAPI-TIMESTAMP": str(timestamp_ms),
            "X-BAPI-RECV-WINDOW": str(self._recv_window_ms),
            "X-BAPI-SIGN": signature,
            "Content-Type": "application/json",
        }

    def get_private(self, endpoint: str, *, params: dict[str, object]) -> BybitRESTResponse:
        query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        return self._request("GET", endpoint, query_or_body=query, encoded_for_url=query)

    def post_private(self, endpoint: str, *, payload: dict[str, object]) -> BybitRESTResponse:
        body = json.dumps({k: v for k, v in payload.items() if v is not None}, separators=(",", ":"), sort_keys=True)
        return self._request("POST", endpoint, query_or_body=body, body=body)

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        query_or_body: str,
        encoded_for_url: str = "",
        body: str | None = None,
    ) -> BybitRESTResponse:
        attempt = 0
        while True:
            timestamp_ms = self._time_provider()
            url = f"{self._base_url}{endpoint}"
            if encoded_for_url:
                url = f"{url}?{encoded_for_url}"
            headers = self._headers(timestamp_ms=timestamp_ms, query_or_body=query_or_body)
            request = urllib.request.Request(
                url=url,
                method=method,
                headers=headers,
                data=(body.encode("utf-8") if body is not None else None),
            )
            try:
                response = self._opener(request, self._timeout_seconds)
                payload = json.loads(response.read().decode("utf-8"))
                ret_code = int(payload.get("retCode", -1))
                ret_msg = str(payload.get("retMsg", "unknown"))
                if ret_code != 0:
                    if ret_code in {10003, 10004, 10005, 10007}:
                        raise BybitAuthError(f"Bybit auth failed retCode={ret_code} retMsg={ret_msg}")
                    if ret_code in {10006, 10429}:
                        raise BybitTransportError(f"Bybit rate limit retCode={ret_code} retMsg={ret_msg}")
                    raise BybitAPIError(ret_code=ret_code, ret_msg=ret_msg, endpoint=endpoint)
                result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
                rate_limit_status = response.headers.get("X-Bapi-Limit-Status")
                self._last_rate_limit_status = rate_limit_status
                return BybitRESTResponse(
                    endpoint=endpoint,
                    ret_code=ret_code,
                    ret_msg=ret_msg,
                    result=result,
                    time_utc=pd.Timestamp.now(tz="UTC"),
                    rate_limit_status=rate_limit_status,
                )
            except BybitAuthError:
                raise
            except BybitAPIError:
                raise
            except urllib.error.HTTPError as exc:
                if exc.code in {401, 403}:
                    raise BybitAuthError(f"Bybit HTTP auth error: {exc.code}") from exc
                if attempt >= self._max_retries:
                    raise BybitTransportError(f"HTTPError for {endpoint}: {exc.code}") from exc
            except Exception as exc:  # pragma: no cover - defensive fallback
                if attempt >= self._max_retries:
                    raise BybitTransportError(f"Transport error for {endpoint}: {exc}") from exc
            attempt += 1
            self._sleeper((self._retry_backoff_ms * (2 ** (attempt - 1))) / 1000.0)
