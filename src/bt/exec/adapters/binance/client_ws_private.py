from __future__ import annotations

import json
import threading
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Protocol

import pandas as pd

from bt.exec.adapters.base import AdapterHealth, AdapterHealthStatus
from bt.exec.adapters.binance.client_rest import BinanceRESTClient


class WebSocketLike(Protocol):
    def recv(self) -> str: ...
    def close(self) -> None: ...


@dataclass(frozen=True)
class BinancePrivateMessage:
    ts: pd.Timestamp
    event_type: str
    payload: dict[str, Any]


class BinancePrivateWSClient:
    def __init__(self, *, rest: BinanceRESTClient, ws_base_url: str, product_type: str, enabled: bool = True, socket_factory: Callable[[str], WebSocketLike] | None = None) -> None:
        self._rest, self._ws_base_url, self._product_type, self._enabled = rest, ws_base_url.rstrip("/"), product_type, enabled
        self._socket_factory = socket_factory or self._default_socket
        self._messages: deque[BinancePrivateMessage] = deque(maxlen=10_000)
        self._socket: WebSocketLike | None = None
        self._thread: threading.Thread | None = None
        self._listen_key: str | None = None
        self._running = False
        self._last_event_at: pd.Timestamp | None = None
        self._last_error: str | None = None
        self._reconnect_count = 0

    @staticmethod
    def _default_socket(url: str) -> WebSocketLike:
        import websocket

        return websocket.create_connection(url, timeout=10)

    def _endpoint(self) -> str:
        return "/api/v3/userDataStream" if self._product_type == "spot" else "/fapi/v1/listenKey"

    def start(self) -> None:
        if not self._enabled or self._running:
            return
        response = self._rest.api_post(self._endpoint())
        if not isinstance(response.result, dict) or not response.result.get("listenKey"):
            raise RuntimeError("binance_listen_key_missing")
        self._listen_key = str(response.result["listenKey"])
        self._running = True
        self._socket = self._socket_factory(f"{self._ws_base_url}/{self._listen_key}")
        self._thread = threading.Thread(target=self._read_loop, name="binance-private-stream", daemon=True)
        self._thread.start()

    def _read_loop(self) -> None:
        while self._running and self._socket is not None:
            try:
                raw = self._socket.recv()
                payload = json.loads(raw)
                if isinstance(payload, dict):
                    now = pd.Timestamp.now(tz="UTC")
                    self._last_event_at = now
                    self._messages.append(BinancePrivateMessage(now, str(payload.get("e", "unknown")), payload))
            except Exception as exc:
                self._last_error = str(exc)[:240]
                self._running = False

    def keepalive(self) -> None:
        if self._listen_key:
            self._rest.api_put(self._endpoint(), params={"listenKey": self._listen_key})

    def stop(self) -> None:
        self._running = False
        if self._socket is not None:
            self._socket.close()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2)
        if self._listen_key:
            try:
                self._rest.api_delete(self._endpoint(), params={"listenKey": self._listen_key})
            except Exception:
                pass
        self._socket = None
        self._listen_key = None

    def ready(self) -> bool:
        return self._enabled and self._running and self._listen_key is not None and self._last_error is None

    def drain_messages(self) -> list[BinancePrivateMessage]:
        values = list(self._messages)
        self._messages.clear()
        return values

    def push_test_message(self, payload: dict[str, Any]) -> None:
        now = pd.Timestamp.now(tz="UTC")
        self._last_event_at = now
        self._messages.append(BinancePrivateMessage(now, str(payload.get("e", "unknown")), payload))

    def health(self) -> AdapterHealth:
        age = None if self._last_event_at is None else max(0.0, (pd.Timestamp.now(tz="UTC") - self._last_event_at).total_seconds())
        status = AdapterHealthStatus.HEALTHY if self.ready() else AdapterHealthStatus.DEGRADED
        return AdapterHealth(source="binance_ws_private", ts=pd.Timestamp.now(tz="UTC"), status=status, message=self._last_error, metadata={"authenticated":self._listen_key is not None,"last_event_at":None if self._last_event_at is None else self._last_event_at.isoformat(),"event_age_seconds":age,"reconnect_count":self._reconnect_count,"product_type":self._product_type})
