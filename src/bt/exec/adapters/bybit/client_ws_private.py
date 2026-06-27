from __future__ import annotations

import hashlib
import hmac
import json
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Protocol

import pandas as pd

from bt.exec.adapters.base import AdapterHealth, AdapterHealthStatus


class WebSocketLike(Protocol):
    def send(self, payload: str) -> None: ...
    def recv(self) -> str: ...
    def close(self) -> None: ...


@dataclass(frozen=True)
class PrivateWSMessage:
    ts: pd.Timestamp
    topic: str
    payload: dict[str, Any]


class BybitPrivateWSClient:
    def __init__(self, *, url: str, topics: list[str], api_key: str, api_secret: str, enabled: bool = True, socket_factory: Callable[[str], WebSocketLike] | None = None) -> None:
        self._url, self._topics, self._api_key, self._api_secret, self._enabled = url, topics, api_key, api_secret, enabled
        self._socket_factory = socket_factory or self._default_socket
        self._connected = False
        self._authenticated = False
        self._running = False
        self._socket: WebSocketLike | None = None
        self._thread: threading.Thread | None = None
        self._messages: deque[PrivateWSMessage] = deque(maxlen=10_000)
        self._last_message_ts: pd.Timestamp | None = None
        self._last_auth_success_ts: pd.Timestamp | None = None
        self._last_error: str | None = None
        self._reconnect_count = 0

    @staticmethod
    def _default_socket(url: str) -> WebSocketLike:
        import websocket
        return websocket.create_connection(url, timeout=10)

    def _connect(self) -> None:
        socket = self._socket_factory(self._url)
        expires = int(time.time() * 1000) + 10_000
        signature = hmac.new(self._api_secret.encode(), f"GET/realtime{expires}".encode(), hashlib.sha256).hexdigest()
        socket.send(json.dumps({"op":"auth","args":[self._api_key,expires,signature]},separators=(",",":")))
        auth = json.loads(socket.recv())
        if not isinstance(auth,dict) or auth.get("success") is not True:
            socket.close()
            raise RuntimeError(f"bybit_private_auth_failed:{auth.get('ret_msg', auth.get('retMsg', 'unknown')) if isinstance(auth,dict) else 'invalid_response'}")
        if self._topics:
            socket.send(json.dumps({"op":"subscribe","args":self._topics},separators=(",",":")))
        self._socket = socket
        self._connected = self._authenticated = True
        self._last_auth_success_ts = pd.Timestamp.now(tz="UTC")
        self._last_error = None

    def start(self) -> None:
        if not self._enabled or self._running:
            return
        self._running = True
        self._connect()
        self._thread = threading.Thread(target=self._read_loop,name="bybit-private-stream",daemon=True)
        self._thread.start()

    def _read_loop(self) -> None:
        while self._running:
            try:
                if self._socket is None:
                    self._connect()
                raw = self._socket.recv() if self._socket is not None else ""
                payload = json.loads(raw)
                if not isinstance(payload,dict) or "topic" not in payload:
                    continue
                now = pd.Timestamp.now(tz="UTC")
                self._messages.append(PrivateWSMessage(ts=now,topic=str(payload["topic"]),payload=payload))
                self._last_message_ts = now
            except Exception as exc:
                self._last_error = str(exc)[:240]
                self._connected = self._authenticated = False
                if self._socket is not None:
                    try:
                        self._socket.close()
                    except Exception:
                        pass
                self._socket = None
                if not self._running:
                    break
                self._reconnect_count += 1
                time.sleep(min(5.0, 0.25 * (2 ** min(self._reconnect_count, 4))))

    def stop(self) -> None:
        self._running = self._connected = self._authenticated = False
        if self._socket is not None:
            self._socket.close()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._socket = None

    def subscribed_topics(self) -> list[str]:
        return list(self._topics)

    def push_test_message(self, topic: str, payload: dict[str, Any]) -> None:
        now = pd.Timestamp.now(tz="UTC")
        self._messages.append(PrivateWSMessage(ts=now, topic=topic, payload=payload))
        self._last_message_ts = now

    def drain_messages(self) -> list[PrivateWSMessage]:
        values = list(self._messages)
        self._messages.clear()
        return values

    def last_message_ts(self) -> pd.Timestamp | None:
        return self._last_message_ts

    def last_auth_success_ts(self) -> pd.Timestamp | None:
        return self._last_auth_success_ts

    def health(self) -> AdapterHealth:
        age=None if self._last_message_ts is None else max(0.0,(pd.Timestamp.now(tz="UTC")-self._last_message_ts).total_seconds())
        status=AdapterHealthStatus.HEALTHY if self._connected and self._authenticated else AdapterHealthStatus.DEGRADED
        return AdapterHealth(source="bybit_ws_private",ts=pd.Timestamp.now(tz="UTC"),status=status,message=self._last_error,metadata={"url":self._url,"topics":self._topics,"authenticated":self._authenticated,"last_message_ts":None if self._last_message_ts is None else self._last_message_ts.isoformat(),"last_auth_success_ts":None if self._last_auth_success_ts is None else self._last_auth_success_ts.isoformat(),"event_age_seconds":age,"reconnect_count":self._reconnect_count})
