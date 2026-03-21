from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any

import pandas as pd

from bt.exec.adapters.base import AdapterHealth, AdapterHealthStatus


@dataclass(frozen=True)
class PrivateWSMessage:
    ts: pd.Timestamp
    topic: str
    payload: dict[str, Any]


class BybitPrivateWSClient:
    def __init__(self, *, url: str, topics: list[str], api_key: str, api_secret: str, enabled: bool = True) -> None:
        self._url = url
        self._topics = topics
        self._api_key = api_key
        self._api_secret = api_secret
        self._enabled = enabled
        self._connected = False
        self._authenticated = False
        self._messages: deque[PrivateWSMessage] = deque()

    def start(self) -> None:
        if not self._enabled:
            return
        self._connected = True
        self._authenticated = bool(self._api_key and self._api_secret)

    def stop(self) -> None:
        self._connected = False
        self._authenticated = False

    def subscribed_topics(self) -> list[str]:
        return list(self._topics)

    def push_test_message(self, topic: str, payload: dict[str, Any]) -> None:
        self._messages.append(PrivateWSMessage(ts=pd.Timestamp.now(tz="UTC"), topic=topic, payload=payload))

    def drain_messages(self) -> list[PrivateWSMessage]:
        out = list(self._messages)
        self._messages.clear()
        return out

    def health(self) -> AdapterHealth:
        status = AdapterHealthStatus.HEALTHY if (self._connected and self._authenticated) else AdapterHealthStatus.DEGRADED
        if not self._enabled:
            status = AdapterHealthStatus.DEGRADED
        return AdapterHealth(
            source="bybit_ws_private",
            ts=pd.Timestamp.now(tz="UTC"),
            status=status,
            metadata={"url": self._url, "topics": self._topics, "authenticated": self._authenticated},
        )
