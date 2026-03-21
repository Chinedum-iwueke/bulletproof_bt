from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any

import pandas as pd

from bt.exec.adapters.base import AdapterHealth, AdapterHealthStatus


@dataclass(frozen=True)
class PublicWSMessage:
    ts: pd.Timestamp
    topic: str
    payload: dict[str, Any]


class BybitPublicWSClient:
    def __init__(self, *, url: str, topics: list[str], symbols: list[str], enabled: bool = True) -> None:
        self._url = url
        self._topics = topics
        self._symbols = symbols
        self._enabled = enabled
        self._connected = False
        self._messages: deque[PublicWSMessage] = deque()

    def start(self) -> None:
        if not self._enabled:
            return
        self._connected = True

    def stop(self) -> None:
        self._connected = False

    def subscribed_topics(self) -> list[str]:
        return [f"{topic}.{symbol}" for topic in self._topics for symbol in self._symbols]

    def push_test_message(self, topic: str, payload: dict[str, Any]) -> None:
        self._messages.append(PublicWSMessage(ts=pd.Timestamp.now(tz="UTC"), topic=topic, payload=payload))

    def drain_messages(self) -> list[PublicWSMessage]:
        out = list(self._messages)
        self._messages.clear()
        return out

    def health(self) -> AdapterHealth:
        if not self._enabled:
            return AdapterHealth(source="bybit_ws_public", ts=pd.Timestamp.now(tz="UTC"), status=AdapterHealthStatus.DEGRADED, message="disabled")
        return AdapterHealth(
            source="bybit_ws_public",
            ts=pd.Timestamp.now(tz="UTC"),
            status=AdapterHealthStatus.HEALTHY if self._connected else AdapterHealthStatus.DEGRADED,
            metadata={"url": self._url, "topics": self.subscribed_topics()},
        )
