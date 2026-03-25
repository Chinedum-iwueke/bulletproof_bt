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
        self._last_message_ts: pd.Timestamp | None = None
        self._last_auth_success_ts: pd.Timestamp | None = None

    def start(self) -> None:
        if not self._enabled:
            return
        self._connected = True
        self._authenticated = bool(self._api_key and self._api_secret)
        if self._authenticated:
            self._last_auth_success_ts = pd.Timestamp.now(tz="UTC")

    def stop(self) -> None:
        self._connected = False
        self._authenticated = False

    def subscribed_topics(self) -> list[str]:
        return list(self._topics)

    def push_test_message(self, topic: str, payload: dict[str, Any]) -> None:
        now = pd.Timestamp.now(tz="UTC")
        self._messages.append(PrivateWSMessage(ts=now, topic=topic, payload=payload))
        self._last_message_ts = now

    def drain_messages(self) -> list[PrivateWSMessage]:
        out = list(self._messages)
        self._messages.clear()
        return out

    def last_message_ts(self) -> pd.Timestamp | None:
        return self._last_message_ts

    def last_auth_success_ts(self) -> pd.Timestamp | None:
        return self._last_auth_success_ts

    def health(self) -> AdapterHealth:
        status = AdapterHealthStatus.HEALTHY if (self._connected and self._authenticated) else AdapterHealthStatus.DEGRADED
        if not self._enabled:
            status = AdapterHealthStatus.DEGRADED
        return AdapterHealth(
            source="bybit_ws_private",
            ts=pd.Timestamp.now(tz="UTC"),
            status=status,
            metadata={
                "url": self._url,
                "topics": self._topics,
                "authenticated": self._authenticated,
                "last_message_ts": None if self._last_message_ts is None else self._last_message_ts.isoformat(),
                "last_auth_success_ts": (
                    None if self._last_auth_success_ts is None else self._last_auth_success_ts.isoformat()
                ),
            },
        )
