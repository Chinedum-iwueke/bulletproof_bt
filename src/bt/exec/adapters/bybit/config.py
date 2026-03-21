from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

from bt.exec.adapters.bybit.errors import BybitConfigError


_ENDPOINTS: dict[str, dict[str, str]] = {
    "live": {
        "rest_base_url": "https://api.bybit.com",
        "public_ws_url": "wss://stream.bybit.com/v5/public/linear",
        "private_ws_url": "wss://stream.bybit.com/v5/private",
    },
    "demo": {
        "rest_base_url": "https://api-demo.bybit.com",
        "public_ws_url": "wss://stream-demo.bybit.com/v5/public/linear",
        "private_ws_url": "wss://stream-demo.bybit.com/v5/private",
    },
}


@dataclass(frozen=True)
class BybitWSConfig:
    enabled: bool = True
    connect_timeout_seconds: int = 10
    heartbeat_seconds: int = 20
    private_topics: list[str] = field(default_factory=lambda: ["position", "wallet", "order", "execution"])
    public_topics: list[str] = field(default_factory=lambda: ["tickers"])


@dataclass(frozen=True)
class BybitAuthConfig:
    api_key_env: str
    api_secret_env: str

    def resolve(self) -> tuple[str, str]:
        key = os.getenv(self.api_key_env, "").strip()
        secret = os.getenv(self.api_secret_env, "").strip()
        if not key or not secret:
            raise BybitConfigError(
                f"Bybit auth env vars missing/empty: key={self.api_key_env!r}, secret={self.api_secret_env!r}"
            )
        return key, secret


@dataclass(frozen=True)
class BybitBrokerConfig:
    environment: str
    category: str
    symbols: list[str]
    recv_window_ms: int
    request_timeout_ms: int
    max_retries: int
    retry_backoff_ms: int
    rest_base_url: str
    public_ws_url: str
    private_ws_url: str
    auth: BybitAuthConfig
    ws: BybitWSConfig


def resolve_bybit_config(config: dict[str, Any]) -> BybitBrokerConfig:
    broker = config.get("broker")
    if not isinstance(broker, dict):
        raise BybitConfigError("Missing broker config")
    venue = str(broker.get("venue", "simulated")).strip().lower()
    if venue != "bybit":
        raise BybitConfigError(f"broker.venue must be 'bybit', got {venue!r}")

    environment = str(broker.get("environment", "demo")).strip().lower()
    if environment not in _ENDPOINTS:
        raise BybitConfigError("broker.environment must be one of: demo, live")

    endpoints_raw = broker.get("endpoints") if isinstance(broker.get("endpoints"), dict) else {}
    defaults = _ENDPOINTS[environment]
    rest_base_url = str(endpoints_raw.get("rest_base_url", defaults["rest_base_url"]))
    public_ws_url = str(endpoints_raw.get("public_ws_url", defaults["public_ws_url"]))
    private_ws_url = str(endpoints_raw.get("private_ws_url", defaults["private_ws_url"]))

    if environment == "demo" and "api.bybit.com" in rest_base_url:
        raise BybitConfigError("Demo environment cannot use live REST endpoint")
    if environment == "live" and "api-demo.bybit.com" in rest_base_url:
        raise BybitConfigError("Live environment cannot use demo REST endpoint")

    auth_raw = broker.get("auth")
    if not isinstance(auth_raw, dict):
        raise BybitConfigError("broker.auth block is required")
    auth = BybitAuthConfig(
        api_key_env=str(auth_raw.get("api_key_env", "BYBIT_API_KEY")),
        api_secret_env=str(auth_raw.get("api_secret_env", "BYBIT_API_SECRET")),
    )

    ws_raw = broker.get("ws") if isinstance(broker.get("ws"), dict) else {}
    ws = BybitWSConfig(
        enabled=bool(ws_raw.get("enabled", True)),
        connect_timeout_seconds=int(ws_raw.get("connect_timeout_seconds", 10)),
        heartbeat_seconds=int(ws_raw.get("heartbeat_seconds", 20)),
        private_topics=[str(t) for t in ws_raw.get("private_topics", ["position", "wallet", "order", "execution"])],
        public_topics=[str(t) for t in ws_raw.get("public_topics", ["tickers"])],
    )

    symbols = [str(s) for s in broker.get("symbols", [])]
    if not symbols:
        raise BybitConfigError("broker.symbols must contain at least one symbol")

    return BybitBrokerConfig(
        environment=environment,
        category=str(broker.get("category", "linear")),
        symbols=symbols,
        recv_window_ms=int(broker.get("recv_window_ms", 5000)),
        request_timeout_ms=int(broker.get("request_timeout_ms", 4000)),
        max_retries=int(broker.get("max_retries", 3)),
        retry_backoff_ms=int(broker.get("retry_backoff_ms", 250)),
        rest_base_url=rest_base_url,
        public_ws_url=public_ws_url,
        private_ws_url=private_ws_url,
        auth=auth,
        ws=ws,
    )
