from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from bt.exec.adapters.binance.errors import BinanceConfigError


_ENDPOINTS = {
    ("demo", "perpetual"): {"rest_base_url": "https://testnet.binancefuture.com", "ws_base_url": "wss://stream.binancefuture.com/ws"},
    ("live", "perpetual"): {"rest_base_url": "https://fapi.binance.com", "ws_base_url": "wss://fstream.binance.com/ws"},
    ("demo", "spot"): {"rest_base_url": "https://testnet.binance.vision", "ws_base_url": "wss://stream.testnet.binance.vision/ws"},
    ("live", "spot"): {"rest_base_url": "https://api.binance.com", "ws_base_url": "wss://stream.binance.com:9443/ws"},
}


@dataclass(frozen=True)
class BinanceAuthConfig:
    api_key_env: str
    api_secret_env: str

    def resolve(self) -> tuple[str, str]:
        key = os.getenv(self.api_key_env, "").strip()
        secret = os.getenv(self.api_secret_env, "").strip()
        if not key or not secret:
            raise BinanceConfigError(
                f"Binance auth env vars missing/empty: key={self.api_key_env!r}, secret={self.api_secret_env!r}"
            )
        return key, secret


@dataclass(frozen=True)
class BinanceBrokerConfig:
    environment: str
    product_type: str
    symbols: list[str]
    recv_window_ms: int
    request_timeout_ms: int
    max_retries: int
    retry_backoff_ms: int
    rest_base_url: str
    ws_base_url: str
    ws_enabled: bool
    auth: BinanceAuthConfig


def resolve_binance_config(config: dict[str, Any]) -> BinanceBrokerConfig:
    broker = config.get("broker")
    if not isinstance(broker, dict):
        raise BinanceConfigError("Missing broker config")
    if str(broker.get("venue", "")).strip().lower() != "binance":
        raise BinanceConfigError("broker.venue must be 'binance'")
    environment = str(broker.get("environment", "demo")).strip().lower()
    product_type = str(broker.get("product_type", "perpetual")).strip().lower()
    if product_type not in {"spot", "perpetual"}:
        raise BinanceConfigError("broker.product_type must be one of: spot, perpetual")
    if (environment, product_type) not in _ENDPOINTS:
        raise BinanceConfigError("broker.environment must be one of: demo, live")
    symbols = [str(item).strip().upper() for item in broker.get("symbols", []) if str(item).strip()]
    if not symbols:
        raise BinanceConfigError("broker.symbols must contain at least one symbol")

    endpoints = broker.get("endpoints") if isinstance(broker.get("endpoints"), dict) else {}
    defaults = _ENDPOINTS[(environment, product_type)]
    rest_base_url = str(endpoints.get("rest_base_url", defaults["rest_base_url"])).rstrip("/")
    ws_base_url = str(endpoints.get("ws_base_url", defaults["ws_base_url"])).rstrip("/")
    if environment == "demo" and rest_base_url in {"https://fapi.binance.com", "https://api.binance.com"}:
        raise BinanceConfigError("Binance demo credentials cannot use live endpoints")
    if environment == "live" and ("testnet" in rest_base_url or "binancefuture.com" in rest_base_url):
        raise BinanceConfigError("Binance live credentials cannot use testnet endpoints")
    if product_type == "spot" and ("fapi" in rest_base_url or "binancefuture" in rest_base_url):
        raise BinanceConfigError("Binance spot connector cannot use futures endpoints")
    if product_type == "perpetual" and "api.binance.com" in rest_base_url and "fapi" not in rest_base_url:
        raise BinanceConfigError("Binance perpetual connector cannot use spot endpoints")

    auth_raw = broker.get("auth") if isinstance(broker.get("auth"), dict) else {}
    key_default = "BINANCE_API_KEY" if environment == "live" else "BINANCE_DEMO_API_KEY"
    secret_default = "BINANCE_API_SECRET" if environment == "live" else "BINANCE_DEMO_API_SECRET"
    key_env = str(auth_raw.get("api_key_env", key_default)).strip()
    secret_env = str(auth_raw.get("api_secret_env", secret_default)).strip()
    if environment == "live" and ("DEMO" in key_env.upper() or "DEMO" in secret_env.upper()):
        raise BinanceConfigError("Binance live environment cannot use demo auth variables")

    return BinanceBrokerConfig(
        environment=environment,
        product_type=product_type,
        symbols=symbols,
        recv_window_ms=int(broker.get("recv_window_ms", 5000)),
        request_timeout_ms=int(broker.get("request_timeout_ms", 4000)),
        max_retries=int(broker.get("max_retries", 3)),
        retry_backoff_ms=int(broker.get("retry_backoff_ms", 250)),
        rest_base_url=rest_base_url,
        ws_base_url=ws_base_url,
        ws_enabled=bool((broker.get("ws") or {}).get("enabled", True)),
        auth=BinanceAuthConfig(api_key_env=key_env, api_secret_env=secret_env),
    )
