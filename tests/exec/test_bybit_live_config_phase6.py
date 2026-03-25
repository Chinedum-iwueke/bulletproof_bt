from __future__ import annotations

import pytest

from bt.exec.adapters.bybit.config import resolve_bybit_config
from bt.exec.adapters.bybit.errors import BybitConfigError


def test_live_config_rejects_demo_auth_env() -> None:
    with pytest.raises(BybitConfigError):
        resolve_bybit_config(
            {
                "broker": {
                    "venue": "bybit",
                    "environment": "live",
                    "category": "linear",
                    "symbols": ["BTCUSDT"],
                    "auth": {"api_key_env": "BYBIT_DEMO_API_KEY", "api_secret_env": "BYBIT_DEMO_API_SECRET"},
                }
            }
        )


def test_live_config_rejects_demo_ws_endpoint() -> None:
    with pytest.raises(BybitConfigError):
        resolve_bybit_config(
            {
                "broker": {
                    "venue": "bybit",
                    "environment": "live",
                    "category": "linear",
                    "symbols": ["BTCUSDT"],
                    "auth": {"api_key_env": "BYBIT_API_KEY", "api_secret_env": "BYBIT_API_SECRET"},
                    "endpoints": {"private_ws_url": "wss://stream-demo.bybit.com/v5/private"},
                }
            }
        )
