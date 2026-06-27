from __future__ import annotations

import pytest

from bt.exec.adapters.bybit.config import resolve_bybit_config
from bt.exec.adapters.bybit.errors import BybitConfigError


def _base(environment: str) -> dict[str, object]:
    key_env = "BYBIT_API_KEY" if environment == "live" else "BYBIT_DEMO_API_KEY"
    secret_env = "BYBIT_API_SECRET" if environment == "live" else "BYBIT_DEMO_API_SECRET"
    return {
        "broker": {
            "venue": "bybit",
            "environment": environment,
            "category": "linear",
            "symbols": ["BTCUSDT"],
            "auth": {"api_key_env": key_env, "api_secret_env": secret_env},
        }
    }


def test_valid_demo_config() -> None:
    cfg = resolve_bybit_config(_base("demo"))
    assert cfg.environment == "demo"
    assert "api-demo.bybit.com" in cfg.rest_base_url


def test_valid_live_config() -> None:
    cfg = resolve_bybit_config(_base("live"))
    assert cfg.environment == "live"
    assert "api.bybit.com" in cfg.rest_base_url


def test_missing_auth_block_rejected() -> None:
    with pytest.raises(BybitConfigError):
        resolve_bybit_config({"broker": {"venue": "bybit", "environment": "demo", "symbols": ["BTCUSDT"]}})


def test_bad_endpoint_combo_rejected() -> None:
    with pytest.raises(BybitConfigError):
        resolve_bybit_config(
            {
                "broker": {
                    **_base("demo")["broker"],
                    "endpoints": {"rest_base_url": "https://api.bybit.com"},
                }
            }
        )


def test_spot_product_resolves_spot_category_and_stream() -> None:
    config = _base("demo")
    config["broker"]["product_type"] = "spot"
    cfg = resolve_bybit_config(config)
    assert cfg.product_type == "spot"
    assert cfg.category == "spot"
    assert cfg.public_ws_url == "wss://stream.bybit.com/v5/public/spot"
    assert cfg.public_ws_url.endswith("/spot")
