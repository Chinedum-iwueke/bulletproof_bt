from __future__ import annotations

import pytest

from bt.core.config_resolver import resolve_config
from bt.risk.spec import parse_risk_spec


def test_resolve_config_keeps_canonical_r_per_trade_without_legacy_duplication() -> None:
    resolved = resolve_config({"risk": {"mode": "r_fixed", "r_per_trade": 0.005}})

    assert resolved["risk"]["r_per_trade"] == 0.005
    assert "risk_per_trade_pct" not in resolved["risk"]
    assert "risk_per_trade_pct" not in resolved


def test_resolve_config_maps_legacy_risk_per_trade_pct_to_canonical_key() -> None:
    resolved = resolve_config({"risk": {"mode": "r_fixed", "risk_per_trade_pct": 0.01}})

    assert resolved["risk"]["r_per_trade"] == 0.01
    # Legacy key is preserved only because the input explicitly provided it.
    assert resolved["risk"]["risk_per_trade_pct"] == 0.01


def test_parse_risk_spec_rejects_missing_risk_fraction() -> None:
    resolved = resolve_config({"risk": {"mode": "r_fixed"}})

    with pytest.raises(ValueError, match=r"risk\.mode and risk\.r_per_trade are required"):
        parse_risk_spec(resolved)


def test_resolve_config_injects_default_stop_resolution() -> None:
    resolved = resolve_config({"risk": {"mode": "r_fixed", "r_per_trade": 0.01}})

    assert resolved["risk"]["stop_resolution"] == "safe"
    assert resolved["risk"]["allow_legacy_proxy"] is False


def test_resolve_config_injects_safe_margin_and_slippage_proxy_defaults() -> None:
    resolved = resolve_config({"risk": {"mode": "r_fixed", "r_per_trade": 0.01}})

    assert resolved["risk"]["margin_buffer_tier"] == 1
    assert resolved["risk"]["slippage_k_proxy"] == 0.0


def test_resolve_config_maps_legacy_top_level_slippage_k_only_when_explicit() -> None:
    resolved = resolve_config({"slippage_k": 0.002, "risk": {"mode": "r_fixed", "r_per_trade": 0.01}})

    assert resolved["risk"]["slippage_k_proxy"] == 0.002


def test_resolve_config_rejects_invalid_margin_and_slippage_proxy_values() -> None:
    with pytest.raises(ValueError, match=r"Invalid risk\.margin_buffer_tier"):
        resolve_config({"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "margin_buffer_tier": 99}})

    with pytest.raises(ValueError, match=r"Invalid risk\.slippage_k_proxy"):
        resolve_config({"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "slippage_k_proxy": -1}})


def test_resolve_config_injects_risk_guardrail_defaults() -> None:
    resolved = resolve_config({"risk": {"mode": "r_fixed", "r_per_trade": 0.01}})

    assert resolved["risk"]["min_stop_distance_pct"] == 0.001
    assert resolved["risk"]["max_notional_pct_equity"] == 0.5
    assert resolved["risk"]["max_gross_notional_pct_equity"] == 0.5
    assert resolved["risk"]["maintenance_free_margin_pct"] == 0.01


def test_resolve_config_rejects_invalid_risk_guardrail_values() -> None:
    with pytest.raises(ValueError, match=r"Invalid risk\.min_stop_distance_pct"):
        resolve_config({"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "min_stop_distance_pct": 0.2}})

    with pytest.raises(ValueError, match=r"Invalid risk\.max_notional_pct_equity"):
        resolve_config({"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "max_notional_pct_equity": 0.0}})

    with pytest.raises(ValueError, match=r"Invalid risk\.max_gross_notional_pct_equity"):
        resolve_config({"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "max_gross_notional_pct_equity": 0.0}})

    with pytest.raises(ValueError, match=r"Invalid risk\.maintenance_free_margin_pct"):
        resolve_config({"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "maintenance_free_margin_pct": 0.5}})


def test_config_used_yaml_contains_injected_risk_guardrail_defaults(tmp_path) -> None:
    import yaml

    from bt.logging.trades import write_config_used

    resolved = resolve_config({"risk": {"mode": "r_fixed", "r_per_trade": 0.01}})
    write_config_used(tmp_path, resolved)

    payload = yaml.safe_load((tmp_path / "config_used.yaml").read_text(encoding="utf-8"))
    assert payload["risk"]["min_stop_distance_pct"] == 0.001
    assert payload["risk"]["max_notional_pct_equity"] == 0.5
    assert payload["risk"]["max_gross_notional_pct_equity"] == 0.5
    assert payload["risk"]["maintenance_free_margin_pct"] == 0.01
