from __future__ import annotations

import pytest

from bt.validation.config_completeness import validate_resolved_config_completeness


def _full_config() -> dict:
    return {
        "signal_delay_bars": 1,
        "initial_cash": 100000.0,
        "model": "fixed_bps",
        "fixed_bps": 5.0,
        "outputs": {"root_dir": "outputs/runs", "jsonl": True},
        "strategy": {"name": "coinflip"},
        "execution": {"intrabar_mode": "worst_case", "spread_mode": "none", "spread_bps": 0.0},
        "data": {"mode": "streaming", "symbols_subset": None, "chunksize": 50000},
        "risk": {
            "mode": "equity_pct",
            "r_per_trade": 0.005,
            "max_positions": 5,
            "max_leverage": 2.0,
            "stop_resolution": "safe",
            "allow_legacy_proxy": False,
            "may_liquidate": True,
            "margin_buffer_tier": 1,
            "slippage_k_proxy": 0.0,
            "min_stop_distance_pct": 0.001,
            "max_notional_pct_equity": 1.0,
            "max_gross_notional_pct_equity": None,
            "maintenance_free_margin_pct": 0.01,
        },
    }


def test_validate_resolved_config_completeness_raises_for_missing_keys() -> None:
    config = _full_config()
    del config["outputs"]["root_dir"]
    del config["risk"]["max_leverage"]

    with pytest.raises(ValueError, match="outputs.root_dir"):
        validate_resolved_config_completeness(config)


def test_validate_resolved_config_completeness_passes_for_complete_config() -> None:
    validate_resolved_config_completeness(_full_config())
