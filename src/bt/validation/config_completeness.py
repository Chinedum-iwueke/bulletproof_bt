"""Resolved-config completeness checks for critical runtime knobs."""
from __future__ import annotations

from typing import Any

from bt.core.config_resolver import ConfigError


def _has_key(mapping: dict[str, Any], path: str) -> bool:
    current: Any = mapping
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return False
        current = current[part]
    return True


def validate_resolved_config_completeness(config: dict[str, Any]) -> None:
    """Fail fast if the resolved config omits critical runtime keys."""
    if not isinstance(config, dict):
        raise ConfigError("Resolved config must be a mapping")

    missing: list[str] = []
    required_paths = [
        "execution.intrabar_mode",
        "signal_delay_bars",
        "initial_cash",
        "outputs.root_dir",
        "outputs.jsonl",
        "model",
        "strategy.name",
        "execution.spread_mode",
        "data.mode",
        "data.symbols_subset",
        "risk.mode",
        "risk.r_per_trade",
        "risk.max_positions",
        "risk.max_leverage",
        "risk.stop_resolution",
        "risk.allow_legacy_proxy",
        "risk.may_liquidate",
        "risk.margin_buffer_tier",
        "risk.slippage_k_proxy",
        "risk.min_stop_distance_pct",
        "risk.max_notional_pct_equity",
        "risk.max_gross_notional_pct_equity",
        "risk.maintenance_free_margin_pct",
    ]
    for path in required_paths:
        if not _has_key(config, path):
            missing.append(path)

    model_value = config.get("model")
    if model_value == "fixed_bps" and not _has_key(config, "fixed_bps"):
        missing.append("fixed_bps")

    data_cfg = config.get("data") if isinstance(config.get("data"), dict) else {}
    data_mode = data_cfg.get("mode")
    if data_mode == "streaming" and "chunksize" not in data_cfg:
        missing.append("data.chunksize")

    if "htf_timeframes" in config:
        if not _has_key(config, "htf_resampler.timeframes"):
            missing.append("htf_resampler.timeframes")
        if not _has_key(config, "htf_resampler.strict"):
            missing.append("htf_resampler.strict")

    risk_cfg = config.get("risk") if isinstance(config.get("risk"), dict) else {}
    if "stop" in risk_cfg and not _has_key(config, "risk.stop"):
        missing.append("risk.stop")

    if missing:
        rendered = ", ".join(sorted(set(missing)))
        raise ConfigError(
            "Resolved config is missing required keys: "
            f"{rendered}. "
            "This usually indicates a missing default injection or config resolver regression. "
            "Fix by adding defaults in config_resolver or specifying them in configs/engine.yaml."
        )
