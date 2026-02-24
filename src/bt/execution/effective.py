from __future__ import annotations

from dataclasses import asdict
from typing import Any

from bt.execution.intrabar import parse_intrabar_spec
from bt.execution.profile import resolve_execution_profile

_VALID_SPREAD_MODES = {"none", "fixed_bps", "bar_range_proxy", "fixed_pips"}


def _as_spread_bps(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid execution.spread_bps: expected a number, got {value!r}") from exc
    if parsed < 0:
        raise ValueError(f"Invalid execution.spread_bps: expected >= 0, got {parsed!r}")
    return parsed


def _as_spread_pips(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid execution.spread_pips: expected a number, got {value!r}") from exc
    if parsed <= 0:
        raise ValueError(f"Invalid execution.spread_pips: expected > 0, got {parsed!r}")
    return parsed


def build_effective_execution_snapshot(config: dict[str, Any]) -> dict[str, Any]:
    """
    Return a deterministic dict describing the effective execution assumptions.
    """
    profile = resolve_execution_profile(config)
    execution_cfg_raw = config.get("execution") if isinstance(config, dict) else None
    execution_cfg = execution_cfg_raw if isinstance(execution_cfg_raw, dict) else {}

    spread_mode_raw = execution_cfg.get("spread_mode", "none")
    spread_mode = spread_mode_raw if isinstance(spread_mode_raw, str) else "none"
    if spread_mode not in _VALID_SPREAD_MODES:
        raise ValueError(
            "Invalid execution.spread_mode: expected one of none|fixed_bps|bar_range_proxy|fixed_pips, "
            f"got {spread_mode_raw!r}"
        )

    snapshot: dict[str, Any] = {
        "execution_profile": profile.name,
        "effective_execution": asdict(profile),
        "spread_mode": spread_mode,
        "intrabar_mode": parse_intrabar_spec(config).mode,
    }
    snapshot["effective_execution"].pop("name", None)

    if spread_mode == "fixed_bps":
        spread_bps_value = execution_cfg.get("spread_bps")
        if spread_bps_value is None:
            spread_bps = profile.spread_bps
        else:
            spread_bps = _as_spread_bps(spread_bps_value)
        snapshot["spread_bps"] = spread_bps
    elif spread_mode == "fixed_pips":
        spread_pips_value = execution_cfg.get("spread_pips")
        if spread_pips_value is None:
            raise ValueError(
                "Invalid execution.spread_pips: required when execution.spread_mode='fixed_pips'"
            )
        snapshot["spread_pips"] = _as_spread_pips(spread_pips_value)

    return snapshot
