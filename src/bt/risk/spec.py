"""Structured risk sizing configuration."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class RiskSpec:
    """Validated risk sizing settings.

    Defaults:
    - ``min_stop_distance`` defaults to ``None`` when omitted.
    - ``max_leverage`` defaults to ``None`` when omitted.
    """

    mode: Literal["r_fixed", "equity_pct", "risk_at_stop", "fixed_notional_pct_equity"]
    r_per_trade: float | None
    notional_pct_equity: float | None
    cap_policy: Literal["allow_clip_with_truth", "reject_if_clipped"]
    min_risk_utilization_pct: float
    report_under_risked_trades: bool
    min_stop_distance: float | None
    max_leverage: float | None
    maintenance_free_margin_pct: float


def _as_positive_float(value: object, key: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Invalid risk.{key}: expected positive float got {value!r}")

    parsed = float(value)
    if parsed <= 0:
        raise ValueError(f"Invalid risk.{key}: expected positive float got {value!r}")
    return parsed


def _as_optional_positive_float(value: object, key: str) -> float | None:
    if value is None:
        return None
    return _as_positive_float(value, key)


def _as_fraction(value: object, key: str, *, allow_zero: bool = False, upper: float = 5.0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Invalid risk.{key}: expected float got {value!r}")
    parsed = float(value)
    lower_ok = parsed >= 0 if allow_zero else parsed > 0
    if not lower_ok or parsed > upper:
        bound = f"[0.0, {upper}]" if allow_zero else f"(0.0, {upper}]"
        raise ValueError(f"Invalid risk.{key}: expected float in {bound} got {value!r}")
    return parsed


def parse_risk_spec(config: dict[str, object]) -> RiskSpec:
    """Parse and validate a :class:`RiskSpec` from a config mapping."""

    risk_cfg = config.get("risk", {})
    if not isinstance(risk_cfg, dict):
        raise ValueError("risk.mode and risk.r_per_trade are required")

    raw_mode = risk_cfg.get("mode")
    raw_r_per_trade = risk_cfg.get("r_per_trade")
    if raw_mode is None:
        raise ValueError("risk.mode is required")

    if raw_mode not in ("r_fixed", "equity_pct", "risk_at_stop", "fixed_notional_pct_equity"):
        raise ValueError(
            "Invalid risk.mode: expected 'r_fixed', 'equity_pct', 'risk_at_stop', "
            f"or 'fixed_notional_pct_equity' got {raw_mode!r}"
        )

    r_per_trade: float | None = None
    notional_pct_equity: float | None = None
    if raw_mode in ("r_fixed", "equity_pct", "risk_at_stop"):
        if raw_r_per_trade is None:
            raise ValueError("risk.r_per_trade is required for risk-at-stop sizing")
        r_per_trade = _as_positive_float(raw_r_per_trade, "r_per_trade")
    else:
        raw_notional_pct = risk_cfg.get("notional_pct_equity")
        if raw_notional_pct is None:
            raise ValueError("risk.notional_pct_equity is required for fixed_notional_pct_equity sizing")
        notional_pct_equity = _as_fraction(raw_notional_pct, "notional_pct_equity")

    raw_cap_policy = risk_cfg.get("cap_policy", "allow_clip_with_truth")
    if raw_cap_policy not in ("allow_clip_with_truth", "reject_if_clipped"):
        raise ValueError(
            "Invalid risk.cap_policy: expected 'allow_clip_with_truth' or "
            f"'reject_if_clipped' got {raw_cap_policy!r}"
        )
    min_risk_utilization_pct = _as_fraction(
        risk_cfg.get("min_risk_utilization_pct", 0.0),
        "min_risk_utilization_pct",
        allow_zero=True,
        upper=1.0,
    )
    raw_report = risk_cfg.get("report_under_risked_trades", True)
    if not isinstance(raw_report, bool):
        raise ValueError(
            "Invalid risk.report_under_risked_trades: expected boolean "
            f"got {raw_report!r}"
        )

    min_stop_distance = _as_optional_positive_float(risk_cfg.get("min_stop_distance"), "min_stop_distance")
    max_leverage = _as_optional_positive_float(risk_cfg.get("max_leverage"), "max_leverage")

    raw_maintenance = risk_cfg.get("maintenance_free_margin_pct", 0.01)
    if isinstance(raw_maintenance, bool) or not isinstance(raw_maintenance, (int, float)):
        raise ValueError(
            "Invalid risk.maintenance_free_margin_pct: expected float in [0.0, 0.20] "
            f"got {raw_maintenance!r}"
        )
    maintenance_free_margin_pct = float(raw_maintenance)
    if not (0.0 <= maintenance_free_margin_pct <= 0.20):
        raise ValueError(
            "Invalid risk.maintenance_free_margin_pct: expected float in [0.0, 0.20] "
            f"got {raw_maintenance!r}"
        )

    return RiskSpec(
        mode=raw_mode,
        r_per_trade=r_per_trade,
        notional_pct_equity=notional_pct_equity,
        cap_policy=raw_cap_policy,
        min_risk_utilization_pct=min_risk_utilization_pct,
        report_under_risked_trades=raw_report,
        min_stop_distance=min_stop_distance,
        max_leverage=max_leverage,
        maintenance_free_margin_pct=maintenance_free_margin_pct,
    )
