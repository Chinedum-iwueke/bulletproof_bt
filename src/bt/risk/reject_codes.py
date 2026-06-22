"""Stable reason-code taxonomy for risk rejections and fallback outcomes."""

from bt.core.reason_codes import (
    RISK_REJECT_INSUFFICIENT_MARGIN as _rc_insufficient_margin,
    RISK_REJECT_MAX_POSITIONS as _rc_max_positions,
    RISK_REJECT_MIN_STOP_DISTANCE as _rc_min_stop_distance,
    RISK_REJECT_NOTIONAL_CAP as _rc_notional_cap,
    RISK_REJECT_STOP_UNRESOLVABLE as _rc_stop_unresolvable,
    RISK_SCALED_BY_MARGIN as _rc_scaled_by_margin,
)

# Stop/contract
STOP_UNRESOLVABLE_STRICT = "risk_rejected:stop_unresolvable:strict"
STOP_UNRESOLVABLE_SAFE_NO_PROXY = "risk_rejected:stop_unresolvable:safe_no_proxy"
STOP_FALLBACK_LEGACY_PROXY = "risk_fallback:stop_legacy_proxy"
MIN_STOP_DISTANCE_VIOLATION = "risk_rejected:min_stop_distance_violation"

# Exposure/margin
INSUFFICIENT_FREE_MARGIN = "risk_rejected:insufficient_free_margin"
MAX_POSITIONS_REACHED = "risk_rejected:max_positions_reached"
MAX_NOTIONAL_EXCEEDED = "risk_rejected:max_notional_exceeded"
MAX_NOTIONAL_PCT_EQUITY_EXCEEDED = "risk_rejected:max_notional_pct_equity_exceeded"
MAX_GROSS_NOTIONAL_PCT_EQUITY_EXCEEDED = "risk_rejected:max_gross_notional_pct_equity_exceeded"

# Additional stable reject codes used by RiskEngine
NO_SIDE = "risk_rejected:no_side"
SYMBOL_MISMATCH = "risk_rejected:symbol_mismatch"
NO_EQUITY = "risk_rejected:no_equity"
CLOSE_ONLY_NO_POSITION = "risk_rejected:close_only_no_position"
ALREADY_IN_POSITION = "risk_rejected:already_in_position"
INVALID_SIDE = "risk_rejected:invalid_side"
INVALID_FLIP = "risk_rejected:invalid_flip"
QTY_SIGN_INVARIANT_FAILED = "risk_rejected:qty_sign_invariant_failed"
RISK_APPROVED = "risk_approved"
RISK_APPROVED_CLOSE_ONLY = "risk_approved:close_only"

# Backwards-compatible aliases for older stop resolver/reporting references.
RISK_REJECT_STOP_MISSING = STOP_UNRESOLVABLE_STRICT
RISK_REJECT_STOP_UNRESOLVABLE = STOP_UNRESOLVABLE_STRICT
RISK_REJECT_ATR_NOT_READY = "risk_rejected:atr_not_ready"
RISK_REJECT_INVALID_STOP_DISTANCE = "risk_rejected:invalid_stop_distance"
RISK_REJECT_MIN_STOP_DISTANCE = MIN_STOP_DISTANCE_VIOLATION
RISK_FALLBACK_LEGACY_PROXY = STOP_FALLBACK_LEGACY_PROXY

_ALL_CODES = {
    STOP_UNRESOLVABLE_STRICT,
    STOP_UNRESOLVABLE_SAFE_NO_PROXY,
    STOP_FALLBACK_LEGACY_PROXY,
    MIN_STOP_DISTANCE_VIOLATION,
    INSUFFICIENT_FREE_MARGIN,
    MAX_POSITIONS_REACHED,
    MAX_NOTIONAL_EXCEEDED,
    MAX_NOTIONAL_PCT_EQUITY_EXCEEDED,
    MAX_GROSS_NOTIONAL_PCT_EQUITY_EXCEEDED,
    NO_SIDE,
    SYMBOL_MISMATCH,
    NO_EQUITY,
    CLOSE_ONLY_NO_POSITION,
    ALREADY_IN_POSITION,
    INVALID_SIDE,
    INVALID_FLIP,
    QTY_SIGN_INVARIANT_FAILED,
    RISK_APPROVED,
    RISK_APPROVED_CLOSE_ONLY,
    RISK_REJECT_ATR_NOT_READY,
    RISK_REJECT_INVALID_STOP_DISTANCE,
    _rc_insufficient_margin,
    _rc_max_positions,
    _rc_notional_cap,
    _rc_stop_unresolvable,
    _rc_min_stop_distance,
    _rc_scaled_by_margin,
}


def is_known(code: str) -> bool:
    """Returns True when code belongs to the stable risk taxonomy."""
    return code in _ALL_CODES


def validate_known(code: str) -> None:
    """Raises ValueError for unknown risk reject/reason codes."""
    if not is_known(code):
        raise ValueError(f"Unknown risk reject code: {code}")
