from __future__ import annotations

from dataclasses import dataclass
from numbers import Real
from typing import Any

from bt.risk.stop_resolution import (
    STOP_RESOLUTION_ATR_MULTIPLE,
    STOP_RESOLUTION_EXPLICIT_STOP_PRICE,
    STOP_RESOLUTION_LEGACY_HIGH_LOW_PROXY,
)


@dataclass(frozen=True)
class StopDistanceResult:
    stop_distance: float
    source: str
    details: dict[str, Any]


def _build_stop_result(*, stop_distance: float, source: str, details: dict[str, Any]) -> StopDistanceResult:
    if not source:
        raise ValueError("stop resolution source must be non-empty")
    return StopDistanceResult(stop_distance=stop_distance, source=source, details=details)


def _get_indicator_value(ctx: dict[str, Any], symbol: str, name: str) -> Any:
    indicators = ctx.get("indicators", {})
    symbol_indicators = indicators.get(symbol)
    if isinstance(symbol_indicators, dict) and name in symbol_indicators:
        return symbol_indicators[name]
    return indicators.get(name)


def _extract_indicator_numeric(indicator: Any, symbol: str, name: str) -> float:
    if isinstance(indicator, Real):
        value = float(indicator)
    elif hasattr(indicator, "is_ready") and hasattr(indicator, "value"):
        if not bool(indicator.is_ready):
            raise ValueError(f"{symbol}: ATR indicator '{name}' is not ready. Ensure enough history is available before sizing.")
        value = float(indicator.value)
    else:
        raise ValueError(
            f"{symbol}: ATR indicator '{name}' is invalid in ctx. Provide a numeric value or indicator object with is_ready/value."
        )

    if value <= 0:
        raise ValueError(f"{symbol}: ATR indicator '{name}' must be > 0, got {value}.")
    return value


def resolve_stop_distance(
    *,
    symbol: str,
    side: str,  # "long" or "short"
    entry_price: float,
    signal: Any,
    bars_by_symbol: dict[str, Any],
    ctx: dict[str, Any],
    config: dict[str, Any],
) -> StopDistanceResult:
    """
    Compute stop distance in price units for risk sizing.

    Priority order:
      1) If signal provides an explicit stop price -> use it.
      2) Else, if config provides ATR-multiple stop rule and ATR is available in ctx -> use it.
      3) Else raise ValueError with actionable guidance.

    Must validate:
      - entry_price > 0
      - stop_distance > 0
      - if explicit stop: stop must be on correct side of entry for long/short
      - ATR must be ready and positive when used
    """
    if entry_price <= 0:
        raise ValueError(f"{symbol}: entry_price must be > 0, got {entry_price}.")
    if side not in {"long", "short"}:
        raise ValueError(f"{symbol}: side must be 'long' or 'short', got {side!r}.")

    stop_price = None
    if isinstance(signal, dict):
        stop_price = signal.get("stop_price")
    if stop_price is None:
        stop_price = getattr(signal, "stop_price", None)

    if stop_price is not None:
        stop_price = float(stop_price)
        is_valid_direction = (side == "long" and stop_price < entry_price) or (side == "short" and stop_price > entry_price)
        stop_distance = abs(entry_price - stop_price)
        if stop_distance <= 0:
            raise ValueError(f"{symbol}: invalid stop_price for {side}: stop={stop_price} entry={entry_price}")
        details: dict[str, Any] = {"stop_price": stop_price}
        if not is_valid_direction:
            details["direction_mismatch_vs_entry"] = True
            details["direction_mismatch_side"] = side
        return _build_stop_result(
            stop_distance=stop_distance,
            source=STOP_RESOLUTION_EXPLICIT_STOP_PRICE,
            details=details,
        )

    risk_cfg = config.get("risk", {})
    stop_cfg = risk_cfg.get("stop", {})
    mode = stop_cfg.get("mode")
    if mode == "atr":
        atr_name = stop_cfg.get("atr_indicator", "atr")
        if "atr_multiple" not in stop_cfg:
            raise ValueError(
                f"{symbol}: risk.stop.mode=atr requires risk.stop.atr_multiple > 0 and ATR available in ctx."
            )
        atr_multiple = float(stop_cfg.get("atr_multiple"))
        if atr_multiple <= 0:
            raise ValueError(f"{symbol}: risk.stop.atr_multiple must be > 0, got {atr_multiple}.")

        indicator_value = _get_indicator_value(ctx, symbol, atr_name)
        if indicator_value is None:
            raise ValueError(
                f"{symbol}: ATR indicator '{atr_name}' not found in ctx. Configure ctx.indicators and ensure ATR is available."
            )
        atr_value = _extract_indicator_numeric(indicator_value, symbol, atr_name)
        stop_distance = atr_multiple * atr_value
        if stop_distance <= 0:
            raise ValueError(f"{symbol}: computed stop_distance must be > 0, got {stop_distance}.")
        return _build_stop_result(
            stop_distance=stop_distance,
            source=STOP_RESOLUTION_ATR_MULTIPLE,
            details={"atr_multiple": atr_multiple, "atr_value": atr_value, "atr_name": atr_name},
        )

    if mode == "legacy_proxy":
        bar = bars_by_symbol.get(symbol)
        if bar is None:
            raise ValueError(f"{symbol}: bars_by_symbol is missing current bar for legacy proxy stop resolution.")
        high = float(getattr(bar, "high"))
        low = float(getattr(bar, "low"))
        if side == "long":
            stop_distance = entry_price - low
        else:
            stop_distance = high - entry_price
        if stop_distance <= 0:
            raise ValueError(f"{symbol}: legacy proxy stop_distance must be > 0, got {stop_distance}.")
        return _build_stop_result(
            stop_distance=stop_distance,
            source=STOP_RESOLUTION_LEGACY_HIGH_LOW_PROXY,
            details={"proxy_high": high, "proxy_low": low},
        )

    raise ValueError(
        f"{symbol}: stop distance cannot be resolved. Provide signal.stop_price or configure risk.stop.mode=atr with risk.stop.atr_multiple and ensure ATR is available in ctx."
    )
