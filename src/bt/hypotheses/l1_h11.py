"""L1-H11 reusable quality-filtered continuation primitives."""
from __future__ import annotations


def swing_distance_atr(*, trend_dir: str, trend_anchor_price: float, trend_extreme_price: float, atr: float) -> float | None:
    """Return directional impulse distance from trend anchor to trend extreme in ATR units."""
    if atr <= 0:
        return None
    side = str(trend_dir).lower()
    if side == "long":
        raw = float(trend_extreme_price) - float(trend_anchor_price)
    elif side == "short":
        raw = float(trend_anchor_price) - float(trend_extreme_price)
    else:
        raise ValueError(f"Unsupported trend_dir={trend_dir!r}")
    return raw / float(atr)


def pullback_depth_atr(*, trend_dir: str, ema_fast: float, pullback_extreme_low: float, pullback_extreme_high: float, atr: float) -> float | None:
    """Return pullback depth relative to EMA fast in ATR units."""
    if atr <= 0:
        return None
    side = str(trend_dir).lower()
    if side == "long":
        depth = float(ema_fast) - float(pullback_extreme_low)
    elif side == "short":
        depth = float(pullback_extreme_high) - float(ema_fast)
    else:
        raise ValueError(f"Unsupported trend_dir={trend_dir!r}")
    return depth / float(atr)


def entry_position_ratio(*, trend_dir: str, entry_price: float, pullback_extreme_low: float, pullback_extreme_high: float, trend_extreme_price: float) -> float | None:
    """Entry position inside pullback geometry in [0, 1] where 1 is nearest trend extreme."""
    side = str(trend_dir).lower()
    if side == "long":
        denom = float(trend_extreme_price) - float(pullback_extreme_low)
        if denom <= 0:
            return None
        raw = (float(entry_price) - float(pullback_extreme_low)) / denom
    elif side == "short":
        denom = float(pullback_extreme_high) - float(trend_extreme_price)
        if denom <= 0:
            return None
        raw = (float(pullback_extreme_high) - float(entry_price)) / denom
    else:
        raise ValueError(f"Unsupported trend_dir={trend_dir!r}")
    return max(0.0, min(1.0, float(raw)))


__all__ = ["swing_distance_atr", "pullback_depth_atr", "entry_position_ratio"]
