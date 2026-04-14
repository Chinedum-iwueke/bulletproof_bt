"""L1-H10 reusable high-win-rate/tight-TP primitives."""
from __future__ import annotations


def vwap_deviation_z(*, close: float, session_vwap: float, atr: float) -> float | None:
    """Return VWAP deviation in ATR units."""
    if atr <= 0:
        return None
    return (float(close) - float(session_vwap)) / float(atr)


def breakout_distance_atr(*, close: float, reference: float, atr: float, side: str) -> float | None:
    """Return directional breakout distance from reference in ATR units."""
    if atr <= 0:
        return None
    side_key = str(side).lower()
    if side_key == "long":
        raw = float(close) - float(reference)
    elif side_key == "short":
        raw = float(reference) - float(close)
    else:
        raise ValueError(f"Unsupported side={side!r}")
    return raw / float(atr)


__all__ = ["vwap_deviation_z", "breakout_distance_atr"]
