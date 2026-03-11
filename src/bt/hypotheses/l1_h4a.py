"""L1-H4A hypothesis primitives (liquidity-uncertainty gate over L1-H2)."""
from __future__ import annotations

from bt.core.types import Bar
from bt.hypotheses.l1_h2 import RollingQuantileGate, bars_for_30_calendar_days


def spread_proxy_from_bar(bar: Bar) -> float | None:
    """Deterministic spread proxy: 0.5 * (high - low) / close."""
    if bar.close <= 0:
        return None
    return float(0.5 * (bar.high - bar.low) / bar.close)


__all__ = ["RollingQuantileGate", "bars_for_30_calendar_days", "spread_proxy_from_bar"]
