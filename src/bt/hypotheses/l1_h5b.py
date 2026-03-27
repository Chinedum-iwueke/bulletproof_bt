"""L1-H5B hypothesis primitives.

L1-H5B reuses the deterministic volatility overlay primitives introduced in L1-H5A.
"""
from __future__ import annotations

from bt.hypotheses.l1_h5a import (  # re-export for explicit hypothesis-level API
    RollingMedianReference,
    RollingRmsVolatility,
    clipped_inverse_vol_scale,
    vol_window_bars,
)

__all__ = [
    "RollingMedianReference",
    "RollingRmsVolatility",
    "clipped_inverse_vol_scale",
    "vol_window_bars",
]
