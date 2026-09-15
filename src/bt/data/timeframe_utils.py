"""Utilities for validated timeframe alignment checks."""
from __future__ import annotations

import pandas as pd

from bt.data.resample import timeframe_minutes


def is_timeframe_boundary(ts: pd.Timestamp, timeframe: str) -> bool:
    """Return ``True`` when ``ts`` is aligned to the given timeframe boundary."""
    return ts == ts.floor(f"{timeframe_minutes(timeframe)}min")
