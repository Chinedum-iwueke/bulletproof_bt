from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class ClosedBarGate:
    close_bar_only: bool = True
    warmup_bars: int = 0

    def __post_init__(self) -> None:
        self._seen = 0
        self._last_ts: pd.Timestamp | None = None

    def is_eligible(self, *, ts: pd.Timestamp) -> bool:
        if self._last_ts is not None and ts <= self._last_ts:
            return False
        self._last_ts = ts
        self._seen += 1
        if self._seen <= self.warmup_bars:
            return False
        return bool(self.close_bar_only)
