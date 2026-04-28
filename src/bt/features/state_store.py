"""Market state snapshot store with as-of causal access."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from bt.features.state_schema import STATE_FIELDS


@dataclass
class MarketStateStore:
    _frames: dict[str, pd.DataFrame]

    @classmethod
    def from_frame(cls, df: pd.DataFrame, *, symbol_col: str = "symbol") -> "MarketStateStore":
        frames: dict[str, pd.DataFrame] = {}
        for symbol, part in df.groupby(symbol_col):
            p = part.copy().sort_values("ts")
            p["ts"] = pd.to_datetime(p["ts"], utc=True, errors="coerce")
            frames[str(symbol)] = p
        return cls(_frames=frames)

    def snapshot(self, *, symbol: str, ts: pd.Timestamp) -> dict[str, Any]:
        frame = self._frames.get(symbol)
        base = {field: None for field in STATE_FIELDS}
        if frame is None:
            base.update({"symbol": symbol, "ts": ts})
            return base

        ts_utc = pd.Timestamp(ts).tz_convert("UTC") if pd.Timestamp(ts).tzinfo else pd.Timestamp(ts, tz="UTC")
        eligible = frame.loc[frame["ts"] <= ts_utc]
        if eligible.empty:
            base.update({"symbol": symbol, "ts": ts_utc})
            return base

        row = eligible.iloc[-1].to_dict()
        for key in STATE_FIELDS:
            if key in row:
                base[key] = row[key]
        base["symbol"] = symbol
        base["ts"] = base.get("ts") or ts_utc
        return base
