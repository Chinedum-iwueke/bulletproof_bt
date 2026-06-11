"""Read-once data session helpers for future fast-path kernels."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SymbolArrays:
    symbol: str
    ts: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray


class DataSession:
    """Load canonical data once and expose contiguous NumPy arrays.

    This class is deliberately independent from the classic engine. The current
    fast path uses it only for supported prototypes; unsupported strategies fall
    back before any semantic changes are made.
    """

    def __init__(self, *, data_path: str | Path, config: dict[str, Any]) -> None:
        self.data_path = Path(data_path)
        self.config = config
        self._bars: pd.DataFrame | None = None
        self._arrays: dict[str, SymbolArrays] = {}

    def load_frame(self) -> pd.DataFrame:
        if self._bars is not None:
            return self._bars
        if self.data_path.is_file() and self.data_path.suffix.lower() == ".parquet":
            frame = pd.read_parquet(self.data_path)
        else:
            raise ValueError("fast DataSession currently supports parquet files only")
        required = {"ts", "symbol", "open", "high", "low", "close", "volume"}
        missing = required - set(frame.columns)
        if missing:
            raise ValueError(f"fast DataSession missing required columns: {sorted(missing)}")
        frame = frame.sort_values(["symbol", "ts"], kind="mergesort").reset_index(drop=True)
        frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
        self._bars = frame
        return frame

    def arrays_for_symbol(self, symbol: str) -> SymbolArrays:
        symbol = str(symbol)
        cached = self._arrays.get(symbol)
        if cached is not None:
            return cached
        frame = self.load_frame()
        part = frame[frame["symbol"].astype(str).eq(symbol)]
        if part.empty:
            raise ValueError(f"symbol not present in fast DataSession: {symbol}")
        arrays = SymbolArrays(
            symbol=symbol,
            ts=part["ts"].astype("int64").to_numpy(copy=True),
            open=part["open"].to_numpy(dtype="float64", copy=True),
            high=part["high"].to_numpy(dtype="float64", copy=True),
            low=part["low"].to_numpy(dtype="float64", copy=True),
            close=part["close"].to_numpy(dtype="float64", copy=True),
            volume=part["volume"].to_numpy(dtype="float64", copy=True),
        )
        self._arrays[symbol] = arrays
        return arrays

