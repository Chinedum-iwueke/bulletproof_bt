"""Bucket definitions and assignment helpers for structural EV analysis."""
from __future__ import annotations

import pandas as pd


def assign_bucket(series: pd.Series, spec: list[tuple[float, float, str]]) -> pd.Series:
    vals = pd.to_numeric(series, errors="coerce")

    def _one(v: float) -> str | None:
        if pd.isna(v):
            return None
        for lo, hi, label in spec:
            if v >= lo and v < hi:
                return label
        return spec[-1][2]

    return vals.apply(_one)


CSI_SPEC = [(0.0, 0.5, "csi_low"), (0.5, 0.7, "csi_mid"), (0.7, 0.85, "csi_high"), (0.85, 1.01, "csi_extreme")]
VOL_SPEC = [(0.0, 0.3, "vol_low"), (0.3, 0.7, "vol_mid"), (0.7, 0.9, "vol_high"), (0.9, 1.01, "vol_extreme")]
LIQ_SPEC = [(0.0, 0.6, "liquid"), (0.6, 0.8, "moderate"), (0.8, 0.95, "fragile"), (0.95, 1.01, "broken")]
DISP_SPEC = [(0.0, 1.0, "no_impulse"), (1.0, 1.5, "mild_impulse"), (1.5, 2.0, "strong_impulse"), (2.0, float("inf"), "extreme_impulse")]
