from __future__ import annotations

import pandas as pd


def detect_tail_generation_states(metrics: pd.DataFrame) -> pd.DataFrame:
    if metrics.empty:
        return metrics
    return metrics[(metrics["tail_5r_rate"] >= 0.05) | (metrics["tail_10r_rate"] >= 0.01)].copy()
