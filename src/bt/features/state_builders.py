"""Causal market state feature construction helpers."""
from __future__ import annotations

from collections.abc import Iterable
import numpy as np
import pandas as pd


def _rolling_percentile(series: pd.Series, window: int) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")

    def _pct(arr: np.ndarray) -> float:
        if arr.size == 0 or np.isnan(arr[-1]):
            return np.nan
        valid = arr[~np.isnan(arr)]
        if valid.size == 0:
            return np.nan
        return float((valid <= valid[-1]).mean())

    return values.rolling(window=window, min_periods=5).apply(_pct, raw=True)


def _bucket(value: float | None, edges: list[tuple[float, float, str]]) -> str | None:
    if value is None or pd.isna(value):
        return None
    for lo, hi, label in edges:
        if value >= lo and value < hi:
            return label
    return edges[-1][2]


def build_state_features(
    bars: pd.DataFrame,
    *,
    symbol: str,
    dataset_id: str | None = None,
    signal_timeframe: str | None = None,
    execution_timeframe: str | None = None,
    percentile_window: int = 200,
) -> pd.DataFrame:
    """Build causal entry_state_* features from OHLCV bars (past-only rolling ops)."""
    if bars.empty:
        return pd.DataFrame()

    df = bars.copy().sort_values("ts").reset_index(drop=True)
    close = pd.to_numeric(df["close"], errors="coerce")
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    volume = pd.to_numeric(df.get("volume", 0.0), errors="coerce").fillna(0.0)

    ema_fast = close.ewm(span=20, adjust=False, min_periods=5).mean()
    ema_slow = close.ewm(span=50, adjust=False, min_periods=10).mean()
    tr = pd.concat([(high - low), (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)
    atr = tr.rolling(14, min_periods=5).mean()
    returns = close.pct_change()
    realized_vol = returns.rolling(20, min_periods=10).std()

    spread_proxy = pd.to_numeric(df.get("spread_proxy", (high - low) / close.replace(0, np.nan)), errors="coerce")
    dollar_volume = close * volume

    out = pd.DataFrame(
        {
            "ts": df["ts"],
            "symbol": symbol,
            "dataset_id": dataset_id,
            "signal_timeframe": signal_timeframe,
            "execution_timeframe": execution_timeframe,
            "entry_state_ema_fast": ema_fast,
            "entry_state_ema_slow": ema_slow,
            "entry_state_ema_relationship": np.where(ema_fast >= ema_slow, "fast_above", "fast_below"),
            "entry_state_ema_separation": (ema_fast - ema_slow) / close.replace(0, np.nan),
            "entry_state_ema_slope_fast": ema_fast.diff(),
            "entry_state_ema_slope_slow": ema_slow.diff(),
            "entry_state_atr": atr,
            "entry_state_atr_pct": atr / close.replace(0, np.nan),
            "entry_state_true_range": tr,
            "entry_state_tr_over_atr": tr / atr.replace(0, np.nan),
            "entry_state_spread_proxy": spread_proxy,
            "entry_state_volume": volume,
            "entry_state_dollar_volume": dollar_volume,
        }
    )
    out["entry_state_vol_pctile"] = _rolling_percentile(realized_vol, percentile_window)
    out["entry_state_atr_pct_pctile"] = _rolling_percentile(out["entry_state_atr_pct"], percentile_window)
    out["entry_state_spread_proxy_pctile"] = _rolling_percentile(spread_proxy, percentile_window)
    out["entry_state_volume_pctile"] = _rolling_percentile(volume, percentile_window)
    out["entry_state_tr_over_atr_pctile"] = _rolling_percentile(out["entry_state_tr_over_atr"], percentile_window)
    out["entry_state_volume_z"] = (volume - volume.rolling(50, min_periods=10).mean()) / volume.rolling(50, min_periods=10).std()

    out["entry_state_csi_raw"] = (
        0.35 * out["entry_state_vol_pctile"].fillna(0.0)
        + 0.35 * out["entry_state_tr_over_atr_pctile"].fillna(0.0)
        - 0.30 * out["entry_state_spread_proxy_pctile"].fillna(0.0)
    ).clip(0.0, 1.0)
    out["entry_state_csi_pctile"] = _rolling_percentile(out["entry_state_csi_raw"], percentile_window)

    out["entry_state_trend_state"] = np.where(
        out["entry_state_ema_fast"] > out["entry_state_ema_slow"],
        "uptrend",
        "downtrend",
    )
    out["entry_state_vol_regime"] = out["entry_state_vol_pctile"].apply(
        lambda v: _bucket(v, [(0.0, 0.3, "vol_low"), (0.3, 0.7, "vol_mid"), (0.7, 0.9, "vol_high"), (0.9, 1.01, "vol_extreme")])
    )
    out["entry_state_liquidity_regime"] = out["entry_state_spread_proxy_pctile"].apply(
        lambda v: _bucket(v, [(0.0, 0.6, "liquid"), (0.6, 0.8, "moderate"), (0.8, 0.95, "fragile"), (0.95, 1.01, "broken")])
    )
    out["entry_state_displacement_regime"] = out["entry_state_tr_over_atr"].apply(
        lambda v: _bucket(v, [(0.0, 1.0, "no_impulse"), (1.0, 1.5, "mild_impulse"), (1.5, 2.0, "strong_impulse"), (2.0, float("inf"), "extreme_impulse")])
    )
    out["entry_state_csi_bucket"] = out["entry_state_csi_raw"].apply(
        lambda v: _bucket(v, [(0.0, 0.5, "csi_low"), (0.5, 0.7, "csi_mid"), (0.7, 0.85, "csi_high"), (0.85, 1.01, "csi_extreme")])
    )
    ts = pd.to_datetime(out["ts"], utc=True, errors="coerce")
    out["entry_state_time_of_day_bucket"] = ts.dt.hour.apply(lambda h: f"h{int(h):02d}" if pd.notna(h) else None)
    out["entry_state_day_of_week"] = ts.dt.day_name()
    out["entry_state_asset_bucket"] = symbol[:3] if symbol else None
    out["entry_state_dataset_bucket"] = dataset_id

    out["entry_state_trend_ready"] = out["entry_state_ema_slow"].notna()
    out["entry_state_vol_ready"] = out["entry_state_vol_pctile"].notna()
    out["entry_state_liquidity_ready"] = out["entry_state_spread_proxy_pctile"].notna()
    out["entry_state_csi_ready"] = out["entry_state_csi_raw"].notna()
    out["entry_state_htf_ready"] = False
    return out
