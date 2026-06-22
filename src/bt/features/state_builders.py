"""Causal market state feature construction helpers."""
from __future__ import annotations

from collections.abc import Iterable
import json
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
        current = valid[-1]
        # Mid-rank ties keep a constant causal series neutral (0.5) instead
        # of falsely classifying every repeated observation as an extreme.
        return float(((valid < current).sum() + 0.5 * (valid == current).sum()) / valid.size)

    return values.rolling(window=window, min_periods=5).apply(_pct, raw=True)


def _bucket(value: float | None, edges: list[tuple[float, float, str]]) -> str | None:
    if value is None or pd.isna(value):
        return None
    for lo, hi, label in edges:
        if value >= lo and value < hi:
            return label
    return edges[-1][2]


def _first_numeric(df: pd.DataFrame, aliases: tuple[str, ...]) -> pd.Series | None:
    for col in aliases:
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce")
    return None


def _zscore(series: pd.Series, window: int = 50) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return (values - values.rolling(window, min_periods=10).mean()) / values.rolling(window, min_periods=10).std()


def _signed_regime(value: float | None, pctile: float | None, prefix: str) -> str | None:
    if value is None or pd.isna(value):
        return None
    p = 0.5 if pctile is None or pd.isna(pctile) else float(pctile)
    if p <= 0.1:
        return f"{prefix}_very_negative"
    if p <= 0.3:
        return f"{prefix}_negative"
    if p < 0.7:
        return f"{prefix}_neutral"
    if p < 0.9:
        return f"{prefix}_positive"
    return f"{prefix}_very_positive"


def _high_regime(value: float | None, pctile: float | None, prefix: str) -> str | None:
    if value is None or pd.isna(value):
        return None
    p = 0.5 if pctile is None or pd.isna(pctile) else float(pctile)
    if p < 0.3:
        return f"{prefix}_low"
    if p < 0.7:
        return f"{prefix}_mid"
    if p < 0.9:
        return f"{prefix}_high"
    return f"{prefix}_extreme"


def _extreme_score(pctile: pd.Series) -> pd.Series:
    return (pd.to_numeric(pctile, errors="coerce") - 0.5).abs().mul(2.0).clip(0.0, 1.0)


def _renormalized_csi(row: pd.Series) -> tuple[float, str, str, str]:
    components: dict[str, float] = {}
    weights: dict[str, float] = {}
    for name, weight in (
        ("entry_state_vol_pctile", 0.20),
        ("entry_state_tr_over_atr_pctile", 0.20),
        ("entry_state_funding_extreme_score", 0.20),
        ("entry_state_oi_accel_pctile", 0.20),
        ("entry_state_basis_extreme_score", 0.10),
        ("entry_state_spread_proxy_pctile", -0.10),
    ):
        value = row.get(name)
        if value is None or pd.isna(value):
            continue
        short = name.removeprefix("entry_state_")
        components[short] = float(value)
        weights[short] = float(weight)
    if "funding_extreme_score" not in components and "oi_accel_pctile" not in components and "basis_extreme_score" not in components:
        components = {
            "vol_pctile": float(row.get("entry_state_vol_pctile") or 0.0),
            "tr_over_atr_pctile": float(row.get("entry_state_tr_over_atr_pctile") or 0.0),
            "spread_proxy_pctile": float(row.get("entry_state_spread_proxy_pctile") or 0.0),
        }
        weights = {"vol_pctile": 0.35, "tr_over_atr_pctile": 0.35, "spread_proxy_pctile": -0.30}
    denom = sum(abs(v) for v in weights.values()) or 1.0
    raw = sum(weights[k] * components[k] for k in components) / denom
    source = "enriched" if any(k in components for k in ("funding_extreme_score", "oi_accel_pctile", "basis_extreme_score")) else "ohlcv_proxy"
    return (
        float(min(1.0, max(0.0, raw))),
        source,
        json.dumps(components, sort_keys=True, separators=(",", ":")),
        json.dumps({k: True for k in components}, sort_keys=True, separators=(",", ":")),
    )


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
    ts = pd.to_datetime(out["ts"], utc=True, errors="coerce")
    out["entry_state_time_of_day_bucket"] = ts.dt.hour.apply(lambda h: f"h{int(h):02d}" if pd.notna(h) else None)
    out["entry_state_day_of_week"] = ts.dt.day_name()
    out["entry_state_asset_bucket"] = symbol[:3] if symbol else None
    out["entry_state_dataset_bucket"] = dataset_id
    mark = _first_numeric(df, ("mark_close", "mark_price", "mark"))
    index = _first_numeric(df, ("index_close", "index_price", "index"))
    funding = _first_numeric(df, ("funding_rate", "funding", "funding_raw", "funding_rate_realized"))
    oi = _first_numeric(df, ("open_interest", "oi", "oi_value", "oi_contracts", "oi_usd"))
    premium = _first_numeric(df, ("premium_mark_vs_index", "premium", "premium_pct"))
    basis = _first_numeric(df, ("basis_close_vs_index", "basis", "basis_pct", "mark_index_basis", "mark_index_basis_pct"))
    if mark is not None:
        out["entry_state_mark_price"] = mark
        out["entry_state_mark_close"] = mark
    if index is not None:
        out["entry_state_index_price"] = index
        out["entry_state_index_close"] = index
    if funding is not None:
        out["entry_state_funding_raw"] = funding
        out["entry_state_funding_rate"] = funding
        out["entry_state_funding_pctile"] = _rolling_percentile(funding, percentile_window)
        out["entry_state_funding_z"] = _zscore(funding)
        out["entry_state_funding_regime"] = [
            _signed_regime(v, p, "funding")
            for v, p in zip(funding, out["entry_state_funding_pctile"])
        ]
        out["entry_state_funding_extreme_score"] = _extreme_score(out["entry_state_funding_pctile"])
    for source, target in (("funding_source_ts", "entry_state_funding_source_ts"), ("funding_available_at", "entry_state_funding_available_at"), ("oi_source_ts", "entry_state_oi_source_ts"), ("oi_available_at", "entry_state_oi_available_at")):
        if source in df.columns:
            out[target] = df[source]
    if oi is not None:
        oi_change = _first_numeric(df, ("oi_change_1", "oi_change", "open_interest_change"))
        if oi_change is None:
            oi_change = oi.diff()
        oi_change_pct = _first_numeric(df, ("oi_change_pct_1", "oi_change_pct", "open_interest_change_pct"))
        if oi_change_pct is None:
            oi_change_pct = oi_change / oi.shift(1).replace(0, np.nan)
        oi_accel = oi_change_pct.diff()
        out["entry_state_open_interest"] = oi
        out["entry_state_oi_level"] = oi
        out["entry_state_oi_change"] = oi_change
        out["entry_state_oi_change_1"] = oi_change
        out["entry_state_oi_change_pct"] = oi_change_pct
        out["entry_state_oi_change_pct_1"] = oi_change_pct
        out["entry_state_oi_accel"] = oi_accel
        out["entry_state_oi_accel_pctile"] = _rolling_percentile(oi_accel, percentile_window)
        out["entry_state_oi_z"] = _zscore(oi)
        oi_pct = _rolling_percentile(oi, percentile_window)
        out["entry_state_oi_regime"] = [_high_regime(v, p, "oi") for v, p in zip(oi, oi_pct)]
    if premium is not None:
        out["entry_state_premium_mark_vs_index"] = premium
        out["entry_state_premium_raw"] = premium
        out["entry_state_premium_pctile"] = _rolling_percentile(premium, percentile_window)
    if basis is not None:
        out["entry_state_basis_close_vs_index"] = basis
        out["entry_state_basis_raw"] = basis
        out["entry_state_basis_pct"] = basis
        out["entry_state_basis_pctile"] = _rolling_percentile(basis, percentile_window)
        out["entry_state_basis_regime"] = [
            _signed_regime(v, p, "basis")
            for v, p in zip(basis, out["entry_state_basis_pctile"])
        ]
        out["entry_state_basis_extreme_score"] = _extreme_score(out["entry_state_basis_pctile"])
    if "entry_state_funding_extreme_score" in out or "entry_state_oi_accel_pctile" in out:
        out["entry_state_crowding_proxy"] = pd.concat(
            [
                out.get("entry_state_funding_raw", pd.Series(index=out.index, dtype="float64")).abs(),
                out.get("entry_state_oi_change_pct", pd.Series(index=out.index, dtype="float64")).abs(),
            ],
            axis=1,
        ).max(axis=1)
        out["entry_state_crowding_proxy_pctile"] = _rolling_percentile(out["entry_state_crowding_proxy"], percentile_window)
    csi_rows = out.apply(_renormalized_csi, axis=1, result_type="expand")
    out["entry_state_csi_raw"] = csi_rows[0]
    out["entry_state_csi_source"] = csi_rows[1]
    out["entry_state_csi_components_json"] = csi_rows[2]
    out["entry_state_csi_components_available_json"] = csi_rows[3]
    out["entry_state_csi_pctile"] = _rolling_percentile(out["entry_state_csi_raw"], percentile_window)
    out["entry_state_csi_bucket"] = out["entry_state_csi_raw"].apply(
        lambda v: _bucket(v, [(0.0, 0.5, "csi_low"), (0.5, 0.7, "csi_mid"), (0.7, 0.85, "csi_high"), (0.85, 1.01, "csi_extreme")])
    )
    out["entry_state_constraint_stress_proxy"] = out["entry_state_csi_raw"]
    out["entry_state_constraint_stress_pctile"] = out["entry_state_csi_pctile"]
    if "entry_state_liq_buy_notional" in out and "entry_state_liq_sell_notional" in out:
        total_liq = pd.to_numeric(out["entry_state_liq_buy_notional"], errors="coerce").fillna(0.0) + pd.to_numeric(out["entry_state_liq_sell_notional"], errors="coerce").fillna(0.0)
        out["entry_state_liq_imbalance"] = (
            (pd.to_numeric(out["entry_state_liq_buy_notional"], errors="coerce").fillna(0.0) - pd.to_numeric(out["entry_state_liq_sell_notional"], errors="coerce").fillna(0.0))
            / total_liq.replace(0, np.nan)
        )

    out["entry_state_trend_ready"] = out["entry_state_ema_slow"].notna()
    out["entry_state_vol_ready"] = out["entry_state_vol_pctile"].notna()
    out["entry_state_liquidity_ready"] = out["entry_state_spread_proxy_pctile"].notna()
    out["entry_state_csi_ready"] = out["entry_state_csi_raw"].notna()
    out["entry_state_htf_ready"] = False
    return out
