"""Compiled causal feature kernel for the L7-H1 CSI displacement family.

This module deliberately compiles only the strategy-family feature calculation.
It does not replace the classic event engine, risk engine, execution model, or
artifact writers. The classic engine consumes these columns through
``bar.extra`` and remains the source of truth for orders, fills, logs, and PnL.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
from collections import deque
from typing import Any

import numpy as np
import pandas as pd

from bt.data.resample import normalize_timeframe
from bt.engine.fast_path.numba_kernels import njit


@dataclass(frozen=True)
class L7H1KernelParams:
    signal_timeframe: str = "15m"
    atr_period: int = 14
    basis_lookback_days: int = 30
    d0: float = 1.8


def prefix_for_timeframe(signal_timeframe: str) -> str:
    normalized = normalize_timeframe(str(signal_timeframe), key_path="signal_timeframe")
    return f"l7h1_{normalized.replace('m', 'm').replace('h', 'h')}_"


def build_l7_h1_feature_frame(panel: pd.DataFrame, *, params: L7H1KernelParams) -> pd.DataFrame:
    """Return causal L7-H1 feature columns keyed by 1m decision timestamp.

    The feature at decision timestamp ``t`` is based on:
    - 1m aux data available through and including ``t``;
    - the latest fully closed HTF candle ending at ``t``;
    - ATR/displacement values from prior closed HTF candles.
    """

    required = {"ts", "symbol", "open", "high", "low", "close", "volume"}
    missing = required - set(panel.columns)
    if missing:
        raise ValueError(f"L7-H1 kernel input missing required columns: {sorted(missing)}")
    tf = normalize_timeframe(params.signal_timeframe, key_path="signal_timeframe")
    prefix = prefix_for_timeframe(tf)
    frames: list[pd.DataFrame] = []
    base = panel.copy()
    base["ts"] = pd.to_datetime(base["ts"], utc=True)
    base = base.sort_values(["symbol", "ts"], kind="mergesort").reset_index(drop=True)
    for symbol, group in base.groupby("symbol", sort=False):
        features = _build_symbol_features(group.reset_index(drop=True), symbol=str(symbol), params=params, prefix=prefix)
        if not features.empty:
            frames.append(features)
    if not frames:
        return pd.DataFrame(columns=["ts", "symbol"])
    out = pd.concat(frames, ignore_index=True)
    return out.sort_values(["symbol", "ts"], kind="mergesort").reset_index(drop=True)


def _build_symbol_features(group: pd.DataFrame, *, symbol: str, params: L7H1KernelParams, prefix: str) -> pd.DataFrame:
    tf = normalize_timeframe(params.signal_timeframe, key_path="signal_timeframe")
    minutes = _timeframe_minutes(tf)
    work = group.copy()
    work["bucket_start"] = work["ts"].dt.floor("h" if tf == "1h" else f"{minutes}min")
    agg = work.groupby("bucket_start", sort=True).agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        n_bars=("close", "size"),
    )
    agg = agg[agg["n_bars"].eq(minutes)].reset_index()
    if agg.empty:
        return pd.DataFrame(columns=["ts", "symbol"])
    agg["emit_ts"] = agg["bucket_start"] + pd.to_timedelta(minutes, unit="m")
    available_decision_ts = set(pd.to_datetime(work["ts"], utc=True))
    agg = agg[agg["emit_ts"].isin(available_decision_ts)].reset_index(drop=True)
    if agg.empty:
        return pd.DataFrame(columns=["ts", "symbol"])
    exact = _exact_classic_symbol_features(work, agg, params=params)
    out = pd.DataFrame(
        {
            "ts": agg["emit_ts"],
            "symbol": symbol,
            f"{prefix}compiled_feature_ready": True,
            f"{prefix}ATR_14": exact["atr_prev"],
            f"{prefix}D_t": exact["d_t"],
            f"{prefix}CSI": exact["csi"],
            f"{prefix}CSI_raw": exact["csi_raw"],
            f"{prefix}funding_pct": exact["funding_pct"],
            f"{prefix}basis_pct": exact["basis_pct"],
            f"{prefix}oi_z": exact["oi_z"],
            f"{prefix}volume_z": exact["volume_z"],
            f"{prefix}S_t": exact["spread_raw"],
            f"{prefix}spread_rank_desc": exact["spread_component"],
            f"{prefix}csi_component_funding": exact["comp_funding"],
            f"{prefix}csi_component_oi": exact["comp_oi"],
            f"{prefix}csi_component_displacement": exact["comp_d"],
            f"{prefix}csi_component_spread": exact["comp_s"],
            f"{prefix}side_code": exact["side_code"],
        }
    )
    return out


def _finite_or_nan(value: Any) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return out if math.isfinite(out) else float("nan")


def _window_values(values: deque[float]) -> list[float]:
    return [float(v) for v in values if math.isfinite(float(v))]


def _classic_pctile(values: deque[float], value: float | None, *, min_count: int = 5) -> float:
    if value is None or not math.isfinite(float(value)):
        return float("nan")
    vals = _window_values(values)
    if len(vals) < min_count:
        return float("nan")
    return sum(1 for v in vals if v <= float(value)) / len(vals)


def _classic_z(values: deque[float], value: float | None, *, min_count: int = 5) -> float:
    if value is None or not math.isfinite(float(value)):
        return float("nan")
    vals = _window_values(values)
    if len(vals) < min_count:
        return float("nan")
    mean = sum(vals) / len(vals)
    var = sum((v - mean) ** 2 for v in vals) / len(vals)
    std = var ** 0.5
    if std <= 0:
        return float("nan")
    return (float(value) - mean) / std


def _exact_classic_symbol_features(work: pd.DataFrame, agg: pd.DataFrame, *, params: L7H1KernelParams) -> dict[str, list[float]]:
    """Emulate the classic L7-H1 strategy's feature state exactly enough for parity.

    The strategy updates funding/OI/basis/volume/spread/close state every 1m,
    then samples that state when a fully closed HTF candle is emitted. The
    previous compiled kernel sampled those aux series only at HTF frequency,
    which was faster but changed gates and exits. This routine keeps the
    compiled columns causal while matching the classic 1m state cadence.
    """
    aux_window = max(5, int(params.basis_lookback_days) * 24 * 60)
    funding_values: deque[float] = deque(maxlen=aux_window)
    basis_values: deque[float] = deque(maxlen=aux_window)
    oi_values: deque[float] = deque(maxlen=240)
    oi_returns: deque[float] = deque(maxlen=240)
    oi_return_zs: deque[float] = deque(maxlen=240)
    volume_values: deque[float] = deque(maxlen=240)
    volume_returns: deque[float] = deque(maxlen=240)
    spread_values: deque[float] = deque(maxlen=240)
    csi_raw_values: deque[float] = deque(maxlen=240)
    close_values: deque[float] = deque(maxlen=240)

    prev_signal_close: float | None = None
    atr_prev_close: float | None = None
    atr_seed: deque[float] = deque(maxlen=int(params.atr_period))
    atr_value: float | None = None

    funding_series = _optional_numeric(work, ("funding_rate", "funding", "funding_raw", "funding_rate_realized")).to_numpy()
    basis_series = _optional_numeric(work, ("basis_close_vs_index", "basis", "basis_pct", "mark_index_basis", "mark_index_basis_pct"))
    if basis_series.isna().all() and {"mark_close", "index_close"} <= set(work.columns):
        mark = pd.to_numeric(work["mark_close"], errors="coerce")
        index = pd.to_numeric(work["index_close"], errors="coerce")
        basis_series = (mark - index) / index.replace(0.0, np.nan)
    basis_values_1m = basis_series.to_numpy()
    oi_series = _optional_numeric(work, ("open_interest", "oi", "oi_value", "oi_contracts", "oi_usd")).to_numpy()
    ts_values = pd.to_datetime(work["ts"], utc=True).to_numpy()
    open_values = pd.to_numeric(work["open"], errors="coerce").to_numpy()
    high_values = pd.to_numeric(work["high"], errors="coerce").to_numpy()
    low_values = pd.to_numeric(work["low"], errors="coerce").to_numpy()
    close_values_1m = pd.to_numeric(work["close"], errors="coerce").to_numpy()
    volume_series = pd.to_numeric(work["volume"], errors="coerce").to_numpy()

    htf_by_emit: dict[pd.Timestamp, tuple[float, float, float, float]] = {}
    for row in agg.itertuples(index=False):
        htf_by_emit[pd.Timestamp(row.emit_ts)] = (float(row.open), float(row.high), float(row.low), float(row.close))
    emit_order = [pd.Timestamp(ts) for ts in agg["emit_ts"]]
    out = {key: [] for key in ("atr_prev", "d_t", "csi", "csi_raw", "funding_pct", "basis_pct", "oi_z", "volume_z", "spread_raw", "spread_component", "comp_funding", "comp_oi", "comp_d", "comp_s", "side_code")}

    emit_idx = 0
    for idx in range(len(work)):
        ts = pd.Timestamp(ts_values[idx]).tz_convert("UTC")
        funding = _finite_or_nan(funding_series[idx])
        if math.isfinite(funding):
            funding_values.append(funding)
        basis = _finite_or_nan(basis_values_1m[idx])
        if math.isfinite(basis):
            basis_values.append(basis)
        oi = _finite_or_nan(oi_series[idx])
        if math.isfinite(oi):
            prev_oi = oi_values[-1] if oi_values else None
            oi_values.append(oi)
            if prev_oi not in (None, 0):
                oi_ret = (oi - float(prev_oi)) / float(prev_oi)
                oi_z = _classic_z(oi_returns, oi_ret)
                oi_returns.append(oi_ret)
                if math.isfinite(oi_z):
                    oi_return_zs.append(oi_z)

        volume = _finite_or_nan(volume_series[idx])
        prev_vol = volume_values[-1] if volume_values else None
        if math.isfinite(volume):
            volume_values.append(volume)
            if prev_vol not in (None, 0):
                volume_returns.append((volume - float(prev_vol)) / float(prev_vol))

        high = _finite_or_nan(high_values[idx])
        low = _finite_or_nan(low_values[idx])
        close = _finite_or_nan(close_values_1m[idx])
        spread = 0.5 * (high - low) / close if math.isfinite(close) and close else 0.0
        spread_values.append(float(spread))
        if math.isfinite(close):
            close_values.append(close)

        while emit_idx < len(emit_order) and emit_order[emit_idx] == ts:
            htf_open, htf_high, htf_low, htf_close = htf_by_emit[ts]
            prev_atr = atr_value
            tr = None
            if prev_signal_close is not None:
                tr = max(htf_high - htf_low, abs(htf_high - prev_signal_close), abs(htf_low - prev_signal_close))
            if atr_prev_close is None:
                atr_prev_close = htf_close
            else:
                tr_for_atr = max(htf_high - htf_low, abs(htf_high - atr_prev_close), abs(htf_low - atr_prev_close))
                atr_prev_close = htf_close
                if atr_value is None:
                    atr_seed.append(float(tr_for_atr))
                    if len(atr_seed) == int(params.atr_period):
                        atr_value = sum(atr_seed) / int(params.atr_period)
                else:
                    atr_value = ((atr_value * (int(params.atr_period) - 1)) + tr_for_atr) / int(params.atr_period)
            d_t = (float(tr) / float(prev_atr)) if tr is not None and prev_atr is not None and prev_atr > 0 else float("nan")

            funding_latest = funding_values[-1] if funding_values else None
            funding_pct = _classic_pctile(funding_values, funding_latest)
            basis_latest = basis_values[-1] if basis_values else None
            basis_pct = _classic_pctile(basis_values, basis_latest)
            funding_component = funding_pct if math.isfinite(funding_pct) else basis_pct
            if not math.isfinite(funding_component):
                funding_component = 0.5

            oi_z = oi_return_zs[-1] if oi_return_zs else float("nan")
            volume_z = _classic_z(volume_returns, volume_returns[-1] if volume_returns else None)
            oi_component = _norm_z(float(oi_z) if math.isfinite(float(oi_z)) else volume_z)

            signal_spread = 0.5 * (htf_high - htf_low) / htf_close if htf_close else 0.0
            spread_pct = _classic_pctile(spread_values, signal_spread)
            spread_component = 1.0 - float(spread_pct) if math.isfinite(spread_pct) else 0.5
            comp_d = _norm_displacement(d_t, float(params.d0))
            raw_csi = 0.35 * funding_component + 0.25 * oi_component + 0.30 * comp_d + 0.10 * spread_component
            csi_raw_values.append(float(raw_csi))
            csi_min = min(csi_raw_values)
            csi_max = max(csi_raw_values)
            csi = (raw_csi - csi_min) / (csi_max - csi_min) if csi_max > csi_min and len(csi_raw_values) >= 5 else raw_csi
            csi = min(1.0, max(0.0, float(csi)))
            side_code = 1 if prev_signal_close is not None and htf_close - prev_signal_close > 0 else -1
            prev_signal_close = htf_close

            values = {
                "atr_prev": prev_atr if prev_atr is not None else float("nan"),
                "d_t": d_t,
                "csi": csi,
                "csi_raw": raw_csi,
                "funding_pct": funding_pct,
                "basis_pct": basis_pct,
                "oi_z": oi_z,
                "volume_z": volume_z,
                "spread_raw": signal_spread,
                "spread_component": spread_component,
                "comp_funding": funding_component,
                "comp_oi": oi_component,
                "comp_d": comp_d,
                "comp_s": spread_component,
                "side_code": side_code,
            }
            for key, value in values.items():
                out[key].append(value)
            emit_idx += 1
    return out


def _aux_at_emit(work: pd.DataFrame, emit_ts: pd.Series) -> pd.DataFrame:
    aux = work[["ts", "volume", "high", "low", "close"]].copy()
    aux["funding"] = _optional_numeric(work, ("funding_rate", "funding", "funding_raw", "funding_rate_realized"))
    basis = _optional_numeric(work, ("basis_close_vs_index", "basis", "basis_pct", "mark_index_basis", "mark_index_basis_pct"))
    if basis.isna().all() and {"mark_close", "index_close"} <= set(work.columns):
        mark = pd.to_numeric(work["mark_close"], errors="coerce")
        index = pd.to_numeric(work["index_close"], errors="coerce")
        basis = (mark - index) / index.replace(0.0, np.nan)
    aux["basis"] = basis
    aux["oi"] = _optional_numeric(work, ("open_interest", "oi", "oi_value", "oi_contracts", "oi_usd"))
    aux["spread"] = 0.5 * (pd.to_numeric(aux["high"], errors="coerce") - pd.to_numeric(aux["low"], errors="coerce")) / pd.to_numeric(
        aux["close"], errors="coerce"
    ).replace(0.0, np.nan)
    targets = pd.DataFrame({"ts": pd.to_datetime(emit_ts, utc=True)})
    merged = pd.merge_asof(
        targets.sort_values("ts"),
        aux.sort_values("ts"),
        on="ts",
        direction="backward",
        allow_exact_matches=True,
    )
    return merged[["funding", "basis", "oi", "volume", "spread"]]


def _optional_numeric(frame: pd.DataFrame, names: tuple[str, ...]) -> pd.Series:
    for name in names:
        if name in frame.columns:
            return pd.to_numeric(frame[name], errors="coerce")
    return pd.Series(np.nan, index=frame.index, dtype="float64")


def _timeframe_minutes(tf: str) -> int:
    if tf.endswith("m"):
        return int(tf[:-1])
    if tf.endswith("h"):
        return int(tf[:-1]) * 60
    if tf.endswith("d"):
        return int(tf[:-1]) * 1440
    raise ValueError(f"unsupported L7-H1 signal timeframe: {tf}")


@njit
def _rolling_pct(values, idx, window, min_count):
    value = values[idx]
    if not math.isfinite(value):
        return np.nan
    start = idx - window + 1
    if start < 0:
        start = 0
    count = 0
    le_count = 0
    for j in range(start, idx + 1):
        v = values[j]
        if math.isfinite(v):
            count += 1
            if v <= value:
                le_count += 1
    if count < min_count:
        return np.nan
    return le_count / count


@njit
def _rolling_z(values, idx, window, min_count):
    value = values[idx]
    if not math.isfinite(value):
        return np.nan
    start = idx - window + 1
    if start < 0:
        start = 0
    count = 0
    total = 0.0
    for j in range(start, idx + 1):
        v = values[j]
        if math.isfinite(v):
            count += 1
            total += v
    if count < min_count:
        return np.nan
    mean = total / count
    var = 0.0
    for j in range(start, idx + 1):
        v = values[j]
        if math.isfinite(v):
            diff = v - mean
            var += diff * diff
    std = math.sqrt(var / count)
    if std <= 0.0:
        return np.nan
    return (value - mean) / std


@njit
def _norm_z(value):
    if not math.isfinite(value):
        return 0.5
    out = (value + 3.0) / 6.0
    if out < 0.0:
        return 0.0
    if out > 1.0:
        return 1.0
    return out


@njit
def _norm_displacement(value, threshold):
    if not math.isfinite(value):
        return 0.0
    denom = max(threshold * 2.0, 1e-12)
    out = value / denom
    if out < 0.0:
        return 0.0
    if out > 1.0:
        return 1.0
    return out


@njit
def _compute_l7_h1_arrays(
    htf_open,
    htf_high,
    htf_low,
    htf_close,
    htf_volume,
    funding,
    basis,
    oi,
    one_min_volume,
    one_min_spread,
    atr_period,
    aux_window,
    d0_scaled,
):  # pragma: no cover - exercised through public builder
    n = htf_close.shape[0]
    d0 = d0_scaled / 1_000_000.0
    atr_prev = np.full(n, np.nan)
    d_t = np.full(n, np.nan)
    csi = np.full(n, np.nan)
    csi_raw = np.full(n, np.nan)
    funding_pct = np.full(n, np.nan)
    basis_pct = np.full(n, np.nan)
    oi_z = np.full(n, np.nan)
    volume_z = np.full(n, np.nan)
    spread_raw = np.full(n, np.nan)
    spread_component = np.full(n, np.nan)
    comp_funding = np.full(n, np.nan)
    comp_oi = np.full(n, np.nan)
    comp_d = np.full(n, np.nan)
    comp_s = np.full(n, np.nan)
    side_code = np.zeros(n, dtype=np.int64)
    tr = np.full(n, np.nan)
    oi_ret = np.full(n, np.nan)
    oi_ret_z = np.full(n, np.nan)
    vol_ret = np.full(n, np.nan)
    raw_csi_values = np.full(n, np.nan)

    prev_close = np.nan
    atr_value = np.nan
    tr_count = 0
    for i in range(n):
        atr_prev[i] = atr_value
        if math.isfinite(prev_close):
            tr_i = max(htf_high[i] - htf_low[i], abs(htf_high[i] - prev_close), abs(htf_low[i] - prev_close))
            tr[i] = tr_i
            tr_count += 1
            if not math.isfinite(atr_value):
                if tr_count >= atr_period:
                    total = 0.0
                    count = 0
                    j = i
                    while j >= 0 and count < atr_period:
                        if math.isfinite(tr[j]):
                            total += tr[j]
                            count += 1
                        j -= 1
                    if count == atr_period:
                        atr_value = total / atr_period
            else:
                atr_value = (atr_value * (atr_period - 1) + tr_i) / atr_period
            if math.isfinite(atr_prev[i]) and atr_prev[i] > 0.0:
                d_t[i] = tr_i / atr_prev[i]
        prev_signal_close = prev_close
        prev_close = htf_close[i]

        if i > 0 and math.isfinite(oi[i]) and math.isfinite(oi[i - 1]) and oi[i - 1] != 0.0:
            oi_ret[i] = (oi[i] - oi[i - 1]) / oi[i - 1]
            oi_ret_z[i] = _rolling_z(oi_ret, i, 240, 5)
        if i > 0 and math.isfinite(one_min_volume[i]) and math.isfinite(one_min_volume[i - 1]) and one_min_volume[i - 1] != 0.0:
            vol_ret[i] = (one_min_volume[i] - one_min_volume[i - 1]) / one_min_volume[i - 1]
        volume_z[i] = _rolling_z(vol_ret, i, 240, 5)
        funding_pct[i] = _rolling_pct(funding, i, aux_window, 5)
        basis_pct[i] = _rolling_pct(basis, i, aux_window, 5)
        spread_raw[i] = one_min_spread[i]
        spread_pct = _rolling_pct(one_min_spread, i, 240, 5)
        spread_component[i] = 1.0 - spread_pct if math.isfinite(spread_pct) else 0.5
        funding_component = funding_pct[i] if math.isfinite(funding_pct[i]) else basis_pct[i]
        if not math.isfinite(funding_component):
            funding_component = 0.5
        oi_component = _norm_z(oi_ret_z[i] if math.isfinite(oi_ret_z[i]) else volume_z[i])
        displacement_component = _norm_displacement(d_t[i], d0)
        raw = 0.35 * funding_component + 0.25 * oi_component + 0.30 * displacement_component + 0.10 * spread_component[i]
        raw_csi_values[i] = raw
        csi_raw[i] = raw
        raw_min = raw
        raw_max = raw
        raw_count = 0
        start = i - 239
        if start < 0:
            start = 0
        for j in range(start, i + 1):
            v = raw_csi_values[j]
            if math.isfinite(v):
                raw_count += 1
                if v < raw_min:
                    raw_min = v
                if v > raw_max:
                    raw_max = v
        if raw_count >= 5 and raw_max > raw_min:
            csi[i] = (raw - raw_min) / (raw_max - raw_min)
        else:
            csi[i] = raw
        if csi[i] < 0.0:
            csi[i] = 0.0
        if csi[i] > 1.0:
            csi[i] = 1.0
        comp_funding[i] = funding_component
        comp_oi[i] = oi_component
        comp_d[i] = displacement_component
        comp_s[i] = spread_component[i]
        if math.isfinite(prev_signal_close):
            side_code[i] = 1 if htf_close[i] - prev_signal_close > 0.0 else -1
    return (
        atr_prev,
        d_t,
        csi,
        csi_raw,
        funding_pct,
        basis_pct,
        oi_ret_z,
        volume_z,
        spread_raw,
        spread_component,
        comp_funding,
        comp_oi,
        comp_d,
        comp_s,
        side_code,
    )


__all__ = ["L7H1KernelParams", "build_l7_h1_feature_frame", "prefix_for_timeframe"]
