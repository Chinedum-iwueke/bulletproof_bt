"""Online causal state feature layer for all strategies."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from math import sqrt
from typing import Any

import pandas as pd


def _pctile(window: deque[float], value: float | None) -> float | None:
    if value is None or len(window) < 5:
        return None
    vals = [v for v in window if v is not None]
    if len(vals) < 5:
        return None
    return sum(1 for v in vals if v <= value) / len(vals)


@dataclass
class _SymbolState:
    closes: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    highs: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    lows: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    volumes: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    trs: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    atr_pcts: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    realized_vols: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    spreads: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    tr_over_atrs: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    csi_vals: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    ema_fast: float | None = None
    ema_slow: float | None = None
    ts: pd.Timestamp | None = None


class OnlineStateFeatureLayer:
    def __init__(self, *, percentile_window: int = 200, ema_fast_span: int = 20, ema_slow_span: int = 50) -> None:
        self._states: dict[str, _SymbolState] = {}
        self._percentile_window = percentile_window
        self._a_fast = 2 / (ema_fast_span + 1)
        self._a_slow = 2 / (ema_slow_span + 1)

    def update(self, *, symbol: str, ts: pd.Timestamp, open_px: float, high: float, low: float, close: float, volume: float) -> None:
        st = self._states.setdefault(symbol, _SymbolState())
        prev_close = st.closes[-1] if st.closes else close
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))

        st.ts = ts
        st.closes.append(float(close))
        st.highs.append(float(high))
        st.lows.append(float(low))
        st.volumes.append(float(volume))
        st.trs.append(float(tr))
        spread_proxy = (high - low) / close if close else 0.0
        st.spreads.append(float(spread_proxy))

        st.ema_fast = close if st.ema_fast is None else (self._a_fast * close + (1 - self._a_fast) * st.ema_fast)
        st.ema_slow = close if st.ema_slow is None else (self._a_slow * close + (1 - self._a_slow) * st.ema_slow)

        atr = sum(list(st.trs)[-14:]) / min(len(st.trs), 14)
        atr_pct = (atr / close) if close else 0.0
        st.atr_pcts.append(float(atr_pct))
        tr_over_atr = (tr / atr) if atr > 0 else None
        st.tr_over_atrs.append(float(tr_over_atr) if tr_over_atr is not None else 0.0)

        rets = []
        closes = list(st.closes)
        for i in range(max(1, len(closes) - 20), len(closes)):
            prev = closes[i - 1]
            rets.append((closes[i] - prev) / prev if prev else 0.0)
        rv = (sum(r * r for r in rets) / len(rets)) ** 0.5 if rets else 0.0
        st.realized_vols.append(float(rv))

        vol_pct = _pctile(deque(list(st.realized_vols)[-self._percentile_window:], maxlen=self._percentile_window), rv) or 0.0
        tr_pct = _pctile(deque(list(st.tr_over_atrs)[-self._percentile_window:], maxlen=self._percentile_window), tr_over_atr or 0.0) or 0.0
        spread_pct = _pctile(deque(list(st.spreads)[-self._percentile_window:], maxlen=self._percentile_window), spread_proxy) or 0.0
        csi = min(1.0, max(0.0, 0.35 * vol_pct + 0.35 * tr_pct - 0.30 * spread_pct))
        st.csi_vals.append(csi)

    def snapshot(self, *, symbol: str) -> dict[str, Any]:
        st = self._states.get(symbol)
        if st is None or st.ts is None:
            return {
                "entry_state_trend_ready": False,
                "entry_state_vol_ready": False,
                "entry_state_liquidity_ready": False,
                "entry_state_csi_ready": False,
                "entry_state_htf_ready": False,
            }
        close = st.closes[-1]
        atr = sum(list(st.trs)[-14:]) / min(len(st.trs), 14)
        atr_pct = st.atr_pcts[-1] if st.atr_pcts else None
        tr_over_atr = st.tr_over_atrs[-1] if st.tr_over_atrs else None
        vol_pctile = _pctile(deque(list(st.realized_vols)[-self._percentile_window:], maxlen=self._percentile_window), st.realized_vols[-1])
        spread_pctile = _pctile(deque(list(st.spreads)[-self._percentile_window:], maxlen=self._percentile_window), st.spreads[-1])
        tr_pctile = _pctile(deque(list(st.tr_over_atrs)[-self._percentile_window:], maxlen=self._percentile_window), tr_over_atr)
        vol_pctile = vol_pctile if vol_pctile is not None else 0.0
        spread_pctile = spread_pctile if spread_pctile is not None else 0.0
        tr_pctile = tr_pctile if tr_pctile is not None else 0.0
        csi = min(1.0, max(0.0, 0.35 * vol_pctile + 0.35 * tr_pctile - 0.30 * spread_pctile))
        csi_pctile = _pctile(deque(list(st.csi_vals)[-self._percentile_window:], maxlen=self._percentile_window), csi)

        return {
            "entry_state_ts": st.ts,
            "entry_state_symbol": symbol,
            "entry_state_ema_fast": st.ema_fast,
            "entry_state_ema_slow": st.ema_slow,
            "entry_state_ema_relationship": "fast_above" if (st.ema_fast is not None and st.ema_slow is not None and st.ema_fast >= st.ema_slow) else "fast_below",
            "entry_state_ema_separation": ((st.ema_fast - st.ema_slow) / close) if (st.ema_fast is not None and st.ema_slow is not None and close) else None,
            "entry_state_atr": atr,
            "entry_state_atr_pct": atr_pct,
            "entry_state_atr_pct_pctile": _pctile(deque(list(st.atr_pcts)[-self._percentile_window:], maxlen=self._percentile_window), atr_pct or 0.0),
            "entry_state_true_range": st.trs[-1] if st.trs else None,
            "entry_state_tr_over_atr": tr_over_atr,
            "entry_state_tr_over_atr_pctile": tr_pctile,
            "entry_state_volume": st.volumes[-1] if st.volumes else None,
            "entry_state_dollar_volume": (st.volumes[-1] * close) if st.volumes else None,
            "entry_state_volume_pctile": _pctile(deque(list(st.volumes)[-self._percentile_window:], maxlen=self._percentile_window), st.volumes[-1] if st.volumes else None),
            "entry_state_spread_proxy": st.spreads[-1] if st.spreads else None,
            "entry_state_spread_proxy_pctile": spread_pctile,
            "entry_state_csi_raw": csi,
            "entry_state_csi_pctile": csi_pctile,
            "entry_state_csi_bucket": "csi_extreme" if csi >= 0.85 else ("csi_high" if csi >= 0.7 else ("csi_mid" if csi >= 0.5 else "csi_low")),
            "entry_state_vol_pctile": vol_pctile,
            "entry_state_vol_regime": "vol_extreme" if vol_pctile >= 0.9 else ("vol_high" if vol_pctile >= 0.7 else ("vol_mid" if vol_pctile >= 0.3 else "vol_low")),
            "entry_state_liquidity_regime": "broken" if spread_pctile >= 0.95 else ("fragile" if spread_pctile >= 0.8 else ("moderate" if spread_pctile >= 0.6 else "liquid")),
            "entry_state_displacement_regime": "extreme_impulse" if (tr_over_atr or 0) >= 2 else ("strong_impulse" if (tr_over_atr or 0) >= 1.5 else ("mild_impulse" if (tr_over_atr or 0) >= 1.0 else "no_impulse")),
            "entry_state_time_of_day_bucket": f"h{int(st.ts.hour):02d}",
            "entry_state_day_of_week": st.ts.day_name(),
            "entry_state_trend_ready": len(st.closes) >= 10,
            "entry_state_vol_ready": len(st.realized_vols) >= 10,
            "entry_state_liquidity_ready": len(st.spreads) >= 5,
            "entry_state_csi_ready": len(st.csi_vals) >= 5,
            "entry_state_htf_ready": False,
        }
