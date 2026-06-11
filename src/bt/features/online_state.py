"""Online causal state feature layer for all strategies."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import json
from typing import Any

import pandas as pd


def _pctile(window: deque[float], value: float | None) -> float | None:
    if value is None or len(window) < 5:
        return None
    vals = [v for v in window if v is not None]
    if len(vals) < 5:
        return None
    return sum(1 for v in vals if v <= value) / len(vals)


def _num(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(out):
        return None
    return out


def _first_num(mapping: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        if key in mapping:
            value = _num(mapping.get(key))
            if value is not None:
                return value
    return None


def _first_value(mapping: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value is not None and not pd.isna(value):
            return value
    return None


def _z(window: deque[float], value: float | None) -> float | None:
    if value is None or len(window) < 5:
        return None
    vals = [float(v) for v in window if v is not None]
    if len(vals) < 5:
        return None
    mean = sum(vals) / len(vals)
    var = sum((v - mean) ** 2 for v in vals) / len(vals)
    std = var ** 0.5
    return None if std <= 0 else (float(value) - mean) / std


def _pctile_extreme_score(pctile: float | None) -> float | None:
    if pctile is None:
        return None
    return min(1.0, max(0.0, abs(float(pctile) - 0.5) * 2.0))


def _json_compact(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _available(extra: dict[str, Any], ts: pd.Timestamp, keys: tuple[str, ...]) -> bool:
    for key in keys:
        value = extra.get(key)
        if value is None or pd.isna(value):
            continue
        if pd.to_datetime(value, utc=True) > ts:
            return False
    return True


def _regime_signed(value: float | None, pctile: float | None, *, prefix: str) -> str | None:
    if value is None:
        return None
    p = pctile if pctile is not None else 0.5
    if p <= 0.1:
        return f"{prefix}_very_negative"
    if p <= 0.3:
        return f"{prefix}_negative"
    if p < 0.7:
        return f"{prefix}_neutral"
    if p < 0.9:
        return f"{prefix}_positive"
    return f"{prefix}_very_positive"


def _regime_high(value: float | None, pctile: float | None, *, prefix: str) -> str | None:
    if value is None:
        return None
    p = pctile if pctile is not None else 0.5
    if p < 0.3:
        return f"{prefix}_low"
    if p < 0.7:
        return f"{prefix}_mid"
    if p < 0.9:
        return f"{prefix}_high"
    return f"{prefix}_extreme"


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
    funding_rates: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    open_interests: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    oi_changes: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    oi_accels: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    oi_change_pcts: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    premiums: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    bases: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    liq_buy_notionals: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    liq_sell_notionals: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    mark_close: float | None = None
    index_close: float | None = None
    funding_available_at: Any = None
    oi_available_at: Any = None
    mark_available_at: Any = None
    index_available_at: Any = None
    funding_source_ts: Any = None
    oi_source_ts: Any = None
    ema_fast: float | None = None
    ema_slow: float | None = None
    ts: pd.Timestamp | None = None


class OnlineStateFeatureLayer:
    def __init__(
        self,
        *,
        percentile_window: int = 200,
        ema_fast_span: int = 20,
        ema_slow_span: int = 50,
        enabled: bool = True,
        profile: str = "full",
    ) -> None:
        self._states: dict[str, _SymbolState] = {}
        self._percentile_window = percentile_window
        self._a_fast = 2 / (ema_fast_span + 1)
        self._a_slow = 2 / (ema_slow_span + 1)
        self._enabled = bool(enabled)
        normalized_profile = str(profile or "full").strip().lower()
        if normalized_profile not in {"minimal", "full"}:
            raise ValueError("state feature profile must be 'minimal' or 'full'")
        self._profile = normalized_profile
        self._precomputed: dict[str, dict[str, Any]] = {}

    def _precomputed_snapshot(
        self,
        *,
        symbol: str,
        ts: pd.Timestamp,
        extra: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if not extra:
            return None
        snapshot = {
            key: value
            for key, value in extra.items()
            if key.startswith("entry_state_") and value is not None and not pd.isna(value)
        }
        if not snapshot:
            return None
        snapshot.setdefault("entry_state_ts", ts)
        snapshot.setdefault("entry_state_symbol", symbol)
        snapshot["entry_state_precomputed"] = True
        if self._profile == "minimal":
            rich_markers = (
                "funding",
                "oi_",
                "open_interest",
                "mark_",
                "index_",
                "basis",
                "premium",
                "crowding",
                "constraint",
                "liq_",
            )
            snapshot = {
                key: value
                for key, value in snapshot.items()
                if not any(marker in key for marker in rich_markers)
                or key in {"entry_state_precomputed", "entry_state_ts", "entry_state_symbol"}
            }
        return snapshot

    def update(
        self,
        *,
        symbol: str,
        ts: pd.Timestamp,
        open_px: float,
        high: float,
        low: float,
        close: float,
        volume: float,
        extra: dict[str, Any] | None = None,
    ) -> None:
        if not self._enabled:
            return
        precomputed = self._precomputed_snapshot(symbol=symbol, ts=ts, extra=extra)
        if precomputed is not None:
            self._precomputed[symbol] = precomputed
            return
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

        if self._profile == "full":
            extra = extra or {}
            if _available(extra, ts, ("available_at", "mark_available_at")):
                st.mark_close = _first_num(extra, ("mark_close", "mark_price", "mark"))
                st.mark_available_at = _first_value(extra, ("mark_available_at", "available_at"))
            if _available(extra, ts, ("available_at", "index_available_at")):
                st.index_close = _first_num(extra, ("index_close", "index_price", "index"))
                st.index_available_at = _first_value(extra, ("index_available_at", "available_at"))
            if _available(extra, ts, ("available_at", "funding_available_at", "funding_source_ts")):
                funding = _first_num(extra, ("funding_rate", "funding", "funding_raw", "funding_rate_realized"))
                if funding is not None:
                    st.funding_rates.append(funding)
                st.funding_source_ts = _first_value(extra, ("funding_source_ts", "funding_available_at", "available_at"))
                st.funding_available_at = _first_value(extra, ("funding_available_at", "funding_source_ts", "available_at"))
            if _available(extra, ts, ("available_at", "oi_available_at", "oi_source_ts")):
                oi = _first_num(extra, ("open_interest", "oi", "oi_value", "oi_contracts", "oi_usd"))
                if oi is not None:
                    prev_oi = st.open_interests[-1] if st.open_interests else None
                    st.open_interests.append(oi)
                    oi_change = _first_num(extra, ("oi_change_1", "oi_change", "open_interest_change"))
                    if oi_change is None and prev_oi is not None:
                        oi_change = oi - prev_oi
                    if oi_change is not None:
                        prev_change_pct = st.oi_change_pcts[-1] if st.oi_change_pcts else None
                        st.oi_changes.append(float(oi_change))
                        oi_change_pct = _first_num(extra, ("oi_change_pct_1", "oi_change_pct", "open_interest_change_pct"))
                        if oi_change_pct is None and prev_oi not in (None, 0):
                            oi_change_pct = oi_change / prev_oi
                        if oi_change_pct is not None:
                            st.oi_change_pcts.append(float(oi_change_pct))
                            if prev_change_pct is not None:
                                st.oi_accels.append(float(oi_change_pct - prev_change_pct))
                st.oi_source_ts = _first_value(extra, ("oi_source_ts", "oi_available_at", "available_at"))
                st.oi_available_at = _first_value(extra, ("oi_available_at", "oi_source_ts", "available_at"))
            for value, window in (
                (_first_num(extra, ("premium_mark_vs_index", "premium", "premium_pct")), st.premiums),
                (_first_num(extra, ("basis_close_vs_index", "basis", "basis_pct", "mark_index_basis", "mark_index_basis_pct")), st.bases),
                (_first_num(extra, ("liq_buy_notional",)), st.liq_buy_notionals),
                (_first_num(extra, ("liq_sell_notional",)), st.liq_sell_notionals),
            ):
                if value is not None:
                    window.append(value)
        csi, _, _ = self._compute_csi(
            vol_pctile=vol_pct,
            tr_pctile=tr_pct,
            spread_pctile=spread_pct,
            funding_pctile=_pctile(deque(list(st.funding_rates)[-self._percentile_window:], maxlen=self._percentile_window), st.funding_rates[-1] if st.funding_rates else None),
            oi_accel_pctile=_pctile(deque(list(st.oi_accels)[-self._percentile_window:], maxlen=self._percentile_window), st.oi_accels[-1] if st.oi_accels else None)
            or _pctile(deque(list(st.oi_change_pcts)[-self._percentile_window:], maxlen=self._percentile_window), st.oi_change_pcts[-1] if st.oi_change_pcts else None),
            basis_pctile=_pctile(deque(list(st.bases)[-self._percentile_window:], maxlen=self._percentile_window), st.bases[-1] if st.bases else None),
        )
        st.csi_vals.append(csi)

    def _compute_csi(
        self,
        *,
        vol_pctile: float | None,
        tr_pctile: float | None,
        spread_pctile: float | None,
        funding_pctile: float | None,
        oi_accel_pctile: float | None,
        basis_pctile: float | None,
    ) -> tuple[float, str, dict[str, Any]]:
        components: list[tuple[str, float, float]] = []
        if vol_pctile is not None:
            components.append(("vol_pctile", 0.20 if self._profile == "full" else 0.35, float(vol_pctile)))
        if tr_pctile is not None:
            components.append(("tr_over_atr_pctile", 0.20 if self._profile == "full" else 0.35, float(tr_pctile)))
        if self._profile == "full":
            funding_score = _pctile_extreme_score(funding_pctile)
            basis_score = _pctile_extreme_score(basis_pctile)
            if funding_score is not None:
                components.append(("funding_extreme_score", 0.20, funding_score))
            if oi_accel_pctile is not None:
                components.append(("oi_accel_pctile", 0.20, float(oi_accel_pctile)))
            if basis_score is not None:
                components.append(("basis_extreme_score", 0.10, basis_score))
        if spread_pctile is not None:
            components.append(("spread_proxy_pctile", -0.10 if self._profile == "full" else -0.30, float(spread_pctile)))
        positive_weight = sum(abs(weight) for _, weight, _ in components) or 1.0
        score = sum(weight * value for _, weight, value in components) / positive_weight
        score = min(1.0, max(0.0, score))
        enriched_components = {"funding_extreme_score", "oi_accel_pctile", "basis_extreme_score"}
        source = "enriched" if any(name in enriched_components for name, _, _ in components) else "ohlcv_proxy"
        payload = {
            "components": {name: value for name, _, value in components},
            "weights": {name: weight for name, weight, _ in components},
            "available": {name: True for name, _, _ in components},
        }
        return score, source, payload

    def snapshot(self, *, symbol: str) -> dict[str, Any]:
        if not self._enabled:
            return {
                "entry_state_trend_ready": False,
                "entry_state_vol_ready": False,
                "entry_state_liquidity_ready": False,
                "entry_state_csi_ready": False,
                "entry_state_htf_ready": False,
            }
        precomputed = self._precomputed.get(symbol)
        if precomputed is not None:
            return dict(precomputed)
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
        funding_rate = st.funding_rates[-1] if st.funding_rates else None
        funding_pctile = _pctile(deque(list(st.funding_rates)[-self._percentile_window:], maxlen=self._percentile_window), funding_rate)
        open_interest = st.open_interests[-1] if st.open_interests else None
        oi_change_1 = st.oi_changes[-1] if st.oi_changes else None
        oi_change_pct_1 = st.oi_change_pcts[-1] if st.oi_change_pcts else None
        oi_accel = st.oi_accels[-1] if st.oi_accels else None
        oi_accel_pctile = _pctile(deque(list(st.oi_accels)[-self._percentile_window:], maxlen=self._percentile_window), oi_accel)
        if oi_accel_pctile is None:
            oi_accel_pctile = _pctile(deque(list(st.oi_change_pcts)[-self._percentile_window:], maxlen=self._percentile_window), oi_change_pct_1)
        premium = st.premiums[-1] if st.premiums else None
        premium_pctile = _pctile(deque(list(st.premiums)[-self._percentile_window:], maxlen=self._percentile_window), premium)
        basis = st.bases[-1] if st.bases else None
        basis_pctile = _pctile(deque(list(st.bases)[-self._percentile_window:], maxlen=self._percentile_window), basis)
        csi, csi_source, csi_components = self._compute_csi(
            vol_pctile=vol_pctile,
            tr_pctile=tr_pctile,
            spread_pctile=spread_pctile,
            funding_pctile=funding_pctile,
            oi_accel_pctile=oi_accel_pctile,
            basis_pctile=basis_pctile,
        )
        csi_pctile = _pctile(deque(list(st.csi_vals)[-self._percentile_window:], maxlen=self._percentile_window), csi)
        state = {
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
            "entry_state_csi_source": csi_source,
            "entry_state_csi_components_json": _json_compact(csi_components["components"]),
            "entry_state_csi_components_available_json": _json_compact(csi_components["available"]),
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
        if self._profile == "minimal":
            return state

        liq_buy_notional = st.liq_buy_notionals[-1] if st.liq_buy_notionals else None
        liq_sell_notional = st.liq_sell_notionals[-1] if st.liq_sell_notionals else None

        state.update({
            "entry_state_mark_price": st.mark_close,
            "entry_state_index_price": st.index_close,
            "entry_state_mark_close": st.mark_close,
            "entry_state_index_close": st.index_close,
            "entry_state_funding_rate": funding_rate,
            "entry_state_funding_raw": funding_rate,
            "entry_state_funding_source_ts": st.funding_source_ts,
            "entry_state_funding_available_at": st.funding_available_at,
            "entry_state_funding_pctile": funding_pctile,
            "entry_state_funding_z": _z(st.funding_rates, funding_rate),
            "entry_state_funding_regime": _regime_signed(funding_rate, funding_pctile, prefix="funding"),
            "entry_state_open_interest": open_interest,
            "entry_state_oi_level": open_interest,
            "entry_state_oi_source_ts": st.oi_source_ts,
            "entry_state_oi_available_at": st.oi_available_at,
            "entry_state_oi_change_1": oi_change_1,
            "entry_state_oi_change": oi_change_1,
            "entry_state_oi_change_pct_1": oi_change_pct_1,
            "entry_state_oi_change_pct": oi_change_pct_1,
            "entry_state_oi_accel": oi_accel,
            "entry_state_oi_accel_pctile": oi_accel_pctile,
            "entry_state_oi_z": _z(st.open_interests, open_interest),
            "entry_state_oi_regime": _regime_high(open_interest, _pctile(deque(list(st.open_interests)[-self._percentile_window:], maxlen=self._percentile_window), open_interest), prefix="oi"),
            "entry_state_premium_mark_vs_index": premium,
            "entry_state_premium_raw": premium,
            "entry_state_premium_pctile": premium_pctile,
            "entry_state_basis_close_vs_index": basis,
            "entry_state_basis_raw": basis,
            "entry_state_basis_pct": basis,
            "entry_state_basis_pctile": basis_pctile,
            "entry_state_basis_regime": _regime_signed(basis, basis_pctile, prefix="basis"),
            "entry_state_crowding_proxy": max(abs(funding_rate or 0.0), abs(oi_change_pct_1 or 0.0)) if (funding_rate is not None or oi_change_pct_1 is not None) else None,
            "entry_state_crowding_proxy_pctile": max(_pctile_extreme_score(funding_pctile) or 0.0, oi_accel_pctile or 0.0) if (funding_pctile is not None or oi_accel_pctile is not None) else None,
            "entry_state_constraint_stress_proxy": csi,
            "entry_state_constraint_stress_pctile": csi_pctile,
            "entry_state_liq_buy_notional": liq_buy_notional,
            "entry_state_liq_sell_notional": liq_sell_notional,
            "entry_state_liq_imbalance": (
                (liq_buy_notional - liq_sell_notional) / (liq_buy_notional + liq_sell_notional)
                if liq_buy_notional is not None and liq_sell_notional is not None and (liq_buy_notional + liq_sell_notional) > 0
                else None
            ),
        })
        return state
