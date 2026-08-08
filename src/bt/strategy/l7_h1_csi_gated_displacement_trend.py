"""L7-H1 CSI-gated displacement trend strategy."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import json
import math
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.data.resample import TimeframeResampler
from bt.engine.fast_path.l7_h1_kernel import prefix_for_timeframe
from bt.indicators.atr import ATR
from bt.logging.decision_trace import make_decision_trace
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


def _finite(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _is_missing_metadata_value(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(value != value)
    except Exception:
        return False


def _extra_num(extra: Mapping[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = _finite(extra.get(key))
        if value is not None:
            return value
    return None


def _pctile(values: deque[float], value: float | None, *, min_count: int = 5) -> float | None:
    if value is None:
        return None
    vals = [float(v) for v in values if math.isfinite(float(v))]
    if len(vals) < min_count:
        return None
    return sum(1 for v in vals if v <= float(value)) / len(vals)


def _zscore(values: deque[float], value: float | None, *, min_count: int = 5) -> float | None:
    if value is None:
        return None
    vals = [float(v) for v in values if math.isfinite(float(v))]
    if len(vals) < min_count:
        return None
    mean = sum(vals) / len(vals)
    var = sum((v - mean) ** 2 for v in vals) / len(vals)
    std = var ** 0.5
    if std <= 0:
        return None
    return (float(value) - mean) / std


def _norm_z(value: float | None) -> float:
    if value is None:
        return 0.5
    return min(1.0, max(0.0, (float(value) + 3.0) / 6.0))


def _norm_displacement(value: float | None, threshold: float) -> float:
    if value is None:
        return 0.0
    denom = max(float(threshold) * 2.0, 1e-12)
    return min(1.0, max(0.0, float(value) / denom))


def _basis_from_extra(extra: Mapping[str, Any]) -> tuple[float | None, str | None]:
    basis = _extra_num(extra, ("basis_close_vs_index", "basis", "basis_pct", "mark_index_basis", "mark_index_basis_pct"))
    if basis is not None:
        return basis, None
    mark = _extra_num(extra, ("mark_close", "mark_price", "mark"))
    index = _extra_num(extra, ("index_close", "index_price", "index"))
    if mark is None or index is None or index == 0:
        return None, "missing_mark_or_index"
    return (mark - index) / index, None


def _json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


@dataclass
class _FeatureState:
    atr: ATR
    prev_signal_close: float | None = None
    last_signal_ts: pd.Timestamp | None = None
    funding_values: deque[float] = field(default_factory=lambda: deque(maxlen=30 * 24 * 60))
    basis_values: deque[float] = field(default_factory=lambda: deque(maxlen=30 * 24 * 60))
    oi_values: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    oi_returns: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    oi_return_zs: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    volume_values: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    volume_returns: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    spread_values: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    csi_raw_values: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    close_values: deque[float] = field(default_factory=lambda: deque(maxlen=240))
    position: Side | None = None
    entry_price: float | None = None
    atr_entry: float | None = None
    stop_distance: float | None = None
    stop_price: float | None = None
    trailing_stop: float | None = None
    high_since_entry: float | None = None
    low_since_entry: float | None = None
    signal_bars_held: int = 0
    csi_low_count: int = 0
    last_features: dict[str, Any] = field(default_factory=dict)


@register_strategy("l7_h1_csi_gated_displacement_trend")
class L7H1CSIGatedDisplacementTrendStrategy(Strategy):
    def __init__(
        self,
        *,
        timeframe: str = "15m",
        signal_timeframe: str | None = None,
        atr_period: int = 14,
        basis_lookback_days: int = 30,
        d0: float = 1.8,
        theta: float = 0.7,
        theta_low: float = 0.5,
        csi_low_bars: int = 3,
        k_stop: float = 3.0,
        k_trail: float = 3.0,
        T_hold: int = 48,
        r_per_trade: float = 0.01,
        disallow_flip: bool = True,
        use_compiled_features: bool = True,
        use_compiled_event_kernel: bool = False,
        compiled_event_source: str = "columns",
    ) -> None:
        self._timeframe = str(signal_timeframe or timeframe).lower()
        self._atr_period = int(atr_period)
        self._basis_lookback_bars = max(5, int(basis_lookback_days) * 24 * 60)
        self._d0 = float(d0)
        self._theta = float(theta)
        self._theta_low = float(theta_low)
        self._csi_low_bars = int(csi_low_bars)
        self._k_stop = float(k_stop)
        self._k_trail = float(k_trail)
        self._T_hold = int(T_hold)
        self._r_per_trade = float(r_per_trade)
        self._disallow_flip = bool(disallow_flip)
        self._use_compiled_features = bool(use_compiled_features)
        self._use_compiled_event_kernel = bool(use_compiled_event_kernel)
        self._compiled_event_source = str(compiled_event_source or "columns").strip().lower()
        if self._compiled_event_source not in {"columns", "online"}:
            raise ValueError("compiled_event_source must be one of: columns, online")
        self._online_resampler = TimeframeResampler(timeframes=[self._timeframe], strict=True)
        self._state: dict[str, _FeatureState] = {}

    def _state_for(self, symbol: str) -> _FeatureState:
        state = self._state.get(symbol)
        if state is None:
            state = _FeatureState(atr=ATR(self._atr_period))
            state.funding_values = deque(maxlen=self._basis_lookback_bars)
            state.basis_values = deque(maxlen=self._basis_lookback_bars)
            self._state[symbol] = state
        return state

    @staticmethod
    def _ctx_position_side(ctx: Mapping[str, Any], symbol: str) -> Side | None:
        positions = ctx.get("positions")
        if not isinstance(positions, Mapping):
            return None
        raw = positions.get(symbol)
        if not isinstance(raw, Mapping):
            return None
        value = raw.get("side")
        if isinstance(value, Side):
            return value
        if isinstance(value, str):
            lowered = value.lower()
            if lowered == "buy":
                return Side.BUY
            if lowered == "sell":
                return Side.SELL
        return None

    @staticmethod
    def _clear_trade_state(state: _FeatureState) -> None:
        state.position = None
        state.entry_price = None
        state.atr_entry = None
        state.stop_distance = None
        state.stop_price = None
        state.trailing_stop = None
        state.high_since_entry = None
        state.low_since_entry = None
        state.signal_bars_held = 0
        state.csi_low_count = 0

    def _update_base_features(self, state: _FeatureState, bar: Bar) -> None:
        extra = bar.extra if isinstance(bar.extra, Mapping) else {}
        funding = _extra_num(extra, ("funding_rate", "funding", "funding_raw", "funding_rate_realized"))
        if funding is not None:
            state.funding_values.append(funding)
        basis, _ = _basis_from_extra(extra)
        if basis is not None:
            state.basis_values.append(basis)

        oi = _extra_num(extra, ("open_interest", "oi", "oi_value", "oi_contracts", "oi_usd"))
        if oi is not None:
            prev_oi = state.oi_values[-1] if state.oi_values else None
            state.oi_values.append(oi)
            if prev_oi not in (None, 0):
                oi_ret = (oi - float(prev_oi)) / float(prev_oi)
                oi_z = _zscore(state.oi_returns, oi_ret)
                state.oi_returns.append(oi_ret)
                if oi_z is not None:
                    state.oi_return_zs.append(oi_z)

        prev_vol = state.volume_values[-1] if state.volume_values else None
        state.volume_values.append(float(bar.volume))
        if prev_vol not in (None, 0):
            state.volume_returns.append((float(bar.volume) - float(prev_vol)) / float(prev_vol))

        spread = 0.5 * (float(bar.high) - float(bar.low)) / float(bar.close) if float(bar.close) else 0.0
        state.spread_values.append(spread)
        state.close_values.append(float(bar.close))

    def _features_for_signal_bar(self, *, state: _FeatureState, signal_bar: Bar) -> dict[str, Any]:
        prev_atr = state.atr.value
        prev_close = state.prev_signal_close
        tr = None
        if prev_close is not None:
            tr = max(
                float(signal_bar.high) - float(signal_bar.low),
                abs(float(signal_bar.high) - float(prev_close)),
                abs(float(signal_bar.low) - float(prev_close)),
            )
        state.atr.update(signal_bar)
        atr_for_signal = _finite(prev_atr)
        d_t = (float(tr) / atr_for_signal) if tr is not None and atr_for_signal and atr_for_signal > 0 else None

        funding_latest = state.funding_values[-1] if state.funding_values else None
        funding_pct = _pctile(state.funding_values, funding_latest)
        basis_latest = state.basis_values[-1] if state.basis_values else None
        basis_pct = _pctile(state.basis_values, basis_latest)
        funding_source = "funding" if funding_pct is not None else "basis_fallback"
        funding_component = funding_pct if funding_pct is not None else basis_pct

        oi_z = state.oi_return_zs[-1] if state.oi_return_zs else None
        volume_z = _zscore(state.volume_returns, state.volume_returns[-1] if state.volume_returns else None)
        oi_source = "oi" if oi_z is not None else "volume_fallback"
        oi_component = _norm_z(oi_z if oi_z is not None else volume_z)

        spread = 0.5 * (float(signal_bar.high) - float(signal_bar.low)) / float(signal_bar.close) if float(signal_bar.close) else 0.0
        spread_pct = _pctile(state.spread_values, spread)
        spread_component = 1.0 - float(spread_pct) if spread_pct is not None else 0.5

        components = {
            "funding_pct": float(funding_component) if funding_component is not None else 0.5,
            "oi_z": oi_component,
            "D_t": _norm_displacement(d_t, self._d0),
            "S_t": spread_component,
        }
        weights = {"funding_pct": 0.35, "oi_z": 0.25, "D_t": 0.30, "S_t": 0.10}
        raw_csi = sum(weights[key] * components[key] for key in weights)
        state.csi_raw_values.append(float(raw_csi))
        csi_min = min(state.csi_raw_values)
        csi_max = max(state.csi_raw_values)
        csi = (raw_csi - csi_min) / (csi_max - csi_min) if csi_max > csi_min and len(state.csi_raw_values) >= 5 else raw_csi
        csi = min(1.0, max(0.0, float(csi)))

        recent_returns = []
        closes = list(state.close_values)[-6:]
        for idx in range(1, len(closes)):
            prev = closes[idx - 1]
            recent_returns.append((closes[idx] - prev) / prev if prev else 0.0)
        ret_1 = recent_returns[-1] if recent_returns else None
        ret_5 = (closes[-1] - closes[0]) / closes[0] if len(closes) >= 6 and closes[0] else None
        vol_20 = None
        all_closes = list(state.close_values)
        if len(all_closes) >= 21:
            returns = [
                (all_closes[i] - all_closes[i - 1]) / all_closes[i - 1]
                for i in range(len(all_closes) - 20, len(all_closes))
                if all_closes[i - 1]
            ]
            vol_20 = (sum(r * r for r in returns) / len(returns)) ** 0.5 if returns else None

        direction = float(signal_bar.close) - float(prev_close if prev_close is not None else signal_bar.open)
        side = Side.BUY if direction > 0 else Side.SELL
        basis_skip_reason = None if basis_latest is not None else "spot_index_missing"
        oi_fallback_used = oi_z is None

        state.prev_signal_close = float(signal_bar.close)
        features = {
            "D_t": d_t,
            "ATR_14": atr_for_signal,
            "CSI": csi,
            "CSI_raw": raw_csi,
            "funding_pct": funding_pct,
            "basis_pct": basis_pct,
            "oi_z": oi_z,
            "volume_z": volume_z,
            "S_t": spread,
            "spread_rank_desc": spread_component,
            "csi_component_funding": components["funding_pct"],
            "csi_component_oi": components["oi_z"],
            "csi_component_displacement": components["D_t"],
            "csi_component_spread": components["S_t"],
            "csi_components_json": _json(components),
            "csi_source": f"{funding_source}+{oi_source}",
            "basis_skip_reason": basis_skip_reason,
            "oi_fallback_used": oi_fallback_used,
            "signal_timeframe": self._timeframe,
            "exit_monitoring_timeframe": "1m",
            "entry_signal_ts": str(signal_bar.ts),
            "side_from_displacement": side.name,
            "recent_return_1": ret_1,
            "recent_return_5": ret_5,
            "volatility_20": vol_20,
        }
        features["state_vector"] = _json(
            {
                "CSI": csi,
                "ATR_14": atr_for_signal,
                "D_t": d_t,
                "S_t": spread,
                "oi_z": oi_z,
                "volume_z": volume_z,
                "funding_pct": funding_pct,
                "basis_pct": basis_pct,
                "recent_return_1": ret_1,
                "recent_return_5": ret_5,
                "volatility_20": vol_20,
            }
        )
        state.last_features = dict(features)
        return features

    def _precomputed_features_for_signal_bar(
        self,
        *,
        state: _FeatureState,
        decision_bar: Bar,
        signal_bar: Bar,
    ) -> dict[str, Any] | None:
        if not self._use_compiled_features:
            return None
        extra = decision_bar.extra if isinstance(decision_bar.extra, Mapping) else {}
        prefix = prefix_for_timeframe(self._timeframe)
        if not bool(extra.get(f"{prefix}compiled_feature_ready", False)):
            return None

        def pick(name: str) -> Any:
            return extra.get(f"{prefix}{name}")

        d_t = _finite(pick("D_t"))
        atr = _finite(pick("ATR_14"))
        csi = _finite(pick("CSI"))
        csi_raw = _finite(pick("CSI_raw"))
        side_code = _finite(pick("side_code"))
        side = Side.BUY if side_code is not None and side_code > 0 else Side.SELL
        components = {
            "funding_pct": float(_finite(pick("csi_component_funding")) or 0.5),
            "oi_z": float(_finite(pick("csi_component_oi")) or 0.5),
            "D_t": float(_finite(pick("csi_component_displacement")) or 0.0),
            "S_t": float(_finite(pick("csi_component_spread")) or 0.5),
        }
        funding_pct = _finite(pick("funding_pct"))
        basis_pct = _finite(pick("basis_pct"))
        oi_z = _finite(pick("oi_z"))
        volume_z = _finite(pick("volume_z"))
        funding_source = "funding" if funding_pct is not None else "basis_fallback"
        oi_source = "oi" if oi_z is not None else "volume_fallback"
        spread = _finite(pick("S_t"))
        spread_component = _finite(pick("spread_rank_desc"))
        state.prev_signal_close = float(signal_bar.close)
        features = {
            "D_t": d_t,
            "ATR_14": atr,
            "CSI": csi,
            "CSI_raw": csi_raw,
            "funding_pct": funding_pct,
            "basis_pct": basis_pct,
            "oi_z": oi_z,
            "volume_z": volume_z,
            "S_t": spread,
            "spread_rank_desc": spread_component,
            "csi_component_funding": components["funding_pct"],
            "csi_component_oi": components["oi_z"],
            "csi_component_displacement": components["D_t"],
            "csi_component_spread": components["S_t"],
            "csi_components_json": _json(components),
            "csi_source": f"{funding_source}+{oi_source}+compiled_l7h1",
            "basis_skip_reason": None if basis_pct is not None else "spot_index_missing",
            "oi_fallback_used": oi_z is None,
            "signal_timeframe": self._timeframe,
            "exit_monitoring_timeframe": "1m",
            "entry_signal_ts": str(signal_bar.ts),
            "side_from_displacement": side.name,
            "recent_return_1": None,
            "recent_return_5": None,
            "volatility_20": None,
            "l7h1_feature_kernel": "compiled",
        }
        for key, value in extra.items():
            if key.startswith("entry_state_") and key not in features:
                features[key] = value
        panel_state_aliases = {
            "entry_state_mark_price": ("mark_close", "mark_price", "mark"),
            "entry_state_mark_close": ("mark_close", "mark_price", "mark"),
            "entry_state_index_price": ("index_close", "index_price", "index"),
            "entry_state_index_close": ("index_close", "index_price", "index"),
            "entry_state_funding_raw": ("funding_rate", "funding", "funding_raw", "funding_rate_realized"),
            "entry_state_funding_rate": ("funding_rate", "funding", "funding_raw", "funding_rate_realized"),
            "entry_state_funding_source_ts": ("funding_source_ts", "funding_available_at", "available_at"),
            "entry_state_open_interest": ("open_interest", "oi", "oi_value", "oi_contracts", "oi_usd"),
            "entry_state_oi_level": ("open_interest", "oi", "oi_value", "oi_contracts", "oi_usd"),
            "entry_state_oi_source_ts": ("oi_source_ts", "oi_available_at", "available_at"),
            "entry_state_oi_change_1": ("oi_change_1", "oi_change", "open_interest_change"),
            "entry_state_oi_change_pct_1": ("oi_change_pct_1", "oi_change_pct", "open_interest_change_pct"),
            "entry_state_premium_raw": ("premium_mark_vs_index", "premium", "premium_pct"),
            "entry_state_premium_mark_vs_index": ("premium_mark_vs_index", "premium", "premium_pct"),
            "entry_state_basis_raw": ("basis_close_vs_index", "basis", "basis_pct", "mark_index_basis", "mark_index_basis_pct"),
            "entry_state_basis_close_vs_index": ("basis_close_vs_index", "basis", "basis_pct", "mark_index_basis", "mark_index_basis_pct"),
            "entry_state_liq_buy_notional": ("liq_buy_notional",),
            "entry_state_liq_sell_notional": ("liq_sell_notional",),
        }
        for state_key, source_keys in panel_state_aliases.items():
            if state_key in features and not _is_missing_metadata_value(features.get(state_key)):
                continue
            for source_key in source_keys:
                if source_key in extra and not _is_missing_metadata_value(extra.get(source_key)):
                    features[state_key] = extra.get(source_key)
                    break
        features["state_vector"] = _json(
            {
                "CSI": csi,
                "ATR_14": atr,
                "D_t": d_t,
                "S_t": spread,
                "oi_z": oi_z,
                "volume_z": volume_z,
                "funding_pct": funding_pct,
                "basis_pct": basis_pct,
                "kernel": "compiled_l7h1",
            }
        )
        state.last_features = dict(features)
        return features

    def _compiled_features_available(self, bar: Bar) -> bool:
        if not self._use_compiled_features:
            return False
        extra = bar.extra if isinstance(bar.extra, Mapping) else {}
        prefix = prefix_for_timeframe(self._timeframe)
        return any(key.startswith(prefix) for key in extra)

    def _compiled_ready(self, bar: Bar) -> bool:
        if not self._compiled_features_available(bar):
            return False
        extra = bar.extra if isinstance(bar.extra, Mapping) else {}
        return bool(extra.get(f"{prefix_for_timeframe(self._timeframe)}compiled_feature_ready", False))

    def _compiled_features_for_decision_bar(self, *, state: _FeatureState, bar: Bar) -> dict[str, Any] | None:
        return self._precomputed_features_for_signal_bar(state=state, decision_bar=bar, signal_bar=bar)

    def _entry_metadata(self, *, symbol: str, ts: pd.Timestamp, side: Side, entry_ref: float, stop_distance: float, features: dict[str, Any]) -> dict[str, Any] | None:
        entry_ref = float(entry_ref)
        stop_distance = float(stop_distance)
        stop_price = entry_ref - stop_distance if side == Side.BUY else entry_ref + stop_distance
        # Strategy-side risk metadata must be executable before it reaches the
        # engine. Tiny-priced symbols can otherwise produce a long stop below
        # zero when ATR * k_stop exceeds entry_ref; that is an invalid trade,
        # not an engine/accounting failure.
        if not math.isfinite(entry_ref) or entry_ref <= 0:
            return None
        if not math.isfinite(stop_distance) or stop_distance <= 0:
            return None
        if not math.isfinite(stop_price) or stop_price <= 0:
            return None
        gate_values = {"D_t": features.get("D_t"), "CSI": features.get("CSI")}
        gate_thresholds = {"D_t": self._d0, "CSI": self._theta}
        gate_margins = {
            "D_t": (float(features["D_t"]) - self._d0) if features.get("D_t") is not None else None,
            "CSI": (float(features["CSI"]) - self._theta) if features.get("CSI") is not None else None,
        }
        return {
            **features,
            "strategy": "l7_h1_csi_gated_displacement_trend",
            "strategy_id": "l7_h1_csi_gated_displacement_trend",
            "family_variant": "L7-H1",
            "family_pattern": "csi_gated_displacement_trend",
            "entry_reason": "csi_gated_displacement",
            "entry_price": entry_ref,
            "entry_reference_price": entry_ref,
            "intended_entry_price": entry_ref,
            "signal_timeframe": self._timeframe,
            "execution_timeframe": "1m",
            "exit_monitoring_timeframe": "1m",
            "risk_accounting": "engine_canonical_R",
            "r_per_trade": self._r_per_trade,
            "stop_model": "chandelier_trail",
            "stop_update_policy": "trailing",
            "stop_price": stop_price,
            "entry_stop_price": stop_price,
            "stop_distance": stop_distance,
            "trailing_stop": stop_price,
            "trailing_stop_initial": stop_price,
            "atr_entry": features.get("ATR_14"),
            "k_stop": self._k_stop,
            "k_trail": self._k_trail,
            "theta": self._theta,
            "theta_low": self._theta_low,
            "d0": self._d0,
            "gate_pass": True,
            "position_side": "long" if side == Side.BUY else "short",
            "entry_state_csi_pctile": features.get("CSI"),
            "entry_state_csi_raw": features.get("CSI"),
            "entry_state_tr_over_atr": features.get("D_t"),
            "entry_state_spread_proxy": features.get("S_t"),
            "entry_state_spread_proxy_pctile": 1.0 - float(features.get("spread_rank_desc", 0.5)),
            "entry_state_funding_pctile": features.get("funding_pct"),
            "entry_state_basis_pctile": features.get("basis_pct"),
            "entry_state_oi_z": features.get("oi_z"),
            "entry_state_oi_accel_pctile": features.get("csi_component_oi"),
            "entry_state_volume_z": features.get("volume_z"),
            "entry_state_csi_source": features.get("csi_source"),
            "entry_state_csi_components_json": features.get("csi_components_json"),
            "entry_state_displacement_regime": "extreme_impulse" if float(features.get("D_t") or 0.0) >= 2.0 else "strong_impulse",
            "entry_decision_setup_class": "trend_displacement",
            "decision_trace": make_decision_trace(
                reason_code="csi_gated_displacement",
                setup_class="trend_displacement",
                hypothesis_branch="entry",
                conditions_bool_map={"displacement_gate": True, "csi_gate": True},
                blockers_bool_map={},
                permission_layer_state={"basis_skip_reason": features.get("basis_skip_reason"), "oi_fallback_used": features.get("oi_fallback_used")},
                parameter_combination={"strategy": "l7_h1_csi_gated_displacement_trend", "timeframe": self._timeframe},
                gate_values=gate_values,
                gate_thresholds=gate_thresholds,
                gate_margins=gate_margins,
                most_binding_gate=min((k for k, v in gate_margins.items() if v is not None), key=lambda k: gate_margins[k], default=None),
            ),
        }

    def _state_log_signal(self, *, ts: pd.Timestamp, symbol: str, features: dict[str, Any], reason: str) -> Signal:
        metadata = {
            **features,
            "strategy": "l7_h1_csi_gated_displacement_trend",
            "family_variant": "L7-H1",
            "entry_reason": reason,
            "gate_pass": False,
            "state_log_only": True,
            "entry_state_csi_pctile": features.get("CSI"),
            "entry_state_tr_over_atr": features.get("D_t"),
            "entry_state_spread_proxy": features.get("S_t"),
            "entry_state_funding_pctile": features.get("funding_pct"),
            "entry_state_basis_pctile": features.get("basis_pct"),
            "entry_state_oi_z": features.get("oi_z"),
            "entry_state_volume_z": features.get("volume_z"),
        }
        return Signal(ts=ts, symbol=symbol, side=None, signal_type="l7_h1_state", confidence=0.0, metadata=metadata)

    def _handle_open_position(self, *, ts: pd.Timestamp, symbol: str, bar: Bar, state: _FeatureState, side: Side, has_new_signal_bar: bool) -> list[Signal]:
        state.position = side
        state.high_since_entry = float(bar.high) if state.high_since_entry is None else max(float(state.high_since_entry), float(bar.high))
        state.low_since_entry = float(bar.low) if state.low_since_entry is None else min(float(state.low_since_entry), float(bar.low))

        if has_new_signal_bar:
            state.signal_bars_held += 1
            csi = _finite(state.last_features.get("CSI"))
            if csi is not None and csi < self._theta_low:
                state.csi_low_count += 1
            else:
                state.csi_low_count = 0

        atr = state.atr_entry
        if atr is not None and atr > 0:
            if side == Side.BUY and state.high_since_entry is not None:
                trail = float(state.high_since_entry) - self._k_trail * float(atr)
                state.trailing_stop = trail if state.trailing_stop is None else max(float(state.trailing_stop), trail)
                if state.stop_price is not None:
                    state.stop_price = max(float(state.stop_price), float(state.trailing_stop))
            elif side == Side.SELL and state.low_since_entry is not None:
                trail = float(state.low_since_entry) + self._k_trail * float(atr)
                state.trailing_stop = trail if state.trailing_stop is None else min(float(state.trailing_stop), trail)
                if state.stop_price is not None:
                    state.stop_price = min(float(state.stop_price), float(state.trailing_stop))

        exit_reason = None
        stop = state.stop_price
        if stop is not None:
            if side == Side.BUY and float(bar.low) <= float(stop):
                exit_reason = "chandelier_trailing_stop"
            elif side == Side.SELL and float(bar.high) >= float(stop):
                exit_reason = "chandelier_trailing_stop"
        if exit_reason is None and self._csi_low_bars > 0 and state.csi_low_count >= self._csi_low_bars:
            exit_reason = "csi_exhaustion"
        if exit_reason is None and self._T_hold > 0 and state.signal_bars_held >= self._T_hold:
            exit_reason = "time_stop"
        if exit_reason is None:
            return []

        metadata = {
            **state.last_features,
            "strategy": "l7_h1_csi_gated_displacement_trend",
            "close_only": True,
            "is_exit": True,
            "exit_reason": exit_reason,
            "exit_monitoring_timeframe": "1m",
            "signal_timeframe": self._timeframe,
            "stop_model": "chandelier_trail",
            "trailing_stop": state.trailing_stop,
            "stop_price": state.stop_price,
            "stop_distance": state.stop_distance,
            "atr_entry": state.atr_entry,
            "holding_period_bars_signal": state.signal_bars_held,
            "hold_duration_bars": state.signal_bars_held,
            "csi_low_count": state.csi_low_count,
        }
        self._clear_trade_state(state)
        return [
            Signal(
                ts=ts,
                symbol=symbol,
                side=Side.SELL if side == Side.BUY else Side.BUY,
                signal_type="l7_h1_exit",
                confidence=1.0,
                metadata=metadata,
            )
        ]

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        if self._use_compiled_event_kernel:
            return self._on_bars_compiled_event(ts, bars_by_symbol, tradeable, ctx)

        htf_root = ctx.get("htf") if isinstance(ctx, Mapping) else None
        if not isinstance(htf_root, Mapping):
            raise RuntimeError(f"L7-H1 requires ctx['htf']['{self._timeframe}'] for two-clock semantics.")
        htf_for_tf = htf_root.get(self._timeframe, {})
        if not isinstance(htf_for_tf, Mapping):
            raise RuntimeError(f"L7-H1 requires mapping ctx['htf']['{self._timeframe}'] for two-clock semantics.")

        if bool(ctx.get("warmup")):
            self._warmup_feature_state(bars_by_symbol=bars_by_symbol, htf_for_tf=htf_for_tf)
            return []

        signals: list[Signal] = []
        for symbol in sorted(tradeable):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            state = self._state_for(symbol)
            compiled_available = self._compiled_features_available(bar)
            if not compiled_available:
                self._update_base_features(state, bar)

            signal_bar = htf_for_tf.get(symbol)
            has_new_signal_bar = signal_bar is not None and signal_bar.ts != state.last_signal_ts
            if has_new_signal_bar and signal_bar is not None:
                state.last_signal_ts = signal_bar.ts
                features = self._precomputed_features_for_signal_bar(
                    state=state,
                    decision_bar=bar,
                    signal_bar=signal_bar,
                )
                if features is None:
                    if compiled_available:
                        features = {
                            "D_t": None,
                            "ATR_14": None,
                            "CSI": None,
                            "CSI_raw": None,
                            "signal_timeframe": self._timeframe,
                            "exit_monitoring_timeframe": "1m",
                            "entry_signal_ts": str(signal_bar.ts),
                            "basis_skip_reason": "compiled_features_not_ready",
                            "oi_fallback_used": None,
                            "l7h1_feature_kernel": "compiled_missing",
                        }
                    else:
                        features = self._features_for_signal_bar(state=state, signal_bar=signal_bar)
                state.last_features = features

            current = self._ctx_position_side(ctx, symbol)
            if current is not None:
                signals.extend(self._handle_open_position(ts=ts, symbol=symbol, bar=bar, state=state, side=current, has_new_signal_bar=has_new_signal_bar))
                continue
            if state.position is not None:
                self._clear_trade_state(state)
            if not has_new_signal_bar:
                continue

            features = state.last_features
            d_t = _finite(features.get("D_t"))
            csi = _finite(features.get("CSI"))
            if d_t is None or features.get("ATR_14") is None:
                signals.append(self._state_log_signal(ts=ts, symbol=symbol, features=features, reason="indicators_not_ready"))
                continue
            if d_t < self._d0 or csi is None or csi < self._theta:
                signals.append(self._state_log_signal(ts=ts, symbol=symbol, features=features, reason="gates_failed"))
                continue

            side = Side.BUY if features.get("side_from_displacement") == Side.BUY.name else Side.SELL
            if self._disallow_flip and current is not None and current != side:
                signals.append(self._state_log_signal(ts=ts, symbol=symbol, features=features, reason="flip_disallowed"))
                continue
            atr = float(features["ATR_14"])
            stop_distance = self._k_stop * atr
            entry_ref = float(bar.close)
            metadata = self._entry_metadata(symbol=symbol, ts=ts, side=side, entry_ref=entry_ref, stop_distance=stop_distance, features=features)
            if metadata is None:
                signals.append(self._state_log_signal(ts=ts, symbol=symbol, features=features, reason="invalid_stop_geometry"))
                continue
            state.entry_price = entry_ref
            state.atr_entry = atr
            state.stop_distance = stop_distance
            state.stop_price = metadata["stop_price"]
            state.trailing_stop = metadata["trailing_stop"]
            state.high_since_entry = float(bar.high)
            state.low_since_entry = float(bar.low)
            state.signal_bars_held = 0
            state.csi_low_count = 0
            signals.append(Signal(ts=ts, symbol=symbol, side=side, signal_type="l7_h1_entry", confidence=1.0, metadata=metadata))

        return signals

    def _warmup_feature_state(self, *, bars_by_symbol: dict[str, Bar], htf_for_tf: Mapping[str, Any]) -> None:
        """Warm causal L7-H1 feature state without emitting entries/exits.

        Engine warmup intentionally passes an empty tradeable set so strategies
        cannot create pre-start orders. L7-H1 still needs the same passive
        funding/OI/basis/spread/HTF state it would have accumulated online.
        """
        for symbol in sorted(bars_by_symbol):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            state = self._state_for(symbol)
            if not self._compiled_features_available(bar):
                self._update_base_features(state, bar)
            signal_bar = htf_for_tf.get(symbol)
            has_new_signal_bar = signal_bar is not None and signal_bar.ts != state.last_signal_ts
            if has_new_signal_bar and signal_bar is not None:
                state.last_signal_ts = signal_bar.ts
                features = self._precomputed_features_for_signal_bar(
                    state=state,
                    decision_bar=bar,
                    signal_bar=signal_bar,
                )
                if features is None:
                    features = self._features_for_signal_bar(state=state, signal_bar=signal_bar)
                state.last_features = features

    def _on_bars_compiled_event(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        """Compiled-column event adapter for L7-H1.

        This path deliberately emits the same public ``Signal`` objects as the
        classic strategy and leaves all order/risk/execution/accounting/logging
        semantics to the existing engine. It is safe only when exact causal
        L7-H1 columns have been stamped onto the panel.
        """
        if self._compiled_event_source == "online":
            return self._on_bars_online_event(ts, bars_by_symbol, tradeable, ctx)

        signals: list[Signal] = []
        positions = ctx.get("positions") if isinstance(ctx, Mapping) else {}
        active_position_symbols: set[str] = set()
        if isinstance(positions, Mapping):
            active_position_symbols = {
                str(symbol)
                for symbol, payload in positions.items()
                if isinstance(payload, Mapping) and payload.get("side") is not None
            }

        # Exits only need to inspect currently open positions. This avoids the
        # classic per-minute scan over every tradeable symbol for inactive names.
        for symbol in sorted(active_position_symbols):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            state = self._state_for(symbol)
            current = self._ctx_position_side(ctx, symbol)
            if current is None:
                self._clear_trade_state(state)
                continue
            has_new_signal_bar = self._compiled_ready(bar) and bar.ts != state.last_signal_ts
            if has_new_signal_bar:
                state.last_signal_ts = bar.ts
                features = self._compiled_features_for_decision_bar(state=state, bar=bar)
                if features is not None:
                    state.last_features = features
            signals.extend(
                self._handle_open_position(
                    ts=ts,
                    symbol=symbol,
                    bar=bar,
                    state=state,
                    side=current,
                    has_new_signal_bar=has_new_signal_bar,
                )
            )

        # Entries are only possible on compiled decision timestamps.
        for symbol in sorted(tradeable):
            if symbol in active_position_symbols:
                continue
            bar = bars_by_symbol.get(symbol)
            if bar is None or not self._compiled_ready(bar):
                continue
            state = self._state_for(symbol)
            if bar.ts == state.last_signal_ts:
                continue
            state.last_signal_ts = bar.ts
            if state.position is not None:
                self._clear_trade_state(state)
            features = self._compiled_features_for_decision_bar(state=state, bar=bar)
            if features is None:
                continue
            state.last_features = features

            d_t = _finite(features.get("D_t"))
            csi = _finite(features.get("CSI"))
            if d_t is None or features.get("ATR_14") is None:
                signals.append(self._state_log_signal(ts=ts, symbol=symbol, features=features, reason="indicators_not_ready"))
                continue
            if d_t < self._d0 or csi is None or csi < self._theta:
                signals.append(self._state_log_signal(ts=ts, symbol=symbol, features=features, reason="gates_failed"))
                continue

            side = Side.BUY if features.get("side_from_displacement") == Side.BUY.name else Side.SELL
            atr = float(features["ATR_14"])
            stop_distance = self._k_stop * atr
            entry_ref = float(bar.close)
            metadata = self._entry_metadata(symbol=symbol, ts=ts, side=side, entry_ref=entry_ref, stop_distance=stop_distance, features=features)
            if metadata is None:
                signals.append(self._state_log_signal(ts=ts, symbol=symbol, features=features, reason="invalid_stop_geometry"))
                continue
            state.position = side
            state.entry_price = entry_ref
            state.atr_entry = atr
            state.stop_distance = stop_distance
            state.stop_price = metadata["stop_price"]
            state.trailing_stop = metadata["trailing_stop"]
            state.high_since_entry = float(bar.high)
            state.low_since_entry = float(bar.low)
            state.signal_bars_held = 0
            state.csi_low_count = 0
            signals.append(Signal(ts=ts, symbol=symbol, side=side, signal_type="l7_h1_entry", confidence=1.0, metadata=metadata))

        return signals

    def _on_bars_online_event(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        """Online event adapter for path-dependent volatile streams.

        Volatile feeds emit active membership bars plus continuation bars for
        symbols with open positions or live orders. That exact stream is
        path-dependent, so pre-stamped static L7-H1 columns cannot be the source
        of truth. This adapter keeps the HTF resampling and feature state inside
        the strategy while consuming exactly the same bars the classic strategy
        sees. Risk, execution, accounting, and artifact writing remain in the
        canonical engine.
        """
        htf_for_tf: dict[str, Any] = {}
        for bar in bars_by_symbol.values():
            for htf_bar in self._online_resampler.update(bar):
                if htf_bar.timeframe == self._timeframe:
                    htf_for_tf[htf_bar.symbol] = htf_bar

        signals: list[Signal] = []
        for symbol in sorted(tradeable):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            state = self._state_for(symbol)
            self._update_base_features(state, bar)

            signal_bar = htf_for_tf.get(symbol)
            has_new_signal_bar = signal_bar is not None and signal_bar.ts != state.last_signal_ts
            if has_new_signal_bar and signal_bar is not None:
                state.last_signal_ts = signal_bar.ts
                features = self._features_for_signal_bar(state=state, signal_bar=signal_bar)
                features["l7h1_feature_kernel"] = "online_event"
                state.last_features = features

            current = self._ctx_position_side(ctx, symbol)
            if current is not None:
                signals.extend(
                    self._handle_open_position(
                        ts=ts,
                        symbol=symbol,
                        bar=bar,
                        state=state,
                        side=current,
                        has_new_signal_bar=has_new_signal_bar,
                    )
                )
                continue
            if state.position is not None:
                self._clear_trade_state(state)
            if not has_new_signal_bar:
                continue

            features = state.last_features
            d_t = _finite(features.get("D_t"))
            csi = _finite(features.get("CSI"))
            if d_t is None or features.get("ATR_14") is None:
                signals.append(self._state_log_signal(ts=ts, symbol=symbol, features=features, reason="indicators_not_ready"))
                continue
            if d_t < self._d0 or csi is None or csi < self._theta:
                signals.append(self._state_log_signal(ts=ts, symbol=symbol, features=features, reason="gates_failed"))
                continue

            side = Side.BUY if features.get("side_from_displacement") == Side.BUY.name else Side.SELL
            if self._disallow_flip and current is not None and current != side:
                signals.append(self._state_log_signal(ts=ts, symbol=symbol, features=features, reason="flip_disallowed"))
                continue
            atr = float(features["ATR_14"])
            stop_distance = self._k_stop * atr
            entry_ref = float(bar.close)
            metadata = self._entry_metadata(symbol=symbol, ts=ts, side=side, entry_ref=entry_ref, stop_distance=stop_distance, features=features)
            if metadata is None:
                signals.append(self._state_log_signal(ts=ts, symbol=symbol, features=features, reason="invalid_stop_geometry"))
                continue
            state.entry_price = entry_ref
            state.atr_entry = atr
            state.stop_distance = stop_distance
            state.stop_price = metadata["stop_price"]
            state.trailing_stop = metadata["trailing_stop"]
            state.high_since_entry = float(bar.high)
            state.low_since_entry = float(bar.low)
            state.signal_bars_held = 0
            state.csi_low_count = 0
            signals.append(Signal(ts=ts, symbol=symbol, side=side, signal_type="l7_h1_entry", confidence=1.0, metadata=metadata))

        return signals
