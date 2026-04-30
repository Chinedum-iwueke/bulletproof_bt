"""L1-H3C HAR-based regime switch between L1-H1 and L1-H2 branches."""
from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.hypotheses.l1_h1 import RollingPercentileGate, bars_for_30_calendar_days
from bt.indicators.atr import ATR
from bt.indicators.ema import EMA
from bt.indicators.har_rv import HarFitRecord, HarRVForecaster
from bt.indicators.vwap import SessionVWAP
from bt.strategy import register_strategy
from bt.strategy.base import Strategy
from bt.logging.decision_trace import make_decision_trace


@dataclass
class _State:
    ema_fast: EMA
    ema_slow: EMA
    atr_signal: ATR
    signal_vwap: SessionVWAP
    base_vwap: SessionVWAP
    rv_gate: RollingPercentileGate
    rv_forecaster: HarRVForecaster
    position: Side | None = None
    active_branch: str | None = None
    entry_signal_ts: pd.Timestamp | None = None
    entry_execution_ts: pd.Timestamp | None = None
    rv_hat_entry: float | None = None
    stop_distance_frozen: float | None = None
    stop_price_frozen: float | None = None
    signal_bars_held: int = 0
    last_5m_ts: pd.Timestamp | None = None
    last_15m_ts: pd.Timestamp | None = None
    regime_label: str = "neutral"
    regime_branch: str | None = None
    regime_fit_ts_used: str | None = None
    regime_rv_hat_t: float | None = None
    regime_rvhat_pct_t: float | None = None
    regime_rv_payload: dict[str, Any] | None = None


@register_strategy("l1_h3c_har_regime_switch")
class L1H3CHarRegimeSwitchStrategy(Strategy):
    def __init__(
        self,
        *,
        trend_timeframe: str = "15m",
        reversion_timeframe: str = "5m",
        q_low: float = 0.3,
        q_high: float = 0.7,
        fit_window_days: int = 180,
        z0: float = 0.8,
        k: float = 1.5,
        T_hold_trend: int = 24,
        T_hold_reversion: int = 12,
        max_concurrent_positions: int = 5,
        no_pyramiding: bool = True,
        disallow_flip: bool = True,
    ) -> None:
        self._trend_timeframe = trend_timeframe
        self._reversion_timeframe = reversion_timeframe
        self._q_low = float(q_low)
        self._q_high = float(q_high)
        self._fit_window_days = int(fit_window_days)
        self._z0 = float(z0)
        self._k = float(k)
        self._t_hold_trend = int(T_hold_trend)
        self._t_hold_reversion = int(T_hold_reversion)
        self._max_concurrent_positions = int(max_concurrent_positions)
        self._no_pyramiding = bool(no_pyramiding)
        self._disallow_flip = bool(disallow_flip)
        self._state: dict[str, _State] = {}

    def _state_for(self, symbol: str) -> _State:
        if symbol not in self._state:
            self._state[symbol] = _State(
                ema_fast=EMA(20),
                ema_slow=EMA(50),
                atr_signal=ATR(14),
                signal_vwap=SessionVWAP(session="utc_day", price_source="typical"),
                base_vwap=SessionVWAP(session="utc_day", price_source="typical"),
                rv_gate=RollingPercentileGate(bars_for_30_calendar_days(self._trend_timeframe), include_current=True),
                rv_forecaster=HarRVForecaster(timeframe=self._trend_timeframe, fit_window_days=self._fit_window_days),
                regime_rv_payload={"rv1_t": None, "rv_d": None, "rv_w": None, "rv_m": None},
            )
        return self._state[symbol]

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
            if value.lower() == "buy":
                return Side.BUY
            if value.lower() == "sell":
                return Side.SELL
        return None

    @staticmethod
    def _position_count(ctx: Mapping[str, Any]) -> int:
        positions = ctx.get("positions")
        if not isinstance(positions, Mapping):
            return 0
        return sum(1 for payload in positions.values() if isinstance(payload, Mapping) and payload.get("side"))

    @staticmethod
    def _clear_position_state(st: _State) -> None:
        st.position = None
        st.active_branch = None
        st.entry_signal_ts = None
        st.entry_execution_ts = None
        st.rv_hat_entry = None
        st.stop_distance_frozen = None
        st.stop_price_frozen = None
        st.signal_bars_held = 0

    def _fit_history_payload(self) -> dict[str, list[dict[str, Any]]]:
        payload: dict[str, list[dict[str, Any]]] = {}
        for symbol, st in self._state.items():
            payload[symbol] = [
                {
                    "fit_ts": str(row.fit_ts),
                    "fit_window_days": row.fit_window_days,
                    "train_start_ts": str(row.train_start_ts),
                    "train_end_ts": str(row.train_end_ts),
                    "n_obs": row.n_obs,
                    "a": row.a,
                    "b": row.b,
                    "c": row.c,
                    "d": row.d,
                }
                for row in st.rv_forecaster.fit_history
            ]
        return payload

    def strategy_artifacts(self) -> dict[str, Any]:
        symbols_payload = self._fit_history_payload()
        return {
            "har_coefficients": {
                "feature_windows": {"d_days": 1, "w_days": 7, "m_days": 30},
                "signal_basis": self._trend_timeframe,
                "fit_window_days": self._fit_window_days,
                "refit_cadence": "daily_on_completed_signal_day",
                "rows": symbols_payload,
            },
            "har_split_manifest": {
                "signal_basis": self._trend_timeframe,
                "fit_window_days": self._fit_window_days,
                "refit_cadence": "daily_on_completed_signal_day",
                "walk_forward": "rolling_window_past_only",
                "rows": symbols_payload,
            },
        }

    def _update_regime(self, st: _State, signal_bar_15m: Any) -> None:
        signal_bar = Bar(
            ts=signal_bar_15m.ts,
            symbol=signal_bar_15m.symbol,
            open=float(signal_bar_15m.open),
            high=float(signal_bar_15m.high),
            low=float(signal_bar_15m.low),
            close=float(signal_bar_15m.close),
            volume=float(signal_bar_15m.volume),
        )
        st.ema_fast.update(signal_bar)
        st.ema_slow.update(signal_bar)
        st.last_15m_ts = signal_bar.ts

        rv_payload = st.rv_forecaster.update(signal_bar.ts, float(signal_bar.close))
        rv_hat_t = rv_payload["rv_hat_t"]
        rvhat_pct_t = st.rv_gate.update(float(rv_hat_t)) if rv_hat_t is not None else None
        st.regime_rv_payload = rv_payload
        st.regime_rv_hat_t = float(rv_hat_t) if rv_hat_t is not None else None
        st.regime_rvhat_pct_t = float(rvhat_pct_t) if rvhat_pct_t is not None else None
        st.regime_fit_ts_used = rv_payload["fit_ts_used"]

        if rvhat_pct_t is None:
            st.regime_label = "neutral"
            st.regime_branch = None
            return
        if rvhat_pct_t >= self._q_high:
            st.regime_label = "high_vol_trend"
            st.regime_branch = "L1-H1"
            return
        if rvhat_pct_t <= self._q_low:
            st.regime_label = "low_vol_reversion"
            st.regime_branch = "L1-H2"
            return
        st.regime_label = "neutral"
        st.regime_branch = None

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        signals: list[Signal] = []
        htf_root = ctx.get("htf") if isinstance(ctx, Mapping) else None
        if not isinstance(htf_root, Mapping):
            raise RuntimeError("L1-H3C requires ctx['htf'] for branch-aware two-clock semantics.")

        htf_5m = htf_root.get(self._reversion_timeframe)
        htf_15m = htf_root.get(self._trend_timeframe)
        if htf_5m is None:
            htf_5m = {}
        if htf_15m is None:
            htf_15m = {}
        if not isinstance(htf_5m, Mapping) or not isinstance(htf_15m, Mapping):
            raise RuntimeError("L1-H3C requires mapping htf contexts for both 5m and 15m clocks.")

        for symbol in sorted(tradeable):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            st = self._state_for(symbol)
            st.base_vwap.update(bar)

            signal_bar_5m = htf_5m.get(symbol)
            signal_bar_15m = htf_15m.get(symbol)
            has_new_5m = signal_bar_5m is not None and signal_bar_5m.ts != st.last_5m_ts
            has_new_15m = signal_bar_15m is not None and signal_bar_15m.ts != st.last_15m_ts

            if has_new_5m:
                signal_bar = Bar(
                    ts=signal_bar_5m.ts,
                    symbol=signal_bar_5m.symbol,
                    open=float(signal_bar_5m.open),
                    high=float(signal_bar_5m.high),
                    low=float(signal_bar_5m.low),
                    close=float(signal_bar_5m.close),
                    volume=float(signal_bar_5m.volume),
                )
                st.atr_signal.update(signal_bar)
                st.signal_vwap.update(signal_bar)
                st.last_5m_ts = signal_bar.ts

            if has_new_15m:
                self._update_regime(st, signal_bar_15m)

            atr_v = st.atr_signal.value
            signal_close_5m = signal_bar_5m.close if signal_bar_5m is not None else bar.close
            signal_vwap_t = st.signal_vwap.value
            z_vwap_t = None if atr_v in (None, 0.0) or signal_vwap_t is None else float((signal_close_5m - signal_vwap_t) / atr_v)
            ema_f = st.ema_fast.value
            ema_s = st.ema_slow.value

            current = self._ctx_position_side(ctx, symbol)
            if current is not None:
                st.position = current
                if st.entry_execution_ts is None:
                    st.entry_execution_ts = ts

                if st.active_branch == "L1-H1" and has_new_15m:
                    st.signal_bars_held += 1
                    if st.signal_bars_held >= self._t_hold_trend:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL if current == Side.BUY else Side.BUY, signal_type="l1_h3c_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="har_regime_switch_entry",
                            setup_class="har_regime_switch",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h3c_har_regime_switch"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),"close_only": True, "exit_reason": "time_stop", "signal_bars_held": st.signal_bars_held, "hold_time_unit": "signal_bars", "signal_timeframe": self._trend_timeframe, "branch_selected": "L1-H1"}))
                        self._clear_position_state(st)
                        continue
                elif st.active_branch == "L1-H2" and has_new_5m:
                    st.signal_bars_held += 1
                    if st.signal_bars_held >= self._t_hold_reversion:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL if current == Side.BUY else Side.BUY, signal_type="l1_h3c_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="har_regime_switch_entry",
                            setup_class="har_regime_switch",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h3c_har_regime_switch"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),"close_only": True, "exit_reason": "time_stop", "signal_bars_held": st.signal_bars_held, "hold_time_unit": "signal_bars", "signal_timeframe": self._reversion_timeframe, "branch_selected": "L1-H2"}))
                        self._clear_position_state(st)
                        continue

                if st.stop_price_frozen is not None:
                    if current == Side.BUY and bar.low <= st.stop_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h3c_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="har_regime_switch_entry",
                            setup_class="har_regime_switch",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h3c_har_regime_switch"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),"close_only": True, "exit_reason": "rvhat_stop", "stop_price": st.stop_price_frozen, "stop_distance": st.stop_distance_frozen, "rv_hat_entry": st.rv_hat_entry, "exit_monitoring_timeframe": "1m", "branch_selected": st.active_branch}))
                        self._clear_position_state(st)
                        continue
                    if current == Side.SELL and bar.high >= st.stop_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h3c_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="har_regime_switch_entry",
                            setup_class="har_regime_switch",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h3c_har_regime_switch"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),"close_only": True, "exit_reason": "rvhat_stop", "stop_price": st.stop_price_frozen, "stop_distance": st.stop_distance_frozen, "rv_hat_entry": st.rv_hat_entry, "exit_monitoring_timeframe": "1m", "branch_selected": st.active_branch}))
                        self._clear_position_state(st)
                        continue

                if st.active_branch == "L1-H2":
                    active_base_vwap = st.base_vwap.value
                    if active_base_vwap is not None:
                        if current == Side.BUY and bar.close >= active_base_vwap:
                            signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h3c_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="har_regime_switch_entry",
                            setup_class="har_regime_switch",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h3c_har_regime_switch"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),"close_only": True, "exit_reason": "vwap_touch", "vwap_t": active_base_vwap, "vwap_mode": "session", "exit_monitoring_timeframe": "1m", "branch_selected": "L1-H2"}))
                            self._clear_position_state(st)
                            continue
                        if current == Side.SELL and bar.close <= active_base_vwap:
                            signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h3c_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="har_regime_switch_entry",
                            setup_class="har_regime_switch",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h3c_har_regime_switch"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),"close_only": True, "exit_reason": "vwap_touch", "vwap_t": active_base_vwap, "vwap_mode": "session", "exit_monitoring_timeframe": "1m", "branch_selected": "L1-H2"}))
                            self._clear_position_state(st)
                            continue
                continue

            self._clear_position_state(st)
            if self._position_count(ctx) >= self._max_concurrent_positions:
                continue
            rv_payload = st.regime_rv_payload or {"rv1_t": None, "rv_d": None, "rv_w": None, "rv_m": None}
            rv_hat_t = st.regime_rv_hat_t
            rvhat_pct_t = st.regime_rvhat_pct_t
            branch_selected = st.regime_branch

            if branch_selected == "L1-H1":
                if not has_new_15m or ema_f is None or ema_s is None or rv_hat_t is None or rvhat_pct_t is None:
                    continue
                trend_dir_t = 1 if ema_f > ema_s else -1 if ema_f < ema_s else 0
                if trend_dir_t == 0:
                    continue
                side = Side.BUY if trend_dir_t > 0 else Side.SELL
                if self._disallow_flip and current is not None and ((current == Side.BUY and side == Side.SELL) or (current == Side.SELL and side == Side.BUY)):
                    continue
                if self._no_pyramiding and current is not None:
                    continue

                signal_close = float(signal_bar_15m.close)
                stop_distance = self._k * signal_close * sqrt(float(rv_hat_t))
                stop_price = signal_close - stop_distance if side == Side.BUY else signal_close + stop_distance
                st.active_branch = "L1-H1"
                st.entry_signal_ts = signal_bar_15m.ts
                st.entry_execution_ts = None
                st.rv_hat_entry = float(rv_hat_t)
                st.stop_distance_frozen = float(stop_distance)
                st.stop_price_frozen = float(stop_price)
                st.signal_bars_held = 0

                fit_snapshot: HarFitRecord | None = st.rv_forecaster.fit_history[-1] if st.rv_forecaster.fit_history else None
                signals.append(Signal(ts=ts, symbol=symbol, side=side, signal_type="l1_h3c_har_regime_switch", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="har_regime_switch_entry",
                            setup_class="har_regime_switch",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h3c_har_regime_switch"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                    "strategy": "l1_h3c_har_regime_switch",
                    "base_data_frequency_expected": "1m",
                    "exit_monitoring_timeframe": "1m",
                    "strategy_family": "regime_switch",
                    "allocation_signal_basis": self._trend_timeframe,
                    "trend_signal_timeframe": self._trend_timeframe,
                    "reversion_signal_timeframe": self._reversion_timeframe,
                    "branch_selected": "L1-H1",
                    "regime_label": st.regime_label,
                    "hold_time_unit": "signal_bars",
                    "signal_timeframe": self._trend_timeframe,
                    "stop_model": "fixed_close_sqrt_rvhat_multiple",
                    "stop_update_policy": "frozen_at_entry",
                    "gate_model": "har_rv_percentile_switch",
                    "fit_window_days": self._fit_window_days,
                    "fit_ts_used": st.regime_fit_ts_used,
                    "q_low": self._q_low,
                    "q_high": self._q_high,
                    "k": self._k,
                    "trend_dir_t": trend_dir_t,
                    "rv1_t": rv_payload.get("rv1_t"),
                    "rv_d": rv_payload.get("rv_d"),
                    "rv_w": rv_payload.get("rv_w"),
                    "rv_m": rv_payload.get("rv_m"),
                    "RV_hat_t": rv_hat_t,
                    "rvhat_pct_t": rvhat_pct_t,
                    "entry_signal_ts": str(signal_bar_15m.ts),
                    "rv_hat_entry": st.rv_hat_entry,
                    "stop_distance": st.stop_distance_frozen,
                    "stop_price": st.stop_price_frozen,
                    "fit_a": fit_snapshot.a if fit_snapshot is not None else None,
                    "fit_b": fit_snapshot.b if fit_snapshot is not None else None,
                    "fit_c": fit_snapshot.c if fit_snapshot is not None else None,
                    "fit_d": fit_snapshot.d if fit_snapshot is not None else None,
                    "risk_amount": ctx.get("equity", None),
                }))
                continue

            if branch_selected == "L1-H2":
                if not has_new_5m or atr_v is None or signal_vwap_t is None or z_vwap_t is None or rv_hat_t is None or rvhat_pct_t is None:
                    continue
                side: Side | None = None
                entry_reason = ""
                if z_vwap_t <= -self._z0:
                    side = Side.BUY
                    entry_reason = "har_lowvol_fade_long"
                elif z_vwap_t >= self._z0:
                    side = Side.SELL
                    entry_reason = "har_lowvol_fade_short"
                if side is None:
                    continue
                if self._no_pyramiding and current is not None:
                    continue

                signal_close = float(signal_bar_5m.close)
                stop_distance = self._k * signal_close * sqrt(float(rv_hat_t))
                stop_price = signal_close - stop_distance if side == Side.BUY else signal_close + stop_distance
                st.active_branch = "L1-H2"
                st.entry_signal_ts = signal_bar_5m.ts
                st.entry_execution_ts = None
                st.rv_hat_entry = float(rv_hat_t)
                st.stop_distance_frozen = float(stop_distance)
                st.stop_price_frozen = float(stop_price)
                st.signal_bars_held = 0

                fit_snapshot = st.rv_forecaster.fit_history[-1] if st.rv_forecaster.fit_history else None
                signals.append(Signal(ts=ts, symbol=symbol, side=side, signal_type="l1_h3c_har_regime_switch", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="har_regime_switch_entry",
                            setup_class="har_regime_switch",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h3c_har_regime_switch"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                    "strategy": "l1_h3c_har_regime_switch",
                    "base_data_frequency_expected": "1m",
                    "exit_monitoring_timeframe": "1m",
                    "strategy_family": "regime_switch",
                    "allocation_signal_basis": self._trend_timeframe,
                    "trend_signal_timeframe": self._trend_timeframe,
                    "reversion_signal_timeframe": self._reversion_timeframe,
                    "branch_selected": "L1-H2",
                    "regime_label": st.regime_label,
                    "hold_time_unit": "signal_bars",
                    "signal_timeframe": self._reversion_timeframe,
                    "stop_model": "fixed_close_sqrt_rvhat_multiple",
                    "stop_update_policy": "frozen_at_entry",
                    "profit_exit_model": "vwap_touch",
                    "vwap_mode": "session",
                    "gate_model": "har_rv_percentile_switch",
                    "fit_window_days": self._fit_window_days,
                    "fit_ts_used": st.regime_fit_ts_used,
                    "q_low": self._q_low,
                    "q_high": self._q_high,
                    "k": self._k,
                    "session_vwap_t": signal_vwap_t,
                    "z_vwap_t": z_vwap_t,
                    "entry_reason": entry_reason,
                    "rv1_t": rv_payload.get("rv1_t"),
                    "rv_d": rv_payload.get("rv_d"),
                    "rv_w": rv_payload.get("rv_w"),
                    "rv_m": rv_payload.get("rv_m"),
                    "RV_hat_t": rv_hat_t,
                    "rvhat_pct_t": rvhat_pct_t,
                    "entry_signal_ts": str(signal_bar_5m.ts),
                    "rv_hat_entry": st.rv_hat_entry,
                    "stop_distance": st.stop_distance_frozen,
                    "stop_price": st.stop_price_frozen,
                    "fit_a": fit_snapshot.a if fit_snapshot is not None else None,
                    "fit_b": fit_snapshot.b if fit_snapshot is not None else None,
                    "fit_c": fit_snapshot.c if fit_snapshot is not None else None,
                    "fit_d": fit_snapshot.d if fit_snapshot is not None else None,
                    "risk_amount": ctx.get("equity", None),
                }))
        return signals
