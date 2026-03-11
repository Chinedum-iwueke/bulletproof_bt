"""L1-H3B HAR-gated mean-reversion strategy (L1-H2 directional baseline)."""
from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.hypotheses.l1_h1 import RollingPercentileGate, bars_for_30_calendar_days
from bt.indicators.atr import ATR
from bt.indicators.har_rv import HarFitRecord, HarRVForecaster
from bt.indicators.vwap import SessionVWAP
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


@dataclass
class _State:
    atr_signal: ATR
    signal_vwap: SessionVWAP
    base_vwap: SessionVWAP
    rv_gate: RollingPercentileGate
    rv_forecaster: HarRVForecaster
    position: Side | None = None
    entry_signal_ts: pd.Timestamp | None = None
    entry_execution_ts: pd.Timestamp | None = None
    rv_hat_entry: float | None = None
    stop_distance_frozen: float | None = None
    stop_price_frozen: float | None = None
    signal_bars_held: int = 0
    last_signal_ts: pd.Timestamp | None = None


@register_strategy("l1_h3b_har_rv_gate_mean_reversion")
class L1H3BHarRVGateMeanReversionStrategy(Strategy):
    def __init__(
        self,
        *,
        timeframe: str = "5m",
        gate_quantile_low: float = 0.3,
        fit_window_days: int = 180,
        z0: float = 0.8,
        k: float = 1.5,
        T_hold: int = 12,
        max_concurrent_positions: int = 5,
        no_pyramiding: bool = True,
    ) -> None:
        self._timeframe = timeframe
        self._gate_quantile_low = float(gate_quantile_low)
        self._fit_window_days = int(fit_window_days)
        self._z0 = float(z0)
        self._k = float(k)
        self._t_hold = int(T_hold)
        self._max_concurrent_positions = int(max_concurrent_positions)
        self._no_pyramiding = bool(no_pyramiding)
        self._state: dict[str, _State] = {}

    def _state_for(self, symbol: str) -> _State:
        if symbol not in self._state:
            self._state[symbol] = _State(
                atr_signal=ATR(14),
                signal_vwap=SessionVWAP(session="utc_day", price_source="typical"),
                base_vwap=SessionVWAP(session="utc_day", price_source="typical"),
                rv_gate=RollingPercentileGate(bars_for_30_calendar_days(self._timeframe), include_current=True),
                rv_forecaster=HarRVForecaster(timeframe=self._timeframe, fit_window_days=self._fit_window_days),
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
                "signal_timeframe": self._timeframe,
                "fit_window_days": self._fit_window_days,
                "refit_cadence": "daily_on_completed_signal_day",
                "rows": symbols_payload,
            },
            "har_split_manifest": {
                "signal_timeframe": self._timeframe,
                "bars_per_day": 288 if self._timeframe == "5m" else None,
                "fit_window_days": self._fit_window_days,
                "refit_cadence": "daily_on_completed_signal_day",
                "walk_forward": "rolling_window_past_only",
                "rows": symbols_payload,
            },
        }

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        signals: list[Signal] = []
        htf_root = ctx.get("htf") if isinstance(ctx, Mapping) else None
        if not isinstance(htf_root, Mapping):
            raise RuntimeError(f"L1-H3B requires ctx['htf']['{self._timeframe}'] for two-clock semantics.")
        htf_for_tf = htf_root.get(self._timeframe)
        if htf_for_tf is None:
            htf_for_tf = {}
        if not isinstance(htf_for_tf, Mapping):
            raise RuntimeError(f"L1-H3B requires mapping ctx['htf']['{self._timeframe}'] for two-clock semantics.")

        for symbol in sorted(tradeable):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            st = self._state_for(symbol)
            st.base_vwap.update(bar)

            signal_bar = htf_for_tf.get(symbol)
            has_new_signal_bar = signal_bar is not None and signal_bar.ts != st.last_signal_ts

            rv_payload: dict[str, Any] = {
                "rv1_t": None,
                "rv_d": None,
                "rv_w": None,
                "rv_m": None,
                "rv_hat_t": None,
                "fit_ts_used": None,
            }
            rvhat_pct_t = None
            if has_new_signal_bar:
                signal_bar_as_base = Bar(
                    ts=signal_bar.ts,
                    symbol=signal_bar.symbol,
                    open=float(signal_bar.open),
                    high=float(signal_bar.high),
                    low=float(signal_bar.low),
                    close=float(signal_bar.close),
                    volume=float(signal_bar.volume),
                )
                st.atr_signal.update(signal_bar_as_base)
                st.signal_vwap.update(signal_bar_as_base)
                st.last_signal_ts = signal_bar.ts

                rv_payload = st.rv_forecaster.update(signal_bar.ts, float(signal_bar.close))
                rv_hat_t = rv_payload["rv_hat_t"]
                rvhat_pct_t = st.rv_gate.update(float(rv_hat_t)) if rv_hat_t is not None else None

            atr_v = st.atr_signal.value
            signal_close = signal_bar.close if signal_bar is not None else bar.close
            signal_vwap_t = st.signal_vwap.value
            z_vwap_t = None if atr_v in (None, 0.0) or signal_vwap_t is None else float((signal_close - signal_vwap_t) / atr_v)

            current = self._ctx_position_side(ctx, symbol)
            if current is not None:
                st.position = current
                if st.entry_execution_ts is None:
                    st.entry_execution_ts = ts
                if has_new_signal_bar:
                    st.signal_bars_held += 1
                    if st.signal_bars_held >= self._t_hold:
                        signals.append(
                            Signal(
                                ts=ts,
                                symbol=symbol,
                                side=Side.SELL if current == Side.BUY else Side.BUY,
                                signal_type="l1_h3b_exit",
                                confidence=1.0,
                                metadata={
                                    "close_only": True,
                                    "exit_reason": "time_stop",
                                    "signal_bars_held": st.signal_bars_held,
                                    "hold_time_unit": "signal_bars",
                                    "signal_timeframe": self._timeframe,
                                },
                            )
                        )
                        self._clear_position_state(st)
                        continue

                if st.stop_price_frozen is not None:
                    if current == Side.BUY and bar.low <= st.stop_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h3b_exit", confidence=1.0, metadata={"close_only": True, "exit_reason": "rvhat_stop", "stop_price": st.stop_price_frozen, "stop_distance": st.stop_distance_frozen, "rv_hat_entry": st.rv_hat_entry, "exit_monitoring_timeframe": "1m"}))
                        self._clear_position_state(st)
                        continue
                    if current == Side.SELL and bar.high >= st.stop_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h3b_exit", confidence=1.0, metadata={"close_only": True, "exit_reason": "rvhat_stop", "stop_price": st.stop_price_frozen, "stop_distance": st.stop_distance_frozen, "rv_hat_entry": st.rv_hat_entry, "exit_monitoring_timeframe": "1m"}))
                        self._clear_position_state(st)
                        continue

                active_base_vwap = st.base_vwap.value
                if active_base_vwap is not None:
                    if current == Side.BUY and bar.close >= active_base_vwap:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h3b_exit", confidence=1.0, metadata={"close_only": True, "exit_reason": "vwap_touch", "vwap_t": active_base_vwap, "vwap_mode": "session", "exit_monitoring_timeframe": "1m"}))
                        self._clear_position_state(st)
                        continue
                    if current == Side.SELL and bar.close <= active_base_vwap:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h3b_exit", confidence=1.0, metadata={"close_only": True, "exit_reason": "vwap_touch", "vwap_t": active_base_vwap, "vwap_mode": "session", "exit_monitoring_timeframe": "1m"}))
                        self._clear_position_state(st)
                        continue
                continue

            self._clear_position_state(st)
            if not has_new_signal_bar:
                continue
            if atr_v is None or signal_vwap_t is None or z_vwap_t is None or rv_payload["rv_hat_t"] is None or rvhat_pct_t is None:
                continue
            if self._position_count(ctx) >= self._max_concurrent_positions:
                continue

            gate_pass = bool(rvhat_pct_t <= self._gate_quantile_low)
            if not gate_pass:
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

            rv_hat = float(rv_payload["rv_hat_t"])
            stop_distance = self._k * float(signal_bar.close) * sqrt(rv_hat)
            stop_price = float(signal_bar.close) - stop_distance if side == Side.BUY else float(signal_bar.close) + stop_distance

            st.entry_signal_ts = signal_bar.ts
            st.entry_execution_ts = None
            st.rv_hat_entry = rv_hat
            st.stop_distance_frozen = float(stop_distance)
            st.stop_price_frozen = float(stop_price)
            st.signal_bars_held = 0

            fit_snapshot: HarFitRecord | None = st.rv_forecaster.fit_history[-1] if st.rv_forecaster.fit_history else None
            signals.append(
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=side,
                    signal_type="l1_h3b_har_rv_gate_mean_reversion",
                    confidence=1.0,
                    metadata={
                        "strategy": "l1_h3b_har_rv_gate_mean_reversion",
                        "timeframe": self._timeframe,
                        "signal_timeframe": self._timeframe,
                        "base_data_frequency_expected": "1m",
                        "exit_monitoring_timeframe": "1m",
                        "hold_time_unit": "signal_bars",
                        "atr_source_timeframe": "signal_timeframe",
                        "stop_model": "fixed_close_sqrt_rvhat_multiple",
                        "stop_update_policy": "frozen_at_entry",
                        "tp_update_policy": "none",
                        "profit_exit_model": "vwap_touch",
                        "vwap_mode": "session",
                        "gate_model": "har_rv_percentile_low",
                        "gate_pass": gate_pass,
                        "rv1_t": rv_payload["rv1_t"],
                        "rv_d": rv_payload["rv_d"],
                        "rv_w": rv_payload["rv_w"],
                        "rv_m": rv_payload["rv_m"],
                        "RV_hat_t": rv_hat,
                        "rvhat_pct_t": rvhat_pct_t,
                        "fit_ts_used": rv_payload["fit_ts_used"],
                        "fit_window_days": self._fit_window_days,
                        "z_vwap_t": z_vwap_t,
                        "session_vwap_t": signal_vwap_t,
                        "entry_reason": entry_reason,
                        "entry_signal_ts": str(signal_bar.ts),
                        "rv_hat_entry": st.rv_hat_entry,
                        "stop_distance": st.stop_distance_frozen,
                        "stop_price": st.stop_price_frozen,
                        "k": self._k,
                        "fit_a": fit_snapshot.a if fit_snapshot is not None else None,
                        "fit_b": fit_snapshot.b if fit_snapshot is not None else None,
                        "fit_c": fit_snapshot.c if fit_snapshot is not None else None,
                        "fit_d": fit_snapshot.d if fit_snapshot is not None else None,
                        "risk_amount": ctx.get("equity", None),
                    },
                )
            )

        return signals
