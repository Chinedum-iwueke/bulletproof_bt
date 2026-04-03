"""L1-H5B volatility-managed exposure overlay on L1-H3A HAR-gated trend."""
from __future__ import annotations

from dataclasses import dataclass
from math import log, sqrt
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.hypotheses.l1_h1 import RollingPercentileGate, bars_for_30_calendar_days
from bt.hypotheses.l1_h5a import (
    RollingMedianReference,
    RollingRmsVolatility,
    clipped_inverse_vol_scale,
    vol_window_bars,
)
from bt.indicators.ema import EMA
from bt.indicators.har_rv import HarFitRecord, HarRVForecaster
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


@dataclass
class _State:
    ema_fast: EMA
    ema_slow: EMA
    rv_gate: RollingPercentileGate
    rv_forecaster: HarRVForecaster
    sigma_estimator: RollingRmsVolatility
    sigma_reference: RollingMedianReference
    position: Side | None = None
    entry_signal_ts: pd.Timestamp | None = None
    entry_execution_ts: pd.Timestamp | None = None
    rv_hat_entry: float | None = None
    rvhat_pct_entry: float | None = None
    sigma_entry: float | None = None
    sigma_star_entry: float | None = None
    size_factor_entry: float | None = None
    qty_r_entry: float | None = None
    qty_final_entry: float | None = None
    cap_hit_lower_entry: bool = False
    cap_hit_upper_entry: bool = False
    stop_distance_frozen: float | None = None
    stop_price_frozen: float | None = None
    signal_bars_held: int = 0
    last_signal_ts: pd.Timestamp | None = None
    previous_signal_close: float | None = None


@register_strategy("l1_h5b_vol_managed_har_trend")
class L1H5BVolManagedHarTrendStrategy(Strategy):
    def __init__(
        self,
        *,
        timeframe: str = "15m",
        gate_quantile: float = 0.7,
        fit_window_days: int = 180,
        k: float = 2.0,
        T_hold: int = 24,
        max_concurrent_positions: int = 5,
        no_pyramiding: bool = True,
        disallow_flip: bool = True,
        vol_window_hours: int = 24,
        sigma_reference_window_hours: int = 72,
        s_min: float = 0.25,
        s_max: float = 1.5,
    ) -> None:
        self._timeframe = timeframe
        self._gate_quantile = float(gate_quantile)
        self._fit_window_days = int(fit_window_days)
        self._k = float(k)
        self._t_hold = int(T_hold)
        self._max_concurrent_positions = int(max_concurrent_positions)
        self._no_pyramiding = bool(no_pyramiding)
        self._disallow_flip = bool(disallow_flip)
        self._vol_window_hours = int(vol_window_hours)
        self._sigma_reference_window_hours = int(sigma_reference_window_hours)
        self._s_min = float(s_min)
        self._s_max = float(s_max)
        self._state: dict[str, _State] = {}

    def _state_for(self, symbol: str) -> _State:
        if symbol not in self._state:
            self._state[symbol] = _State(
                ema_fast=EMA(20),
                ema_slow=EMA(50),
                rv_gate=RollingPercentileGate(bars_for_30_calendar_days(self._timeframe), include_current=True),
                rv_forecaster=HarRVForecaster(timeframe=self._timeframe, fit_window_days=self._fit_window_days),
                sigma_estimator=RollingRmsVolatility(vol_window_bars(timeframe=self._timeframe, hours=self._vol_window_hours)),
                sigma_reference=RollingMedianReference(
                    vol_window_bars(timeframe=self._timeframe, hours=self._sigma_reference_window_hours)
                ),
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
    def _extract_r_per_trade(ctx: Mapping[str, Any]) -> float | None:
        risk_cfg = ctx.get("risk")
        if isinstance(risk_cfg, Mapping) and "r_per_trade" in risk_cfg:
            try:
                return float(risk_cfg["r_per_trade"])
            except (TypeError, ValueError):
                return None
        raw = ctx.get("r_per_trade")
        if raw is None:
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _clear_position_state(st: _State) -> None:
        st.position = None
        st.entry_signal_ts = None
        st.entry_execution_ts = None
        st.rv_hat_entry = None
        st.rvhat_pct_entry = None
        st.sigma_entry = None
        st.sigma_star_entry = None
        st.size_factor_entry = None
        st.qty_r_entry = None
        st.qty_final_entry = None
        st.cap_hit_lower_entry = False
        st.cap_hit_upper_entry = False
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
                "bars_per_day": 96 if self._timeframe == "15m" else None,
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
            raise RuntimeError(f"L1-H5B requires ctx['htf']['{self._timeframe}'] for two-clock semantics.")
        htf_for_tf = htf_root.get(self._timeframe)
        if htf_for_tf is None:
            htf_for_tf = {}
        if not isinstance(htf_for_tf, Mapping):
            raise RuntimeError(f"L1-H5B requires mapping ctx['htf']['{self._timeframe}'] for two-clock semantics.")

        for symbol in sorted(tradeable):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            st = self._state_for(symbol)
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
            sigma_t = None
            sigma_star = None
            scale_factor_t = None
            cap_hit_lower = False
            cap_hit_upper = False

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
                st.ema_fast.update(signal_bar_as_base)
                st.ema_slow.update(signal_bar_as_base)
                st.last_signal_ts = signal_bar.ts

                rv_payload = st.rv_forecaster.update(signal_bar.ts, float(signal_bar.close))
                rv_hat_t = rv_payload["rv_hat_t"]
                rvhat_pct_t = st.rv_gate.update(float(rv_hat_t)) if rv_hat_t is not None else None

                log_return = None
                if st.previous_signal_close is not None and st.previous_signal_close > 0 and signal_bar.close > 0:
                    log_return = float(log(float(signal_bar.close) / float(st.previous_signal_close)))
                sigma_t = st.sigma_estimator.update(log_return)
                sigma_star = st.sigma_reference.update(sigma_t)
                scale_factor_t, cap_hit_lower, cap_hit_upper = clipped_inverse_vol_scale(
                    sigma_t=sigma_t,
                    sigma_star=sigma_star,
                    s_min=self._s_min,
                    s_max=self._s_max,
                )
                st.previous_signal_close = float(signal_bar.close)

            ema_f = st.ema_fast.value
            ema_s = st.ema_slow.value
            current = self._ctx_position_side(ctx, symbol)
            if current is not None:
                st.position = current
                if st.entry_execution_ts is None:
                    st.entry_execution_ts = ts
                if has_new_signal_bar:
                    st.signal_bars_held += 1
                if st.signal_bars_held >= self._t_hold and has_new_signal_bar:
                    signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL if current == Side.BUY else Side.BUY, signal_type="l1_h5b_exit", confidence=1.0, metadata={"close_only": True, "exit_reason": "time_stop", "signal_bars_held": st.signal_bars_held, "hold_time_unit": "signal_bars"}))
                    self._clear_position_state(st)
                    continue

                if st.stop_price_frozen is not None:
                    if current == Side.BUY and bar.low <= st.stop_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h5b_exit", confidence=1.0, metadata={"close_only": True, "exit_reason": "rvhat_stop", "stop_price": st.stop_price_frozen, "stop_distance": st.stop_distance_frozen, "rv_hat_entry": st.rv_hat_entry, "exit_monitoring_timeframe": "1m"}))
                        self._clear_position_state(st)
                        continue
                    if current == Side.SELL and bar.high >= st.stop_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h5b_exit", confidence=1.0, metadata={"close_only": True, "exit_reason": "rvhat_stop", "stop_price": st.stop_price_frozen, "stop_distance": st.stop_distance_frozen, "rv_hat_entry": st.rv_hat_entry, "exit_monitoring_timeframe": "1m"}))
                        self._clear_position_state(st)
                        continue
                continue

            self._clear_position_state(st)
            if not has_new_signal_bar:
                continue
            if ema_f is None or ema_s is None or rv_payload["rv_hat_t"] is None or rvhat_pct_t is None:
                continue
            if scale_factor_t is None:
                continue
            if self._position_count(ctx) >= self._max_concurrent_positions:
                continue

            trend_dir_t = 1 if ema_f > ema_s else -1 if ema_f < ema_s else 0
            gate_pass = bool(rvhat_pct_t >= self._gate_quantile)
            if trend_dir_t == 0 or not gate_pass:
                continue
            side = Side.BUY if trend_dir_t > 0 else Side.SELL
            entry_reason = "vol_managed_har_trend_long" if side == Side.BUY else "vol_managed_har_trend_short"
            if self._disallow_flip and current is not None and ((current == Side.BUY and side == Side.SELL) or (current == Side.SELL and side == Side.BUY)):
                continue
            if self._no_pyramiding and current is not None:
                continue

            rv_hat = float(rv_payload["rv_hat_t"])
            stop_distance = self._k * float(signal_bar.close) * sqrt(rv_hat)
            if stop_distance <= 0:
                continue
            stop_price = float(signal_bar.close) - stop_distance if side == Side.BUY else float(signal_bar.close) + stop_distance

            equity_t = ctx.get("equity") if isinstance(ctx, Mapping) else None
            r_per_trade = self._extract_r_per_trade(ctx)
            qty_r = None
            qty_final = None
            try:
                if equity_t is not None and r_per_trade is not None:
                    qty_r = (float(equity_t) * float(r_per_trade)) / float(stop_distance)
                    qty_final = float(qty_r) * float(scale_factor_t)
            except (TypeError, ValueError, ZeroDivisionError):
                qty_r = None
                qty_final = None

            st.entry_signal_ts = signal_bar.ts
            st.entry_execution_ts = None
            st.rv_hat_entry = rv_hat
            st.rvhat_pct_entry = float(rvhat_pct_t)
            st.sigma_entry = sigma_t
            st.sigma_star_entry = sigma_star
            st.size_factor_entry = scale_factor_t
            st.qty_r_entry = qty_r
            st.qty_final_entry = qty_final
            st.cap_hit_lower_entry = cap_hit_lower
            st.cap_hit_upper_entry = cap_hit_upper
            st.stop_distance_frozen = float(stop_distance)
            st.stop_price_frozen = float(stop_price)
            st.signal_bars_held = 0

            fit_snapshot: HarFitRecord | None = st.rv_forecaster.fit_history[-1] if st.rv_forecaster.fit_history else None
            signals.append(
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=side,
                    signal_type="l1_h5b_vol_managed_har_trend",
                    confidence=1.0,
                    metadata={
                        "strategy": "l1_h5b_vol_managed_har_trend",
                        "timeframe": self._timeframe,
                        "signal_timeframe": self._timeframe,
                        "base_data_frequency_expected": "1m",
                        "exit_monitoring_timeframe": "1m",
                        "hold_time_unit": "signal_bars",
                        "stop_model": "fixed_close_sqrt_rvhat_multiple",
                        "stop_update_policy": "frozen_at_entry",
                        "tp_update_policy": "none",
                        "gate_model": "har_rv_percentile",
                        "trend_dir_t": trend_dir_t,
                        "entry_reason": entry_reason,
                        "gate_pass": gate_pass,
                        "rv1_t": rv_payload["rv1_t"],
                        "rv_d": rv_payload["rv_d"],
                        "rv_w": rv_payload["rv_w"],
                        "rv_m": rv_payload["rv_m"],
                        "RV_hat_t": rv_hat,
                        "rvhat_pct_t": rvhat_pct_t,
                        "fit_ts_used": rv_payload["fit_ts_used"],
                        "fit_window_days": self._fit_window_days,
                        "entry_signal_ts": str(signal_bar.ts),
                        "rv_hat_entry": st.rv_hat_entry,
                        "rvhat_pct_entry": st.rvhat_pct_entry,
                        "stop_distance": st.stop_distance_frozen,
                        "stop_price": st.stop_price_frozen,
                        "k": self._k,
                        "fit_a": fit_snapshot.a if fit_snapshot is not None else None,
                        "fit_b": fit_snapshot.b if fit_snapshot is not None else None,
                        "fit_c": fit_snapshot.c if fit_snapshot is not None else None,
                        "fit_d": fit_snapshot.d if fit_snapshot is not None else None,
                        "risk_amount": ctx.get("equity", None),
                        "vol_window_hours": self._vol_window_hours,
                        "sigma_reference_window_hours": self._sigma_reference_window_hours,
                        "sigma_t": sigma_t,
                        "sigma_star": sigma_star,
                        "s_t": scale_factor_t,
                        "size_factor_t": scale_factor_t,
                        "size_factor_min": self._s_min,
                        "size_factor_max": self._s_max,
                        "qty_R": qty_r,
                        "qty_final": qty_final,
                        "cap_hit_lower": cap_hit_lower,
                        "cap_hit_upper": cap_hit_upper,
                    },
                )
            )
        return signals
