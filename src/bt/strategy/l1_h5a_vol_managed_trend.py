"""L1-H5A volatility-managed sizing overlay on L1-H1 trend continuation."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import math

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.hypotheses.l1_h5a import (
    RollingMedianReference,
    RollingRmsVolatility,
    clipped_inverse_vol_scale,
    vol_window_bars,
)
from bt.indicators.atr import ATR
from bt.indicators.ema import EMA
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


@dataclass
class _State:
    atr: ATR
    ema_fast: EMA
    ema_slow: EMA
    sigma_estimator: RollingRmsVolatility
    sigma_reference: RollingMedianReference
    position: Side | None = None
    entry_price: float | None = None
    entry_signal_ts: pd.Timestamp | None = None
    entry_execution_ts: pd.Timestamp | None = None
    atr_entry: float | None = None
    stop_distance_frozen: float | None = None
    stop_price_frozen: float | None = None
    signal_bars_held: int = 0
    last_signal_ts: pd.Timestamp | None = None
    previous_signal_close: float | None = None


@register_strategy("l1_h5a_vol_managed_trend")
class L1H5AVolManagedTrendStrategy(Strategy):
    def __init__(
        self,
        *,
        timeframe: str = "15m",
        k_atr: float = 2.0,
        T_hold: int = 24,
        disallow_flip: bool = True,
        vol_window_hours: int = 24,
        sigma_reference_window_hours: int = 72,
        s_min: float = 0.25,
        s_max: float = 1.5,
    ) -> None:
        self._timeframe = timeframe
        self._k_atr = float(k_atr)
        self._t_hold = int(T_hold)
        self._disallow_flip = bool(disallow_flip)
        self._vol_window_hours = int(vol_window_hours)
        self._sigma_reference_window_hours = int(sigma_reference_window_hours)
        self._s_min = float(s_min)
        self._s_max = float(s_max)
        self._state: dict[str, _State] = {}

    def _state_for(self, symbol: str) -> _State:
        if symbol not in self._state:
            self._state[symbol] = _State(
                atr=ATR(14),
                ema_fast=EMA(20),
                ema_slow=EMA(50),
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
    def _clear_position_state(st: _State) -> None:
        st.position = None
        st.entry_price = None
        st.entry_signal_ts = None
        st.entry_execution_ts = None
        st.atr_entry = None
        st.stop_distance_frozen = None
        st.stop_price_frozen = None
        st.signal_bars_held = 0

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

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        signals: list[Signal] = []
        htf_root = ctx.get("htf") if isinstance(ctx, Mapping) else None
        if not isinstance(htf_root, Mapping):
            raise RuntimeError(f"L1-H5A requires ctx['htf']['{self._timeframe}'] for two-clock semantics.")
        htf_for_tf = htf_root.get(self._timeframe)
        if htf_for_tf is None:
            htf_for_tf = {}
        if not isinstance(htf_for_tf, Mapping):
            raise RuntimeError(f"L1-H5A requires mapping ctx['htf']['{self._timeframe}'] for two-clock semantics.")

        for symbol in sorted(tradeable):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            st = self._state_for(symbol)
            signal_bar = htf_for_tf.get(symbol)
            has_new_signal_bar = signal_bar is not None and signal_bar.ts != st.last_signal_ts

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
                st.atr.update(signal_bar_as_base)
                st.ema_fast.update(signal_bar_as_base)
                st.ema_slow.update(signal_bar_as_base)

                log_return = None
                if st.previous_signal_close is not None and st.previous_signal_close > 0 and signal_bar.close > 0:
                    log_return = float(math.log(float(signal_bar.close) / float(st.previous_signal_close)))
                sigma_t = st.sigma_estimator.update(log_return)
                sigma_star = st.sigma_reference.update(sigma_t)
                scale_factor_t, cap_hit_lower, cap_hit_upper = clipped_inverse_vol_scale(
                    sigma_t=sigma_t,
                    sigma_star=sigma_star,
                    s_min=self._s_min,
                    s_max=self._s_max,
                )

                st.previous_signal_close = float(signal_bar.close)
                st.last_signal_ts = signal_bar.ts

            atr_v = st.atr.value
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
                    signals.append(
                        Signal(
                            ts=ts,
                            symbol=symbol,
                            side=Side.SELL if current == Side.BUY else Side.BUY,
                            signal_type="l1_h5a_exit",
                            confidence=1.0,
                            metadata={
                                "close_only": True,
                                "exit_reason": "time_stop",
                                "signal_bars_held": st.signal_bars_held,
                                "hold_time_unit": "signal_bars",
                            },
                        )
                    )
                    self._clear_position_state(st)
                    continue

                if st.stop_price_frozen is not None:
                    if current == Side.BUY and bar.low <= st.stop_price_frozen:
                        signals.append(
                            Signal(
                                ts=ts,
                                symbol=symbol,
                                side=Side.SELL,
                                signal_type="l1_h5a_exit",
                                confidence=1.0,
                                metadata={
                                    "close_only": True,
                                    "exit_reason": "atr_stop",
                                    "stop_price": st.stop_price_frozen,
                                    "stop_distance": st.stop_distance_frozen,
                                    "atr_entry": st.atr_entry,
                                    "exit_monitoring_timeframe": "1m",
                                },
                            )
                        )
                        self._clear_position_state(st)
                        continue
                    if current == Side.SELL and bar.high >= st.stop_price_frozen:
                        signals.append(
                            Signal(
                                ts=ts,
                                symbol=symbol,
                                side=Side.BUY,
                                signal_type="l1_h5a_exit",
                                confidence=1.0,
                                metadata={
                                    "close_only": True,
                                    "exit_reason": "atr_stop",
                                    "stop_price": st.stop_price_frozen,
                                    "stop_distance": st.stop_distance_frozen,
                                    "atr_entry": st.atr_entry,
                                    "exit_monitoring_timeframe": "1m",
                                },
                            )
                        )
                        self._clear_position_state(st)
                        continue
                continue

            self._clear_position_state(st)
            if not has_new_signal_bar:
                continue
            if ema_f is None or ema_s is None or atr_v is None:
                continue
            if scale_factor_t is None:
                continue
            trend_dir_t = 1 if ema_f > ema_s else -1 if ema_f < ema_s else 0
            if trend_dir_t == 0:
                continue
            side = Side.BUY if trend_dir_t > 0 else Side.SELL
            if self._disallow_flip and current is not None and (
                (current == Side.BUY and side == Side.SELL) or (current == Side.SELL and side == Side.BUY)
            ):
                continue

            stop_distance = self._k_atr * atr_v
            if stop_distance <= 0:
                continue
            stop_price = bar.close - stop_distance if side == Side.BUY else bar.close + stop_distance

            st.entry_price = float(bar.close)
            st.entry_signal_ts = signal_bar.ts
            st.entry_execution_ts = None
            st.atr_entry = float(atr_v)
            st.stop_distance_frozen = float(stop_distance)
            st.stop_price_frozen = float(stop_price)
            st.signal_bars_held = 0

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

            signals.append(
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=side,
                    signal_type="l1_h5a_vol_managed_trend",
                    confidence=1.0,
                    metadata={
                        "strategy": "l1_h5a_vol_managed_trend",
                        "signal_timeframe": self._timeframe,
                        "exit_monitoring_timeframe": "1m",
                        "base_data_frequency_expected": "1m",
                        "hold_time_unit": "signal_bars",
                        "atr_source_timeframe": "signal_timeframe",
                        "stop_model": "fixed_atr_multiple",
                        "stop_update_policy": "frozen_at_entry",
                        "tp_update_policy": "none",
                        "no_pyramiding": True,
                        "trend_dir_t": trend_dir_t,
                        "entry_signal_ts": str(signal_bar.ts),
                        "atr_entry": st.atr_entry,
                        "stop_distance": st.stop_distance_frozen,
                        "stop_price": st.stop_price_frozen,
                        "vol_window_hours": self._vol_window_hours,
                        "sigma_reference_window_hours": self._sigma_reference_window_hours,
                        "sigma_t": sigma_t,
                        "sigma_star": sigma_star,
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
