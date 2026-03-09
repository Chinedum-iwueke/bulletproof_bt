"""L1-H1 Volatility Floor Gates Trend Continuation strategy."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.hypotheses.l1_h1 import RollingPercentileGate, bars_for_30_calendar_days
from bt.indicators.atr import ATR
from bt.indicators.ema import EMA
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


@dataclass
class _State:
    atr: ATR
    ema_fast: EMA
    ema_slow: EMA
    gate: RollingPercentileGate
    position: Side | None = None
    entry_price: float | None = None
    entry_signal_ts: pd.Timestamp | None = None
    entry_execution_ts: pd.Timestamp | None = None
    atr_entry: float | None = None
    stop_distance_frozen: float | None = None
    stop_price_frozen: float | None = None
    tp_distance_frozen: float | None = None
    tp_price_frozen: float | None = None
    signal_bars_held: int = 0
    last_signal_ts: pd.Timestamp | None = None


@register_strategy("l1_h1_vol_floor_trend")
class L1H1VolFloorTrendStrategy(Strategy):
    def __init__(
        self,
        *,
        timeframe: str = "15m",
        theta_vol: float = 0.7,
        k_atr: float = 2.0,
        T_hold: int = 24,
        tp_enabled: bool = False,
        m_atr: float = 2.0,
        disallow_flip: bool = True,
    ) -> None:
        self._timeframe = timeframe
        self._theta_vol = float(theta_vol)
        self._k_atr = float(k_atr)
        self._t_hold = int(T_hold)
        self._tp_enabled = bool(tp_enabled)
        self._m_atr = float(m_atr)
        self._disallow_flip = bool(disallow_flip)
        self._state: dict[str, _State] = {}

    def _state_for(self, symbol: str) -> _State:
        if symbol not in self._state:
            self._state[symbol] = _State(
                atr=ATR(14),
                ema_fast=EMA(20),
                ema_slow=EMA(50),
                gate=RollingPercentileGate(bars_for_30_calendar_days(self._timeframe), include_current=True),
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
        st.tp_distance_frozen = None
        st.tp_price_frozen = None
        st.signal_bars_held = 0

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        signals: list[Signal] = []
        htf_root = ctx.get("htf") if isinstance(ctx, Mapping) else None
        if not isinstance(htf_root, Mapping):
            raise RuntimeError(f"L1-H1 requires ctx['htf']['{self._timeframe}'] for two-clock semantics.")
        htf_for_tf = htf_root.get(self._timeframe)
        if htf_for_tf is None:
            htf_for_tf = {}
        if not isinstance(htf_for_tf, Mapping):
            raise RuntimeError(f"L1-H1 requires mapping ctx['htf']['{self._timeframe}'] for two-clock semantics.")

        for symbol in sorted(tradeable):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            st = self._state_for(symbol)
            signal_bar = htf_for_tf.get(symbol)
            has_new_signal_bar = signal_bar is not None and signal_bar.ts != st.last_signal_ts
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
                st.last_signal_ts = signal_bar.ts

            atr_v = st.atr.value
            ema_f = st.ema_fast.value
            ema_s = st.ema_slow.value
            signal_close = signal_bar.close if signal_bar is not None else bar.close
            rv_t = None if atr_v is None or signal_close <= 0 else float(atr_v / signal_close)
            vol_pct_t = st.gate.update(rv_t) if has_new_signal_bar else None

            current = self._ctx_position_side(ctx, symbol)
            if current is not None:
                st.position = current
                if st.entry_execution_ts is None:
                    st.entry_execution_ts = ts
                if has_new_signal_bar:
                    st.signal_bars_held += 1
                if st.signal_bars_held >= self._t_hold and has_new_signal_bar:
                    signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL if current == Side.BUY else Side.BUY, signal_type="l1_h1_exit", confidence=1.0, metadata={"close_only": True, "exit_reason": "time_stop", "signal_bars_held": st.signal_bars_held, "hold_time_unit": "signal_bars"}))
                    self._clear_position_state(st)
                    continue

                if st.stop_price_frozen is not None:
                    if current == Side.BUY and bar.low <= st.stop_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h1_exit", confidence=1.0, metadata={"close_only": True, "exit_reason": "atr_stop", "stop_price": st.stop_price_frozen, "stop_distance": st.stop_distance_frozen, "atr_entry": st.atr_entry, "exit_monitoring_timeframe": "1m"}))
                        self._clear_position_state(st)
                        continue
                    if current == Side.SELL and bar.high >= st.stop_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h1_exit", confidence=1.0, metadata={"close_only": True, "exit_reason": "atr_stop", "stop_price": st.stop_price_frozen, "stop_distance": st.stop_distance_frozen, "atr_entry": st.atr_entry, "exit_monitoring_timeframe": "1m"}))
                        self._clear_position_state(st)
                        continue

                if self._tp_enabled and st.tp_price_frozen is not None:
                    if current == Side.BUY and bar.high >= st.tp_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h1_exit", confidence=1.0, metadata={"close_only": True, "exit_reason": "take_profit", "tp_price": st.tp_price_frozen, "tp_distance": st.tp_distance_frozen, "atr_entry": st.atr_entry, "exit_monitoring_timeframe": "1m"}))
                        self._clear_position_state(st)
                        continue
                    if current == Side.SELL and bar.low <= st.tp_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h1_exit", confidence=1.0, metadata={"close_only": True, "exit_reason": "take_profit", "tp_price": st.tp_price_frozen, "tp_distance": st.tp_distance_frozen, "atr_entry": st.atr_entry, "exit_monitoring_timeframe": "1m"}))
                        self._clear_position_state(st)
                        continue
                continue

            self._clear_position_state(st)
            if not has_new_signal_bar:
                continue
            if ema_f is None or ema_s is None or atr_v is None or vol_pct_t is None:
                continue
            trend_dir_t = 1 if ema_f > ema_s else -1 if ema_f < ema_s else 0
            gate_pass = bool(vol_pct_t >= self._theta_vol)
            if trend_dir_t == 0 or not gate_pass:
                continue
            side = Side.BUY if trend_dir_t > 0 else Side.SELL
            if self._disallow_flip and current is not None and ((current == Side.BUY and side == Side.SELL) or (current == Side.SELL and side == Side.BUY)):
                continue
            stop_distance = self._k_atr * atr_v
            stop_price = bar.close - stop_distance if side == Side.BUY else bar.close + stop_distance
            tp_distance = self._m_atr * atr_v if self._tp_enabled else None
            tp_price = (bar.close + tp_distance) if (self._tp_enabled and side == Side.BUY and tp_distance is not None) else (bar.close - tp_distance if (self._tp_enabled and side == Side.SELL and tp_distance is not None) else None)

            st.entry_price = float(bar.close)
            st.entry_signal_ts = signal_bar.ts
            st.entry_execution_ts = None
            st.atr_entry = float(atr_v)
            st.stop_distance_frozen = float(stop_distance)
            st.stop_price_frozen = float(stop_price)
            st.tp_distance_frozen = float(tp_distance) if tp_distance is not None else None
            st.tp_price_frozen = float(tp_price) if tp_price is not None else None
            st.signal_bars_held = 0

            signals.append(
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=side,
                    signal_type="l1_h1_vol_floor_trend",
                    confidence=1.0,
                    metadata={
                        "strategy": "l1_h1_vol_floor_trend",
                        "timeframe": self._timeframe,
                        "signal_timeframe": self._timeframe,
                        "exit_monitoring_timeframe": "1m",
                        "hold_time_unit": "signal_bars",
                        "atr_source_timeframe": "signal_timeframe",
                        "stop_model": "fixed_atr_multiple",
                        "stop_update_policy": "frozen_at_entry",
                        "tp_update_policy": "frozen_at_entry",
                        "rv_t": rv_t,
                        "vol_pct_t": vol_pct_t,
                        "gate_pass": gate_pass,
                        "trend_dir_t": trend_dir_t,
                        "entry_signal_ts": str(signal_bar.ts),
                        "atr_entry": st.atr_entry,
                        "stop_distance": st.stop_distance_frozen,
                        "stop_price": st.stop_price_frozen,
                        "tp_enabled": self._tp_enabled,
                        "tp_price": st.tp_price_frozen,
                        "tp_distance": st.tp_distance_frozen,
                    },
                )
            )
        return signals
