"""L1-H10A mean-reversion small-TP strategy."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.hypotheses.l1_h10 import vwap_deviation_z
from bt.indicators.atr import ATR
from bt.indicators.vwap import SessionVWAP
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


@dataclass
class _State:
    atr_signal: ATR
    signal_vwap: SessionVWAP
    position: Side | None = None
    last_signal_ts: pd.Timestamp | None = None
    entry_signal_ts: pd.Timestamp | None = None
    atr_entry: float | None = None
    stop_distance_frozen: float | None = None
    stop_price_frozen: float | None = None
    tp_distance_frozen: float | None = None
    tp_price_frozen: float | None = None
    signal_bars_held: int = 0


@register_strategy("l1_h10a_mean_reversion_small_tp")
class L1H10AMeanReversionSmallTPStrategy(Strategy):
    def __init__(
        self,
        *,
        timeframe: str = "5m",
        z0: float = 1.0,
        tp_r: float = 0.5,
        k_atr_stop: float = 2.5,
        family_variant: str = "L1-H10A",
        setup_type: str = "mean_reversion_small_tp",
    ) -> None:
        self._timeframe = str(timeframe)
        self._z0 = float(z0)
        self._tp_r = float(tp_r)
        self._k_atr_stop = float(k_atr_stop)
        self._family_variant = str(family_variant)
        self._setup_type = str(setup_type)
        self._state: dict[str, _State] = {}

    def _state_for(self, symbol: str) -> _State:
        if symbol not in self._state:
            self._state[symbol] = _State(
                atr_signal=ATR(14),
                signal_vwap=SessionVWAP(session="utc_day", price_source="typical"),
            )
        return self._state[symbol]

    @staticmethod
    def _ctx_position_side(ctx: Mapping[str, Any], symbol: str) -> Side | None:
        positions = ctx.get("positions")
        if not isinstance(positions, Mapping):
            return None
        payload = positions.get(symbol)
        if not isinstance(payload, Mapping):
            return None
        side = payload.get("side")
        if isinstance(side, Side):
            return side
        if isinstance(side, str):
            if side.lower() == "buy":
                return Side.BUY
            if side.lower() == "sell":
                return Side.SELL
        return None

    @staticmethod
    def _clear_position(st: _State) -> None:
        st.position = None
        st.entry_signal_ts = None
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
            raise RuntimeError(f"L1-H10A requires ctx['htf']['{self._timeframe}'] for two-clock semantics.")
        htf_for_tf = htf_root.get(self._timeframe) or {}
        if not isinstance(htf_for_tf, Mapping):
            raise RuntimeError(f"L1-H10A requires mapping ctx['htf']['{self._timeframe}'] for two-clock semantics.")

        for symbol in sorted(tradeable):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            st = self._state_for(symbol)
            signal_bar = htf_for_tf.get(symbol)
            has_new_signal_bar = signal_bar is not None and signal_bar.ts != st.last_signal_ts

            if has_new_signal_bar and signal_bar is not None:
                st.atr_signal.update(signal_bar)
                st.signal_vwap.update(signal_bar)
                st.last_signal_ts = signal_bar.ts

            current = self._ctx_position_side(ctx, symbol)
            if current is not None:
                st.position = current
                if has_new_signal_bar:
                    st.signal_bars_held += 1
                if st.stop_price_frozen is None or st.tp_price_frozen is None:
                    continue

                if current == Side.BUY and bar.low <= st.stop_price_frozen:
                    signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h10a_exit", confidence=1.0, metadata={
                        "close_only": True,
                        "exit_reason": "atr_stop",
                        "setup_type": self._setup_type,
                        "tp_hit_flag": False,
                        "stop_price": st.stop_price_frozen,
                        "stop_distance": st.stop_distance_frozen,
                        "tp_price": st.tp_price_frozen,
                        "tp_distance": st.tp_distance_frozen,
                        "atr_entry": st.atr_entry,
                        "holding_period_bars_signal": st.signal_bars_held,
                        "signal_timeframe": self._timeframe,
                        "exit_monitoring_timeframe": "1m",
                    }))
                    self._clear_position(st)
                    continue
                if current == Side.SELL and bar.high >= st.stop_price_frozen:
                    signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h10a_exit", confidence=1.0, metadata={
                        "close_only": True,
                        "exit_reason": "atr_stop",
                        "setup_type": self._setup_type,
                        "tp_hit_flag": False,
                        "stop_price": st.stop_price_frozen,
                        "stop_distance": st.stop_distance_frozen,
                        "tp_price": st.tp_price_frozen,
                        "tp_distance": st.tp_distance_frozen,
                        "atr_entry": st.atr_entry,
                        "holding_period_bars_signal": st.signal_bars_held,
                        "signal_timeframe": self._timeframe,
                        "exit_monitoring_timeframe": "1m",
                    }))
                    self._clear_position(st)
                    continue

                if current == Side.BUY and bar.high >= st.tp_price_frozen:
                    signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h10a_exit", confidence=1.0, metadata={
                        "close_only": True,
                        "exit_reason": "take_profit",
                        "setup_type": self._setup_type,
                        "tp_hit_flag": True,
                        "time_to_tp_bars_signal": st.signal_bars_held,
                        "stop_price": st.stop_price_frozen,
                        "stop_distance": st.stop_distance_frozen,
                        "tp_price": st.tp_price_frozen,
                        "tp_distance": st.tp_distance_frozen,
                        "atr_entry": st.atr_entry,
                        "holding_period_bars_signal": st.signal_bars_held,
                        "signal_timeframe": self._timeframe,
                        "exit_monitoring_timeframe": "1m",
                    }))
                    self._clear_position(st)
                    continue
                if current == Side.SELL and bar.low <= st.tp_price_frozen:
                    signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h10a_exit", confidence=1.0, metadata={
                        "close_only": True,
                        "exit_reason": "take_profit",
                        "setup_type": self._setup_type,
                        "tp_hit_flag": True,
                        "time_to_tp_bars_signal": st.signal_bars_held,
                        "stop_price": st.stop_price_frozen,
                        "stop_distance": st.stop_distance_frozen,
                        "tp_price": st.tp_price_frozen,
                        "tp_distance": st.tp_distance_frozen,
                        "atr_entry": st.atr_entry,
                        "holding_period_bars_signal": st.signal_bars_held,
                        "signal_timeframe": self._timeframe,
                        "exit_monitoring_timeframe": "1m",
                    }))
                    self._clear_position(st)
                continue

            self._clear_position(st)
            if not has_new_signal_bar or signal_bar is None:
                continue
            atr_v = st.atr_signal.value
            vwap_v = st.signal_vwap.value
            if atr_v is None or vwap_v is None or float(atr_v) <= 0:
                continue

            z_vwap_t = vwap_deviation_z(close=float(signal_bar.close), session_vwap=float(vwap_v), atr=float(atr_v))
            if z_vwap_t is None:
                continue

            side: Side | None = None
            if z_vwap_t <= -self._z0:
                side = Side.BUY
            elif z_vwap_t >= self._z0:
                side = Side.SELL
            if side is None:
                continue

            entry_reference_price = float(bar.close)
            stop_distance = self._k_atr_stop * float(atr_v)
            tp_distance = self._tp_r * stop_distance
            stop_price = entry_reference_price - stop_distance if side == Side.BUY else entry_reference_price + stop_distance
            tp_price = entry_reference_price + tp_distance if side == Side.BUY else entry_reference_price - tp_distance

            st.position = side
            st.entry_signal_ts = signal_bar.ts
            st.atr_entry = float(atr_v)
            st.stop_distance_frozen = float(stop_distance)
            st.stop_price_frozen = float(stop_price)
            st.tp_distance_frozen = float(tp_distance)
            st.tp_price_frozen = float(tp_price)
            st.signal_bars_held = 0

            signals.append(
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=side,
                    signal_type="l1_h10a_entry",
                    confidence=1.0,
                    metadata={
                        "strategy": "l1_h10a_mean_reversion_small_tp",
                        "family_variant": self._family_variant,
                        "parent_family": "L1-H10",
                        "setup_type": self._setup_type,
                        "entry_reason": "session_vwap_deviation_reversion",
                        "signal_timeframe": self._timeframe,
                        "exit_monitoring_timeframe": "1m",
                        "base_data_frequency_expected": "1m",
                        "risk_accounting": "engine_canonical_R",
                        "no_pyramiding": True,
                        "entry_signal_ts": str(signal_bar.ts),
                        "entry_reference_price": entry_reference_price,
                        "atr_entry": float(atr_v),
                        "vwap_t": float(vwap_v),
                        "z_vwap_t": float(z_vwap_t),
                        "z0": self._z0,
                        "tp_r": self._tp_r,
                        "rr_ratio": self._tp_r,
                        "stop_model": "fixed_atr_multiple",
                        "stop_update_policy": "frozen_at_entry",
                        "atr_source_timeframe": self._timeframe,
                        "stop_distance": float(stop_distance),
                        "stop_price": float(stop_price),
                        "tp_distance": float(tp_distance),
                        "tp_price": float(tp_price),
                    },
                )
            )
        return signals
