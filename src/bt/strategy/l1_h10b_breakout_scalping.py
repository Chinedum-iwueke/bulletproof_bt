"""L1-H10B breakout-scalping strategy."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.hypotheses.l1_h10 import breakout_distance_atr
from bt.indicators.atr import ATR
from bt.indicators.dmi_adx import DMIADX
from bt.strategy import register_strategy
from bt.strategy.base import Strategy
from bt.logging.decision_trace import make_decision_trace


@dataclass
class _State:
    atr_signal: ATR
    adx: DMIADX
    position: Side | None = None
    last_signal_ts: pd.Timestamp | None = None
    prev_signal_bar: Bar | None = None
    atr_entry: float | None = None
    stop_distance_frozen: float | None = None
    stop_price_frozen: float | None = None
    tp_distance_frozen: float | None = None
    tp_price_frozen: float | None = None
    signal_bars_held: int = 0


@register_strategy("l1_h10b_breakout_scalping")
class L1H10BBreakoutScalpingStrategy(Strategy):
    def __init__(
        self,
        *,
        timeframe: str = "5m",
        breakout_atr: float = 0.75,
        tp_r: float = 0.5,
        k_atr_stop: float = 2.5,
        adx_min_fixed: float = 20.0,
        family_variant: str = "L1-H10B",
        setup_type: str = "breakout_scalping",
    ) -> None:
        self._timeframe = str(timeframe)
        self._breakout_atr = float(breakout_atr)
        self._tp_r = float(tp_r)
        self._k_atr_stop = float(k_atr_stop)
        self._adx_min_fixed = float(adx_min_fixed)
        self._family_variant = str(family_variant)
        self._setup_type = str(setup_type)
        self._state: dict[str, _State] = {}

    def _state_for(self, symbol: str) -> _State:
        if symbol not in self._state:
            self._state[symbol] = _State(atr_signal=ATR(14), adx=DMIADX(14))
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
            raise RuntimeError(f"L1-H10B requires ctx['htf']['{self._timeframe}'] for two-clock semantics.")
        htf_for_tf = htf_root.get(self._timeframe) or {}
        if not isinstance(htf_for_tf, Mapping):
            raise RuntimeError(f"L1-H10B requires mapping ctx['htf']['{self._timeframe}'] for two-clock semantics.")

        for symbol in sorted(tradeable):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            st = self._state_for(symbol)
            signal_bar = htf_for_tf.get(symbol)
            has_new_signal_bar = signal_bar is not None and signal_bar.ts != st.last_signal_ts

            if has_new_signal_bar and signal_bar is not None:
                st.atr_signal.update(signal_bar)
                st.adx.update(signal_bar)
                st.last_signal_ts = signal_bar.ts

            current = self._ctx_position_side(ctx, symbol)
            if current is not None:
                st.position = current
                if has_new_signal_bar:
                    st.signal_bars_held += 1
                    if signal_bar is not None:
                        # Keep reference bar fresh across long holds so the next
                        # flat-state breakout check uses the most recent prior
                        # signal bar rather than a stale pre-entry bar.
                        st.prev_signal_bar = signal_bar
                if st.stop_price_frozen is None or st.tp_price_frozen is None:
                    continue

                if current == Side.BUY and bar.low <= st.stop_price_frozen:
                    signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h10b_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="breakout_scalping_entry",
                            setup_class="breakout_scalping",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h10b_breakout_scalping"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
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
                    signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h10b_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="breakout_scalping_entry",
                            setup_class="breakout_scalping",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h10b_breakout_scalping"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
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
                    signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h10b_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="breakout_scalping_entry",
                            setup_class="breakout_scalping",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h10b_breakout_scalping"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
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
                    signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h10b_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="breakout_scalping_entry",
                            setup_class="breakout_scalping",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h10b_breakout_scalping"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
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
            adx_v = st.adx.values.get("adx")
            if atr_v is None or float(atr_v) <= 0 or adx_v is None or float(adx_v) < self._adx_min_fixed:
                st.prev_signal_bar = signal_bar
                continue
            if st.prev_signal_bar is None:
                st.prev_signal_bar = signal_bar
                continue

            breakout_reference_up = float(st.prev_signal_bar.close)
            breakout_reference_down = float(st.prev_signal_bar.close)
            close = float(signal_bar.close)
            threshold = self._breakout_atr * float(atr_v)
            long_trigger = close >= (breakout_reference_up + threshold)
            short_trigger = close <= (breakout_reference_down - threshold)
            if not long_trigger and not short_trigger:
                st.prev_signal_bar = signal_bar
                continue

            side = Side.BUY if long_trigger else Side.SELL
            side_name = "long" if side == Side.BUY else "short"
            breakout_reference = breakout_reference_up if side == Side.BUY else breakout_reference_down
            breakout_dist_atr = breakout_distance_atr(close=close, reference=breakout_reference, atr=float(atr_v), side=side_name)
            entry_reference_price = float(bar.close)
            stop_distance = self._k_atr_stop * float(atr_v)
            tp_distance = self._tp_r * stop_distance
            stop_price = entry_reference_price - stop_distance if side == Side.BUY else entry_reference_price + stop_distance
            tp_price = entry_reference_price + tp_distance if side == Side.BUY else entry_reference_price - tp_distance

            st.position = side
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
                    signal_type="l1_h10b_entry",
                    confidence=1.0,
                    metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="breakout_scalping_entry",
                            setup_class="breakout_scalping",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h10b_breakout_scalping"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                        "strategy": "l1_h10b_breakout_scalping",
                        "family_variant": self._family_variant,
                        "parent_family": "L1-H10",
                        "setup_type": self._setup_type,
                        "entry_reason": "atr_scaled_breakout_scalp",
                        "signal_timeframe": self._timeframe,
                        "exit_monitoring_timeframe": "1m",
                        "base_data_frequency_expected": "1m",
                        "risk_accounting": "engine_canonical_R",
                        "no_pyramiding": True,
                        "entry_signal_ts": str(signal_bar.ts),
                        "entry_reference_price": entry_reference_price,
                        "atr_entry": float(atr_v),
                        "adx_entry": float(adx_v),
                        "adx_min_fixed": self._adx_min_fixed,
                        "breakout_reference_price": float(breakout_reference),
                        "breakout_reference_type": "prior_signal_close",
                        "breakout_atr": self._breakout_atr,
                        "breakout_distance_atr": float(breakout_dist_atr) if breakout_dist_atr is not None else None,
                        "breakout_distance_price": abs(close - float(breakout_reference)),
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
            st.prev_signal_bar = signal_bar

        return signals
