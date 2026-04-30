"""L1-H9 momentum breakout continuation strategy family."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.hypotheses.l1_h9 import breakout_close_strength, breakout_distance_atr, ema_spread_pct
from bt.indicators.atr import ATR
from bt.indicators.dmi_adx import DMIADX
from bt.indicators.ema import EMA
from bt.strategy import register_strategy
from bt.strategy.base import Strategy
from bt.logging.decision_trace import make_decision_trace


@dataclass
class _State:
    adx: DMIADX
    ema_fast: EMA
    ema_slow: EMA
    atr_signal: ATR
    position: Side | None = None
    last_signal_ts: pd.Timestamp | None = None
    prev_signal_bar: Bar | None = None
    bars_since_trend_alignment: int = 0
    trend_side: Side | None = None
    signal_bars_held: int = 0
    entry_price: float | None = None
    atr_entry: float | None = None
    stop_price_frozen: float | None = None
    stop_distance_frozen: float | None = None
    partial_taken: bool = False
    tp1_target_price: float | None = None
    protection_active: bool = False
    trail_stop_price: float | None = None
    high_since_entry: float | None = None
    low_since_entry: float | None = None


@register_strategy("l1_h9_momentum_breakout")
class L1H9MomentumBreakoutStrategy(Strategy):
    def __init__(
        self,
        *,
        timeframe: str = "15m",
        adx_min: float = 25.0,
        breakout_atr_mult: float = 1.0,
        stop_atr_mult: float = 2.0,
        tp1_at_r: float = 2.0,
        partial_fraction: float = 0.5,
        post_tp1_lock_r: float = 0.0,
        trail_atr_mult: float | None = None,
        family_variant: str = "L1-H9A",
    ) -> None:
        self._timeframe = str(timeframe)
        self._adx_min = float(adx_min)
        self._breakout_atr_mult = float(breakout_atr_mult)
        self._stop_atr_mult = float(stop_atr_mult)
        self._tp1_at_r = float(tp1_at_r)
        self._partial_fraction = float(partial_fraction)
        self._post_tp1_lock_r = float(post_tp1_lock_r)
        self._trail_atr_mult = float(trail_atr_mult) if trail_atr_mult is not None else None
        self._family_variant = str(family_variant)
        self._state: dict[str, _State] = {}

    def _state_for(self, symbol: str) -> _State:
        current = self._state.get(symbol)
        if current is None:
            current = _State(adx=DMIADX(14), ema_fast=EMA(9), ema_slow=EMA(21), atr_signal=ATR(14))
            self._state[symbol] = current
        return current

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
    def _clear_trade_state(st: _State) -> None:
        st.position = None
        st.signal_bars_held = 0
        st.entry_price = None
        st.atr_entry = None
        st.stop_price_frozen = None
        st.stop_distance_frozen = None
        st.partial_taken = False
        st.tp1_target_price = None
        st.protection_active = False
        st.trail_stop_price = None
        st.high_since_entry = None
        st.low_since_entry = None

    def _handle_open_position(self, *, ts: pd.Timestamp, symbol: str, bar: Bar, st: _State, side: Side, has_new_signal_bar: bool) -> list[Signal]:
        signals: list[Signal] = []
        st.position = side
        st.high_since_entry = float(bar.high) if st.high_since_entry is None else max(float(st.high_since_entry), float(bar.high))
        st.low_since_entry = float(bar.low) if st.low_since_entry is None else min(float(st.low_since_entry), float(bar.low))

        if has_new_signal_bar:
            st.signal_bars_held += 1

        if not st.partial_taken and st.tp1_target_price is not None:
            tp1_hit = (side == Side.BUY and bar.high >= st.tp1_target_price) or (side == Side.SELL and bar.low <= st.tp1_target_price)
            if tp1_hit:
                st.partial_taken = True
                st.protection_active = True
                signals.append(
                    Signal(
                        ts=ts,
                        symbol=symbol,
                        side=Side.SELL if side == Side.BUY else Side.BUY,
                        signal_type="l1_h9_partial",
                        confidence=1.0,
                        metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="momentum_breakout_entry",
                            setup_class="momentum_breakout",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h9_momentum_breakout"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                            "close_only": True,
                            "close_fraction": self._partial_fraction,
                            "partial_fraction": self._partial_fraction,
                            "tp1_at_r": self._tp1_at_r,
                            "post_tp1_lock_r": self._post_tp1_lock_r,
                            "trail_atr_mult": self._trail_atr_mult,
                            "exit_reason": "tp1_partial",
                            "tp1_hit": True,
                            "protection_activated": True,
                            "exit_monitoring_timeframe": "1m",
                        },
                    )
                )
                return signals

        effective_stop = st.stop_price_frozen
        if st.protection_active and st.entry_price is not None and st.stop_distance_frozen is not None and effective_stop is not None:
            lock_price = (float(st.entry_price) + (self._post_tp1_lock_r * float(st.stop_distance_frozen))) if side == Side.BUY else (float(st.entry_price) - (self._post_tp1_lock_r * float(st.stop_distance_frozen)))
            if side == Side.BUY:
                effective_stop = max(float(effective_stop), lock_price)
            else:
                effective_stop = min(float(effective_stop), lock_price)

        if self._trail_atr_mult is not None and st.protection_active and st.atr_entry is not None and st.atr_entry > 0:
            if side == Side.BUY and st.high_since_entry is not None:
                candidate = float(st.high_since_entry) - (self._trail_atr_mult * float(st.atr_entry))
                st.trail_stop_price = candidate if st.trail_stop_price is None else max(float(st.trail_stop_price), candidate)
                effective_stop = max(float(effective_stop), float(st.trail_stop_price)) if effective_stop is not None else float(st.trail_stop_price)
            elif side == Side.SELL and st.low_since_entry is not None:
                candidate = float(st.low_since_entry) + (self._trail_atr_mult * float(st.atr_entry))
                st.trail_stop_price = candidate if st.trail_stop_price is None else min(float(st.trail_stop_price), candidate)
                effective_stop = min(float(effective_stop), float(st.trail_stop_price)) if effective_stop is not None else float(st.trail_stop_price)

        if effective_stop is None:
            return signals
        if side == Side.BUY and bar.low <= effective_stop:
            signals.append(
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=Side.SELL,
                    signal_type="l1_h9_exit",
                    confidence=1.0,
                    metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="momentum_breakout_entry",
                            setup_class="momentum_breakout",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h9_momentum_breakout"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                        "close_only": True,
                        "exit_reason": "stop_or_trail",
                        "stop_price": float(effective_stop),
                        "entry_stop_price": st.stop_price_frozen,
                        "stop_distance": st.stop_distance_frozen,
                        "atr_entry": st.atr_entry,
                        "tp1_hit": st.partial_taken,
                        "post_tp1_lock_r": self._post_tp1_lock_r,
                        "trail_stop_price": st.trail_stop_price,
                        "trail_atr_mult": self._trail_atr_mult,
                        "exit_monitoring_timeframe": "1m",
                    },
                )
            )
            self._clear_trade_state(st)
        elif side == Side.SELL and bar.high >= effective_stop:
            signals.append(
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=Side.BUY,
                    signal_type="l1_h9_exit",
                    confidence=1.0,
                    metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="momentum_breakout_entry",
                            setup_class="momentum_breakout",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h9_momentum_breakout"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                        "close_only": True,
                        "exit_reason": "stop_or_trail",
                        "stop_price": float(effective_stop),
                        "entry_stop_price": st.stop_price_frozen,
                        "stop_distance": st.stop_distance_frozen,
                        "atr_entry": st.atr_entry,
                        "tp1_hit": st.partial_taken,
                        "post_tp1_lock_r": self._post_tp1_lock_r,
                        "trail_stop_price": st.trail_stop_price,
                        "trail_atr_mult": self._trail_atr_mult,
                        "exit_monitoring_timeframe": "1m",
                    },
                )
            )
            self._clear_trade_state(st)
        return signals

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        signals: list[Signal] = []
        htf_root = ctx.get("htf") if isinstance(ctx, Mapping) else None
        if not isinstance(htf_root, Mapping):
            raise RuntimeError(f"L1-H9 requires ctx['htf']['{self._timeframe}'] for two-clock semantics.")
        htf_for_tf = htf_root.get(self._timeframe)
        if htf_for_tf is None:
            htf_for_tf = {}
        if not isinstance(htf_for_tf, Mapping):
            raise RuntimeError(f"L1-H9 requires mapping ctx['htf']['{self._timeframe}'] for two-clock semantics.")

        for symbol in sorted(tradeable):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            st = self._state_for(symbol)

            signal_bar = htf_for_tf.get(symbol)
            has_new_signal_bar = signal_bar is not None and signal_bar.ts != st.last_signal_ts

            current = self._ctx_position_side(ctx, symbol)
            if current is not None:
                signals.extend(self._handle_open_position(ts=ts, symbol=symbol, bar=bar, st=st, side=current, has_new_signal_bar=has_new_signal_bar))
                if has_new_signal_bar and signal_bar is not None:
                    st.last_signal_ts = signal_bar.ts
                continue
            if st.position is not None:
                self._clear_trade_state(st)

            if not has_new_signal_bar or signal_bar is None:
                continue
            st.last_signal_ts = signal_bar.ts

            st.adx.update(signal_bar)
            st.ema_fast.update(signal_bar)
            st.ema_slow.update(signal_bar)
            st.atr_signal.update(signal_bar)

            adx_value = st.adx.values.get("adx")
            ema_fast = st.ema_fast.value
            ema_slow = st.ema_slow.value
            atr_value = st.atr_signal.value
            if ema_fast is None or ema_slow is None or adx_value is None or atr_value is None or float(atr_value) <= 0:
                st.prev_signal_bar = signal_bar
                continue

            trend_dir = Side.BUY if ema_fast > ema_slow else Side.SELL if ema_fast < ema_slow else None
            if st.trend_side != trend_dir:
                st.trend_side = trend_dir
                st.bars_since_trend_alignment = 0
            else:
                st.bars_since_trend_alignment += 1
            if trend_dir is None or float(adx_value) < self._adx_min:
                st.prev_signal_bar = signal_bar
                continue
            if st.prev_signal_bar is None:
                st.prev_signal_bar = signal_bar
                continue

            prev_high = float(st.prev_signal_bar.high)
            prev_low = float(st.prev_signal_bar.low)
            close = float(signal_bar.close)
            open_price = float(signal_bar.open)
            high = float(signal_bar.high)
            low = float(signal_bar.low)

            threshold = self._breakout_atr_mult * float(atr_value)
            long_trigger = close >= (prev_high + threshold)
            short_trigger = close <= (prev_low - threshold)
            if not long_trigger and not short_trigger:
                st.prev_signal_bar = signal_bar
                continue

            side = Side.BUY if long_trigger else Side.SELL
            side_name = "long" if side == Side.BUY else "short"
            breakout_level = prev_high if side == Side.BUY else prev_low
            breakout_dist_atr = breakout_distance_atr(close=close, breakout_level=breakout_level, atr=float(atr_value), side=side_name)
            close_strength = breakout_close_strength(open_price=open_price, high=high, low=low, close=close, side=side_name)
            bar_range_atr = (high - low) / float(atr_value) if float(atr_value) > 0 else None
            spread_pct = ema_spread_pct(ema_fast=float(ema_fast), ema_slow=float(ema_slow), price=close)

            entry_ref = float(bar.close)
            stop_distance = self._stop_atr_mult * float(atr_value)
            stop_price = entry_ref - stop_distance if side == Side.BUY else entry_ref + stop_distance
            tp1_target = entry_ref + (self._tp1_at_r * stop_distance) if side == Side.BUY else entry_ref - (self._tp1_at_r * stop_distance)

            st.entry_price = entry_ref
            st.atr_entry = float(atr_value)
            st.stop_distance_frozen = float(stop_distance)
            st.stop_price_frozen = float(stop_price)
            st.tp1_target_price = float(tp1_target)
            st.partial_taken = False
            st.protection_active = False
            st.trail_stop_price = None
            st.high_since_entry = float(bar.high)
            st.low_since_entry = float(bar.low)
            st.signal_bars_held = 0

            signals.append(
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=side,
                    signal_type="l1_h9_entry",
                    confidence=1.0,
                    metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="momentum_breakout_entry",
                            setup_class="momentum_breakout",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h9_momentum_breakout"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                        "strategy": "l1_h9_momentum_breakout",
                        "family_variant": self._family_variant,
                        "family_pattern": "momentum_breakout_continuation",
                        "entry_reason": "trend_aligned_breakout_confirmed",
                        "signal_timeframe": self._timeframe,
                        "exit_monitoring_timeframe": "1m",
                        "base_data_frequency_expected": "1m",
                        "risk_accounting": "engine_canonical_R",
                        "trend_dir": side_name,
                        "trend_strength_adx": adx_value,
                        "adx_entry": adx_value,
                        "adx_min": self._adx_min,
                        "ema_fast_entry": ema_fast,
                        "ema_slow_entry": ema_slow,
                        "ema_spread_pct": spread_pct,
                        "bars_since_trend_alignment": st.bars_since_trend_alignment,
                        "breakout_level": breakout_level,
                        "breakout_level_type": "prior_bar_extreme",
                        "breakout_distance_atr": breakout_dist_atr,
                        "breakout_close_strength": close_strength,
                        "breakout_bar_range_atr": bar_range_atr,
                        "breakout_atr_mult": self._breakout_atr_mult,
                        "entry_signal_ts": str(signal_bar.ts),
                        "entry_reference_price": entry_ref,
                        "stop_model": "fixed_atr_multiple",
                        "stop_update_policy": "frozen_at_entry",
                        "stop_distance": float(stop_distance),
                        "stop_price": float(stop_price),
                        "tp1_at_r": self._tp1_at_r,
                        "post_tp1_lock_r": self._post_tp1_lock_r,
                        "trail_atr_mult": self._trail_atr_mult,
                        "stop_details": {
                            "entry_price": entry_ref,
                            "atr_entry": float(atr_value),
                            "stop_atr_mult": self._stop_atr_mult,
                            "stop_mode": "atr",
                            "atr_source_timeframe": self._timeframe,
                        },
                    },
                )
            )
            st.prev_signal_bar = signal_bar

        return signals
