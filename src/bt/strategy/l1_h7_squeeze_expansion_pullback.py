"""L1-H7 squeeze -> expansion -> pullback continuation strategy family."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.indicators.atr import ATR
from bt.indicators.dmi_adx import DMIADX
from bt.indicators.ema import EMA
from bt.indicators.squeeze import BBKCSqueeze
from bt.indicators.vwap import SessionVWAP
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


@dataclass
class _State:
    squeeze: BBKCSqueeze
    adx: DMIADX
    ema_fast: EMA
    ema_slow: EMA
    ema_pullback: EMA
    atr_signal: ATR
    signal_vwap: SessionVWAP
    base_position: Side | None = None
    last_signal_ts: pd.Timestamp | None = None
    last_signal_close: float | None = None
    squeeze_qualified: bool = False
    expansion_direction: Side | None = None
    pullback_wait_bars: int = 0
    entry_signal_ts: pd.Timestamp | None = None
    atr_entry: float | None = None
    entry_price: float | None = None
    stop_price_frozen: float | None = None
    stop_distance_frozen: float | None = None
    partial_taken: bool = False
    tp1_target_price: float | None = None
    protection_active: bool = False
    trail_stop_price: float | None = None
    high_since_entry: float | None = None
    low_since_entry: float | None = None


@register_strategy("l1_h7_squeeze_expansion_pullback")
class L1H7SqueezeExpansionPullbackStrategy(Strategy):
    def __init__(
        self,
        *,
        timeframe: str = "15m",
        bb_period: int = 20,
        bb_std_mult: float = 2.0,
        kc_period: int = 20,
        kc_atr_mult: float = 1.5,
        squeeze_min_bars: int = 4,
        adx_min: float = 18.0,
        ema_fast_period: int = 9,
        ema_slow_period: int = 21,
        pullback_ema_period: int = 21,
        pullback_use_ema21: bool = True,
        pullback_use_session_vwap: bool = True,
        pullback_max_wait: int = 12,
        stop_atr_mult: float = 2.0,
        partial_at_r: float = 1.5,
        partial_fraction: float = 0.5,
        trail_atr_mult: float = 2.5,
        no_pyramiding: bool = True,
        family_variant: str = "L1-H7A",
    ) -> None:
        self._timeframe = str(timeframe)
        self._bb_period = int(bb_period)
        self._bb_std_mult = float(bb_std_mult)
        self._kc_period = int(kc_period)
        self._kc_atr_mult = float(kc_atr_mult)
        self._squeeze_min_bars = int(squeeze_min_bars)
        self._adx_min = float(adx_min)
        self._ema_fast_period = int(ema_fast_period)
        self._ema_slow_period = int(ema_slow_period)
        self._pullback_ema_period = int(pullback_ema_period)
        self._pullback_use_ema21 = bool(pullback_use_ema21)
        self._pullback_use_session_vwap = bool(pullback_use_session_vwap)
        self._pullback_max_wait = int(pullback_max_wait)
        self._stop_atr_mult = float(stop_atr_mult)
        self._partial_at_r = float(partial_at_r)
        self._partial_fraction = float(partial_fraction)
        self._trail_atr_mult = float(trail_atr_mult)
        self._no_pyramiding = bool(no_pyramiding)
        self._family_variant = str(family_variant)
        self._state: dict[str, _State] = {}

    def _state_for(self, symbol: str) -> _State:
        current = self._state.get(symbol)
        if current is None:
            current = _State(
                squeeze=BBKCSqueeze(
                    bb_period=self._bb_period,
                    bb_std_mult=self._bb_std_mult,
                    kc_period=self._kc_period,
                    kc_atr_mult=self._kc_atr_mult,
                ),
                adx=DMIADX(14),
                ema_fast=EMA(self._ema_fast_period),
                ema_slow=EMA(self._ema_slow_period),
                ema_pullback=EMA(self._pullback_ema_period),
                atr_signal=ATR(14),
                signal_vwap=SessionVWAP(session="utc_day", price_source="typical"),
            )
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
        st.base_position = None
        st.entry_signal_ts = None
        st.atr_entry = None
        st.entry_price = None
        st.stop_price_frozen = None
        st.stop_distance_frozen = None
        st.partial_taken = False
        st.tp1_target_price = None
        st.protection_active = False
        st.trail_stop_price = None
        st.high_since_entry = None
        st.low_since_entry = None

    def _handle_open_position(self, *, ts: pd.Timestamp, symbol: str, bar: Bar, st: _State, side: Side) -> list[Signal]:
        signals: list[Signal] = []
        st.base_position = side
        st.high_since_entry = float(bar.high) if st.high_since_entry is None else max(float(st.high_since_entry), float(bar.high))
        st.low_since_entry = float(bar.low) if st.low_since_entry is None else min(float(st.low_since_entry), float(bar.low))

        if (
            not st.partial_taken
            and st.tp1_target_price is not None
            and st.stop_distance_frozen is not None
            and st.stop_distance_frozen > 0
        ):
            tp1_hit = (side == Side.BUY and bar.high >= st.tp1_target_price) or (side == Side.SELL and bar.low <= st.tp1_target_price)
            if tp1_hit:
                partial_side = Side.SELL if side == Side.BUY else Side.BUY
                st.partial_taken = True
                st.protection_active = True
                signals.append(
                    Signal(
                        ts=ts,
                        symbol=symbol,
                        side=partial_side,
                        signal_type="l1_h7_partial",
                        confidence=1.0,
                        metadata={
                            "close_only": True,
                            "close_fraction": self._partial_fraction,
                            "partial_fraction": self._partial_fraction,
                            "partial_at_r": self._partial_at_r,
                            "partial_target_price": st.tp1_target_price,
                            "exit_reason": "tp1_partial",
                            "tp1_hit": True,
                            "protection_activated": True,
                            "exit_monitoring_timeframe": "1m",
                        },
                    )
                )
                return signals

        effective_stop = st.stop_price_frozen
        if st.protection_active and st.entry_price is not None and effective_stop is not None:
            if side == Side.BUY:
                effective_stop = max(float(effective_stop), float(st.entry_price))
            else:
                effective_stop = min(float(effective_stop), float(st.entry_price))

        if st.protection_active and st.atr_entry is not None and st.atr_entry > 0:
            if side == Side.BUY and st.high_since_entry is not None:
                candidate = float(st.high_since_entry) - (self._trail_atr_mult * float(st.atr_entry))
                st.trail_stop_price = candidate if st.trail_stop_price is None else max(float(st.trail_stop_price), candidate)
                if effective_stop is not None:
                    effective_stop = max(float(effective_stop), float(st.trail_stop_price))
            elif side == Side.SELL and st.low_since_entry is not None:
                candidate = float(st.low_since_entry) + (self._trail_atr_mult * float(st.atr_entry))
                st.trail_stop_price = candidate if st.trail_stop_price is None else min(float(st.trail_stop_price), candidate)
                if effective_stop is not None:
                    effective_stop = min(float(effective_stop), float(st.trail_stop_price))

        if effective_stop is not None:
            if side == Side.BUY and bar.low <= effective_stop:
                signals.append(
                    Signal(
                        ts=ts,
                        symbol=symbol,
                        side=Side.SELL,
                        signal_type="l1_h7_exit",
                        confidence=1.0,
                        metadata={
                            "close_only": True,
                            "exit_reason": "stop_or_trail",
                            "stop_price": float(effective_stop),
                            "entry_stop_price": st.stop_price_frozen,
                            "stop_distance": st.stop_distance_frozen,
                            "atr_entry": st.atr_entry,
                            "protection_activated": st.protection_active,
                            "trail_stop_price": st.trail_stop_price,
                            "tp1_hit": st.partial_taken,
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
                        signal_type="l1_h7_exit",
                        confidence=1.0,
                        metadata={
                            "close_only": True,
                            "exit_reason": "stop_or_trail",
                            "stop_price": float(effective_stop),
                            "entry_stop_price": st.stop_price_frozen,
                            "stop_distance": st.stop_distance_frozen,
                            "atr_entry": st.atr_entry,
                            "protection_activated": st.protection_active,
                            "trail_stop_price": st.trail_stop_price,
                            "tp1_hit": st.partial_taken,
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
            raise RuntimeError(f"L1-H7 requires ctx['htf']['{self._timeframe}'] for two-clock semantics.")
        htf_for_tf = htf_root.get(self._timeframe)
        if htf_for_tf is None:
            htf_for_tf = {}
        if not isinstance(htf_for_tf, Mapping):
            raise RuntimeError(f"L1-H7 requires mapping ctx['htf']['{self._timeframe}'] for two-clock semantics.")

        for symbol in sorted(tradeable):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            st = self._state_for(symbol)

            current = self._ctx_position_side(ctx, symbol)
            if current is not None:
                signals.extend(self._handle_open_position(ts=ts, symbol=symbol, bar=bar, st=st, side=current))
                continue
            if st.base_position is not None:
                self._clear_trade_state(st)

            signal_bar = htf_for_tf.get(symbol)
            has_new_signal_bar = signal_bar is not None and signal_bar.ts != st.last_signal_ts
            if not has_new_signal_bar:
                continue

            st.last_signal_ts = signal_bar.ts
            st.squeeze.update(signal_bar)
            st.adx.update(signal_bar)
            st.ema_fast.update(signal_bar)
            st.ema_slow.update(signal_bar)
            st.ema_pullback.update(signal_bar)
            st.atr_signal.update(signal_bar)
            st.signal_vwap.update(signal_bar)

            squeeze_on = st.squeeze.values.get("squeeze_on")
            squeeze_duration = int(st.squeeze.values.get("squeeze_duration") or 0)
            bb_upper = st.squeeze.values.get("bb_upper")
            bb_lower = st.squeeze.values.get("bb_lower")
            adx_value = st.adx.values.get("adx")
            ema_fast = st.ema_fast.value
            ema_slow = st.ema_slow.value
            ema_pullback = st.ema_pullback.value
            signal_vwap = st.signal_vwap.value
            atr_value = st.atr_signal.value
            signal_close = float(signal_bar.close)
            signal_low = float(signal_bar.low)
            signal_high = float(signal_bar.high)

            if bool(squeeze_on) and squeeze_duration >= self._squeeze_min_bars:
                st.squeeze_qualified = True

            long_bias = ema_fast is not None and ema_slow is not None and ema_fast > ema_slow
            short_bias = ema_fast is not None and ema_slow is not None and ema_fast < ema_slow
            breakout_long = bb_upper is not None and signal_close > float(bb_upper)
            breakout_short = bb_lower is not None and signal_close < float(bb_lower)
            direction_confirm_long = (
                adx_value is not None
                and float(adx_value) >= self._adx_min
                and long_bias
                and st.last_signal_close is not None
                and signal_close > float(st.last_signal_close)
            )
            direction_confirm_short = (
                adx_value is not None
                and float(adx_value) >= self._adx_min
                and short_bias
                and st.last_signal_close is not None
                and signal_close < float(st.last_signal_close)
            )

            expansion_trigger = False
            expansion_direction: Side | None = None
            if st.squeeze_qualified:
                if breakout_long or direction_confirm_long:
                    expansion_trigger = True
                    expansion_direction = Side.BUY
                elif breakout_short or direction_confirm_short:
                    expansion_trigger = True
                    expansion_direction = Side.SELL

            if expansion_trigger:
                st.expansion_direction = expansion_direction
                st.pullback_wait_bars = 0
                st.squeeze_qualified = False
            elif st.expansion_direction is not None:
                st.pullback_wait_bars += 1
                if st.pullback_wait_bars > self._pullback_max_wait:
                    st.expansion_direction = None

            pullback_hit_ema = False
            pullback_hit_vwap = False
            pullback_valid = False
            direction = st.expansion_direction
            if direction is not None:
                if direction == Side.BUY:
                    if self._pullback_use_ema21 and ema_pullback is not None:
                        pullback_hit_ema = signal_low <= float(ema_pullback) and signal_close >= float(ema_pullback)
                    if self._pullback_use_session_vwap and signal_vwap is not None:
                        pullback_hit_vwap = signal_low <= float(signal_vwap) and signal_close >= float(signal_vwap)
                    pullback_valid = (pullback_hit_ema or pullback_hit_vwap) and long_bias
                else:
                    if self._pullback_use_ema21 and ema_pullback is not None:
                        pullback_hit_ema = signal_high >= float(ema_pullback) and signal_close <= float(ema_pullback)
                    if self._pullback_use_session_vwap and signal_vwap is not None:
                        pullback_hit_vwap = signal_high >= float(signal_vwap) and signal_close <= float(signal_vwap)
                    pullback_valid = (pullback_hit_ema or pullback_hit_vwap) and short_bias

            if direction is not None and pullback_valid and atr_value is not None and atr_value > 0 and self._no_pyramiding:
                side = direction
                entry_ref = float(bar.close)
                stop_distance = self._stop_atr_mult * float(atr_value)
                stop_price = entry_ref - stop_distance if side == Side.BUY else entry_ref + stop_distance
                tp1_target = entry_ref + (self._partial_at_r * stop_distance) if side == Side.BUY else entry_ref - (self._partial_at_r * stop_distance)

                st.entry_signal_ts = signal_bar.ts
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
                st.expansion_direction = None

                signals.append(
                    Signal(
                        ts=ts,
                        symbol=symbol,
                        side=side,
                        signal_type="l1_h7_entry",
                        confidence=1.0,
                        metadata={
                            "strategy": "l1_h7_squeeze_expansion_pullback",
                            "family_variant": self._family_variant,
                            "family_pattern": "squeeze_expansion_pullback",
                            "entry_reason": "squeeze_expansion_pullback_long" if side == Side.BUY else "squeeze_expansion_pullback_short",
                            "signal_timeframe": self._timeframe,
                            "exit_monitoring_timeframe": "1m",
                            "base_data_frequency_expected": "1m",
                            "risk_accounting": "engine_canonical_R",
                            "no_pyramiding": self._no_pyramiding,
                            "squeeze_on": squeeze_on,
                            "squeeze_duration": squeeze_duration,
                            "squeeze_state": "qualified_then_expanded" if expansion_trigger else "expanded_wait_pullback",
                            "expansion_trigger_state": expansion_trigger,
                            "breakout_direction": "long" if side == Side.BUY else "short",
                            "directional_bias": "long" if long_bias else "short" if short_bias else "flat",
                            "adx": adx_value,
                            "adx_min": self._adx_min,
                            "ema_fast": ema_fast,
                            "ema_slow": ema_slow,
                            "ema_pullback": ema_pullback,
                            "session_vwap": signal_vwap,
                            "pullback_hit_ema21": pullback_hit_ema,
                            "pullback_hit_session_vwap": pullback_hit_vwap,
                            "pullback_reference_logic": "ema21_or_session_vwap",
                            "pullback_max_wait": self._pullback_max_wait,
                            "partial_at_r": self._partial_at_r,
                            "partial_fraction": self._partial_fraction,
                            "runner_trail_atr_mult": self._trail_atr_mult,
                            "tp1_hit": False,
                            "protection_activated": False,
                            "entry_reference_price": entry_ref,
                            "entry_signal_ts": str(signal_bar.ts),
                            "stop_model": "fixed_atr_multiple",
                            "stop_update_policy": "frozen_at_entry",
                            "stop_details": {
                                "entry_price": entry_ref,
                                "atr_entry": float(atr_value),
                                "stop_atr_mult": self._stop_atr_mult,
                                "stop_mode": "atr",
                            },
                            "stop_distance": float(stop_distance),
                            "stop_price": float(stop_price),
                            "atr_entry": float(atr_value),
                        },
                    )
                )

            st.last_signal_close = signal_close

        return signals
