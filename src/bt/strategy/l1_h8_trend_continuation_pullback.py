"""L1-H8 trend continuation after shallow pullback strategy family."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.indicators.atr import ATR
from bt.indicators.dmi_adx import DMIADX
from bt.indicators.ema import EMA
from bt.indicators.vwap import SessionVWAP
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


@dataclass
class _State:
    adx: DMIADX
    ema_fast: EMA
    ema_slow: EMA
    atr_signal: ATR
    signal_vwap: SessionVWAP
    position: Side | None = None
    last_signal_ts: pd.Timestamp | None = None
    signal_bars_held: int = 0
    pullback_direction: Side | None = None
    pullback_start_ts: pd.Timestamp | None = None
    pullback_bars: int = 0
    pullback_extreme_low: float | None = None
    pullback_extreme_high: float | None = None
    pullback_reclaim_ema: bool = False
    pullback_hit_ema: bool = False
    pullback_hit_vwap: bool = False
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


@register_strategy("l1_h8_trend_continuation_pullback")
class L1H8TrendContinuationPullbackStrategy(Strategy):
    def __init__(
        self,
        *,
        timeframe: str = "15m",
        adx_min: float = 20.0,
        pullback_max_bars: int = 3,
        pullback_reference_mode: str = "ema_or_vwap",
        require_break_pullback_extreme: bool = False,
        stop_atr_mult: float = 2.0,
        partial_at_r: float = 1.5,
        partial_fraction: float = 0.5,
        trail_atr_mult: float = 2.5,
        fail_fast_bars: int | None = None,
        family_variant: str = "L1-H8A",
    ) -> None:
        self._timeframe = str(timeframe)
        self._adx_min = float(adx_min)
        self._pullback_max_bars = int(pullback_max_bars)
        self._pullback_reference_mode = str(pullback_reference_mode)
        self._require_break_pullback_extreme = bool(require_break_pullback_extreme)
        self._stop_atr_mult = float(stop_atr_mult)
        self._partial_at_r = float(partial_at_r)
        self._partial_fraction = float(partial_fraction)
        self._trail_atr_mult = float(trail_atr_mult)
        self._fail_fast_bars = int(fail_fast_bars) if fail_fast_bars is not None else None
        self._family_variant = str(family_variant)
        self._state: dict[str, _State] = {}

    def _state_for(self, symbol: str) -> _State:
        current = self._state.get(symbol)
        if current is None:
            current = _State(
                adx=DMIADX(14),
                ema_fast=EMA(9),
                ema_slow=EMA(21),
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

    @staticmethod
    def _clear_pullback_state(st: _State) -> None:
        st.pullback_direction = None
        st.pullback_start_ts = None
        st.pullback_bars = 0
        st.pullback_extreme_low = None
        st.pullback_extreme_high = None
        st.pullback_reclaim_ema = False
        st.pullback_hit_ema = False
        st.pullback_hit_vwap = False

    def _reference_is_hit(self, *, direction: Side, low: float, high: float, ema_fast: float | None, vwap: float | None) -> tuple[bool, bool, bool]:
        hit_ema = False
        hit_vwap = False
        if direction == Side.BUY:
            if ema_fast is not None:
                hit_ema = low <= float(ema_fast)
            if vwap is not None:
                hit_vwap = low <= float(vwap)
        else:
            if ema_fast is not None:
                hit_ema = high >= float(ema_fast)
            if vwap is not None:
                hit_vwap = high >= float(vwap)

        if self._pullback_reference_mode == "ema_only":
            return hit_ema, hit_vwap, hit_ema
        if self._pullback_reference_mode == "vwap_only":
            return hit_ema, hit_vwap, hit_vwap
        return hit_ema, hit_vwap, (hit_ema or hit_vwap)

    def _handle_open_position(self, *, ts: pd.Timestamp, symbol: str, bar: Bar, st: _State, side: Side, has_new_signal_bar: bool) -> list[Signal]:
        signals: list[Signal] = []
        st.position = side
        st.high_since_entry = float(bar.high) if st.high_since_entry is None else max(float(st.high_since_entry), float(bar.high))
        st.low_since_entry = float(bar.low) if st.low_since_entry is None else min(float(st.low_since_entry), float(bar.low))

        if has_new_signal_bar:
            st.signal_bars_held += 1
        if self._fail_fast_bars is not None and has_new_signal_bar and not st.partial_taken and st.signal_bars_held >= self._fail_fast_bars:
            signals.append(
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=Side.SELL if side == Side.BUY else Side.BUY,
                    signal_type="l1_h8_exit",
                    confidence=1.0,
                    metadata={
                        "close_only": True,
                        "exit_reason": "fail_fast",
                        "signal_bars_held": st.signal_bars_held,
                        "fail_fast_bars": self._fail_fast_bars,
                        "exit_monitoring_timeframe": "1m",
                    },
                )
            )
            self._clear_trade_state(st)
            return signals

        if (
            not st.partial_taken
            and st.tp1_target_price is not None
            and st.stop_distance_frozen is not None
            and st.stop_distance_frozen > 0
        ):
            tp1_hit = (side == Side.BUY and bar.high >= st.tp1_target_price) or (side == Side.SELL and bar.low <= st.tp1_target_price)
            if tp1_hit:
                st.partial_taken = True
                st.protection_active = True
                signals.append(
                    Signal(
                        ts=ts,
                        symbol=symbol,
                        side=Side.SELL if side == Side.BUY else Side.BUY,
                        signal_type="l1_h8_partial",
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

        if effective_stop is None:
            return signals
        if side == Side.BUY and bar.low <= effective_stop:
            signals.append(
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=Side.SELL,
                    signal_type="l1_h8_exit",
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
                    signal_type="l1_h8_exit",
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
            raise RuntimeError(f"L1-H8 requires ctx['htf']['{self._timeframe}'] for two-clock semantics.")
        htf_for_tf = htf_root.get(self._timeframe)
        if htf_for_tf is None:
            htf_for_tf = {}
        if not isinstance(htf_for_tf, Mapping):
            raise RuntimeError(f"L1-H8 requires mapping ctx['htf']['{self._timeframe}'] for two-clock semantics.")

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
            st.signal_vwap.update(signal_bar)

            adx_value = st.adx.values.get("adx")
            ema_fast = st.ema_fast.value
            ema_slow = st.ema_slow.value
            atr_value = st.atr_signal.value
            vwap_value = st.signal_vwap.value

            if ema_fast is None or ema_slow is None or adx_value is None:
                continue
            trend_dir = Side.BUY if ema_fast > ema_slow else Side.SELL if ema_fast < ema_slow else None
            if trend_dir is None or float(adx_value) < self._adx_min:
                self._clear_pullback_state(st)
                continue

            low = float(signal_bar.low)
            high = float(signal_bar.high)
            close = float(signal_bar.close)
            pullback_hit_ema, pullback_hit_vwap, reference_hit = self._reference_is_hit(
                direction=trend_dir,
                low=low,
                high=high,
                ema_fast=ema_fast,
                vwap=vwap_value,
            )

            if st.pullback_direction is None:
                if reference_hit:
                    st.pullback_direction = trend_dir
                    st.pullback_start_ts = signal_bar.ts
                    st.pullback_bars = 0
                    st.pullback_extreme_low = low
                    st.pullback_extreme_high = high
                    st.pullback_hit_ema = pullback_hit_ema
                    st.pullback_hit_vwap = pullback_hit_vwap
                continue

            if st.pullback_direction != trend_dir:
                self._clear_pullback_state(st)
                if reference_hit:
                    st.pullback_direction = trend_dir
                    st.pullback_start_ts = signal_bar.ts
                    st.pullback_extreme_low = low
                    st.pullback_extreme_high = high
                    st.pullback_hit_ema = pullback_hit_ema
                    st.pullback_hit_vwap = pullback_hit_vwap
                continue

            st.pullback_bars += 1
            st.pullback_extreme_low = low if st.pullback_extreme_low is None else min(float(st.pullback_extreme_low), low)
            st.pullback_extreme_high = high if st.pullback_extreme_high is None else max(float(st.pullback_extreme_high), high)
            st.pullback_hit_ema = st.pullback_hit_ema or pullback_hit_ema
            st.pullback_hit_vwap = st.pullback_hit_vwap or pullback_hit_vwap
            if st.pullback_bars > self._pullback_max_bars:
                self._clear_pullback_state(st)
                continue

            reclaim_ema = (trend_dir == Side.BUY and close >= float(ema_fast)) or (trend_dir == Side.SELL and close <= float(ema_fast))
            break_extreme = True
            if self._require_break_pullback_extreme:
                if trend_dir == Side.BUY and st.pullback_extreme_high is not None:
                    break_extreme = close > float(st.pullback_extreme_high)
                elif trend_dir == Side.SELL and st.pullback_extreme_low is not None:
                    break_extreme = close < float(st.pullback_extreme_low)
            continuation_trigger = reclaim_ema and break_extreme
            if not continuation_trigger or atr_value is None or float(atr_value) <= 0:
                continue

            entry_ref = float(bar.close)
            stop_distance = self._stop_atr_mult * float(atr_value)
            stop_price = entry_ref - stop_distance if trend_dir == Side.BUY else entry_ref + stop_distance
            tp1_target = entry_ref + (self._partial_at_r * stop_distance) if trend_dir == Side.BUY else entry_ref - (self._partial_at_r * stop_distance)
            pullback_depth = (float(ema_fast) - float(st.pullback_extreme_low)) if trend_dir == Side.BUY else (float(st.pullback_extreme_high) - float(ema_fast))
            pullback_depth_atr = float(pullback_depth) / float(atr_value) if float(atr_value) > 0 else None
            pullback_prior_leg = (
                abs(float(st.pullback_extreme_high) - float(st.pullback_extreme_low))
                if st.pullback_extreme_low is not None and st.pullback_extreme_high is not None
                else None
            )
            pullback_depth_pct_of_prior_leg = (
                float(pullback_depth) / float(pullback_prior_leg)
                if pullback_prior_leg is not None and pullback_prior_leg > 0
                else None
            )
            reclaim_strength = (
                (close - float(ema_fast)) / float(atr_value)
                if trend_dir == Side.BUY and float(atr_value) > 0
                else ((float(ema_fast) - close) / float(atr_value) if float(atr_value) > 0 else None)
            )

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
                    side=trend_dir,
                    signal_type="l1_h8_entry",
                    confidence=1.0,
                    metadata={
                        "strategy": "l1_h8_trend_continuation_pullback",
                        "family_variant": self._family_variant,
                        "family_pattern": "trend_continuation_pullback",
                        "entry_reason": "trend_pullback_reclaim_long" if trend_dir == Side.BUY else "trend_pullback_reclaim_short",
                        "signal_timeframe": self._timeframe,
                        "exit_monitoring_timeframe": "1m",
                        "base_data_frequency_expected": "1m",
                        "risk_accounting": "engine_canonical_R",
                        "trend_dir": "long" if trend_dir == Side.BUY else "short",
                        "ema_fast": ema_fast,
                        "ema_slow": ema_slow,
                        "adx": adx_value,
                        "adx_min": self._adx_min,
                        "session_vwap": vwap_value,
                        "pullback_state": "active_then_triggered",
                        "pullback_bars": st.pullback_bars,
                        "pullback_bars_used": st.pullback_bars,
                        "pullback_max_bars": self._pullback_max_bars,
                        "pullback_depth": float(pullback_depth),
                        "pullback_depth_atr": pullback_depth_atr,
                        "pullback_depth_pct_of_prior_leg": pullback_depth_pct_of_prior_leg,
                        "pullback_hit_ema": st.pullback_hit_ema,
                        "pullback_hit_vwap": st.pullback_hit_vwap,
                        "pullback_reference_mode": self._pullback_reference_mode,
                        "reference_hit": "ema" if st.pullback_hit_ema and not st.pullback_hit_vwap else "vwap" if st.pullback_hit_vwap and not st.pullback_hit_ema else "ema_or_vwap",
                        "pullback_reference_hit": "ema" if st.pullback_hit_ema and not st.pullback_hit_vwap else "vwap" if st.pullback_hit_vwap and not st.pullback_hit_ema else "ema_or_vwap",
                        "continuation_trigger": "reclaim_ema_fast" if not self._require_break_pullback_extreme else "reclaim_ema_fast_and_break_pullback_extreme",
                        "reclaim_strength": reclaim_strength,
                        "require_break_pullback_extreme": self._require_break_pullback_extreme,
                        "partial_at_r": self._partial_at_r,
                        "partial_fraction": self._partial_fraction,
                        "trail_atr_mult": self._trail_atr_mult,
                        "fail_fast_bars": self._fail_fast_bars,
                        "entry_signal_ts": str(signal_bar.ts),
                        "entry_reference_price": entry_ref,
                        "stop_model": "fixed_atr_multiple",
                        "stop_update_policy": "frozen_at_entry",
                        "stop_distance": float(stop_distance),
                        "stop_price": float(stop_price),
                        "stop_details": {
                            "entry_price": entry_ref,
                            "atr_entry": float(atr_value),
                            "stop_atr_mult": self._stop_atr_mult,
                            "stop_mode": "atr",
                        },
                    },
                )
            )
            self._clear_pullback_state(st)

        return signals
