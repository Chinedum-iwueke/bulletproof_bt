"""L1-H11 quality-filtered continuation strategy variants (A/B/C)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.hypotheses.l1_h11 import entry_position_ratio, pullback_depth_atr, swing_distance_atr
from bt.indicators.atr import ATR
from bt.indicators.dmi_adx import DMIADX
from bt.indicators.ema import EMA
from bt.indicators.vwap import SessionVWAP
from bt.strategy import register_strategy
from bt.strategy.base import Strategy
from bt.logging.decision_trace import make_decision_trace


@dataclass
class _State:
    ema_fast: EMA
    ema_slow: EMA
    adx: DMIADX
    atr_signal: ATR
    exec_vwap: SessionVWAP
    position: Side | None = None
    last_signal_ts: pd.Timestamp | None = None
    trend_dir: Side | None = None
    trend_anchor_price: float | None = None
    trend_extreme_price: float | None = None
    pullback_active: bool = False
    pullback_extreme_low: float | None = None
    pullback_extreme_high: float | None = None
    pullback_signal_ts: pd.Timestamp | None = None
    entry_price: float | None = None
    atr_entry: float | None = None
    stop_price_frozen: float | None = None
    stop_distance_frozen: float | None = None
    lock_armed: bool = False
    signal_bars_held: int = 0


@register_strategy("l1_h11_quality_filtered_continuation")
class L1H11QualityFilteredContinuationStrategy(Strategy):
    def __init__(
        self,
        *,
        timeframe: str = "15m",
        adx_min: float = 20.0,
        adx_min_fixed: float = 20.0,
        pull_entry_atr_low: float = 0.35,
        pull_entry_atr_high: float = 1.0,
        impulse_min_atr_fixed: float = 1.0,
        swing_distance_atr: float = 1.0,
        stop_atr_mult: float = 2.0,
        stop_padding_atr: float = 0.0,
        lock_r: float = 1.0,
        vwap_giveback: str = "off",
        family_variant: str = "L1-H11A",
        setup_type: str = "quality_filtered_continuation",
    ) -> None:
        self._timeframe = str(timeframe)
        self._adx_min = float(adx_min)
        self._adx_min_fixed = float(adx_min_fixed)
        self._pull_entry_atr_low = float(pull_entry_atr_low)
        self._pull_entry_atr_high = float(pull_entry_atr_high)
        self._impulse_min_atr_fixed = float(impulse_min_atr_fixed)
        self._swing_distance_atr = float(swing_distance_atr)
        self._stop_atr_mult = float(stop_atr_mult)
        self._stop_padding_atr = float(stop_padding_atr)
        self._lock_r = float(lock_r)
        self._vwap_giveback = str(vwap_giveback).lower()
        self._family_variant = str(family_variant)
        self._setup_type = str(setup_type)
        self._state: dict[str, _State] = {}

    def _state_for(self, symbol: str) -> _State:
        st = self._state.get(symbol)
        if st is None:
            st = _State(
                ema_fast=EMA(20),
                ema_slow=EMA(50),
                adx=DMIADX(14),
                atr_signal=ATR(14),
                exec_vwap=SessionVWAP(session="utc_day", price_source="typical"),
            )
            self._state[symbol] = st
        return st

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
        st.entry_price = None
        st.atr_entry = None
        st.stop_price_frozen = None
        st.stop_distance_frozen = None
        st.lock_armed = False
        st.signal_bars_held = 0

    @staticmethod
    def _clear_pullback(st: _State) -> None:
        st.pullback_active = False
        st.pullback_extreme_low = None
        st.pullback_extreme_high = None
        st.pullback_signal_ts = None

    def _impulse_threshold(self) -> float:
        if self._family_variant == "L1-H11B":
            return self._swing_distance_atr
        return self._impulse_min_atr_fixed

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        signals: list[Signal] = []
        htf_root = ctx.get("htf") if isinstance(ctx, Mapping) else None
        if not isinstance(htf_root, Mapping):
            raise RuntimeError(f"L1-H11 requires ctx['htf']['{self._timeframe}'] for two-clock semantics.")
        htf_for_tf = htf_root.get(self._timeframe) or {}
        if not isinstance(htf_for_tf, Mapping):
            raise RuntimeError(f"L1-H11 requires mapping ctx['htf']['{self._timeframe}'] for two-clock semantics.")

        for symbol in sorted(tradeable):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            st = self._state_for(symbol)
            st.exec_vwap.update(bar)

            signal_bar = htf_for_tf.get(symbol)
            has_new_signal_bar = signal_bar is not None and signal_bar.ts != st.last_signal_ts

            current = self._ctx_position_side(ctx, symbol)
            if current is not None:
                st.position = current
                if has_new_signal_bar:
                    st.signal_bars_held += 1

                if st.stop_price_frozen is not None:
                    if current == Side.BUY and bar.low <= st.stop_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h11_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="quality_filtered_continuation_entry",
                            setup_class="quality_filtered_continuation",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h11_quality_filtered_continuation"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                            "close_only": True,
                            "exit_reason": "stop_loss",
                            "stop_price": st.stop_price_frozen,
                            "stop_distance": st.stop_distance_frozen,
                            "atr_entry": st.atr_entry,
                            "signal_timeframe": self._timeframe,
                            "exit_monitoring_timeframe": "1m",
                        }))
                        self._clear_position(st)
                        continue
                    if current == Side.SELL and bar.high >= st.stop_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h11_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="quality_filtered_continuation_entry",
                            setup_class="quality_filtered_continuation",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h11_quality_filtered_continuation"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                            "close_only": True,
                            "exit_reason": "stop_loss",
                            "stop_price": st.stop_price_frozen,
                            "stop_distance": st.stop_distance_frozen,
                            "atr_entry": st.atr_entry,
                            "signal_timeframe": self._timeframe,
                            "exit_monitoring_timeframe": "1m",
                        }))
                        self._clear_position(st)
                        continue

                if self._family_variant == "L1-H11C" and st.entry_price is not None and st.stop_distance_frozen and st.stop_distance_frozen > 0:
                    if current == Side.BUY:
                        mfe_r = (float(bar.high) - float(st.entry_price)) / float(st.stop_distance_frozen)
                    else:
                        mfe_r = (float(st.entry_price) - float(bar.low)) / float(st.stop_distance_frozen)
                    if (not st.lock_armed) and mfe_r >= self._lock_r:
                        st.lock_armed = True
                        if current == Side.BUY:
                            st.stop_price_frozen = max(float(st.stop_price_frozen), float(st.entry_price))
                        else:
                            st.stop_price_frozen = min(float(st.stop_price_frozen), float(st.entry_price))

                    if self._vwap_giveback == "on" and st.lock_armed and st.exec_vwap.value is not None:
                        vwap_v = float(st.exec_vwap.value)
                        if current == Side.BUY and float(bar.close) < vwap_v:
                            signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h11_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="quality_filtered_continuation_entry",
                            setup_class="quality_filtered_continuation",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h11_quality_filtered_continuation"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                                "close_only": True,
                                "exit_reason": "vwap_giveback",
                                "vwap_giveback_mode": "on",
                                "lock_r": self._lock_r,
                                "lock_armed": st.lock_armed,
                                "signal_timeframe": self._timeframe,
                                "exit_monitoring_timeframe": "1m",
                            }))
                            self._clear_position(st)
                            continue
                        if current == Side.SELL and float(bar.close) > vwap_v:
                            signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h11_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="quality_filtered_continuation_entry",
                            setup_class="quality_filtered_continuation",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h11_quality_filtered_continuation"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                                "close_only": True,
                                "exit_reason": "vwap_giveback",
                                "vwap_giveback_mode": "on",
                                "lock_r": self._lock_r,
                                "lock_armed": st.lock_armed,
                                "signal_timeframe": self._timeframe,
                                "exit_monitoring_timeframe": "1m",
                            }))
                            self._clear_position(st)
                            continue

                if has_new_signal_bar and signal_bar is not None:
                    st.ema_fast.update(signal_bar)
                    st.ema_slow.update(signal_bar)
                    if st.ema_fast.value is not None and st.ema_slow.value is not None:
                        trend = Side.BUY if st.ema_fast.value > st.ema_slow.value else Side.SELL if st.ema_fast.value < st.ema_slow.value else None
                        if trend is not None and trend != current:
                            signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL if current == Side.BUY else Side.BUY, signal_type="l1_h11_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="quality_filtered_continuation_entry",
                            setup_class="quality_filtered_continuation",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h11_quality_filtered_continuation"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                                "close_only": True,
                                "exit_reason": "trend_failure",
                                "signal_timeframe": self._timeframe,
                                "exit_monitoring_timeframe": "1m",
                            }))
                            self._clear_position(st)
                            st.last_signal_ts = signal_bar.ts
                            continue
                    st.last_signal_ts = signal_bar.ts
                continue

            self._clear_position(st)
            if not has_new_signal_bar or signal_bar is None:
                continue
            st.last_signal_ts = signal_bar.ts
            st.ema_fast.update(signal_bar)
            st.ema_slow.update(signal_bar)
            st.adx.update(signal_bar)
            st.atr_signal.update(signal_bar)

            ema_fast = st.ema_fast.value
            ema_slow = st.ema_slow.value
            adx_v = st.adx.values.get("adx")
            atr_v = st.atr_signal.value
            if ema_fast is None or ema_slow is None or adx_v is None or atr_v is None or float(atr_v) <= 0:
                continue

            trend_dir = Side.BUY if ema_fast > ema_slow else Side.SELL if ema_fast < ema_slow else None
            if trend_dir is None:
                self._clear_pullback(st)
                continue

            if trend_dir != st.trend_dir:
                st.trend_dir = trend_dir
                st.trend_anchor_price = float(signal_bar.close)
                st.trend_extreme_price = float(signal_bar.close)
                self._clear_pullback(st)
            else:
                if trend_dir == Side.BUY:
                    st.trend_extreme_price = max(float(st.trend_extreme_price or signal_bar.close), float(signal_bar.high))
                else:
                    st.trend_extreme_price = min(float(st.trend_extreme_price or signal_bar.close), float(signal_bar.low))

            adx_threshold = self._adx_min if self._family_variant in {"L1-H11A", "L1-H11B"} else self._adx_min_fixed
            if float(adx_v) < float(adx_threshold):
                self._clear_pullback(st)
                continue

            trend_key = "long" if trend_dir == Side.BUY else "short"
            swing_atr = swing_distance_atr(
                trend_dir=trend_key,
                trend_anchor_price=float(st.trend_anchor_price or signal_bar.close),
                trend_extreme_price=float(st.trend_extreme_price or signal_bar.close),
                atr=float(atr_v),
            )
            if swing_atr is None or swing_atr < self._impulse_threshold():
                self._clear_pullback(st)
                continue

            low, high, close = float(signal_bar.low), float(signal_bar.high), float(signal_bar.close)
            touched_pullback_zone = (trend_dir == Side.BUY and low <= float(ema_fast)) or (trend_dir == Side.SELL and high >= float(ema_fast))
            if touched_pullback_zone and not st.pullback_active:
                st.pullback_active = True
                st.pullback_signal_ts = signal_bar.ts
                st.pullback_extreme_low = low
                st.pullback_extreme_high = high
                continue

            if not st.pullback_active:
                continue
            st.pullback_extreme_low = low if st.pullback_extreme_low is None else min(float(st.pullback_extreme_low), low)
            st.pullback_extreme_high = high if st.pullback_extreme_high is None else max(float(st.pullback_extreme_high), high)

            pb_depth = pullback_depth_atr(
                trend_dir=trend_key,
                ema_fast=float(ema_fast),
                pullback_extreme_low=float(st.pullback_extreme_low),
                pullback_extreme_high=float(st.pullback_extreme_high),
                atr=float(atr_v),
            )
            if pb_depth is None:
                continue
            reclaim_ok = (trend_dir == Side.BUY and close >= float(ema_fast)) or (trend_dir == Side.SELL and close <= float(ema_fast))
            in_zone = self._pull_entry_atr_low <= float(pb_depth) <= self._pull_entry_atr_high
            if not reclaim_ok or not in_zone:
                if float(pb_depth) > self._pull_entry_atr_high:
                    self._clear_pullback(st)
                continue

            entry_ref = float(bar.close)
            struct_stop_distance = None
            if trend_dir == Side.BUY and st.pullback_extreme_low is not None:
                struct_stop_distance = max(0.0, entry_ref - float(st.pullback_extreme_low))
            if trend_dir == Side.SELL and st.pullback_extreme_high is not None:
                struct_stop_distance = max(0.0, float(st.pullback_extreme_high) - entry_ref)
            atr_stop_distance = self._stop_atr_mult * float(atr_v)
            if self._family_variant == "L1-H11C":
                base_distance = max(float(struct_stop_distance or 0.0), float(atr_stop_distance))
                stop_distance = base_distance + (self._stop_padding_atr * float(atr_v))
                stop_model = "structure_plus_atr_padding"
            else:
                stop_distance = atr_stop_distance
                stop_model = "fixed_atr_multiple"
            stop_price = entry_ref - stop_distance if trend_dir == Side.BUY else entry_ref + stop_distance

            entry_pos = entry_position_ratio(
                trend_dir=trend_key,
                entry_price=entry_ref,
                pullback_extreme_low=float(st.pullback_extreme_low),
                pullback_extreme_high=float(st.pullback_extreme_high),
                trend_extreme_price=float(st.trend_extreme_price or close),
            )

            st.position = trend_dir
            st.entry_price = entry_ref
            st.atr_entry = float(atr_v)
            st.stop_distance_frozen = float(stop_distance)
            st.stop_price_frozen = float(stop_price)
            st.lock_armed = False
            st.signal_bars_held = 0

            signals.append(Signal(ts=ts, symbol=symbol, side=trend_dir, signal_type="l1_h11_entry", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="quality_filtered_continuation_entry",
                            setup_class="quality_filtered_continuation",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h11_quality_filtered_continuation"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                "strategy": "l1_h11_quality_filtered_continuation",
                "family_variant": self._family_variant,
                "parent_family": "L1-H11",
                "setup_type": self._setup_type,
                "signal_timeframe": self._timeframe,
                "exit_monitoring_timeframe": "1m",
                "entry_ts": str(ts),
                "trend_dir": trend_key,
                "ema_fast_entry": float(ema_fast),
                "ema_slow_entry": float(ema_slow),
                "adx_entry": float(adx_v),
                "atr_entry": float(atr_v),
                "impulse_strength_atr": float(swing_atr),
                "swing_distance_atr": float(swing_atr),
                "pullback_depth_atr": float(pb_depth),
                "pull_entry_atr_low": self._pull_entry_atr_low,
                "pull_entry_atr_high": self._pull_entry_atr_high,
                "entry_position_metric": entry_pos,
                "reclaim_position_metric": entry_pos,
                "continuation_trigger_state": "ema20_reclaim_confirmed",
                "entry_reference_price": entry_ref,
                "stop_distance": float(stop_distance),
                "stop_price": float(stop_price),
                "stop_padding_atr": self._stop_padding_atr if self._family_variant == "L1-H11C" else None,
                "lock_r": self._lock_r if self._family_variant == "L1-H11C" else None,
                "vwap_giveback_mode": self._vwap_giveback if self._family_variant == "L1-H11C" else None,
                "impulse_min_threshold_atr": self._impulse_threshold(),
                "entry_reason": "quality_filtered_continuation_reclaim",
                "base_data_frequency_expected": "1m",
                "risk_accounting": "engine_canonical_R",
                "no_pyramiding": True,
                "stop_model": stop_model,
                "stop_update_policy": "frozen_at_entry_then_rule_based_protection",
                "atr_source_timeframe": self._timeframe,
            }))
            self._clear_pullback(st)
        return signals
