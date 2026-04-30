"""L1-H2B Compression + Confirmed Fade mean-reversion strategy."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.hypotheses.l1_h2 import RollingQuantileGate, bars_for_30_calendar_days
from bt.indicators.atr import ATR
from bt.indicators.vwap import SessionVWAP
from bt.strategy import register_strategy
from bt.strategy.base import Strategy
from bt.logging.decision_trace import make_decision_trace


@dataclass
class _ArmedSetup:
    armed: bool = False
    extension_ts: pd.Timestamp | None = None
    extension_z_extreme: float | None = None
    bars_since_extension: int = 0


@dataclass
class _State:
    atr_signal: ATR
    signal_vwap: SessionVWAP
    base_vwap: SessionVWAP
    gate: RollingQuantileGate
    position: Side | None = None
    entry_signal_ts: pd.Timestamp | None = None
    entry_execution_ts: pd.Timestamp | None = None
    atr_entry: float | None = None
    stop_distance_frozen: float | None = None
    stop_price_frozen: float | None = None
    signal_bars_held: int = 0
    last_signal_ts: pd.Timestamp | None = None
    setup_short: _ArmedSetup = field(default_factory=_ArmedSetup)
    setup_long: _ArmedSetup = field(default_factory=_ArmedSetup)
    touched_vwap_while_open: bool = False


@register_strategy("l1_h2b_confirmed_fade")
class L1H2BConfirmedFadeStrategy(Strategy):
    def __init__(
        self,
        *,
        timeframe: str = "5m",
        q_comp: float = 0.20,
        z_ext: float = 1.0,
        z_reentry: float = 0.4,
        k_atr: float = 1.5,
        T_hold: int = 8,
        max_concurrent_positions: int = 5,
        no_pyramiding: bool = True,
        require_reversal_close: bool = False,
    ) -> None:
        if z_reentry >= z_ext:
            raise ValueError("L1-H2B requires z_reentry < z_ext")
        self._timeframe = timeframe
        self._q_comp = float(q_comp)
        self._z_ext = float(z_ext)
        self._z_reentry = float(z_reentry)
        self._k_atr = float(k_atr)
        self._t_hold = int(T_hold)
        self._max_concurrent_positions = int(max_concurrent_positions)
        self._no_pyramiding = bool(no_pyramiding)
        self._require_reversal_close = bool(require_reversal_close)
        self._state: dict[str, _State] = {}
        self._armed_setup_count = 0
        self._confirmed_reentry_count = 0

    def strategy_artifacts(self) -> dict[str, Any]:
        return {
            "l1_h2b_mechanism": {
                "armed_setup_count": self._armed_setup_count,
                "confirmed_reentry_count": self._confirmed_reentry_count,
            }
        }

    def _state_for(self, symbol: str) -> _State:
        if symbol not in self._state:
            self._state[symbol] = _State(
                atr_signal=ATR(14),
                signal_vwap=SessionVWAP(session="utc_day", price_source="typical"),
                base_vwap=SessionVWAP(session="utc_day", price_source="typical"),
                gate=RollingQuantileGate(bars_for_30_calendar_days(self._timeframe), q=self._q_comp),
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
    def _reset_setup(setup: _ArmedSetup) -> None:
        setup.armed = False
        setup.extension_ts = None
        setup.extension_z_extreme = None
        setup.bars_since_extension = 0

    def _clear_position_state(self, st: _State) -> None:
        st.position = None
        st.entry_signal_ts = None
        st.entry_execution_ts = None
        st.atr_entry = None
        st.stop_distance_frozen = None
        st.stop_price_frozen = None
        st.signal_bars_held = 0
        st.touched_vwap_while_open = False

    def _arm_or_update_setup(self, setup: _ArmedSetup, *, ts: pd.Timestamp, z_vwap_t: float, side: str) -> None:
        if not setup.armed:
            setup.armed = True
            setup.extension_ts = ts
            setup.extension_z_extreme = float(z_vwap_t)
            setup.bars_since_extension = 0
            self._armed_setup_count += 1
            return
        setup.bars_since_extension += 1
        if side == "short":
            setup.extension_z_extreme = max(float(setup.extension_z_extreme or z_vwap_t), float(z_vwap_t))
        else:
            setup.extension_z_extreme = min(float(setup.extension_z_extreme or z_vwap_t), float(z_vwap_t))

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        signals: list[Signal] = []
        htf_root = ctx.get("htf") if isinstance(ctx, Mapping) else None
        if not isinstance(htf_root, Mapping):
            raise RuntimeError(f"L1-H2B requires ctx['htf']['{self._timeframe}'] for two-clock semantics.")
        htf_for_tf = htf_root.get(self._timeframe)
        if htf_for_tf is None:
            htf_for_tf = {}
        if not isinstance(htf_for_tf, Mapping):
            raise RuntimeError(f"L1-H2B requires mapping ctx['htf']['{self._timeframe}'] for two-clock semantics.")

        for symbol in sorted(tradeable):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            st = self._state_for(symbol)
            st.base_vwap.update(bar)

            signal_bar = htf_for_tf.get(symbol)
            has_new_signal_bar = signal_bar is not None and signal_bar.ts != st.last_signal_ts
            if has_new_signal_bar:
                sig_bar = Bar(
                    ts=signal_bar.ts,
                    symbol=signal_bar.symbol,
                    open=float(signal_bar.open),
                    high=float(signal_bar.high),
                    low=float(signal_bar.low),
                    close=float(signal_bar.close),
                    volume=float(signal_bar.volume),
                )
                st.atr_signal.update(sig_bar)
                st.signal_vwap.update(sig_bar)
                st.last_signal_ts = signal_bar.ts

            atr_v = st.atr_signal.value
            signal_close = signal_bar.close if signal_bar is not None else bar.close
            rv_t = None if atr_v is None or signal_close <= 0 else float(atr_v / signal_close)
            q_threshold_t, comp_gate_t = st.gate.update(rv_t) if has_new_signal_bar else (None, None)
            signal_vwap_t = st.signal_vwap.value
            z_vwap_t = None if atr_v in (None, 0.0) or signal_vwap_t is None else float((signal_close - signal_vwap_t) / atr_v)

            current = self._ctx_position_side(ctx, symbol)
            if current is not None:
                st.position = current
                if st.entry_execution_ts is None:
                    st.entry_execution_ts = ts
                active_base_vwap = st.base_vwap.value
                if active_base_vwap is not None:
                    if (current == Side.BUY and bar.close >= active_base_vwap) or (current == Side.SELL and bar.close <= active_base_vwap):
                        st.touched_vwap_while_open = True

                if has_new_signal_bar:
                    st.signal_bars_held += 1
                    if st.signal_bars_held >= self._t_hold:
                        signals.append(
                            Signal(
                                ts=ts,
                                symbol=symbol,
                                side=Side.SELL if current == Side.BUY else Side.BUY,
                                signal_type="l1_h2b_exit",
                                confidence=1.0,
                                metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="confirmed_fade_entry",
                            setup_class="confirmed_compression_fade",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h2b_confirmed_fade"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                                    "close_only": True,
                                    "exit_reason": "time_stop",
                                    "signal_bars_held": st.signal_bars_held,
                                    "hold_time_unit": "signal_bars",
                                    "signal_timeframe": self._timeframe,
                                    "touched_vwap_before_exit": st.touched_vwap_while_open,
                                    "holding_period_bars_signal": st.signal_bars_held,
                                },
                            )
                        )
                        self._clear_position_state(st)
                        continue

                if st.stop_price_frozen is not None:
                    if current == Side.BUY and bar.low <= st.stop_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h2b_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="confirmed_fade_entry",
                            setup_class="confirmed_compression_fade",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h2b_confirmed_fade"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),"close_only": True, "exit_reason": "stop_initial", "stop_price": st.stop_price_frozen, "entry_stop_price": st.stop_price_frozen, "entry_stop_distance": st.stop_distance_frozen, "atr_entry": st.atr_entry, "exit_monitoring_timeframe": "1m", "touched_vwap_before_exit": st.touched_vwap_while_open, "holding_period_bars_signal": st.signal_bars_held}))
                        self._clear_position_state(st)
                        continue
                    if current == Side.SELL and bar.high >= st.stop_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h2b_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="confirmed_fade_entry",
                            setup_class="confirmed_compression_fade",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h2b_confirmed_fade"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),"close_only": True, "exit_reason": "stop_initial", "stop_price": st.stop_price_frozen, "entry_stop_price": st.stop_price_frozen, "entry_stop_distance": st.stop_distance_frozen, "atr_entry": st.atr_entry, "exit_monitoring_timeframe": "1m", "touched_vwap_before_exit": st.touched_vwap_while_open, "holding_period_bars_signal": st.signal_bars_held}))
                        self._clear_position_state(st)
                        continue

                active_base_vwap = st.base_vwap.value
                if active_base_vwap is not None:
                    if current == Side.BUY and bar.close >= active_base_vwap:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h2b_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="confirmed_fade_entry",
                            setup_class="confirmed_compression_fade",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h2b_confirmed_fade"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),"close_only": True, "exit_reason": "vwap_touch", "vwap_t": active_base_vwap, "vwap_mode": "session", "exit_monitoring_timeframe": "1m", "touched_vwap_before_exit": True, "holding_period_bars_signal": st.signal_bars_held}))
                        self._clear_position_state(st)
                        continue
                    if current == Side.SELL and bar.close <= active_base_vwap:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h2b_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="confirmed_fade_entry",
                            setup_class="confirmed_compression_fade",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h2b_confirmed_fade"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),"close_only": True, "exit_reason": "vwap_touch", "vwap_t": active_base_vwap, "vwap_mode": "session", "exit_monitoring_timeframe": "1m", "touched_vwap_before_exit": True, "holding_period_bars_signal": st.signal_bars_held}))
                        self._clear_position_state(st)
                        continue
                continue

            self._clear_position_state(st)
            if not has_new_signal_bar or comp_gate_t is not True:
                self._reset_setup(st.setup_long)
                self._reset_setup(st.setup_short)
                continue
            if atr_v is None or signal_vwap_t is None or z_vwap_t is None:
                continue
            if self._position_count(ctx) >= self._max_concurrent_positions:
                continue

            # Stage B: extension arms setup.
            if z_vwap_t >= self._z_ext:
                self._arm_or_update_setup(st.setup_short, ts=signal_bar.ts, z_vwap_t=z_vwap_t, side="short")
            elif st.setup_short.armed:
                st.setup_short.bars_since_extension += 1

            if z_vwap_t <= -self._z_ext:
                self._arm_or_update_setup(st.setup_long, ts=signal_bar.ts, z_vwap_t=z_vwap_t, side="long")
            elif st.setup_long.armed:
                st.setup_long.bars_since_extension += 1

            # Stage C + D: failed expansion then inward re-entry confirmation.
            short_confirm = st.setup_short.armed and z_vwap_t <= self._z_reentry
            long_confirm = st.setup_long.armed and z_vwap_t >= -self._z_reentry
            if self._require_reversal_close:
                short_confirm = short_confirm and signal_bar.close < signal_bar.open
                long_confirm = long_confirm and signal_bar.close > signal_bar.open

            side: Side | None = None
            setup = None
            setup_side = ""
            if short_confirm:
                side = Side.SELL
                setup = st.setup_short
                setup_side = "short"
            elif long_confirm:
                side = Side.BUY
                setup = st.setup_long
                setup_side = "long"

            if side is None or setup is None:
                continue
            self._confirmed_reentry_count += 1
            delay = setup.bars_since_extension
            vwap_distance_at_entry = abs(z_vwap_t)
            stop_distance = self._k_atr * atr_v
            stop_price = bar.close - stop_distance if side == Side.BUY else bar.close + stop_distance

            st.entry_signal_ts = signal_bar.ts
            st.entry_execution_ts = None
            st.atr_entry = float(atr_v)
            st.stop_distance_frozen = float(stop_distance)
            st.stop_price_frozen = float(stop_price)
            st.signal_bars_held = 0

            signals.append(
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=side,
                    signal_type="l1_h2b_confirmed_fade",
                    confidence=1.0,
                    metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="confirmed_fade_entry",
                            setup_class="confirmed_compression_fade",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h2b_confirmed_fade"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                        "strategy": "l1_h2b_confirmed_fade",
                        "hypothesis_id": "L1-H2B",
                        "signal_timeframe": self._timeframe,
                        "exit_monitoring_timeframe": "1m",
                        "base_data_frequency_expected": "1m",
                        "hold_time_unit": "signal_bars",
                        "atr_source_timeframe": "signal_timeframe",
                        "stop_model": "fixed_atr_multiple",
                        "stop_update_policy": "frozen_at_entry",
                        "profit_exit_model": "vwap_touch",
                        "no_pyramiding": self._no_pyramiding,
                        "rv_t": rv_t,
                        "q_comp": self._q_comp,
                        "q_threshold_t": q_threshold_t,
                        "comp_gate_t": comp_gate_t,
                        "session_vwap": signal_vwap_t,
                        "vwap_mode": "session",
                        "z_vwap_t": z_vwap_t,
                        "z_ext": self._z_ext,
                        "z_reentry": self._z_reentry,
                        "extension_armed": True,
                        "extension_ts": str(setup.extension_ts),
                        "extension_z_extreme": setup.extension_z_extreme,
                        "reentry_confirmed": True,
                        "reentry_ts": str(signal_bar.ts),
                        "entry_reason": f"confirmed_fade_{setup_side}",
                        "setup_side": setup_side,
                        "extension_to_entry_delay_signal_bars": delay,
                        "max_extension_z_before_entry": setup.extension_z_extreme,
                        "z_vwap_at_entry": z_vwap_t,
                        "vwap_distance_at_entry_atr_units": vwap_distance_at_entry,
                        "atr_entry": st.atr_entry,
                        "stop_distance": st.stop_distance_frozen,
                        "stop_price": st.stop_price_frozen,
                        "entry_stop_price": st.stop_price_frozen,
                        "entry_stop_distance": st.stop_distance_frozen,
                    },
                )
            )
            self._reset_setup(st.setup_long)
            self._reset_setup(st.setup_short)

        return signals
