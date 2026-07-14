"""L2-H3 session VWAP reference-price reversion strategy."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.hypotheses.l1_h2 import RollingQuantileGate, bars_for_30_calendar_days
from bt.hypotheses.l1_h4a import spread_proxy_from_bar
from bt.indicators.atr import ATR
from bt.indicators.vwap import SessionVWAP
from bt.logging.decision_trace import make_decision_trace
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


@dataclass
class _State:
    atr_signal: ATR
    signal_vwap: SessionVWAP
    base_vwap: SessionVWAP
    compression_gate: RollingQuantileGate
    liquidity_gate: RollingQuantileGate
    position: Side | None = None
    entry_signal_ts: pd.Timestamp | None = None
    entry_session_key: pd.Timestamp | None = None
    atr_entry: float | None = None
    stop_distance_frozen: float | None = None
    stop_price_frozen: float | None = None
    signal_bars_held: int = 0
    last_signal_ts: pd.Timestamp | None = None


@register_strategy("l2_h3_reference_price_reversion")
class L2H3ReferencePriceReversionStrategy(Strategy):
    """Fade session-VWAP dislocations only in compression/liquid regimes."""

    def __init__(
        self,
        *,
        timeframe: str = "5m",
        z0: float = 0.8,
        k_atr: float = 1.5,
        q_comp: float = 0.20,
        q_liq: float = 0.60,
        T_hold: int = 12,
        max_concurrent_positions: int = 5,
        no_pyramiding: bool = True,
    ) -> None:
        self._timeframe = str(timeframe).lower()
        self._z0 = float(z0)
        self._k_atr = float(k_atr)
        self._q_comp = float(q_comp)
        self._q_liq = float(q_liq)
        self._t_hold = int(T_hold)
        self._max_concurrent_positions = int(max_concurrent_positions)
        self._no_pyramiding = bool(no_pyramiding)
        self._state: dict[str, _State] = {}

    def _state_for(self, symbol: str) -> _State:
        if symbol not in self._state:
            self._state[symbol] = _State(
                atr_signal=ATR(14),
                signal_vwap=SessionVWAP(session="utc_day", price_source="typical"),
                base_vwap=SessionVWAP(session="utc_day", price_source="typical"),
                compression_gate=RollingQuantileGate(bars_for_30_calendar_days(self._timeframe), q=self._q_comp),
                liquidity_gate=RollingQuantileGate(bars_for_30_calendar_days(self._timeframe), q=self._q_liq),
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
            if value.lower() in {"buy", "long"}:
                return Side.BUY
            if value.lower() in {"sell", "short"}:
                return Side.SELL
        return None

    @staticmethod
    def _position_count(ctx: Mapping[str, Any]) -> int:
        positions = ctx.get("positions")
        if not isinstance(positions, Mapping):
            return 0
        return sum(1 for payload in positions.values() if isinstance(payload, Mapping) and payload.get("side"))

    @staticmethod
    def _session_key(ts: pd.Timestamp) -> pd.Timestamp:
        return ts.floor("D")

    @staticmethod
    def _anchor_id(ts: pd.Timestamp) -> str:
        return str(ts.floor("D").date())

    @staticmethod
    def _is_utc_session_end(ts: pd.Timestamp) -> bool:
        return ts.hour == 23 and ts.minute == 59

    @staticmethod
    def _clear_position_state(st: _State) -> None:
        st.position = None
        st.entry_signal_ts = None
        st.entry_session_key = None
        st.atr_entry = None
        st.stop_distance_frozen = None
        st.stop_price_frozen = None
        st.signal_bars_held = 0

    def _decision_trace(
        self,
        *,
        conditions: dict[str, bool],
        blockers: dict[str, bool],
        gate_values: dict[str, Any],
        gate_thresholds: dict[str, Any],
        most_binding_gate: str | None,
    ):
        return make_decision_trace(
            reason_code="session_vwap_reference_reversion_entry",
            setup_class="reference_price_reversion",
            hypothesis_branch="entry",
            conditions_bool_map=conditions,
            blockers_bool_map=blockers,
            permission_layer_state={"reference_price": "session_vwap", "anchor": "utc_day"},
            parameter_combination={
                "strategy": "l2_h3_reference_price_reversion",
                "z0": self._z0,
                "k_atr": self._k_atr,
                "q_comp": self._q_comp,
                "q_liq": self._q_liq,
            },
            gate_values=gate_values,
            gate_thresholds=gate_thresholds,
            gate_margins={},
            most_binding_gate=most_binding_gate,
        )

    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        signals: list[Signal] = []
        htf_root = ctx.get("htf") if isinstance(ctx, Mapping) else None
        if not isinstance(htf_root, Mapping):
            raise RuntimeError(f"L2-H3 requires ctx['htf']['{self._timeframe}'] for two-clock semantics.")
        htf_for_tf = htf_root.get(self._timeframe) or {}
        if not isinstance(htf_for_tf, Mapping):
            raise RuntimeError(f"L2-H3 requires mapping ctx['htf']['{self._timeframe}'] for two-clock semantics.")

        for symbol in sorted(tradeable):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            st = self._state_for(symbol)
            current_session_key = self._session_key(ts)
            current = self._ctx_position_side(ctx, symbol)

            st.base_vwap.update(bar)

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
                st.atr_signal.update(signal_bar_as_base)
                st.signal_vwap.update(signal_bar_as_base)
                st.last_signal_ts = signal_bar.ts

            atr_v = st.atr_signal.value
            signal_close = float(signal_bar.close) if signal_bar is not None else float(bar.close)
            rv_t = None if atr_v is None or signal_close <= 0 else float(atr_v / signal_close)
            spread_proxy_t = spread_proxy_from_bar(signal_bar if signal_bar is not None else bar)
            comp_threshold_t, comp_gate_t = st.compression_gate.update(rv_t) if has_new_signal_bar else (None, None)
            liq_threshold_t, liq_gate_t = st.liquidity_gate.update(spread_proxy_t) if has_new_signal_bar else (None, None)
            signal_vwap_t = st.signal_vwap.value
            base_vwap_t = st.base_vwap.value
            z_vwap_t = None if atr_v in (None, 0.0) or signal_vwap_t is None else float((signal_close - signal_vwap_t) / atr_v)

            if current is not None:
                st.position = current
                session_changed = st.entry_session_key is not None and current_session_key != st.entry_session_key
                session_end = self._is_utc_session_end(ts)
                if session_changed or session_end:
                    signals.append(
                        Signal(
                            ts=ts,
                            symbol=symbol,
                            side=Side.SELL if current == Side.BUY else Side.BUY,
                            signal_type="l2_h3_exit",
                            confidence=1.0,
                            metadata={
                                "decision_trace": self._decision_trace(
                                    conditions={},
                                    blockers={},
                                    gate_values={},
                                    gate_thresholds={},
                                    most_binding_gate=None,
                                ),
                                "close_only": True,
                                "exit_reason": "session_end" if session_end else "session_rollover",
                                "anchor_id": str(st.entry_session_key.date()) if st.entry_session_key is not None else None,
                                "session_vwap": base_vwap_t,
                                "vwap_mode": "session",
                                "exit_monitoring_timeframe": "1m",
                            },
                        )
                    )
                    self._clear_position_state(st)
                    continue
                if has_new_signal_bar:
                    st.signal_bars_held += 1
                    if st.signal_bars_held >= self._t_hold:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL if current == Side.BUY else Side.BUY, signal_type="l2_h3_exit", confidence=1.0, metadata={
                            "decision_trace": self._decision_trace(conditions={}, blockers={}, gate_values={}, gate_thresholds={}, most_binding_gate=None),
                            "close_only": True, "exit_reason": "time_stop", "signal_bars_held": st.signal_bars_held, "hold_time_unit": "signal_bars", "signal_timeframe": self._timeframe}))
                        self._clear_position_state(st)
                        continue
                if st.stop_price_frozen is not None:
                    if current == Side.BUY and bar.low <= st.stop_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l2_h3_exit", confidence=1.0, metadata={
                            "decision_trace": self._decision_trace(conditions={}, blockers={}, gate_values={}, gate_thresholds={}, most_binding_gate=None),
                            "close_only": True, "exit_reason": "atr_stop", "stop_price": st.stop_price_frozen, "stop_distance": st.stop_distance_frozen, "atr_entry": st.atr_entry, "exit_monitoring_timeframe": "1m"}))
                        self._clear_position_state(st)
                        continue
                    if current == Side.SELL and bar.high >= st.stop_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l2_h3_exit", confidence=1.0, metadata={
                            "decision_trace": self._decision_trace(conditions={}, blockers={}, gate_values={}, gate_thresholds={}, most_binding_gate=None),
                            "close_only": True, "exit_reason": "atr_stop", "stop_price": st.stop_price_frozen, "stop_distance": st.stop_distance_frozen, "atr_entry": st.atr_entry, "exit_monitoring_timeframe": "1m"}))
                        self._clear_position_state(st)
                        continue
                if base_vwap_t is not None:
                    if current == Side.BUY and bar.close >= base_vwap_t:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l2_h3_exit", confidence=1.0, metadata={
                            "decision_trace": self._decision_trace(conditions={}, blockers={}, gate_values={}, gate_thresholds={}, most_binding_gate=None),
                            "close_only": True, "exit_reason": "session_vwap_touch", "session_vwap": base_vwap_t, "vwap_mode": "session", "exit_monitoring_timeframe": "1m"}))
                        self._clear_position_state(st)
                        continue
                    if current == Side.SELL and bar.close <= base_vwap_t:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l2_h3_exit", confidence=1.0, metadata={
                            "decision_trace": self._decision_trace(conditions={}, blockers={}, gate_values={}, gate_thresholds={}, most_binding_gate=None),
                            "close_only": True, "exit_reason": "session_vwap_touch", "session_vwap": base_vwap_t, "vwap_mode": "session", "exit_monitoring_timeframe": "1m"}))
                        self._clear_position_state(st)
                        continue
                continue

            self._clear_position_state(st)
            if not has_new_signal_bar:
                continue
            if atr_v is None or signal_vwap_t is None or z_vwap_t is None:
                continue
            if comp_gate_t is not True or liq_gate_t is not True:
                continue
            if self._position_count(ctx) >= self._max_concurrent_positions:
                continue

            side: Side | None = None
            entry_reason = ""
            if z_vwap_t <= -self._z0:
                side = Side.BUY
                entry_reason = "session_vwap_reference_fade_long"
            elif z_vwap_t >= self._z0:
                side = Side.SELL
                entry_reason = "session_vwap_reference_fade_short"
            if side is None:
                continue
            if self._no_pyramiding and current is not None:
                continue

            stop_distance = self._k_atr * atr_v
            stop_price = bar.close - stop_distance if side == Side.BUY else bar.close + stop_distance
            entry_session_key = self._session_key(signal_bar.ts)
            st.entry_signal_ts = signal_bar.ts
            st.entry_session_key = entry_session_key
            st.atr_entry = float(atr_v)
            st.stop_distance_frozen = float(stop_distance)
            st.stop_price_frozen = float(stop_price)
            st.signal_bars_held = 0

            conditions = {
                "has_new_signal_bar": has_new_signal_bar,
                "compression_gate": comp_gate_t is True,
                "liquidity_gate": liq_gate_t is True,
                "abs_z_ge_z0": abs(z_vwap_t) >= self._z0,
            }
            blockers = {
                "max_positions_reached": False,
                "no_pyramiding_blocked": False,
            }
            signals.append(
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=side,
                    signal_type="l2_h3_reference_price_reversion",
                    confidence=1.0,
                    metadata={
                        "decision_trace": self._decision_trace(
                            conditions=conditions,
                            blockers=blockers,
                            gate_values={
                                "z": z_vwap_t,
                                "rv_t": rv_t,
                                "spread_proxy_t": spread_proxy_t,
                                "session_vwap": signal_vwap_t,
                            },
                            gate_thresholds={
                                "z0": self._z0,
                                "q_comp_threshold_t": comp_threshold_t,
                                "q_liq_threshold_t": liq_threshold_t,
                            },
                            most_binding_gate="z0",
                        ),
                        "strategy": "l2_h3_reference_price_reversion",
                        "hypothesis_id": "L2-H3",
                        "signal_timeframe": self._timeframe,
                        "exit_monitoring_timeframe": "1m",
                        "base_data_frequency_expected": "1m",
                        "hold_time_unit": "signal_bars",
                        "atr_source_timeframe": "signal_timeframe",
                        "stop_model": "fixed_atr_multiple",
                        "stop_update_policy": "frozen_at_entry",
                        "profit_exit_model": "session_vwap_touch",
                        "hard_exit_model": "utc_session_end",
                        "no_pyramiding": self._no_pyramiding,
                        "anchor_id": str(entry_session_key.date()),
                        "session_anchor_id": str(entry_session_key.date()),
                        "session_vwap": signal_vwap_t,
                        "vwap_t": signal_vwap_t,
                        "vwap_mode": "session",
                        "z": z_vwap_t,
                        "z_vwap_t": z_vwap_t,
                        "z0": self._z0,
                        "rv_t": rv_t,
                        "q_comp": self._q_comp,
                        "q_comp_threshold_t": comp_threshold_t,
                        "comp_gate_t": comp_gate_t,
                        "spread_proxy_t": spread_proxy_t,
                        "q_liq": self._q_liq,
                        "q_liq_threshold_t": liq_threshold_t,
                        "liq_gate_t": liq_gate_t,
                        "entry_reason": entry_reason,
                        "entry_signal_ts": str(signal_bar.ts),
                        "session_hour": int(signal_bar.ts.hour),
                        "atr_entry": st.atr_entry,
                        "stop_distance": st.stop_distance_frozen,
                        "stop_price": st.stop_price_frozen,
                    },
                )
            )
        return signals
