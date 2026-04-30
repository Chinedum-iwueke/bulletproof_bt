"""L1-H4B Liquidity-gated mean reversion with capped deterministic size adjustment."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.hypotheses.l1_h4b import (
    RollingMedianReference,
    RollingQuantileGate,
    bars_for_30_calendar_days,
    capped_inverse_spread_ratio,
    spread_proxy_from_bar,
)
from bt.indicators.atr import ATR
from bt.indicators.vwap import SessionVWAP
from bt.strategy import register_strategy
from bt.strategy.base import Strategy
from bt.logging.decision_trace import make_decision_trace


@dataclass
class _State:
    atr_signal: ATR
    signal_vwap: SessionVWAP
    base_vwap: SessionVWAP
    gate: RollingQuantileGate
    spread_ref: RollingMedianReference
    position: Side | None = None
    entry_signal_ts: pd.Timestamp | None = None
    entry_execution_ts: pd.Timestamp | None = None
    atr_entry: float | None = None
    stop_distance_frozen: float | None = None
    stop_price_frozen: float | None = None
    signal_bars_held: int = 0
    last_signal_ts: pd.Timestamp | None = None


@register_strategy("l1_h4b_liquidity_gate_size_adjusted_mean_reversion")
class L1H4BLiquidityGateSizeAdjustedMeanReversionStrategy(Strategy):
    def __init__(
        self,
        *,
        timeframe: str = "5m",
        q_liq: float = 0.6,
        z0: float = 0.8,
        k_atr: float = 1.5,
        T_hold: int = 12,
        max_concurrent_positions: int = 5,
        no_pyramiding: bool = True,
        cap_multiplier: float = 0.75,
    ) -> None:
        self._timeframe = timeframe
        self._q_liq = float(q_liq)
        self._z0 = float(z0)
        self._k_atr = float(k_atr)
        self._t_hold = int(T_hold)
        self._max_concurrent_positions = int(max_concurrent_positions)
        self._no_pyramiding = bool(no_pyramiding)
        self._cap_multiplier = float(cap_multiplier)
        self._state: dict[str, _State] = {}

    def _state_for(self, symbol: str) -> _State:
        if symbol not in self._state:
            lookback = bars_for_30_calendar_days(self._timeframe)
            self._state[symbol] = _State(
                atr_signal=ATR(14),
                signal_vwap=SessionVWAP(session="utc_day", price_source="typical"),
                base_vwap=SessionVWAP(session="utc_day", price_source="typical"),
                gate=RollingQuantileGate(lookback, q=self._q_liq),
                spread_ref=RollingMedianReference(lookback),
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
    def _spread_assumptions_snapshot(ctx: Mapping[str, Any]) -> dict[str, Any] | None:
        execution = ctx.get("execution")
        if not isinstance(execution, Mapping):
            return None
        snapshot: dict[str, Any] = {}
        for key in ("profile", "spread_bps", "slippage_bps", "slippage_k_proxy"):
            if key in execution:
                snapshot[key] = execution[key]
        return snapshot or None

    @staticmethod
    def _clear_position_state(st: _State) -> None:
        st.position = None
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
            raise RuntimeError(f"L1-H4B requires ctx['htf']['{self._timeframe}'] for two-clock semantics.")
        htf_for_tf = htf_root.get(self._timeframe)
        if htf_for_tf is None:
            htf_for_tf = {}
        if not isinstance(htf_for_tf, Mapping):
            raise RuntimeError(f"L1-H4B requires mapping ctx['htf']['{self._timeframe}'] for two-clock semantics.")

        for symbol in sorted(tradeable):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            st = self._state_for(symbol)
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
            signal_close = signal_bar.close if signal_bar is not None else bar.close
            signal_vwap_t = st.signal_vwap.value
            z_vwap_t = None if atr_v in (None, 0.0) or signal_vwap_t is None else float((signal_close - signal_vwap_t) / atr_v)
            spread_proxy_t = spread_proxy_from_bar(signal_bar if signal_bar is not None else bar)
            q_threshold_t, liq_gate_t = st.gate.update(spread_proxy_t) if has_new_signal_bar else (None, None)
            spread_proxy_ref = st.spread_ref.update(spread_proxy_t) if has_new_signal_bar else None
            size_factor_t = capped_inverse_spread_ratio(
                spread_proxy_t=spread_proxy_t,
                spread_proxy_ref=spread_proxy_ref,
                cap_multiplier=self._cap_multiplier,
            ) if has_new_signal_bar else None

            current = self._ctx_position_side(ctx, symbol)
            if current is not None:
                st.position = current
                if st.entry_execution_ts is None:
                    st.entry_execution_ts = ts
                if has_new_signal_bar:
                    st.signal_bars_held += 1
                    if st.signal_bars_held >= self._t_hold:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL if current == Side.BUY else Side.BUY, signal_type="l1_h4b_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="liquidity_size_adjusted_mean_reversion_entry",
                            setup_class="liquidity_size_adjusted_mean_reversion",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h4b_liquidity_gate_size_adjusted_mean_reversion"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),"close_only": True, "exit_reason": "time_stop", "signal_bars_held": st.signal_bars_held, "hold_time_unit": "signal_bars", "signal_timeframe": self._timeframe}))
                        self._clear_position_state(st)
                        continue

                if st.stop_price_frozen is not None:
                    if current == Side.BUY and bar.low <= st.stop_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h4b_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="liquidity_size_adjusted_mean_reversion_entry",
                            setup_class="liquidity_size_adjusted_mean_reversion",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h4b_liquidity_gate_size_adjusted_mean_reversion"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),"close_only": True, "exit_reason": "atr_stop", "stop_price": st.stop_price_frozen, "stop_distance": st.stop_distance_frozen, "atr_entry": st.atr_entry, "exit_monitoring_timeframe": "1m"}))
                        self._clear_position_state(st)
                        continue
                    if current == Side.SELL and bar.high >= st.stop_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h4b_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="liquidity_size_adjusted_mean_reversion_entry",
                            setup_class="liquidity_size_adjusted_mean_reversion",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h4b_liquidity_gate_size_adjusted_mean_reversion"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),"close_only": True, "exit_reason": "atr_stop", "stop_price": st.stop_price_frozen, "stop_distance": st.stop_distance_frozen, "atr_entry": st.atr_entry, "exit_monitoring_timeframe": "1m"}))
                        self._clear_position_state(st)
                        continue

                active_base_vwap = st.base_vwap.value
                if active_base_vwap is not None:
                    if current == Side.BUY and bar.close >= active_base_vwap:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h4b_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="liquidity_size_adjusted_mean_reversion_entry",
                            setup_class="liquidity_size_adjusted_mean_reversion",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h4b_liquidity_gate_size_adjusted_mean_reversion"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),"close_only": True, "exit_reason": "vwap_touch", "vwap_t": active_base_vwap, "vwap_mode": "session", "exit_monitoring_timeframe": "1m"}))
                        self._clear_position_state(st)
                        continue
                    if current == Side.SELL and bar.close <= active_base_vwap:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h4b_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="liquidity_size_adjusted_mean_reversion_entry",
                            setup_class="liquidity_size_adjusted_mean_reversion",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h4b_liquidity_gate_size_adjusted_mean_reversion"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),"close_only": True, "exit_reason": "vwap_touch", "vwap_t": active_base_vwap, "vwap_mode": "session", "exit_monitoring_timeframe": "1m"}))
                        self._clear_position_state(st)
                        continue
                continue

            self._clear_position_state(st)
            if not has_new_signal_bar or liq_gate_t is not True:
                continue
            if atr_v is None or signal_vwap_t is None or z_vwap_t is None:
                continue
            if size_factor_t is None:
                continue
            if self._position_count(ctx) >= self._max_concurrent_positions:
                continue

            side: Side | None = None
            entry_reason = ""
            if z_vwap_t <= -self._z0:
                side = Side.BUY
                entry_reason = "liquidity_gate_size_adjusted_fade_long"
            elif z_vwap_t >= self._z0:
                side = Side.SELL
                entry_reason = "liquidity_gate_size_adjusted_fade_short"
            if side is None:
                continue
            if self._no_pyramiding and current is not None:
                continue

            stop_distance = self._k_atr * atr_v
            stop_price = bar.close - stop_distance if side == Side.BUY else bar.close + stop_distance
            st.entry_signal_ts = signal_bar.ts
            st.entry_execution_ts = None
            st.atr_entry = float(atr_v)
            st.stop_distance_frozen = float(stop_distance)
            st.stop_price_frozen = float(stop_price)
            st.signal_bars_held = 0

            equity_t = ctx.get("equity") if isinstance(ctx, Mapping) else None
            r_per_trade = self._extract_r_per_trade(ctx)
            qty_base = None
            qty_adj = None
            try:
                if equity_t is not None and r_per_trade is not None and stop_distance > 0:
                    qty_base = (float(equity_t) * float(r_per_trade)) / float(stop_distance)
                    qty_adj = float(qty_base) * float(size_factor_t)
            except (TypeError, ValueError, ZeroDivisionError):
                qty_base = None
                qty_adj = None

            signals.append(
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=side,
                    signal_type="l1_h4b_liquidity_gate_size_adjusted_mean_reversion",
                    confidence=1.0,
                    metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="liquidity_size_adjusted_mean_reversion_entry",
                            setup_class="liquidity_size_adjusted_mean_reversion",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h4b_liquidity_gate_size_adjusted_mean_reversion"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                        "strategy": "l1_h4b_liquidity_gate_size_adjusted_mean_reversion",
                        "signal_timeframe": self._timeframe,
                        "exit_monitoring_timeframe": "1m",
                        "base_data_frequency_expected": "1m",
                        "hold_time_unit": "signal_bars",
                        "atr_source_timeframe": "signal_timeframe",
                        "stop_model": "fixed_atr_multiple",
                        "stop_update_policy": "frozen_at_entry",
                        "profit_exit_model": "vwap_touch",
                        "no_pyramiding": self._no_pyramiding,
                        "vwap_t": signal_vwap_t,
                        "session_vwap_t": signal_vwap_t,
                        "vwap_mode": "session",
                        "z_vwap_t": z_vwap_t,
                        "spread_proxy_t": spread_proxy_t,
                        "spread_proxy_ref": spread_proxy_ref,
                        "q_liq": self._q_liq,
                        "q_threshold_t": q_threshold_t,
                        "liq_gate_t": liq_gate_t,
                        "entry_reason": entry_reason,
                        "entry_signal_ts": str(signal_bar.ts),
                        "atr_entry": st.atr_entry,
                        "stop_distance": st.stop_distance_frozen,
                        "stop_price": st.stop_price_frozen,
                        "size_adjustment_enabled": True,
                        "size_adjustment_model": "capped_inverse_spread_proxy_ratio",
                        "spread_proxy_reference_model": "rolling_median",
                        "cap_multiplier": self._cap_multiplier,
                        "size_factor_t": size_factor_t,
                        "qty_base": qty_base,
                        "qty_adj": qty_adj,
                        "effective_spread_assumptions": self._spread_assumptions_snapshot(ctx),
                    },
                )
            )

        return signals
