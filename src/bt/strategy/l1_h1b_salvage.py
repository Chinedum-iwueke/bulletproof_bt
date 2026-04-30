"""L1-H1B salvage fork with chandelier exit and mandatory trail diagnostics."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.hypotheses.l1_h1 import RollingPercentileGate, bars_for_30_calendar_days
from bt.indicators.atr import ATR
from bt.indicators.ema import EMA
from bt.strategy import register_strategy
from bt.strategy.base import Strategy
from bt.logging.decision_trace import make_decision_trace


@dataclass
class _State:
    atr: ATR
    ema_fast: EMA
    ema_slow: EMA
    gate: RollingPercentileGate
    signal_highs: deque[float] = field(default_factory=lambda: deque(maxlen=200))
    signal_lows: deque[float] = field(default_factory=lambda: deque(maxlen=200))
    position: Side | None = None
    entry_price: float | None = None
    entry_signal_ts: pd.Timestamp | None = None
    entry_execution_ts: pd.Timestamp | None = None
    atr_entry: float | None = None
    stop_distance_frozen: float | None = None
    stop_price_frozen: float | None = None
    signal_bars_held: int = 0
    last_signal_ts: pd.Timestamp | None = None
    trail_active: bool = False
    trail_price: float | None = None
    bars_until_trail_activation: int | None = None
    profit_r_at_trail_activation: float | None = None


@register_strategy("l1_h1b_salvage")
class L1H1BSalvageStrategy(Strategy):
    def __init__(
        self,
        *,
        timeframe: str = "15m",
        theta_vol: float = 0.75,
        k_atr_entry_stop: float = 2.5,
        T_hold: int = 48,
        tp_enabled: bool = False,
        chandelier_lookback: int = 8,
        chandelier_atr_mult: float = 3.0,
        trail_activation_mode: str = "bars",
        trail_activate_after_bars: int = 1,
        trail_activate_after_profit_r: float = 0.5,
        disallow_flip: bool = True,
    ) -> None:
        self._timeframe = timeframe
        self._theta_vol = float(theta_vol)
        self._k_atr = float(k_atr_entry_stop)
        self._t_hold = int(T_hold)
        self._tp_enabled = bool(tp_enabled)
        self._chandelier_lookback = int(chandelier_lookback)
        self._chandelier_atr_mult = float(chandelier_atr_mult)
        self._trail_activation_mode = str(trail_activation_mode)
        self._trail_activate_after_bars = int(trail_activate_after_bars)
        self._trail_activate_after_profit_r = float(trail_activate_after_profit_r)
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
        st.signal_bars_held = 0
        st.trail_active = False
        st.trail_price = None
        st.bars_until_trail_activation = None
        st.profit_r_at_trail_activation = None

    def _should_activate_trail(self, *, st: _State, side: Side, bar: Bar) -> tuple[bool, float | None]:
        if st.entry_price is None or st.stop_distance_frozen is None or st.stop_distance_frozen <= 0:
            return False, None
        profit_r = None
        if side == Side.BUY:
            profit_r = (float(bar.high) - float(st.entry_price)) / float(st.stop_distance_frozen)
        else:
            profit_r = (float(st.entry_price) - float(bar.low)) / float(st.stop_distance_frozen)

        if self._trail_activation_mode == "bars":
            return st.signal_bars_held >= self._trail_activate_after_bars, profit_r
        if self._trail_activation_mode == "profit_r":
            return profit_r >= self._trail_activate_after_profit_r, profit_r
        raise ValueError("trail_activation_mode must be one of ['bars', 'profit_r']")

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        signals: list[Signal] = []
        htf_root = ctx.get("htf") if isinstance(ctx, Mapping) else None
        if not isinstance(htf_root, Mapping):
            raise RuntimeError(f"L1-H1B requires ctx['htf']['{self._timeframe}'] for two-clock semantics.")
        htf_for_tf = htf_root.get(self._timeframe)
        if htf_for_tf is None:
            htf_for_tf = {}
        if not isinstance(htf_for_tf, Mapping):
            raise RuntimeError(f"L1-H1B requires mapping ctx['htf']['{self._timeframe}'] for two-clock semantics.")

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
                st.signal_highs.append(float(signal_bar.high))
                st.signal_lows.append(float(signal_bar.low))
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

                activate_trail, profit_r = self._should_activate_trail(st=st, side=current, bar=bar)
                if activate_trail and not st.trail_active:
                    st.trail_active = True
                    st.bars_until_trail_activation = st.signal_bars_held
                    st.profit_r_at_trail_activation = profit_r

                if st.trail_active and atr_v is not None and len(st.signal_highs) >= self._chandelier_lookback:
                    highs = list(st.signal_highs)[-self._chandelier_lookback :]
                    lows = list(st.signal_lows)[-self._chandelier_lookback :]
                    if current == Side.BUY:
                        candidate = max(highs) - (self._chandelier_atr_mult * float(atr_v))
                        st.trail_price = candidate if st.trail_price is None else max(float(st.trail_price), candidate)
                    else:
                        candidate = min(lows) + (self._chandelier_atr_mult * float(atr_v))
                        st.trail_price = candidate if st.trail_price is None else min(float(st.trail_price), candidate)

                if st.signal_bars_held >= self._t_hold and has_new_signal_bar:
                    signals.append(
                        Signal(
                            ts=ts,
                            symbol=symbol,
                            side=Side.SELL if current == Side.BUY else Side.BUY,
                            signal_type="l1_h1b_exit",
                            confidence=1.0,
                            metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="salvage_trend_entry",
                            setup_class="salvage_trend",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h1b_salvage"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                                "close_only": True,
                                "exit_reason": "time_stop",
                                "signal_bars_held": st.signal_bars_held,
                                "hold_time_unit": "signal_bars",
                                "trail_activated": st.trail_active,
                                "trail_activation_mode": self._trail_activation_mode,
                                "bars_until_trail_activation": st.bars_until_trail_activation,
                                "profit_r_at_trail_activation": st.profit_r_at_trail_activation,
                                "holding_period_bars_signal": st.signal_bars_held,
                            },
                        )
                    )
                    self._clear_position_state(st)
                    continue

                effective_stop = st.stop_price_frozen
                if st.trail_active and st.trail_price is not None:
                    if current == Side.BUY:
                        effective_stop = max(float(st.stop_price_frozen), float(st.trail_price))
                    else:
                        effective_stop = min(float(st.stop_price_frozen), float(st.trail_price))

                if effective_stop is not None:
                    if current == Side.BUY and bar.low <= effective_stop:
                        reason = "stop_chandelier" if st.trail_active and st.trail_price is not None and float(effective_stop) == float(st.trail_price) else "stop_initial"
                        signals.append(
                            Signal(
                                ts=ts,
                                symbol=symbol,
                                side=Side.SELL,
                                signal_type="l1_h1b_exit",
                                confidence=1.0,
                                metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="salvage_trend_entry",
                            setup_class="salvage_trend",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h1b_salvage"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                                    "close_only": True,
                                    "exit_reason": reason,
                                    "stop_price": float(effective_stop),
                                    "entry_stop_price": st.stop_price_frozen,
                                    "entry_stop_distance": st.stop_distance_frozen,
                                    "atr_entry": st.atr_entry,
                                    "exit_monitoring_timeframe": "1m",
                                    "trail_activated": st.trail_active,
                                    "trail_activation_mode": self._trail_activation_mode,
                                    "bars_until_trail_activation": st.bars_until_trail_activation,
                                    "profit_r_at_trail_activation": st.profit_r_at_trail_activation,
                                    "holding_period_bars_signal": st.signal_bars_held,
                                },
                            )
                        )
                        self._clear_position_state(st)
                        continue
                    if current == Side.SELL and bar.high >= effective_stop:
                        reason = "stop_chandelier" if st.trail_active and st.trail_price is not None and float(effective_stop) == float(st.trail_price) else "stop_initial"
                        signals.append(
                            Signal(
                                ts=ts,
                                symbol=symbol,
                                side=Side.BUY,
                                signal_type="l1_h1b_exit",
                                confidence=1.0,
                                metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="salvage_trend_entry",
                            setup_class="salvage_trend",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h1b_salvage"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                                    "close_only": True,
                                    "exit_reason": reason,
                                    "stop_price": float(effective_stop),
                                    "entry_stop_price": st.stop_price_frozen,
                                    "entry_stop_distance": st.stop_distance_frozen,
                                    "atr_entry": st.atr_entry,
                                    "exit_monitoring_timeframe": "1m",
                                    "trail_activated": st.trail_active,
                                    "trail_activation_mode": self._trail_activation_mode,
                                    "bars_until_trail_activation": st.bars_until_trail_activation,
                                    "profit_r_at_trail_activation": st.profit_r_at_trail_activation,
                                    "holding_period_bars_signal": st.signal_bars_held,
                                },
                            )
                        )
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
            entry_reason = "vol_floor_trend_long" if side == Side.BUY else "vol_floor_trend_short"
            if self._disallow_flip and current is not None and ((current == Side.BUY and side == Side.SELL) or (current == Side.SELL and side == Side.BUY)):
                continue
            stop_distance = self._k_atr * atr_v
            stop_price = bar.close - stop_distance if side == Side.BUY else bar.close + stop_distance

            st.entry_price = float(bar.close)
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
                    signal_type="l1_h1b_salvage",
                    confidence=1.0,
                    metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="salvage_trend_entry",
                            setup_class="salvage_trend",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h1b_salvage"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                        "strategy": "l1_h1b_salvage",
                        "hypothesis_id": "L1-H1B",
                        "timeframe": self._timeframe,
                        "signal_timeframe": self._timeframe,
                        "exit_monitoring_timeframe": "1m",
                        "hold_time_unit": "signal_bars",
                        "atr_source_timeframe": "signal_timeframe",
                        "stop_model": "fixed_atr_multiple",
                        "stop_update_policy": "frozen_at_entry",
                        "rv_t": rv_t,
                        "vol_pct_t": vol_pct_t,
                        "gate_pass": gate_pass,
                        "trend_dir_t": trend_dir_t,
                        "entry_reason": entry_reason,
                        "entry_signal_ts": str(signal_bar.ts),
                        "atr_entry": st.atr_entry,
                        "stop_distance": st.stop_distance_frozen,
                        "stop_price": st.stop_price_frozen,
                        "entry_stop_price": st.stop_price_frozen,
                        "entry_stop_distance": st.stop_distance_frozen,
                        "tp_enabled": self._tp_enabled,
                        "chandelier_lookback": self._chandelier_lookback,
                        "chandelier_atr_mult": self._chandelier_atr_mult,
                        "trail_activation_mode": self._trail_activation_mode,
                        "trail_activate_after_bars": self._trail_activate_after_bars,
                        "trail_activate_after_profit_r": self._trail_activate_after_profit_r,
                    },
                )
            )
        return signals
