"""L2-H4 prior-day high/low liquidity-level trap strategy."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.hypotheses.l1_h1 import RollingPercentileGate, bars_for_30_calendar_days
from bt.hypotheses.l1_h2 import RollingQuantileGate
from bt.indicators.atr import ATR
from bt.logging.decision_trace import make_decision_trace
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


@dataclass
class _State:
    atr_signal: ATR
    vol_gate: RollingPercentileGate
    compression_gate: RollingQuantileGate
    last_signal_ts: pd.Timestamp | None = None
    last_signal_close: float | None = None
    last_daily_ts: pd.Timestamp | None = None
    pdh: float | None = None
    pdl: float | None = None
    pd_anchor_id: str | None = None
    attempted_days: set[str] = field(default_factory=set)
    entry_side: Side | None = None
    signal_bars_held: int = 0
    stop_price_frozen: float | None = None
    stop_distance_frozen: float | None = None
    atr_entry: float | None = None


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
        lower = value.lower()
        if lower in {"buy", "long"}:
            return Side.BUY
        if lower in {"sell", "short"}:
            return Side.SELL
    return None


@register_strategy("l2_h4_prior_day_extreme_traps")
class L2H4PriorDayExtremeTrapsStrategy(Strategy):
    """Trade prior-day extremes as regime-conditioned coordination levels."""

    def __init__(
        self,
        *,
        timeframe: str = "5m",
        epsilon_atr: float = 0.25,
        delta_atr: float = 0.10,
        k_atr: float = 2.0,
        theta_vol: float = 0.70,
        q_comp: float = 0.20,
        T_hold: int = 12,
        no_pyramiding: bool = True,
        family_variant: str = "L2-H4",
    ) -> None:
        self._timeframe = str(timeframe).lower()
        self._epsilon_atr = float(epsilon_atr)
        self._delta_atr = float(delta_atr)
        self._k_atr = float(k_atr)
        self._theta_vol = float(theta_vol)
        self._q_comp = float(q_comp)
        self._t_hold = int(T_hold)
        self._no_pyramiding = bool(no_pyramiding)
        self._family_variant = str(family_variant)
        self._state: dict[str, _State] = {}
        if self._timeframe not in {"5m", "15m", "1h"}:
            raise ValueError("L2-H4 supports timeframe in {'5m', '15m', '1h'}")
        if self._epsilon_atr <= 0 or self._delta_atr <= 0 or self._k_atr <= 0:
            raise ValueError("epsilon_atr, delta_atr, and k_atr must be positive")

    def _state_for(self, symbol: str) -> _State:
        st = self._state.get(symbol)
        if st is None:
            lookback = bars_for_30_calendar_days(self._timeframe)
            st = _State(
                atr_signal=ATR(14),
                vol_gate=RollingPercentileGate(lookback, include_current=True),
                compression_gate=RollingQuantileGate(lookback, q=self._q_comp),
            )
            self._state[symbol] = st
        return st

    @staticmethod
    def _day_id(ts: pd.Timestamp) -> str:
        return str(ts.floor("D").date())

    def _update_prior_day(self, symbol: str, ctx: Mapping[str, Any], st: _State) -> None:
        htf = ctx.get("htf")
        if not isinstance(htf, Mapping):
            return
        daily_by_symbol = htf.get("1d")
        if not isinstance(daily_by_symbol, Mapping):
            return
        daily = daily_by_symbol.get(symbol)
        if daily is None:
            return
        daily_ts = pd.Timestamp(getattr(daily, "ts"))
        if st.last_daily_ts is not None and daily_ts <= st.last_daily_ts:
            return
        st.last_daily_ts = daily_ts
        st.pdh = float(daily.high)
        st.pdl = float(daily.low)
        st.pd_anchor_id = self._day_id(daily_ts)

    def _signal_bar(self, symbol: str, current_bar: Bar, ctx: Mapping[str, Any]) -> Bar | None:
        htf = ctx.get("htf")
        if not isinstance(htf, Mapping):
            return None
        by_symbol = htf.get(self._timeframe)
        if not isinstance(by_symbol, Mapping):
            return None
        candidate = by_symbol.get(symbol)
        return candidate if candidate is not None and hasattr(candidate, "close") else None

    def _trace(
        self,
        *,
        branch: str,
        conditions: dict[str, bool],
        blockers: dict[str, bool] | None = None,
        gate_values: dict[str, Any] | None = None,
        gate_thresholds: dict[str, Any] | None = None,
        most_binding_gate: str | None = None,
    ):
        return make_decision_trace(
            reason_code="prior_day_extreme_regime_trap",
            setup_class="prior_day_liquidity_level_trap",
            hypothesis_branch=branch,
            conditions_bool_map=conditions,
            blockers_bool_map=blockers or {},
            permission_layer_state={"pdh_pdl_source": "strict_closed_1d", "attempt_limit": "one_per_utc_day_symbol"},
            parameter_combination={
                "strategy": "l2_h4_prior_day_extreme_traps",
                "epsilon_atr": self._epsilon_atr,
                "delta_atr": self._delta_atr,
                "k_atr": self._k_atr,
                "theta_vol": self._theta_vol,
                "q_comp": self._q_comp,
            },
            gate_values=gate_values or {},
            gate_thresholds=gate_thresholds or {},
            gate_margins={},
            most_binding_gate=most_binding_gate,
        )

    def _exit_signal(self, *, ts: pd.Timestamp, symbol: str, side: Side, reason: str, st: _State) -> Signal:
        return Signal(
            ts=ts,
            symbol=symbol,
            side=Side.SELL if side == Side.BUY else Side.BUY,
            signal_type="l2_h4_exit",
            confidence=1.0,
            metadata={
                "decision_trace": self._trace(branch="exit", conditions={}),
                "strategy": "l2_h4_prior_day_extreme_traps",
                "close_only": True,
                "is_exit": True,
                "exit_reason": reason,
                "signal_timeframe": self._timeframe,
                "exit_monitoring_timeframe": "1m",
                "hold_time_unit": "signal_bars",
                "signal_bars_held": st.signal_bars_held,
                "stop_price": st.stop_price_frozen,
                "stop_distance": st.stop_distance_frozen,
                "atr_entry": st.atr_entry,
            },
        )

    @staticmethod
    def _clear_trade(st: _State) -> None:
        st.entry_side = None
        st.signal_bars_held = 0
        st.stop_price_frozen = None
        st.stop_distance_frozen = None
        st.atr_entry = None

    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        signals: list[Signal] = []
        for symbol in sorted(tradeable):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            st = self._state_for(symbol)
            self._update_prior_day(symbol, ctx, st)
            signal_bar = self._signal_bar(symbol, bar, ctx)
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
                st.last_signal_ts = signal_bar.ts

            atr_v = st.atr_signal.value
            signal_close = float(signal_bar.close) if signal_bar is not None else float(bar.close)
            rv_t = None if atr_v is None or signal_close <= 0 else float(atr_v / signal_close)
            vol_pct_t = st.vol_gate.update(rv_t) if has_new_signal_bar else None
            q_threshold_t, comp_gate_t = st.compression_gate.update(rv_t) if has_new_signal_bar else (None, None)

            current = _ctx_position_side(ctx, symbol)
            if current is not None:
                st.entry_side = current
                if has_new_signal_bar:
                    st.signal_bars_held += 1
                    if st.signal_bars_held >= self._t_hold:
                        signals.append(self._exit_signal(ts=ts, symbol=symbol, side=current, reason="time_stop", st=st))
                        self._clear_trade(st)
                        continue
                if st.stop_price_frozen is not None:
                    if current == Side.BUY and bar.low <= st.stop_price_frozen:
                        signals.append(self._exit_signal(ts=ts, symbol=symbol, side=current, reason="atr_stop", st=st))
                        self._clear_trade(st)
                        continue
                    if current == Side.SELL and bar.high >= st.stop_price_frozen:
                        signals.append(self._exit_signal(ts=ts, symbol=symbol, side=current, reason="atr_stop", st=st))
                        self._clear_trade(st)
                        continue
                continue

            self._clear_trade(st)
            if not has_new_signal_bar or atr_v is None or st.pdh is None or st.pdl is None:
                continue

            day_id = self._day_id(signal_bar.ts)
            if day_id in st.attempted_days:
                continue

            epsilon_distance = self._epsilon_atr * atr_v
            delta_distance = self._delta_atr * atr_v
            dist_to_pdh = float(signal_close - st.pdh)
            dist_to_pdl = float(signal_close - st.pdl)
            abs_dist_to_nearest = min(abs(dist_to_pdh), abs(dist_to_pdl))
            near_level = abs_dist_to_nearest <= epsilon_distance
            high_vol_gate_t = vol_pct_t is not None and vol_pct_t >= self._theta_vol
            compression_gate_t = comp_gate_t is True

            side: Side | None = None
            trigger_type: str | None = None
            regime_chosen: str | None = None
            level_name: str | None = None
            level_price: float | None = None

            if high_vol_gate_t:
                if signal_close >= st.pdh + delta_distance and abs(signal_close - st.pdh) <= epsilon_distance:
                    side = Side.BUY
                    trigger_type = "pdh_breakout_continuation"
                    regime_chosen = "high_vol_breakout"
                    level_name = "PDH"
                    level_price = st.pdh
                elif signal_close <= st.pdl - delta_distance and abs(signal_close - st.pdl) <= epsilon_distance:
                    side = Side.SELL
                    trigger_type = "pdl_breakout_continuation"
                    regime_chosen = "high_vol_breakout"
                    level_name = "PDL"
                    level_price = st.pdl

            if side is None and compression_gate_t and near_level:
                if signal_bar.high >= st.pdh and signal_close < st.pdh:
                    side = Side.SELL
                    trigger_type = "pdh_rejection_fade"
                    regime_chosen = "compression_fade"
                    level_name = "PDH"
                    level_price = st.pdh
                elif signal_bar.low <= st.pdl and signal_close > st.pdl:
                    side = Side.BUY
                    trigger_type = "pdl_rejection_fade"
                    regime_chosen = "compression_fade"
                    level_name = "PDL"
                    level_price = st.pdl

            if side is None:
                continue
            if self._no_pyramiding and current is not None:
                continue

            stop_distance = self._k_atr * atr_v
            stop_price = bar.close - stop_distance if side == Side.BUY else bar.close + stop_distance
            st.entry_side = side
            st.atr_entry = float(atr_v)
            st.stop_distance_frozen = float(stop_distance)
            st.stop_price_frozen = float(stop_price)
            st.signal_bars_held = 0
            st.attempted_days.add(day_id)

            signals.append(
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=side,
                    signal_type="l2_h4_prior_day_extreme_traps",
                    confidence=1.0,
                    metadata={
                        "decision_trace": self._trace(
                            branch="entry",
                            conditions={
                                "has_prior_day_level": True,
                                "near_prior_day_level": near_level,
                                "high_vol_gate": high_vol_gate_t,
                                "compression_gate": compression_gate_t,
                                "one_attempt_available": True,
                            },
                            blockers={"attempt_used_for_day": False},
                            gate_values={
                                "vol_pct_t": vol_pct_t,
                                "rv_t": rv_t,
                                "dist_to_pdh": dist_to_pdh,
                                "dist_to_pdl": dist_to_pdl,
                                "epsilon_distance": epsilon_distance,
                                "delta_distance": delta_distance,
                            },
                            gate_thresholds={
                                "theta_vol": self._theta_vol,
                                "q_comp_threshold_t": q_threshold_t,
                                "epsilon_atr": self._epsilon_atr,
                                "delta_atr": self._delta_atr,
                            },
                            most_binding_gate="regime_chosen",
                        ),
                        "strategy": "l2_h4_prior_day_extreme_traps",
                        "hypothesis_id": "L2-H4",
                        "family_variant": self._family_variant,
                        "signal_timeframe": self._timeframe,
                        "exit_monitoring_timeframe": "1m",
                        "base_data_frequency_expected": "1m",
                        "hold_time_unit": "signal_bars",
                        "atr_source_timeframe": "signal_timeframe",
                        "stop_model": "fixed_atr_multiple",
                        "stop_update_policy": "frozen_at_entry",
                        "no_pyramiding": self._no_pyramiding,
                        "entry_reason": trigger_type,
                        "trigger_type": trigger_type,
                        "regime_chosen": regime_chosen,
                        "pdh": st.pdh,
                        "pdl": st.pdl,
                        "prior_day_anchor_id": st.pd_anchor_id,
                        "level_name": level_name,
                        "level_price": level_price,
                        "distance_to_pdh": dist_to_pdh,
                        "distance_to_pdl": dist_to_pdl,
                        "distance_to_nearest_level": abs_dist_to_nearest,
                        "epsilon_atr": self._epsilon_atr,
                        "delta_atr": self._delta_atr,
                        "epsilon_distance": epsilon_distance,
                        "delta_distance": delta_distance,
                        "vol_pct_t": vol_pct_t,
                        "theta_vol": self._theta_vol,
                        "high_vol_gate_t": high_vol_gate_t,
                        "rv_t": rv_t,
                        "q_comp": self._q_comp,
                        "q_comp_threshold_t": q_threshold_t,
                        "comp_gate_t": comp_gate_t,
                        "entry_signal_ts": str(signal_bar.ts),
                        "attempt_day_id": day_id,
                        "one_attempt_per_day": True,
                        "atr_entry": st.atr_entry,
                        "stop_distance": st.stop_distance_frozen,
                        "stop_price": st.stop_price_frozen,
                    },
                )
            )
        return signals
