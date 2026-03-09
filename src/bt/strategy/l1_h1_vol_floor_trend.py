"""L1-H1 Volatility Floor Gates Trend Continuation strategy."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.hypotheses.l1_h1 import RollingPercentileGate, bars_for_30_calendar_days
from bt.indicators.atr import ATR
from bt.indicators.ema import EMA
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


@dataclass
class _State:
    atr: ATR
    ema_fast: EMA
    ema_slow: EMA
    gate: RollingPercentileGate
    position: Side | None = None
    entry_price: float | None = None
    bars_held: int = 0


@register_strategy("l1_h1_vol_floor_trend")
class L1H1VolFloorTrendStrategy(Strategy):
    def __init__(
        self,
        *,
        timeframe: str = "15m",
        theta_vol: float = 0.7,
        k_atr: float = 2.0,
        T_hold: int = 24,
        tp_enabled: bool = False,
        m_atr: float = 2.0,
        disallow_flip: bool = True,
    ) -> None:
        self._timeframe = timeframe
        self._theta_vol = float(theta_vol)
        self._k_atr = float(k_atr)
        self._t_hold = int(T_hold)
        self._tp_enabled = bool(tp_enabled)
        self._m_atr = float(m_atr)
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

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        signals: list[Signal] = []
        for symbol in sorted(tradeable):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            st = self._state_for(symbol)
            st.atr.update(bar)
            st.ema_fast.update(bar)
            st.ema_slow.update(bar)
            atr_v = st.atr.value
            ema_f = st.ema_fast.value
            ema_s = st.ema_slow.value
            rv_t = None if atr_v is None or bar.close <= 0 else float(atr_v / bar.close)
            vol_pct_t = st.gate.update(rv_t)

            current = self._ctx_position_side(ctx, symbol)
            if current is not None:
                st.position = current
                st.bars_held += 1
                if st.bars_held >= self._t_hold:
                    signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL if current == Side.BUY else Side.BUY, signal_type="l1_h1_exit", confidence=1.0, metadata={"close_only": True, "exit_reason": "time_stop"}))
                    st.bars_held = 0
                    continue
                if st.entry_price is not None and atr_v is not None:
                    stop_distance = self._k_atr * atr_v
                    if current == Side.BUY and bar.low <= st.entry_price - stop_distance:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h1_exit", confidence=1.0, metadata={"close_only": True, "exit_reason": "atr_stop"}))
                        st.bars_held = 0
                        continue
                    if current == Side.SELL and bar.high >= st.entry_price + stop_distance:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h1_exit", confidence=1.0, metadata={"close_only": True, "exit_reason": "atr_stop"}))
                        st.bars_held = 0
                        continue
                    if self._tp_enabled:
                        tp = self._m_atr * atr_v
                        if current == Side.BUY and bar.high >= st.entry_price + tp:
                            signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h1_exit", confidence=1.0, metadata={"close_only": True, "exit_reason": "take_profit"}))
                            st.bars_held = 0
                            continue
                        if current == Side.SELL and bar.low <= st.entry_price - tp:
                            signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h1_exit", confidence=1.0, metadata={"close_only": True, "exit_reason": "take_profit"}))
                            st.bars_held = 0
                            continue
                continue

            st.position = None
            st.bars_held = 0
            if ema_f is None or ema_s is None or atr_v is None or vol_pct_t is None:
                continue
            trend_dir_t = 1 if ema_f > ema_s else -1 if ema_f < ema_s else 0
            gate_pass = bool(vol_pct_t >= self._theta_vol)
            if trend_dir_t == 0 or not gate_pass:
                continue
            side = Side.BUY if trend_dir_t > 0 else Side.SELL
            if self._disallow_flip and current is not None and ((current == Side.BUY and side == Side.SELL) or (current == Side.SELL and side == Side.BUY)):
                continue
            stop_distance = self._k_atr * atr_v
            stop_price = bar.close - stop_distance if side == Side.BUY else bar.close + stop_distance
            st.entry_price = float(bar.close)
            signals.append(
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=side,
                    signal_type="l1_h1_vol_floor_trend",
                    confidence=1.0,
                    metadata={
                        "strategy": "l1_h1_vol_floor_trend",
                        "timeframe": self._timeframe,
                        "rv_t": rv_t,
                        "vol_pct_t": vol_pct_t,
                        "gate_pass": gate_pass,
                        "trend_dir_t": trend_dir_t,
                        "stop_distance": stop_distance,
                        "stop_price": stop_price,
                    },
                )
            )
        return signals
