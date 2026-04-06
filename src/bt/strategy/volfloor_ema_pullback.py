"""Hypothesis 1 volatility-floor EMA pullback continuation strategy."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.indicators.atr import ATR
from bt.indicators.dmi_adx import DMIADX
from bt.indicators.ema import EMA
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


@dataclass
class _SymbolState:
    highs: deque[float]
    lows: deque[float]
    closes: deque[float]
    natr_history: deque[float]
    atr: ATR
    adx: DMIADX
    ema_fast: EMA
    ema_slow: EMA
    position: Side | None = None
    last_htf_ts: pd.Timestamp | None = None


@register_strategy("volfloor_ema_pullback")
class VolFloorEmaPullbackStrategy(Strategy):
    def __init__(
        self,
        *,
        seed: int | None = None,
        timeframe: str = "15m",
        adx_min: float = 18.0,
        vol_floor_pct: float = 60.0,
        atr_period: int = 14,
        vol_lookback_bars: int = 2880,
        er_lookback: int = 10,
        er_min: float | None = None,
        ema_fast_period: int = 20,
        ema_slow_period: int = 50,
        stop_atr_mult: float = 2.0,
        symbols: list[str] | None = None,
        exit_type: str = "ema_trend_end",
        chandelier_lookback: int = 22,
        chandelier_mult: float = 2.5,
    ) -> None:
        if exit_type not in {"ema_trend_end", "chandelier"}:
            raise ValueError("exit_type must be one of: ema_trend_end, chandelier")
        self._seed = seed
        self._timeframe = timeframe
        self._adx_min = adx_min
        self._vol_floor_pct = vol_floor_pct
        self._atr_period = atr_period
        self._vol_lookback_bars = vol_lookback_bars
        self._er_lookback = er_lookback
        self._er_min = er_min
        self._ema_fast_period = ema_fast_period
        self._ema_slow_period = ema_slow_period
        self._stop_atr_mult = stop_atr_mult
        self._symbols = set(symbols) if symbols is not None else None
        self._exit_type = exit_type
        self._chandelier_lookback = chandelier_lookback
        self._chandelier_mult = chandelier_mult
        self._state: dict[str, _SymbolState] = {}

    @classmethod
    def smoke_config_overrides(cls) -> dict[str, Any]:
        return {
            "strategy": {
                "timeframe": "15m",
                "adx_min": 0.0,
                "vol_floor_pct": 0.0,
                "atr_period": 3,
                "vol_lookback_bars": 10,
                "er_lookback": 3,
                "er_min": 0.0,
                "ema_fast_period": 3,
                "ema_slow_period": 5,
                "stop_atr_mult": 2.0,
                "exit_type": "ema_trend_end",
            },
            "htf_resampler": {"timeframes": ["15m"], "strict": True},
            "htf_timeframes": ["15m"],
        }

    def _state_for(self, symbol: str) -> _SymbolState:
        current = self._state.get(symbol)
        if current is None:
            current = _SymbolState(
                highs=deque(maxlen=self._chandelier_lookback),
                lows=deque(maxlen=self._chandelier_lookback),
                closes=deque(maxlen=self._er_lookback + 1),
                natr_history=deque(maxlen=self._vol_lookback_bars),
                atr=ATR(self._atr_period),
                adx=DMIADX(14),
                ema_fast=EMA(self._ema_fast_period),
                ema_slow=EMA(self._ema_slow_period),
            )
            self._state[symbol] = current
        return current

    @staticmethod
    def _percentile_rank(reference: tuple[float, ...], value: float) -> float:
        if not reference:
            return 0.0
        count = sum(1 for item in reference if item <= value)
        return (count / len(reference)) * 100.0

    def _chandelier_stop(self, *, side: Side, highs: tuple[float, ...], lows: tuple[float, ...], atr_value: float | None) -> float | None:
        if atr_value is None or len(highs) < self._chandelier_lookback or len(lows) < self._chandelier_lookback:
            return None
        if side == Side.BUY:
            return max(highs[-self._chandelier_lookback :]) - (self._chandelier_mult * atr_value)
        return min(lows[-self._chandelier_lookback :]) + (self._chandelier_mult * atr_value)


    def _efficiency_ratio(self, prev_closes: tuple[float, ...], current_close: float) -> float | None:
        if self._er_lookback <= 0:
            return None
        if len(prev_closes) < self._er_lookback:
            return None
        window = prev_closes[-self._er_lookback :] + (current_close,)
        directional_move = abs(window[-1] - window[0])
        path_length = sum(abs(window[i] - window[i - 1]) for i in range(1, len(window)))
        if path_length == 0:
            return 0.0
        return directional_move / path_length

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        signals: list[Signal] = []
        htf_ctx = ctx.get("htf", {})
        tf_bars = htf_ctx.get(self._timeframe, {}) if isinstance(htf_ctx, Mapping) else {}

        target_symbols = self._symbols if self._symbols is not None else tradeable
        for symbol in sorted(target_symbols):
            htf_bar = tf_bars.get(symbol)
            if htf_bar is None:
                continue
            live_bar = bars_by_symbol.get(symbol)
            entry_ref_price = float(live_bar.close) if live_bar is not None else float(htf_bar.close)

            state = self._state_for(symbol)
            if state.last_htf_ts is not None and htf_bar.ts <= state.last_htf_ts:
                continue
            state.last_htf_ts = htf_bar.ts

            prev_highs = tuple(state.highs)
            prev_lows = tuple(state.lows)
            prev_closes = tuple(state.closes)
            state.atr.update(htf_bar)
            state.adx.update(htf_bar)
            state.ema_fast.update(htf_bar)
            state.ema_slow.update(htf_bar)

            atr_value = state.atr.value
            adx_value = state.adx.values.get("adx")
            ema_fast = state.ema_fast.value
            ema_slow = state.ema_slow.value

            natr_value: float | None = None
            vol_rank: float | None = None
            if atr_value is not None and htf_bar.close > 0:
                natr_value = atr_value / htf_bar.close
                if len(state.natr_history) >= self._vol_lookback_bars:
                    vol_rank = self._percentile_rank(tuple(state.natr_history), natr_value)
            er_value = self._efficiency_ratio(prev_closes, float(htf_bar.close))

            base_meta = {
                "strategy": "volfloor_ema_pullback",
                "tf": self._timeframe,
                "signal_timeframe": self._timeframe,
                "exit_monitoring_timeframe": "1m",
                "exit_type": self._exit_type,
                "ema_fast": ema_fast,
                "ema_slow": ema_slow,
                "adx": adx_value,
                "efficiency_ratio": er_value,
                "er_min": self._er_min,
                "vol_pct_rank": vol_rank,
                "vol_floor_pct": self._vol_floor_pct,
                "stop_update_policy": "frozen_at_entry",
                "stop_model": "fixed_atr_multiple_at_entry",
                "chandelier": {"lookback": self._chandelier_lookback, "mult": self._chandelier_mult},
            }

            action = False
            if state.position is not None:
                should_exit = False
                if self._exit_type == "ema_trend_end" and ema_fast is not None and ema_slow is not None:
                    should_exit = (state.position == Side.BUY and ema_fast < ema_slow) or (state.position == Side.SELL and ema_fast > ema_slow)
                elif self._exit_type == "chandelier":
                    trail = self._chandelier_stop(side=state.position, highs=prev_highs, lows=prev_lows, atr_value=atr_value)
                    if trail is not None:
                        should_exit = (state.position == Side.BUY and htf_bar.close < trail) or (state.position == Side.SELL and htf_bar.close > trail)
                if should_exit:
                    exit_side = Side.SELL if state.position == Side.BUY else Side.BUY
                    signals.append(
                        Signal(
                            ts=ts,
                            symbol=symbol,
                            side=exit_side,
                            signal_type="h1_volfloor_ema_pullback_exit",
                            confidence=1.0,
                            metadata={**base_meta, "is_exit": True, "close_only": True},
                        )
                    )
                    state.position = None
                    action = True

            adx_ok = adx_value is not None and adx_value >= self._adx_min
            vol_ok = vol_rank is not None and len(state.natr_history) >= self._vol_lookback_bars and vol_rank >= self._vol_floor_pct
            trend_ready = ema_fast is not None and ema_slow is not None
            er_ok = self._er_min is None or (er_value is not None and er_value > self._er_min)
            if not action and state.position is None and adx_ok and vol_ok and er_ok and trend_ready and atr_value is not None:
                long_bias = ema_fast > ema_slow
                short_bias = ema_fast < ema_slow
                long_pullback = htf_bar.low <= ema_fast and htf_bar.close > ema_fast
                short_pullback = htf_bar.high >= ema_fast and htf_bar.close < ema_fast

                if long_bias and long_pullback:
                    stop_price = entry_ref_price - (self._stop_atr_mult * atr_value)
                    if stop_price < entry_ref_price:
                        signals.append(
                            Signal(
                                ts=ts,
                                symbol=symbol,
                                side=Side.BUY,
                                signal_type="h1_volfloor_ema_pullback_entry",
                                confidence=1.0,
                                metadata={
                                    **base_meta,
                                    "long_bias": long_bias,
                                    "short_bias": short_bias,
                                    "long_pullback": long_pullback,
                                    "short_pullback": short_pullback,
                                    "stop_price": stop_price,
                                    "stop_source": "ema_pullback_atr",
                                    "stop_details": {
                                        "entry_price": entry_ref_price,
                                        "atr_value": atr_value,
                                        "atr_stop_multiple": self._stop_atr_mult,
                                        "stop_mode": "atr",
                                    },
                                    "entry_reference_price": entry_ref_price,
                                    "stop_distance": entry_ref_price - stop_price,
                                },
                            )
                        )
                        state.position = Side.BUY
                elif short_bias and short_pullback:
                    stop_price = entry_ref_price + (self._stop_atr_mult * atr_value)
                    if stop_price > entry_ref_price:
                        signals.append(
                            Signal(
                                ts=ts,
                                symbol=symbol,
                                side=Side.SELL,
                                signal_type="h1_volfloor_ema_pullback_entry",
                                confidence=1.0,
                                metadata={
                                    **base_meta,
                                    "long_bias": long_bias,
                                    "short_bias": short_bias,
                                    "long_pullback": long_pullback,
                                    "short_pullback": short_pullback,
                                    "stop_price": stop_price,
                                    "stop_source": "ema_pullback_atr",
                                    "stop_details": {
                                        "entry_price": entry_ref_price,
                                        "atr_value": atr_value,
                                        "atr_stop_multiple": self._stop_atr_mult,
                                        "stop_mode": "atr",
                                    },
                                    "entry_reference_price": entry_ref_price,
                                    "stop_distance": stop_price - entry_ref_price,
                                },
                            )
                        )
                        state.position = Side.SELL

            state.highs.append(htf_bar.high)
            state.lows.append(htf_bar.low)
            state.closes.append(float(htf_bar.close))
            if natr_value is not None:
                state.natr_history.append(natr_value)

        return signals
