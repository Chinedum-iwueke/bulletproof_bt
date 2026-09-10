"""Causal BTC weekend lagged-return momentum research strategy."""
from __future__ import annotations

from collections import defaultdict, deque
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.logging.decision_trace import make_decision_trace
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


@register_strategy("alpha_weekend_momentum")
class AlphaWeekendMomentumStrategy(Strategy):
    def __init__(
        self,
        *,
        momentum_lookback_bars: int = 60,
        momentum_threshold: float = 0.0025,
        stop_atr_multiple: float = 2.0,
        max_hold_bars: int = 30,
        atr_window: int = 60,
        r_per_trade: float = 0.005,
    ) -> None:
        self._lookback = int(momentum_lookback_bars)
        self._threshold = float(momentum_threshold)
        self._stop_multiple = float(stop_atr_multiple)
        self._max_hold = int(max_hold_bars)
        self._atr_window = int(atr_window)
        self._r_per_trade = float(r_per_trade)
        size = max(self._lookback, self._atr_window) + 2
        self._closes: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=size))
        self._ranges: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=size))
        self._held: dict[str, int] = {}

    @staticmethod
    def _position(ctx: Mapping[str, Any], symbol: str) -> Side | None:
        positions = ctx.get("positions")
        raw = positions.get(symbol) if isinstance(positions, Mapping) else None
        side = raw.get("side") if isinstance(raw, Mapping) else None
        if isinstance(side, Side):
            return side
        if isinstance(side, str) and side.lower() in {"buy", "sell"}:
            return Side.BUY if side.lower() == "buy" else Side.SELL
        return None

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
            closes = self._closes[symbol]
            ranges = self._ranges[symbol]
            position = self._position(ctx, symbol)
            if position is not None:
                held = self._held.get(symbol, 0) + 1
                self._held[symbol] = held
                if held >= self._max_hold:
                    signals.append(
                        Signal(
                            ts=ts,
                            symbol=symbol,
                            side=Side.SELL if position == Side.BUY else Side.BUY,
                            signal_type="alpha_weekend_momentum_time_exit",
                            confidence=1.0,
                            metadata={"strategy": "alpha_weekend_momentum", "close_only": True, "is_exit": True, "exit_reason": "max_hold_bars", "bars_held": held},
                        )
                    )
            else:
                self._held.pop(symbol, None)
                if len(closes) >= self._lookback and len(ranges) >= self._atr_window:
                    momentum = bar.close / closes[-self._lookback] - 1.0
                    atr = sum(list(ranges)[-self._atr_window:]) / self._atr_window
                    weekend = pd.Timestamp(ts).dayofweek >= 5
                    passed = weekend and abs(momentum) >= self._threshold and atr > 0
                    if passed:
                        side = Side.BUY if momentum > 0 else Side.SELL
                        stop_distance = atr * self._stop_multiple
                        stop_price = bar.close - stop_distance if side == Side.BUY else bar.close + stop_distance
                        trace = make_decision_trace(
                            reason_code="weekend_lagged_momentum",
                            setup_class="weekend_momentum",
                            hypothesis_branch="entry",
                            conditions_bool_map={"utc_weekend": weekend, "momentum_threshold": abs(momentum) >= self._threshold},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"momentum_lookback_bars": self._lookback, "momentum_threshold": self._threshold, "stop_atr_multiple": self._stop_multiple, "max_hold_bars": self._max_hold},
                            gate_values={"momentum_60m": momentum, "day_of_week": float(pd.Timestamp(ts).dayofweek), "atr_60": atr},
                            gate_thresholds={"momentum_threshold": self._threshold, "weekend_day": 5.0},
                            gate_margins={"momentum": abs(momentum) - self._threshold},
                            most_binding_gate="momentum_threshold",
                        )
                        signals.append(
                            Signal(
                                ts=ts, symbol=symbol, side=side,
                                signal_type="alpha_weekend_momentum_entry", confidence=1.0,
                                metadata={
                                    "strategy": "alpha_weekend_momentum", "strategy_id": "ALPHA-WEEKEND-MOMENTUM",
                                    "family_variant": "weekend-60m", "family_pattern": "calendar_gated_continuation",
                                    "entry_reason": "weekend_lagged_momentum", "entry_price": bar.close,
                                    "entry_reference_price": bar.close, "intended_entry_price": bar.close,
                                    "signal_timeframe": "1m", "execution_timeframe": "1m",
                                    "risk_accounting": "engine_canonical_R", "r_per_trade": self._r_per_trade,
                                    "sizing_mode": "risk_at_stop", "cap_policy": "allow_clip_with_truth",
                                    "stop_model": "atr_multiple", "stop_price": stop_price,
                                    "entry_stop_price": stop_price, "stop_distance": stop_distance,
                                    "momentum_60m": momentum, "day_of_week": pd.Timestamp(ts).day_name(),
                                    "decision_trace": trace,
                                },
                            )
                        )
                        self._held[symbol] = 0
            previous = closes[-1] if closes else bar.close
            ranges.append(max(bar.high - bar.low, abs(bar.high - previous), abs(bar.low - previous)))
            closes.append(bar.close)
        return signals
