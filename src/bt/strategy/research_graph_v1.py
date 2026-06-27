"""Reviewed causal runtime for the small portable research-graph subset."""
from __future__ import annotations

from collections import defaultdict, deque
import math
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.logging.decision_trace import make_decision_trace
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


def _finite(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


@register_strategy("research_graph_v1")
class ResearchGraphV1Strategy(Strategy):
    """Execute only closed-bar OHLCV graphs admitted by research_specs_v2.

    This deliberately excludes auxiliary joins and custom state machines. Those
    cards remain implementation tasks until a reviewed strategy is registered.
    """

    def __init__(self, *, research_graph: Mapping[str, Any], parameters: Mapping[str, Any] | None = None) -> None:
        self._graph = dict(research_graph)
        self._parameters = dict(parameters or {})
        self._raw_history: dict[str, dict[str, deque[float]]] = defaultdict(lambda: defaultdict(lambda: deque(maxlen=4096)))
        self._value_history: dict[str, dict[str, deque[float]]] = defaultdict(lambda: defaultdict(lambda: deque(maxlen=4096)))
        self._bars_held: dict[str, int] = {}
        self._previous_close: dict[str, float] = {}
        exit_spec = self._graph.get("exit", {})
        if not isinstance(exit_spec, Mapping) or exit_spec.get("type") != "fixed_stop_time_exit":
            raise ValueError("research_graph_v1 requires fixed_stop_time_exit")

    @staticmethod
    def _position_side(ctx: Mapping[str, Any], symbol: str) -> Side | None:
        positions = ctx.get("positions")
        raw = positions.get(symbol) if isinstance(positions, Mapping) else None
        side = raw.get("side") if isinstance(raw, Mapping) else None
        if isinstance(side, Side):
            return side
        if isinstance(side, str) and side.lower() in {"buy", "sell"}:
            return Side.BUY if side.lower() == "buy" else Side.SELL
        return None

    def _window(self, feature: Mapping[str, Any]) -> int:
        raw = feature.get("window", 1)
        if isinstance(raw, str):
            raw = self._parameters.get(raw, 1)
        return max(1, int(raw))

    def _feature(self, symbol: str, feature: Mapping[str, Any], bar: Bar, values: Mapping[str, float]) -> float | None:
        transform = str(feature.get("transform"))
        source = str(feature.get("field", feature.get("source_field", "close")))
        raw = _finite(getattr(bar, source, None))
        if transform == "identity":
            return raw
        if transform == "true_range":
            prev = self._previous_close.get(symbol, bar.close)
            return max(bar.high - bar.low, abs(bar.high - prev), abs(bar.low - prev))
        if transform == "half_range_over_close":
            return 0.5 * (bar.high - bar.low) / bar.close if bar.close else None
        if transform == "return":
            prev = self._previous_close.get(symbol)
            return bar.close / prev - 1.0 if prev not in (None, 0) else None

        inputs = feature.get("inputs", [])
        base = values.get(str(inputs[0])) if isinstance(inputs, list) and inputs else raw
        history = self._raw_history[symbol][str(feature["id"])]
        window = self._window(feature)
        prior = list(history)[-window:]
        if transform == "sma":
            if base is None:
                return None
            result = sum([*prior, base]) / (len(prior) + 1)
            history.append(base)
            return result
        if transform == "ema":
            if base is None:
                return None
            previous = prior[-1] if prior else base
            alpha = 2.0 / (window + 1.0)
            result = alpha * base + (1.0 - alpha) * previous
            history.append(result)
            return result
        if transform == "atr":
            prev = self._previous_close.get(symbol, bar.close)
            tr = max(bar.high - bar.low, abs(bar.high - prev), abs(bar.low - prev))
            result = sum([*prior, tr]) / (len(prior) + 1)
            history.append(tr)
            return result
        if transform == "true_range_over":
            denominator = values.get(str(inputs[0])) if isinstance(inputs, list) and inputs else None
            prev = self._previous_close.get(symbol, bar.close)
            tr = max(bar.high - bar.low, abs(bar.high - prev), abs(bar.low - prev))
            return tr / denominator if denominator not in (None, 0) else None
        if transform == "zscore" and base is not None and len(prior) >= 2:
            mean = sum(prior) / len(prior)
            variance = sum((item - mean) ** 2 for item in prior) / len(prior)
            result = (base - mean) / math.sqrt(variance) if variance > 0 else None
            history.append(base)
            return result
        if transform == "percentile_rank" and base is not None and prior:
            result = sum(item <= base for item in prior) / len(prior)
            history.append(base)
            return result
        if base is not None and transform in {"zscore", "percentile_rank"}:
            history.append(base)
        return None

    def _gates(self, values: Mapping[str, float]) -> tuple[bool, dict[str, bool], dict[str, float]]:
        conditions: dict[str, bool] = {}
        thresholds: dict[str, float] = {}
        for index, gate in enumerate(self._graph.get("gates", [])):
            if not isinstance(gate, Mapping):
                return False, conditions, thresholds
            left_name = str(gate.get("left", gate.get("field", "")))
            left = values.get(left_name)
            right = gate.get("right")
            if right is None:
                right = self._parameters.get(str(gate.get("right_param", gate.get("param", ""))))
            threshold = _finite(right)
            op = str(gate.get("op"))
            passed = left is not None and threshold is not None and {
                ">": left > threshold if threshold is not None else False,
                ">=": left >= threshold if threshold is not None else False,
                "<": left < threshold if threshold is not None else False,
                "<=": left <= threshold if threshold is not None else False,
                "==": left == threshold if threshold is not None else False,
            }.get(op, False)
            key = f"gate_{index}_{left_name}"
            conditions[key] = passed
            if threshold is not None:
                thresholds[left_name] = threshold
        return bool(conditions) and all(conditions.values()), conditions, thresholds

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        signals: list[Signal] = []
        exit_spec = self._graph["exit"]
        max_hold = int(self._parameters.get(str(exit_spec.get("max_hold_param", "max_hold_bars")), exit_spec.get("max_hold_bars", 60)))
        stop_multiple = float(self._parameters.get(str(exit_spec.get("stop_param", "stop_atr_multiple")), exit_spec.get("stop_atr_multiple", 2.0)))
        for symbol in sorted(tradeable):
            bar = bars_by_symbol.get(symbol)
            if bar is None:  # Missing bar means no state advance and no decision.
                continue
            side = self._position_side(ctx, symbol)
            if side is not None:
                held = self._bars_held.get(symbol, 0) + 1
                self._bars_held[symbol] = held
                if held >= max_hold:
                    signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL if side == Side.BUY else Side.BUY, signal_type="research_graph_time_exit", confidence=1.0, metadata={"strategy": "research_graph_v1", "close_only": True, "exit_reason": "max_hold_bars", "bars_held": held}))
                self._previous_close[symbol] = bar.close
                continue
            self._bars_held.pop(symbol, None)
            values: dict[str, float] = {}
            for feature in self._graph.get("features", []):
                if not isinstance(feature, Mapping):
                    continue
                current = self._feature(symbol, feature, bar, values)
                feature_id = str(feature["id"])
                value_history = self._value_history[symbol][feature_id]
                lag = max(0, int(feature.get("lag", 0)))
                output = list(value_history)[-lag] if lag > 0 and len(value_history) >= lag else current if lag == 0 else None
                if current is not None:
                    value_history.append(current)
                if output is not None:
                    values[feature_id] = output
            passed, conditions, thresholds = self._gates(values)
            if passed:
                direction = str(self._graph.get("entry", {}).get("direction", "bar_direction"))
                entry_side = Side.BUY if direction == "long" or (direction == "bar_direction" and bar.close >= bar.open) else Side.SELL
                atr_value = next((value for key, value in values.items() if key.lower().startswith("atr")), bar.high - bar.low)
                stop_distance = max(abs(float(atr_value)) * stop_multiple, 1e-12)
                trace = make_decision_trace("portable_graph_entry", "research_graph_v1", conditions_bool_map=conditions, gate_values=values, gate_thresholds=thresholds)
                signals.append(Signal(ts=ts, symbol=symbol, side=entry_side, signal_type="research_graph_entry", confidence=1.0, metadata={"strategy": "research_graph_v1", "stop_distance": stop_distance, "stop_price": bar.close - stop_distance if entry_side == Side.BUY else bar.close + stop_distance, "decision_trace": trace, "feature_values": values, "compiler_version": "research_graph_compiler_v1"}))
                self._bars_held[symbol] = 0
            self._previous_close[symbol] = bar.close
        return signals
