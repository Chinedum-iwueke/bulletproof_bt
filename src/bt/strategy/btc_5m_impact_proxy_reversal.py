"""Causal BTC 5m return-per-quote-volume reversal research strategy."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.evaluation.alpha_research import empirical_lower_quantile
from bt.logging.decision_trace import make_decision_trace
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


@dataclass
class _QuoteBucket:
    start: pd.Timestamp
    total: float
    valid: bool
    count: int
    last_ts: pd.Timestamp
    open: float
    high: float
    low: float
    close: float

    @property
    def complete(self) -> bool:
        return (
            self.valid
            and self.count == 5
            and self.last_ts - self.start == pd.Timedelta(minutes=4)
        )


@register_strategy("btc_5m_impact_proxy_reversal")
class Btc5mImpactProxyReversalStrategy(Strategy):
    """Fade extreme completed-5m absolute return per unit quote volume."""

    SIGNAL_TIMEFRAME = "5m"
    LIQUIDITY_FLOOR_USD = 1_000_000.0
    ATR_WINDOW = 20
    STOP_ATR_MULTIPLE = 3.0
    TARGET_HORIZON = pd.Timedelta(minutes=30)
    EXECUTION_DELAY = pd.Timedelta(minutes=1)

    def __init__(
        self,
        *,
        impact_proxy_threshold: float = 0.99,
        normalization_window: int = 288,
        signal_direction: str = "both",
        return_shock_control_band: float = 0.10,
        r_per_trade: float = 0.005,
    ) -> None:
        if not 0.5 < float(impact_proxy_threshold) < 1.0:
            raise ValueError("impact_proxy_threshold must be between 0.5 and 1")
        if int(normalization_window) < 2:
            raise ValueError("normalization_window must be at least 2")
        if signal_direction not in {"both", "long_only", "short_only"}:
            raise ValueError("signal_direction must be both, long_only, or short_only")
        if not 0.0 < float(return_shock_control_band) < 1.0:
            raise ValueError("return_shock_control_band must be between 0 and 1")
        self._threshold = float(impact_proxy_threshold)
        self._window = int(normalization_window)
        self._signal_direction = signal_direction
        self._control_band = float(return_shock_control_band)
        self._r_per_trade = float(r_per_trade)
        self._ratios: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=self._window)
        )
        self._ranges: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=self.ATR_WINDOW)
        )
        self._previous_close: dict[str, float] = {}
        self._quote_bucket: dict[str, _QuoteBucket] = {}
        self._exit_submitted: set[str] = set()
        self._last_signal_bar_ts: dict[str, pd.Timestamp] = {}

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

    @staticmethod
    def _target_exit_ts(
        ctx: Mapping[str, Any], symbol: str
    ) -> pd.Timestamp | None:
        positions = ctx.get("positions")
        raw = positions.get(symbol) if isinstance(positions, Mapping) else None
        metadata = raw.get("metadata") if isinstance(raw, Mapping) else None
        value = metadata.get("target_exit_ts") if isinstance(metadata, Mapping) else None
        if value is None:
            return None
        timestamp = pd.Timestamp(value)
        if timestamp.tz is None:
            raise ValueError("target_exit_ts must be timezone aware")
        return timestamp

    @staticmethod
    def _stop_price(ctx: Mapping[str, Any], symbol: str) -> float | None:
        positions = ctx.get("positions")
        raw = positions.get(symbol) if isinstance(positions, Mapping) else None
        metadata = raw.get("metadata") if isinstance(raw, Mapping) else None
        value = metadata.get("entry_stop_price") if isinstance(metadata, Mapping) else None
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _quote_volume(bar: Bar) -> float | None:
        raw = bar.extra.get("quote_volume") if isinstance(bar.extra, Mapping) else None
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return None
        return value if value >= 0.0 else None

    def _roll_quote_bucket(
        self, bar: Bar
    ) -> _QuoteBucket | None:
        """Consume a completed 1m row and return a just-completed 5m bucket."""
        bucket = bar.ts.floor("5min")
        value = self._quote_volume(bar)
        prior = self._quote_bucket.get(bar.symbol)
        if prior is None or prior.start != bucket:
            current = _QuoteBucket(
                start=bucket,
                total=value or 0.0,
                valid=value is not None and bar.ts == bucket,
                count=1,
                last_ts=bar.ts,
                open=float(bar.open),
                high=float(bar.high),
                low=float(bar.low),
                close=float(bar.close),
            )
        else:
            sequential = bar.ts == prior.last_ts + pd.Timedelta(minutes=1)
            current = _QuoteBucket(
                start=bucket,
                total=prior.total + (value or 0.0),
                valid=prior.valid and value is not None and sequential,
                count=prior.count + 1,
                last_ts=bar.ts,
                open=prior.open,
                high=max(prior.high, float(bar.high)),
                low=min(prior.low, float(bar.low)),
                close=float(bar.close),
            )
        self._quote_bucket[bar.symbol] = current
        return current if current.complete else None

    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        signals: list[Signal] = []
        for symbol, bar in sorted(bars_by_symbol.items()):
            prior = self._quote_bucket.get(symbol)
            bucket_start = bar.ts.floor("5min")
            if (
                prior is not None
                and prior.start != bucket_start
                and (
                    not prior.complete
                    or bucket_start != prior.start + pd.Timedelta(minutes=5)
                )
            ):
                # Never turn a gap into a multi-bucket return.
                self._previous_close.pop(symbol, None)
            completed = self._roll_quote_bucket(bar)
            position = self._position(ctx, symbol)
            if position is None:
                self._exit_submitted.discard(symbol)
            target_exit = self._target_exit_ts(ctx, symbol)
            submit_exit_at = (
                target_exit - self.EXECUTION_DELAY
                if target_exit is not None
                else None
            )
            if (
                position is not None
                and symbol not in self._exit_submitted
                and target_exit is not None
                and submit_exit_at is not None
                and ts >= submit_exit_at
            ):
                signals.append(Signal(
                    ts=ts,
                    symbol=symbol,
                    side=Side.SELL if position == Side.BUY else Side.BUY,
                    signal_type="btc_5m_impact_proxy_reversal_time_exit",
                    confidence=1.0,
                    metadata={
                        "strategy": "btc_5m_impact_proxy_reversal",
                        "close_only": True,
                        "is_exit": True,
                        "exit_reason": "fixed_30m_target_horizon",
                        "target_exit_ts": target_exit.isoformat(),
                        "exit_submission_ts": pd.Timestamp(ts).isoformat(),
                        "execution_delay_minutes": 1,
                    },
                ))
                self._exit_submitted.add(symbol)

            stop_price = self._stop_price(ctx, symbol)
            if position is not None and stop_price is not None:
                breached = (
                    position == Side.BUY and float(bar.low) <= stop_price
                ) or (
                    position == Side.SELL and float(bar.high) >= stop_price
                )
                if breached and symbol not in self._exit_submitted:
                    signals.append(Signal(
                        ts=ts, symbol=symbol,
                        side=Side.SELL if position == Side.BUY else Side.BUY,
                        signal_type="btc_5m_impact_proxy_reversal_stop_exit",
                        confidence=1.0,
                        metadata={
                            "strategy": "btc_5m_impact_proxy_reversal",
                            "close_only": True, "is_exit": True,
                            "exit_reason": "fixed_completed_5m_atr_stop_breached",
                            "stop_detection_policy": "completed_1m_then_next_bar",
                        },
                    ))
                    self._exit_submitted.add(symbol)

            if completed is None:
                continue
            closed_ts = completed.start
            decision_ts = completed.start + pd.Timedelta(minutes=5)
            previous_signal_ts = self._last_signal_bar_ts.get(symbol)
            if previous_signal_ts is not None and closed_ts <= previous_signal_ts:
                continue
            self._last_signal_bar_ts[symbol] = closed_ts
            previous_close = self._previous_close.get(symbol)
            self._previous_close[symbol] = completed.close
            if previous_close is None or previous_close <= 0:
                continue
            signal_return = completed.close / previous_close - 1.0
            true_range = max(
                completed.high - completed.low,
                abs(completed.high - previous_close),
                abs(completed.low - previous_close),
            )
            ranges = self._ranges[symbol]
            ranges.append(true_range)
            if completed.total <= 0:
                continue
            ratio = abs(signal_return) / completed.total
            ratios = self._ratios[symbol]
            enough_history = len(ratios) == self._window
            threshold_value = (
                empirical_lower_quantile(ratios, self._threshold)
                if enough_history else None
            )
            liquid = completed.total >= self.LIQUIDITY_FLOOR_USD
            extreme = threshold_value is not None and ratio >= threshold_value
            flat = position is None
            proposed_side = Side.SELL if signal_return > 0 else Side.BUY
            direction_allowed = (
                self._signal_direction == "both"
                or (self._signal_direction == "long_only" and proposed_side == Side.BUY)
                or (self._signal_direction == "short_only" and proposed_side == Side.SELL)
            )
            atr_ready = len(ranges) == self.ATR_WINDOW and sum(ranges) > 0
            eligible = (
                symbol in tradeable and flat
                and signal_return != 0 and liquid
                and extreme and direction_allowed and atr_ready
            )
            if eligible:
                atr = sum(ranges) / len(ranges)
                stop_distance = atr * self.STOP_ATR_MULTIPLE
                stop_price = (
                    completed.close - stop_distance
                    if proposed_side == Side.BUY
                    else completed.close + stop_distance
                )
                percentile = sum(item <= ratio for item in ratios) / len(ratios)
                trace = make_decision_trace(
                    reason_code="extreme_5m_return_per_quote_volume_reversal",
                    setup_class="impact_proxy_reversal",
                    hypothesis_branch="opposite_direction_30m",
                    conditions_bool_map={
                        "completed_5m_bar": True,
                        "quote_volume_complete": completed.complete,
                        "liquidity_floor": liquid,
                        "impact_proxy_extreme": extreme,
                        "direction_allowed": direction_allowed,
                    },
                    blockers_bool_map={"existing_position": not flat},
                    permission_layer_state={"tradeable": symbol in tradeable},
                    parameter_combination={
                        "impact_proxy_threshold": self._threshold,
                        "normalization_window": self._window,
                        "signal_direction": self._signal_direction,
                        "return_shock_control_band": self._control_band,
                    },
                    gate_values={
                        "signal_return_5m": signal_return,
                        "quote_volume_5m": completed.total,
                        "quote_volume_bucket_ts": completed.start.isoformat(),
                        "impact_proxy": ratio,
                        "impact_proxy_percentile": percentile,
                    },
                    gate_thresholds={
                        "impact_proxy_quantile": float(threshold_value),
                        "liquidity_floor_usd": self.LIQUIDITY_FLOOR_USD,
                    },
                    gate_margins={"impact_proxy": ratio - float(threshold_value)},
                    most_binding_gate="impact_proxy_quantile",
                )
                target_exit_ts = decision_ts + self.TARGET_HORIZON
                signals.append(Signal(
                    ts=ts,
                    symbol=symbol,
                    side=proposed_side,
                    signal_type="btc_5m_impact_proxy_reversal_entry",
                    confidence=1.0,
                    metadata={
                        "strategy": "btc_5m_impact_proxy_reversal",
                        "strategy_id": "ALPHA-003-BTC-IMPACT-PROXY-REVERSAL",
                        "family_variant": "completed-5m-impact-proxy",
                        "family_pattern": "liquidity_stress_proxy_reversal",
                        "entry_reason": "extreme_5m_return_per_quote_volume_reversal",
                        "entry_price": completed.close,
                        "entry_reference_price": completed.close,
                        "intended_entry_price": completed.close,
                        "signal_timeframe": "5m",
                        "execution_timeframe": "1m",
                        "risk_accounting": "engine_canonical_R",
                        "r_per_trade": self._r_per_trade,
                        "sizing_mode": "risk_at_stop",
                        "cap_policy": "allow_clip_with_truth",
                        "stop_model": "fixed_completed_5m_atr",
                        "stop_price": stop_price,
                        "entry_stop_price": stop_price,
                        "stop_distance": stop_distance,
                        "signal_return_5m": signal_return,
                        "quote_volume_5m": completed.total,
                        "quote_volume_bucket_ts": completed.start.isoformat(),
                        "impact_proxy": ratio,
                        "impact_proxy_threshold_value": threshold_value,
                        "impact_proxy_percentile": percentile,
                        "normalization_window": self._window,
                        "counterfactual_return_shock_band": self._control_band,
                        "target_horizon_minutes": 30,
                        "target_exit_ts": target_exit_ts.isoformat(),
                        "signal_ts": decision_ts.isoformat(),
                        "signal_emitted_at": pd.Timestamp(ts).isoformat(),
                        "decision_ts": decision_ts.isoformat(),
                        "decision_trace": trace,
                        "requested_risk_amount": None,
                        "risk_utilization_pct": None,
                        "under_risked_trade": None,
                        "risk_metadata_authority": "bt.risk.risk_engine.RiskEngine",
                    },
                ))
            ratios.append(ratio)
        return signals
