"""L2-H1 HTF trend filter for LTF pullback entries."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.indicators.atr import ATR
from bt.indicators.ema import EMA
from bt.logging.decision_trace import make_decision_trace
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


@dataclass
class _SymbolState:
    ltf_ema: EMA
    ltf_atr: ATR
    htf_ema_fast: EMA
    htf_ema_slow: EMA
    last_ltf_ts: pd.Timestamp | None = None
    last_ltf_close: float | None = None
    last_ltf_ema: float | None = None
    dir_htf: int = 0
    htf_ready: bool = False
    last_htf_ts: pd.Timestamp | None = None
    pullback_side: Side | None = None
    pullback_bars: int = 0
    pullback_start_ts: pd.Timestamp | None = None
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
        if lower == "buy":
            return Side.BUY
        if lower == "sell":
            return Side.SELL
    return None


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(out):
        return None
    return out


@register_strategy("l2_h1_htf_trend_filter_pullback")
class L2H1HTFTrendFilterPullbackStrategy(Strategy):
    """Enter LTF EMA20 pullback recoveries only with closed-bar 1h trend context."""

    def __init__(
        self,
        *,
        timeframe: str = "5m",
        htf_timeframe: str = "1h",
        ltf_ema_period: int = 20,
        htf_ema_fast_period: int = 50,
        htf_ema_slow_period: int = 200,
        atr_period: int = 14,
        K: int = 3,
        k_atr: float = 2.0,
        T_hold: int = 48,
        use_htf_filter: bool = True,
        family_variant: str = "L2-H1",
        disallow_flip: bool = True,
        r_per_trade: float = 0.005,
        sizing_mode: str = "risk_at_stop",
        cap_policy: str = "allow_clip_with_truth",
    ) -> None:
        self._timeframe = str(timeframe).lower()
        self._htf_timeframe = str(htf_timeframe).lower()
        self._ltf_ema_period = int(ltf_ema_period)
        self._htf_ema_fast_period = int(htf_ema_fast_period)
        self._htf_ema_slow_period = int(htf_ema_slow_period)
        self._atr_period = int(atr_period)
        self._max_pullback_bars = int(K)
        self._k_atr = float(k_atr)
        self._t_hold = int(T_hold)
        self._use_htf_filter = bool(use_htf_filter)
        self._family_variant = str(family_variant)
        self._disallow_flip = bool(disallow_flip)
        self._r_per_trade = float(r_per_trade)
        self._sizing_mode = str(sizing_mode)
        self._cap_policy = str(cap_policy)
        self._state: dict[str, _SymbolState] = {}

        if self._timeframe not in {"1m", "5m"}:
            raise ValueError("L2-H1 supports timeframe in {'1m', '5m'}")
        if self._htf_timeframe != "1h":
            raise ValueError("L2-H1 requires htf_timeframe='1h'")
        if self._max_pullback_bars <= 0:
            raise ValueError("K must be positive")
        if self._k_atr <= 0:
            raise ValueError("k_atr must be positive")

    def _state_for(self, symbol: str) -> _SymbolState:
        st = self._state.get(symbol)
        if st is None:
            st = _SymbolState(
                ltf_ema=EMA(self._ltf_ema_period),
                ltf_atr=ATR(self._atr_period),
                htf_ema_fast=EMA(self._htf_ema_fast_period),
                htf_ema_slow=EMA(self._htf_ema_slow_period),
            )
            self._state[symbol] = st
        return st

    def _update_htf(self, symbol: str, ctx: Mapping[str, Any], st: _SymbolState) -> None:
        htf = ctx.get("htf")
        if not isinstance(htf, Mapping):
            return
        by_symbol = htf.get(self._htf_timeframe)
        if not isinstance(by_symbol, Mapping):
            return
        htf_bar = by_symbol.get(symbol)
        if htf_bar is None:
            return
        ts = getattr(htf_bar, "ts", None)
        if ts is not None and st.last_htf_ts is not None and pd.Timestamp(ts) <= st.last_htf_ts:
            return
        st.htf_ema_fast.update(htf_bar)
        st.htf_ema_slow.update(htf_bar)
        st.last_htf_ts = pd.Timestamp(ts) if ts is not None else st.last_htf_ts
        fast = st.htf_ema_fast.value
        slow = st.htf_ema_slow.value
        st.htf_ready = fast is not None and slow is not None
        if st.htf_ready:
            st.dir_htf = 1 if float(fast) > float(slow) else -1 if float(fast) < float(slow) else 0

    def _ltf_signal_bar(self, symbol: str, current_bar: Bar, ctx: Mapping[str, Any]) -> Bar | None:
        if self._timeframe == "1m":
            return current_bar
        htf = ctx.get("htf")
        if not isinstance(htf, Mapping):
            return None
        by_symbol = htf.get(self._timeframe)
        if not isinstance(by_symbol, Mapping):
            return None
        candidate = by_symbol.get(symbol)
        return candidate if isinstance(candidate, Bar) or hasattr(candidate, "close") else None

    def _clear_pullback(self, st: _SymbolState) -> None:
        st.pullback_side = None
        st.pullback_bars = 0
        st.pullback_start_ts = None

    def _clear_trade(self, st: _SymbolState) -> None:
        st.entry_side = None
        st.signal_bars_held = 0
        st.stop_price_frozen = None
        st.stop_distance_frozen = None
        st.atr_entry = None

    def _exit_signal(self, *, ts: pd.Timestamp, symbol: str, side: Side, reason: str, st: _SymbolState) -> Signal:
        return Signal(
            ts=ts,
            symbol=symbol,
            side=Side.SELL if side == Side.BUY else Side.BUY,
            signal_type="l2_h1_htf_trend_filter_pullback_exit",
            confidence=1.0,
            metadata={
                "decision_trace": make_decision_trace(
                    reason_code=reason,
                    setup_class="htf_filtered_ltf_pullback",
                    hypothesis_branch="exit",
                    parameter_combination={"strategy": "l2_h1_htf_trend_filter_pullback"},
                ),
                "strategy": "l2_h1_htf_trend_filter_pullback",
                "strategy_id": "l2_h1_htf_trend_filter_pullback",
                "family_variant": self._family_variant,
                "family_pattern": "htf_trend_ltf_ema20_pullback",
                "close_only": True,
                "is_exit": True,
                "exit_reason": reason,
                "signal_timeframe": self._timeframe,
                "execution_timeframe": "1m",
                "exit_monitoring_timeframe": "1m",
                "hold_time_unit": "signal_bars",
                "signal_bars_held": st.signal_bars_held,
                "stop_price": st.stop_price_frozen,
                "entry_stop_price": st.stop_price_frozen,
                "entry_stop_distance": st.stop_distance_frozen,
                "atr_entry": st.atr_entry,
            },
        )

    def _handle_open_position(
        self,
        *,
        ts: pd.Timestamp,
        symbol: str,
        bar: Bar,
        current: Side,
        st: _SymbolState,
        has_ltf_bar: bool,
    ) -> list[Signal]:
        st.entry_side = current
        out: list[Signal] = []
        if has_ltf_bar:
            st.signal_bars_held += 1
            if st.signal_bars_held >= self._t_hold:
                out.append(self._exit_signal(ts=ts, symbol=symbol, side=current, reason="time_stop", st=st))
                self._clear_trade(st)
                return out
        if st.stop_price_frozen is not None:
            if current == Side.BUY and float(bar.low) <= float(st.stop_price_frozen):
                out.append(self._exit_signal(ts=ts, symbol=symbol, side=current, reason="atr_stop", st=st))
                self._clear_trade(st)
            elif current == Side.SELL and float(bar.high) >= float(st.stop_price_frozen):
                out.append(self._exit_signal(ts=ts, symbol=symbol, side=current, reason="atr_stop", st=st))
                self._clear_trade(st)
        return out

    def _rich_entry_state(self, bar: Bar, st: _SymbolState, ltf_bar: Bar) -> dict[str, Any]:
        extra = bar.extra if isinstance(bar.extra, Mapping) else {}
        mark = _safe_float(extra.get("mark_close") or extra.get("mark_price") or extra.get("mark"))
        index = _safe_float(extra.get("index_close") or extra.get("index_price") or extra.get("index"))
        funding = _safe_float(extra.get("funding_rate") or extra.get("funding") or extra.get("funding_raw"))
        oi = _safe_float(extra.get("open_interest") or extra.get("oi") or extra.get("oi_value"))
        basis_raw = _safe_float(extra.get("basis_close_vs_index") or extra.get("basis") or extra.get("mark_index_basis"))
        premium = _safe_float(extra.get("premium_mark_vs_index") or extra.get("premium") or extra.get("premium_pct"))
        if basis_raw is None and mark is not None and index is not None and index:
            basis_raw = (float(mark) - float(index)) / float(index)
        return {
            "entry_state_ts": str(bar.ts),
            "entry_state_mark_price": mark,
            "entry_state_index_price": index,
            "entry_state_funding_raw": funding,
            "entry_state_oi_level": oi,
            "entry_state_oi_change": _safe_float(extra.get("oi_change_1") or extra.get("oi_change")),
            "entry_state_oi_change_pct": _safe_float(extra.get("oi_change_pct_1") or extra.get("oi_change_pct")),
            "entry_state_basis_raw": basis_raw,
            "entry_state_basis_pct": basis_raw,
            "entry_state_premium_raw": premium,
            "entry_state_funding_source_ts": extra.get("funding_source_ts") or extra.get("funding_available_at"),
            "entry_state_oi_source_ts": extra.get("oi_source_ts") or extra.get("oi_available_at"),
            "entry_state_htf_timeframe": self._htf_timeframe,
            "entry_state_ltf_timeframe": self._timeframe,
            "entry_state_dir_htf": st.dir_htf,
            "entry_state_htf_ready": st.htf_ready,
            "entry_state_htf_ema_fast": st.htf_ema_fast.value,
            "entry_state_htf_ema_slow": st.htf_ema_slow.value,
            "entry_state_htf_source_ts": str(st.last_htf_ts) if st.last_htf_ts is not None else None,
            "entry_state_ltf_ema20": st.ltf_ema.value,
            "entry_state_ltf_close": float(ltf_bar.close),
            "entry_state_pullback_bars": st.pullback_bars,
        }

    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        signals: list[Signal] = []
        for symbol, bar in bars_by_symbol.items():
            if symbol not in tradeable:
                continue
            st = self._state_for(symbol)
            self._update_htf(symbol, ctx, st)
            current = _ctx_position_side(ctx, symbol)
            ltf_bar = self._ltf_signal_bar(symbol, bar, ctx)
            has_ltf_bar = ltf_bar is not None

            if current is not None:
                signals.extend(self._handle_open_position(ts=ts, symbol=symbol, bar=bar, current=current, st=st, has_ltf_bar=has_ltf_bar))
                continue
            else:
                self._clear_trade(st)

            if ltf_bar is None:
                continue
            prev_close = st.last_ltf_close
            prev_ema = st.last_ltf_ema
            st.ltf_ema.update(ltf_bar)
            st.ltf_atr.update(ltf_bar)
            ema_v = st.ltf_ema.value
            atr_v = st.ltf_atr.value
            st.last_ltf_ts = pd.Timestamp(getattr(ltf_bar, "ts", ts))
            st.last_ltf_close = float(ltf_bar.close)
            st.last_ltf_ema = float(ema_v) if ema_v is not None else None

            if ema_v is None or atr_v is None or prev_close is None or prev_ema is None:
                continue
            close = float(ltf_bar.close)
            ema = float(ema_v)
            atr = float(atr_v)
            htf_allows_long = (not self._use_htf_filter) or (st.htf_ready and st.dir_htf == 1)
            htf_allows_short = (not self._use_htf_filter) or (st.htf_ready and st.dir_htf == -1)

            crossed_below = prev_close >= prev_ema and close < ema
            crossed_above = prev_close <= prev_ema and close > ema

            if st.pullback_side is None:
                if crossed_below and htf_allows_long:
                    st.pullback_side = Side.BUY
                    st.pullback_bars = 1
                    st.pullback_start_ts = st.last_ltf_ts
                elif crossed_above and htf_allows_short:
                    st.pullback_side = Side.SELL
                    st.pullback_bars = 1
                    st.pullback_start_ts = st.last_ltf_ts
                continue

            st.pullback_bars += 1
            if st.pullback_bars > self._max_pullback_bars:
                self._clear_pullback(st)
                continue

            entry_side: Side | None = None
            if st.pullback_side == Side.BUY and close > ema and htf_allows_long:
                entry_side = Side.BUY
            elif st.pullback_side == Side.SELL and close < ema and htf_allows_short:
                entry_side = Side.SELL
            if entry_side is None:
                continue
            if self._disallow_flip and current is not None and current != entry_side:
                self._clear_pullback(st)
                continue

            stop_distance = self._k_atr * atr
            if stop_distance <= 0:
                self._clear_pullback(st)
                continue
            stop_price = close - stop_distance if entry_side == Side.BUY else close + stop_distance
            st.entry_side = entry_side
            st.signal_bars_held = 0
            st.stop_price_frozen = float(stop_price)
            st.stop_distance_frozen = float(stop_distance)
            st.atr_entry = float(atr)

            conditions = {
                "ltf_recovered_ema20": True,
                "pullback_within_K": st.pullback_bars <= self._max_pullback_bars,
                "htf_filter_enabled": self._use_htf_filter,
                "htf_direction_allowed": (entry_side == Side.BUY and htf_allows_long) or (entry_side == Side.SELL and htf_allows_short),
                "htf_ready": st.htf_ready,
            }
            gate_values = {
                "dir_htf": st.dir_htf,
                "htf_ema_fast": st.htf_ema_fast.value,
                "htf_ema_slow": st.htf_ema_slow.value,
                "ltf_close": close,
                "ltf_ema20": ema,
                "pullback_bars": st.pullback_bars,
                "K": self._max_pullback_bars,
                "atr": atr,
                "stop_distance": stop_distance,
            }
            trace = make_decision_trace(
                reason_code="htf_trend_ltf_pullback_recovery",
                setup_class="htf_filtered_ltf_pullback",
                hypothesis_branch="entry",
                conditions_bool_map=conditions,
                blockers_bool_map={},
                permission_layer_state={"tradeable": True, "use_htf_filter": self._use_htf_filter},
                parameter_combination={
                    "strategy": "l2_h1_htf_trend_filter_pullback",
                    "timeframe": self._timeframe,
                    "K": self._max_pullback_bars,
                    "k_atr": self._k_atr,
                    "use_htf_filter": self._use_htf_filter,
                },
                gate_values=gate_values,
                gate_thresholds={"K": self._max_pullback_bars, "k_atr": self._k_atr},
                gate_margins={"pullback_bars_margin": self._max_pullback_bars - st.pullback_bars},
                most_binding_gate="pullback_bars",
            )
            meta = {
                "decision_trace": trace,
                "strategy": "l2_h1_htf_trend_filter_pullback",
                "strategy_id": "l2_h1_htf_trend_filter_pullback",
                "family_variant": self._family_variant,
                "family_pattern": "htf_trend_ltf_ema20_pullback",
                "entry_reason": "htf_trend_ltf_pullback_recovery",
                "entry_price": close,
                "entry_reference_price": close,
                "intended_entry_price": close,
                "signal_timeframe": self._timeframe,
                "execution_timeframe": "1m",
                "exit_monitoring_timeframe": "1m",
                "hold_time_unit": "signal_bars",
                "risk_accounting": "engine_canonical_R",
                "r_per_trade": self._r_per_trade,
                "sizing_mode": self._sizing_mode,
                "cap_policy": self._cap_policy,
                "stop_model": "fixed_atr_multiple",
                "stop_update_policy": "frozen_at_entry",
                "stop_price": float(stop_price),
                "entry_stop_price": float(stop_price),
                "stop_distance": float(stop_distance),
                "entry_stop_distance": float(stop_distance),
                "atr_entry": float(atr),
                "K": self._max_pullback_bars,
                "k_atr": self._k_atr,
                "T_hold": self._t_hold,
                "use_htf_filter": self._use_htf_filter,
                "dir_htf": st.dir_htf,
                "htf_ready": st.htf_ready,
                "htf_ema_fast": st.htf_ema_fast.value,
                "htf_ema_slow": st.htf_ema_slow.value,
                "htf_source_ts": str(st.last_htf_ts) if st.last_htf_ts is not None else None,
                "ltf_ema20": ema,
                "ltf_close": close,
                "pullback_bars": st.pullback_bars,
                "pullback_start_ts": str(st.pullback_start_ts) if st.pullback_start_ts is not None else None,
                **self._rich_entry_state(bar, st, ltf_bar),
            }
            signals.append(
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=entry_side,
                    signal_type="l2_h1_htf_trend_filter_pullback",
                    confidence=1.0,
                    metadata=meta,
                )
            )
            self._clear_pullback(st)
        return signals
