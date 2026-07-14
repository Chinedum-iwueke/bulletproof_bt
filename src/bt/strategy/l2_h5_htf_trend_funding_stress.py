"""L2-H5 HTF trend pullback with causal funding-stress gate."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import hashlib
import json
from math import sqrt
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.indicators.atr import ATR
from bt.indicators.ema import EMA
from bt.logging.decision_trace import make_decision_trace
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(out):
        return None
    return out


def _safe_ts(value: Any) -> pd.Timestamp | None:
    if value is None or value is pd.NaT:
        return None
    try:
        ts = pd.Timestamp(value)
    except Exception:
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


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


@dataclass
class _FundingStats:
    values: deque[float]
    last_source_ts: pd.Timestamp | None = None

    def observe(self, value: float | None, source_ts: pd.Timestamp | None) -> tuple[float | None, bool]:
        if value is None or source_ts is None:
            return self.zscore(value), False
        is_new = self.last_source_ts is None or source_ts > self.last_source_ts
        z = self.zscore(value)
        if is_new:
            self.values.append(float(value))
            self.last_source_ts = source_ts
        return z, is_new

    def zscore(self, value: float | None) -> float | None:
        if value is None or len(self.values) < 5:
            return None
        vals = list(self.values)
        mean = sum(vals) / len(vals)
        var = sum((item - mean) ** 2 for item in vals) / max(len(vals) - 1, 1)
        std = sqrt(var)
        if std <= 0:
            return 0.0
        return float((value - mean) / std)


@dataclass
class _SymbolState:
    ltf_ema: EMA
    ltf_atr: ATR
    htf_ema_fast: EMA
    htf_ema_slow: EMA
    funding_stats: _FundingStats
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
    entry_funding_sign: int | None = None
    entry_fund_z: float | None = None
    signal_bars_held: int = 0
    stop_price_frozen: float | None = None
    stop_distance_frozen: float | None = None
    atr_entry: float | None = None
    funding_causality_violations: int = 0


@register_strategy("l2_h5_htf_trend_funding_stress")
class L2H5HTFTrendFundingStressStrategy(Strategy):
    """L2-H1 trend pullback with a causal perps funding-stress gate."""

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
        funding_z_threshold: float = 1.0,
        funding_lookback_days: int = 30,
        funding_unwind_band: float = 0.25,
        family_variant: str = "L2-H5",
        disallow_flip: bool = True,
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
        self._funding_z_threshold = float(funding_z_threshold)
        self._funding_lookback_days = int(funding_lookback_days)
        self._funding_unwind_band = float(funding_unwind_band)
        self._family_variant = str(family_variant)
        self._disallow_flip = bool(disallow_flip)
        self._state: dict[str, _SymbolState] = {}
        if self._htf_timeframe != "1h":
            raise ValueError("L2-H5 requires htf_timeframe='1h'")
        if self._funding_lookback_days <= 0:
            raise ValueError("funding_lookback_days must be positive")

    @property
    def _funding_window_events(self) -> int:
        # Perp funding is normally published every 8h; event-asof joins repeat
        # that value on many 1m bars, so we keep one sample per source timestamp.
        return max(5, int(self._funding_lookback_days * 3))

    def _state_for(self, symbol: str) -> _SymbolState:
        st = self._state.get(symbol)
        if st is None:
            st = _SymbolState(
                ltf_ema=EMA(self._ltf_ema_period),
                ltf_atr=ATR(self._atr_period),
                htf_ema_fast=EMA(self._htf_ema_fast_period),
                htf_ema_slow=EMA(self._htf_ema_slow_period),
                funding_stats=_FundingStats(deque(maxlen=self._funding_window_events)),
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
        ts = pd.Timestamp(getattr(htf_bar, "ts"))
        if st.last_htf_ts is not None and ts <= st.last_htf_ts:
            return
        st.htf_ema_fast.update(htf_bar)
        st.htf_ema_slow.update(htf_bar)
        st.last_htf_ts = ts
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
        return candidate if candidate is not None and hasattr(candidate, "close") else None

    @staticmethod
    def _funding_payload(bar: Bar, symbol: str) -> dict[str, Any]:
        extra = bar.extra if isinstance(bar.extra, Mapping) else {}
        funding = _safe_float(extra.get("funding_rate") or extra.get("funding") or extra.get("funding_raw"))
        source_ts = _safe_ts(extra.get("funding_source_ts") or extra.get("funding_available_at") or extra.get("available_at"))
        native = {
            "symbol": symbol,
            "funding_rate": funding,
            "funding_source_ts": str(source_ts) if source_ts is not None else None,
        }
        encoded = json.dumps(native, sort_keys=True, separators=(",", ":"))
        return {
            "funding_rate": funding,
            "funding_source_ts": source_ts,
            "funding_provenance_hash": hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
        }

    @staticmethod
    def _funding_sign(value: float | None) -> int | None:
        if value is None:
            return None
        if value > 0:
            return 1
        if value < 0:
            return -1
        return 0

    def _funding_state(self, *, ts: pd.Timestamp, bar: Bar, symbol: str, st: _SymbolState) -> dict[str, Any]:
        payload = self._funding_payload(bar, symbol)
        source_ts = payload["funding_source_ts"]
        funding = payload["funding_rate"]
        source_valid = source_ts is not None and source_ts <= ts
        if source_ts is not None and source_ts > ts:
            st.funding_causality_violations += 1
            return {
                **payload,
                "funding_source_valid": False,
                "fund_z": None,
                "funding_stress": None,
                "funding_event_updated": False,
                "funding_history_count": len(st.funding_stats.values),
            }
        fund_z, updated = st.funding_stats.observe(funding, source_ts if source_valid else None)
        stress = None if fund_z is None else bool(fund_z > self._funding_z_threshold)
        return {
            **payload,
            "funding_source_valid": source_valid,
            "fund_z": fund_z,
            "funding_stress": stress,
            "funding_event_updated": updated,
            "funding_history_count": len(st.funding_stats.values),
        }

    def _clear_pullback(self, st: _SymbolState) -> None:
        st.pullback_side = None
        st.pullback_bars = 0
        st.pullback_start_ts = None

    def _clear_trade(self, st: _SymbolState) -> None:
        st.entry_side = None
        st.entry_funding_sign = None
        st.entry_fund_z = None
        st.signal_bars_held = 0
        st.stop_price_frozen = None
        st.stop_distance_frozen = None
        st.atr_entry = None

    def _trace(self, *, branch: str, conditions: dict[str, bool], gate_values: dict[str, Any], blockers: dict[str, bool] | None = None):
        return make_decision_trace(
            reason_code="htf_trend_funding_stress_pullback",
            setup_class="htf_trend_funding_stress",
            hypothesis_branch=branch,
            conditions_bool_map=conditions,
            blockers_bool_map=blockers or {},
            permission_layer_state={"perps_only": True, "funding_join": "backward_asof_source_ts_checked"},
            parameter_combination={
                "strategy": "l2_h5_htf_trend_funding_stress",
                "timeframe": self._timeframe,
                "funding_z_threshold": self._funding_z_threshold,
                "funding_lookback_days": self._funding_lookback_days,
                "K": self._max_pullback_bars,
                "k_atr": self._k_atr,
            },
            gate_values=gate_values,
            gate_thresholds={"funding_z_threshold": self._funding_z_threshold, "K": self._max_pullback_bars},
            gate_margins={},
            most_binding_gate="funding_stress",
        )

    def _exit_signal(
        self,
        *,
        ts: pd.Timestamp,
        symbol: str,
        side: Side,
        reason: str,
        st: _SymbolState,
        funding_state: dict[str, Any] | None = None,
    ) -> Signal:
        funding_state = funding_state or {}
        return Signal(
            ts=ts,
            symbol=symbol,
            side=Side.SELL if side == Side.BUY else Side.BUY,
            signal_type="l2_h5_exit",
            confidence=1.0,
            metadata={
                "decision_trace": self._trace(branch="exit", conditions={}, gate_values=funding_state),
                "strategy": "l2_h5_htf_trend_funding_stress",
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
                **funding_state,
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
        funding_state: dict[str, Any],
    ) -> list[Signal]:
        st.entry_side = current
        out: list[Signal] = []
        funding = funding_state.get("funding_rate")
        fund_z = funding_state.get("fund_z")
        current_sign = self._funding_sign(funding)
        flipped = (
            st.entry_funding_sign not in (None, 0)
            and current_sign not in (None, 0)
            and current_sign != st.entry_funding_sign
        )
        reverted_inside_band = (
            st.entry_fund_z is not None
            and abs(float(st.entry_fund_z)) > self._funding_unwind_band
            and fund_z is not None
            and abs(float(fund_z)) <= self._funding_unwind_band
        )
        if flipped or reverted_inside_band:
            reason = "funding_flip" if flipped else "funding_unwind_inside_band"
            out.append(self._exit_signal(ts=ts, symbol=symbol, side=current, reason=reason, st=st, funding_state=funding_state))
            self._clear_trade(st)
            return out
        if has_ltf_bar:
            st.signal_bars_held += 1
            if st.signal_bars_held >= self._t_hold:
                out.append(self._exit_signal(ts=ts, symbol=symbol, side=current, reason="time_stop", st=st, funding_state=funding_state))
                self._clear_trade(st)
                return out
        if st.stop_price_frozen is not None:
            if current == Side.BUY and float(bar.low) <= float(st.stop_price_frozen):
                out.append(self._exit_signal(ts=ts, symbol=symbol, side=current, reason="atr_stop", st=st, funding_state=funding_state))
                self._clear_trade(st)
            elif current == Side.SELL and float(bar.high) >= float(st.stop_price_frozen):
                out.append(self._exit_signal(ts=ts, symbol=symbol, side=current, reason="atr_stop", st=st, funding_state=funding_state))
                self._clear_trade(st)
        return out

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        signals: list[Signal] = []
        for symbol, bar in bars_by_symbol.items():
            if symbol not in tradeable:
                continue
            st = self._state_for(symbol)
            self._update_htf(symbol, ctx, st)
            funding_state = self._funding_state(ts=ts, bar=bar, symbol=symbol, st=st)
            current = _ctx_position_side(ctx, symbol)
            ltf_bar = self._ltf_signal_bar(symbol, bar, ctx)
            has_ltf_bar = ltf_bar is not None

            if current is not None:
                signals.extend(self._handle_open_position(ts=ts, symbol=symbol, bar=bar, current=current, st=st, has_ltf_bar=has_ltf_bar, funding_state=funding_state))
                continue
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
            htf_allows_long = st.htf_ready and st.dir_htf == 1
            htf_allows_short = st.htf_ready and st.dir_htf == -1
            funding_ready = bool(funding_state.get("funding_source_valid")) and funding_state.get("fund_z") is not None
            funding_stress = funding_state.get("funding_stress")
            funding_gate_allows = funding_ready and funding_stress is False

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
            conditions = {
                "ltf_recovered_ema20": True,
                "pullback_within_K": st.pullback_bars <= self._max_pullback_bars,
                "htf_direction_allowed": (entry_side == Side.BUY and htf_allows_long) or (entry_side == Side.SELL and htf_allows_short),
                "htf_ready": st.htf_ready,
                "funding_ready": funding_ready,
                "funding_not_stressed": funding_gate_allows,
            }
            blockers = {
                "funding_source_future": not bool(funding_state.get("funding_source_valid")),
                "funding_stress": funding_stress is True,
            }
            if not funding_gate_allows:
                self._clear_pullback(st)
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
            st.entry_funding_sign = self._funding_sign(funding_state.get("funding_rate"))
            st.entry_fund_z = _safe_float(funding_state.get("fund_z"))
            st.signal_bars_held = 0
            st.stop_price_frozen = float(stop_price)
            st.stop_distance_frozen = float(stop_distance)
            st.atr_entry = float(atr)

            gate_values = {
                "dir_htf": st.dir_htf,
                "ltf_close": close,
                "ltf_ema20": ema,
                "pullback_bars": st.pullback_bars,
                "atr": atr,
                **funding_state,
            }
            trace = self._trace(branch="entry", conditions=conditions, blockers=blockers, gate_values=gate_values)
            source_ts = funding_state.get("funding_source_ts")
            meta = {
                "decision_trace": trace,
                "strategy": "l2_h5_htf_trend_funding_stress",
                "strategy_id": "l2_h5_htf_trend_funding_stress",
                "hypothesis_id": "L2-H5",
                "family_variant": self._family_variant,
                "family_pattern": "htf_trend_ltf_pullback_funding_stress_gate",
                "entry_reason": "htf_trend_pullback_low_funding_stress",
                "entry_price": close,
                "entry_reference_price": close,
                "intended_entry_price": close,
                "signal_timeframe": self._timeframe,
                "execution_timeframe": "1m",
                "exit_monitoring_timeframe": "1m",
                "hold_time_unit": "signal_bars",
                "risk_accounting": "engine_canonical_R",
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
                "dir_htf": st.dir_htf,
                "htf_ready": st.htf_ready,
                "htf_ema_fast": st.htf_ema_fast.value,
                "htf_ema_slow": st.htf_ema_slow.value,
                "htf_source_ts": str(st.last_htf_ts) if st.last_htf_ts is not None else None,
                "ltf_ema20": ema,
                "ltf_close": close,
                "pullback_bars": st.pullback_bars,
                "pullback_start_ts": str(st.pullback_start_ts) if st.pullback_start_ts is not None else None,
                "funding_rate": funding_state.get("funding_rate"),
                "fund_z": funding_state.get("fund_z"),
                "funding_z_threshold": self._funding_z_threshold,
                "funding_lookback_days": self._funding_lookback_days,
                "funding_stress": funding_state.get("funding_stress"),
                "funding_source_ts": str(source_ts) if source_ts is not None else None,
                "funding_source_valid": funding_state.get("funding_source_valid"),
                "funding_provenance_hash": funding_state.get("funding_provenance_hash"),
                "funding_history_count": funding_state.get("funding_history_count"),
                "funding_event_updated": funding_state.get("funding_event_updated"),
                "entry_state_funding_raw": funding_state.get("funding_rate"),
                "entry_state_funding_rate": funding_state.get("funding_rate"),
                "entry_state_funding_z": funding_state.get("fund_z"),
                "entry_state_funding_source_ts": str(source_ts) if source_ts is not None else None,
                "entry_state_funding_stress": funding_state.get("funding_stress"),
                "entry_state_dir_htf": st.dir_htf,
                "entry_state_htf_ready": st.htf_ready,
                "entry_state_htf_ema_fast": st.htf_ema_fast.value,
                "entry_state_htf_ema_slow": st.htf_ema_slow.value,
                "entry_state_htf_source_ts": str(st.last_htf_ts) if st.last_htf_ts is not None else None,
                "entry_state_ltf_ema20": ema,
                "entry_state_ltf_close": close,
            }
            signals.append(Signal(ts=ts, symbol=symbol, side=entry_side, signal_type="l2_h5_htf_trend_funding_stress", confidence=1.0, metadata=meta))
            self._clear_pullback(st)
        return signals
