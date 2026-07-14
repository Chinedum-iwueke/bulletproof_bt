"""L1-H11 quality-filtered continuation strategy variants (A/B/C)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.hypotheses.l1_h11 import entry_position_ratio, pullback_depth_atr, swing_distance_atr
from bt.indicators.atr import ATR
from bt.indicators.dmi_adx import DMIADX
from bt.indicators.ema import EMA
from bt.indicators.vwap import SessionVWAP
from bt.strategy import register_strategy
from bt.strategy.base import Strategy
from bt.logging.decision_trace import make_decision_trace


@dataclass
class _State:
    ema_fast: EMA
    ema_slow: EMA
    adx: DMIADX
    atr_signal: ATR
    exec_vwap: SessionVWAP
    position: Side | None = None
    last_signal_ts: pd.Timestamp | None = None
    trend_dir: Side | None = None
    trend_anchor_price: float | None = None
    trend_extreme_price: float | None = None
    pullback_active: bool = False
    pullback_extreme_low: float | None = None
    pullback_extreme_high: float | None = None
    pullback_signal_ts: pd.Timestamp | None = None
    entry_price: float | None = None
    atr_entry: float | None = None
    stop_price_frozen: float | None = None
    stop_distance_frozen: float | None = None
    favorable_extreme_price: float | None = None
    lock_armed: bool = False
    runner_trail_armed: bool = False
    signal_bars_held: int = 0


@register_strategy("l1_h11_quality_filtered_continuation")
class L1H11QualityFilteredContinuationStrategy(Strategy):
    def __init__(
        self,
        *,
        timeframe: str = "15m",
        adx_min: float = 20.0,
        adx_min_fixed: float = 20.0,
        pull_entry_atr_low: float = 0.35,
        pull_entry_atr_high: float = 1.0,
        impulse_min_atr_fixed: float = 1.0,
        swing_distance_atr: float = 1.0,
        stop_atr_mult: float = 2.0,
        stop_padding_atr: float = 0.0,
        lock_r: float = 1.0,
        vwap_giveback: str = "off",
        h11_tuning_profile: str = "",
        h11a_tuning_profile: str = "baseline",
        allowed_sides: str = "both",
        allowed_vol_regimes: str = "",
        blocked_vol_regimes: str = "",
        allowed_liquidity_regimes: str = "",
        blocked_liquidity_regimes: str = "",
        allowed_displacement_regimes: str = "",
        blocked_displacement_regimes: str = "",
        allowed_csi_buckets: str = "",
        blocked_csi_buckets: str = "",
        allowed_basis_regimes: str = "",
        blocked_basis_regimes: str = "",
        allowed_funding_regimes: str = "",
        blocked_funding_regimes: str = "",
        excluded_symbols: str = "",
        entry_cooldown_hours: float = 0.0,
        break_even_trigger_r: float = 0.0,
        profit_lock_trigger_r: float = 0.0,
        profit_lock_r: float = 0.0,
        runner_trail_trigger_r: float = 0.0,
        runner_trail_distance_r: float = 0.0,
        loss_cap_r: float = 0.0,
        family_variant: str = "L1-H11A",
        setup_type: str = "quality_filtered_continuation",
    ) -> None:
        self._timeframe = str(timeframe)
        self._adx_min = float(adx_min)
        self._adx_min_fixed = float(adx_min_fixed)
        self._pull_entry_atr_low = float(pull_entry_atr_low)
        self._pull_entry_atr_high = float(pull_entry_atr_high)
        self._impulse_min_atr_fixed = float(impulse_min_atr_fixed)
        self._swing_distance_atr = float(swing_distance_atr)
        self._stop_atr_mult = float(stop_atr_mult)
        self._stop_padding_atr = float(stop_padding_atr)
        self._lock_r = float(lock_r)
        self._vwap_giveback = str(vwap_giveback).lower()
        self._h11a_tuning_profile = str(h11_tuning_profile or h11a_tuning_profile or "baseline")
        self._allowed_sides = str(allowed_sides or "both").lower()
        self._allowed_vol_regimes = self._parse_set(allowed_vol_regimes)
        self._blocked_vol_regimes = self._parse_set(blocked_vol_regimes)
        self._allowed_liquidity_regimes = self._parse_set(allowed_liquidity_regimes)
        self._blocked_liquidity_regimes = self._parse_set(blocked_liquidity_regimes)
        self._allowed_displacement_regimes = self._parse_set(allowed_displacement_regimes)
        self._blocked_displacement_regimes = self._parse_set(blocked_displacement_regimes)
        self._allowed_csi_buckets = self._parse_set(allowed_csi_buckets)
        self._blocked_csi_buckets = self._parse_set(blocked_csi_buckets)
        self._allowed_basis_regimes = self._parse_set(allowed_basis_regimes)
        self._blocked_basis_regimes = self._parse_set(blocked_basis_regimes)
        self._allowed_funding_regimes = self._parse_set(allowed_funding_regimes)
        self._blocked_funding_regimes = self._parse_set(blocked_funding_regimes)
        self._excluded_symbols = self._parse_set(excluded_symbols)
        self._entry_cooldown_hours = float(entry_cooldown_hours or 0.0)
        self._last_entry_ts_by_symbol: dict[str, pd.Timestamp] = {}
        self._break_even_trigger_r = float(break_even_trigger_r or 0.0)
        self._profit_lock_trigger_r = float(profit_lock_trigger_r or 0.0)
        self._profit_lock_r = float(profit_lock_r or 0.0)
        self._runner_trail_trigger_r = float(runner_trail_trigger_r or 0.0)
        self._runner_trail_distance_r = float(runner_trail_distance_r or 0.0)
        self._loss_cap_r = float(loss_cap_r or 0.0)
        self._family_variant = str(family_variant)
        self._setup_type = str(setup_type)
        self._state: dict[str, _State] = {}
        if self._family_variant == "L1-H11C" and self._break_even_trigger_r <= 0:
            self._break_even_trigger_r = self._lock_r
        self._apply_h11a_tuning_profile()

    def _state_for(self, symbol: str) -> _State:
        st = self._state.get(symbol)
        if st is None:
            st = _State(
                ema_fast=EMA(20),
                ema_slow=EMA(50),
                adx=DMIADX(14),
                atr_signal=ATR(14),
                exec_vwap=SessionVWAP(session="utc_day", price_source="typical"),
            )
            self._state[symbol] = st
        return st

    @staticmethod
    def _ctx_position_side(ctx: Mapping[str, Any], symbol: str) -> Side | None:
        positions = ctx.get("positions")
        if not isinstance(positions, Mapping):
            return None
        payload = positions.get(symbol)
        if not isinstance(payload, Mapping):
            return None
        side = payload.get("side")
        if isinstance(side, Side):
            return side
        if isinstance(side, str):
            if side.lower() == "buy":
                return Side.BUY
            if side.lower() == "sell":
                return Side.SELL
        return None

    @staticmethod
    def _clear_position(st: _State) -> None:
        st.position = None
        st.entry_price = None
        st.atr_entry = None
        st.stop_price_frozen = None
        st.stop_distance_frozen = None
        st.favorable_extreme_price = None
        st.lock_armed = False
        st.runner_trail_armed = False
        st.signal_bars_held = 0

    @staticmethod
    def _clear_pullback(st: _State) -> None:
        st.pullback_active = False
        st.pullback_extreme_low = None
        st.pullback_extreme_high = None
        st.pullback_signal_ts = None

    def _impulse_threshold(self) -> float:
        if self._family_variant == "L1-H11B":
            return self._swing_distance_atr
        return self._impulse_min_atr_fixed

    @staticmethod
    def _parse_set(raw: str | None) -> set[str]:
        if raw is None:
            return set()
        return {part.strip() for part in str(raw).replace("|", ",").split(",") if part.strip()}

    def _apply_h11a_tuning_profile(self) -> None:
        profile = self._h11a_tuning_profile.strip().lower()
        if profile in {"", "baseline", "none"}:
            return
        profiles: dict[str, dict[str, Any]] = {
            "h11a_1h_core_protected": {
                "adx_min": 20.0,
                "pull_entry_atr_low": 0.65,
                "pull_entry_atr_high": 1.0,
                "allowed_sides": "long",
                "allowed_liquidity_regimes": "liquid,moderate",
                "blocked_basis_regimes": "basis_positive,basis_very_positive",
                "blocked_funding_regimes": "funding_negative,funding_very_negative",
                "break_even_trigger_r": 1.0,
                "profit_lock_trigger_r": 2.0,
                "profit_lock_r": 1.0,
                "runner_trail_trigger_r": 3.0,
                "runner_trail_distance_r": 1.5,
                "loss_cap_r": 0.75,
            },
            "h11a_1h_quality_balanced": {
                "adx_min": 20.0,
                "pull_entry_atr_low": 0.65,
                "pull_entry_atr_high": 1.0,
                "blocked_liquidity_regimes": "fragile,broken",
                "blocked_basis_regimes": "basis_positive,basis_very_positive",
                "blocked_funding_regimes": "funding_negative,funding_very_negative",
                "allowed_csi_buckets": "csi_low,csi_mid",
                "break_even_trigger_r": 1.0,
                "profit_lock_trigger_r": 2.0,
                "profit_lock_r": 1.0,
                "runner_trail_trigger_r": 3.0,
                "runner_trail_distance_r": 1.5,
                "loss_cap_r": 0.75,
            },
            "h11a_15m_liquid_midvol_runner": {
                "adx_min": 20.0,
                "pull_entry_atr_low": 0.5,
                "pull_entry_atr_high": 0.8,
                "allowed_sides": "long",
                "allowed_vol_regimes": "vol_mid",
                "allowed_liquidity_regimes": "liquid",
                "allowed_displacement_regimes": "mild_impulse,no_impulse",
                "allowed_csi_buckets": "csi_low",
                "break_even_trigger_r": 1.0,
                "profit_lock_trigger_r": 1.5,
                "profit_lock_r": 0.5,
                "runner_trail_trigger_r": 2.5,
                "runner_trail_distance_r": 1.25,
                "loss_cap_r": 0.75,
            },
            "h11a_15m_explosive_moderate": {
                "adx_min": 20.0,
                "pull_entry_atr_low": 0.65,
                "pull_entry_atr_high": 0.8,
                "allowed_sides": "long",
                "allowed_vol_regimes": "vol_extreme",
                "allowed_liquidity_regimes": "moderate",
                "allowed_displacement_regimes": "strong_impulse,extreme_impulse",
                "blocked_basis_regimes": "basis_positive,basis_very_positive",
                "break_even_trigger_r": 1.0,
                "profit_lock_trigger_r": 2.0,
                "profit_lock_r": 1.0,
                "runner_trail_trigger_r": 3.0,
                "runner_trail_distance_r": 1.75,
                "loss_cap_r": 0.75,
            },
            "h11b_1h_core_geometry": {
                "adx_min_fixed": 20.0,
                "pull_entry_atr_low": 0.5,
                "pull_entry_atr_high": 1.0,
                "swing_distance_atr": 1.5,
                "allowed_sides": "long",
                "blocked_liquidity_regimes": "fragile,broken",
                "allowed_csi_buckets": "csi_low,csi_mid",
                "blocked_basis_regimes": "basis_very_positive",
                "blocked_funding_regimes": "funding_very_negative",
                "break_even_trigger_r": 1.0,
                "profit_lock_trigger_r": 2.0,
                "profit_lock_r": 1.0,
                "runner_trail_trigger_r": 3.0,
                "runner_trail_distance_r": 1.5,
                "loss_cap_r": 0.75,
            },
            "h11b_1h_mild_basis_runner": {
                "adx_min_fixed": 20.0,
                "pull_entry_atr_low": 0.5,
                "pull_entry_atr_high": 1.0,
                "swing_distance_atr": 1.5,
                "allowed_sides": "long",
                "allowed_displacement_regimes": "mild_impulse",
                "allowed_basis_regimes": "basis_positive",
                "allowed_funding_regimes": "funding_neutral",
                "break_even_trigger_r": 1.0,
                "profit_lock_trigger_r": 2.0,
                "profit_lock_r": 1.0,
                "runner_trail_trigger_r": 3.0,
                "runner_trail_distance_r": 1.75,
                "loss_cap_r": 0.75,
            },
            "h11b_15m_midvol_funding_squeeze": {
                "adx_min_fixed": 20.0,
                "pull_entry_atr_low": 0.35,
                "pull_entry_atr_high": 0.8,
                "swing_distance_atr": 1.0,
                "allowed_sides": "long",
                "allowed_vol_regimes": "vol_mid",
                "allowed_liquidity_regimes": "liquid",
                "allowed_funding_regimes": "funding_negative",
                "break_even_trigger_r": 1.0,
                "profit_lock_trigger_r": 1.5,
                "profit_lock_r": 0.5,
                "runner_trail_trigger_r": 2.5,
                "runner_trail_distance_r": 1.25,
                "loss_cap_r": 0.75,
            },
            "h11b_15m_liquid_mild_squeeze": {
                "adx_min_fixed": 20.0,
                "pull_entry_atr_low": 0.35,
                "pull_entry_atr_high": 0.8,
                "swing_distance_atr": 1.0,
                "allowed_sides": "long",
                "allowed_liquidity_regimes": "liquid",
                "allowed_displacement_regimes": "mild_impulse",
                "allowed_funding_regimes": "funding_negative",
                "break_even_trigger_r": 1.0,
                "profit_lock_trigger_r": 1.5,
                "profit_lock_r": 0.5,
                "runner_trail_trigger_r": 2.5,
                "runner_trail_distance_r": 1.25,
                "loss_cap_r": 0.75,
            },
            "h11c_1h_core_protected": {
                "adx_min_fixed": 20.0,
                "pull_entry_atr_low": 0.5,
                "pull_entry_atr_high": 1.0,
                "impulse_min_atr_fixed": 1.0,
                "stop_padding_atr": 0.25,
                "lock_r": 1.5,
                "vwap_giveback": "on",
                "allowed_sides": "long",
                "blocked_liquidity_regimes": "fragile,broken",
                "allowed_csi_buckets": "csi_low,csi_mid",
                "blocked_basis_regimes": "basis_positive,basis_very_positive",
                "blocked_funding_regimes": "funding_negative,funding_very_negative",
                "break_even_trigger_r": 1.0,
                "profit_lock_trigger_r": 2.0,
                "profit_lock_r": 1.0,
                "runner_trail_trigger_r": 3.0,
                "runner_trail_distance_r": 1.5,
            },
            "h11c_1h_mid_moderate_runner": {
                "adx_min_fixed": 20.0,
                "pull_entry_atr_low": 0.5,
                "pull_entry_atr_high": 1.0,
                "impulse_min_atr_fixed": 1.0,
                "stop_padding_atr": 0.25,
                "lock_r": 1.5,
                "vwap_giveback": "on",
                "allowed_sides": "long",
                "allowed_vol_regimes": "vol_mid",
                "allowed_liquidity_regimes": "moderate",
                "allowed_funding_regimes": "funding_neutral",
                "break_even_trigger_r": 1.0,
                "profit_lock_trigger_r": 2.0,
                "profit_lock_r": 1.0,
                "runner_trail_trigger_r": 3.0,
                "runner_trail_distance_r": 1.75,
            },
            "h11c_15m_fragile_extreme_runner": {
                "adx_min_fixed": 20.0,
                "pull_entry_atr_low": 0.5,
                "pull_entry_atr_high": 1.0,
                "impulse_min_atr_fixed": 1.0,
                "stop_padding_atr": 0.25,
                "lock_r": 1.0,
                "vwap_giveback": "off",
                "allowed_sides": "long",
                "allowed_liquidity_regimes": "fragile",
                "allowed_displacement_regimes": "extreme_impulse",
                "allowed_basis_regimes": "basis_very_positive",
                "break_even_trigger_r": 1.0,
                "profit_lock_trigger_r": 1.5,
                "profit_lock_r": 0.5,
                "runner_trail_trigger_r": 2.5,
                "runner_trail_distance_r": 1.25,
            },
            "h11c_15m_mid_fragile_basis_runner": {
                "adx_min_fixed": 20.0,
                "pull_entry_atr_low": 0.5,
                "pull_entry_atr_high": 1.0,
                "impulse_min_atr_fixed": 1.0,
                "stop_padding_atr": 0.25,
                "lock_r": 1.0,
                "vwap_giveback": "off",
                "allowed_sides": "long",
                "allowed_vol_regimes": "vol_mid",
                "allowed_liquidity_regimes": "fragile",
                "allowed_basis_regimes": "basis_very_positive",
                "break_even_trigger_r": 1.0,
                "profit_lock_trigger_r": 1.5,
                "profit_lock_r": 0.5,
                "runner_trail_trigger_r": 2.5,
                "runner_trail_distance_r": 1.25,
            },
        }
        payload = profiles.get(profile)
        if payload is None:
            raise ValueError(f"unknown h11a_tuning_profile={self._h11a_tuning_profile!r}")
        self._adx_min = float(payload.get("adx_min", self._adx_min))
        self._adx_min_fixed = float(payload.get("adx_min_fixed", self._adx_min_fixed))
        self._pull_entry_atr_low = float(payload.get("pull_entry_atr_low", self._pull_entry_atr_low))
        self._pull_entry_atr_high = float(payload.get("pull_entry_atr_high", self._pull_entry_atr_high))
        self._impulse_min_atr_fixed = float(payload.get("impulse_min_atr_fixed", self._impulse_min_atr_fixed))
        self._swing_distance_atr = float(payload.get("swing_distance_atr", self._swing_distance_atr))
        self._stop_atr_mult = float(payload.get("stop_atr_mult", self._stop_atr_mult))
        self._stop_padding_atr = float(payload.get("stop_padding_atr", self._stop_padding_atr))
        self._lock_r = float(payload.get("lock_r", self._lock_r))
        self._vwap_giveback = str(payload.get("vwap_giveback", self._vwap_giveback)).lower()
        if "allowed_sides" in payload:
            self._allowed_sides = str(payload["allowed_sides"]).lower()
        for attr, key in (
            ("_allowed_vol_regimes", "allowed_vol_regimes"),
            ("_blocked_vol_regimes", "blocked_vol_regimes"),
            ("_allowed_liquidity_regimes", "allowed_liquidity_regimes"),
            ("_blocked_liquidity_regimes", "blocked_liquidity_regimes"),
            ("_allowed_displacement_regimes", "allowed_displacement_regimes"),
            ("_blocked_displacement_regimes", "blocked_displacement_regimes"),
            ("_allowed_csi_buckets", "allowed_csi_buckets"),
            ("_blocked_csi_buckets", "blocked_csi_buckets"),
            ("_allowed_basis_regimes", "allowed_basis_regimes"),
            ("_blocked_basis_regimes", "blocked_basis_regimes"),
            ("_allowed_funding_regimes", "allowed_funding_regimes"),
            ("_blocked_funding_regimes", "blocked_funding_regimes"),
        ):
            if key in payload:
                setattr(self, attr, self._parse_set(str(payload[key])))
        self._break_even_trigger_r = float(payload.get("break_even_trigger_r", self._break_even_trigger_r))
        self._profit_lock_trigger_r = float(payload.get("profit_lock_trigger_r", self._profit_lock_trigger_r))
        self._profit_lock_r = float(payload.get("profit_lock_r", self._profit_lock_r))
        self._runner_trail_trigger_r = float(payload.get("runner_trail_trigger_r", self._runner_trail_trigger_r))
        self._runner_trail_distance_r = float(payload.get("runner_trail_distance_r", self._runner_trail_distance_r))
        self._loss_cap_r = float(payload.get("loss_cap_r", self._loss_cap_r))

    def _state_gate_ok(self, ctx: Mapping[str, Any], symbol: str) -> bool:
        gates = (
            self._allowed_vol_regimes,
            self._blocked_vol_regimes,
            self._allowed_liquidity_regimes,
            self._blocked_liquidity_regimes,
            self._allowed_displacement_regimes,
            self._blocked_displacement_regimes,
            self._allowed_csi_buckets,
            self._blocked_csi_buckets,
            self._allowed_basis_regimes,
            self._blocked_basis_regimes,
            self._allowed_funding_regimes,
            self._blocked_funding_regimes,
        )
        if not any(gates):
            return True
        root = ctx.get("state") if isinstance(ctx, Mapping) else None
        snapshot = root.get(symbol) if isinstance(root, Mapping) else None
        if not isinstance(snapshot, Mapping):
            return False

        def check(key: str, allowed: set[str], blocked: set[str]) -> bool:
            value = snapshot.get(key)
            if value is None:
                return not allowed
            normalized = str(value)
            if allowed and normalized not in allowed:
                return False
            if blocked and normalized in blocked:
                return False
            return True

        return (
            check("entry_state_vol_regime", self._allowed_vol_regimes, self._blocked_vol_regimes)
            and check("entry_state_liquidity_regime", self._allowed_liquidity_regimes, self._blocked_liquidity_regimes)
            and check("entry_state_displacement_regime", self._allowed_displacement_regimes, self._blocked_displacement_regimes)
            and check("entry_state_csi_bucket", self._allowed_csi_buckets, self._blocked_csi_buckets)
            and check("entry_state_basis_regime", self._allowed_basis_regimes, self._blocked_basis_regimes)
            and check("entry_state_funding_regime", self._allowed_funding_regimes, self._blocked_funding_regimes)
        )

    def _side_gate_ok(self, side: Side) -> bool:
        if self._allowed_sides in {"both", "all", ""}:
            return True
        if self._allowed_sides in {"long", "buy"}:
            return side == Side.BUY
        if self._allowed_sides in {"short", "sell"}:
            return side == Side.SELL
        raise ValueError(f"unknown allowed_sides={self._allowed_sides!r}")

    def _concentration_gate_ok(self, symbol: str, signal_ts: pd.Timestamp) -> bool:
        if symbol in self._excluded_symbols:
            return False
        if self._entry_cooldown_hours <= 0:
            return True
        last_entry_ts = self._last_entry_ts_by_symbol.get(symbol)
        if last_entry_ts is None:
            return True
        elapsed_hours = (pd.Timestamp(signal_ts) - pd.Timestamp(last_entry_ts)).total_seconds() / 3600.0
        return elapsed_hours >= self._entry_cooldown_hours

    def _protection_enabled(self) -> bool:
        return self._family_variant == "L1-H11C" or self._break_even_trigger_r > 0 or (
            self._profit_lock_trigger_r > 0 and self._profit_lock_r > 0
        ) or (
            self._runner_trail_trigger_r > 0 and self._runner_trail_distance_r > 0
        )

    def _apply_profit_protection(self, st: _State, current: Side, bar: Bar) -> None:
        if st.entry_price is None or not st.stop_distance_frozen or st.stop_distance_frozen <= 0:
            return
        if current == Side.BUY:
            st.favorable_extreme_price = max(float(st.favorable_extreme_price or st.entry_price), float(bar.high))
            mfe_r = (float(st.favorable_extreme_price) - float(st.entry_price)) / float(st.stop_distance_frozen)
        else:
            st.favorable_extreme_price = min(float(st.favorable_extreme_price or st.entry_price), float(bar.low))
            mfe_r = (float(st.entry_price) - float(st.favorable_extreme_price)) / float(st.stop_distance_frozen)

        target_stop: float | None = None
        if self._break_even_trigger_r > 0 and mfe_r >= self._break_even_trigger_r:
            target_stop = float(st.entry_price)
        if self._profit_lock_trigger_r > 0 and self._profit_lock_r > 0 and mfe_r >= self._profit_lock_trigger_r:
            if current == Side.BUY:
                target_stop = float(st.entry_price) + (self._profit_lock_r * float(st.stop_distance_frozen))
            else:
                target_stop = float(st.entry_price) - (self._profit_lock_r * float(st.stop_distance_frozen))
        if self._runner_trail_trigger_r > 0 and self._runner_trail_distance_r > 0 and mfe_r >= self._runner_trail_trigger_r:
            st.runner_trail_armed = True
            trail_distance = self._runner_trail_distance_r * float(st.stop_distance_frozen)
            if current == Side.BUY:
                target_stop = max(float(target_stop or st.entry_price), float(st.favorable_extreme_price) - trail_distance)
            else:
                target_stop = min(float(target_stop or st.entry_price), float(st.favorable_extreme_price) + trail_distance)
        if target_stop is None:
            return
        st.lock_armed = True
        if current == Side.BUY:
            st.stop_price_frozen = max(float(st.stop_price_frozen), target_stop)
        else:
            st.stop_price_frozen = min(float(st.stop_price_frozen), target_stop)

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        signals: list[Signal] = []
        htf_root = ctx.get("htf") if isinstance(ctx, Mapping) else None
        if not isinstance(htf_root, Mapping):
            raise RuntimeError(f"L1-H11 requires ctx['htf']['{self._timeframe}'] for two-clock semantics.")
        htf_for_tf = htf_root.get(self._timeframe) or {}
        if not isinstance(htf_for_tf, Mapping):
            raise RuntimeError(f"L1-H11 requires mapping ctx['htf']['{self._timeframe}'] for two-clock semantics.")

        for symbol in sorted(tradeable):
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue
            st = self._state_for(symbol)
            st.exec_vwap.update(bar)

            signal_bar = htf_for_tf.get(symbol)
            has_new_signal_bar = signal_bar is not None and signal_bar.ts != st.last_signal_ts

            current = self._ctx_position_side(ctx, symbol)
            if current is not None:
                st.position = current
                if has_new_signal_bar:
                    st.signal_bars_held += 1

                if st.stop_price_frozen is not None:
                    if current == Side.BUY and bar.low <= st.stop_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h11_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="quality_filtered_continuation_entry",
                            setup_class="quality_filtered_continuation",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h11_quality_filtered_continuation"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                            "close_only": True,
                            "is_exit": True,
                            "exit_reason": "runner_trail_stop" if st.runner_trail_armed else "protected_stop" if st.lock_armed else "stop_loss",
                            "stop_price": st.stop_price_frozen,
                            "stop_distance": st.stop_distance_frozen,
                            "lock_armed": st.lock_armed,
                            "runner_trail_armed": st.runner_trail_armed,
                            "atr_entry": st.atr_entry,
                            "signal_timeframe": self._timeframe,
                            "exit_monitoring_timeframe": "1m",
                        }))
                        self._clear_position(st)
                        continue
                    if current == Side.SELL and bar.high >= st.stop_price_frozen:
                        signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h11_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="quality_filtered_continuation_entry",
                            setup_class="quality_filtered_continuation",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h11_quality_filtered_continuation"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                            "close_only": True,
                            "is_exit": True,
                            "exit_reason": "runner_trail_stop" if st.runner_trail_armed else "protected_stop" if st.lock_armed else "stop_loss",
                            "stop_price": st.stop_price_frozen,
                            "stop_distance": st.stop_distance_frozen,
                            "lock_armed": st.lock_armed,
                            "runner_trail_armed": st.runner_trail_armed,
                            "atr_entry": st.atr_entry,
                            "signal_timeframe": self._timeframe,
                            "exit_monitoring_timeframe": "1m",
                        }))
                        self._clear_position(st)
                        continue

                if self._protection_enabled():
                    self._apply_profit_protection(st, current, bar)
                if self._family_variant == "L1-H11C" and st.entry_price is not None and st.stop_distance_frozen and st.stop_distance_frozen > 0:
                    if self._vwap_giveback == "on" and st.lock_armed and st.exec_vwap.value is not None:
                        vwap_v = float(st.exec_vwap.value)
                        if current == Side.BUY and float(bar.close) < vwap_v:
                            signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL, signal_type="l1_h11_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="quality_filtered_continuation_entry",
                            setup_class="quality_filtered_continuation",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h11_quality_filtered_continuation"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                                "close_only": True,
                                "is_exit": True,
                                "exit_reason": "vwap_giveback",
                                "vwap_giveback_mode": "on",
                                "lock_r": self._lock_r,
                                "lock_armed": st.lock_armed,
                                "signal_timeframe": self._timeframe,
                                "exit_monitoring_timeframe": "1m",
                            }))
                            self._clear_position(st)
                            continue
                        if current == Side.SELL and float(bar.close) > vwap_v:
                            signals.append(Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="l1_h11_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="quality_filtered_continuation_entry",
                            setup_class="quality_filtered_continuation",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h11_quality_filtered_continuation"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                                "close_only": True,
                                "is_exit": True,
                                "exit_reason": "vwap_giveback",
                                "vwap_giveback_mode": "on",
                                "lock_r": self._lock_r,
                                "lock_armed": st.lock_armed,
                                "signal_timeframe": self._timeframe,
                                "exit_monitoring_timeframe": "1m",
                            }))
                            self._clear_position(st)
                            continue

                if has_new_signal_bar and signal_bar is not None:
                    st.ema_fast.update(signal_bar)
                    st.ema_slow.update(signal_bar)
                    if st.ema_fast.value is not None and st.ema_slow.value is not None:
                        trend = Side.BUY if st.ema_fast.value > st.ema_slow.value else Side.SELL if st.ema_fast.value < st.ema_slow.value else None
                        if trend is not None and trend != current:
                            signals.append(Signal(ts=ts, symbol=symbol, side=Side.SELL if current == Side.BUY else Side.BUY, signal_type="l1_h11_exit", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="quality_filtered_continuation_entry",
                            setup_class="quality_filtered_continuation",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h11_quality_filtered_continuation"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                                "close_only": True,
                                "is_exit": True,
                                "exit_reason": "trend_failure",
                                "signal_timeframe": self._timeframe,
                                "exit_monitoring_timeframe": "1m",
                            }))
                            self._clear_position(st)
                            st.last_signal_ts = signal_bar.ts
                            continue
                    st.last_signal_ts = signal_bar.ts
                continue

            self._clear_position(st)
            if not has_new_signal_bar or signal_bar is None:
                continue
            st.last_signal_ts = signal_bar.ts
            st.ema_fast.update(signal_bar)
            st.ema_slow.update(signal_bar)
            st.adx.update(signal_bar)
            st.atr_signal.update(signal_bar)

            ema_fast = st.ema_fast.value
            ema_slow = st.ema_slow.value
            adx_v = st.adx.values.get("adx")
            atr_v = st.atr_signal.value
            if ema_fast is None or ema_slow is None or adx_v is None or atr_v is None or float(atr_v) <= 0:
                continue

            trend_dir = Side.BUY if ema_fast > ema_slow else Side.SELL if ema_fast < ema_slow else None
            if trend_dir is None:
                self._clear_pullback(st)
                continue
            if (
                not self._side_gate_ok(trend_dir)
                or not self._state_gate_ok(ctx, symbol)
                or not self._concentration_gate_ok(symbol, pd.Timestamp(signal_bar.ts))
            ):
                self._clear_pullback(st)
                continue

            if trend_dir != st.trend_dir:
                st.trend_dir = trend_dir
                st.trend_anchor_price = float(signal_bar.close)
                st.trend_extreme_price = float(signal_bar.close)
                self._clear_pullback(st)
            else:
                if trend_dir == Side.BUY:
                    st.trend_extreme_price = max(float(st.trend_extreme_price or signal_bar.close), float(signal_bar.high))
                else:
                    st.trend_extreme_price = min(float(st.trend_extreme_price or signal_bar.close), float(signal_bar.low))

            adx_threshold = self._adx_min if self._family_variant in {"L1-H11A", "L1-H11B"} else self._adx_min_fixed
            if float(adx_v) < float(adx_threshold):
                self._clear_pullback(st)
                continue

            trend_key = "long" if trend_dir == Side.BUY else "short"
            swing_atr = swing_distance_atr(
                trend_dir=trend_key,
                trend_anchor_price=float(st.trend_anchor_price or signal_bar.close),
                trend_extreme_price=float(st.trend_extreme_price or signal_bar.close),
                atr=float(atr_v),
            )
            if swing_atr is None or swing_atr < self._impulse_threshold():
                self._clear_pullback(st)
                continue

            low, high, close = float(signal_bar.low), float(signal_bar.high), float(signal_bar.close)
            touched_pullback_zone = (trend_dir == Side.BUY and low <= float(ema_fast)) or (trend_dir == Side.SELL and high >= float(ema_fast))
            if touched_pullback_zone and not st.pullback_active:
                st.pullback_active = True
                st.pullback_signal_ts = signal_bar.ts
                st.pullback_extreme_low = low
                st.pullback_extreme_high = high
                continue

            if not st.pullback_active:
                continue
            st.pullback_extreme_low = low if st.pullback_extreme_low is None else min(float(st.pullback_extreme_low), low)
            st.pullback_extreme_high = high if st.pullback_extreme_high is None else max(float(st.pullback_extreme_high), high)

            pb_depth = pullback_depth_atr(
                trend_dir=trend_key,
                ema_fast=float(ema_fast),
                pullback_extreme_low=float(st.pullback_extreme_low),
                pullback_extreme_high=float(st.pullback_extreme_high),
                atr=float(atr_v),
            )
            if pb_depth is None:
                continue
            reclaim_ok = (trend_dir == Side.BUY and close >= float(ema_fast)) or (trend_dir == Side.SELL and close <= float(ema_fast))
            in_zone = self._pull_entry_atr_low <= float(pb_depth) <= self._pull_entry_atr_high
            if not reclaim_ok or not in_zone:
                if float(pb_depth) > self._pull_entry_atr_high:
                    self._clear_pullback(st)
                continue

            entry_ref = float(bar.close)
            struct_stop_distance = None
            if trend_dir == Side.BUY and st.pullback_extreme_low is not None:
                struct_stop_distance = max(0.0, entry_ref - float(st.pullback_extreme_low))
            if trend_dir == Side.SELL and st.pullback_extreme_high is not None:
                struct_stop_distance = max(0.0, float(st.pullback_extreme_high) - entry_ref)
            atr_stop_distance = self._stop_atr_mult * float(atr_v)
            if self._family_variant == "L1-H11C":
                base_distance = max(float(struct_stop_distance or 0.0), float(atr_stop_distance))
                stop_distance = base_distance + (self._stop_padding_atr * float(atr_v))
                stop_model = "structure_plus_atr_padding"
            else:
                stop_distance = atr_stop_distance
                stop_model = "fixed_atr_multiple"
            if self._loss_cap_r > 0:
                stop_distance = min(float(stop_distance), float(self._loss_cap_r) * float(atr_stop_distance))
            stop_price = entry_ref - stop_distance if trend_dir == Side.BUY else entry_ref + stop_distance

            entry_pos = entry_position_ratio(
                trend_dir=trend_key,
                entry_price=entry_ref,
                pullback_extreme_low=float(st.pullback_extreme_low),
                pullback_extreme_high=float(st.pullback_extreme_high),
                trend_extreme_price=float(st.trend_extreme_price or close),
            )

            st.position = trend_dir
            st.entry_price = entry_ref
            st.atr_entry = float(atr_v)
            st.stop_distance_frozen = float(stop_distance)
            st.stop_price_frozen = float(stop_price)
            st.favorable_extreme_price = entry_ref
            st.lock_armed = False
            st.runner_trail_armed = False
            st.signal_bars_held = 0
            self._last_entry_ts_by_symbol[symbol] = pd.Timestamp(signal_bar.ts)

            signals.append(Signal(ts=ts, symbol=symbol, side=trend_dir, signal_type="l1_h11_entry", confidence=1.0, metadata={
                        "decision_trace": make_decision_trace(
                            reason_code="quality_filtered_continuation_entry",
                            setup_class="quality_filtered_continuation",
                            hypothesis_branch="entry",
                            conditions_bool_map={},
                            blockers_bool_map={},
                            permission_layer_state={},
                            parameter_combination={"strategy": "l1_h11_quality_filtered_continuation"},
                            gate_values={},
                            gate_thresholds={},
                            gate_margins={},
                            most_binding_gate=None,
                        ),
                "strategy": "l1_h11_quality_filtered_continuation",
                "strategy_id": "l1_h11_quality_filtered_continuation",
                "family_variant": self._family_variant,
                "family_pattern": "quality_filtered_continuation",
                "parent_family": "L1-H11",
                "setup_type": self._setup_type,
                "signal_timeframe": self._timeframe,
                "execution_timeframe": "1m",
                "exit_monitoring_timeframe": "1m",
                "entry_ts": str(ts),
                "entry_price": entry_ref,
                "intended_entry_price": entry_ref,
                "trend_dir": trend_key,
                "ema_fast_entry": float(ema_fast),
                "ema_slow_entry": float(ema_slow),
                "adx_entry": float(adx_v),
                "atr_entry": float(atr_v),
                "impulse_strength_atr": float(swing_atr),
                "swing_distance_atr": float(swing_atr),
                "pullback_depth_atr": float(pb_depth),
                "pull_entry_atr_low": self._pull_entry_atr_low,
                "pull_entry_atr_high": self._pull_entry_atr_high,
                "entry_position_metric": entry_pos,
                "reclaim_position_metric": entry_pos,
                "continuation_trigger_state": "ema20_reclaim_confirmed",
                "entry_reference_price": entry_ref,
                "stop_distance": float(stop_distance),
                "stop_price": float(stop_price),
                "entry_stop_price": float(stop_price),
                "stop_padding_atr": self._stop_padding_atr if self._family_variant == "L1-H11C" else None,
                "lock_r": self._lock_r if self._family_variant == "L1-H11C" else None,
                "vwap_giveback_mode": self._vwap_giveback if self._family_variant == "L1-H11C" else None,
                "h11a_tuning_profile": self._h11a_tuning_profile,
                "h11_tuning_profile": self._h11a_tuning_profile,
                "allowed_sides": self._allowed_sides,
                "allowed_vol_regimes": sorted(self._allowed_vol_regimes),
                "blocked_vol_regimes": sorted(self._blocked_vol_regimes),
                "allowed_liquidity_regimes": sorted(self._allowed_liquidity_regimes),
                "blocked_liquidity_regimes": sorted(self._blocked_liquidity_regimes),
                "allowed_displacement_regimes": sorted(self._allowed_displacement_regimes),
                "blocked_displacement_regimes": sorted(self._blocked_displacement_regimes),
                "allowed_csi_buckets": sorted(self._allowed_csi_buckets),
                "blocked_csi_buckets": sorted(self._blocked_csi_buckets),
                "allowed_basis_regimes": sorted(self._allowed_basis_regimes),
                "blocked_basis_regimes": sorted(self._blocked_basis_regimes),
                "allowed_funding_regimes": sorted(self._allowed_funding_regimes),
                "blocked_funding_regimes": sorted(self._blocked_funding_regimes),
                "excluded_symbols": sorted(self._excluded_symbols),
                "entry_cooldown_hours": self._entry_cooldown_hours,
                "break_even_trigger_r": self._break_even_trigger_r,
                "profit_lock_trigger_r": self._profit_lock_trigger_r,
                "profit_lock_r": self._profit_lock_r,
                "runner_trail_trigger_r": self._runner_trail_trigger_r,
                "runner_trail_distance_r": self._runner_trail_distance_r,
                "loss_cap_r": self._loss_cap_r,
                "impulse_min_threshold_atr": self._impulse_threshold(),
                "entry_reason": "quality_filtered_continuation_reclaim",
                "base_data_frequency_expected": "1m",
                "risk_accounting": "engine_canonical_R",
                "r_per_trade": 0.005,
                "no_pyramiding": True,
                "stop_model": stop_model,
                "stop_update_policy": "frozen_at_entry_then_breakeven_profit_lock_runner_trail",
                "atr_source_timeframe": self._timeframe,
            }))
            self._clear_pullback(st)
        return signals
