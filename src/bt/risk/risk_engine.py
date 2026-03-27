"""Risk engine that converts signals into order intents."""
from __future__ import annotations

from dataclasses import replace
import math
from typing import Any

import pandas as pd

from bt.core.enums import OrderType, Side
from bt.core.types import Bar, OrderIntent, Signal
from bt.risk.reject_codes import (
    ALREADY_IN_POSITION,
    CLOSE_ONLY_NO_POSITION,
    INSUFFICIENT_FREE_MARGIN,
    INVALID_FLIP,
    INVALID_SIDE,
    MAX_POSITIONS_REACHED,
    MIN_STOP_DISTANCE_VIOLATION,
    NO_EQUITY,
    NO_SIDE,
    QTY_SIGN_INVARIANT_FAILED,
    RISK_APPROVED,
    RISK_APPROVED_CLOSE_ONLY,
    STOP_FALLBACK_LEGACY_PROXY,
    STOP_UNRESOLVABLE_SAFE_NO_PROXY,
    STOP_UNRESOLVABLE_STRICT,
    SYMBOL_MISMATCH,
    validate_known,
)
from bt.risk.stop_resolver import resolve_stop_from_spec
from bt.risk.stop_spec import normalize_stop_spec
from bt.risk.margin_math import compute_snapshot, initial_margin_required
from bt.risk.spec import parse_risk_spec
from bt.risk.stop_distance import resolve_stop_distance
from bt.orders.side import resolve_order_side
from bt.instruments.registry import resolve_instrument_spec
from bt.risk.instrument_sizing import size_position_from_risk


class RiskEngine:
    def __init__(
        self,
        *,
        max_positions: int,
        max_notional_per_symbol: float | None = None,
        margin_buffer_tier: int = 1,
        maker_fee_bps: float = 0.0,
        taker_fee_bps: float = 0.0,
        slippage_k_proxy: float = 0.0,
        eps: float = 1e-12,
        config: dict[str, Any] | None = None,
    ) -> None:
        self.max_positions = max_positions
        self.max_notional_per_symbol = max_notional_per_symbol
        self.margin_buffer_tier = int(margin_buffer_tier)
        self.maker_fee_bps = float(maker_fee_bps)
        self.taker_fee_bps = float(taker_fee_bps)
        self.slippage_k_proxy = float(slippage_k_proxy)
        self.eps = eps
        if config is None:
            raise ValueError("risk.mode and risk.r_per_trade are required")
        self._config = config
        self._risk_spec = parse_risk_spec(config)

    @staticmethod
    def _round_qty(qty: float, rounding: str) -> float:
        if rounding == "none":
            return qty
        scale = 10**8
        if rounding == "floor":
            return math.floor(qty * scale) / scale
        if rounding == "round":
            return round(qty, 8)
        raise ValueError(f"Invalid risk.qty_rounding={rounding!r}")

    def _risk_cfg(self) -> dict[str, Any]:
        risk_cfg = self._config.get("risk", {}) if isinstance(self._config, dict) else {}
        return risk_cfg if isinstance(risk_cfg, dict) else {}

    def _resolve_instrument_for_symbol(self, symbol: str):
        try:
            return resolve_instrument_spec(self._config, symbol=symbol)
        except ValueError:
            return None

    def _fx_lot_step(self) -> float | None:
        fx_cfg = self._risk_cfg().get("fx")
        if not isinstance(fx_cfg, dict):
            return None
        value = fx_cfg.get("lot_step")
        return None if value is None else float(value)

    def _fx_pip_value_override(self) -> float | None:
        fx_cfg = self._risk_cfg().get("fx")
        if not isinstance(fx_cfg, dict):
            return None
        value = fx_cfg.get("pip_value_override")
        return None if value is None else float(value)

    def _margin_leverage_override(self) -> float | None:
        margin_cfg = self._risk_cfg().get("margin")
        if not isinstance(margin_cfg, dict):
            return None
        value = margin_cfg.get("leverage")
        return None if value is None else float(value)

    def _entry_notional_for_qty(self, *, qty: float, price: float, symbol: str) -> float:
        instrument = self._resolve_instrument_for_symbol(symbol)
        if instrument is not None and instrument.type == "forex":
            contract_size = float(instrument.contract_size or 0.0)
            return abs(qty) * contract_size * price
        return abs(qty) * price

    def _stop_resolution_mode(self) -> str:
        # risk.stop_resolution applies only to ENTRY / increase-risk signals.
        # Exit/reduce-risk flows bypass stop resolution entirely.
        risk_cfg = self._config.get("risk", {}) if isinstance(self._config, dict) else {}
        mode = risk_cfg.get("stop_resolution", "safe") if isinstance(risk_cfg, dict) else "safe"
        normalized = str(mode)
        if normalized not in {"safe", "strict"}:
            raise ValueError(
                "Invalid config: risk.stop_resolution must be one of ['safe', 'strict']. "
                f"Got risk.stop_resolution={normalized!r}. Example fix:\n"
                "risk:\n"
                "  stop_resolution: safe\n"
                "  allow_legacy_proxy: false"
            )
        return normalized

    def _allow_legacy_proxy(self) -> bool:
        risk_cfg = self._config.get("risk", {}) if isinstance(self._config, dict) else {}
        allow_legacy = risk_cfg.get("allow_legacy_proxy", False) if isinstance(risk_cfg, dict) else False
        if not isinstance(allow_legacy, bool):
            raise ValueError(
                "Invalid config: risk.allow_legacy_proxy must be a boolean. "
                f"Got risk.allow_legacy_proxy={allow_legacy!r}. Example fix:\n"
                "risk:\n"
                "  stop_resolution: safe\n"
                "  allow_legacy_proxy: true"
            )
        if self._stop_resolution_mode() == "strict" and allow_legacy:
            raise ValueError(
                "Invalid config: risk.allow_legacy_proxy=true is not allowed when risk.stop_resolution=strict. "
                "Example fix:\n"
                "risk:\n"
                "  stop_resolution: strict\n"
                "  allow_legacy_proxy: false"
            )
        return allow_legacy

    def _resolve_stop_contract(
        self,
        *,
        signal: Signal,
        symbol: str,
        side: str,
        entry_price: float,
        bar: Bar,
        ctx_payload: dict[str, object],
        equity: float,
    ) -> tuple[float, dict[str, object]]:
        stop_resolution_mode = self._stop_resolution_mode()
        allow_legacy = self._allow_legacy_proxy()
        stop_spec = normalize_stop_spec(signal, config=self._config)

        risk_amount = equity * self._risk_spec.r_per_trade
        risk_meta: dict[str, object] = {
            "risk_amount": risk_amount,
            "stop_distance": None,
            "stop_price": None,
            "stop_source": None,
            "stop_details": {},
            "stop_reason_code": None,
            "stop_contract_version": stop_spec.contract_version if stop_spec is not None else None,
            "stop_resolution_mode": stop_resolution_mode,
            "used_legacy_stop_proxy": False,
            "r_metrics_valid": False,
        }

        if stop_spec is not None:
            resolved = resolve_stop_from_spec(
                stop_spec,
                symbol=symbol,
                side=side,
                entry_price=entry_price,
                bar=bar,
                ctx=ctx_payload,
                config=self._config,
            )
            if resolved.stop_distance is None:
                raise ValueError(
                    f"{symbol}: StrategyContractError: stop_spec was provided but stop_distance is unresolved for signal_type={signal.signal_type}. "
                    "Example fix snippet:\n"
                    "signal:\n"
                    "  metadata:\n"
                    "    stop_spec:\n"
                    "      kind: atr\n"
                    "      atr_multiple: 2.5"
                )
            stop_distance = float(resolved.stop_distance)
            if stop_distance <= 0:
                raise ValueError(f"{symbol}: invalid stop_distance computed: {stop_distance}")
            risk_meta.update(
                {
                    "stop_distance": stop_distance,
                    "stop_price": resolved.stop_price,
                    "stop_source": resolved.stop_source,
                    "stop_details": resolved.details or {},
                    "stop_reason_code": resolved.reason_code,
                    "used_legacy_stop_proxy": bool(resolved.used_fallback),
                    "r_metrics_valid": not bool(resolved.used_fallback),
                }
            )
        elif stop_resolution_mode == "strict":
            validate_known(STOP_UNRESOLVABLE_STRICT)
            raise ValueError(
                f"{STOP_UNRESOLVABLE_STRICT}: {symbol}: StrategyContractError: missing stop for entry sizing in strict mode "
                f"(risk.stop_resolution={stop_resolution_mode}, signal_type={signal.signal_type}, side={side}). "
                "Provide a resolvable stop via signal.stop_price or signal.metadata.stop_spec. "
                "Example fix snippet:\n"
                "signal:\n"
                "  stop_price: 123.45\n"
                "# OR\n"
                "signal:\n"
                "  metadata:\n"
                "    stop_spec: {kind: atr, atr_multiple: 2.5}\n"
                "# OR, if fallback is truly intended:\n"
                "risk:\n"
                "  stop_resolution: safe\n"
                "  allow_legacy_proxy: true"
            )
        elif not allow_legacy:
            validate_known(STOP_UNRESOLVABLE_SAFE_NO_PROXY)
            raise ValueError(
                f"{STOP_UNRESOLVABLE_SAFE_NO_PROXY}: {symbol}: Safe mode is active but legacy proxy fallback is disabled "
                f"(risk.stop_resolution={stop_resolution_mode}, risk.allow_legacy_proxy={allow_legacy}, signal_type={signal.signal_type}, side={side}). "
                "Set risk.allow_legacy_proxy: true to allow fallback OR attach stop_spec/stop_price. "
                "Example fix snippet:\n"
                "risk:\n"
                "  stop_resolution: safe\n"
                "  allow_legacy_proxy: true"
            )
        else:
            legacy_cfg = {"risk": dict(self._config.get("risk", {}))}
            legacy_stop_cfg = dict(legacy_cfg["risk"].get("stop", {}))
            legacy_stop_cfg["mode"] = "legacy_proxy"
            legacy_cfg["risk"]["stop"] = legacy_stop_cfg
            stop_result = resolve_stop_distance(
                symbol=symbol,
                side=side,
                entry_price=entry_price,
                signal={},
                bars_by_symbol={symbol: bar},
                ctx=ctx_payload,
                config=legacy_cfg,
            )
            stop_distance = float(stop_result.stop_distance)
            if stop_distance <= 0:
                raise ValueError(f"{symbol}: invalid stop_distance computed: {stop_distance}")
            risk_meta.update(
                {
                    "stop_distance": stop_distance,
                    "stop_source": "legacy_high_low_proxy",
                    "stop_details": stop_result.details,
                    "stop_reason_code": STOP_FALLBACK_LEGACY_PROXY,
                    "used_legacy_stop_proxy": True,
                    "r_metrics_valid": False,
                }
            )

        min_stop_distance = self._risk_spec.min_stop_distance
        stop_distance = float(risk_meta["stop_distance"])
        if min_stop_distance is not None:
            stop_distance = max(stop_distance, min_stop_distance)
            risk_meta["stop_distance"] = stop_distance

        instrument = self._resolve_instrument_for_symbol(symbol)
        if instrument is None or instrument.type == "crypto":
            qty = risk_amount / stop_distance
            risk_cfg = self._risk_cfg()
            qty_rounding = str(risk_cfg.get("qty_rounding", "none"))
            qty = self._round_qty(qty, qty_rounding)
            if not math.isfinite(qty) or qty <= 0:
                raise ValueError(f"{symbol}: invalid qty computed: {qty}")
            risk_meta.update(
                {
                    "qty_rounding_unit": 0.0,
                    "instrument_type": "crypto" if instrument is None else instrument.type,
                    "notional": abs(qty) * entry_price,
                    "margin_required": None,
                }
            )
            return qty, risk_meta

        sizing = size_position_from_risk(
            instrument=instrument,
            entry_price=entry_price,
            stop_price=float(risk_meta["stop_price"]),
            risk_amount=risk_amount,
            account_leverage=self._margin_leverage_override(),
            fx_lot_step=self._fx_lot_step(),
            fx_pip_value_override=self._fx_pip_value_override(),
        )
        risk_meta.update(
            {
                "qty_rounding_unit": sizing.rounding_unit,
                "instrument_type": instrument.type,
                "notional": sizing.notional,
                "margin_required": sizing.margin_required,
            }
        )
        return sizing.qty_rounded, risk_meta

    def _min_stop_distance_pct(self) -> float:
        risk_cfg = self._config.get("risk", {}) if isinstance(self._config, dict) else {}
        if not isinstance(risk_cfg, dict):
            return 0.001
        return float(risk_cfg.get("min_stop_distance_pct", 0.001))

    def _max_notional_pct_equity(self) -> float:
        risk_cfg = self._config.get("risk", {}) if isinstance(self._config, dict) else {}
        if not isinstance(risk_cfg, dict):
            return 1.0
        return float(risk_cfg.get("max_notional_pct_equity", 1.0))

    @staticmethod
    def _is_exit_signal(signal: Signal) -> bool:
        metadata = signal.metadata if isinstance(signal.metadata, dict) else {}
        if bool(metadata.get("is_exit")) or bool(metadata.get("reduce_only")):
            return True
        signal_type = str(signal.signal_type)
        return signal_type.endswith("_exit")


    def _maintenance_free_margin_pct(self) -> float:
        return float(self._risk_spec.maintenance_free_margin_pct)

    def allows_may_liquidate(self) -> bool:
        risk_cfg = self._config.get("risk", {}) if isinstance(self._config, dict) else {}
        return bool(risk_cfg.get("may_liquidate", False)) if isinstance(risk_cfg, dict) else False

    def _margin_adverse_move_tier_multiplier(self) -> float:
        return {1: 1.0, 2: 2.0, 3: 3.0}.get(self.margin_buffer_tier, 1.0)
    @staticmethod
    def _qty_sign_invariant_ok(*, signal_side: Side, current_qty: float, flip: bool, order_qty: float, close_only: bool) -> bool:
        if close_only:
            return order_qty == -current_qty
        if signal_side == Side.BUY and current_qty >= 0 and order_qty <= 0:
            return False
        if signal_side == Side.SELL and current_qty <= 0 and order_qty >= 0:
            return False
        if current_qty != 0 and flip:
            if current_qty > 0 and signal_side == Side.SELL and order_qty >= 0:
                return False
            if current_qty < 0 and signal_side == Side.BUY and order_qty <= 0:
                return False
        return True

    def compute_position_size_r(
        self,
        *,
        symbol: str,
        side: str,
        entry_price: float,
        signal: object,
        bars_by_symbol: dict[str, object],
        ctx: dict[str, object],
        equity: float,
    ) -> tuple[float, dict[str, object]]:
        if equity <= 0:
            raise ValueError(f"{symbol}: equity must be > 0, got {equity}")
        if self._risk_spec.r_per_trade <= 0:
            raise ValueError(f"{symbol}: r_per_trade must be > 0, got {self._risk_spec.r_per_trade}")

        risk_amount = equity * self._risk_spec.r_per_trade
        stop_result = resolve_stop_distance(
            symbol=symbol,
            side=side,
            entry_price=entry_price,
            signal=signal,
            bars_by_symbol=bars_by_symbol,
            ctx=ctx,
            config=self._config,
        )
        stop_distance = float(stop_result.stop_distance)
        if stop_distance <= 0:
            raise ValueError(f"{symbol}: invalid stop_distance computed: {stop_distance}")

        min_stop_distance = self._risk_spec.min_stop_distance
        if min_stop_distance is not None:
            stop_distance = max(stop_distance, min_stop_distance)

        instrument = self._resolve_instrument_for_symbol(symbol)
        if instrument is None or instrument.type == "crypto":
            qty = risk_amount / stop_distance
            risk_cfg = self._risk_cfg()
            qty_rounding = str(risk_cfg.get("qty_rounding", "none"))
            qty = self._round_qty(qty, qty_rounding)
            if not math.isfinite(qty) or qty <= 0:
                raise ValueError(f"{symbol}: invalid qty computed: {qty}")

            return qty, {
                "risk_amount": risk_amount,
                "stop_distance": stop_distance,
                "stop_source": stop_result.source,
                "stop_details": stop_result.details,
                "r_metrics_valid": bool(stop_result.source) and stop_distance > 0,
            }

        stop_price = entry_price - stop_distance if side == "long" else entry_price + stop_distance
        sizing = size_position_from_risk(
            instrument=instrument,
            entry_price=entry_price,
            stop_price=stop_price,
            risk_amount=risk_amount,
            account_leverage=self._margin_leverage_override(),
            fx_lot_step=self._fx_lot_step(),
            fx_pip_value_override=self._fx_pip_value_override(),
        )

        return sizing.qty_rounded, {
            "risk_amount": risk_amount,
            "stop_distance": stop_distance,
            "stop_source": stop_result.source,
            "stop_details": stop_result.details,
            "r_metrics_valid": bool(stop_result.source) and stop_distance > 0,
            "qty_rounding_unit": sizing.rounding_unit,
            "notional": sizing.notional,
            "margin_required": sizing.margin_required,
            "instrument_type": instrument.type,
        }

    def estimate_required_margin(
        self,
        *,
        notional: float,
        max_leverage: float,
        fee_buffer: float,
        slippage_buffer: float,
    ) -> float:
        return initial_margin_required(notional=notional, max_leverage=max_leverage, eps=self.eps) + fee_buffer + slippage_buffer

    def _estimate_buffers(self, notional: float) -> tuple[float, float]:
        fee_bps = max(self.maker_fee_bps, self.taker_fee_bps)
        fee_buffer = notional * (fee_bps / 1e4)

        slippage_model = ""
        fixed_bps: float | None = None
        if isinstance(self._config, dict):
            model_value = self._config.get("model")
            fixed_bps_value = self._config.get("fixed_bps")
            slippage_cfg = self._config.get("slippage")
            if isinstance(slippage_cfg, dict):
                if model_value is None:
                    model_value = slippage_cfg.get("model")
                if fixed_bps_value is None:
                    fixed_bps_value = slippage_cfg.get("fixed_bps")
            slippage_model = str(model_value or "")
            if fixed_bps_value is not None:
                fixed_bps = float(fixed_bps_value)

        if slippage_model == "fixed_bps" and fixed_bps is not None:
            slippage_buffer = notional * (fixed_bps / 1e4)
        else:
            slippage_buffer = notional * self.slippage_k_proxy
        return fee_buffer, slippage_buffer

    def signal_to_order_intent(
        self,
        *,
        ts: pd.Timestamp,
        signal: Signal,
        bar: Bar,
        equity: float,
        free_margin: float,
        open_positions: int,
        max_leverage: float,
        current_qty: float,
    ) -> tuple[OrderIntent | None, str]:
        """
        Returns (order_intent_or_none, reason_string).
        reason_string must be non-empty for both approve and reject.
        """

        if signal.side is None:
            return None, NO_SIDE
        if signal.symbol != bar.symbol:
            return None, SYMBOL_MISMATCH
        if open_positions >= self.max_positions and current_qty == 0:
            return None, MAX_POSITIONS_REACHED
        if equity <= 0:
            return None, NO_EQUITY
        cur_qty = current_qty
        is_exit_signal = self._is_exit_signal(signal)
        close_only = bool(signal.metadata.get("close_only")) or is_exit_signal
        if close_only and cur_qty == 0:
            return None, CLOSE_ONLY_NO_POSITION
        cur_side = None
        if cur_qty > 0:
            cur_side = Side.BUY
        elif cur_qty < 0:
            cur_side = Side.SELL

        if close_only and cur_qty != 0:
            order_qty = -cur_qty
            order_side = resolve_order_side(order_qty)
            if not self._qty_sign_invariant_ok(
                signal_side=signal.side,
                current_qty=cur_qty,
                flip=(cur_qty != 0 and signal.side != cur_side),
                order_qty=order_qty,
                close_only=True,
            ):
                return None, QTY_SIGN_INVARIANT_FAILED
            reason = RISK_APPROVED_CLOSE_ONLY
            metadata = dict(signal.metadata)
            metadata.update(
                {
                    "current_qty": cur_qty,
                    "desired_qty": 0.0,
                    "flip": False,
                    "close_only": True,
                    "notional_est": self._entry_notional_for_qty(qty=order_qty, price=bar.close, symbol=signal.symbol),
                    "cap_applied": False,
                    "margin_required": 0.0,
                    "margin_fee_buffer": 0.0,
                    "margin_slippage_buffer": 0.0,
                    "margin_adverse_move_buffer": 0.0,
                    "free_margin": free_margin,
                    "max_leverage": max_leverage,
                    "scaled_by_margin": False,
                    "reason": reason,
                    "stop_resolution_skipped": is_exit_signal,
                    "stop_resolution_skip_reason": "exit_signal" if is_exit_signal else None,
                }
            )
            signal_with_metadata = replace(signal, metadata=metadata)
            order_intent = OrderIntent(
                ts=ts,
                symbol=signal.symbol,
                side=order_side,
                qty=order_qty,
                order_type=OrderType.MARKET,
                limit_price=None,
                reason=reason,
                metadata=signal_with_metadata.metadata,
            )
            return order_intent, reason

        if cur_qty != 0 and signal.side == cur_side:
            return None, ALREADY_IN_POSITION

        if signal.side == Side.BUY:
            side = "long"
        elif signal.side == Side.SELL:
            side = "short"
        else:
            return None, INVALID_SIDE

        ctx_payload: dict[str, object] = {}
        if isinstance(signal, Signal):
            maybe_ctx = signal.metadata.get("ctx")
            if isinstance(maybe_ctx, dict):
                ctx_payload = maybe_ctx

        if self._risk_spec.mode not in {"r_fixed", "equity_pct"}:
            raise NotImplementedError(f"Unsupported risk mode: {self._risk_spec.mode}")
        stop_resolution_mode = self._stop_resolution_mode()
        try:
            desired_qty, risk_meta = self._resolve_stop_contract(
                signal=signal,
                symbol=signal.symbol,
                side=side,
                entry_price=bar.close,
                bar=bar,
                ctx_payload=ctx_payload,
                equity=equity,
            )
        except ValueError as exc:
            raise ValueError(str(exc)) from exc

        qty_base = float(desired_qty)
        size_factor_t = None
        size_factor_min = None
        size_factor_max = None
        if isinstance(signal.metadata, dict) and "size_factor_t" in signal.metadata:
            raw = signal.metadata.get("size_factor_t")
            raw_min = signal.metadata.get("size_factor_min", signal.metadata.get("cap_multiplier"))
            raw_max = signal.metadata.get("size_factor_max", 1.0)
            try:
                size_factor_t = float(raw)
            except (TypeError, ValueError):
                size_factor_t = None
            try:
                size_factor_min = float(raw_min) if raw_min is not None else 0.0
            except (TypeError, ValueError):
                size_factor_min = 0.0
            try:
                size_factor_max = float(raw_max) if raw_max is not None else 1.0
            except (TypeError, ValueError):
                size_factor_max = 1.0
            if size_factor_min > size_factor_max:
                size_factor_min, size_factor_max = size_factor_max, size_factor_min
            if size_factor_t is not None:
                size_factor_t = float(min(size_factor_max, max(size_factor_min, size_factor_t)))
                desired_qty = float(desired_qty) * size_factor_t
                risk_meta["size_factor_t"] = size_factor_t
                risk_meta["size_factor_min"] = size_factor_min
                risk_meta["size_factor_max"] = size_factor_max
                risk_meta["qty_base"] = qty_base
                risk_meta["qty_adj"] = float(desired_qty)

        risk_meta["r_metrics_valid"] = bool(risk_meta.get("r_metrics_valid", True)) and bool(risk_meta.get("stop_source")) and float(risk_meta.get("stop_distance", 0.0)) > 0

        risk_budget = risk_meta["risk_amount"]
        stop_dist = float(risk_meta["stop_distance"])
        min_stop_distance_pct = self._min_stop_distance_pct()
        if bar.close > 0:
            stop_distance_pct = stop_dist / bar.close
            if stop_distance_pct < min_stop_distance_pct:
                return None, MIN_STOP_DISTANCE_VIOLATION

        desired_notional = self._entry_notional_for_qty(qty=desired_qty, price=bar.close, symbol=signal.symbol)
        cap_applied = False
        cap_reason: str | None = None
        max_notional: float | None = None

        if self.max_notional_per_symbol is not None and desired_notional > self.max_notional_per_symbol:
            scale = self.max_notional_per_symbol / desired_notional
            desired_qty *= scale
            desired_notional = self._entry_notional_for_qty(qty=desired_qty, price=bar.close, symbol=signal.symbol)
            cap_applied = True
            cap_reason = "max_notional_per_symbol"
            max_notional = float(self.max_notional_per_symbol)

        max_notional_equity = equity * self._max_notional_pct_equity()
        if desired_notional > max_notional_equity:
            desired_qty = math.copysign(max_notional_equity / max(self._entry_notional_for_qty(qty=1.0, price=bar.close, symbol=signal.symbol), self.eps), desired_qty)
            desired_notional = self._entry_notional_for_qty(qty=desired_qty, price=bar.close, symbol=signal.symbol)
            cap_applied = True
            cap_reason = "max_notional_pct_equity"
            max_notional = max_notional_equity

        flip = cur_qty != 0 and signal.side != cur_side
        if flip:
            if signal.side == Side.SELL and cur_qty > 0:
                order_qty = -cur_qty - desired_qty
            elif signal.side == Side.BUY and cur_qty < 0:
                order_qty = -cur_qty + desired_qty
            else:
                return None, INVALID_FLIP
        else:
            order_qty = desired_qty if signal.side == Side.BUY else -desired_qty

        if not self._qty_sign_invariant_ok(
            signal_side=signal.side,
            current_qty=cur_qty,
            flip=flip,
            order_qty=order_qty,
            close_only=False,
        ):
            return None, QTY_SIGN_INVARIANT_FAILED

        if free_margin <= 0:
            return None, INSUFFICIENT_FREE_MARGIN

        order_side = resolve_order_side(order_qty)
        mark_price_used_for_margin = bar.close
        if order_side == Side.BUY:
            mark_price_used_for_margin = bar.high
        elif order_side == Side.SELL:
            mark_price_used_for_margin = bar.low

        margin_leverage_used = self._margin_leverage_override() if self._margin_leverage_override() is not None else max_leverage
        notional = self._entry_notional_for_qty(qty=order_qty, price=mark_price_used_for_margin, symbol=signal.symbol)
        fee_buffer, slippage_buffer = self._estimate_buffers(notional)
        adverse_move_per_unit = 0.0
        tier_multiplier = self._margin_adverse_move_tier_multiplier()
        if order_side == Side.BUY:
            adverse_move_per_unit = max(bar.high - bar.close, 0.0) * tier_multiplier
        elif order_side == Side.SELL:
            adverse_move_per_unit = max(bar.close - bar.low, 0.0) * tier_multiplier
        adverse_move_buffer = abs(order_qty) * max(adverse_move_per_unit, 0.0)
        maintenance_free_margin_pct = self._maintenance_free_margin_pct()
        snapshot = compute_snapshot(
            equity=equity,
            notional=notional,
            max_leverage=margin_leverage_used,
            maintenance_free_margin_pct=maintenance_free_margin_pct,
            fee_buffer=fee_buffer,
            slippage_buffer=slippage_buffer,
            adverse_move_buffer=adverse_move_buffer,
            mark_price_used_for_margin=mark_price_used_for_margin,
        )
        margin_required = snapshot.margin_locked
        total_required = snapshot.total_required
        max_total_required = free_margin * (1.0 - maintenance_free_margin_pct)
        scaled_by_margin = False
        if total_required > max_total_required + self.eps:
            adverse_move_per_notional = adverse_move_buffer / notional if notional > 0 else 0.0

            total_required_per_notional = (
                (1.0 / max(margin_leverage_used, self.eps))
                + (fee_buffer / notional if notional > 0 else 0.0)
                + (slippage_buffer / notional if notional > 0 else 0.0)
                + adverse_move_per_notional
            )
            max_affordable_notional = max_total_required / max(total_required_per_notional, self.eps)
            if max_affordable_notional <= 0:
                return None, INSUFFICIENT_FREE_MARGIN

            max_affordable_qty = max_affordable_notional / max(self._entry_notional_for_qty(qty=1.0, price=mark_price_used_for_margin, symbol=signal.symbol), self.eps)
            if max_affordable_qty <= 0:
                return None, INSUFFICIENT_FREE_MARGIN

            if abs(order_qty) > max_affordable_qty:
                order_qty = math.copysign(max_affordable_qty, order_qty)
                scaled_by_margin = True
                if abs(order_qty) <= 0:
                    return None, INSUFFICIENT_FREE_MARGIN
                notional = self._entry_notional_for_qty(qty=order_qty, price=mark_price_used_for_margin, symbol=signal.symbol)
                fee_buffer, slippage_buffer = self._estimate_buffers(notional)
                adverse_move_buffer = abs(order_qty) * max(adverse_move_per_unit, 0.0)
                snapshot = compute_snapshot(
                    equity=equity,
                    notional=notional,
                    max_leverage=margin_leverage_used,
                    maintenance_free_margin_pct=maintenance_free_margin_pct,
                    fee_buffer=fee_buffer,
                    slippage_buffer=slippage_buffer,
                    adverse_move_buffer=adverse_move_buffer,
                    mark_price_used_for_margin=mark_price_used_for_margin,
                )
                margin_required = snapshot.margin_locked
                total_required = snapshot.total_required

            if abs(order_qty) <= 0 or total_required > max_total_required + self.eps:
                return None, INSUFFICIENT_FREE_MARGIN

        reason = RISK_APPROVED
        metadata = dict(signal.metadata)
        metadata.update(
            {
                "risk_budget": risk_budget,
                "stop_dist": stop_dist,
                "risk_amount": risk_meta["risk_amount"],
                "stop_distance": risk_meta["stop_distance"],
                "qty_rounding_unit": risk_meta.get("qty_rounding_unit"),
                "instrument_type": risk_meta.get("instrument_type"),
                "sizing_notional": risk_meta.get("notional"),
                "sizing_margin_required": risk_meta.get("margin_required"),
                "stop_source": risk_meta["stop_source"],
                "stop_details": risk_meta["stop_details"],
                "stop_reason_code": risk_meta.get("stop_reason_code"),
                "stop_contract_version": risk_meta.get("stop_contract_version"),
                "stop_price": risk_meta.get("stop_price"),
                "r_metrics_valid": risk_meta["r_metrics_valid"],
                "used_legacy_stop_proxy": bool(risk_meta.get("used_legacy_stop_proxy", False)),
                "stop_resolution_mode": stop_resolution_mode,
                "size_factor_t": risk_meta.get("size_factor_t"),
                "size_factor_min": risk_meta.get("size_factor_min"),
                "size_factor_max": risk_meta.get("size_factor_max"),
                "qty_base": risk_meta.get("qty_base"),
                "qty_adj": risk_meta.get("qty_adj"),
                "current_qty": cur_qty,
                "desired_qty": desired_qty,
                "flip": flip,
                "notional_est": self._entry_notional_for_qty(qty=order_qty, price=bar.close, symbol=signal.symbol),
                "cap_applied": cap_applied,
                "cap_reason": cap_reason,
                "max_notional": max_notional,
                "margin_required": margin_required,
                "margin_fee_buffer": fee_buffer,
                "margin_slippage_buffer": slippage_buffer,
                "margin_adverse_move_buffer": adverse_move_buffer,
                "free_margin": free_margin,
                "max_leverage": max_leverage,
                "margin_leverage_used": margin_leverage_used,
                "scaled_by_margin": scaled_by_margin,
                "maintenance_free_margin_pct": maintenance_free_margin_pct,
                "max_total_required": max_total_required,
                "total_required": total_required,
                "mark_price_used_for_margin": mark_price_used_for_margin,
                "free_margin_post": snapshot.free_margin_post,
                "maintenance_required": snapshot.maintenance_required,
                "equity_used": snapshot.equity,
                "reason": reason,
            }
        )
        signal_with_metadata = replace(signal, metadata=metadata)

        order_intent = OrderIntent(
            ts=ts,
            symbol=signal.symbol,
            side=order_side,
            qty=order_qty,
            order_type=OrderType.MARKET,
            limit_price=None,
            reason=reason,
            metadata=signal_with_metadata.metadata,
        )
        return order_intent, reason
