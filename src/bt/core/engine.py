"""Backtest engine main event loop."""
from __future__ import annotations

from dataclasses import replace
import csv
from pathlib import Path
from typing import Any, Mapping
import pandas as pd

from bt.core.enums import OrderState, OrderType, PositionState, Side
from bt.core.reason_codes import FORCED_LIQUIDATION_END_OF_RUN, FORCED_LIQUIDATION_MARGIN
from bt.core.types import Order
from bt.data.feed import HistoricalDataFeed
from bt.execution.execution_model import ExecutionModel
from bt.indicators.base import Indicator
from bt.indicators.atr import ATR
from bt.indicators.ema import EMA
from bt.indicators.vwap import VWAP
from bt.features.online_state import OnlineStateFeatureLayer
from bt.logging.jsonl import JsonlWriter
from bt.orders.side import resolve_order_side, validate_order_side_consistency
from bt.logging.sanity import SanityCounters
from bt.logging.trades import TradesCsvWriter
from bt.portfolio.constants import QTY_EPSILON
from bt.portfolio.portfolio import Portfolio
from bt.risk.risk_engine import RiskEngine
from bt.strategy.base import Strategy
from bt.universe.universe import UniverseEngine
from bt.audit.audit_manager import AuditManager
from bt.audit.signal_audit import inspect_signal_context
from bt.audit.order_audit import inspect_order
from bt.audit.fill_audit import inspect_fill
from bt.audit.position_audit import inspect_position
from bt.audit.portfolio_audit import inspect_portfolio
from bt.audit.alignment_audit import inspect_alignment


def _state_feature_options(config: dict[str, Any]) -> tuple[bool, str]:
    """Read state-feature controls without changing the default rich output."""
    nested = config.get("state_features")
    if not isinstance(nested, dict):
        nested = {}
    enabled = config.get("enable_state_features", nested.get("enabled", True))
    profile = config.get("state_feature_profile", nested.get("profile", "full"))
    return bool(enabled), str(profile or "full")


def _positive_int_option(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _decision_logging_options(config: dict[str, Any]) -> tuple[str, int]:
    outputs_cfg = config.get("outputs") if isinstance(config.get("outputs"), dict) else {}
    profile = str(
        config.get(
            "decision_logging_profile",
            outputs_cfg.get("decision_logging_profile", "full"),
        )
        or "full"
    ).strip().lower()
    if profile not in {"full", "research_sparse", "parity_debug"}:
        raise ValueError("decision_logging_profile must be one of: full, research_sparse, parity_debug")
    sample_every = _positive_int_option(
        config.get(
            "decision_negative_sample_every",
            outputs_cfg.get("decision_negative_sample_every", 0),
        ),
        0,
    )
    return profile, sample_every


def _is_missing_metadata_value(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(value != value)
    except Exception:
        return False


class BacktestEngine:
    """Event-driven backtest engine."""

    def __init__(
        self,
        *,
        datafeed: HistoricalDataFeed,
        universe: UniverseEngine,
        strategy: Strategy,
        risk: RiskEngine,
        execution: ExecutionModel,
        portfolio: Portfolio,
        decisions_writer: JsonlWriter,
        fills_writer: JsonlWriter,
        trades_writer: TradesCsvWriter,
        equity_path: Path,
        config: dict,
        sanity_counters: SanityCounters | None = None,
        audit_manager: AuditManager | None = None,
    ) -> None:
        self._datafeed = datafeed
        self._universe = universe
        self._strategy = strategy
        self._risk = risk
        self._execution = execution
        self._portfolio = portfolio
        self._decisions_writer = decisions_writer
        self._fills_writer = fills_writer
        self._trades_writer = trades_writer
        self._equity_path = equity_path
        self._config = config
        self._order_counter = 0
        self._indicators: dict[str, dict[str, Indicator]] = {}
        self._indicator_profile = str(config.get("indicator_profile", "default") or "default").strip().lower()
        if self._indicator_profile not in {"default", "none"}:
            raise ValueError("indicator_profile must be one of: default, none")
        self._sanity_counters = sanity_counters
        self._audit = audit_manager
        state_enabled, state_profile = _state_feature_options(config)
        self._state_context_enabled = bool(state_enabled)
        self._state_layer = OnlineStateFeatureLayer(enabled=state_enabled, profile=state_profile)
        outputs_cfg = config.get("outputs") if isinstance(config.get("outputs"), dict) else {}
        self._equity_flush_every = _positive_int_option(
            config.get("equity_flush_every", outputs_cfg.get("equity_flush_every", 5000)),
            5000,
        )
        self._equity_rows_since_flush = 0
        self._decision_logging_profile, self._decision_negative_sample_every = _decision_logging_options(config)
        self._decision_sequence = 0
        self._decision_summary: dict[str, Any] = {
            "schema_version": 1,
            "profile": self._decision_logging_profile,
            "negative_sample_every": self._decision_negative_sample_every,
            "written": 0,
            "skipped": 0,
            "by_reason": {},
        }
        data_cfg = config.get("data") if isinstance(config.get("data"), dict) else {}
        analysis_start = data_cfg.get("analysis_start_ts") if isinstance(data_cfg, dict) else None
        self._analysis_start_ts = pd.Timestamp(analysis_start).tz_convert("UTC") if analysis_start else None
        research_cfg = config.get("research") if isinstance(config.get("research"), dict) else {}
        self._research_metadata = {
            "identity_research_tier": research_cfg.get("research_tier"),
            "identity_research_mode": research_cfg.get("research_mode", "portfolio_backtest"),
            "identity_evidence_type": research_cfg.get("evidence_type", "portfolio_outcome"),
            "portfolio_constraints_applied": research_cfg.get("portfolio_constraints_applied", True),
            "capital_path_valid": research_cfg.get("capital_path_valid", True),
            "deployability_evidence": research_cfg.get("deployability_evidence", True),
            "signal_episode_evidence": research_cfg.get("signal_episode_evidence", False),
        }

    def _is_warmup_ts(self, ts: Any) -> bool:
        if self._analysis_start_ts is None:
            return False
        current = pd.Timestamp(ts)
        if current.tzinfo is None:
            current = current.tz_localize("UTC")
        else:
            current = current.tz_convert("UTC")
        return current < self._analysis_start_ts

    def _sync_datafeed_required_symbols(self, open_orders: list[Order]) -> None:
        setter = getattr(self._datafeed, "set_required_symbols", None)
        if not callable(setter):
            return
        required = {
            order.symbol
            for order in open_orders
            if order.state not in {OrderState.FILLED, OrderState.CANCELLED, OrderState.REJECTED}
        }
        for symbol, position in self._portfolio.position_book.all_positions().items():
            if position.state in {PositionState.OPEN, PositionState.OPENING, PositionState.REDUCING}:
                required.add(symbol)
        setter(required)

    def _sync_datafeed_execution_state(self, open_orders: list[Order]) -> None:
        setter = getattr(self._datafeed, "set_execution_state", None)
        if not callable(setter):
            return
        has_open_orders = any(
            order.state not in {OrderState.FILLED, OrderState.CANCELLED, OrderState.REJECTED}
            for order in open_orders
        )
        has_positions = self._portfolio.position_book.open_positions_count() > 0
        setter(has_exposure=bool(has_open_orders or has_positions))

    def _positions_context(self) -> dict[str, dict[str, Any]]:
        positions_ctx: dict[str, dict[str, Any]] = {}
        for symbol, position in self._portfolio.position_book.all_positions().items():
            qty = float(position.qty)
            if position.side is None or qty == 0.0:
                side: str | None = None
                entry_price: float | None = None
                notional = 0.0
            else:
                side = position.side.value
                entry_price = float(position.avg_entry_price)
                notional = abs(qty) * entry_price
            positions_ctx[symbol] = {
                "side": side,
                "qty": qty,
                "entry_price": entry_price,
                "notional": float(notional),
            }
        return positions_ctx

    def _ctx_with_positions(self, ctx: Mapping[str, Any]) -> Mapping[str, Any]:
        if isinstance(ctx, dict):
            next_ctx = dict(ctx)
        else:
            next_ctx = dict(ctx.items())
        next_ctx["positions"] = self._positions_context()
        return next_ctx

    def _handle_fills(self, fills: list[Any]) -> None:
        for fill in fills:
            fill_qty = float(fill.qty)
            if fill_qty <= 0:
                raise ValueError(
                    f"BacktestEngine._handle_fills: fill.qty must be > 0 (symbol={fill.symbol}, qty={fill_qty})"
                )

            position_before = self._portfolio.position_book.get(fill.symbol)
            signed_position_before = self._signed_position_qty(position_before)
            signed_fill_delta = fill_qty if fill.side == Side.BUY else -fill_qty
            metadata = fill.metadata if isinstance(fill.metadata, dict) else {}
            if bool(metadata.get("close_only") or metadata.get("reduce_only")):
                if signed_position_before == 0.0:
                    raise ValueError(
                        "BacktestEngine._handle_fills: close/reduce fill while flat "
                        f"(symbol={fill.symbol}, side={fill.side.name}, qty={fill_qty})"
                    )
                if signed_position_before * signed_fill_delta >= 0:
                    raise ValueError(
                        "BacktestEngine._handle_fills: close/reduce fill must oppose current exposure "
                        f"(symbol={fill.symbol}, pos_before={signed_position_before}, delta={signed_fill_delta})"
                    )

            self._fills_writer.write(
                {
                    "ts": fill.ts,
                    "symbol": fill.symbol,
                    "order_id": fill.order_id,
                    "side": fill.side,
                    "qty": fill.qty,
                    "price": fill.price,
                    "fee": fill.fee,
                    "slippage": fill.slippage,
                    "metadata": fill.metadata,
                }
            )
            if self._sanity_counters is not None:
                self._sanity_counters.fills += 1
                if bool((fill.metadata or {}).get("forced_liquidation")):
                    self._sanity_counters.forced_liquidations += 1

        trades_closed = self._portfolio.apply_fills(fills)
        for trade in trades_closed:
            self._trades_writer.write_trade(trade)
            if self._sanity_counters is not None:
                self._sanity_counters.closed_trades += 1

    def _drop_stale_close_reduce_orders(self, open_orders: list[Order]) -> list[Order]:
        valid_orders: list[Order] = []
        for order in open_orders:
            metadata = order.metadata if isinstance(order.metadata, dict) else {}
            is_close_reduce = bool(metadata.get("close_only") or metadata.get("reduce_only"))
            if not is_close_reduce:
                valid_orders.append(order)
                continue

            signed_position_qty = self._signed_position_qty(self._portfolio.position_book.get(order.symbol))
            if signed_position_qty == 0.0:
                continue

            signed_order_qty = float(order.qty) if order.side == Side.BUY else -float(order.qty)
            if signed_position_qty * signed_order_qty >= 0:
                continue

            valid_orders.append(order)
        return valid_orders


    def _assert_post_fill_margin_invariants(self, fills: list[Any]) -> None:
        if self._risk.allows_may_liquidate():
            return
        for fill in fills:
            metadata = fill.metadata if isinstance(fill.metadata, dict) else {}
            if bool(metadata.get("close_only")):
                continue
            free_margin_post = float(metadata.get("free_margin_post", 0.0))
            if free_margin_post < 0:
                raise RuntimeError(
                    "strict margin invariant violated after non-close fill: "
                    f"symbol={fill.symbol} ts={fill.ts.isoformat()} "
                    f"equity={metadata.get('equity_used')} "
                    f"mark_price={metadata.get('mark_price_used_for_margin')} "
                    f"im={metadata.get('margin_required')} "
                    f"mm={metadata.get('maintenance_required')} "
                    f"fee_buffer={metadata.get('margin_fee_buffer')} "
                    f"slippage_buffer={metadata.get('margin_slippage_buffer')} "
                    f"adverse_buffer={metadata.get('margin_adverse_move_buffer')} "
                    f"free_margin_post={free_margin_post}"
                )

    @staticmethod
    def _signed_position_qty(position: Any) -> float:
        qty = float(getattr(position, "qty", 0.0) or 0.0)
        side = getattr(position, "side", None)
        if side == Side.SELL:
            return -abs(qty)
        if side == Side.BUY:
            return abs(qty)
        return 0.0

    def _force_liquidate_open_positions(
        self,
        *,
        ts: Any,
        bars_by_symbol: dict[str, Any],
        writer: csv.writer,
        liquidation_reason: str,
    ) -> None:
        liquidation_orders: list[Order] = []
        is_end_of_run = liquidation_reason == FORCED_LIQUIDATION_END_OF_RUN
        for symbol, position in self._portfolio.position_book.all_positions().items():
            if position.side is None or abs(float(position.qty)) < QTY_EPSILON:
                continue
            signed_position_qty = float(position.qty) if position.side == Side.BUY else -float(position.qty)
            close_qty = -signed_position_qty
            close_side = resolve_order_side(close_qty)
            liquidation_orders.append(
                Order(
                    id=self._next_order_id(),
                    ts_submitted=ts,
                    symbol=symbol,
                    side=close_side,
                    qty=abs(close_qty),
                    order_type=OrderType.MARKET,
                    limit_price=None,
                    state=OrderState.NEW,
                    metadata={
                        "reason": "end_of_run_flatten" if is_end_of_run else "forced_liquidation",
                        "close_only": True,
                        "forced_liquidation": not is_end_of_run,
                        "exit_reason": "end_of_run_flatten" if is_end_of_run else "forced_liquidation",
                        "liquidation_reason": liquidation_reason,
                        "delay_remaining": 0,
                    },
                )
            )

        if not liquidation_orders:
            return

        _, fills = self._execution.process(ts=ts, bars_by_symbol=bars_by_symbol, open_orders=liquidation_orders)

        self._handle_fills(fills)

        self._portfolio.mark_to_market(bars_by_symbol)
        writer.writerow(
            [
                ts.isoformat(),
                self._portfolio.cash,
                self._portfolio.equity,
                self._portfolio.realized_pnl,
                self._portfolio.unrealized_pnl,
                self._portfolio.used_margin,
                self._portfolio.free_margin,
            ]
        )

    def _build_indicator_set(self) -> dict[str, Indicator]:
        if self._indicator_profile == "none":
            return {}
        return {
            "ema_20": EMA(20),
            "ema_50": EMA(50),
            "atr_14": ATR(14),
            "vwap": VWAP(),
        }

    def _ensure_symbol_indicators(self, symbol: str) -> dict[str, Indicator]:
        if symbol not in self._indicators:
            self._indicators[symbol] = self._build_indicator_set()
        return self._indicators[symbol]

    def _next_order_id(self) -> str:
        self._order_counter += 1
        return f"order_{self._order_counter}"


    def _emit_decision_record(self, record: dict[str, Any]) -> None:
        self._decision_sequence += 1
        order = record.get("order")
        if order is not None:
            order_qty = float(record.get("order_qty", 0.0))
            validate_order_side_consistency(
                side=order.side,
                qty=float(order.qty),
                signed_qty=order_qty,
                where="BacktestEngine._emit_decision_record",
            )
            signal = record.get("signal")
            if signal is not None and getattr(signal, "side", None) != order.side:
                record = dict(record)
                record["signal"] = replace(signal, side=order.side)
            if self._audit is not None and self._audit.enabled:
                self._audit.record_event(
                    "order_normalization_check",
                    {
                        "ts": str(record.get("ts")),
                        "symbol": record.get("symbol"),
                        "approved": bool(record.get("approved")),
                        "order_side": order.side.name,
                        "order_qty": order_qty,
                    },
                    violation=False,
                )
        elif self._audit is not None and self._audit.enabled:
            self._audit.record_event(
                "order_normalization_check",
                {
                    "ts": str(record.get("ts")),
                    "symbol": record.get("symbol"),
                    "approved": bool(record.get("approved")),
                    "order_side": None,
                    "order_qty": None,
                },
                violation=False,
            )

        if not self._should_write_decision_record(record):
            reason = str(record.get("reason") or "unknown")
            self._decision_summary["skipped"] += 1
            by_reason = self._decision_summary["by_reason"]
            by_reason[reason] = int(by_reason.get(reason, 0)) + 1
            return

        self._decisions_writer.write(record)
        self._decision_summary["written"] += 1

    def _should_write_decision_record(self, record: dict[str, Any]) -> bool:
        profile = self._decision_logging_profile
        if profile == "full":
            return True
        signal = record.get("signal")
        signal_metadata = getattr(signal, "metadata", None)
        state_log_only = bool(signal_metadata.get("state_log_only")) if isinstance(signal_metadata, dict) else False
        if profile == "parity_debug":
            if bool(record.get("approved")) or record.get("order") is not None:
                return True
            if state_log_only:
                if self._decision_negative_sample_every > 0:
                    return self._decision_sequence % self._decision_negative_sample_every == 0
                return False
            if self._decision_negative_sample_every > 0:
                return self._decision_sequence % self._decision_negative_sample_every == 0
            return True

        if bool(record.get("approved")) or record.get("order") is not None:
            return True
        if state_log_only:
            if self._decision_negative_sample_every > 0:
                return self._decision_sequence % self._decision_negative_sample_every == 0
            return False
        reason = str(record.get("reason") or "")
        # Keep rejected explicit signals: they are evidence for risk gates,
        # strategy blockers, admission failures, and later diagnosis.
        if reason.startswith("risk_rejected") or "rejected" in reason:
            return True
        if self._decision_negative_sample_every > 0:
            return self._decision_sequence % self._decision_negative_sample_every == 0
        return False

    @staticmethod
    def _is_state_log_only_signal(signal: Any) -> bool:
        metadata = getattr(signal, "metadata", None)
        return bool(metadata.get("state_log_only")) if isinstance(metadata, dict) else False

    def _skip_state_log_only_signal_fast(self, signal: Any) -> bool:
        if self._decision_logging_profile != "research_sparse":
            return False
        if not self._is_state_log_only_signal(signal):
            return False
        if self._decision_negative_sample_every > 0:
            self._decision_sequence += 1
            if self._decision_sequence % self._decision_negative_sample_every == 0:
                self._decisions_writer.write(
                    {
                        "ts": getattr(signal, "ts", None),
                        "symbol": getattr(signal, "symbol", None),
                        "signal": signal,
                        "approved": False,
                        "reason": "state_log_only_sampled",
                    }
                )
                self._decision_summary["written"] += 1
                return True
        self._decision_summary["skipped"] += 1
        by_reason = self._decision_summary["by_reason"]
        by_reason["state_log_only_fast_skip"] = int(by_reason.get("state_log_only_fast_skip", 0)) + 1
        return True

    def _write_decision_logging_summary(self) -> None:
        path = self._decisions_writer.path.parent / "decision_logging_summary.json"
        import json

        payload = dict(self._decision_summary)
        payload["total_seen"] = int(payload.get("written", 0)) + int(payload.get("skipped", 0))
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        tmp.replace(path)

    def _write_datafeed_fast_path_artifacts(self) -> None:
        stats = getattr(self._datafeed, "candidate_event_stats", None)
        if not callable(stats):
            return
        payload = stats()
        if not isinstance(payload, dict):
            return
        import json

        path = self._decisions_writer.path.parent / "candidate_event_summary.json"
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        tmp.replace(path)

    def _write_equity_header(self, writer: csv.writer) -> None:
        writer.writerow(
            [
                "ts",
                "cash",
                "equity",
                "realized_pnl",
                "unrealized_pnl",
                "used_margin",
                "free_margin",
            ]
        )

    def _drain_datafeed_skipped_flat_timestamps(self) -> list[pd.Timestamp]:
        drain = getattr(self._datafeed, "drain_skipped_flat_timestamps", None)
        if not callable(drain):
            return []
        values = drain()
        if not isinstance(values, list):
            return []
        return [pd.Timestamp(value) for value in values]

    def _write_flat_equity_rows(self, writer: csv.writer, timestamps: list[pd.Timestamp]) -> None:
        if not timestamps:
            return
        for ts in timestamps:
            writer.writerow(
                [
                    ts.isoformat(),
                    self._portfolio.cash,
                    self._portfolio.equity,
                    self._portfolio.realized_pnl,
                    self._portfolio.unrealized_pnl,
                    self._portfolio.used_margin,
                    self._portfolio.free_margin,
                ]
            )
            self._equity_rows_since_flush += 1

    def run(self) -> None:
        """
        Loop:
        1) bars = feed.next()
        2) universe.update(...) for each bar
        3) build bars_by_symbol dict for this ts
        4) update indicators and strategy.on_bars(ts, bars_by_symbol, tradeable_set, ctx)
        5) for each Signal: risk.signal_to_order_intent(...)
        6) turn OrderIntent into Order and submit to open_orders list
        7) execution.process(ts, bars_by_symbol, open_orders) -> (open_orders, fills)
        8) portfolio.apply_fills(fills) -> trades_closed
        9) portfolio.mark_to_market(bars_by_symbol)
        10) log decisions, fills, trades, and equity per timestamp
        """
        open_orders: list[Order] = []
        self._sync_datafeed_execution_state(open_orders)
        self._equity_path.parent.mkdir(parents=True, exist_ok=True)
        with self._equity_path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            if self._equity_path.stat().st_size == 0:
                self._write_equity_header(writer)

            last_ts = None
            last_bars_by_symbol: dict[str, Any] = {}
            while True:
                bars = self._datafeed.next()
                if bars is None:
                    self._write_flat_equity_rows(writer, self._drain_datafeed_skipped_flat_timestamps())
                    break

                self._write_flat_equity_rows(writer, self._drain_datafeed_skipped_flat_timestamps())

                if isinstance(bars, dict):
                    bars_by_symbol = bars
                    bars_list = list(bars.values())
                else:
                    bars_list = list(bars)
                    bars_by_symbol = {bar.symbol: bar for bar in bars_list}

                if not bars_list:
                    continue

                ts = bars_list[0].ts
                last_ts = ts
                last_bars_by_symbol = bars_by_symbol

                for bar in bars_list:
                    self._universe.update(bar)
                    indicators = self._ensure_symbol_indicators(bar.symbol)
                    for indicator in indicators.values():
                        indicator.update(bar)
                    if self._state_context_enabled:
                        self._state_layer.update(
                            symbol=bar.symbol,
                            ts=bar.ts,
                            open_px=bar.open,
                            high=bar.high,
                            low=bar.low,
                            close=bar.close,
                            volume=bar.volume,
                            extra=bar.extra,
                        )

                tradeable = self._universe.tradeable_at(ts)
                in_warmup = self._is_warmup_ts(ts)
                strategy_tradeable = set() if in_warmup else tradeable
                indicators_snapshot: dict[str, dict[str, tuple[float | None, bool]]] = {}
                if self._indicator_profile != "none":
                    for symbol in bars_by_symbol:
                        symbol_indicators = self._indicators.get(symbol, {})
                        indicators_snapshot[symbol] = {
                            name: (indicator.value, indicator.is_ready)
                            for name, indicator in symbol_indicators.items()
                        }
                ctx: Mapping[str, Any] = {
                    "indicators": indicators_snapshot,
                    "tradeable": strategy_tradeable,
                    "state": (
                        {symbol: self._state_layer.snapshot(symbol=symbol) for symbol in bars_by_symbol}
                        if self._state_context_enabled
                        else {}
                    ),
                    "warmup": in_warmup,
                    "analysis_start_ts": self._analysis_start_ts,
                }
                signals = self._strategy.on_bars(ts, bars_by_symbol, strategy_tradeable, self._ctx_with_positions(ctx))
                if self._audit is not None and self._audit.enabled:
                    self._audit.mark_layer_executed("alignment_audit")
                    for violation in inspect_alignment(ts=ts, bars_by_symbol=bars_by_symbol):
                        self._audit.record_event("alignment_audit", violation, violation=True)
                    self._audit.mark_layer_executed("signal_audit")
                    for symbol, indicators in indicators_snapshot.items():
                        for violation in inspect_signal_context(symbol=symbol, ts=ts, indicators=indicators):
                            self._audit.record_event("signal_audit", violation, violation=True)
                if self._sanity_counters is not None:
                    self._sanity_counters.signals_emitted += len(signals)

                if in_warmup:
                    self._sync_datafeed_required_symbols(open_orders)
                    self._sync_datafeed_execution_state(open_orders)
                    continue

                reserved_open_positions = self._portfolio.position_book.open_positions_count()
                reserved_free_margin = self._portfolio.free_margin
                reserved_gross_notional = self._portfolio.gross_notional()

                for signal in signals:
                    signal = self._enrich_signal_metadata(signal=signal, ts=ts)
                    if self._skip_state_log_only_signal_fast(signal):
                        continue
                    bar = bars_by_symbol.get(signal.symbol)
                    if bar is None:
                        decision_reason = "risk_rejected:no_bar"
                        self._emit_decision_record(
                            {
                                "ts": ts,
                                "symbol": signal.symbol,
                                "signal": signal,
                                "approved": False,
                                "reason": decision_reason,
                            }
                        )
                        if self._sanity_counters is not None:
                            self._sanity_counters.record_decision(approved=False, reason=decision_reason)
                        continue

                    position = self._portfolio.position_book.get(signal.symbol)
                    current_qty = self._signed_position_qty(position)
                    order_intent, decision_reason = self._risk.signal_to_order_intent(
                        ts=ts,
                        signal=signal,
                        bar=bar,
                        equity=self._portfolio.equity,
                        free_margin=reserved_free_margin,
                        open_positions=reserved_open_positions,
                        max_leverage=self._portfolio.max_leverage,
                        current_qty=current_qty,
                        current_gross_notional=reserved_gross_notional,
                    )

                    if order_intent is None:
                        self._emit_decision_record(
                            {
                                "ts": ts,
                                "symbol": signal.symbol,
                                "signal": signal,
                                "approved": False,
                                "reason": decision_reason,
                            }
                        )
                        if self._sanity_counters is not None:
                            self._sanity_counters.record_decision(approved=False, reason=decision_reason)
                        continue

                    order_side = resolve_order_side(order_intent.qty)
                    order = Order(
                        id=self._next_order_id(),
                        ts_submitted=ts,
                        symbol=order_intent.symbol,
                        side=order_side,
                        qty=abs(order_intent.qty),
                        order_type=order_intent.order_type,
                        limit_price=order_intent.limit_price,
                        state=OrderState.NEW,
                        metadata=dict(order_intent.metadata),
                    )
                    open_orders.append(order)
                    if self._audit is not None and self._audit.enabled:
                        intent, violations = inspect_order(ts=ts, order=order)
                        self._audit.record_event("order_audit", {"order": intent}, violation=False)
                        for violation in violations:
                            self._audit.record_event("order_audit", violation, violation=True)

                    total_required = float(order_intent.metadata.get("total_required", 0.0))
                    if total_required <= 0:
                        notional_est = float(order_intent.metadata.get("notional_est", abs(order_intent.qty) * bar.close))
                        fee_buffer = float(order_intent.metadata.get("margin_fee_buffer", 0.0))
                        adverse_move_buffer = float(order_intent.metadata.get("margin_adverse_move_buffer", 0.0))
                        slippage_buffer = float(order_intent.metadata.get("margin_slippage_buffer", 0.0))
                        total_required = self._risk.estimate_required_margin(
                            notional=notional_est,
                            max_leverage=self._portfolio.max_leverage,
                            fee_buffer=fee_buffer + adverse_move_buffer,
                            slippage_buffer=slippage_buffer,
                        )
                    reserved_free_margin = max(reserved_free_margin - total_required, 0.0)
                    if current_qty == 0:
                        reserved_open_positions += 1
                        try:
                            reserved_gross_notional += abs(float(order_intent.metadata.get("notional_est", 0.0) or 0.0))
                        except (TypeError, ValueError):
                            reserved_gross_notional = self._portfolio.gross_notional()

                    self._emit_decision_record(
                        {
                            "ts": ts,
                            "symbol": signal.symbol,
                            "signal": signal,
                            "approved": True,
                            "reason": decision_reason,
                            "order_qty": order_intent.qty,
                            "notional_est": order_intent.metadata.get("notional_est"),
                            "order": order,
                        }
                    )
                    if self._sanity_counters is not None:
                        self._sanity_counters.record_decision(approved=True, reason=decision_reason)

                open_orders = self._drop_stale_close_reduce_orders(open_orders)
                open_orders, fills = self._execution.process(
                    ts=ts,
                    bars_by_symbol=bars_by_symbol,
                    open_orders=open_orders,
                )
                open_orders = [
                    order
                    for order in open_orders
                    if order.state
                    not in {
                        OrderState.FILLED,
                        OrderState.CANCELLED,
                        OrderState.REJECTED,
                    }
                ]

                self._handle_fills(fills)
                if self._audit is not None and self._audit.enabled:
                    self._audit.mark_layer_executed("fill_audit")
                    for fill in fills:
                        bar = bars_by_symbol.get(fill.symbol)
                        for violation in inspect_fill(ts=ts, fill=fill, bar=bar):
                            self._audit.record_event("fill_audit", violation, violation=True)
                self._assert_post_fill_margin_invariants(fills)

                self._portfolio.mark_to_market(bars_by_symbol)
                forced_liquidated = False
                if self._portfolio.free_margin < 0 and self._risk.allows_may_liquidate():
                    self._force_liquidate_open_positions(
                        ts=ts,
                        bars_by_symbol=bars_by_symbol,
                        writer=writer,
                        liquidation_reason=FORCED_LIQUIDATION_MARGIN,
                    )
                    forced_liquidated = True

                if forced_liquidated:
                    self._sync_datafeed_required_symbols(open_orders)
                    self._sync_datafeed_execution_state(open_orders)
                    handle.flush()
                    self._equity_rows_since_flush = 0
                    continue

                writer.writerow(
                    [
                        ts.isoformat(),
                        self._portfolio.cash,
                        self._portfolio.equity,
                        self._portfolio.realized_pnl,
                        self._portfolio.unrealized_pnl,
                        self._portfolio.used_margin,
                        self._portfolio.free_margin,
                    ]
                )
                self._equity_rows_since_flush += 1
                if self._equity_rows_since_flush >= self._equity_flush_every:
                    handle.flush()
                    self._equity_rows_since_flush = 0
                self._sync_datafeed_required_symbols(open_orders)
                self._sync_datafeed_execution_state(open_orders)

                if self._audit is not None and self._audit.enabled:
                    self._audit.mark_layer_executed("position_audit")
                    for symbol, position in self._portfolio.position_book.all_positions().items():
                        for violation in inspect_position(symbol, position):
                            self._audit.record_event("position_audit", violation, violation=True)
                    self._audit.mark_layer_executed("portfolio_audit")
                    for violation in inspect_portfolio(
                        cash=self._portfolio.cash,
                        equity=self._portfolio.equity,
                        used_margin=self._portfolio.used_margin,
                    ):
                        self._audit.record_event("portfolio_audit", violation, violation=True)

            if last_ts is not None:
                self._force_liquidate_open_positions(
                    ts=last_ts,
                    bars_by_symbol=last_bars_by_symbol,
                    writer=writer,
                    liquidation_reason=FORCED_LIQUIDATION_END_OF_RUN,
                )
                handle.flush()
                self._equity_rows_since_flush = 0

        self._decisions_writer.close()
        self._fills_writer.close()
        self._trades_writer.close()
        self._write_decision_logging_summary()
        self._write_datafeed_fast_path_artifacts()

    def _enrich_signal_metadata(self, *, signal: Any, ts: Any) -> Any:
        metadata = dict(signal.metadata) if isinstance(signal.metadata, dict) else {}
        state_snapshot = self._state_layer.snapshot(symbol=signal.symbol)
        for key, value in state_snapshot.items():
            if key.startswith("entry_state_") and (key not in metadata or _is_missing_metadata_value(metadata.get(key))):
                metadata[key] = value
        metadata.setdefault("signal_ts", ts)
        for key, value in self._research_metadata.items():
            if value is not None:
                metadata.setdefault(key, value)
        if "decision_trace" not in metadata:
            metadata["decision_trace"] = {
                "reason_code": metadata.get("entry_reason", signal.signal_type if hasattr(signal, "signal_type") else None),
                "setup_class": metadata.get("setup_class"),
                "conditions_bool_map": {},
                "blockers_bool_map": {},
                "permission_layer_state": {},
                "score": None,
                "rank": None,
                "parameter_combination": metadata.get("parameter_set_id"),
                "gate_thresholds": {},
                "gate_values": {},
                "gate_margins": {},
                "most_binding_gate": None,
            }
        return replace(signal, metadata=metadata)
        if self._audit is not None:
            self._audit.write_summary()
