"""Backtest engine main event loop."""
from __future__ import annotations

from pathlib import Path
from dataclasses import replace
import csv
from typing import Any, Mapping

from bt.core.enums import OrderState, OrderType, Side
from bt.core.reason_codes import FORCED_LIQUIDATION_END_OF_RUN, FORCED_LIQUIDATION_MARGIN
from bt.core.types import Order
from bt.data.feed import HistoricalDataFeed
from bt.execution.execution_model import ExecutionModel
from bt.indicators.base import Indicator
from bt.indicators.atr import ATR
from bt.indicators.ema import EMA
from bt.indicators.vwap import VWAP
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
        self._sanity_counters = sanity_counters
        self._audit = audit_manager

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

        self._decisions_writer.write(record)

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
                    break

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

                tradeable = self._universe.tradeable_at(ts)
                indicators_snapshot: dict[str, dict[str, tuple[float | None, bool]]] = {}
                for symbol in bars_by_symbol:
                    symbol_indicators = self._indicators.get(symbol, {})
                    indicators_snapshot[symbol] = {
                        name: (indicator.value, indicator.is_ready)
                        for name, indicator in symbol_indicators.items()
                    }
                ctx: Mapping[str, Any] = {
                    "indicators": indicators_snapshot,
                    "tradeable": tradeable,
                }
                signals = self._strategy.on_bars(ts, bars_by_symbol, tradeable, self._ctx_with_positions(ctx))
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

                reserved_open_positions = self._portfolio.position_book.open_positions_count()
                reserved_free_margin = self._portfolio.free_margin

                for signal in signals:
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
                    handle.flush()
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
                handle.flush()

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

        self._decisions_writer.close()
        self._fills_writer.close()
        self._trades_writer.close()
        if self._audit is not None:
            self._audit.write_summary()
