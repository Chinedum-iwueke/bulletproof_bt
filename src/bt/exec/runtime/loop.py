from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol

import pandas as pd

from bt.core.enums import Side
from bt.exec.logging.heartbeats import heartbeat_record
from bt.exec.logging.schemas import DecisionArtifactRecord
from bt.exec.reconcile import ReconciliationResult
from bt.exec.services.execution_router import ExecutionRouter, build_submitted_order_artifact
from bt.exec.services.kill_switch import KillSwitch
from bt.exec.services.live_controls import CanaryGuard
from bt.exec.services.portfolio_runner import PortfolioRunner
from bt.exec.services.risk_runner import RiskRunner
from bt.exec.services.strategy_runner import StrategyRunner, build_positions_context
from bt.exec.state.recovery import should_skip_bar


@dataclass(frozen=True)
class ReconciliationConfig:
    enabled: bool
    interval_seconds: int


@dataclass
class RuntimeLoopState:
    client_order_seq: int = 0
    checkpoint_sequence: int = 0
    last_processed_bar_ts: pd.Timestamp | None = None
    frozen: bool = False


class ReconcileFn(Protocol):
    def __call__(self, ts: pd.Timestamp) -> ReconciliationResult | dict[str, object] | None: ...


@dataclass
class RuntimeLoop:
    feed: object
    strategy_runner: StrategyRunner
    risk_runner: RiskRunner
    portfolio_runner: PortfolioRunner
    execution_router: ExecutionRouter
    artifacts: object
    scheduler: object
    bar_gate: object
    health: object
    mode: str
    state: RuntimeLoopState
    reconciliation: ReconciliationConfig
    reconcile_fn: ReconcileFn | None = None
    on_bar_complete: Callable[[pd.Timestamp, RuntimeLoopState], None] | None = None
    kill_switch: KillSwitch | None = None
    canary_guard: CanaryGuard | None = None

    def run(self) -> RuntimeLoopState:
        last_reconcile_ts: pd.Timestamp | None = None
        while True:
            bars = self.feed.next()  # type: ignore[attr-defined]
            if bars is None:
                break
            bars_by_symbol = bars if isinstance(bars, dict) else {bar.symbol: bar for bar in list(bars)}
            if not bars_by_symbol:
                continue
            ts = next(iter(bars_by_symbol.values())).ts
            if should_skip_bar(checkpoint_bar_ts=self.state.last_processed_bar_ts, bar_ts=ts):
                continue
            self.health.observe_bar(ts)

            for hb in self.scheduler.on_timestamp(ts):  # type: ignore[attr-defined]
                snap = self.health.snapshot(ts)
                self.artifacts.write_heartbeat(heartbeat_record(hb, healthy=snap.healthy, stale_seconds=snap.stale_seconds))

            if self.reconciliation.enabled and self.reconcile_fn is not None:
                should_run = last_reconcile_ts is None or (ts - last_reconcile_ts).total_seconds() >= float(self.reconciliation.interval_seconds)
                if should_run:
                    result = self.reconcile_fn(ts)
                    if result is not None:
                        self.artifacts.write_reconciliation(result)
                        action = ""
                        if isinstance(result, dict):
                            action = str(((result.get("decision") or {}) if isinstance(result.get("decision"), dict) else {}).get("action", ""))
                        else:
                            action = result.decision.action.value
                        if action == "freeze":
                            self.state.frozen = True
                            if self.kill_switch is not None:
                                self.kill_switch.freeze(reason="reconciliation_freeze", ts=ts)
                    last_reconcile_ts = ts

            for fill_artifact in self.execution_router.process_broker_events():
                self.artifacts.write_fill(fill_artifact)

            if not self.bar_gate.is_eligible(ts=ts):  # type: ignore[attr-defined]
                continue

            tradeable = set(bars_by_symbol)
            ctx = {"tradeable": tradeable, "indicators": {}, "positions": build_positions_context(self.portfolio_runner.portfolio)}
            signals = self.strategy_runner.run(ts=ts, bars_by_symbol=bars_by_symbol, tradeable=tradeable, ctx=ctx)

            for signal in signals:
                frozen = self.state.frozen or (self.kill_switch.state().freeze_new_orders if self.kill_switch is not None else False)
                if frozen:
                    break
                bar = bars_by_symbol.get(signal.symbol)
                if bar is None:
                    continue
                position = self.portfolio_runner.portfolio.position_book.get(signal.symbol)
                current_qty = float(position.qty) if position.side == Side.BUY else (-float(position.qty) if position.side == Side.SELL else 0.0)
                decision = self.risk_runner.evaluate(
                    ts=ts,
                    signal=signal,
                    bar=bar,
                    equity=self.portfolio_runner.portfolio.equity,
                    free_margin=self.portfolio_runner.portfolio.free_margin,
                    open_positions=self.portfolio_runner.portfolio.position_book.open_positions_count(),
                    max_leverage=self.portfolio_runner.portfolio.max_leverage,
                    current_qty=current_qty,
                )
                self.artifacts.write_decision(
                    DecisionArtifactRecord(ts=ts, symbol=signal.symbol, signal=signal, approved=decision.approved, reason=decision.reason)
                )
                if not decision.approved or decision.order_intent is None or self.mode == "shadow":
                    continue

                if self.canary_guard is not None:
                    canary_error = self.canary_guard.validate_intent(
                        intent=decision.order_intent,
                        open_orders=self.execution_router.current_open_orders(),
                        positions=list(self.portfolio_runner.portfolio.position_book.all_positions().values()),
                        current_price=float(getattr(bar, "close", 0.0) or 0.0),
                    )
                    if canary_error is not None:
                        if self.kill_switch is not None:
                            self.kill_switch.freeze(reason=f"canary_guard:{canary_error}", ts=ts)
                        self.state.frozen = True
                        break

                self.state.client_order_seq += 1
                try:
                    submit_result = self.execution_router.submit_order(order_seq=self.state.client_order_seq, intent=decision.order_intent, ts=ts)
                    if self.canary_guard is not None:
                        self.canary_guard.record_submission()
                    if self.kill_switch is not None:
                        self.kill_switch.clear_transport_errors()
                except Exception:
                    if self.kill_switch is not None:
                        self.kill_switch.record_transport_error(ts=ts, max_consecutive_transport_errors=1)
                    self.state.frozen = True
                    raise

                self.artifacts.write_order(
                    build_submitted_order_artifact(
                        ts=ts,
                        symbol=decision.order_intent.symbol,
                        side=decision.order_intent.side,
                        qty=abs(float(decision.order_intent.qty)),
                        result=submit_result,
                    )
                )

            for fill_artifact in self.execution_router.process_bar(ts=ts, bars_by_symbol=bars_by_symbol):
                self.artifacts.write_fill(fill_artifact)

            self.portfolio_runner.mark_to_market(bars_by_symbol)
            self.state.last_processed_bar_ts = ts
            if self.on_bar_complete is not None:
                self.on_bar_complete(ts, self.state)
        return self.state
