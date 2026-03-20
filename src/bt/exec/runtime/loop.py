from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import pandas as pd

from bt.core.enums import Side
from bt.exec.logging.heartbeats import heartbeat_record
from bt.exec.logging.schemas import DecisionArtifactRecord
from bt.exec.services.execution_router import ExecutionRouter, build_submitted_order_artifact
from bt.exec.services.portfolio_runner import PortfolioRunner
from bt.exec.services.risk_runner import RiskRunner
from bt.exec.services.strategy_runner import StrategyRunner, build_positions_context
from bt.exec.state.recovery import should_skip_bar


@dataclass
class RuntimeLoopState:
    client_order_seq: int = 0
    checkpoint_sequence: int = 0
    last_processed_bar_ts: pd.Timestamp | None = None


@dataclass
class RuntimeLoop:
    feed: Any
    strategy_runner: StrategyRunner
    risk_runner: RiskRunner
    portfolio_runner: PortfolioRunner
    execution_router: ExecutionRouter
    artifacts: Any
    scheduler: Any
    bar_gate: Any
    health: Any
    mode: str
    state: RuntimeLoopState
    on_bar_complete: Callable[[pd.Timestamp, RuntimeLoopState], None] | None = None

    def run(self) -> RuntimeLoopState:
        while True:
            bars = self.feed.next()
            if bars is None:
                break
            bars_by_symbol = bars if isinstance(bars, dict) else {bar.symbol: bar for bar in list(bars)}
            if not bars_by_symbol:
                continue
            ts = next(iter(bars_by_symbol.values())).ts
            if should_skip_bar(checkpoint_bar_ts=self.state.last_processed_bar_ts, bar_ts=ts):
                continue
            self.health.observe_bar(ts)

            for hb in self.scheduler.on_timestamp(ts):
                snap = self.health.snapshot(ts)
                self.artifacts.write_heartbeat(heartbeat_record(hb, healthy=snap.healthy, stale_seconds=snap.stale_seconds))

            if not self.bar_gate.is_eligible(ts=ts):
                continue

            tradeable = set(bars_by_symbol)
            ctx = {"tradeable": tradeable, "indicators": {}, "positions": build_positions_context(self.portfolio_runner.portfolio)}
            signals = self.strategy_runner.run(ts=ts, bars_by_symbol=bars_by_symbol, tradeable=tradeable, ctx=ctx)

            for signal in signals:
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
                self.state.client_order_seq += 1
                submit_result = self.execution_router.submit_order(order_seq=self.state.client_order_seq, intent=decision.order_intent, ts=ts)
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
