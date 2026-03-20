from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bt.core.enums import Side
from bt.exec.adapters.base import BrokerOrderRequest
from bt.exec.adapters.simulated import SimulatedBrokerAdapter
from bt.exec.events.broker_events import BrokerOrderFilledEvent
from bt.exec.logging.heartbeats import heartbeat_record
from bt.exec.logging.orders import order_record
from bt.exec.services.portfolio_runner import PortfolioRunner
from bt.exec.services.risk_runner import RiskRunner
from bt.exec.services.strategy_runner import StrategyRunner, build_positions_context


@dataclass
class RuntimeLoop:
    feed: Any
    strategy_runner: StrategyRunner
    risk_runner: RiskRunner
    portfolio_runner: PortfolioRunner
    adapter: SimulatedBrokerAdapter
    artifacts: Any
    scheduler: Any
    bar_gate: Any
    health: Any
    mode: str

    def run(self) -> None:
        client_order_seq = 0
        while True:
            bars = self.feed.next()
            if bars is None:
                break
            bars_by_symbol = bars if isinstance(bars, dict) else {bar.symbol: bar for bar in list(bars)}
            if not bars_by_symbol:
                continue
            ts = next(iter(bars_by_symbol.values())).ts
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
                self.artifacts.write_decision({"ts": ts, "symbol": signal.symbol, "signal": signal, "approved": decision.approved, "reason": decision.reason})
                if not decision.approved or decision.order_intent is None or self.mode == "shadow":
                    continue
                client_order_seq += 1
                request = BrokerOrderRequest(
                    client_order_id=f"co-{client_order_seq}",
                    symbol=decision.order_intent.symbol,
                    side=decision.order_intent.side.value,
                    qty=abs(float(decision.order_intent.qty)),
                    order_type=decision.order_intent.order_type.value,
                    limit_price=decision.order_intent.limit_price,
                    metadata=dict(decision.order_intent.metadata),
                )
                req_id = self.adapter.submit_order(request)
                self.artifacts.write_order(order_record(ts=ts, event="submitted", payload={"request_id": req_id, "symbol": request.symbol, "side": request.side, "qty": request.qty}))

            if self.mode == "paper_simulated":
                self.adapter.process_bar(ts=ts, bars_by_symbol=bars_by_symbol)
                fills = []
                for evt in self.adapter.iter_events():
                    if isinstance(evt, BrokerOrderFilledEvent):
                        fills.append(evt.fill)
                        self.artifacts.write_fill({"ts": evt.fill.ts, "symbol": evt.fill.symbol, "order_id": evt.fill.order_id, "side": evt.fill.side, "qty": evt.fill.qty, "price": evt.fill.price, "fee": evt.fill.fee, "slippage": evt.fill.slippage, "metadata": evt.fill.metadata})
                if fills:
                    self.portfolio_runner.apply_fills(fills)
            self.portfolio_runner.mark_to_market(bars_by_symbol)
