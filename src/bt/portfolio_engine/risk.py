"""Portfolio-level risk coordination and conflict auditing."""
from __future__ import annotations

from dataclasses import asdict
from typing import Any

from bt.portfolio_engine.models import (
    PortfolioConfig,
    PortfolioOrderIntent,
    PortfolioRunState,
    PortfolioSignal,
    StrategyId,
)


class PortfolioRiskCoordinator:
    """Auditable risk layer above individual strategy sleeves."""

    def __init__(self, config: PortfolioConfig) -> None:
        self.config = config
        self.risk_events: list[dict[str, Any]] = []
        self.conflict_events: list[dict[str, Any]] = []

    def _record_risk(self, **payload: Any) -> None:
        self.risk_events.append({"portfolio_id": self.config.portfolio_id, **payload})

    def _record_conflict(self, **payload: Any) -> None:
        self.conflict_events.append({"portfolio_id": self.config.portfolio_id, **payload})

    def check_state(self, state: PortfolioRunState) -> bool:
        policy = self.config.allocation_policy
        if state.kill_switch_triggered:
            self._record_risk(event="portfolio_risk_limit_hit", reason=state.kill_switch_reason)
            return False
        if state.high_watermark is None:
            state.high_watermark = state.equity
        else:
            state.high_watermark = max(float(state.high_watermark), float(state.equity))
        if policy.max_drawdown_pct is not None and state.high_watermark > 0:
            drawdown = (float(state.high_watermark) - float(state.equity)) / float(state.high_watermark)
            if drawdown >= float(policy.max_drawdown_pct):
                state.kill_switch_triggered = True
                state.kill_switch_reason = "max_drawdown_pct"
                self._record_risk(event="portfolio_kill_switch_triggered", drawdown=drawdown, limit=policy.max_drawdown_pct)
                return False
        gross_limit = self.config.starting_equity * float(policy.max_total_gross_exposure)
        if abs(state.gross_exposure) > gross_limit:
            self._record_risk(event="portfolio_risk_limit_hit", reason="max_total_gross_exposure", exposure=state.gross_exposure, limit=gross_limit)
            return False
        net_limit = self.config.starting_equity * float(policy.max_total_net_exposure)
        if abs(state.net_exposure) > net_limit:
            self._record_risk(event="portfolio_risk_limit_hit", reason="max_total_net_exposure", exposure=state.net_exposure, limit=net_limit)
            return False
        if policy.max_positions_portfolio is not None and state.active_positions >= int(policy.max_positions_portfolio):
            self._record_risk(event="portfolio_risk_limit_hit", reason="max_positions_portfolio", active_positions=state.active_positions)
            return False
        return True

    def check_strategy_state(self, state: PortfolioRunState, strategy_id: StrategyId) -> bool:
        policy = self.config.allocation_policy
        strategy_state = state.strategy_states[strategy_id]
        if strategy_state.disabled:
            self._record_risk(event="strategy_disabled", strategy_id=strategy_id, reason=strategy_state.disable_reason)
            return False
        max_loss = policy.max_strategy_daily_loss_pct
        if max_loss is not None and strategy_state.allocated_capital > 0:
            loss_pct = -min(strategy_state.realized_pnl + strategy_state.unrealized_pnl, 0.0) / strategy_state.allocated_capital
            if loss_pct >= float(max_loss):
                strategy_state.disabled = True
                strategy_state.disable_reason = "max_strategy_daily_loss_pct"
                self._record_risk(event="strategy_risk_limit_hit", strategy_id=strategy_id, loss_pct=loss_pct, limit=max_loss)
                return False
        if policy.max_positions_per_strategy is not None and strategy_state.active_positions >= int(policy.max_positions_per_strategy):
            self._record_risk(event="strategy_risk_limit_hit", strategy_id=strategy_id, reason="max_positions_per_strategy")
            return False
        return True

    def resolve_signal_conflicts(self, signals: list[PortfolioSignal]) -> list[PortfolioSignal]:
        policy = self.config.allocation_policy.conflict_policy
        by_symbol: dict[str, list[PortfolioSignal]] = {}
        for signal in signals:
            by_symbol.setdefault(signal.symbol, []).append(signal)

        resolved: list[PortfolioSignal] = []
        for symbol, symbol_signals in by_symbol.items():
            sides = {signal.side.lower() for signal in symbol_signals}
            if len(sides) <= 1 or policy == "allow_hedged":
                resolved.extend(symbol_signals)
                continue

            self._record_conflict(
                event="conflict_detected",
                symbol=symbol,
                policy=policy,
                signals=[asdict(signal) for signal in symbol_signals],
            )
            if policy == "block_conflict":
                self._record_conflict(event="conflict_blocked", symbol=symbol)
                continue
            if policy == "highest_expected_value":
                winner = max(symbol_signals, key=lambda signal: float(signal.expected_value or 0.0))
            else:
                winner = max(symbol_signals, key=lambda signal: float(signal.confidence))
            self._record_conflict(event="conflict_winner_selected", symbol=symbol, strategy_id=winner.strategy_id)
            resolved.append(winner)
        return resolved

    def approve_order(self, order: PortfolioOrderIntent, state: PortfolioRunState) -> PortfolioOrderIntent:
        if not self.check_state(state):
            return self._reject(order, state.kill_switch_reason or "portfolio_risk_limit_hit")
        if not self.check_strategy_state(state, order.strategy_id):
            strategy_state = state.strategy_states[order.strategy_id]
            return self._reject(order, strategy_state.disable_reason or "strategy_risk_limit_hit")

        policy = self.config.allocation_policy
        strategy_state = state.strategy_states[order.strategy_id]
        strategy_limit = strategy_state.allocated_capital * float(policy.max_strategy_weight)
        if abs(strategy_state.gross_exposure) + abs(order.notional) > strategy_limit + 1e-9:
            return self._reject(order, "max_strategy_weight")

        symbol_limit = self.config.starting_equity * float(policy.max_symbol_exposure)
        symbol_notional = abs(float(order.metadata.get("current_symbol_exposure", 0.0))) + abs(order.notional)
        if symbol_notional > symbol_limit + 1e-9:
            return self._reject(order, "max_symbol_exposure")

        gross_limit = self.config.starting_equity * float(policy.max_total_gross_exposure)
        if abs(state.gross_exposure) + abs(order.notional) > gross_limit + 1e-9:
            return self._reject(order, "max_total_gross_exposure")
        self._record_risk(event="portfolio_order_approved", strategy_id=order.strategy_id, symbol=order.symbol, notional=order.notional)
        return order

    def _reject(self, order: PortfolioOrderIntent, reason: str) -> PortfolioOrderIntent:
        rejected = PortfolioOrderIntent(
            portfolio_id=order.portfolio_id,
            strategy_id=order.strategy_id,
            hypothesis_id=order.hypothesis_id,
            ts=order.ts,
            symbol=order.symbol,
            side=order.side,
            qty=order.qty,
            notional=order.notional,
            approved=False,
            risk_rejection_reason=reason,
            metadata=dict(order.metadata),
        )
        self._record_risk(event="portfolio_order_rejected", strategy_id=order.strategy_id, symbol=order.symbol, reason=reason)
        return rejected

