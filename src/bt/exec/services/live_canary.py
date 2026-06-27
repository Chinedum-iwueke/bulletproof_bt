from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class CanaryRiskPolicy:
    allowed_symbols: frozenset[str]
    max_order_quantity: float
    max_order_notional_usd: float
    max_gross_notional_usd: float
    max_open_orders: int
    max_open_positions: int
    max_daily_loss_usd: float
    max_session_loss_usd: float
    require_demo_qualification: bool = True
    allow_reduce_only_when_frozen: bool = True


@dataclass(frozen=True)
class CanaryState:
    environment: Literal["demo", "live"]
    connector_healthy: bool
    connector_checked_age_seconds: float
    reconciliation_healthy: bool
    unresolved_critical_incidents: int
    demo_qualified: bool
    kill_switch_tested: bool
    open_orders: int
    open_positions: int
    gross_notional_usd: float
    daily_pnl_usd: float
    session_pnl_usd: float
    frozen: bool


@dataclass(frozen=True)
class CanaryOrderIntent:
    symbol: str
    quantity: float
    price: float
    reduce_only: bool = False


@dataclass(frozen=True)
class CanaryDecision:
    allowed: bool
    read_only: bool
    reason_codes: tuple[str, ...]
    effective_quantity: float


def evaluate_canary_order(
    *, policy: CanaryRiskPolicy, state: CanaryState, intent: CanaryOrderIntent
) -> CanaryDecision:
    reasons: list[str] = []
    symbol = intent.symbol.upper()
    notional = abs(intent.quantity * intent.price)
    if state.environment == "live" and policy.require_demo_qualification and not state.demo_qualified:
        reasons.append("demo_qualification_required")
    if state.environment == "live" and not state.kill_switch_tested:
        reasons.append("kill_switch_test_required")
    if not state.connector_healthy or state.connector_checked_age_seconds > 60:
        reasons.append("connector_unhealthy_or_stale")
    if not state.reconciliation_healthy:
        reasons.append("reconciliation_unhealthy")
    if state.unresolved_critical_incidents:
        reasons.append("critical_incident_unresolved")
    if symbol not in policy.allowed_symbols:
        reasons.append("symbol_not_allowed")
    if intent.quantity <= 0 or intent.quantity > policy.max_order_quantity:
        reasons.append("order_quantity_limit")
    if notional > policy.max_order_notional_usd:
        reasons.append("order_notional_limit")
    if state.open_orders >= policy.max_open_orders and not intent.reduce_only:
        reasons.append("open_order_limit")
    if state.open_positions >= policy.max_open_positions and not intent.reduce_only:
        reasons.append("open_position_limit")
    if state.gross_notional_usd + (0 if intent.reduce_only else notional) > policy.max_gross_notional_usd:
        reasons.append("gross_notional_limit")
    if state.daily_pnl_usd <= -abs(policy.max_daily_loss_usd):
        reasons.append("daily_loss_limit")
    if state.session_pnl_usd <= -abs(policy.max_session_loss_usd):
        reasons.append("session_loss_limit")
    if state.frozen and not (intent.reduce_only and policy.allow_reduce_only_when_frozen):
        reasons.append("kill_switch_frozen")
    allowed = not reasons or (
        state.frozen
        and intent.reduce_only
        and policy.allow_reduce_only_when_frozen
        and set(reasons) == {"kill_switch_frozen"}
    )
    return CanaryDecision(
        allowed=allowed,
        read_only=not allowed,
        reason_codes=tuple(reasons),
        effective_quantity=intent.quantity if allowed else 0.0,
    )

