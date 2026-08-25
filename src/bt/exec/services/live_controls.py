from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

import pandas as pd

from bt.core.types import Order, OrderIntent, Position


@dataclass(frozen=True)
class CanaryPolicy:
    enabled: bool = False
    max_symbols: int = 1
    allowed_symbols: tuple[str, ...] = ()
    max_total_open_positions: int = 1
    max_open_orders_total: int = 2
    max_notional_usd: float = 100.0
    max_order_qty: float = 0.001
    max_orders_per_hour: int = 10
    max_gross_notional_usd: float = 100.0
    max_daily_loss_usd: float = 25.0
    max_session_loss_usd: float = 25.0
    max_duration_seconds: int = 3600
    max_market_data_age_seconds: int = 15


@dataclass
class CanaryGuard:
    policy: CanaryPolicy
    submitted_this_session: int = 0
    session_started_at: datetime = field(
        default_factory=lambda: datetime.now(tz=UTC)
    )

    def validate_intent(
        self,
        *,
        intent: OrderIntent,
        open_orders: list[Order],
        positions: list[Position],
        current_price: float,
        current_equity: float | None = None,
        starting_equity: float | None = None,
        gross_notional_usd: float | None = None,
        bar_ts: pd.Timestamp | None = None,
        wall_clock: datetime | None = None,
    ) -> str | None:
        if not self.policy.enabled:
            return None
        if self.policy.allowed_symbols and intent.symbol not in self.policy.allowed_symbols:
            return f"symbol_not_allowed:{intent.symbol}"
        if len({o.symbol for o in open_orders} | {p.symbol for p in positions}) > self.policy.max_symbols:
            return "max_symbols_exceeded"
        if len([p for p in positions if float(p.qty) > 0]) > self.policy.max_total_open_positions:
            return "max_total_open_positions_exceeded"
        if len(open_orders) >= self.policy.max_open_orders_total:
            return "max_open_orders_total_exceeded"
        if abs(float(intent.qty)) > self.policy.max_order_qty:
            return "max_order_qty_exceeded"
        if (abs(float(intent.qty)) * float(current_price)) > self.policy.max_notional_usd:
            return "max_notional_usd_exceeded"
        if self.submitted_this_session >= self.policy.max_orders_per_hour:
            return "max_orders_per_hour_exceeded"
        if gross_notional_usd is not None and (
            gross_notional_usd + abs(float(intent.qty)) * float(current_price)
            > self.policy.max_gross_notional_usd
        ):
            return "max_gross_notional_usd_exceeded"
        if current_equity is not None and starting_equity is not None:
            loss = max(starting_equity - current_equity, 0.0)
            if loss >= self.policy.max_daily_loss_usd:
                return "max_daily_loss_usd_exceeded"
            if loss >= self.policy.max_session_loss_usd:
                return "max_session_loss_usd_exceeded"
        if bar_ts is not None:
            current_time = wall_clock or datetime.now(tz=UTC)
            if (
                current_time - self.session_started_at
            ).total_seconds() > self.policy.max_duration_seconds:
                return "max_duration_seconds_exceeded"
            current = pd.Timestamp(current_time)
            timestamp = pd.Timestamp(bar_ts)
            age = (current - timestamp).total_seconds()
            if age < 0 or age > self.policy.max_market_data_age_seconds:
                return "market_data_not_wall_clock_fresh"
        return None

    def record_submission(self) -> None:
        self.submitted_this_session += 1


def load_canary_policy(config: dict[str, object]) -> CanaryPolicy:
    live_controls = config.get("live_controls") if isinstance(config.get("live_controls"), dict) else {}
    canary = config.get("canary") if isinstance(config.get("canary"), dict) else {}
    enabled = bool(live_controls.get("enabled", False)) and bool(live_controls.get("canary_mode", False))
    allowed_symbols_raw = canary.get("allowed_symbols", [])
    allowed_symbols = (
        tuple(str(s) for s in allowed_symbols_raw)
        if isinstance(allowed_symbols_raw, list)
        else ()
    )
    policy = CanaryPolicy(
        enabled=enabled,
        max_symbols=int(canary.get("max_symbols", 1)),
        allowed_symbols=allowed_symbols,
        max_total_open_positions=int(canary.get("max_total_open_positions", 1)),
        max_open_orders_total=int(canary.get("max_open_orders_total", 2)),
        max_notional_usd=float(canary.get("max_notional_usd", 100.0)),
        max_order_qty=float(canary.get("max_order_qty", 0.001)),
        max_orders_per_hour=int(canary.get("max_orders_per_hour", 10)),
        max_gross_notional_usd=float(canary.get("max_gross_notional_usd", 100.0)),
        max_daily_loss_usd=float(canary.get("max_daily_loss_usd", 25.0)),
        max_session_loss_usd=float(canary.get("max_session_loss_usd", 25.0)),
        max_duration_seconds=int(canary.get("max_duration_seconds", 3600)),
        max_market_data_age_seconds=int(
            canary.get("max_market_data_age_seconds", 15)
        ),
    )
    if policy.enabled and policy.max_symbols <= 0:
        raise ValueError("canary.max_symbols must be > 0 when canary is enabled")
    if policy.enabled and policy.max_order_qty <= 0:
        raise ValueError("canary.max_order_qty must be > 0 when canary is enabled")
    if policy.enabled and any(
        value <= 0
        for value in (
            policy.max_gross_notional_usd,
            policy.max_daily_loss_usd,
            policy.max_session_loss_usd,
            policy.max_duration_seconds,
            policy.max_market_data_age_seconds,
        )
    ):
        raise ValueError("live canary loss, duration, gross and freshness limits must be positive")
    return policy
