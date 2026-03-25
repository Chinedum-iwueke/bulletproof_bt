from __future__ import annotations

from dataclasses import dataclass

from bt.core.types import Order, OrderIntent, Position


@dataclass(frozen=True)
class CanaryPolicy:
    enabled: bool = False
    max_symbols: int = 1
    allowed_symbols: tuple[str, ...] = tuple()
    max_total_open_positions: int = 1
    max_open_orders_total: int = 2
    max_notional_usd: float = 100.0
    max_order_qty: float = 0.001
    max_orders_per_hour: int = 10


@dataclass
class CanaryGuard:
    policy: CanaryPolicy
    submitted_this_session: int = 0

    def validate_intent(
        self,
        *,
        intent: OrderIntent,
        open_orders: list[Order],
        positions: list[Position],
        current_price: float,
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
        return None

    def record_submission(self) -> None:
        self.submitted_this_session += 1


def load_canary_policy(config: dict[str, object]) -> CanaryPolicy:
    live_controls = config.get("live_controls") if isinstance(config.get("live_controls"), dict) else {}
    canary = config.get("canary") if isinstance(config.get("canary"), dict) else {}
    enabled = bool(live_controls.get("enabled", False)) and bool(live_controls.get("canary_mode", False))
    allowed_symbols_raw = canary.get("allowed_symbols", [])
    allowed_symbols = tuple(str(s) for s in allowed_symbols_raw) if isinstance(allowed_symbols_raw, list) else tuple()
    policy = CanaryPolicy(
        enabled=enabled,
        max_symbols=int(canary.get("max_symbols", 1)),
        allowed_symbols=allowed_symbols,
        max_total_open_positions=int(canary.get("max_total_open_positions", 1)),
        max_open_orders_total=int(canary.get("max_open_orders_total", 2)),
        max_notional_usd=float(canary.get("max_notional_usd", 100.0)),
        max_order_qty=float(canary.get("max_order_qty", 0.001)),
        max_orders_per_hour=int(canary.get("max_orders_per_hour", 10)),
    )
    if policy.enabled and policy.max_symbols <= 0:
        raise ValueError("canary.max_symbols must be > 0 when canary is enabled")
    if policy.enabled and policy.max_order_qty <= 0:
        raise ValueError("canary.max_order_qty must be > 0 when canary is enabled")
    return policy
