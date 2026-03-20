from __future__ import annotations

from dataclasses import dataclass

from bt.core.types import Fill
from bt.exec.lifecycle.idempotency import fill_identity_key


@dataclass(frozen=True)
class FillAggregate:
    order_id: str
    requested_qty: float
    cumulative_qty: float = 0.0

    @property
    def remaining_qty(self) -> float:
        return max(0.0, self.requested_qty - self.cumulative_qty)

    @property
    def is_terminal(self) -> bool:
        return self.remaining_qty <= 1e-12


def accumulate_fills(*, order_id: str, requested_qty: float, fills: list[Fill]) -> FillAggregate:
    qty = sum(max(0.0, float(f.qty)) for f in fills if f.order_id == order_id)
    return FillAggregate(order_id=order_id, requested_qty=max(0.0, requested_qty), cumulative_qty=min(max(0.0, requested_qty), qty))


def canonical_fill_keys(fills: list[Fill]) -> set[str]:
    return {fill_identity_key(fill) for fill in fills}
