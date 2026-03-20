from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class OrderLifecycleState(str, Enum):
    CREATED = "created"
    PENDING_SUBMIT = "pending_submit"
    SUBMITTED = "submitted"
    ACKNOWLEDGED = "acknowledged"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    PENDING_CANCEL = "pending_cancel"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


TERMINAL_ORDER_STATES: frozenset[OrderLifecycleState] = frozenset(
    {
        OrderLifecycleState.FILLED,
        OrderLifecycleState.CANCELLED,
        OrderLifecycleState.REJECTED,
        OrderLifecycleState.EXPIRED,
    }
)


VALID_TRANSITIONS: dict[OrderLifecycleState, frozenset[OrderLifecycleState]] = {
    OrderLifecycleState.CREATED: frozenset({OrderLifecycleState.PENDING_SUBMIT, OrderLifecycleState.SUBMITTED}),
    OrderLifecycleState.PENDING_SUBMIT: frozenset({OrderLifecycleState.SUBMITTED, OrderLifecycleState.REJECTED, OrderLifecycleState.EXPIRED}),
    OrderLifecycleState.SUBMITTED: frozenset(
        {
            OrderLifecycleState.ACKNOWLEDGED,
            OrderLifecycleState.PARTIALLY_FILLED,
            OrderLifecycleState.FILLED,
            OrderLifecycleState.PENDING_CANCEL,
            OrderLifecycleState.REJECTED,
            OrderLifecycleState.EXPIRED,
        }
    ),
    OrderLifecycleState.ACKNOWLEDGED: frozenset(
        {
            OrderLifecycleState.PARTIALLY_FILLED,
            OrderLifecycleState.FILLED,
            OrderLifecycleState.PENDING_CANCEL,
            OrderLifecycleState.CANCELLED,
            OrderLifecycleState.REJECTED,
            OrderLifecycleState.EXPIRED,
        }
    ),
    OrderLifecycleState.PARTIALLY_FILLED: frozenset(
        {
            OrderLifecycleState.PARTIALLY_FILLED,
            OrderLifecycleState.FILLED,
            OrderLifecycleState.PENDING_CANCEL,
            OrderLifecycleState.CANCELLED,
            OrderLifecycleState.EXPIRED,
        }
    ),
    OrderLifecycleState.PENDING_CANCEL: frozenset(
        {
            OrderLifecycleState.CANCELLED,
            OrderLifecycleState.PARTIALLY_FILLED,
            OrderLifecycleState.FILLED,
            OrderLifecycleState.EXPIRED,
        }
    ),
    OrderLifecycleState.FILLED: frozenset(),
    OrderLifecycleState.CANCELLED: frozenset(),
    OrderLifecycleState.REJECTED: frozenset(),
    OrderLifecycleState.EXPIRED: frozenset(),
}


@dataclass(frozen=True)
class LifecycleTransition:
    from_state: OrderLifecycleState | None
    to_state: OrderLifecycleState
    valid: bool
    reason: str | None = None


def can_transition(from_state: OrderLifecycleState | None, to_state: OrderLifecycleState) -> bool:
    if from_state is None:
        return to_state in {OrderLifecycleState.CREATED, OrderLifecycleState.PENDING_SUBMIT, OrderLifecycleState.SUBMITTED}
    if from_state == to_state:
        return from_state == OrderLifecycleState.PARTIALLY_FILLED
    return to_state in VALID_TRANSITIONS[from_state]


def validate_transition(from_state: OrderLifecycleState | None, to_state: OrderLifecycleState) -> LifecycleTransition:
    if can_transition(from_state, to_state):
        return LifecycleTransition(from_state=from_state, to_state=to_state, valid=True)
    return LifecycleTransition(
        from_state=from_state,
        to_state=to_state,
        valid=False,
        reason=f"invalid lifecycle transition {from_state!s} -> {to_state!s}",
    )


def is_terminal_state(state: OrderLifecycleState) -> bool:
    return state in TERMINAL_ORDER_STATES
