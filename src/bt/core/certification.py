"""BT-004 causal ordering, accounting, and replay certification contracts."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import hashlib
import json
import math
import re
from typing import Any, Iterable

import pandas as pd


CONTRACT_VERSION = "classic-engine-state-v1.0.0"


class CertificationError(ValueError):
    """Raised when engine evidence cannot satisfy the BT-004 contract."""


class EventKind(str, Enum):
    MARKET = "market"
    SESSION = "session"
    FUNDING = "funding"
    BORROW = "borrow"
    VALUATION = "valuation"
    DECISION = "decision"
    ORDER = "order"
    FILL = "fill"
    LIQUIDATION = "liquidation"


_EVENT_PRIORITY = {kind: index for index, kind in enumerate(EventKind)}


def _utc(value: pd.Timestamp | datetime | str, field: str) -> pd.Timestamp:
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None:
        raise CertificationError(f"{field} must be timezone-aware UTC")
    return parsed.tz_convert("UTC")


def _normalize(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CertificationError("canonical state contains a non-finite number")
        return value
    if isinstance(value, (pd.Timestamp, datetime)):
        return _utc(value, "state timestamp").isoformat()
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise CertificationError("canonical state keys must be strings")
        return {key: _normalize(item) for key, item in value.items()}
    raise CertificationError(
        f"canonical state contains unsupported type: {type(value).__name__}"
    )


def _canonical(value: Any) -> bytes:
    return json.dumps(
        _normalize(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")


def digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


@dataclass(frozen=True)
class CausalEvent:
    event_id: str
    kind: EventKind
    event_time: pd.Timestamp
    available_at: pd.Timestamp
    source: str
    sequence: int
    payload: dict[str, Any]

    def __post_init__(self) -> None:
        if not self.event_id or not self.source:
            raise CertificationError("event_id and source are required")
        if self.sequence < 0:
            raise CertificationError("sequence must be non-negative")
        object.__setattr__(self, "event_time", _utc(self.event_time, "event_time"))
        object.__setattr__(self, "available_at", _utc(self.available_at, "available_at"))

    @property
    def ordering_key(self) -> tuple[Any, ...]:
        return (
            self.available_at.value,
            self.event_time.value,
            _EVENT_PRIORITY[self.kind],
            self.source,
            self.sequence,
            self.event_id,
        )

    def to_document(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "kind": self.kind.value,
            "event_time": self.event_time.isoformat(),
            "available_at": self.available_at.isoformat(),
            "source": self.source,
            "sequence": self.sequence,
            "payload": self.payload,
        }


def canonical_event_order(events: Iterable[CausalEvent]) -> list[CausalEvent]:
    materialized = list(events)
    identifiers = [event.event_id for event in materialized]
    if len(identifiers) != len(set(identifiers)):
        raise CertificationError("duplicate event_id")
    return sorted(materialized, key=lambda event: event.ordering_key)


def information_available_at(
    events: Iterable[CausalEvent], decision_at: pd.Timestamp | datetime | str
) -> list[CausalEvent]:
    cutoff = _utc(decision_at, "decision_at")
    return [
        event
        for event in canonical_event_order(events)
        if event.available_at <= cutoff
    ]


def assert_decision_causality(
    evidence: Iterable[CausalEvent], decision_at: pd.Timestamp | datetime | str
) -> None:
    cutoff = _utc(decision_at, "decision_at")
    future = [event.event_id for event in evidence if event.available_at > cutoff]
    if future:
        raise CertificationError(
            "decision observed unavailable information: " + ", ".join(sorted(future))
        )


@dataclass(frozen=True)
class AccountingSnapshot:
    cash: float
    equity: float
    realized_pnl: float
    unrealized_pnl: float
    used_margin: float
    free_margin: float

    def validate(self, *, tolerance: float = 1e-8) -> None:
        values = (
            self.cash,
            self.equity,
            self.realized_pnl,
            self.unrealized_pnl,
            self.used_margin,
            self.free_margin,
        )
        if not all(math.isfinite(value) for value in values):
            raise CertificationError("accounting snapshot contains non-finite values")
        if self.used_margin < -tolerance:
            raise CertificationError("used_margin must be non-negative")
        expected_equity = self.cash + self.realized_pnl + self.unrealized_pnl
        if abs(self.equity - expected_equity) > tolerance:
            raise CertificationError("equity accounting identity failed")
        if abs(self.free_margin - (self.equity - self.used_margin)) > tolerance:
            raise CertificationError("free-margin accounting identity failed")


@dataclass(frozen=True)
class CertifiedCheckpoint:
    dataset_digest: str
    config_digest: str
    last_event_key: tuple[Any, ...] | None
    state: dict[str, Any]
    state_digest: str
    contract_version: str = CONTRACT_VERSION

    @classmethod
    def create(
        cls,
        *,
        dataset_digest: str,
        config_digest: str,
        last_event: CausalEvent | None,
        state: dict[str, Any],
    ) -> "CertifiedCheckpoint":
        return cls(
            dataset_digest=dataset_digest,
            config_digest=config_digest,
            last_event_key=last_event.ordering_key if last_event else None,
            state=state,
            state_digest=digest(state),
        )

    def validate(self, *, dataset_digest: str, config_digest: str) -> None:
        for name, value in (
            ("dataset_digest", self.dataset_digest),
            ("config_digest", self.config_digest),
            ("state_digest", self.state_digest),
        ):
            if re.fullmatch(r"[0-9a-f]{64}", value) is None:
                raise CertificationError(f"checkpoint {name} must be SHA-256")
        if self.contract_version != CONTRACT_VERSION:
            raise CertificationError("checkpoint contract version is unsupported")
        if self.dataset_digest != dataset_digest:
            raise CertificationError("checkpoint dataset digest mismatch")
        if self.config_digest != config_digest:
            raise CertificationError("checkpoint config digest mismatch")
        if digest(self.state) != self.state_digest:
            raise CertificationError("checkpoint state digest mismatch")


def transition_digest(
    *, previous_digest: str, event: CausalEvent, state: dict[str, Any]
) -> str:
    return digest(
        {
            "contract_version": CONTRACT_VERSION,
            "previous_digest": previous_digest,
            "event": event.to_document(),
            "state_digest": digest(state),
        }
    )
