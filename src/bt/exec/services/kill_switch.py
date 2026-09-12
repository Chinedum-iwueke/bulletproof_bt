from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

import pandas as pd

from bt.institutional.runtime_safety import (
    RuntimeSafetyError,
    contain_runtime,
    load_safety_state,
    verify_safety_journal,
)


@dataclass(frozen=True)
class FreezeState:
    freeze_new_orders: bool
    reason: str | None
    ts: pd.Timestamp | None
    allow_reduce_only_exits: bool


@dataclass
class KillSwitch:
    allow_reduce_only_exits: bool = False
    state_path: str | None = None
    journal_path: str | None = None
    _freeze_reason: str | None = None
    _freeze_ts: pd.Timestamp | None = None
    _transport_error_count: int = 0
    _rate_limit_breach_count: int = 0

    def freeze(self, *, reason: str, ts: pd.Timestamp) -> None:
        if self.state_path is not None:
            if self.journal_path is None:
                raise RuntimeSafetyError("execution safety journal path is required")
            contain_runtime(
                state_path=self.state_path,
                journal_path=self.journal_path,
                action="freeze",
                request_id=f"runtime-{uuid4()}",
                actor="execution-runtime",
                reason=reason,
                now=ts.to_pydatetime(),
            )
        self._freeze_reason = reason
        self._freeze_ts = ts

    def record_transport_error(
        self, *, ts: pd.Timestamp, max_consecutive_transport_errors: int
    ) -> None:
        self._transport_error_count += 1
        if self._transport_error_count >= max_consecutive_transport_errors:
            self.freeze(reason="max_consecutive_transport_errors", ts=ts)

    def clear_transport_errors(self) -> None:
        self._transport_error_count = 0

    def record_rate_limit_breach(
        self, *, ts: pd.Timestamp, max_rate_limit_breaches: int
    ) -> None:
        self._rate_limit_breach_count += 1
        if self._rate_limit_breach_count >= max_rate_limit_breaches:
            self.freeze(reason="max_rate_limit_breaches", ts=ts)

    def state(self) -> FreezeState:
        if self.state_path is not None:
            try:
                durable = load_safety_state(Path(self.state_path))
                if self.journal_path is None:
                    raise RuntimeSafetyError("execution safety journal path is required")
                events = verify_safety_journal(Path(self.journal_path))
                if not events or events[-1]["event_digest"] != durable["event_digest"]:
                    raise RuntimeSafetyError("execution safety state and journal diverge")
                if durable["status"] != "ready":
                    return FreezeState(
                        freeze_new_orders=True,
                        reason=str(durable["reason"]),
                        ts=pd.Timestamp(durable["changed_at"]),
                        allow_reduce_only_exits=self.allow_reduce_only_exits,
                    )
            except RuntimeSafetyError:
                return FreezeState(
                    freeze_new_orders=True,
                    reason="execution_safety_state_unavailable",
                    ts=pd.Timestamp.now(tz="UTC"),
                    allow_reduce_only_exits=self.allow_reduce_only_exits,
                )
        return FreezeState(
            freeze_new_orders=self._freeze_reason is not None,
            reason=self._freeze_reason,
            ts=self._freeze_ts,
            allow_reduce_only_exits=self.allow_reduce_only_exits,
        )


@dataclass(frozen=True)
class LiveHealthPolicy:
    max_consecutive_transport_errors: int = 5
    max_private_stream_stale_seconds: int = 15
    max_rate_limit_breaches: int = 2


@dataclass(frozen=True)
class LiveControlState:
    startup_reconciled: bool
    startup_blocked_reason: str | None
    read_only: bool
    canary_enabled: bool
    kill_switch: FreezeState
    metadata: dict[str, object] = field(default_factory=dict)
