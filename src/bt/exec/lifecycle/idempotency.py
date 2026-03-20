from __future__ import annotations

import hashlib

from bt.core.types import Fill
from bt.exec.state.store import ExecutionStateStore


_CLIENT_ID_PREFIX = "co"


def build_client_order_id(*, order_seq: int) -> str:
    return f"{_CLIENT_ID_PREFIX}-{order_seq}"


def is_valid_client_order_id(value: str) -> bool:
    parts = value.split("-")
    return len(parts) == 2 and parts[0] == _CLIENT_ID_PREFIX and parts[1].isdigit()


def lifecycle_dedupe_key(*, order_id: str, event_type: str, sequence_hint: int | None = None) -> str:
    suffix = "" if sequence_hint is None else f":{sequence_hint}"
    return f"lifecycle:{order_id}:{event_type}{suffix}"


def fill_identity_key(fill: Fill) -> str:
    raw = f"{fill.order_id}|{fill.symbol}|{fill.side.value}|{fill.qty:.12f}|{fill.price:.12f}|{fill.ts.isoformat()}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return f"fill:{digest}"


def should_process_once(*, store: ExecutionStateStore | None, run_id: str, dedupe_key: str) -> bool:
    if store is None:
        return True
    return not store.has_processed_event(run_id=run_id, dedupe_key=dedupe_key)
