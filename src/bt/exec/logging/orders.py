from __future__ import annotations

from typing import Any


def order_record(*, ts: Any, event: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {"ts": ts, "event": event, **payload}
