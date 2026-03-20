from __future__ import annotations

from bt.exec.events.runtime_events import RuntimeHeartbeatEvent


def heartbeat_record(event: RuntimeHeartbeatEvent, *, healthy: bool, stale_seconds: float) -> dict[str, object]:
    return {"ts": event.ts, "sequence": event.sequence, "healthy": healthy, "stale_seconds": stale_seconds, "metadata": event.metadata}
