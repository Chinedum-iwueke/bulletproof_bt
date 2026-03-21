from __future__ import annotations

from dataclasses import asdict
from enum import Enum
from typing import Any

from bt.exec.reconcile.engine import ReconciliationResult


def _normalize(obj: Any) -> Any:
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, dict):
        return {k: _normalize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_normalize(v) for v in obj]
    return obj


def reconciliation_record(result: ReconciliationResult) -> dict[str, Any]:
    payload = _normalize(asdict(result))
    payload["ts"] = result.ts.isoformat()
    return payload
