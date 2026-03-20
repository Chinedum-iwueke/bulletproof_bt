from __future__ import annotations

from dataclasses import asdict
from typing import Any

from bt.exec.reconcile.engine import ReconciliationResult


def reconciliation_record(result: ReconciliationResult) -> dict[str, Any]:
    payload = asdict(result)
    payload["ts"] = result.ts.isoformat()
    return payload
