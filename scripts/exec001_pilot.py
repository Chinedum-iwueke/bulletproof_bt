#!/usr/bin/env python3
"""Run the deterministic no-capital EXEC-001 native pilot."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from bt.institutional.execution import canonical_event, execution_journal_receipt
from bt.institutional.receipt import build_receipt, digest


def dependency(milestone: str, source_commit: str, dataset_digest: str) -> dict:
    return build_receipt(
        milestone=milestone,
        producer=f"exec001.fixture.{milestone.lower()}",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"milestone": milestone},
        dataset_digest=dataset_digest,
        configuration={},
        artifacts={},
        result={"qualified": True},
    ).as_dict()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    dataset_digest = digest({"dataset": "exec001-dual-clock-fixture-v1"})
    start = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)

    def item(sequence: int, received: int):
        return canonical_event(
            event_id=f"bybit-order-{sequence}",
            source="bybit",
            stream="BTCUSDT:private-orders:epoch-1",
            kind="order_update",
            instrument_id="BTCUSDT-PERP",
            event_time=start + timedelta(seconds=sequence),
            receive_time=start + timedelta(seconds=received),
            source_sequence=sequence,
            payload={"order_id": "order-1", "state": ["acknowledged", "partially_filled", "filled"][sequence - 1]},
        )

    receipt = execution_journal_receipt(
        events=[item(1, 2), item(3, 4), item(2, 5)],
        data_reference_receipt=dependency("DATA-001", args.source_commit, dataset_digest),
        market_catalog_receipt=dependency("DATA-002", args.source_commit, dataset_digest),
        source_commit=args.source_commit,
        dataset_digest=dataset_digest,
        configuration={"maximum_future_drift_seconds": 5, "replay_order": "event-source-stream-sequence-receive-digest"},
    )
    report = {
        "schema_version": "exec001-native-pilot-v1.0.0",
        "success": receipt.result["reconstructable"],
        "capital_or_order_authority": False,
        "event_schema_specification": {"schema_version": "canonical-execution-event-v1.0.0"},
        "event_schema_specification_digest": receipt.result["event_schema_digest"],
        "receipt": receipt.as_dict(),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
