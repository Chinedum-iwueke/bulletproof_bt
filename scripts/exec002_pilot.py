#!/usr/bin/env python3
"""Run deterministic no-capital EXEC-002 microstructure pilot."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from bt.institutional.execution import canonical_event
from bt.institutional.microstructure import microstructure_state_receipt
from bt.institutional.receipt import build_receipt, digest


def dependency(milestone, source_commit, dataset_digest, result):
    return build_receipt(
        milestone=milestone, producer=f"exec002.fixture.{milestone.lower()}", producer_version="1.0.0",
        source_commit=source_commit, inputs={}, dataset_digest=dataset_digest, configuration={}, artifacts={}, result=result,
    ).as_dict()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    start = datetime(2026, 8, 28, 13, 0, tzinfo=UTC)
    dataset_digest = digest({"dataset": "exec002-observed-microstructure-fixture-v1"})

    def item(sequence, kind, payload):
        return canonical_event(
            event_id=f"{kind}-{sequence}", source="bybit", stream="BTCUSDT:microstructure:epoch-1", kind=kind,
            instrument_id="BTCUSDT-PERP", event_time=start + timedelta(seconds=sequence),
            receive_time=start + timedelta(seconds=sequence + 1), source_sequence=sequence, payload=payload,
        )

    events = [
        item(1, "order_book", {"bids": [[100, 3], [99, 2]], "asks": [[101, 2], [102, 1]]}),
        item(2, "trade", {"price": 101, "size": 2, "aggressor_side": "buy"}),
        item(3, "trade", {"price": 100, "size": 1, "aggressor_side": "sell"}),
        item(4, "mark_price", {"price": 101}), item(5, "index_price", {"price": 100}),
        item(6, "funding", {"value": "0.0001"}), item(7, "open_interest", {"value": "1200000"}),
        item(8, "liquidation", {"notional": "25000"}),
    ]
    receipt = microstructure_state_receipt(
        events=events,
        exec001_receipt=dependency("EXEC-001", args.source_commit, dataset_digest, {"reconstructable": True}),
        data003_receipt=dependency("DATA-003", args.source_commit, dataset_digest, {"qualified": True}),
        as_of=start + timedelta(seconds=20), source_commit=args.source_commit, dataset_digest=dataset_digest,
        configuration={"maximum_auxiliary_age_seconds": 3600},
    )
    report = {
        "schema_version": "exec002-native-pilot-v1.0.0", "success": True,
        "capital_or_order_authority": False,
        "model_specification": {"schema_version": "exec002-microstructure-model-v1.0.0"},
        "model_specification_digest": receipt.result["model_schema_digest"], "receipt": receipt.as_dict(),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
