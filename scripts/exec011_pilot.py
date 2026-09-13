#!/usr/bin/env python3
"""Produce one deterministic, no-capital EXEC-011 venue replay bundle."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

from bt.institutional.receipt import build_receipt, digest
from bt.institutional.venue_telemetry import (
    DEPENDENCY_PRODUCERS,
    VENUE_TELEMETRY_SPECIFICATION,
    venue_event,
    venue_telemetry_receipt,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip()
    dataset_digest = digest(
        {"fixture": "exec011-bybit-demo-replay", "version": "1.0.0"}
    )
    dependency_receipts = {
        milestone: build_receipt(
            milestone=milestone,
            producer=producer,
            producer_version="1.0.0",
            source_commit=source_commit,
            inputs={"pilot_dependency": milestone},
            dataset_digest=dataset_digest,
            configuration={"bounded_fixture": True},
            artifacts={"digest": digest(milestone)},
            result={"success": True, "pilot_only": True},
        )
        for milestone, producer in DEPENDENCY_PRODUCERS.items()
    }
    start = datetime(2026, 9, 12, 12, tzinfo=UTC)

    def create(sequence: int, kind: str, payload: dict):
        observed = start + timedelta(seconds=sequence)
        return venue_event(
            event_id=f"exec011-{sequence}",
            venue="bybit",
            environment="demo",
            account_pseudonym="acct-demo-exec011",
            instrument_id="bybit:linear:BTCUSDT",
            stream="private-account",
            kind=kind,
            exchange_time=observed,
            source_time=observed + timedelta(milliseconds=10),
            receive_time=observed + timedelta(milliseconds=20),
            sequence=sequence,
            cursor=str(sequence),
            raw_reference_digest=digest({"raw_event": sequence}),
            normalization_version="1.0.0",
            payload=payload,
            reconciliation_id="reconcile-1" if kind == "reconciliation" else None,
        )

    events = [
        create(
            1,
            "order",
            {
                "client_order_id": "exec011-o1",
                "status": "filled",
                "side": "buy",
                "quantity": "1",
            },
        ),
        create(
            2,
            "fill",
            {
                "execution_id": "exec011-f1",
                "client_order_id": "exec011-o1",
                "side": "buy",
                "quantity": "1",
                "price": "100",
            },
        ),
        create(
            3, "fee", {"execution_id": "exec011-f1", "asset": "USDT", "amount": "0.10"}
        ),
        create(4, "funding", {"asset": "USDT", "amount": "-0.02"}),
        create(
            5,
            "fill",
            {
                "execution_id": "exec011-f2",
                "client_order_id": "exec011-o2",
                "side": "sell",
                "quantity": "1",
                "price": "110",
            },
        ),
        create(6, "position", {"quantity": "0", "entry_price": "0"}),
        create(7, "cash", {"asset": "USDT", "balance": "1009.88"}),
        create(
            8, "margin", {"initial": "0", "maintenance": "0", "available": "1009.88"}
        ),
        create(
            9,
            "reconciliation",
            {"positions": {"bybit:linear:BTCUSDT": "0"}, "cash": {"USDT": "1009.88"}},
        ),
    ]
    schema_digest = digest(VENUE_TELEMETRY_SPECIFICATION)
    receipt = venue_telemetry_receipt(
        events=[*events, events[1]],
        dependencies=dependency_receipts,
        known_at=start + timedelta(seconds=10),
        source_commit=source_commit,
        dataset_digest=dataset_digest,
        telemetry_schema_digest=schema_digest,
        configuration={"freshness_seconds": 120, "fixture": "bybit-demo"},
        governance_digests={
            "GOV-003": "1" * 64,
            "PLAT-005": "2" * 64,
            "PLAT-007": "3" * 64,
            "UI-007": "4" * 64,
        },
    )
    report = {
        "schema_version": "exec011-native-pilot-v1.0.0",
        "success": True,
        "telemetry_specification": VENUE_TELEMETRY_SPECIFICATION,
        "telemetry_specification_digest": schema_digest,
        "receipt": receipt.as_dict(),
        "checks": {
            "duplicate_suppressed": receipt.result["projection"][
                "duplicate_event_count"
            ]
            == 1,
            "reconciliation_exact": not receipt.result["projection"][
                "reconciliation_discrepancies"
            ],
            "trade_episode_reconstructed": len(
                receipt.result["projection"]["trade_episodes"]
            )
            == 1,
            "authority_absent": not any(receipt.authority.values()),
        },
    }
    report["report_digest"] = digest(report)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="ascii"
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if all(report["checks"].values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
