#!/usr/bin/env python3
"""Run the deterministic, no-authority EXEC-003 venue identity pilot."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from bt.institutional.receipt import build_receipt, digest
from bt.institutional.venue import MAPPING_SCHEMA_VERSION, venue_identity_receipt


def dependency(milestone: str, source_commit: str, dataset_digest: str, result: dict):
    return build_receipt(
        milestone=milestone,
        producer=f"exec003.fixture.{milestone.lower()}",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={},
        dataset_digest=dataset_digest,
        configuration={},
        artifacts={},
        result=result,
    ).as_dict()


def listing(venue: str, *, price_tick: str, quantity_step: str) -> dict:
    start = datetime(2026, 8, 1, tzinfo=UTC).isoformat()
    return {
        "venue_id": venue,
        "listing_id": "BTCUSDT",
        "canonical_instrument_id": "BTC-USDT-LINEAR-PERP",
        "symbol": "BTCUSDT",
        "instrument_type": "linear_perpetual",
        "base_asset": "BTC",
        "quote_asset": "USDT",
        "settlement_asset": "USDT",
        "margin_asset": "USDT",
        "contract_size": "1",
        "inverse": False,
        "expiry": None,
        "price_tick": price_tick,
        "quantity_step": quantity_step,
        "minimum_quantity": quantity_step,
        "maximum_quantity": "100",
        "minimum_notional": "5",
        "status": "active",
        "effective_from": start,
        "effective_to": None,
        "available_at": start,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    now = datetime(2026, 8, 28, 13, 0, tzinfo=UTC)
    records = [
        listing("bybit", price_tick="0.10", quantity_step="0.001"),
        listing("binance", price_tick="0.10", quantity_step="0.001"),
    ]
    dataset_digest = digest({"dataset": "exec003-bybit-binance-identity-fixture-v1"})
    data001 = dependency(
        "DATA-001",
        args.source_commit,
        dataset_digest,
        {"schema_version": "data001-reference-snapshot-v1.0.0", "records": records},
    )
    receipt = venue_identity_receipt(
        data001_receipt=data001,
        exec001_receipt=dependency(
            "EXEC-001", args.source_commit, dataset_digest, {"reconstructable": True}
        ),
        exec002_receipt=dependency(
            "EXEC-002",
            args.source_commit,
            dataset_digest,
            {"state": {"coverage": {"observed": 9}}},
        ),
        relationships=[
            {
                "mapping_id": "btc-usdt-linear-perp-v1",
                "relationship_kind": "economic_equivalent",
                "members": [
                    {"venue_id": "bybit", "listing_id": "BTCUSDT"},
                    {"venue_id": "binance", "listing_id": "BTCUSDT"},
                ],
                "effective_from": (now - timedelta(days=1)).isoformat(),
                "effective_to": None,
                "available_at": (now - timedelta(days=1)).isoformat(),
            }
        ],
        venue_observations=[
            {
                "venue_id": venue,
                "status": "operational",
                "observed_at": (now - timedelta(seconds=2)).isoformat(),
                "available_at": (now - timedelta(seconds=1)).isoformat(),
            }
            for venue in ("bybit", "binance")
        ],
        as_of=now,
        known_at=now,
        source_commit=args.source_commit,
        dataset_digest=dataset_digest,
        configuration={"maximum_observation_age_seconds": 60},
    )
    report = {
        "schema_version": "exec003-native-pilot-v1.0.0",
        "success": receipt.result["qualified"],
        "capital_or_order_authority": False,
        "mapping_specification": {"schema_version": MAPPING_SCHEMA_VERSION},
        "mapping_specification_digest": receipt.result["mapping_schema_digest"],
        "receipt": receipt.as_dict(),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
