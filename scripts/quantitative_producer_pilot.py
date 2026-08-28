#!/usr/bin/env python3
"""Generate one no-capital native receipt chain for reopened quantitative milestones."""

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from pathlib import Path

import numpy as np

from bt.institutional.data import (
    lake_quality_receipt,
    market_catalog_receipt,
    reference_snapshot_receipt,
)
from bt.institutional.discovery import (
    factor_program_receipt,
    opportunity_map_receipt,
    search_proposal_receipt,
    selection_audit_receipt,
    symbolic_candidate_receipt,
)
from bt.institutional.ml import (
    calibration_receipt,
    causal_materialization_receipt,
    model_family_evaluation_receipt,
)
from bt.institutional.receipt import digest, verify_receipt
from bt.institutional.risk import stress_dossier_receipt, venue_rule_receipt
from bt.institutional.rl import off_policy_evaluation_receipt, offline_dataset_receipt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    receipts = []
    reference = reference_snapshot_receipt(
        records=[
            {
                "venue_id": "bybit",
                "listing_id": "btc-perp",
                "instrument_id": "btc",
                "available_at": "2026-01-01T00:00:00Z",
                "effective_from": "2026-01-01T00:00:00Z",
                "effective_to": None,
                "status": "active",
            }
        ],
        as_of="2026-02-01T00:00:00Z",
        source_commit=source_commit,
    )
    receipts.append(reference)
    payload = b"canonical-partition"
    partition_digest = digest({"bytes_hex": payload.hex()})
    catalog = market_catalog_receipt(
        partitions=[
            {
                "partition_id": "part-1",
                "venue_id": "bybit",
                "listing_id": "btc-perp",
                "row_count": 1200,
                "duplicate_count": 0,
                "observed_start": "2026-01-01T00:00:00Z",
                "observed_end": "2026-01-31T00:00:00Z",
                "available_at": "2026-01-31T00:00:01Z",
                "content_digest": partition_digest,
            }
        ],
        reference_receipt=reference,
        source_commit=source_commit,
    )
    receipts.append(catalog)
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "part-1"
        path.write_bytes(payload)
        receipts.append(
            lake_quality_receipt(
                catalog_receipt=catalog, files=[path], source_commit=source_commit
            )
        )
    dataset_digest = catalog.dataset_digest
    program = factor_program_receipt(
        specification={
            "fields": {"return": {"unit": "dimensionless", "availability_lag": 0}},
            "factors": {
                "momentum": {
                    "expression": {"op": "field", "args": ["return", 1]},
                    "output_unit": "dimensionless",
                }
            },
            "parameters": {"window": [2, 4]},
            "maximum_trials": 2,
        },
        dataset_digest=dataset_digest,
        source_commit=source_commit,
    )
    opportunity = opportunity_map_receipt(
        baseline=np.zeros(40),
        candidate=np.full(40, 0.03),
        costs=np.full(40, 0.005),
        dataset_digest=dataset_digest,
        source_commit=source_commit,
    )
    search = search_proposal_receipt(
        program_receipt=program,
        observations=[],
        method="random",
        seed=20260828,
        budget=2,
        source_commit=source_commit,
    )
    symbolic = symbolic_candidate_receipt(
        base_program=program,
        candidates=[
            {"operators": ["add"], "expression": "lagged_return_plus_constant"},
            {"operators": ["network"], "expression": "forbidden"},
        ],
        allowed_operators={"add", "lag"},
        maximum_nodes=4,
        source_commit=source_commit,
    )
    audit = selection_audit_receipt(
        trials=[
            {
                "trial_digest": program.result["trials"][0]["trial_digest"],
                "p_value": 0.01,
                "validation_rank": 1,
            },
            {
                "trial_digest": program.result["trials"][1]["trial_digest"],
                "p_value": 0.5,
                "validation_rank": 2,
            },
        ],
        alpha=0.05,
        dataset_digest=dataset_digest,
        source_commit=source_commit,
    )
    receipts.extend([opportunity, program, search, symbolic, audit])
    features = np.arange(80, dtype=float).reshape(40, 2) / 80
    labels = np.asarray([0, 1] * 20)
    material = causal_materialization_receipt(
        timestamps=[
            f"2026-01-{1 + i // 24:02d}T{i % 24:02d}:00:00Z" for i in range(40)
        ],
        features=features,
        labels=labels,
        feature_lags=[1, 2],
        folds=[
            {"train_start": 0, "train_end": 15, "test_start": 18, "test_end": 22},
            {"train_start": 0, "train_end": 22, "test_start": 25, "test_end": 30},
        ],
        purge=2,
        embargo=2,
        dataset_digest=dataset_digest,
        factor_receipt=program,
        source_commit=source_commit,
    )
    probabilities = np.where(labels == 1, 0.8, 0.2)
    evaluation = model_family_evaluation_receipt(
        materialization_receipt=material,
        labels=labels,
        predictions={"linear_baseline": np.full(40, 0.5), "candidate": probabilities},
        regimes=["calm"] * 20 + ["volatile"] * 20,
        baseline_families={"linear_baseline"},
        minimum_increment=0.1,
        dataset_digest=dataset_digest,
        source_commit=source_commit,
    )
    calibration = calibration_receipt(
        evaluation_receipt=evaluation,
        family="candidate",
        labels=labels,
        probabilities=probabilities,
        bins=5,
        minimum_confidence=0.6,
        maximum_ece=0.25,
        dataset_digest=dataset_digest,
        source_commit=source_commit,
    )
    receipts.extend([material, evaluation, calibration])
    transitions = [
        {
            "episode_id": f"episode-{i // 4}",
            "step": i % 4,
            "state": [float(i)],
            "action": "hold" if i % 2 == 0 else "trade",
            "behavior_propensity": 0.5,
            "reward": 0.01 if i % 2 else 0.0,
            "next_state": [float(i + 1)],
            "terminal": i % 4 == 3,
        }
        for i in range(40)
    ]
    offline = offline_dataset_receipt(
        transitions=transitions,
        allowed_actions={"hold", "trade"},
        dataset_digest=dataset_digest,
        shadow_receipt_digest="b" * 64,
        source_commit=source_commit,
    )
    off_policy = off_policy_evaluation_receipt(
        dataset_receipt=offline,
        rewards=np.full(40, 0.02),
        behavior_probabilities=np.full(40, 0.5),
        target_probabilities=np.full(40, 0.5),
        direct_values=np.full(40, 0.018),
        confidence_z=1.96,
        maximum_weight=5,
        minimum_effective_sample_size=20,
        source_commit=source_commit,
    )
    receipts.extend([offline, off_policy])
    scenario_names = (
        "price_gap",
        "correlation_break",
        "liquidity_freeze",
        "model_failure",
        "prolonged_drawdown",
    )
    stress = stress_dossier_receipt(
        returns=np.asarray([0.002, -0.001] * 20),
        scenarios={name: [-0.01, -0.01, 0.005] for name in scenario_names},
        scenario_limit=0.1,
        tail_probability=0.1,
        dataset_digest=dataset_digest,
        run_digest="c" * 64,
        source_commit=source_commit,
    )
    rules = {
        "version": "1",
        "margin_tiers": [
            {
                "tier": 1,
                "notional_floor": "0",
                "notional_cap": "100000",
                "maximum_leverage": "5",
                "maintenance_margin_rate": "0.01",
                "maintenance_amount": "0",
            }
        ],
        "quantity_increment": "0.001",
        "price_increment": "0.5",
        "maximum_mark_deviation": "0.02",
        "maximum_abs_funding_rate": "0.01",
        "minimum_liquidation_buffer": "100",
    }
    position = {
        "quantity": "1.000",
        "mark_price": "50000",
        "index_price": "50000",
        "entry_price": "49000.0",
        "requested_leverage": "2",
        "side": "long",
        "collateral": "30000",
        "accrued_funding": "10",
        "fee_reserve": "20",
    }
    rule = venue_rule_receipt(
        stress_receipt=stress,
        rule_pack=rules,
        position=position,
        dataset_digest=dataset_digest,
        source_commit=source_commit,
    )
    receipts.extend([stress, rule])
    if len(receipts) != 15 or not all(verify_receipt(item) for item in receipts):
        raise RuntimeError("native receipt chain is incomplete or invalid")
    report = {
        "schema_version": "quantitative-native-producer-pilot-v1.0.0",
        "source_commit": source_commit,
        "success": True,
        "capital_or_order_authority": False,
        "milestones": [item.milestone for item in receipts],
        "receipts": [item.as_dict() for item in receipts],
    }
    report["report_digest"] = digest(report)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "success": True,
                "receipt_count": len(receipts),
                "report_digest": report["report_digest"],
                "output": str(args.output),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
