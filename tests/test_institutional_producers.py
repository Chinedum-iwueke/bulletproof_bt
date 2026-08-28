from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from bt.institutional.data import (
    DataProducerError,
    lake_quality_receipt,
    market_catalog_receipt,
    reference_snapshot_receipt,
)
from bt.institutional.discovery import (
    DiscoveryProducerError,
    factor_program_receipt,
    opportunity_map_receipt,
    search_proposal_receipt,
    selection_audit_receipt,
    symbolic_candidate_receipt,
)
from bt.institutional.ml import (
    MLProducerError,
    calibration_receipt,
    causal_materialization_receipt,
    model_family_evaluation_receipt,
)
from bt.institutional.receipt import digest, verify_receipt
from bt.institutional.risk import (
    RiskProducerError,
    stress_dossier_receipt,
    venue_rule_receipt,
)
from bt.institutional.rl import (
    RLProducerError,
    off_policy_evaluation_receipt,
    offline_dataset_receipt,
)

SHA = "a" * 64


def reference():
    return reference_snapshot_receipt(
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
        source_commit=SHA,
    )


def factor():
    return factor_program_receipt(
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
        dataset_digest=SHA,
        source_commit=SHA,
    )


def test_data_producer_chain(tmp_path: Path):
    payload = b"canonical partition"
    content = digest({"bytes_hex": payload.hex()})
    ref = reference()
    catalog = market_catalog_receipt(
        partitions=[
            {
                "partition_id": "part-1",
                "venue_id": "bybit",
                "listing_id": "btc-perp",
                "row_count": 10,
                "duplicate_count": 0,
                "observed_start": "2026-01-01T00:00:00Z",
                "observed_end": "2026-01-02T00:00:00Z",
                "available_at": "2026-01-02T00:00:01Z",
                "content_digest": content,
            }
        ],
        reference_receipt=ref,
        source_commit=SHA,
    )
    path = tmp_path / "part-1"
    path.write_bytes(payload)
    quality = lake_quality_receipt(
        catalog_receipt=catalog, files=[path], source_commit=SHA
    )
    assert all(verify_receipt(item) for item in (ref, catalog, quality))
    assert quality.result["admissible"] is True


def test_reference_snapshot_rejects_ambiguity():
    rows = [
        {
            "venue_id": "v",
            "listing_id": "x",
            "available_at": "2026-01-01T00:00:00Z",
            "effective_from": "2026-01-01T00:00:00Z",
            "effective_to": None,
        }
    ] * 2
    with pytest.raises(DataProducerError, match="ambiguous"):
        reference_snapshot_receipt(
            records=rows, as_of="2026-02-01T00:00:00Z", source_commit=SHA
        )


def test_discovery_producer_chain():
    program = factor()
    search = search_proposal_receipt(
        program_receipt=program,
        observations=[],
        method="random",
        seed=7,
        budget=2,
        source_commit=SHA,
    )
    symbolic = symbolic_candidate_receipt(
        base_program=program,
        candidates=[
            {"operators": ["add"], "expression": "x"},
            {"operators": ["network"], "expression": "bad"},
        ],
        allowed_operators={"add"},
        maximum_nodes=3,
        source_commit=SHA,
    )
    audit = selection_audit_receipt(
        trials=[
            {"trial_digest": "1" * 64, "p_value": 0.01, "validation_rank": 1},
            {"trial_digest": "2" * 64, "p_value": 0.5, "validation_rank": 2},
        ],
        alpha=0.05,
        dataset_digest=SHA,
        source_commit=SHA,
    )
    effect = opportunity_map_receipt(
        baseline=np.zeros(30),
        candidate=np.full(30, 0.03),
        costs=np.full(30, 0.005),
        dataset_digest=SHA,
        source_commit=SHA,
    )
    assert len(search.result["proposals"]) == 2
    assert len(symbolic.result["accepted"]) == len(symbolic.result["rejected"]) == 1
    assert audit.result["bonferroni_p_value"] == 0.02
    assert effect.result["qualified"] is True


def test_factor_rejects_current_bar_leakage():
    specification = {
        "fields": {"return": {"unit": "dimensionless", "availability_lag": 0}},
        "factors": {
            "bad": {
                "expression": {"op": "field", "args": ["return", 0]},
                "output_unit": "dimensionless",
            }
        },
        "parameters": {},
        "maximum_trials": 1,
    }
    with pytest.raises(DiscoveryProducerError, match="availability"):
        factor_program_receipt(
            specification=specification, dataset_digest=SHA, source_commit=SHA
        )


def test_ml_producer_chain():
    program = factor()
    features = np.arange(80, dtype=float).reshape(40, 2) / 80
    labels = np.asarray([0, 1] * 20)
    material = causal_materialization_receipt(
        timestamps=[f"t-{i}" for i in range(40)],
        features=features,
        labels=labels,
        feature_lags=[1, 2],
        folds=[
            {"train_start": 0, "train_end": 15, "test_start": 18, "test_end": 22},
            {"train_start": 0, "train_end": 22, "test_start": 25, "test_end": 30},
        ],
        purge=2,
        embargo=2,
        dataset_digest=SHA,
        factor_receipt=program,
        source_commit=SHA,
    )
    baseline = np.full(40, 0.5)
    candidate = np.where(labels == 1, 0.8, 0.2)
    evaluation = model_family_evaluation_receipt(
        materialization_receipt=material,
        labels=labels,
        predictions={"linear_baseline": baseline, "candidate": candidate},
        regimes=["a"] * 20 + ["b"] * 20,
        baseline_families={"linear_baseline"},
        minimum_increment=0.1,
        dataset_digest=SHA,
        source_commit=SHA,
    )
    calibration = calibration_receipt(
        evaluation_receipt=evaluation,
        family="candidate",
        labels=labels,
        probabilities=candidate,
        bins=5,
        minimum_confidence=0.6,
        maximum_ece=0.25,
        dataset_digest=SHA,
        source_commit=SHA,
    )
    assert evaluation.result["qualified_families"] == ["candidate"]
    assert calibration.result["qualified"] is True


def test_ml_rejects_unlagged_features():
    with pytest.raises(MLProducerError, match="lagged"):
        causal_materialization_receipt(
            timestamps=["a", "b"],
            features=np.ones((2, 1)),
            labels=np.asarray([0, 1]),
            feature_lags=[0],
            folds=[],
            purge=1,
            embargo=1,
            dataset_digest=SHA,
            factor_receipt=factor(),
            source_commit=SHA,
        )


def transitions():
    return [
        {
            "episode_id": f"e-{i // 4}",
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


def test_rl_producer_chain():
    dataset = offline_dataset_receipt(
        transitions=transitions(),
        allowed_actions={"hold", "trade"},
        dataset_digest=SHA,
        shadow_receipt_digest="b" * 64,
        source_commit=SHA,
    )
    result = off_policy_evaluation_receipt(
        dataset_receipt=dataset,
        rewards=np.full(40, 0.02),
        behavior_probabilities=np.full(40, 0.5),
        target_probabilities=np.full(40, 0.5),
        direct_values=np.full(40, 0.018),
        confidence_z=1.96,
        maximum_weight=5,
        minimum_effective_sample_size=20,
        source_commit=SHA,
    )
    assert result.result["shadow_eligible"] is True


def test_rl_rejects_unsupported_action():
    with pytest.raises(RLProducerError, match="unknown action"):
        offline_dataset_receipt(
            transitions=[{**transitions()[0], "action": "borrow"}],
            allowed_actions={"hold"},
            dataset_digest=SHA,
            shadow_receipt_digest="b" * 64,
            source_commit=SHA,
        )


def stress():
    scenarios = {
        name: [-0.01, -0.01, 0.005]
        for name in (
            "price_gap",
            "correlation_break",
            "liquidity_freeze",
            "model_failure",
            "prolonged_drawdown",
        )
    }
    return stress_dossier_receipt(
        returns=np.asarray([0.002, -0.001] * 20),
        scenarios=scenarios,
        scenario_limit=0.1,
        tail_probability=0.1,
        dataset_digest=SHA,
        run_digest="c" * 64,
        source_commit=SHA,
    )


def test_risk_producer_chain():
    dossier = stress()
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
    receipt = venue_rule_receipt(
        stress_receipt=dossier,
        rule_pack=rules,
        position=position,
        dataset_digest=SHA,
        source_commit=SHA,
    )
    assert dossier.result["admissible"] is True
    assert receipt.result["allowed"] is True


def test_risk_rejects_incomplete_scenario_pack():
    with pytest.raises(RiskProducerError, match="scenario pack"):
        stress_dossier_receipt(
            returns=np.ones(30) * 0.001,
            scenarios={},
            scenario_limit=0.1,
            tail_probability=0.1,
            dataset_digest=SHA,
            run_digest=SHA,
            source_commit=SHA,
        )
