from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import numpy as np
import pytest
from jsonschema import Draft202012Validator

from bt.ml import (
    ModelRegistry,
    ModelRegistryError,
    fit_binary_model,
    infer,
    validate_model_bundle,
)


def split_contract() -> dict[str, str]:
    return {
        "train_start": "2025-01-01T00:00:00Z",
        "train_end": "2025-06-30T23:59:59Z",
        "calibration_start": "2025-07-01T00:00:00Z",
        "calibration_end": "2025-09-30T23:59:59Z",
        "evaluation_start": "2025-10-01T00:00:00Z",
        "evaluation_end": "2025-12-31T23:59:59Z",
    }


def split_digest() -> str:
    import hashlib
    import json

    return hashlib.sha256(
        json.dumps(split_contract(), sort_keys=True, separators=(",", ":")).encode(
            "ascii"
        )
    ).hexdigest()


def lineage() -> dict[str, str]:
    return {
        "problem_digest": "1" * 64,
        "dataset_digest": "2" * 64,
        "representation_digest": "3" * 64,
        "split_digest": split_digest(),
        "source_digest": "5" * 64,
    }


def matrices(offset: float = 0.0) -> tuple[np.ndarray, ...]:
    train = (
        np.array(
            [[-2, -1], [-1, -2], [-1, 0], [0, -1], [1, 0], [0, 1], [1, 2], [2, 1]],
            dtype=float,
        )
        + offset
    )
    train_y = np.array([0, 0, 0, 0, 1, 1, 1, 1], dtype=float)
    calibration = (
        np.array([[-1.5, -0.5], [-0.5, -1.5], [0.5, 1.5], [1.5, 0.5]], dtype=float)
        + offset
    )
    calibration_y = np.array([0, 0, 1, 1], dtype=float)
    evaluation = (
        np.array([[-1.2, -0.8], [-0.8, -1.2], [0.8, 1.2], [1.2, 0.8]], dtype=float)
        + offset
    )
    evaluation_y = np.array([0, 0, 1, 1], dtype=float)
    return train, train_y, calibration, calibration_y, evaluation, evaluation_y


def bundle(offset: float = 0.0) -> dict:
    train, train_y, calibration, calibration_y, evaluation, evaluation_y = matrices(
        offset
    )
    return fit_binary_model(
        feature_names=["momentum", "liquidity"],
        train_features=train,
        train_labels=train_y,
        calibration_features=calibration,
        calibration_labels=calibration_y,
        evaluation_features=evaluation,
        evaluation_labels=evaluation_y,
        lineage=lineage(),
        split_contract=split_contract(),
        seed=20260825,
    )


def test_fit_is_seed_environment_and_lineage_reproducible() -> None:
    first = bundle()
    second = bundle()
    assert first == second
    assert first["model_digest"] == second["model_digest"]
    assert first["evaluation"]["calibrated_brier"] <= first["evaluation"]["raw_brier"]
    assert all(
        value == "prohibited"
        for key, value in first["authority"].items()
        if key != "permitted_use"
    )


def test_invalid_lineage_and_corrupt_artifact_fail_closed() -> None:
    train, train_y, calibration, calibration_y, evaluation, evaluation_y = matrices()
    with pytest.raises(ModelRegistryError, match="dataset_digest"):
        fit_binary_model(
            feature_names=["a", "b"],
            train_features=train,
            train_labels=train_y,
            calibration_features=calibration,
            calibration_labels=calibration_y,
            evaluation_features=evaluation,
            evaluation_labels=evaluation_y,
            lineage=lineage() | {"dataset_digest": "latest"},
            split_contract=split_contract(),
            seed=1,
        )
    changed = deepcopy(bundle())
    changed["fit"]["weights"][0] += 1
    with pytest.raises(ModelRegistryError, match="digest mismatch"):
        validate_model_bundle(changed)


def test_overlapping_protected_split_fails_before_fit() -> None:
    train, train_y, calibration, calibration_y, evaluation, evaluation_y = matrices()
    overlap = split_contract() | {"calibration_start": "2025-06-01T00:00:00Z"}
    with pytest.raises(ModelRegistryError, match="disjoint and ordered"):
        fit_binary_model(
            feature_names=["a", "b"],
            train_features=train,
            train_labels=train_y,
            calibration_features=calibration,
            calibration_labels=calibration_y,
            evaluation_features=evaluation,
            evaluation_labels=evaluation_y,
            lineage=lineage(),
            split_contract=overlap,
            seed=1,
        )


def test_inference_is_calibrated_and_abstains_on_shift() -> None:
    model = bundle()
    supported = infer(
        model, {"momentum": 1.0, "liquidity": 1.0}, observed_at="2026-08-25T12:00:00Z"
    )
    shifted = infer(
        model,
        {"momentum": 100.0, "liquidity": 100.0},
        observed_at="2026-08-25T12:00:00Z",
    )
    assert supported["applicability"] == "supported"
    assert supported["calibration_method"] == "held_out_platt"
    assert supported["authority"] == "research_evidence_only"
    assert shifted["abstained"] is True
    assert shifted["abstention_reason"] == "distribution_shift"


def test_registry_is_idempotent_immutable_and_supports_approved_rollback(
    tmp_path: Path,
) -> None:
    registry = ModelRegistry(tmp_path / "registry.sqlite", tmp_path / "artifacts")
    first = bundle()
    second = bundle(offset=0.25)
    one = registry.register(family="fixture-quality", bundle=first)
    assert registry.register(family="fixture-quality", bundle=first) == one
    two = registry.register(family="fixture-quality", bundle=second)
    registry.activate(
        first["model_digest"],
        approved_by="ml-reviewer",
        reason="independent evaluation passed",
    )
    registry.activate(
        second["model_digest"],
        approved_by="ml-reviewer",
        reason="challenger evaluation passed",
    )
    assert registry.state(first["model_digest"]) == "superseded"
    assert registry.state(second["model_digest"]) == "active"
    registry.rollback(
        family="fixture-quality",
        target_digest=first["model_digest"],
        approved_by="ml-reviewer",
        reason="challenger drift",
    )
    assert registry.state(first["model_digest"]) == "active"
    assert registry.state(second["model_digest"]) == "superseded"
    assert two["version"] == 2


def test_inference_rejects_schema_and_clock_drift() -> None:
    model = bundle()
    with pytest.raises(ModelRegistryError, match="schema"):
        infer(model, {"momentum": 1.0}, observed_at="2026-08-25T12:00:00Z")
    with pytest.raises(ModelRegistryError, match="timezone-aware"):
        infer(
            model,
            {"momentum": 1.0, "liquidity": 1.0},
            observed_at="2026-08-25T12:00:00",
        )


def test_public_bundle_and_receipt_schemas_accept_canonical_outputs() -> None:
    model = bundle()
    receipt = infer(
        model,
        {"momentum": 1.0, "liquidity": 1.0},
        observed_at="2026-08-25T12:00:00Z",
    )
    root = Path(__file__).parents[1]
    model_schema = json.loads(
        (root / "schemas/ml-model-bundle-v1.schema.json").read_text(encoding="utf-8")
    )
    receipt_schema = json.loads(
        (root / "schemas/ml-inference-receipt-v1.schema.json").read_text(
            encoding="utf-8"
        )
    )
    Draft202012Validator(model_schema).validate(model)
    Draft202012Validator(receipt_schema).validate(receipt)
