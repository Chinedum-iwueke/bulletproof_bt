"""Run the deterministic ML-001 registry, calibration, inference, and rollback pilot."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

import numpy as np

from bt.logging.formatting import write_json_deterministic
from bt.ml import ModelRegistry, fit_binary_model, infer


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def fit(*, source_digest: str, offset: float = 0.0) -> dict:
    train = (
        np.array(
            [[-2, -1], [-1, -2], [-1, 0], [0, -1], [1, 0], [0, 1], [1, 2], [2, 1]],
            dtype=float,
        )
        + offset
    )
    calibration = (
        np.array([[-1.5, -0.5], [-0.5, -1.5], [0.5, 1.5], [1.5, 0.5]], dtype=float)
        + offset
    )
    evaluation = (
        np.array([[-1.2, -0.8], [-0.8, -1.2], [0.8, 1.2], [1.2, 0.8]], dtype=float)
        + offset
    )
    train_y = np.array([0, 0, 0, 0, 1, 1, 1, 1], dtype=float)
    held_y = np.array([0, 0, 1, 1], dtype=float)
    split_contract = {
        "train_start": "2025-01-01T00:00:00Z",
        "train_end": "2025-06-30T23:59:59Z",
        "calibration_start": "2025-07-01T00:00:00Z",
        "calibration_end": "2025-09-30T23:59:59Z",
        "evaluation_start": "2025-10-01T00:00:00Z",
        "evaluation_end": "2025-12-31T23:59:59Z",
    }
    return fit_binary_model(
        feature_names=["momentum", "liquidity"],
        train_features=train,
        train_labels=train_y,
        calibration_features=calibration,
        calibration_labels=held_y,
        evaluation_features=evaluation,
        evaluation_labels=held_y,
        lineage={
            "problem_digest": digest(b"ml001-conditional-probability-fixture-v1"),
            "dataset_digest": digest(b"immutable-point-in-time-fixture-v1"),
            "representation_digest": digest(b"momentum-liquidity-representation-v1"),
            "split_digest": digest(
                json.dumps(
                    split_contract, sort_keys=True, separators=(",", ":")
                ).encode("ascii")
            ),
            "source_digest": source_digest,
        },
        split_contract=split_contract,
        seed=20260825,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    source_digest = digest(commit.encode("ascii"))
    first = fit(source_digest=source_digest)
    reproduced = fit(source_digest=source_digest)
    challenger = fit(source_digest=source_digest, offset=0.25)
    registry_database = args.output / "model-registry.sqlite"
    registry_database.unlink(missing_ok=True)
    registry = ModelRegistry(registry_database, args.output / "models")
    first_registration = registry.register(family="ml001-fixture-quality", bundle=first)
    challenger_registration = registry.register(
        family="ml001-fixture-quality", bundle=challenger
    )
    registry.activate(
        first["model_digest"],
        approved_by="ml001-independent-review",
        reason="baseline evaluation accepted",
    )
    registry.activate(
        challenger["model_digest"],
        approved_by="ml001-independent-review",
        reason="challenger lifecycle exercise",
    )
    registry.rollback(
        family="ml001-fixture-quality",
        target_digest=first["model_digest"],
        approved_by="ml001-independent-review",
        reason="bounded rollback proof",
    )
    supported = infer(
        first, {"momentum": 1.0, "liquidity": 1.0}, observed_at="2026-08-25T12:00:00Z"
    )
    shifted = infer(
        first,
        {"momentum": 100.0, "liquidity": 100.0},
        observed_at="2026-08-25T12:00:01Z",
    )
    registry_snapshot = registry.snapshot(family="ml001-fixture-quality")
    write_json_deterministic(args.output / "registry-snapshot.json", registry_snapshot)
    report = {
        "schema_version": "ml001-pilot-report-v1.0.0",
        "success": first == reproduced
        and registry.state(first["model_digest"]) == "active"
        and shifted["abstained"],
        "source_commit": commit,
        "model_digest": first["model_digest"],
        "reproduced_model_digest": reproduced["model_digest"],
        "registrations": [first_registration, challenger_registration],
        "evaluation": first["evaluation"],
        "supported_inference": supported,
        "shifted_inference": shifted,
        "rollback_restored": first["model_digest"],
        "registry_snapshot_digest": registry_snapshot["snapshot_digest"],
        "capital_or_order_authority": False,
        "production_resources_touched": False,
    }
    report["report_digest"] = digest(
        json.dumps(report, sort_keys=True, separators=(",", ":")).encode("ascii")
    )
    write_json_deterministic(args.output / "ml001-report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
