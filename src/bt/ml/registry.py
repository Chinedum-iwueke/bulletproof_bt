"""ML-001 deterministic fit, calibration, inference, and lifecycle contracts."""

from __future__ import annotations

import hashlib
import json
import math
import os
import platform
import sqlite3
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np

SCHEMA_VERSION = "ml-model-bundle-v1.0.0"
RECEIPT_VERSION = "ml-inference-receipt-v1.0.0"
SHA256_FIELDS = (
    "problem_digest",
    "dataset_digest",
    "representation_digest",
    "split_digest",
    "source_digest",
)


class ModelRegistryError(ValueError):
    """A model artifact or lifecycle transition violates the ML contract."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _is_sha256(value: object) -> bool:
    text = str(value)
    return len(text) == 64 and all(char in "0123456789abcdef" for char in text)


def _sigmoid(value: np.ndarray | float) -> np.ndarray:
    array = np.asarray(value, dtype=np.float64)
    clipped = np.clip(array, -35.0, 35.0)
    return 1.0 / (1.0 + np.exp(-clipped))


def _log_loss(labels: np.ndarray, probabilities: np.ndarray) -> float:
    clipped = np.clip(probabilities, 1e-12, 1.0 - 1e-12)
    return float(
        -np.mean(labels * np.log(clipped) + (1.0 - labels) * np.log(1.0 - clipped))
    )


def _brier(labels: np.ndarray, probabilities: np.ndarray) -> float:
    return float(np.mean(np.square(probabilities - labels)))


def _fit_logistic(
    features: np.ndarray,
    labels: np.ndarray,
    *,
    seed: int,
    steps: int,
    learning_rate: float,
    l2: float,
) -> tuple[np.ndarray, float]:
    if features.ndim != 2 or labels.ndim != 1 or len(features) != len(labels):
        raise ModelRegistryError("features and labels have incompatible shapes")
    if len(features) < 4 or len(np.unique(labels)) != 2:
        raise ModelRegistryError(
            "binary fit requires at least four rows and both labels"
        )
    np.random.default_rng(
        seed
    )  # Seed is part of lineage even though initialization is zero.
    weights = np.zeros(features.shape[1], dtype=np.float64)
    intercept = 0.0
    for _ in range(steps):
        probabilities = _sigmoid(features @ weights + intercept)
        residual = probabilities - labels
        weights -= learning_rate * (
            (features.T @ residual) / len(features) + l2 * weights
        )
        intercept -= learning_rate * float(np.mean(residual))
    return weights, intercept


def _fit_platt(scores: np.ndarray, labels: np.ndarray) -> tuple[float, float]:
    design = scores.reshape(-1, 1)
    weights, intercept = _fit_logistic(
        design, labels, seed=0, steps=600, learning_rate=0.05, l2=1e-6
    )
    return float(weights[0]), float(intercept)


def _environment() -> dict[str, str]:
    return {
        "python": platform.python_version(),
        "numpy": np.__version__,
        "platform": platform.platform(),
        "implementation": sys.implementation.name,
    }


def _validate_split_contract(contract: dict[str, str]) -> dict[str, str]:
    fields = (
        "train_start",
        "train_end",
        "calibration_start",
        "calibration_end",
        "evaluation_start",
        "evaluation_end",
    )
    parsed: dict[str, datetime] = {}
    for field in fields:
        try:
            value = datetime.fromisoformat(str(contract.get(field, "")))
        except ValueError as exc:
            raise ModelRegistryError(f"split {field} must be ISO-8601") from exc
        if value.tzinfo is None or value.utcoffset() is None:
            raise ModelRegistryError(f"split {field} must be timezone-aware")
        parsed[field] = value
    if not (
        parsed["train_start"]
        <= parsed["train_end"]
        < parsed["calibration_start"]
        <= parsed["calibration_end"]
        < parsed["evaluation_start"]
        <= parsed["evaluation_end"]
    ):
        raise ModelRegistryError(
            "split windows must be disjoint and ordered train, calibration, evaluation"
        )
    return {
        field: parsed[field].astimezone(UTC).isoformat().replace("+00:00", "Z")
        for field in fields
    }


def fit_binary_model(
    *,
    feature_names: list[str],
    train_features: np.ndarray,
    train_labels: np.ndarray,
    calibration_features: np.ndarray,
    calibration_labels: np.ndarray,
    evaluation_features: np.ndarray,
    evaluation_labels: np.ndarray,
    lineage: dict[str, str],
    split_contract: dict[str, str],
    seed: int,
    abstention_threshold: float = 0.55,
    shift_z_threshold: float = 5.0,
) -> dict[str, Any]:
    """Fit one deterministic transparent baseline and a held-out Platt calibrator."""
    if not feature_names or len(feature_names) != train_features.shape[1]:
        raise ModelRegistryError("feature_names must match the training matrix")
    if len(set(feature_names)) != len(feature_names):
        raise ModelRegistryError("feature_names must be unique")
    for field in SHA256_FIELDS:
        if not _is_sha256(lineage.get(field)):
            raise ModelRegistryError(f"{field} must be a lowercase sha256")
    normalized_split = _validate_split_contract(split_contract)
    if _digest(normalized_split) != lineage["split_digest"]:
        raise ModelRegistryError("split contract does not match split_digest")
    for name, matrix in (
        ("train", train_features),
        ("calibration", calibration_features),
        ("evaluation", evaluation_features),
    ):
        if (
            matrix.ndim != 2
            or matrix.shape[1] != len(feature_names)
            or not np.isfinite(matrix).all()
        ):
            raise ModelRegistryError(
                f"{name} features violate the finite feature schema"
            )
    means = train_features.mean(axis=0)
    scales = train_features.std(axis=0)
    scales = np.where(scales < 1e-12, 1.0, scales)
    normalized_train = (train_features - means) / scales
    weights, intercept = _fit_logistic(
        normalized_train,
        train_labels.astype(np.float64),
        seed=seed,
        steps=1000,
        learning_rate=0.05,
        l2=1e-4,
    )
    calibration_scores = ((calibration_features - means) / scales) @ weights + intercept
    slope, calibration_intercept = _fit_platt(
        calibration_scores, calibration_labels.astype(np.float64)
    )
    evaluation_scores = ((evaluation_features - means) / scales) @ weights + intercept
    raw = _sigmoid(evaluation_scores)
    calibrated = _sigmoid(slope * evaluation_scores + calibration_intercept)
    support_min = train_features.min(axis=0)
    support_max = train_features.max(axis=0)
    core: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "authority": {
            "alpha": "prohibited",
            "capital": "prohibited",
            "orders": "prohibited",
            "self_promotion": "prohibited",
            "permitted_use": "scientific_estimation_only",
        },
        "lineage": {field: lineage[field] for field in SHA256_FIELDS},
        "split_contract": normalized_split,
        "problem": {
            "role": "conditional_probability_estimation",
            "target": "binary_fixture_outcome",
            "consumer": "research_evaluation",
        },
        "feature_schema": {
            "names": feature_names,
            "means": means.tolist(),
            "scales": scales.tolist(),
            "support_min": support_min.tolist(),
            "support_max": support_max.tolist(),
        },
        "fit": {
            "estimator": "deterministic_logistic_baseline",
            "seed": seed,
            "steps": 1000,
            "learning_rate": 0.05,
            "l2": 1e-4,
            "weights": weights.tolist(),
            "intercept": intercept,
            "train_rows": len(train_features),
            "environment": _environment(),
        },
        "calibration": {
            "method": "held_out_platt",
            "slope": slope,
            "intercept": calibration_intercept,
            "rows": len(calibration_features),
            "uncertainty": "empirical_binary_probability",
        },
        "evaluation": {
            "rows": len(evaluation_features),
            "raw_log_loss": _log_loss(evaluation_labels, raw),
            "calibrated_log_loss": _log_loss(evaluation_labels, calibrated),
            "raw_brier": _brier(evaluation_labels, raw),
            "calibrated_brier": _brier(evaluation_labels, calibrated),
            "positive_rate": float(np.mean(evaluation_labels)),
        },
        "policy": {
            "abstention_threshold": abstention_threshold,
            "shift_z_threshold": shift_z_threshold,
            "on_shift": "abstain",
            "on_invalid": "fail_closed",
        },
    }
    core["model_digest"] = _digest(core)
    return core


def validate_model_bundle(bundle: dict[str, Any]) -> None:
    if bundle.get("schema_version") != SCHEMA_VERSION:
        raise ModelRegistryError("unsupported model bundle schema")
    supplied = bundle.get("model_digest")
    core = {key: value for key, value in bundle.items() if key != "model_digest"}
    if not _is_sha256(supplied) or supplied != _digest(core):
        raise ModelRegistryError("model bundle digest mismatch")
    authority = bundle.get("authority") or {}
    if any(
        authority.get(key) != "prohibited"
        for key in ("alpha", "capital", "orders", "self_promotion")
    ):
        raise ModelRegistryError("model bundle exceeds scientific authority")
    for field in SHA256_FIELDS:
        if not _is_sha256((bundle.get("lineage") or {}).get(field)):
            raise ModelRegistryError(f"invalid lineage field: {field}")
    split_contract = _validate_split_contract(bundle.get("split_contract") or {})
    if _digest(split_contract) != bundle["lineage"]["split_digest"]:
        raise ModelRegistryError("split contract digest mismatch")
    feature = bundle.get("feature_schema") or {}
    names = feature.get("names") or []
    width = len(names)
    arrays = [
        feature.get(key) or []
        for key in ("means", "scales", "support_min", "support_max")
    ]
    weights = (bundle.get("fit") or {}).get("weights") or []
    if (
        not names
        or any(len(values) != width for values in arrays)
        or len(weights) != width
    ):
        raise ModelRegistryError("model feature dimensions disagree")
    numeric = [*weights, *(value for values in arrays for value in values)]
    if not all(math.isfinite(float(value)) for value in numeric):
        raise ModelRegistryError("model bundle contains non-finite values")


def infer(
    bundle: dict[str, Any], features: dict[str, float], *, observed_at: str
) -> dict[str, Any]:
    validate_model_bundle(bundle)
    try:
        timestamp = datetime.fromisoformat(observed_at)
    except ValueError as exc:
        raise ModelRegistryError("observed_at must be ISO-8601") from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ModelRegistryError("observed_at must be timezone-aware")
    schema = bundle["feature_schema"]
    names = list(schema["names"])
    if set(features) != set(names):
        raise ModelRegistryError(
            "inference features do not match the registered schema"
        )
    vector = np.array([features[name] for name in names], dtype=np.float64)
    if not np.isfinite(vector).all():
        raise ModelRegistryError("inference features must be finite")
    means = np.array(schema["means"], dtype=np.float64)
    scales = np.array(schema["scales"], dtype=np.float64)
    z = (vector - means) / scales
    shifted = bool(np.max(np.abs(z)) > float(bundle["policy"]["shift_z_threshold"]))
    score = float(z @ np.array(bundle["fit"]["weights"]) + bundle["fit"]["intercept"])
    probability = float(
        _sigmoid(
            bundle["calibration"]["slope"] * score + bundle["calibration"]["intercept"]
        )
    )
    uncertainty = 1.0 - abs(probability - 0.5) * 2.0
    low_confidence = max(probability, 1.0 - probability) < float(
        bundle["policy"]["abstention_threshold"]
    )
    abstained = shifted or low_confidence
    core = {
        "schema_version": RECEIPT_VERSION,
        "model_digest": bundle["model_digest"],
        "observed_at": observed_at,
        "input_digest": _digest({name: features[name] for name in names}),
        "calibration_method": bundle["calibration"]["method"],
        "probability": probability,
        "uncertainty": uncertainty,
        "applicability": "out_of_support" if shifted else "supported",
        "abstained": abstained,
        "abstention_reason": "distribution_shift"
        if shifted
        else "low_confidence"
        if low_confidence
        else None,
        "authority": "research_evidence_only",
    }
    return core | {"receipt_digest": _digest(core)}


@dataclass
class ModelRegistry:
    database: Path
    artifact_root: Path

    def __post_init__(self) -> None:
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self.artifact_root.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS model_versions (
                  model_digest TEXT PRIMARY KEY, family TEXT NOT NULL, version INTEGER NOT NULL,
                  state TEXT NOT NULL, artifact_path TEXT NOT NULL, created_at TEXT NOT NULL,
                  UNIQUE(family, version)
                );
                CREATE TABLE IF NOT EXISTS model_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT, model_digest TEXT NOT NULL,
                  event_type TEXT NOT NULL, payload_json TEXT NOT NULL, created_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS one_active_model_per_family
                  ON model_versions(family) WHERE state = 'active';
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database)
        connection.row_factory = sqlite3.Row
        return connection

    def register(self, *, family: str, bundle: dict[str, Any]) -> dict[str, Any]:
        validate_model_bundle(bundle)
        digest = bundle["model_digest"]
        destination = self.artifact_root / digest / "model.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(bundle, indent=2, sort_keys=True).encode("ascii") + b"\n"
        if destination.exists() and destination.read_bytes() != encoded:
            raise ModelRegistryError("content-addressed model artifact conflicts")
        if not destination.exists():
            handle, temporary = tempfile.mkstemp(
                dir=destination.parent, prefix=".model-", suffix=".tmp"
            )
            try:
                with os.fdopen(handle, "wb") as stream:
                    stream.write(encoded)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, destination)
            finally:
                Path(temporary).unlink(missing_ok=True)
        now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT * FROM model_versions WHERE model_digest = ?", (digest,)
            ).fetchone()
            if existing is None:
                version = int(
                    connection.execute(
                        "SELECT COALESCE(MAX(version), 0) + 1 FROM model_versions WHERE family = ?",
                        (family,),
                    ).fetchone()[0]
                )
                connection.execute(
                    "INSERT INTO model_versions VALUES (?, ?, ?, 'evaluated', ?, ?)",
                    (digest, family, version, str(destination), now),
                )
                connection.execute(
                    "INSERT INTO model_events(model_digest, event_type, payload_json, created_at) VALUES (?, 'registered', ?, ?)",
                    (
                        digest,
                        json.dumps(
                            {"family": family, "version": version}, sort_keys=True
                        ),
                        now,
                    ),
                )
            else:
                if existing["family"] != family:
                    raise ModelRegistryError(
                        "model digest is already registered to another family"
                    )
                version = int(existing["version"])
        return {
            "model_digest": digest,
            "family": family,
            "version": version,
            "state": self.state(digest),
        }

    def activate(self, model_digest: str, *, approved_by: str, reason: str) -> None:
        if not approved_by.strip() or not reason.strip():
            raise ModelRegistryError("independent approval and reason are required")
        now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT family, state FROM model_versions WHERE model_digest = ?",
                (model_digest,),
            ).fetchone()
            if row is None:
                raise ModelRegistryError("model is not registered")
            connection.execute(
                "UPDATE model_versions SET state = 'superseded' WHERE family = ? AND state = 'active'",
                (row["family"],),
            )
            connection.execute(
                "UPDATE model_versions SET state = 'active' WHERE model_digest = ?",
                (model_digest,),
            )
            connection.execute(
                "INSERT INTO model_events(model_digest, event_type, payload_json, created_at) VALUES (?, 'activated', ?, ?)",
                (
                    model_digest,
                    json.dumps(
                        {"approved_by": approved_by, "reason": reason}, sort_keys=True
                    ),
                    now,
                ),
            )

    def rollback(
        self, *, family: str, target_digest: str, approved_by: str, reason: str
    ) -> None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT family FROM model_versions WHERE model_digest = ?",
                (target_digest,),
            ).fetchone()
        if row is None or row["family"] != family:
            raise ModelRegistryError("rollback target is not registered in this family")
        self.activate(
            target_digest, approved_by=approved_by, reason=f"rollback: {reason}"
        )

    def state(self, model_digest: str) -> str:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT state FROM model_versions WHERE model_digest = ?",
                (model_digest,),
            ).fetchone()
        if row is None:
            raise ModelRegistryError("model is not registered")
        return str(row["state"])

    def snapshot(self, *, family: str) -> dict[str, Any]:
        """Return a deterministic, secret-free projection of versions and events."""
        with self._connect() as connection:
            versions = connection.execute(
                "SELECT model_digest, version, state FROM model_versions WHERE family = ? ORDER BY version",
                (family,),
            ).fetchall()
            events = connection.execute(
                """
                SELECT e.model_digest, e.event_type, e.payload_json
                FROM model_events e JOIN model_versions v USING(model_digest)
                WHERE v.family = ? ORDER BY e.id
                """,
                (family,),
            ).fetchall()
        core = {
            "schema_version": "ml-registry-snapshot-v1.0.0",
            "family": family,
            "versions": [dict(row) for row in versions],
            "events": [
                {
                    "model_digest": row["model_digest"],
                    "event_type": row["event_type"],
                    "payload": json.loads(row["payload_json"]),
                }
                for row in events
            ],
        }
        return core | {"snapshot_digest": _digest(core)}
