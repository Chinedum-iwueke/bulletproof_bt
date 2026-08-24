"""Immutable representation, feature, label, and fit-boundary contracts."""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Literal

import pandas as pd


REPRESENTATION_SCHEMA_VERSION = "representation-contract-v1.0.0"
LEAKAGE_REPORT_SCHEMA_VERSION = "representation-leakage-report-v1.0.0"


class RepresentationContractError(ValueError):
    """Representation lineage cannot be certified without weakening causality."""


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _require_digest(name: str, value: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise RepresentationContractError(f"{name} must be lowercase sha256")


def _timestamp(value: str, name: str) -> pd.Timestamp:
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None:
        raise RepresentationContractError(f"{name} must be timezone-aware")
    return parsed.tz_convert("UTC")


@dataclass(frozen=True)
class EvaluationSplit:
    train_start: str
    train_end: str
    validation_start: str
    validation_end: str
    test_start: str
    test_end: str
    fit_start: str
    fit_end: str
    purge_seconds: int
    embargo_seconds: int

    def validate(self) -> None:
        values = {
            name: _timestamp(str(value), f"split.{name}")
            for name, value in asdict(self).items()
            if name not in {"purge_seconds", "embargo_seconds"}
        }
        if not (
            values["train_start"] < values["train_end"]
            <= values["validation_start"] < values["validation_end"]
            <= values["test_start"] < values["test_end"]
        ):
            raise RepresentationContractError("train, validation, and test boundaries overlap or are unordered")
        if not values["train_start"] <= values["fit_start"] <= values["fit_end"] <= values["train_end"]:
            raise RepresentationContractError("fit boundary must remain inside the training split")
        if self.purge_seconds < 0 or self.embargo_seconds < 0:
            raise RepresentationContractError("purge and embargo seconds cannot be negative")
        train_gap = (values["validation_start"] - values["train_end"]).total_seconds()
        test_gap = (values["test_start"] - values["validation_end"]).total_seconds()
        if train_gap < self.purge_seconds:
            raise RepresentationContractError("training/validation gap is smaller than purge_seconds")
        if test_gap < self.embargo_seconds:
            raise RepresentationContractError("validation/test gap is smaller than embargo_seconds")


@dataclass(frozen=True)
class FieldContract:
    name: str
    kind: Literal["representation", "feature", "label"]
    source_columns: tuple[str, ...]
    transformation: str
    transformation_version: str
    implementation_digest: str
    observation_time_column: str
    availability_time_column: str
    warmup_observations: int
    missing_policy: Literal["remain_missing", "drop_row", "error"]
    fit_policy: Literal["stateless", "train_only"]
    fit_artifact_digest: str | None = None
    completeness_column: str | None = None
    label_horizon_seconds: int | None = None

    def validate(self) -> None:
        if not self.name.strip() or not self.transformation.strip() or not self.transformation_version.strip():
            raise RepresentationContractError("field name, transformation, and version are required")
        if not self.source_columns or any(not column.strip() for column in self.source_columns):
            raise RepresentationContractError(f"field {self.name} requires source columns")
        _require_digest(f"field {self.name} implementation_digest", self.implementation_digest)
        if self.kind not in {"representation", "feature", "label"}:
            raise RepresentationContractError(f"field {self.name} has unsupported kind")
        if self.missing_policy not in {"remain_missing", "drop_row", "error"}:
            raise RepresentationContractError(f"field {self.name} has unsupported missing policy")
        if self.fit_policy not in {"stateless", "train_only"}:
            raise RepresentationContractError(f"field {self.name} has unsupported fit policy")
        if self.warmup_observations < 0:
            raise RepresentationContractError(f"field {self.name} warmup cannot be negative")
        if self.fit_policy == "train_only":
            if self.fit_artifact_digest is None:
                raise RepresentationContractError(f"field {self.name} requires a fitted-state digest")
            _require_digest(f"field {self.name} fit_artifact_digest", self.fit_artifact_digest)
        elif self.fit_artifact_digest is not None:
            raise RepresentationContractError(f"stateless field {self.name} cannot bind fitted state")
        if self.kind == "label":
            if self.label_horizon_seconds is None or self.label_horizon_seconds < 1:
                raise RepresentationContractError(f"label {self.name} requires a positive horizon")
        elif self.label_horizon_seconds is not None:
            raise RepresentationContractError(f"non-label field {self.name} cannot declare a label horizon")


@dataclass(frozen=True)
class RepresentationContract:
    contract_id: str
    dataset_snapshot_id: str
    dataset_digest: str
    repository_commit: str
    code_digest: str
    decision_time_column: str
    entity_columns: tuple[str, ...]
    membership_known_at_column: str
    membership_valid_from_column: str
    membership_valid_to_column: str | None
    fields: tuple[FieldContract, ...]
    split: EvaluationSplit

    def validate(self) -> None:
        if not self.contract_id.strip() or not self.dataset_snapshot_id.strip():
            raise RepresentationContractError("contract_id and dataset_snapshot_id are required")
        for name in ("dataset_digest", "code_digest"):
            _require_digest(name, str(getattr(self, name)))
        if len(self.repository_commit) != 40 or any(
            character not in "0123456789abcdef" for character in self.repository_commit
        ):
            raise RepresentationContractError("repository_commit must be lowercase hexadecimal length 40")
        if not self.decision_time_column.strip() or not self.entity_columns:
            raise RepresentationContractError("decision time and entity columns are required")
        if not self.fields:
            raise RepresentationContractError("at least one representation field is required")
        names = [field.name for field in self.fields]
        if len(names) != len(set(names)):
            raise RepresentationContractError("representation field names must be unique")
        for field in self.fields:
            field.validate()
        self.split.validate()

    def document(self) -> dict[str, Any]:
        self.validate()
        document = json.loads(
            _canonical({"schema_version": REPRESENTATION_SCHEMA_VERSION, **asdict(self)}).decode("ascii")
        )
        document["representation_digest"] = _digest(document)
        return document

    @property
    def digest(self) -> str:
        return str(self.document()["representation_digest"])


def validate_representation_document(document: dict[str, Any]) -> None:
    if document.get("schema_version") != REPRESENTATION_SCHEMA_VERSION:
        raise RepresentationContractError("unsupported representation schema version")
    supplied = document.get("representation_digest")
    expected = _digest({key: value for key, value in document.items() if key != "representation_digest"})
    if supplied != expected:
        raise RepresentationContractError("representation digest mismatch")


def _times(frame: pd.DataFrame, column: str, violations: list[dict[str, Any]]) -> pd.Series:
    if column not in frame:
        violations.append({"code": "missing_clock_column", "field": column, "count": len(frame)})
        return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")
    try:
        return pd.to_datetime(frame[column], utc=True, errors="raise")
    except (TypeError, ValueError) as exc:
        violations.append({"code": "invalid_clock", "field": column, "count": len(frame), "detail": str(exc)})
        return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")


def audit_representation_frame(contract: RepresentationContract, frame: pd.DataFrame) -> dict[str, Any]:
    """Replay causal boundaries against a materialized feature/label frame."""
    contract.validate()
    violations: list[dict[str, Any]] = []
    required = {
        contract.decision_time_column,
        contract.membership_known_at_column,
        contract.membership_valid_from_column,
        *contract.entity_columns,
    }
    if contract.membership_valid_to_column:
        required.add(contract.membership_valid_to_column)
    for field in contract.fields:
        required.update(field.source_columns)
        required.update((field.name, field.observation_time_column, field.availability_time_column))
        if field.completeness_column:
            required.add(field.completeness_column)
    missing = sorted(required - set(frame.columns))
    if missing:
        violations.append({"code": "missing_columns", "fields": missing, "count": len(missing)})
        body = {
            "schema_version": LEAKAGE_REPORT_SCHEMA_VERSION,
            "representation_digest": contract.digest,
            "row_count": len(frame),
            "status": "failed",
            "violations": violations,
        }
        body["report_digest"] = _digest(body)
        return body

    identity_columns = [*contract.entity_columns, contract.decision_time_column]
    available_identity = [column for column in identity_columns if column in frame]
    if len(available_identity) == len(identity_columns):
        duplicates = frame.duplicated(identity_columns, keep=False)
        if duplicates.any():
            violations.append({"code": "duplicate_decision_identity", "count": int(duplicates.sum())})

    decision = _times(frame, contract.decision_time_column, violations)
    known = _times(frame, contract.membership_known_at_column, violations)
    valid_from = _times(frame, contract.membership_valid_from_column, violations)
    membership_future = known.gt(decision) | valid_from.gt(decision)
    if membership_future.any():
        violations.append({"code": "universe_leakage", "count": int(membership_future.sum())})
    if contract.membership_valid_to_column and contract.membership_valid_to_column in frame:
        valid_to = pd.to_datetime(frame[contract.membership_valid_to_column], utc=True, errors="coerce")
        expired = valid_to.notna() & valid_to.le(decision)
        if expired.any():
            violations.append({"code": "expired_membership", "count": int(expired.sum())})

    ordered = frame.sort_values([*contract.entity_columns, contract.decision_time_column], kind="mergesort")
    group_key: str | list[str] = (
        contract.entity_columns[0] if len(contract.entity_columns) == 1 else list(contract.entity_columns)
    )
    position = ordered.groupby(group_key, sort=False).cumcount()
    for field in contract.fields:
        if field.name not in frame:
            continue
        observation = _times(frame, field.observation_time_column, violations)
        availability = _times(frame, field.availability_time_column, violations)
        if field.kind != "label":
            future = observation.gt(decision) | availability.gt(decision)
            if future.any():
                violations.append({"code": "future_information", "field": field.name, "count": int(future.sum())})
        else:
            minimum = decision + pd.to_timedelta(field.label_horizon_seconds or 0, unit="s")
            premature = availability.lt(minimum)
            if premature.any():
                violations.append({"code": "premature_label", "field": field.name, "count": int(premature.sum())})
        values = ordered[field.name]
        premature_warmup = position.lt(field.warmup_observations) & values.notna()
        if premature_warmup.any():
            violations.append(
                {"code": "warmup_leakage", "field": field.name, "count": int(premature_warmup.sum())}
            )
        after_warmup_missing = position.ge(field.warmup_observations) & values.isna()
        if field.missing_policy == "error" and after_warmup_missing.any():
            violations.append(
                {"code": "undeclared_missingness", "field": field.name, "count": int(after_warmup_missing.sum())}
            )
        if field.completeness_column and field.completeness_column in ordered:
            incomplete_value = ~ordered[field.completeness_column].fillna(False).astype(bool) & values.notna()
            if incomplete_value.any():
                violations.append(
                    {"code": "incomplete_period_value", "field": field.name, "count": int(incomplete_value.sum())}
                )

    split = contract.split
    split_bounds = {name: _timestamp(value, name) for name, value in asdict(split).items() if name.endswith(("start", "end"))}
    outside = decision.lt(split_bounds["train_start"]) | decision.gt(split_bounds["test_end"])
    if outside.any():
        violations.append({"code": "outside_registered_split", "count": int(outside.sum())})

    body = {
        "schema_version": LEAKAGE_REPORT_SCHEMA_VERSION,
        "representation_digest": contract.digest,
        "row_count": len(frame),
        "status": "failed" if violations else "certified",
        "violations": violations,
    }
    body["report_digest"] = _digest(body)
    return body


def certify_representation_frame(contract: RepresentationContract, frame: pd.DataFrame) -> dict[str, Any]:
    report = audit_representation_frame(contract, frame)
    if report["status"] != "certified":
        codes = ", ".join(str(item["code"]) for item in report["violations"])
        raise RepresentationContractError(f"representation leakage audit failed: {codes}")
    return report
