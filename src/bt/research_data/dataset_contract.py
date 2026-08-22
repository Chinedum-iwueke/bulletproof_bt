"""Immutable, point-in-time dataset snapshot contracts.

The builder reads bounded Parquet inputs and writes only a JSON manifest. It never
repairs, normalizes, or otherwise mutates the source lake.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import pyarrow.parquet as pq

from bt.contracts.canonical_identity import SCHEMA_VERSION as IDENTITY_SCHEMA_VERSION
from bt.contracts.canonical_identity import validate_identity
from bt.research_data.time import timeframe_delta

DATASET_SCHEMA_VERSION = "immutable-dataset-snapshot-v1.0.0"
VALIDATION_SCHEMA_VERSION = "dataset-validation-report-v1.0.0"
_NAMESPACE = uuid.UUID("44cb7509-598b-4fd0-b359-69d32b89ef41")


class DatasetContractError(ValueError):
    """The dataset snapshot cannot support point-in-time research."""


@dataclass(frozen=True)
class SnapshotRequest:
    dataset_family: str
    source: str
    market: str
    exchange: str
    timeframe: str
    timestamp_semantics: str
    availability_lag_seconds: int
    knowledge_cutoff: datetime
    access_classification: str = "internal-research"


def _canonical_json(document: Any) -> bytes:
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def _digest(document: Any) -> str:
    return hashlib.sha256(_canonical_json(document)).hexdigest()


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _utc(value: Any, label: str) -> datetime:
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None:
        raise DatasetContractError(f"{label} must be timezone-aware")
    return parsed.tz_convert("UTC").to_pydatetime()


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _available_at(event_time: datetime, request: SnapshotRequest) -> datetime:
    lag = timedelta(seconds=request.availability_lag_seconds)
    if request.timestamp_semantics == "bar_open":
        return event_time + timeframe_delta(request.timeframe).to_pytimedelta() + lag
    if request.timestamp_semantics == "bar_close":
        return event_time + lag
    raise DatasetContractError("timestamp_semantics must be bar_open or bar_close")


def _partition(path: Path, source_root: Path, request: SnapshotRequest) -> tuple[dict[str, Any], pd.DataFrame]:
    if request.availability_lag_seconds < 0:
        raise DatasetContractError("availability_lag_seconds cannot be negative")
    resolved = path.resolve(strict=True)
    root = source_root.resolve(strict=True)
    try:
        relative = resolved.relative_to(root)
    except ValueError as exc:
        raise DatasetContractError(f"partition leaves source root: {path}") from exc
    metadata = pq.read_metadata(resolved)
    available = set(metadata.schema.names)
    required = {"ts", "exchange", "symbol"}
    if not required.issubset(available):
        raise DatasetContractError(f"partition missing columns: {sorted(required - available)}")
    columns = [name for name in ("ts", "exchange", "symbol", "available_at") if name in available]
    frame = pd.read_parquet(resolved, columns=columns)
    if frame.empty:
        raise DatasetContractError(f"empty partition is not snapshot-eligible: {relative}")
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True, errors="raise")
    if "available_at" in frame:
        frame["available_at"] = pd.to_datetime(frame["available_at"], utc=True, errors="raise")
    else:
        frame["available_at"] = frame["ts"].map(lambda value: _available_at(value.to_pydatetime(), request))
    earliest_availability = frame["ts"].map(
        lambda value: _available_at(value.to_pydatetime(), request)
    )
    if (frame["available_at"] < earliest_availability).any():
        raise DatasetContractError("available_at predates the declared bar-close availability boundary")
    ordered = frame.sort_values(["exchange", "symbol", "ts"])
    step = timeframe_delta(request.timeframe)
    gaps = int(
        ordered.groupby(["exchange", "symbol"])["ts"]
        .diff()
        .gt(step)
        .sum()
    )
    return {
        "relative_path": relative.as_posix(),
        "content_digest": _file_digest(resolved),
        "byte_size": resolved.stat().st_size,
        "row_count": len(frame),
        "columns": metadata.schema.names,
        "min_event_time": _iso(_utc(frame["ts"].min(), "min event time")),
        "max_event_time": _iso(_utc(frame["ts"].max(), "max event time")),
        "max_available_at": _iso(_utc(frame["available_at"].max(), "max available_at")),
        "quality": {
            "duplicate_key_count": int(frame.duplicated(["exchange", "symbol", "ts"]).sum()),
            "gap_interval_count": gaps,
        },
    }, frame


def build_snapshot_manifest(
    paths: Iterable[Path],
    *,
    source_root: Path,
    request: SnapshotRequest,
    membership: list[dict[str, Any]],
    corrections: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic manifest for explicit, bounded Parquet inputs."""
    cutoff = _utc(request.knowledge_cutoff, "knowledge_cutoff")
    partitions_and_frames = [_partition(Path(path), source_root, request) for path in sorted(map(Path, paths))]
    if not partitions_and_frames:
        raise DatasetContractError("at least one partition is required")
    partitions = [item[0] for item in partitions_and_frames]
    combined = pd.concat([item[1] for item in partitions_and_frames], ignore_index=True)
    content = {
        "dataset_family": request.dataset_family,
        "source": request.source,
        "market": request.market,
        "exchange": request.exchange,
        "timeframe": request.timeframe,
        "knowledge_cutoff": _iso(cutoff),
        "partitions": partitions,
        "clock": {
            "timezone": "UTC",
            "event_time_column": "ts",
            "timestamp_semantics": request.timestamp_semantics,
        },
        "availability_policy": {
            "kind": "recorded_or_deterministic_close_lag",
            "available_at_column": "available_at",
            "lag_seconds": request.availability_lag_seconds,
            "missing_observation_policy": "remain_missing",
        },
        "membership": membership,
        "correction_policy": "append_only_successor",
        "corrections": corrections or [],
        "access_classification": request.access_classification,
        "provenance": {"source_root_alias": "research-data-lake", "source_mutated": False},
    }
    dataset_digest = _digest(content)
    object_id = str(uuid.uuid5(_NAMESPACE, dataset_digest))
    identity = {
        "schema_version": IDENTITY_SCHEMA_VERSION,
        "object_id": object_id,
        "object_type": "dataset-snapshot",
        "content_version": "1",
        "content_digest": dataset_digest,
        "producer": {
            "system": "bulletproof-bt",
            "native_type": "dataset-snapshot",
            "native_id": object_id,
            "schema_version": DATASET_SCHEMA_VERSION,
        },
        "aliases": [{"namespace": "bulletproof-bt", "object_type": "dataset-snapshot", "value": object_id}],
        "supersedes_object_id": None,
    }
    manifest = {"schema_version": DATASET_SCHEMA_VERSION, "identity": identity, **content}
    manifest["manifest_digest"] = _digest(manifest)
    validate_snapshot_manifest(manifest, frames=combined)
    return manifest


def validate_snapshot_manifest(
    manifest: dict[str, Any], *, frames: pd.DataFrame | None = None, source_root: Path | None = None
) -> dict[str, Any]:
    """Validate identity, digests, clocks, membership and optional source replay."""
    errors: list[str] = []
    if manifest.get("schema_version") != DATASET_SCHEMA_VERSION:
        errors.append("unsupported schema_version")
    try:
        validate_identity(manifest["identity"])
    except (KeyError, ValueError) as exc:
        errors.append(f"identity: {exc}")
    expected_manifest_digest = _digest({key: value for key, value in manifest.items() if key != "manifest_digest"})
    if manifest.get("manifest_digest") != expected_manifest_digest:
        errors.append("manifest digest mismatch")
    content_keys = [key for key in manifest if key not in {"schema_version", "identity", "manifest_digest"}]
    expected_dataset_digest = _digest({key: manifest[key] for key in content_keys})
    if manifest.get("identity", {}).get("content_digest") != expected_dataset_digest:
        errors.append("dataset content digest mismatch")
    if manifest.get("clock", {}).get("timezone") != "UTC":
        errors.append("clock timezone must be UTC")
    cutoff = _utc(manifest["knowledge_cutoff"], "knowledge_cutoff")
    for member in manifest.get("membership", []):
        try:
            known = _utc(member["known_at"], "membership known_at")
            valid_from = _utc(member["valid_from"], "membership valid_from")
            valid_to = _utc(member["valid_to"], "membership valid_to") if member.get("valid_to") else None
            if known > valid_from or (valid_to is not None and valid_to <= valid_from):
                errors.append("membership is not point-in-time valid")
        except (KeyError, ValueError, TypeError) as exc:
            errors.append(f"membership: {exc}")
    seen_corrections: set[str] = set()
    for correction in manifest.get("corrections", []):
        correction_id = str(correction.get("correction_id", ""))
        if not correction_id or correction_id in seen_corrections:
            errors.append("correction IDs must be non-empty and unique")
        seen_corrections.add(correction_id)
        if correction.get("prior_content_digest") == correction.get("replacement_content_digest"):
            errors.append("correction must replace different content")
        if _utc(correction["known_at"], "correction known_at") > cutoff:
            errors.append("correction was unknown at snapshot cutoff")
    if frames is not None:
        keys = [column for column in ("exchange", "symbol", "ts") if column in frames]
        if frames.duplicated(keys).any():
            errors.append("duplicate observation keys")
        available = pd.to_datetime(frames["available_at"], utc=True, errors="raise")
        if (available > pd.Timestamp(cutoff)).any():
            errors.append("snapshot contains observations unavailable at knowledge cutoff")
        for (exchange, symbol), group in frames.groupby(["exchange", "symbol"]):
            event_times = pd.to_datetime(group["ts"], utc=True)
            eligible = False
            for member in manifest.get("membership", []):
                if member.get("exchange") != exchange or member.get("symbol") != symbol:
                    continue
                start = pd.Timestamp(member["valid_from"])
                end = pd.Timestamp(member["valid_to"]) if member.get("valid_to") else pd.Timestamp(cutoff)
                if event_times.ge(start).all() and event_times.le(end).all():
                    eligible = True
                    break
            if not eligible:
                errors.append(f"observations lack point-in-time membership: {exchange}/{symbol}")
    if source_root is not None:
        root = source_root.resolve(strict=True)
        for partition in manifest.get("partitions", []):
            path = (root / partition["relative_path"]).resolve(strict=True)
            try:
                path.relative_to(root)
            except ValueError:
                errors.append("partition leaves source root")
                continue
            if _file_digest(path) != partition["content_digest"]:
                errors.append(f"partition digest mismatch: {partition['relative_path']}")
    if errors:
        raise DatasetContractError("; ".join(errors))
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "status": "valid",
        "snapshot_id": manifest["identity"]["object_id"],
        "dataset_digest": manifest["identity"]["content_digest"],
        "manifest_digest": manifest["manifest_digest"],
        "partition_count": len(manifest["partitions"]),
        "row_count": sum(int(item["row_count"]) for item in manifest["partitions"]),
        "source_replayed": source_root is not None,
    }


def write_manifest(manifest: dict[str, Any], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(_canonical_json(manifest) + b"\n")
