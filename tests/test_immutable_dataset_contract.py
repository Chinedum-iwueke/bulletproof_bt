from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import jsonschema
import pandas as pd
import pytest

from bt.research_data.dataset_contract import (
    DatasetContractError,
    SnapshotRequest,
    build_snapshot_manifest,
    validate_snapshot_manifest,
    write_manifest,
)


def _request(**changes) -> SnapshotRequest:
    base = SnapshotRequest(
        dataset_family="ohlcv",
        source="bybit-public-api",
        market="perp",
        exchange="bybit",
        timeframe="1m",
        timestamp_semantics="bar_open",
        availability_lag_seconds=1,
        knowledge_cutoff=datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc),
    )
    return replace(base, **changes)


def _membership(**changes) -> list[dict[str, object]]:
    item = {
        "exchange": "bybit",
        "symbol": "BTCUSDT",
        "valid_from": "2026-01-01T00:00:00Z",
        "valid_to": "2026-01-01T00:10:00Z",
        "known_at": "2025-12-31T23:59:00Z",
        "rule_digest": "a" * 64,
    }
    item.update(changes)
    return [item]


def _write(path: Path, minutes=(0, 1, 2), *, available_at=None) -> Path:
    frame = pd.DataFrame(
        {
            "ts": [pd.Timestamp(f"2026-01-01T00:{minute:02d}:00Z") for minute in minutes],
            "exchange": ["bybit"] * len(minutes),
            "symbol": ["BTCUSDT"] * len(minutes),
            "close": [100.0 + minute for minute in minutes],
        }
    )
    if available_at is not None:
        frame["available_at"] = available_at
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def _build(tmp_path: Path, **kwargs):
    root = tmp_path / "lake"
    path = _write(root / "raw/perp/bybit/BTCUSDT/ohlcv/timeframe=1m/data.parquet")
    return build_snapshot_manifest(
        [path], source_root=root, request=kwargs.get("request", _request()),
        membership=kwargs.get("membership", _membership()), corrections=kwargs.get("corrections"),
    ), root, path


def test_snapshot_is_deterministic_schema_valid_and_replayable(tmp_path) -> None:
    first, root, _ = _build(tmp_path)
    second = build_snapshot_manifest(
        [root / first["partitions"][0]["relative_path"]],
        source_root=root,
        request=_request(),
        membership=_membership(),
    )
    schema = json.loads(Path("schemas/immutable-dataset-snapshot-v1.schema.json").read_text())
    jsonschema.Draft202012Validator(schema).validate(first)
    assert first == second
    report = validate_snapshot_manifest(first, source_root=root)
    assert report["status"] == "valid"
    assert report["row_count"] == 3


def test_duplicate_observations_are_rejected(tmp_path) -> None:
    root = tmp_path / "lake"
    path = _write(root / "duplicate.parquet", minutes=(0, 0))
    with pytest.raises(DatasetContractError, match="duplicate observation"):
        build_snapshot_manifest([path], source_root=root, request=_request(), membership=_membership())


def test_gap_is_explicit_and_missing_policy_forbids_silent_fill(tmp_path) -> None:
    root = tmp_path / "lake"
    path = _write(root / "gap.parquet", minutes=(0, 2))
    manifest = build_snapshot_manifest([path], source_root=root, request=_request(), membership=_membership())
    assert manifest["partitions"][0]["quality"]["gap_interval_count"] == 1
    assert manifest["availability_policy"]["missing_observation_policy"] == "remain_missing"


def test_clock_and_future_availability_are_rejected(tmp_path) -> None:
    root = tmp_path / "lake"
    path = _write(
        root / "future.parquet",
        available_at=[pd.Timestamp("2026-01-02T00:00:00Z")] * 3,
    )
    with pytest.raises(DatasetContractError, match="unavailable at knowledge cutoff"):
        build_snapshot_manifest([path], source_root=root, request=_request(), membership=_membership())
    with pytest.raises(DatasetContractError, match="timestamp_semantics"):
        build_snapshot_manifest(
            [_write(root / "clock.parquet")],
            source_root=root,
            request=_request(timestamp_semantics="unknown"),
            membership=_membership(),
        )
    with pytest.raises(DatasetContractError, match="cannot be negative"):
        build_snapshot_manifest(
            [_write(root / "negative-lag.parquet")],
            source_root=root,
            request=_request(availability_lag_seconds=-1),
            membership=_membership(),
        )


def test_recorded_availability_cannot_claim_an_open_bar_was_known(tmp_path) -> None:
    root = tmp_path / "lake"
    path = _write(
        root / "premature.parquet",
        available_at=[pd.Timestamp(f"2026-01-01T00:{minute:02d}:00Z") for minute in (0, 1, 2)],
    )
    with pytest.raises(DatasetContractError, match="bar-close availability boundary"):
        build_snapshot_manifest([path], source_root=root, request=_request(), membership=_membership())


def test_membership_must_be_known_before_effective_and_cover_rows(tmp_path) -> None:
    with pytest.raises(DatasetContractError, match="point-in-time"):
        _build(tmp_path, membership=_membership(known_at="2026-01-01T00:01:00Z"))
    with pytest.raises(DatasetContractError, match="lack point-in-time membership"):
        _build(tmp_path / "other", membership=_membership(valid_to="2026-01-01T00:01:00Z"))


def test_correction_ledger_is_append_only_and_cutoff_bounded(tmp_path) -> None:
    correction = {
        "correction_id": "vendor-restatement-1",
        "known_at": "2026-01-01T00:03:00Z",
        "prior_content_digest": "b" * 64,
        "replacement_content_digest": "c" * 64,
        "reason": "venue restatement",
    }
    manifest, _, _ = _build(tmp_path, corrections=[correction])
    assert manifest["corrections"] == [correction]
    with pytest.raises(DatasetContractError, match="unknown at snapshot cutoff"):
        _build(tmp_path / "late", corrections=[{**correction, "known_at": "2026-01-02T00:00:00Z"}])


def test_source_lake_is_read_only_and_digest_drift_fails_replay(tmp_path) -> None:
    manifest, root, path = _build(tmp_path)
    before = path.stat()
    destination = tmp_path / "derived/manifests/snapshot.json"
    write_manifest(manifest, destination)
    after = path.stat()
    assert (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)
    _write(path, minutes=(0, 1, 2, 3))
    with pytest.raises(DatasetContractError, match="partition digest mismatch"):
        validate_snapshot_manifest(manifest, source_root=root)
