"""Manifest-first research-lake visibility without execution admission."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from .receipt import ProducerReceipt, build_receipt, digest


REQUIRED_MANIFESTS = ("coverage", "fetch_state", "instruments")
OPTIONAL_MEMBERSHIPS = ("stable_universe", "volatile_universe_membership")


def _sha256(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def _json(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if hasattr(value, "item"):
        return value.item()
    return value


def _descriptor(path: Path, root: Path) -> dict:
    resolved = path.resolve(strict=True)
    try:
        relative = resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError("lake manifest resolves outside the data root") from exc
    if path.is_symlink() or not path.is_file():
        raise ValueError("lake manifest must be a regular non-symlink file")
    metadata = pq.read_metadata(path)
    return {
        "path": relative.as_posix(),
        "content_digest": _sha256(path),
        "row_count": metadata.num_rows,
        "columns": metadata.schema.names,
    }


def _read(path: Path, columns: list[str]) -> list[dict]:
    table = pq.read_table(path, columns=columns)
    return [
        {key: _json(value) for key, value in row.items()}
        for row in table.to_pylist()
    ]


def manifest_catalog_receipt(
    *,
    data_root: Path,
    source_commit: str,
    venues: tuple[str, ...] = ("binance", "bybit"),
) -> ProducerReceipt:
    """Bind compact native manifests used for discovery, not the underlying bytes."""
    if not venues or len(venues) != len(set(venues)) or any(
        venue not in {"binance", "bybit"} for venue in venues
    ):
        raise ValueError("manifest catalog venues must be unique Binance/Bybit identifiers")
    root = data_root.expanduser().resolve(strict=True)
    manifest_root = root / "manifests"
    paths = {name: manifest_root / f"{name}.parquet" for name in REQUIRED_MANIFESTS}
    missing = [name for name, path in paths.items() if not path.is_file()]
    if missing:
        raise ValueError(f"required lake manifests are missing: {missing}")
    optional = {
        name: manifest_root / f"{name}.parquet"
        for name in OPTIONAL_MEMBERSHIPS
        if (manifest_root / f"{name}.parquet").is_file()
    }
    all_paths = {**paths, **optional}
    manifests = {
        name: _descriptor(path, root)
        for name, path in sorted(all_paths.items())
    }

    coverage_fields = [
        "market", "exchange", "symbol", "dataset", "timeframe", "expected_rows",
        "actual_rows", "missing_rows", "largest_gap_minutes", "first_ts", "last_ts",
    ]
    coverage = [
        row for row in _read(paths["coverage"], coverage_fields)
        if row["exchange"] in venues
    ]
    coverage.sort(
        key=lambda item: (
            str(item["market"]), str(item["exchange"]), str(item["symbol"]),
            str(item["dataset"]), str(item["timeframe"]),
        )
    )
    fetch = [
        row for row in _read(
            paths["fetch_state"],
            ["market", "exchange", "symbol", "dataset", "timeframe", "status", "last_row_count"],
        )
        if row["exchange"] in venues
    ]
    fetch_status = {
        (row["market"], row["exchange"], row["symbol"], row["dataset"], row["timeframe"]): row["status"]
        for row in fetch
    }
    availability = []
    for row in coverage:
        key = (row["market"], row["exchange"], row["symbol"], row["dataset"], row["timeframe"])
        availability.append({
            **row,
            "fetch_status": fetch_status.get(key, "unknown"),
            "execution_eligible": False,
        })
    assets = sorted({
        (str(item["market"]), str(item["exchange"]), str(item["symbol"]))
        for item in availability
        if int(item.get("actual_rows") or 0) > 0
    })
    one_year_coverage_candidates = [
        {
            "market": item["market"],
            "venue": item["exchange"],
            "instrument": item["symbol"],
            "timeframe": item["timeframe"],
            "actual_rows": item["actual_rows"],
            "missing_rows": item["missing_rows"],
            "first_ts": item["first_ts"],
            "last_ts": item["last_ts"],
            "fetch_status": item["fetch_status"],
            "execution_eligible": False,
        }
        for item in availability
        if item["dataset"] == "ohlcv"
        and item["timeframe"] == "1m"
        and int(item.get("actual_rows") or 0) >= 525_600
    ]

    memberships = {}
    for name, path in optional.items():
        descriptor = manifests[name]
        memberships[name] = {
            "content_digest": descriptor["content_digest"],
            "row_count": descriptor["row_count"],
            "columns": descriptor["columns"],
            "classification": "optional_research_metadata",
        }
    result = {
        "schema_version": "data002-manifest-catalog-v1.0.0",
        "manifests": manifests,
        "manifest_count": len(manifests),
        "availability": availability,
        "availability_record_count": len(availability),
        "assets": assets,
        "one_year_coverage_candidates": one_year_coverage_candidates,
        "memberships": memberships,
        "venue_scope": list(venues),
        "group_labels_are_optional_metadata": True,
        "execution_eligible": False,
        "claim_boundary": (
            "Native acquisition manifests establish discovery visibility and reported coverage only. "
            "They do not cryptographically attest underlying panel bytes, establish DATA-003 quality, "
            "or grant execution admission; selected panels require an independent content-bound admission."
        ),
    }
    return build_receipt(
        milestone="DATA-002",
        producer="bt.institutional.lake_manifest.manifest_catalog_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs=manifests,
        dataset_digest=digest(manifests),
        configuration={
            "required_manifests": list(REQUIRED_MANIFESTS),
            "optional_memberships": list(OPTIONAL_MEMBERSHIPS),
            "venues": list(venues),
            "execution_admission": False,
        },
        artifacts={"manifest_catalog_digest": digest(result)},
        result=result,
    )
