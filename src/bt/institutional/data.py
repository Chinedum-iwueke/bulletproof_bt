"""DATA-001..003 native point-in-time data and lake evidence producers."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .receipt import ProducerReceipt, build_receipt, digest


class DataProducerError(ValueError):
    """Native data evidence violates point-in-time or integrity requirements."""


def _time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DataProducerError("timestamps must be timezone-aware")
    return parsed.astimezone(UTC)


def reference_snapshot_receipt(
    *, records: Iterable[dict[str, Any]], as_of: str, source_commit: str
) -> ProducerReceipt:
    cutoff = _time(as_of)
    normalized: list[dict[str, Any]] = []
    active_keys: set[tuple[str, str]] = set()
    for item in records:
        available = _time(str(item["available_at"]))
        effective = _time(str(item["effective_from"]))
        effective_to = (
            _time(str(item["effective_to"])) if item.get("effective_to") else None
        )
        if available > cutoff:
            continue
        key = (str(item["venue_id"]), str(item["listing_id"]))
        if effective <= cutoff and (effective_to is None or cutoff < effective_to):
            if key in active_keys:
                raise DataProducerError("ambiguous active listing at as_of")
            active_keys.add(key)
        normalized.append(
            {
                **item,
                "available_at": available.isoformat().replace("+00:00", "Z"),
                "effective_from": effective.isoformat().replace("+00:00", "Z"),
                "effective_to": effective_to.isoformat().replace("+00:00", "Z")
                if effective_to
                else None,
            }
        )
    normalized.sort(
        key=lambda item: (item["venue_id"], item["listing_id"], item["effective_from"])
    )
    result = {
        "schema_version": "data001-reference-snapshot-v1.0.0",
        "as_of": cutoff.isoformat().replace("+00:00", "Z"),
        "records": normalized,
        "record_count": len(normalized),
        "active_listing_count": len(active_keys),
    }
    dataset_digest = digest(normalized)
    return build_receipt(
        milestone="DATA-001",
        producer="bt.institutional.data.reference_snapshot_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"records": normalized, "as_of": as_of},
        dataset_digest=dataset_digest,
        configuration={"knowledge_cutoff": as_of},
        artifacts={"snapshot_digest": digest(result)},
        result=result,
    )


def market_catalog_receipt(
    *,
    partitions: Iterable[dict[str, Any]],
    reference_receipt: ProducerReceipt,
    source_commit: str,
) -> ProducerReceipt:
    if reference_receipt.milestone != "DATA-001":
        raise DataProducerError("DATA-002 requires a DATA-001 producer receipt")
    known = {
        (item["venue_id"], item["listing_id"])
        for item in reference_receipt.result["records"]
    }
    normalized: list[dict[str, Any]] = []
    identities: set[str] = set()
    for item in partitions:
        key = (str(item["venue_id"]), str(item["listing_id"]))
        if key not in known:
            raise DataProducerError(
                "partition references unknown point-in-time identity"
            )
        partition_id = str(item["partition_id"])
        if partition_id in identities:
            raise DataProducerError("duplicate partition identity")
        identities.add(partition_id)
        if int(item["row_count"]) < 0 or int(item.get("duplicate_count", 0)):
            raise DataProducerError("partition has invalid row or duplicate counts")
        observed_start = _time(str(item["observed_start"]))
        observed_end = _time(str(item["observed_end"]))
        available_at = _time(str(item["available_at"]))
        if observed_end < observed_start or available_at < observed_end:
            raise DataProducerError("partition violates event/availability time")
        normalized.append({**item, "partition_id": partition_id})
    normalized.sort(key=lambda item: item["partition_id"])
    result = {
        "schema_version": "data002-market-catalog-v1.0.0",
        "reference_receipt_digest": reference_receipt.receipt_digest,
        "partitions": normalized,
        "partition_count": len(normalized),
        "row_count": sum(int(item["row_count"]) for item in normalized),
    }
    return build_receipt(
        milestone="DATA-002",
        producer="bt.institutional.data.market_catalog_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs=normalized,
        dataset_digest=digest(normalized),
        configuration={"reference_receipt_digest": reference_receipt.receipt_digest},
        artifacts={"catalog_digest": digest(result)},
        result=result,
    )


def lake_quality_receipt(
    *, catalog_receipt: ProducerReceipt, files: Iterable[Path], source_commit: str
) -> ProducerReceipt:
    if catalog_receipt.milestone != "DATA-002":
        raise DataProducerError("DATA-003 requires a DATA-002 producer receipt")
    expected = {
        item["partition_id"]: item for item in catalog_receipt.result["partitions"]
    }
    observed: list[dict[str, Any]] = []
    names = Counter(path.name for path in files)
    for path in sorted(files, key=lambda item: item.name):
        if path.name not in expected:
            raise DataProducerError("unregistered lake object")
        payload = path.read_bytes()
        observed.append(
            {
                "partition_id": path.name,
                "byte_size": len(payload),
                "content_digest": digest({"bytes_hex": payload.hex()}),
                "registered_content_digest": expected[path.name]["content_digest"],
                "integrity_ok": digest({"bytes_hex": payload.hex()})
                == expected[path.name]["content_digest"],
            }
        )
    failures = sorted(
        [name for name, count in names.items() if count != 1]
        + [name for name in expected if name not in names]
        + [item["partition_id"] for item in observed if not item["integrity_ok"]]
    )
    result = {
        "schema_version": "data003-lake-quality-v1.0.0",
        "catalog_receipt_digest": catalog_receipt.receipt_digest,
        "objects": observed,
        "failures": sorted(set(failures)),
        "admissible": not failures,
    }
    return build_receipt(
        milestone="DATA-003",
        producer="bt.institutional.data.lake_quality_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={
            "catalog": catalog_receipt.receipt_digest,
            "files": [item["partition_id"] for item in observed],
        },
        dataset_digest=catalog_receipt.dataset_digest,
        configuration={"fail_closed": True},
        artifacts=observed,
        result=result,
    )
