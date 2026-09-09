"""ALPHA-001 admission for immutable, locally retained exchange history."""

from __future__ import annotations

from datetime import UTC, datetime
import hashlib
from pathlib import Path
from typing import Any

import pyarrow.compute as pc
import pyarrow.parquet as pq

from .receipt import ProducerReceipt, build_receipt, digest


class AlphaAdmissionError(ValueError):
    """A dataset cannot enter an autonomous real-data research campaign."""


REQUIRED_FIELDS = {
    "ts",
    "exchange",
    "symbol",
    "canonical_symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
}


def _sha256_file(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def _manifest_rows(path: Path, *, venue: str, instrument: str, timeframe: str) -> list[dict[str, Any]]:
    if not path.is_file():
        raise AlphaAdmissionError(f"required acquisition manifest is missing: {path.name}")
    table = pq.read_table(path)
    mask = pc.and_(
        pc.and_(pc.equal(table["exchange"], venue), pc.equal(table["symbol"], instrument)),
        pc.equal(table["timeframe"], timeframe),
    )
    rows = table.filter(mask).to_pylist()
    if not rows:
        raise AlphaAdmissionError(f"{path.name} has no matching acquisition record")
    return sorted(
        ({key: _json_value(value) for key, value in row.items()} for row in rows),
        key=lambda item: str(item.get("dataset", "")),
    )


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if hasattr(value, "item"):
        return value.item()
    return value


def real_data_admission_receipt(
    *,
    data_root: Path,
    panel_path: Path,
    venue: str,
    instrument: str,
    timeframe: str,
    source_commit: str,
) -> ProducerReceipt:
    root = data_root.expanduser().resolve(strict=True)
    panel = panel_path.expanduser().resolve(strict=True)
    try:
        relative = panel.relative_to(root)
    except ValueError as exc:
        raise AlphaAdmissionError("panel must resolve beneath the configured data root") from exc
    expected = Path("canonical") / "perp" / venue / instrument / f"timeframe={timeframe}" / "research_panel.parquet"
    if relative != expected:
        raise AlphaAdmissionError(f"panel does not match canonical exchange layout: expected {expected}")
    metadata = pq.ParquetFile(panel)
    columns = set(metadata.schema_arrow.names)
    missing = sorted(REQUIRED_FIELDS - columns)
    if missing:
        raise AlphaAdmissionError(f"panel lacks required fields: {missing}")
    identity = pq.read_table(panel, columns=["ts", "exchange", "symbol", "canonical_symbol"])
    if identity.num_rows < 2:
        raise AlphaAdmissionError("panel must contain at least two observations")
    timestamps = identity["ts"].combine_chunks()
    if timestamps.null_count:
        raise AlphaAdmissionError("panel timestamps contain nulls")
    values = timestamps.to_numpy(zero_copy_only=False)
    duplicate_count = int(len(values) - len(set(values.tolist())))
    monotonic = bool((values[1:] > values[:-1]).all())
    exchanges = set(identity["exchange"].combine_chunks().to_pylist())
    symbols = set(identity["symbol"].combine_chunks().to_pylist())
    canonical_symbols = set(identity["canonical_symbol"].combine_chunks().to_pylist())
    if exchanges != {venue} or symbols != {instrument} or not canonical_symbols:
        raise AlphaAdmissionError("panel identity columns do not match its canonical path")
    coverage_path = root / "manifests" / "coverage.parquet"
    fetch_path = root / "manifests" / "fetch_state.parquet"
    coverage = _manifest_rows(coverage_path, venue=venue, instrument=instrument, timeframe=timeframe)
    fetch_state = _manifest_rows(fetch_path, venue=venue, instrument=instrument, timeframe=timeframe)
    fetch_failures = [item for item in fetch_state if item.get("status") != "success"]
    panel_digest = _sha256_file(panel)
    checks = {
        "canonical_path": True,
        "required_fields": True,
        "identity_consistent": True,
        "timestamps_strictly_increasing": monotonic,
        "duplicate_timestamps": duplicate_count == 0,
        "coverage_manifest_bound": bool(coverage),
        "fetch_manifest_bound": bool(fetch_state),
        "fetch_records_successful": not fetch_failures,
    }
    result = {
        "schema_version": "alpha001-real-data-admission-v1.0.0",
        "evidence_class": "live_exchange_history",
        "admitted": all(checks.values()),
        "venue": venue,
        "instrument": instrument,
        "canonical_symbols": sorted(canonical_symbols),
        "market": "perp",
        "timeframe": timeframe,
        "row_count": metadata.metadata.num_rows,
        "first_timestamp": _json_value(timestamps[0].as_py()),
        "last_timestamp": _json_value(timestamps[-1].as_py()),
        "panel_sha256": panel_digest,
        "schema_digest": digest(sorted(metadata.schema_arrow.names)),
        "checks": checks,
        "coverage_records": coverage,
        "fetch_records": fetch_state,
        "manifest_digests": {
            "coverage": _sha256_file(coverage_path),
            "fetch_state": _sha256_file(fetch_path),
        },
        "claim_boundary": (
            "The receipt proves the content and internal lineage of locally retained exchange-labelled "
            "history. It does not claim cryptographic attestation by the exchange or grant order authority."
        ),
    }
    if not result["admitted"]:
        failed = sorted(key for key, passed in checks.items() if not passed)
        raise AlphaAdmissionError(f"real-data admission failed: {failed}")
    return build_receipt(
        milestone="ALPHA-001",
        producer="bt.institutional.alpha.real_data_admission_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={
            "panel": str(relative),
            "panel_sha256": panel_digest,
            "coverage_manifest": result["manifest_digests"]["coverage"],
            "fetch_manifest": result["manifest_digests"]["fetch_state"],
        },
        dataset_digest=panel_digest,
        configuration={
            "venue": venue,
            "instrument": instrument,
            "timeframe": timeframe,
            "required_fields": sorted(REQUIRED_FIELDS),
            "fail_closed": True,
        },
        artifacts=result["manifest_digests"],
        result=result,
    )
