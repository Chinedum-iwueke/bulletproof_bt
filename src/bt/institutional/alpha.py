"""ALPHA-001 admission for immutable, locally retained exchange history."""

from __future__ import annotations

from datetime import UTC, datetime
import hashlib
from pathlib import Path
import shutil
import tempfile
from typing import Any

import pyarrow.compute as pc
import pyarrow.parquet as pq
import numpy as np

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


def _manifest_rows(
    path: Path, *, venue: str, instrument: str, timeframe: str
) -> list[dict[str, Any]]:
    if not path.is_file():
        raise AlphaAdmissionError(
            f"required acquisition manifest is missing: {path.name}"
        )
    table = pq.read_table(path)
    mask = pc.and_(
        pc.and_(
            pc.equal(table["exchange"], venue), pc.equal(table["symbol"], instrument)
        ),
        pc.equal(table["timeframe"], timeframe),
    )
    rows = table.filter(mask).to_pylist()
    if not rows:
        raise AlphaAdmissionError(f"{path.name} has no matching acquisition record")
    return sorted(
        ({key: _json_value(value) for key, value in row.items()} for row in rows),
        key=lambda item: str(item.get("dataset", "")),
    )


def _instrument_record(path: Path, *, venue: str, instrument: str) -> dict[str, Any]:
    if not path.is_file():
        raise AlphaAdmissionError("required instrument manifest is missing")
    table = pq.read_table(path)
    mask = pc.and_(
        pc.equal(table["exchange"], venue),
        pc.equal(table["native_symbol"], instrument),
    )
    rows = table.filter(mask).to_pylist()
    if len(rows) != 1:
        raise AlphaAdmissionError("instrument identity is missing or ambiguous")
    return {key: _json_value(value) for key, value in rows[0].items()}


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
    backup_root: Path | None = None,
) -> ProducerReceipt:
    root = data_root.expanduser().resolve(strict=True)
    panel = panel_path.expanduser().resolve(strict=True)
    try:
        relative = panel.relative_to(root)
    except ValueError as exc:
        raise AlphaAdmissionError(
            "panel must resolve beneath the configured data root"
        ) from exc
    expected = (
        Path("canonical")
        / "perp"
        / venue
        / instrument
        / f"timeframe={timeframe}"
        / "research_panel.parquet"
    )
    if relative != expected:
        raise AlphaAdmissionError(
            f"panel does not match canonical exchange layout: expected {expected}"
        )
    metadata = pq.ParquetFile(panel)
    columns = set(metadata.schema_arrow.names)
    missing = sorted(REQUIRED_FIELDS - columns)
    if missing:
        raise AlphaAdmissionError(f"panel lacks required fields: {missing}")
    values_table = pq.read_table(panel, columns=sorted(REQUIRED_FIELDS))
    if values_table.num_rows < 2:
        raise AlphaAdmissionError("panel must contain at least two observations")
    null_counts = {
        column: values_table[column].null_count for column in sorted(REQUIRED_FIELDS)
    }
    timestamps = values_table["ts"].combine_chunks()
    values = timestamps.to_numpy(zero_copy_only=False)
    duplicate_count = int(len(values) - len(set(values.tolist())))
    monotonic = bool((values[1:] > values[:-1]).all())
    one_minute = np.timedelta64(1, "m")
    timestamp_deltas = np.diff(values)
    gap_count = int(np.count_nonzero(timestamp_deltas != one_minute))
    out_of_order_timestamp_count = int(
        np.count_nonzero(timestamp_deltas < np.timedelta64(0, "m"))
    )
    missing_bar_count = int(
        sum(
            max(0, int(delta / one_minute) - 1)
            for delta in timestamp_deltas
            if delta > one_minute
        )
    )
    exchanges = set(values_table["exchange"].combine_chunks().to_pylist())
    symbols = set(values_table["symbol"].combine_chunks().to_pylist())
    canonical_symbols = set(
        values_table["canonical_symbol"].combine_chunks().to_pylist()
    )
    if exchanges != {venue} or symbols != {instrument} or not canonical_symbols:
        raise AlphaAdmissionError(
            "panel identity columns do not match its canonical path"
        )
    numeric = {
        column: values_table[column].combine_chunks().to_numpy(zero_copy_only=False)
        for column in ("open", "high", "low", "close", "volume")
    }
    non_finite_value_count = int(
        sum(np.count_nonzero(~np.isfinite(array)) for array in numeric.values())
    )
    non_positive_price_count = int(
        sum(
            np.count_nonzero(numeric[column] <= 0)
            for column in ("open", "high", "low", "close")
        )
    )
    negative_volume_count = int(np.count_nonzero(numeric["volume"] < 0))
    valid_geometry = (
        (numeric["high"] >= numeric["open"])
        & (numeric["high"] >= numeric["close"])
        & (numeric["high"] >= numeric["low"])
        & (numeric["low"] <= numeric["open"])
        & (numeric["low"] <= numeric["close"])
    )
    invalid_ohlc_geometry_count = int(np.count_nonzero(~valid_geometry))
    coverage_path = root / "manifests" / "coverage.parquet"
    fetch_path = root / "manifests" / "fetch_state.parquet"
    instruments_path = root / "manifests" / "instruments.parquet"
    coverage = _manifest_rows(
        coverage_path, venue=venue, instrument=instrument, timeframe=timeframe
    )
    fetch_state = _manifest_rows(
        fetch_path, venue=venue, instrument=instrument, timeframe=timeframe
    )
    instrument_record = _instrument_record(
        instruments_path, venue=venue, instrument=instrument
    )
    fetch_failures = [item for item in fetch_state if item.get("status") != "success"]
    panel_digest = _sha256_file(panel)
    recovery = None
    if backup_root is not None:
        requested_backup = backup_root.expanduser().absolute()
        backup_directory = requested_backup.resolve(strict=False)
        if (
            (requested_backup.exists() and requested_backup.is_symlink())
            or backup_directory != requested_backup
        ):
            raise AlphaAdmissionError(
                "backup root and its existing parents cannot be symbolic links"
            )
        backup_directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        backup = backup_directory / f"{panel_digest}.parquet"
        if backup.exists():
            if backup.is_symlink() or _sha256_file(backup) != panel_digest:
                raise AlphaAdmissionError("existing recovery copy conflicts with panel")
        else:
            if shutil.disk_usage(backup_directory).free < panel.stat().st_size * 2:
                raise AlphaAdmissionError(
                    "insufficient free space for verified recovery copy"
                )
            with tempfile.NamedTemporaryFile(
                dir=backup_directory, delete=False
            ) as stream:
                temporary = Path(stream.name)
            try:
                shutil.copyfile(panel, temporary)
                temporary.chmod(0o400)
                if _sha256_file(temporary) != panel_digest:
                    raise AlphaAdmissionError(
                        "recovery copy failed digest verification"
                    )
                temporary.replace(backup)
            finally:
                temporary.unlink(missing_ok=True)
        recovery = {
            "uri": f"file://{backup}",
            "content_digest": panel_digest,
            "byte_size": backup.stat().st_size,
            "integrity_verified": True,
            "scope": "same-host immutable recovery copy; not disaster recovery",
        }
    checks = {
        "canonical_path": True,
        "required_fields": True,
        "identity_consistent": True,
        "timestamps_strictly_increasing": monotonic,
        "duplicate_timestamps": duplicate_count == 0,
        "complete_one_minute_grid": gap_count == 0,
        "required_fields_nonnull": not any(null_counts.values()),
        "finite_ohlcv": non_finite_value_count == 0,
        "positive_prices": non_positive_price_count == 0,
        "nonnegative_volume": negative_volume_count == 0,
        "valid_ohlc_geometry": invalid_ohlc_geometry_count == 0,
        "coverage_manifest_bound": bool(coverage),
        "fetch_manifest_bound": bool(fetch_state),
        "instrument_manifest_bound": True,
        "fetch_records_successful": not fetch_failures,
    }
    result = {
        "schema_version": "alpha001-real-data-admission-v2.0.0",
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
        "panel_uri": f"file://{panel}",
        "byte_size": panel.stat().st_size,
        "schema_digest": digest(sorted(metadata.schema_arrow.names)),
        "quality": {
            "null_counts": null_counts,
            "duplicate_timestamp_count": duplicate_count,
            "gap_count": gap_count,
            "missing_bar_count": missing_bar_count,
            "out_of_order_timestamp_count": out_of_order_timestamp_count,
            "non_finite_value_count": non_finite_value_count,
            "non_positive_price_count": non_positive_price_count,
            "negative_volume_count": negative_volume_count,
            "invalid_ohlc_geometry_count": invalid_ohlc_geometry_count,
        },
        "instrument_reference": instrument_record,
        "recovery_copy": recovery,
        "checks": checks,
        "coverage_records": coverage,
        "fetch_records": fetch_state,
        "manifest_digests": {
            "coverage": _sha256_file(coverage_path),
            "fetch_state": _sha256_file(fetch_path),
            "instruments": _sha256_file(instruments_path),
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
        producer_version="2.0.0",
        source_commit=source_commit,
        inputs={
            "panel": str(relative),
            "panel_sha256": panel_digest,
            "coverage_manifest": result["manifest_digests"]["coverage"],
            "fetch_manifest": result["manifest_digests"]["fetch_state"],
            "instrument_manifest": result["manifest_digests"]["instruments"],
            **(
                {"recovery_copy": recovery["content_digest"]}
                if recovery is not None
                else {}
            ),
        },
        dataset_digest=panel_digest,
        configuration={
            "venue": venue,
            "instrument": instrument,
            "timeframe": timeframe,
            "required_fields": sorted(REQUIRED_FIELDS),
            "quality_checks": sorted(checks),
            "fail_closed": True,
        },
        artifacts=result["manifest_digests"],
        result=result,
    )
