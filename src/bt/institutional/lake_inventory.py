"""Content-bound full-lake accounting, distinct from execution admission."""

from __future__ import annotations

import hashlib
import os
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq

from .receipt import ProducerReceipt, build_receipt, digest


def _identity(relative: Path) -> dict:
    parts = list(relative.parts)
    layer = parts.pop(0)
    if layer == "manifests":
        return {"layer": layer, "dataset": relative.stem}
    if layer not in {"raw", "canonical"}:
        raise ValueError("unknown_layer")
    market = parts.pop(0) if parts and parts[0] in {"perp", "spot"} else "perp"
    if len(parts) < 3 or parts[0] not in {"binance", "bybit"}:
        raise ValueError("unknown_layout")
    venue, instrument = parts[:2]
    timeframe = [part.removeprefix("timeframe=") for part in parts if part.startswith("timeframe=")]
    if len(timeframe) != 1:
        raise ValueError("unknown_timeframe")
    return {
        "layer": layer,
        "market": market,
        "venue": venue,
        "instrument": instrument,
        "timeframe": timeframe[0],
        "dataset": relative.stem if layer == "canonical" else parts[2],
        "legacy_layout": relative.parts[1] not in {"perp", "spot"},
        "chunk": "chunks" in parts,
    }


def _sha256(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def _bounds(metadata) -> dict:
    names = metadata.schema_arrow.names
    if "ts" not in names:
        return {"observed_start": None, "observed_end": None, "bounds_source": "unavailable"}
    index = names.index("ts")
    ranges = []
    for group in range(metadata.metadata.num_row_groups):
        stats = metadata.metadata.row_group(group).column(index).statistics
        if stats is None or not stats.has_min_max:
            return {"observed_start": None, "observed_end": None, "bounds_source": "unavailable"}
        ranges.append((stats.min, stats.max))
    if not ranges:
        return {"observed_start": None, "observed_end": None, "bounds_source": "unavailable"}
    def encode(value):
        return value.isoformat() if hasattr(value, "isoformat") else str(value)
    return {
        "observed_start": encode(min(item[0] for item in ranges)),
        "observed_end": encode(max(item[1] for item in ranges)),
        "bounds_source": "parquet_footer_statistics",
    }


def full_lake_inventory_receipt(*, data_root: Path, source_commit: str, progress=None) -> ProducerReceipt:
    root = data_root.resolve(strict=True)
    objects = []
    for layer in ("raw", "canonical", "manifests"):
        directory = root / layer
        if directory.is_symlink():
            raise ValueError("lake_layer_must_not_be_symlink")
        if not directory.exists():
            continue
        for parent, directories, files in os.walk(directory, followlinks=False):
            for name in list(directories):
                candidate = Path(parent) / name
                if candidate.is_symlink():
                    files.append(name)
                    directories.remove(name)
            for name in sorted(files):
                path = Path(parent) / name
                relative = path.relative_to(root)
                item = {"partition_id": relative.as_posix(), "execution_eligible": False, "reason_codes": []}
                if path.is_symlink():
                    item.update(disposition="quarantined", reason_codes=["symbolic_link"])
                    objects.append(item)
                    continue
                try:
                    before = path.stat()
                    item.update(byte_size=before.st_size, content_digest=_sha256(path))
                    try:
                        item.update(_identity(relative))
                    except ValueError as exc:
                        item["reason_codes"].append(str(exc))
                    if path.suffix == ".parquet":
                        metadata = pq.ParquetFile(path)
                        item.update(row_count=metadata.metadata.num_rows, output_columns=metadata.schema_arrow.names)
                        item.update(_bounds(metadata))
                    else:
                        item["reason_codes"].append("non_parquet_requires_dataset_adapter")
                    after = path.stat()
                    if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
                        item["reason_codes"].append("file_changed_during_inventory")
                    item["disposition"] = "quarantined" if item["reason_codes"] else "cataloged_pending_quality"
                except (OSError, ValueError, TypeError) as exc:
                    item.update(disposition="quarantined")
                    item["reason_codes"].append("unreadable_object:" + type(exc).__name__)
                objects.append(item)
                if progress:
                    progress(len(objects), item["partition_id"])
    objects.sort(key=lambda item: item["partition_id"])
    result = {
        "schema_version": "data002-full-lake-inventory-v1.0.0",
        "objects": objects,
        "object_count": len(objects),
        "dispositions": dict(Counter(item["disposition"] for item in objects)),
        "assets": sorted({(item["market"], item["venue"], item["instrument"]) for item in objects if "instrument" in item}),
        "group_labels_are_optional_metadata": True,
        "claim_boundary": "Content and footer accounting only. No contiguous coverage, point-in-time identity, quality, basket eligibility or execution authority is inferred.",
    }
    return build_receipt(
        milestone="DATA-002", producer="bt.institutional.lake_inventory.full_lake_inventory_receipt",
        producer_version="1.0.0", source_commit=source_commit,
        inputs=objects, dataset_digest=digest(objects),
        configuration={"layers": ["raw", "canonical", "manifests"], "execution_admission": False},
        artifacts={"inventory_digest": digest(result)}, result=result,
    )
