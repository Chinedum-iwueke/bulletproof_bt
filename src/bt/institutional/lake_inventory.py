"""Content-bound full-lake accounting, distinct from execution admission."""

from __future__ import annotations

import hashlib
import os
import stat
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq
import pyarrow as pa

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


def _sha256(handle) -> str:
    result = hashlib.sha256()
    for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
        result.update(block)
    return result.hexdigest()


def _open_beneath(root: Path, relative: Path):
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in relative.parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory)
            directory = child
        descriptor = os.open(relative.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            os.close(descriptor)
            raise ValueError("non_regular_object")
        return os.fdopen(descriptor, "rb")
    finally:
        os.close(directory)


def _bounds(metadata) -> dict:
    names = metadata.schema_arrow.names
    if "ts" not in names:
        return {"observed_start": None, "observed_end": None, "bounds_source": "unavailable"}
    timestamp_type = metadata.schema_arrow.field("ts").type
    if not pa.types.is_timestamp(timestamp_type) or timestamp_type.tz is None:
        return {"observed_start": None, "observed_end": None, "bounds_source": "unavailable"}
    leaves = [index for index in range(len(metadata.schema)) if metadata.schema.column(index).path == "ts"]
    if len(leaves) != 1:
        return {"observed_start": None, "observed_end": None, "bounds_source": "unavailable"}
    index = leaves[0]
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
        "timestamp_type": str(timestamp_type),
    }


def _ordered_paths(directory: Path):
    def walk_error(error):
        raise error
    walker = os.walk(directory, followlinks=False, onerror=walk_error)
    try:
        try:
            parent, directories, files = next(walker)
        except StopIteration:
            return
    finally:
        walker.close()
    for name in sorted(directories + files, key=lambda name: name + "/" if name in directories and not (Path(parent) / name).is_symlink() else name):
        path = Path(parent) / name
        if name in directories and not path.is_symlink():
            yield from _ordered_paths(path)
        else:
            yield path


def iter_lake_inventory(*, data_root: Path, checkpoint=None):
    """Yield accounted objects with bounded memory and deterministic traversal."""
    root = data_root.resolve(strict=True)
    for layer in ("canonical", "manifests", "raw"):
        directory = root / layer
        if directory.is_symlink():
            raise ValueError("lake_layer_must_not_be_symlink")
        if not directory.exists():
            continue
        for path in _ordered_paths(directory):
            relative = path.relative_to(root)
            item = {"partition_id": relative.as_posix(), "execution_eligible": False, "reason_codes": []}
            if path.is_symlink():
                item.update(disposition="quarantined", reason_codes=["symbolic_link"])
                yield item
                continue
            try:
                with _open_beneath(root, relative) as handle:
                    before = os.fstat(handle.fileno())
                    cached = checkpoint.get(relative, before) if checkpoint else None
                    if cached is not None:
                        after = os.fstat(handle.fileno())
                        current = path.lstat()
                        if (before == after and current.st_ino == before.st_ino
                                and current.st_dev == before.st_dev):
                            yield cached
                            continue
                    item.update(byte_size=before.st_size, content_digest=_sha256(handle))
                    try:
                        item.update(_identity(relative))
                    except ValueError as exc:
                        item["reason_codes"].append(str(exc))
                    if path.suffix == ".parquet":
                        handle.seek(0)
                        metadata = pq.ParquetFile(handle)
                        item.update(row_count=metadata.metadata.num_rows, output_columns=metadata.schema_arrow.names)
                        item.update(_bounds(metadata))
                    else:
                        item["reason_codes"].append("non_parquet_requires_dataset_adapter")
                    after = os.fstat(handle.fileno())
                if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                    item["reason_codes"].append("file_changed_during_inventory")
                if path.lstat().st_ino != before.st_ino:
                    item["reason_codes"].append("path_replaced_during_inventory")
                item["disposition"] = "quarantined" if item["reason_codes"] else "cataloged_pending_quality"
                if checkpoint and not item["reason_codes"]:
                    checkpoint.put(relative, after, item)
            except (OSError, ValueError, TypeError) as exc:
                item.update(disposition="quarantined")
                item["reason_codes"].append("unreadable_object:" + type(exc).__name__)
            yield item


def full_lake_inventory_receipt(*, data_root: Path, source_commit: str, progress=None) -> ProducerReceipt:
    objects = []
    for item in iter_lake_inventory(data_root=data_root):
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


def sharded_lake_inventory_receipt(*, data_root: Path, source_commit: str,
                                  run_id: str, emit_shard, shard_size=10000,
                                  progress=None, checkpoint=None) -> ProducerReceipt:
    """Retain complete accounting without a million-object in-memory envelope."""
    if not 1 <= shard_size <= 10000 or not run_id:
        raise ValueError("inventory requires a bounded shard size and run binding")
    pending, shards = [], []
    dispositions = Counter()
    assets = set()
    count = 0

    def flush():
        if not pending:
            return
        objects = sorted(pending, key=lambda item: item["partition_id"])
        result = {
            "schema_version": "data002-lake-inventory-shard-v1.0.0",
            "run_id": run_id, "shard_index": len(shards),
            "objects": objects, "object_count": len(objects),
            "dispositions": dict(Counter(item["disposition"] for item in objects)),
            "assets": sorted({(item["market"], item["venue"], item["instrument"])
                              for item in objects if "instrument" in item}),
            "claim_boundary": "Partial content accounting only; not a complete inventory or execution admission.",
        }
        if checkpoint:
            result["claim_boundary"] = "Partial catalog accounting may reuse prior byte digests under unchanged local filesystem fingerprints. No fresh cryptographic byte verification or execution admission is inferred."
        receipt = build_receipt(
            milestone="DATA-002", producer="bt.institutional.lake_inventory.lake_inventory_shard_receipt",
            producer_version="1.0.0", source_commit=source_commit,
            inputs=objects, dataset_digest=digest(objects),
            configuration={"run_id": run_id, "shard_index": len(shards)},
            artifacts={"shard_digest": digest(result)}, result=result,
        )
        emit_shard(receipt)
        shards.append({"shard_index": len(shards), "object_count": len(objects),
                       "receipt_digest": receipt.receipt_digest, "dataset_digest": receipt.dataset_digest})
        pending.clear()

    for item in iter_lake_inventory(data_root=data_root, checkpoint=checkpoint):
        pending.append(item)
        count += 1
        dispositions[item["disposition"]] += 1
        if "instrument" in item:
            assets.add((item["market"], item["venue"], item["instrument"]))
        if progress:
            progress(count, item["partition_id"])
        if len(pending) == shard_size:
            flush()
    flush()
    result = {
        "schema_version": "data002-full-lake-inventory-v2.0.0",
        "run_id": run_id, "shards": shards, "shard_count": len(shards),
        "object_count": count, "dispositions": dict(dispositions),
        "assets": sorted(assets), "group_labels_are_optional_metadata": True,
        "claim_boundary": "Complete content accounting through all bound shards. No contiguous coverage, point-in-time identity, quality, basket eligibility or execution authority is inferred.",
    }
    if checkpoint:
        result["checkpoint"] = {
            "reused_objects": checkpoint.reused, "validated_objects": checkpoint.written,
            "claim_boundary": "Prior byte digests reused only under unchanged local filesystem fingerprints. This is not fresh cryptographic byte verification or execution admission; selected execution inputs require independent content validation.",
        }
        result["claim_boundary"] = result["checkpoint"]["claim_boundary"]
    return build_receipt(
        milestone="DATA-002", producer="bt.institutional.lake_inventory.full_lake_inventory_receipt",
        producer_version="2.0.0", source_commit=source_commit,
        inputs=shards, dataset_digest=digest([item["dataset_digest"] for item in shards]),
        configuration={"run_id": run_id, "shard_size": shard_size, "execution_admission": False},
        artifacts={"inventory_digest": digest(result)}, result=result,
    )
