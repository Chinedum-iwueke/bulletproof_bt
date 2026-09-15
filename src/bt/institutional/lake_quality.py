"""Window-specific panel quality over a content-bound complete lake inventory."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
import os
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from bt.data.resample import timeframe_minutes

from .alpha import REQUIRED_FIELDS
from .lake_inventory import _open_beneath, _sha256
from .receipt import ProducerReceipt, build_receipt, digest, verify_receipt


def _clock(value: str) -> datetime:
    result = datetime.fromisoformat(value)
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("quality window clocks must be timezone-aware")
    return result.astimezone(UTC)


def _panel_quality(handle, item: dict, start: datetime, end: datetime) -> dict:
    parquet = pq.ParquetFile(handle)
    missing = sorted(REQUIRED_FIELDS - set(parquet.schema_arrow.names))
    if missing:
        return {"reason_codes": ["missing_required_fields"], "missing_fields": missing}
    timestamp_type = parquet.schema_arrow.field("ts").type
    if not pa.types.is_timestamp(timestamp_type) or timestamp_type.tz is None:
        return {"reason_codes": ["timestamp_type_not_timezone_aware"]}
    step = timeframe_minutes(item["timeframe"]) * 60 * 1_000_000_000
    start_ns = int(start.timestamp()) * 1_000_000_000
    end_ns = int(end.timestamp()) * 1_000_000_000
    if start_ns % step or end_ns % step:
        raise ValueError("quality window must align to the native timeframe grid")
    rows = duplicates = disorder = gaps = invalid_prices = identity_errors = null_timestamps = off_grid = 0
    previous = first = last = None
    fields = sorted(REQUIRED_FIELDS)
    nulls = Counter()
    for batch in parquet.iter_batches(batch_size=65536, columns=fields):
        table = pa.Table.from_batches([batch])
        timestamps = pc.cast(pc.cast(table["ts"], pa.timestamp("ns", tz="UTC")), pa.int64())
        null_timestamps += timestamps.null_count
        mask = pc.and_(pc.greater_equal(timestamps, start_ns), pc.less(timestamps, end_ns))
        window = table.filter(pc.fill_null(mask, False))
        if not window.num_rows:
            continue
        values = pc.cast(pc.cast(window["ts"], pa.timestamp("ns", tz="UTC")), pa.int64()).to_numpy()
        deltas = np.diff(values if previous is None else np.concatenate(([previous], values)))
        duplicates += int(np.count_nonzero(deltas == 0))
        disorder += int(np.count_nonzero(deltas < 0))
        positive = deltas[deltas > step]
        gaps += int(np.sum((positive - 1) // step))
        off_grid += int(np.count_nonzero(values % step))
        first = int(values[0]) if first is None else first
        last = previous = int(values[-1])
        rows += window.num_rows
        for name in fields:
            nulls[name] += window[name].null_count
        for name, expected in (("exchange", item["venue"]), ("symbol", item["instrument"])):
            valid = pc.fill_null(pc.equal(window[name], expected), False)
            identity_errors += window.num_rows - int(pc.sum(pc.cast(valid, pa.int64())).as_py())
        canonical = window["canonical_symbol"].to_pylist()
        identity_errors += sum(not isinstance(value, str) or not value for value in canonical)
        numbers = {name: np.asarray(window[name].to_numpy(), dtype=float)
                   for name in ("open", "high", "low", "close", "volume")}
        valid = np.ones(window.num_rows, dtype=bool)
        for name, values in numbers.items():
            valid &= np.isfinite(values) & (values >= 0 if name == "volume" else values > 0)
        valid &= numbers["high"] >= np.maximum.reduce([numbers["open"], numbers["low"], numbers["close"]])
        valid &= numbers["low"] <= np.minimum.reduce([numbers["open"], numbers["high"], numbers["close"]])
        invalid_prices += int(np.count_nonzero(~valid))
    expected_rows = (end_ns - start_ns) // step
    checks = {
        "nonempty_window": rows > 0,
        "complete_window_grid": rows == expected_rows and first == start_ns and last == end_ns - step,
        "strict_timestamp_order": duplicates == 0 and disorder == 0,
        "no_internal_gaps": gaps == 0,
        "aligned_timestamps": off_grid == 0,
        "no_null_timestamps": null_timestamps == 0,
        "required_fields_nonnull": not any(nulls.values()),
        "path_exchange_symbol_consistent": identity_errors == 0,
        "valid_ohlcv": invalid_prices == 0,
    }
    return {
        "window_rows": rows, "expected_rows": expected_rows,
        "adjacent_duplicate_timestamps": duplicates, "out_of_order_timestamps": disorder,
        "duplicate_count_exact": disorder == 0,
        "missing_internal_grid_slots": gaps, "off_grid_timestamps": off_grid,
        "null_timestamps_in_file": null_timestamps, "window_null_counts": dict(nulls),
        "invalid_ohlcv_rows": invalid_prices, "identity_errors": identity_errors,
        "checks": checks, "reason_codes": sorted(key for key, passed in checks.items() if not passed),
    }


def inventory_objects(inventory: ProducerReceipt, load_shard=None):
    version = inventory.result.get("schema_version")
    if version == "data002-full-lake-inventory-v1.0.0":
        yield from inventory.result["objects"]
        return
    if version != "data002-full-lake-inventory-v2.0.0" or load_shard is None:
        raise ValueError("full-lake quality requires complete inventory shard custody")
    descriptors = inventory.result["shards"]
    if (inventory.result["shard_count"] != len(descriptors)
            or inventory.dataset_digest != digest(descriptors)
            or inventory.input_digest != digest(descriptors)):
        raise ValueError("inventory root shard binding mismatch")
    previous_path = None
    count = 0
    for index, descriptor in enumerate(inventory.result["shards"]):
        shard = load_shard(descriptor)
        if (not verify_receipt(shard) or shard.receipt_digest != descriptor["receipt_digest"]
                or shard.producer != "bt.institutional.lake_inventory.lake_inventory_shard_receipt"
                or shard.milestone != "DATA-002"
                or shard.source_commit != inventory.source_commit
                or shard.result.get("run_id") != inventory.result["run_id"]
                or shard.result.get("shard_index") != index
                or descriptor.get("shard_index") != index
                or shard.dataset_digest != descriptor["dataset_digest"]):
            raise ValueError("inventory shard custody mismatch")
        objects = shard.result["objects"]
        if (len(objects) != descriptor["object_count"] or len(objects) > 10000
                or shard.result["object_count"] != len(objects)
                or shard.dataset_digest != digest(objects) or shard.input_digest != digest(objects)):
            raise ValueError("inventory shard object count mismatch")
        for item in objects:
            path = item["partition_id"]
            if previous_path is not None and path <= previous_path:
                raise ValueError("inventory shard paths overlap")
            previous_path = path
            count += 1
            yield item
    if count != inventory.result["object_count"]:
        raise ValueError("inventory completeness mismatch")


def full_lake_quality_receipt(*, data_root: Path, inventory: ProducerReceipt,
                              window_start: str, window_end: str, source_commit: str,
                              progress=None, load_shard=None) -> ProducerReceipt:
    if not verify_receipt(inventory):
        raise ValueError("inventory receipt lost integrity")
    root = data_root.resolve(strict=True)
    start, end = _clock(window_start), _clock(window_end)
    if start.microsecond or end.microsecond:
        raise ValueError("quality window requires whole-second clocks")
    if end <= start:
        raise ValueError("quality window end must follow start")
    objects = []
    unprocessed = Counter()
    inventory_count = 0
    for original in inventory_objects(inventory, load_shard):
        inventory_count += 1
        item = {key: original[key] for key in ("partition_id", "content_digest", "venue", "instrument", "timeframe", "market") if key in original}
        item.update(execution_eligible=False, panel_quality_passed=False)
        if original["disposition"] == "quarantined":
            unprocessed["inventory_quarantined"] += 1
            continue
        elif original.get("layer") != "canonical" or original.get("dataset") != "research_panel":
            unprocessed["non_panel_adapter_required"] += 1
            continue
        else:
            relative = Path(original["partition_id"])
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("inventory partition path escapes the lake")
            try:
                with _open_beneath(root, relative) as handle:
                    before = os.fstat(handle.fileno())
                    observed_digest = _sha256(handle)
                    if observed_digest != original["content_digest"]:
                        raise ValueError("inventory_content_changed")
                    handle.seek(0)
                    item.update(_panel_quality(handle, original, start, end))
                    after = os.fstat(handle.fileno())
                if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                    item["reason_codes"].append("file_changed_during_quality")
                if (root / relative).lstat().st_ino != before.st_ino:
                    item["reason_codes"].append("path_replaced_during_quality")
                item["panel_quality_passed"] = not item["reason_codes"]
                item["disposition"] = "panel_quality_passed" if item["panel_quality_passed"] else "quarantined"
            except (OSError, ValueError, TypeError) as error:
                item.update(disposition="quarantined", reason_codes=[str(error)])
        objects.append(item)
        if progress:
            progress(len(objects), item["partition_id"])
    result = {
        "schema_version": "data003-full-lake-quality-v1.0.0",
        "inventory_receipt_digest": inventory.receipt_digest,
        "window_start": start.isoformat(), "window_end": end.isoformat(),
        "objects": objects, "object_count": len(objects),
        "inventoried_object_count": inventory_count,
        "unprocessed_dispositions": dict(unprocessed),
        "dispositions": dict(Counter(item["disposition"] for item in objects)),
        "claim_boundary": "Window-specific base OHLCV integrity, exchange/symbol label consistency and grid quality only. Canonical/PIT instrument mapping, auxiliary timing, acquisition lineage, point-in-time universe selection, hypothesis-specific field sufficiency and execution admission require their own gates.",
    }
    return build_receipt(
        milestone="DATA-003", producer="bt.institutional.lake_quality.full_lake_quality_receipt",
        producer_version="1.0.0", source_commit=source_commit,
        inputs={"inventory": inventory.receipt_digest, "window_start": window_start, "window_end": window_end},
        dataset_digest=inventory.dataset_digest,
        configuration={"batch_rows": 65536, "execution_admission": False},
        artifacts={"quality_digest": digest(result)}, result=result,
    )
