"""JSONL logging utilities."""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from enum import Enum
import json
from pathlib import Path
from typing import Any

import pandas as pd

from bt.orders.side import coerce_side, validate_order_side_consistency

def _is_fill_record(record: dict[str, Any]) -> bool:
    return "order_id" in record and "qty" in record and "price" in record


def _as_non_negative_float(value: Any, *, field_name: str) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be numeric, got {value!r}") from exc
    if numeric < 0:
        return abs(numeric)
    return numeric


def _with_canonical_fill_costs(record: dict[str, Any]) -> dict[str, Any]:
    if not _is_fill_record(record):
        return record

    metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}

    fee_source = record.get("fee_cost", record.get("fee", record.get("fee_paid", 0.0)))
    slippage_source = record.get(
        "slippage_cost", record.get("slippage", record.get("slip", 0.0))
    )
    spread_source = record.get("spread_cost", metadata.get("spread_cost", 0.0))

    fee_cost = _as_non_negative_float(fee_source, field_name="fee_cost")
    slippage_cost = _as_non_negative_float(slippage_source, field_name="slippage_cost")
    spread_cost = _as_non_negative_float(spread_source, field_name="spread_cost")

    enriched = dict(record)
    enriched["fee_cost"] = fee_cost
    enriched["slippage_cost"] = slippage_cost
    enriched["spread_cost"] = spread_cost
    return enriched




def _extract_field(container: Any, name: str) -> Any:
    if isinstance(container, dict):
        return container.get(name)
    return getattr(container, name, None)


def _validate_order_record(record: dict[str, Any], *, where: str) -> None:
    order_obj = record.get("order")
    if order_obj is None:
        return

    side = coerce_side(_extract_field(order_obj, "side"))
    qty = _extract_field(order_obj, "qty")
    if side is None or qty is None:
        return

    signed_qty = record.get("order_qty")
    signal_side = None
    signal = record.get("signal")
    signal_side = coerce_side(_extract_field(signal, "side"))

    reduce_only = False
    metadata = _extract_field(order_obj, "metadata")
    if isinstance(metadata, dict):
        reduce_only = bool(metadata.get("close_only") or metadata.get("reduce_only"))

    validate_order_side_consistency(
        side=side,
        qty=float(qty),
        signed_qty=None if signed_qty is None else float(signed_qty),
        signal_side=signal_side,
        reduce_only=reduce_only,
        where=where,
    )

def to_jsonable(obj: Any) -> Any:
    """Convert Python objects into JSON-serializable equivalents."""
    if is_dataclass(obj):
        return to_jsonable(asdict(obj))
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if isinstance(obj, Enum):
        return obj.name
    if isinstance(obj, dict):
        return {key: to_jsonable(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [to_jsonable(value) for value in obj]
    if isinstance(obj, (str, float, int, bool)) or obj is None:
        return obj
    return str(obj)


class JsonlWriter:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._file = path.open("a", encoding="utf-8")

    def write(self, record: dict[str, Any]) -> None:
        """Append one JSON line."""
        _validate_order_record(record, where=f"JsonlWriter.write[{self._path.name}]")
        json_record = to_jsonable(_with_canonical_fill_costs(record))
        json.dump(json_record, self._file, ensure_ascii=False)
        self._file.write("\n")
        self._file.flush()

    def close(self) -> None:
        if not self._file.closed:
            self._file.close()
