"""JSONL logging utilities."""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from enum import Enum
import json
import math
from pathlib import Path
from typing import Any

import pandas as pd

from bt.orders.side import coerce_side, resolve_order_side, validate_order_side_consistency

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


def _coerce_position_sign(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"buy", "long", "+", "+1", "1", "positive"}:
            return 1
        if normalized in {"sell", "short", "-", "-1", "negative"}:
            return -1
        if normalized in {"0", "flat", "none"}:
            return 0
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric > 0:
        return 1
    if numeric < 0:
        return -1
    return 0


def _validate_exit_reduction_invariant(*, record: dict[str, Any], signed_qty: float | None, where: str) -> None:
    if signed_qty is None:
        return
    signal = record.get("signal")
    metadata = _extract_field(signal, "metadata")
    is_exit_intent = False
    if isinstance(metadata, dict):
        is_exit_intent = bool(metadata.get("is_exit") or metadata.get("reduce_only") or metadata.get("close_only"))
    signal_type = str(_extract_field(signal, "signal_type") or "")
    if signal_type.endswith("_exit"):
        is_exit_intent = True
    if not is_exit_intent:
        return

    position_before = record.get("position_before") if isinstance(record.get("position_before"), dict) else {}
    position_sign = _coerce_position_sign(position_before.get("sign"))
    if position_sign is None or position_sign == 0:
        return

    delta_sign = 1 if float(signed_qty) > 0 else -1
    allows_flip = bool(position_before.get("allow_flip") or (metadata.get("allow_flip") if isinstance(metadata, dict) else False))
    if delta_sign == position_sign and not allows_flip:
        raise ValueError(
            f"{where}: exit intent must reduce exposure (position_before.sign={position_sign}, order_qty={signed_qty})"
        )


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

    normalized_signed_qty = None if signed_qty is None else float(signed_qty)
    validate_order_side_consistency(
        side=side,
        qty=float(qty),
        signed_qty=normalized_signed_qty,
        signal_side=signal_side,
        reduce_only=reduce_only,
        where=where,
    )
    if normalized_signed_qty is not None:
        canonical_side = resolve_order_side(normalized_signed_qty)
        if canonical_side != side:
            raise ValueError(
                f"{where}: canonical side mismatch from order_qty (expected={canonical_side.name}, actual={side.name})"
            )
    _validate_exit_reduction_invariant(record=record, signed_qty=normalized_signed_qty, where=where)

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
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, (str, int, bool)) or obj is None:
        return obj
    return str(obj)


class JsonlWriter:
    def __init__(self, path: Path, *, flush_every: int = 100):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._file = path.open("a", encoding="utf-8")
        self._flush_every = max(int(flush_every), 1)
        self._pending_lines = 0

    def write(self, record: dict[str, Any]) -> None:
        """Append one JSON line."""
        _validate_order_record(record, where=f"JsonlWriter.write[{self._path.name}]")
        json_record = to_jsonable(_with_canonical_fill_costs(record))
        line = json.dumps(json_record, ensure_ascii=False, allow_nan=False)
        self._file.write(line + "\n")
        self._pending_lines += 1
        if self._pending_lines >= self._flush_every:
            self._file.flush()
            self._pending_lines = 0

    def close(self) -> None:
        if not self._file.closed:
            self._file.flush()
            self._file.close()
