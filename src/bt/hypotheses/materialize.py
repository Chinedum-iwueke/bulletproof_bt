"""Deterministic hypothesis parameter-grid materialization."""
from __future__ import annotations

import hashlib
import itertools
import json
from typing import Any

from bt.hypotheses.exceptions import GridMaterializationError


def canonical_json_hash(payload: dict[str, Any]) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def materialize_grid(parameter_grid: dict[str, tuple[Any, ...]], *, max_variants: int | None = None) -> list[dict[str, Any]]:
    if not parameter_grid:
        raise GridMaterializationError("parameter_grid must be non-empty")

    keys = sorted(parameter_grid.keys())
    for key in keys:
        values = parameter_grid[key]
        if not isinstance(values, tuple) or len(values) == 0:
            raise GridMaterializationError(f"grid field '{key}' must be a non-empty sequence")

    variants: list[dict[str, Any]] = []
    for idx, combo in enumerate(itertools.product(*(parameter_grid[k] for k in keys))):
        params = dict(zip(keys, combo, strict=True))
        config_hash = canonical_json_hash(params)
        variants.append({
            "grid_id": f"g{idx:05d}",
            "config_hash": config_hash,
            "params": params,
        })

    if max_variants is not None and len(variants) > max_variants:
        raise GridMaterializationError(
            f"materialized {len(variants)} variants exceeds runtime.max_variants={max_variants}"
        )
    return variants
