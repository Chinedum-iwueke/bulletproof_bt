"""Manifest helpers for generalized hypothesis-parallel experiments."""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

MANIFEST_COLUMNS = [
    "row_id",
    "hypothesis_id",
    "hypothesis_path",
    "phase",
    "tier",
    "variant_id",
    "config_hash",
    "params_json",
    "run_slug",
    "output_dir",
    "expected_status",
    "enabled",
    "notes",
]


def encode_params(params: dict[str, Any]) -> str:
    return json.dumps(params, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def decode_params(params_json: str) -> dict[str, Any]:
    payload = json.loads(params_json)
    if not isinstance(payload, dict):
        raise ValueError("params_json must decode to an object")
    return payload


def validate_manifest_row(row: dict[str, str]) -> None:
    missing = [c for c in MANIFEST_COLUMNS if c not in row]
    if missing:
        raise ValueError(f"Manifest row missing columns {missing}: {row}")

    required_non_empty = [
        "row_id",
        "hypothesis_id",
        "hypothesis_path",
        "phase",
        "tier",
        "variant_id",
        "config_hash",
        "params_json",
        "run_slug",
        "output_dir",
        "expected_status",
        "enabled",
    ]
    empty = [c for c in required_non_empty if not str(row.get(c, "")).strip()]
    if empty:
        raise ValueError(f"Manifest row has empty required fields {empty}: {row}")

    if row["expected_status"] not in {"pending", "completed"}:
        raise ValueError(f"Unsupported expected_status={row['expected_status']!r}")
    if row["enabled"] not in {"true", "false"}:
        raise ValueError(f"enabled must be 'true' or 'false', got {row['enabled']!r}")
    if row["phase"] not in {"tier2", "tier3", "validate"}:
        raise ValueError(f"Unsupported phase={row['phase']!r}")
    if row["tier"] not in {"Tier1", "Tier2", "Tier3"}:
        raise ValueError(f"Unsupported tier={row['tier']!r}")

    decode_params(row["params_json"])


def write_manifest_csv(rows: list[dict[str, str]], manifest_path: Path) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_COLUMNS)
        writer.writeheader()
        for row in rows:
            validate_manifest_row(row)
            writer.writerow({c: row.get(c, "") for c in MANIFEST_COLUMNS})


def read_manifest_csv(manifest_path: Path) -> list[dict[str, str]]:
    if not manifest_path.exists():
        raise ValueError(f"Manifest path does not exist: {manifest_path}")
    with manifest_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [dict(row) for row in reader]
    for row in rows:
        validate_manifest_row(row)
    return rows
