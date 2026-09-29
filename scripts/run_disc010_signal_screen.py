#!/usr/bin/env python3
"""Run one immutable DISC-010 screen against content-bound native panels."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
from pathlib import Path

import pandas as pd

from bt.institutional.ohlcv_surveillance import ohlcv_signal_surveillance_receipt
from bt.institutional.receipt import digest


def file_sha256(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def read_panel(path: Path) -> pd.DataFrame:
    columns = ["ts", "open", "high", "low", "close", "volume"]
    if path.suffix == ".parquet":
        return pd.read_parquet(path, columns=columns)
    if path.suffix in {".csv", ".gz"}:
        return pd.read_csv(path, usecols=columns)
    raise ValueError(f"unsupported panel format: {path.suffix}")


def atomic_json(path: Path, document: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        os.chmod(temporary, 0o600)
        json.dump(document, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--specification", required=True, type=Path)
    parser.add_argument("--bindings", required=True, type=Path)
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--max-workers", type=int, default=1)
    args = parser.parse_args()

    repository = Path(__file__).resolve().parents[1]
    current_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if current_commit != args.source_commit:
        raise ValueError("source commit differs from the immutable assignment")
    specification = json.loads(args.specification.read_text(encoding="utf-8"))
    bindings = json.loads(args.bindings.read_text(encoding="utf-8"))
    if set(bindings) != {"schema_version", "panels"} or bindings[
        "schema_version"
    ] != "disc010-panel-bindings-v1.0.0":
        raise ValueError("panel binding document is malformed")
    research_root = args.data_root.resolve(strict=True)
    panels: dict[str, pd.DataFrame] = {}
    descriptors = []
    for binding in bindings["panels"]:
        if set(binding) != {"instrument", "path", "sha256"}:
            raise ValueError("panel binding fields are not exact")
        path = Path(binding["path"])
        resolved = path.resolve(strict=True)
        if path.is_symlink() or not resolved.is_relative_to(research_root):
            raise ValueError("panel must be a nonsymlinked file below research_data")
        observed_digest = file_sha256(resolved)
        if observed_digest != binding["sha256"]:
            raise ValueError("panel bytes differ from the immutable binding")
        instrument = binding["instrument"]
        if instrument in panels:
            raise ValueError("panel instrument binding is duplicated")
        panels[instrument] = read_panel(resolved)
        descriptors.append(
            {"instrument": instrument, "path": str(resolved), "sha256": observed_digest}
        )
    if set(panels) != set(specification.get("instruments", [])):
        raise ValueError("panel bindings differ from the frozen basket")
    dataset_digest = digest(sorted(descriptors, key=lambda item: item["instrument"]))
    receipt = ohlcv_signal_surveillance_receipt(
        specification=specification,
        panels=panels,
        dataset_digest=dataset_digest,
        source_commit=args.source_commit,
        max_workers=args.max_workers,
    ).as_dict()
    atomic_json(args.output, receipt)
    print(
        json.dumps(
            {
                "receipt_digest": receipt["receipt_digest"],
                "family_id": receipt["result"]["family_id"],
                "trial_count": receipt["result"]["trial_count"],
                "question_candidates": len(
                    receipt["result"]["question_candidate_digests"]
                ),
                "final_oos_opened": False,
                "authority": receipt["authority"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
