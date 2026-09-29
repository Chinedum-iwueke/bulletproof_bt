#!/usr/bin/env python3
"""Keep a bounded queue of native DISC-010 screens from manifest metadata only."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from orchestrator.db import ResearchDB  # noqa: E402


def atomic_json(path: Path, document: dict[str, Any]) -> None:
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


def file_sha256(path: Path, state: dict[str, Any]) -> str:
    cache = state.setdefault("file_digests", {})
    stat = path.stat()
    fingerprint = f"{stat.st_size}:{stat.st_mtime_ns}"
    cached = cache.get(str(path))
    if isinstance(cached, dict) and cached.get("fingerprint") == fingerprint:
        return str(cached["sha256"])
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            hasher.update(chunk)
    observed = hasher.hexdigest()
    cache[str(path)] = {"fingerprint": fingerprint, "sha256": observed}
    return observed


def eligible_assets(data_root: Path) -> list[dict[str, Any]]:
    coverage_path = data_root / "manifests" / "coverage.parquet"
    coverage = pd.read_parquet(coverage_path)
    selected = coverage.loc[
        coverage["market"].eq("perp")
        & coverage["exchange"].eq("bybit")
        & coverage["dataset"].eq("ohlcv")
        & coverage["timeframe"].eq("1m")
        & coverage["actual_rows"].ge(700_000)
        & coverage["missing_rows"].eq(0)
        & coverage["largest_gap_minutes"].le(1)
    ].copy()
    assets = []
    for row in selected.sort_values("symbol", kind="stable").to_dict("records"):
        path = (
            data_root
            / "canonical"
            / "perp"
            / "bybit"
            / str(row["symbol"])
            / "timeframe=1m"
            / "research_panel.parquet"
        )
        if not path.is_file() or path.is_symlink():
            continue
        assets.append(
            {
                "instrument": str(row["symbol"]),
                "path": str(path.resolve()),
                "first_ts": pd.Timestamp(row["first_ts"]).tz_convert("UTC"),
                "last_ts": pd.Timestamp(row["last_ts"]).tz_convert("UTC"),
                "actual_rows": int(row["actual_rows"]),
            }
        )
    return assets


def select_basket(
    assets: list[dict[str, Any]], *, cycle: int, offset: int, size: int = 3
) -> list[dict[str, Any]]:
    if len(assets) < size:
        raise ValueError("at least three quality-visible assets are required")
    ordered = sorted(
        assets,
        key=lambda item: hashlib.sha256(
            f"{cycle}:{offset}:{item['instrument']}".encode("ascii")
        ).hexdigest(),
    )
    for candidate in itertools.combinations(ordered, size):
        basket = list(candidate)
        try:
            research_window(basket)
        except ValueError:
            continue
        return basket
    raise ValueError("no metadata-visible basket has the required pre-OOS overlap")


def research_window(basket: list[dict[str, Any]]) -> dict[str, str]:
    common_start = max(item["first_ts"] for item in basket)
    common_end = min(item["last_ts"] for item in basket) + pd.Timedelta(minutes=1)
    sealed_oos_start = common_end - pd.Timedelta(days=90)
    exploration_end = sealed_oos_start - pd.Timedelta(days=120)
    exploration_start = exploration_end - pd.Timedelta(days=240)
    if exploration_start < common_start:
        raise ValueError("basket lacks the required pre-OOS overlap")
    return {
        "exploration_start": exploration_start.isoformat(),
        "exploration_end": exploration_end.isoformat(),
        "validation_end": sealed_oos_start.isoformat(),
        "sealed_oos_start": sealed_oos_start.isoformat(),
    }


def trial_family(basket: list[dict[str, Any]]) -> list[dict[str, Any]]:
    instruments = [item["instrument"] for item in basket]
    pairs = [
        (instruments[0], instruments[1]),
        (instruments[1], instruments[2]),
        (instruments[2], instruments[0]),
        (instruments[0], instruments[2]),
    ]
    trials = []
    for ordinal, (predictor, target) in enumerate(pairs, start=1):
        for relation in ("same_direction", "opposite_direction"):
            trials.append(
                {
                    "trial_id": f"lead-lag-{ordinal}-{relation}",
                    "predictor_instrument": predictor,
                    "target_instrument": target,
                    "predictor": "log_return",
                    "lookback_bars": ordinal,
                    "target_horizon_bars": 1 if ordinal < 3 else 2,
                    "tail_quantile": 0.9,
                    "relation": relation,
                    "minimum_support": 40,
                    "minimum_effect": 0.00001,
                    "parameters": {},
                }
            )
    return trials


def build_documents(
    *,
    basket: list[dict[str, Any]],
    timeframe: str,
    cycle: int,
    ordinal: int,
    output_root: Path,
    data_root: Path,
    source_commit: str,
    max_workers: int,
    state: dict[str, Any],
) -> tuple[Path, dict[str, Any]]:
    instruments = [item["instrument"] for item in basket]
    identity = hashlib.sha256(
        json.dumps(
            {
                "cycle": cycle,
                "ordinal": ordinal,
                "timeframe": timeframe,
                "basket": instruments,
            },
            sort_keys=True,
        ).encode("ascii")
    ).hexdigest()[:16]
    family_id = f"disc010-cycle-{cycle:06d}-{identity}"
    directory = output_root / family_id
    specification = {
        "schema_version": "disc010-signal-screen-spec-v1.0.0",
        "family_id": family_id,
        "instruments": instruments,
        "source_timeframe": "1m",
        "research_timeframe": timeframe,
        "selection_data_boundary": "metadata_predictors_only_no_targets",
        "outcome_data_consulted_during_selection": False,
        **research_window(basket),
        "alpha": 0.05,
        "permutation_count": 999,
        "seed": cycle * 100 + ordinal,
        "trials": trial_family(basket),
    }
    bindings = {
        "schema_version": "disc010-panel-bindings-v1.0.0",
        "panels": [
            {
                "instrument": item["instrument"],
                "path": item["path"],
                "sha256": file_sha256(Path(item["path"]), state),
            }
            for item in basket
        ],
    }
    specification_path = directory / "specification.json"
    bindings_path = directory / "bindings.json"
    receipt_path = directory / "receipt.json"
    assignment_path = directory / "assignment.json"
    atomic_json(specification_path, specification)
    atomic_json(bindings_path, bindings)
    assignment = {
        "schema_version": "disc010-capacity-assignment-v1.0.0",
        "family_id": family_id,
        "specification": str(specification_path.resolve()),
        "bindings": str(bindings_path.resolve()),
        "data_root": str(data_root.resolve(strict=True)),
        "output": str(receipt_path.resolve()),
        "source_commit": source_commit,
        "max_workers": max_workers,
    }
    atomic_json(assignment_path, assignment)
    return assignment_path, assignment


def active_screen_count(db: ResearchDB) -> int:
    row = (
        db.connect()
        .execute(
            """
        SELECT COUNT(*) AS n FROM queues
        WHERE queue_name = 'approved_backtests'
          AND item_type = 'disc010_signal_screen'
          AND status IN ('PENDING', 'LOCKED')
        """
        )
        .fetchone()
    )
    return int(row["n"])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--target-active", type=int, default=3)
    parser.add_argument("--max-workers", type=int, default=6)
    args = parser.parse_args()
    if not 1 <= args.target_active <= 3 or not 1 <= args.max_workers <= 8:
        raise ValueError("target-active must be 1-3 and max-workers must be 1-8")
    repository = Path(__file__).resolve().parents[1]
    current_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if current_commit != args.source_commit:
        raise ValueError("source commit differs from replenisher assignment")
    state = (
        json.loads(args.state.read_text(encoding="utf-8"))
        if args.state.exists()
        else {"schema_version": "disc010-replenisher-state-v1.0.0", "cycle": 0}
    )
    db = ResearchDB(args.db)
    db.init_schema()
    needed = args.target_active - active_screen_count(db)
    if needed <= 0:
        print(json.dumps({"event": "disc010_queue_sufficient", "queued": 0}))
        db.close()
        return 0
    assets = eligible_assets(args.data_root.resolve(strict=True))
    cycle = int(state.get("cycle", 0)) + 1
    queued = []
    for ordinal in range(needed):
        basket = select_basket(assets, cycle=cycle, offset=ordinal)
        assignment_path, assignment = build_documents(
            basket=basket,
            timeframe=("5m", "15m", "1h")[ordinal % 3],
            cycle=cycle,
            ordinal=ordinal,
            output_root=args.output_root,
            data_root=args.data_root,
            source_commit=args.source_commit,
            max_workers=args.max_workers,
            state=state,
        )
        subprocess.run(
            [
                sys.executable,
                str(repository / "scripts" / "queue_disc010_signal_screen.py"),
                "--db",
                str(args.db),
                "--assignment",
                str(assignment_path),
                "--repository-root",
                str(repository),
            ],
            cwd=repository,
            check=True,
        )
        queued.append(
            {
                "family_id": assignment["family_id"],
                "instruments": [item["instrument"] for item in basket],
                "timeframe": ("5m", "15m", "1h")[ordinal % 3],
                "workers": args.max_workers,
            }
        )
    state["cycle"] = cycle
    state["last_queued"] = queued
    atomic_json(args.state, state)
    db.close()
    print(
        json.dumps(
            {
                "event": "disc010_queue_replenished",
                "cycle": cycle,
                "queued": queued,
                "selection_inputs": "coverage_metadata_only",
                "final_oos_opened": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
