"""Generalized manifest-driven parallel hypothesis runner."""
from __future__ import annotations

import argparse
import csv
import json
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any

import yaml

from bt.experiments.hypothesis_runner import execute_hypothesis_variant, resolve_phase_tiers
from bt.experiments.manifest import decode_params, encode_params, read_manifest_csv, write_manifest_csv
from bt.experiments.status import detect_run_artifact_status, write_status_csv
from bt.hypotheses.contract import HypothesisContract


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _slug_value(value: Any) -> str:
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, float):
        return str(value).replace(".", "p")
    return str(value).replace("/", "_")


def _variant_slug(grid_id: str, params: dict[str, Any]) -> str:
    suffix = "_".join(f"{key}-{_slug_value(params[key])}" for key in sorted(params.keys()))
    return f"{grid_id}__{suffix}" if suffix else grid_id


def build_hypothesis_manifest_rows(*, contract: HypothesisContract, hypothesis_path: Path, phase: str) -> list[dict[str, str]]:
    tiers = resolve_phase_tiers(contract, phase)
    specs = contract.to_run_specs()
    rows: list[dict[str, str]] = []
    row_idx = 1
    for spec in specs:
        params = spec["params"]
        run_slug = _variant_slug(spec["grid_id"], params)
        for tier in tiers:
            output_dir = f"runs/row_{row_idx:05d}__{run_slug}__{tier.lower()}"
            rows.append(
                {
                    "row_id": f"row_{row_idx:05d}",
                    "hypothesis_id": str(spec["hypothesis_id"]),
                    "hypothesis_path": str(hypothesis_path),
                    "phase": phase,
                    "tier": tier,
                    "variant_id": str(spec["grid_id"]),
                    "config_hash": str(spec["config_hash"]),
                    "params_json": encode_params(params),
                    "run_slug": f"{run_slug}__{tier.lower()}",
                    "output_dir": output_dir,
                    "expected_status": "pending",
                    "enabled": "true",
                    "notes": "",
                }
            )
            row_idx += 1
    return rows


def _manifest_name(hypothesis_path: Path, phase: str) -> str:
    return f"{hypothesis_path.stem}_{phase}_grid.csv"


def build_hypothesis_manifest(
    *,
    hypothesis_path: Path,
    experiment_root: Path,
    phase: str,
) -> Path:
    if not hypothesis_path.exists():
        raise ValueError(f"Hypothesis file does not exist: {hypothesis_path}")
    contract = HypothesisContract.from_yaml(hypothesis_path)
    rows = build_hypothesis_manifest_rows(contract=contract, hypothesis_path=hypothesis_path, phase=phase)
    manifests_dir = experiment_root / "manifests"
    manifest_path = manifests_dir / _manifest_name(hypothesis_path, phase)
    write_manifest_csv(rows, manifest_path)

    snapshot_dir = experiment_root / "contract_snapshot"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = snapshot_dir / hypothesis_path.name
    snapshot_path.write_text(hypothesis_path.read_text(encoding="utf-8"), encoding="utf-8")

    summary_dir = experiment_root / "summaries"
    summary_dir.mkdir(parents=True, exist_ok=True)
    summary_payload = {
        "hypothesis_id": contract.schema.metadata.hypothesis_id,
        "phase": phase,
        "rows": len(rows),
        "tiers": list(resolve_phase_tiers(contract, phase)),
    }
    (summary_dir / f"{hypothesis_path.stem}_{phase}_grid_summary.json").write_text(
        json.dumps(summary_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return manifest_path


def _read_hypothesis_rows_jsonl(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    key_values: dict[str, Any] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if isinstance(payload, dict) and payload.get("grid_id") and payload.get("tier"):
            key = f"{payload['grid_id']}::{payload['tier']}"
            key_values[key] = payload
    return key_values


def _materialize_phase_rollup(experiment_root: Path, manifest_rows: list[dict[str, str]], status_rows: list[dict[str, Any]]) -> None:
    by_key = _read_hypothesis_rows_jsonl(experiment_root / "hypothesis_rows.jsonl")
    status_by_row = {str(row["row_id"]): row for row in status_rows}
    out_rows: list[dict[str, Any]] = []
    for row in manifest_rows:
        if status_by_row.get(row["row_id"], {}).get("status") != "COMPLETED":
            continue
        metrics = by_key.get(f"{row['variant_id']}::{row['tier']}", {})
        out_rows.append(
            {
                "row_id": row["row_id"],
                "variant_id": row["variant_id"],
                "tier": row["tier"],
                "params_json": row["params_json"],
                "output_dir": row["output_dir"],
                "num_trades": metrics.get("num_trades", ""),
                "ev_r_net": metrics.get("ev_r_net", ""),
                "pnl_net": metrics.get("pnl_net", ""),
                "max_drawdown_r": metrics.get("max_drawdown_r", ""),
                "run_dir": metrics.get("run_dir", ""),
            }
        )

    path = experiment_root / "summaries" / "phase_rollup.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "row_id",
        "variant_id",
        "tier",
        "params_json",
        "output_dir",
        "num_trades",
        "ev_r_net",
        "pnl_net",
        "max_drawdown_r",
        "run_dir",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for out_row in out_rows:
            writer.writerow(out_row)




def _materialize_phase_segment_rollups(experiment_root: Path, manifest_rows: list[dict[str, str]], status_rows: list[dict[str, Any]]) -> None:
    status_by_row = {str(row["row_id"]): row for row in status_rows}
    out_rows: list[dict[str, Any]] = []
    for row in manifest_rows:
        if status_by_row.get(row["row_id"], {}).get("status") != "COMPLETED":
            continue
        run_dir = experiment_root / row["output_dir"]
        segment_path = run_dir / "segment_rollups.csv"
        if not segment_path.exists():
            continue
        with segment_path.open("r", encoding="utf-8", newline="") as handle:
            for payload in csv.DictReader(handle):
                out_rows.append(
                    {
                        "row_id": row["row_id"],
                        "variant_id": row["variant_id"],
                        "tier": row["tier"],
                        "hypothesis_id": row["hypothesis_id"],
                        "run_dir": str(run_dir),
                        **payload,
                    }
                )

    out_rows = sorted(out_rows, key=lambda item: (item["row_id"], str(item.get("grouping_keys", "")), str(item.get("segment_value_json", ""))))
    path = experiment_root / "summaries" / "phase_segment_rollups.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "row_id",
        "variant_id",
        "tier",
        "hypothesis_id",
        "run_dir",
        "schema_version",
        "grouping_keys",
        "segment_value_json",
        "source_run_dir",
        "n_trades",
        "ev_r_net",
        "win_rate",
        "avg_win_r",
        "avg_loss_r",
        "payoff_ratio",
        "avg_hold_bars",
        "pnl_net",
        "max_loss_r",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for out in out_rows:
            writer.writerow({field: out.get(field, "") for field in fields})
def _execute_manifest_row(
    *,
    row: dict[str, str],
    config_path: str,
    local_config: str | None,
    data_path: str,
    experiment_root: str,
    override_paths: list[str],
) -> tuple[int, str]:
    contract = HypothesisContract.from_yaml(row["hypothesis_path"])
    spec = {
        "hypothesis_id": row["hypothesis_id"],
        "grid_id": row["variant_id"],
        "config_hash": row["config_hash"],
        "params": decode_params(row["params_json"]),
    }
    run_slug = Path(row["output_dir"]).name
    try:
        execute_hypothesis_variant(
            contract=contract,
            spec=spec,
            tier=row["tier"],
            config_path=config_path,
            data_path=data_path,
            out_root=str(Path(experiment_root) / "runs"),
            local_config=local_config,
            override_paths=override_paths,
            run_slug=run_slug,
        )
        return 0, ""
    except Exception as exc:
        return 1, str(exc)


def run_hypothesis_manifest_in_parallel(
    *,
    manifest_rows: list[dict[str, str]],
    experiment_root: Path,
    config_path: Path,
    local_config: Path | None,
    data_path: Path,
    max_workers: int,
    skip_completed: bool,
    override_paths: list[Path],
    dry_run: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    status_rows: list[dict[str, Any]] = []
    launch_rows: list[dict[str, str]] = []

    if not config_path.exists():
        raise ValueError(f"--config file does not exist: {config_path}")
    if local_config and not local_config.exists():
        raise ValueError(f"--local-config file does not exist: {local_config}")
    if not data_path.exists():
        raise ValueError(f"--data path does not exist: {data_path}")

    for override in override_paths:
        if not override.exists():
            raise ValueError(f"--override file does not exist: {override}")

    for row in manifest_rows:
        row_out_dir = experiment_root / row["output_dir"]
        completed_state = detect_run_artifact_status(row_out_dir)
        if row["enabled"] == "false":
            status_rows.append({
                "row_id": row["row_id"],
                "variant_id": row["variant_id"],
                "tier": row["tier"],
                "status": "SKIPPED",
                "return_code": "",
                "started_at": "",
                "ended_at": "",
                "duration_sec": "",
                "output_dir": row["output_dir"],
                "error_message": "disabled in manifest",
            })
            continue
        if skip_completed and completed_state.state == "SUCCESS":
            status_rows.append({
                "row_id": row["row_id"],
                "variant_id": row["variant_id"],
                "tier": row["tier"],
                "status": "SKIPPED",
                "return_code": "0",
                "started_at": "",
                "ended_at": "",
                "duration_sec": "",
                "output_dir": row["output_dir"],
                "error_message": "already completed",
            })
            continue
        launch_rows.append(row)

    write_status_csv(experiment_root / "summaries" / "manifest_status.csv", sorted(status_rows, key=lambda x: x["row_id"]))

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        in_flight: dict[Any, tuple[dict[str, str], str, float]] = {}
        for row in launch_rows:
            started_at = _utc_now()
            started_clock = monotonic()
            if dry_run:
                status_rows.append(
                    {
                        "row_id": row["row_id"],
                        "variant_id": row["variant_id"],
                        "tier": row["tier"],
                        "status": "PENDING",
                        "return_code": "",
                        "started_at": started_at,
                        "ended_at": "",
                        "duration_sec": "",
                        "output_dir": row["output_dir"],
                        "error_message": "",
                    }
                )
                continue
            future = executor.submit(
                _execute_manifest_row,
                row=row,
                config_path=str(config_path),
                local_config=str(local_config) if local_config else None,
                data_path=str(data_path),
                experiment_root=str(experiment_root),
                override_paths=[str(item) for item in override_paths],
            )
            in_flight[future] = (row, started_at, started_clock)
            status_rows.append(
                {
                    "row_id": row["row_id"],
                    "variant_id": row["variant_id"],
                    "tier": row["tier"],
                    "status": "RUNNING",
                    "return_code": "",
                    "started_at": started_at,
                    "ended_at": "",
                    "duration_sec": "",
                    "output_dir": row["output_dir"],
                    "error_message": "",
                }
            )

        while in_flight:
            done, _ = wait(in_flight.keys(), return_when=FIRST_COMPLETED)
            for future in done:
                row, started_at, started_clock = in_flight.pop(future)
                return_code, error_message = future.result()
                ended_at = _utc_now()
                duration = monotonic() - started_clock
                artifact_state = detect_run_artifact_status(experiment_root / row["output_dir"])
                status = "COMPLETED" if return_code == 0 and artifact_state.state == "SUCCESS" else "FAILED"
                status_rows = [
                    existing
                    for existing in status_rows
                    if not (existing["row_id"] == row["row_id"] and existing["status"] == "RUNNING")
                ]
                status_rows.append(
                    {
                        "row_id": row["row_id"],
                        "variant_id": row["variant_id"],
                        "tier": row["tier"],
                        "status": status,
                        "return_code": str(return_code),
                        "started_at": started_at,
                        "ended_at": ended_at,
                        "duration_sec": f"{duration:.3f}",
                        "output_dir": row["output_dir"],
                        "error_message": error_message or artifact_state.message,
                    }
                )
                write_status_csv(experiment_root / "summaries" / "manifest_status.csv", sorted(status_rows, key=lambda x: x["row_id"]))

    status_rows = sorted(status_rows, key=lambda x: x["row_id"])
    failures = [row for row in status_rows if row["status"] == "FAILED"]
    write_status_csv(experiment_root / "summaries" / "manifest_status.csv", status_rows)
    write_status_csv(experiment_root / "summaries" / "failures.csv", failures)
    _materialize_phase_rollup(experiment_root, manifest_rows, status_rows)
    _materialize_phase_segment_rollups(experiment_root, manifest_rows, status_rows)
    return status_rows, failures


def cli_build_hypothesis_manifest(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build manifest for any hypothesis contract")
    parser.add_argument("--hypothesis", required=True)
    parser.add_argument("--experiment-root", required=True)
    parser.add_argument("--phase", choices=("tier2", "tier3", "validate"), default="tier2")
    args = parser.parse_args(argv)

    manifest = build_hypothesis_manifest(
        hypothesis_path=Path(args.hypothesis),
        experiment_root=Path(args.experiment_root),
        phase=args.phase,
    )
    print(f"wrote manifest: {manifest}")
    return 0


def cli_run_parallel_hypothesis_grid(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a hypothesis manifest in process-level parallelism")
    parser.add_argument("--experiment-root", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--local-config")
    parser.add_argument("--data", required=True)
    parser.add_argument("--phase", choices=("tier2", "tier3", "validate"))
    parser.add_argument("--override", action="append", default=[])
    parser.add_argument("--max-workers", type=int, default=6)
    parser.add_argument("--skip-completed", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    if args.max_workers <= 0:
        raise ValueError("--max-workers must be > 0")

    manifest_rows = read_manifest_csv(Path(args.manifest))
    if args.phase:
        manifest_rows = [row for row in manifest_rows if row["phase"] == args.phase]

    statuses, failures = run_hypothesis_manifest_in_parallel(
        manifest_rows=manifest_rows,
        experiment_root=Path(args.experiment_root),
        config_path=Path(args.config),
        local_config=Path(args.local_config) if args.local_config else None,
        data_path=Path(args.data),
        max_workers=args.max_workers,
        skip_completed=bool(args.skip_completed),
        override_paths=[Path(p) for p in args.override],
        dry_run=bool(args.dry_run),
    )
    print(f"runs_total={len(statuses)}")
    print(f"runs_failed={len(failures)}")
    return 1 if failures and not args.dry_run else 0


def cli_run_parallel_grid(argv: list[str] | None = None) -> int:
    """Backward-compatible wrapper for legacy script argument names."""
    parser = argparse.ArgumentParser(description="Backward-compatible wrapper for run_parallel_grid.py")
    parser.add_argument("--experiment-root", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--base-config", required=True)
    parser.add_argument("--local-config")
    parser.add_argument("--data", required=True)
    parser.add_argument("--max-workers", type=int, default=6)
    parser.add_argument("--skip-completed", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    return cli_run_parallel_hypothesis_grid(
        [
            "--experiment-root",
            args.experiment_root,
            "--manifest",
            args.manifest,
            "--config",
            args.base_config,
            "--data",
            args.data,
            "--max-workers",
            str(args.max_workers),
            *( ["--local-config", args.local_config] if args.local_config else []),
            *( ["--skip-completed"] if args.skip_completed else []),
            *( ["--dry-run"] if args.dry_run else []),
        ]
    )


def cli_build_manifest(argv: list[str] | None = None) -> int:
    """Backward-compatible stub kept for legacy scripts."""
    parser = argparse.ArgumentParser(description="Deprecated strategy-specific manifest builder")
    parser.add_argument("--experiment-root", required=True)
    parser.add_argument("--strategy", required=True)
    args = parser.parse_args(argv)
    raise ValueError(
        f"Legacy --strategy flow is deprecated ({args.strategy!r}). Use scripts/build_hypothesis_grid.py --hypothesis ..."
    )
