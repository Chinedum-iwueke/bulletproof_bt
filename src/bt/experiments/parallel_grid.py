"""Repo-safe parallel grid utilities for H1B volfloor experiments."""
from __future__ import annotations

import csv
import json
import subprocess
import sys
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any

import yaml

from bt.logging.run_contract import REQUIRED_ARTIFACTS


@dataclass(frozen=True)
class GridSpec:
    strategy_name: str
    exit_type: str
    timeframe: str = "15m"
    execution_tier: str = "tier2"
    vol_floors: tuple[int, ...] = (60, 70, 80, 85)
    adx_mins: tuple[int, ...] = (18, 22, 25)
    er_mins: tuple[float, ...] = (0.35, 0.45, 0.55)
    er_lookback: int = 16


@dataclass(frozen=True)
class RunArtifactStatus:
    state: str
    return_code: int | None
    message: str


MANIFEST_COLUMNS = [
    "run_id",
    "strategy_name",
    "exit_type",
    "timeframe",
    "execution_tier",
    "vol_floor",
    "adx_min",
    "er_min",
    "er_lookback",
]

STATUS_COLUMNS = [
    "run_id",
    "status",
    "return_code",
    "started_at",
    "ended_at",
    "duration_sec",
    "log_path",
    "run_dir",
    "vol_floor",
    "adx_min",
    "er_min",
    "er_lookback",
]


def _format_er_slug(er_min: float) -> str:
    return f"{int(round(er_min * 100)):03d}"


def build_grid_rows(spec: GridSpec) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    run_index = 1
    for vol_floor in spec.vol_floors:
        for adx_min in spec.adx_mins:
            for er_min in spec.er_mins:
                run_id = (
                    f"run_{run_index:03d}__vol{vol_floor}_adx{adx_min}_"
                    f"er{_format_er_slug(er_min)}_n{spec.er_lookback}"
                )
                rows.append(
                    {
                        "run_id": run_id,
                        "strategy_name": spec.strategy_name,
                        "exit_type": spec.exit_type,
                        "timeframe": spec.timeframe,
                        "execution_tier": spec.execution_tier,
                        "vol_floor": str(vol_floor),
                        "adx_min": str(adx_min),
                        "er_min": str(er_min),
                        "er_lookback": str(spec.er_lookback),
                    }
                )
                run_index += 1
    return rows


def write_manifest_csv(rows: list[dict[str, str]], manifest_path: Path) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _validate_manifest_row(row: dict[str, str]) -> None:
    missing = [column for column in MANIFEST_COLUMNS if not row.get(column)]
    if missing:
        raise ValueError(f"Manifest row missing required columns {missing}: {row}")


def read_manifest_csv(manifest_path: Path) -> list[dict[str, str]]:
    with manifest_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [dict(row) for row in reader]
    for row in rows:
        _validate_manifest_row(row)
    return rows


def build_override_payload(row: dict[str, str]) -> dict[str, Any]:
    return {
        "data": {
            "mode": "streaming",
            "chunksize": 50000,
            "symbols_subset": None,
            "max_symbols": None,
            "date_range": None,
            "entry_timeframe": row["timeframe"],
            "exit_timeframe": "1m",
        },
        "execution": {
            "profile": row["execution_tier"],
        },
        "strategy": {
            "name": row["strategy_name"],
            "exit_type": row["exit_type"],
            "variant": "B",
            "vol_floor_pct": float(row["vol_floor"]),
            "adx_min": float(row["adx_min"]),
            "er_min": float(row["er_min"]),
            "er_lookback": int(row["er_lookback"]),
        },
    }


def write_override_yaml(row: dict[str, str], overrides_dir: Path) -> Path:
    overrides_dir.mkdir(parents=True, exist_ok=True)
    path = overrides_dir / f"{row['run_id']}.yaml"
    payload = build_override_payload(row)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=True)
    return path


def write_overrides_for_manifest(rows: list[dict[str, str]], overrides_dir: Path) -> dict[str, Path]:
    output: dict[str, Path] = {}
    for row in rows:
        output[row["run_id"]] = write_override_yaml(row, overrides_dir)
    return output


def detect_run_artifact_status(run_dir: Path) -> RunArtifactStatus:
    if not run_dir.exists():
        return RunArtifactStatus(state="MISSING", return_code=None, message="run directory missing")
    status_path = run_dir / "run_status.json"
    if not status_path.exists():
        return RunArtifactStatus(state="INCOMPLETE", return_code=None, message="run_status.json missing")

    try:
        payload = json.loads(status_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return RunArtifactStatus(state="INCOMPLETE", return_code=None, message="run_status.json unreadable")
    if not isinstance(payload, dict):
        return RunArtifactStatus(state="INCOMPLETE", return_code=None, message="run_status.json invalid shape")

    status_value = str(payload.get("status", "")).upper()
    if status_value == "PASS":
        missing_required = sorted(name for name in REQUIRED_ARTIFACTS if not (run_dir / name).exists())
        if missing_required:
            return RunArtifactStatus(
                state="INCOMPLETE",
                return_code=None,
                message=f"missing artifacts: {','.join(missing_required)}",
            )
        return RunArtifactStatus(state="SUCCESS", return_code=0, message="PASS")
    if status_value == "FAIL":
        return RunArtifactStatus(state="FAILED", return_code=1, message=str(payload.get("error_message", "")))
    return RunArtifactStatus(state="INCOMPLETE", return_code=None, message=f"unknown status={status_value!r}")


def build_run_command(
    *,
    base_config: Path,
    data_path: Path,
    out_dir: Path,
    run_id: str,
    override_path: Path,
    python_executable: str,
) -> list[str]:
    return [
        python_executable,
        "scripts/run_backtest.py",
        "--config",
        str(base_config),
        "--data",
        str(data_path),
        "--run-id",
        run_id,
        "--out-dir",
        str(out_dir),
        "--override",
        str(override_path),
    ]


def _execute_command(command: list[str], log_path: Path, dry_run: bool) -> int:
    if dry_run:
        return 0
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as handle:
        completed = subprocess.run(command, stdout=handle, stderr=subprocess.STDOUT, check=False)
    return int(completed.returncode)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _write_status_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=STATUS_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in STATUS_COLUMNS})


def run_manifest_in_parallel(
    *,
    rows: list[dict[str, str]],
    experiment_root: Path,
    base_config: Path,
    data_path: Path,
    max_workers: int,
    skip_completed: bool,
    retry_failed: bool,
    dry_run: bool,
    run_filter: str | None,
    python_executable: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    runs_dir = experiment_root / "runs"
    logs_dir = experiment_root / "logs"
    status_dir = experiment_root / "status"
    overrides_dir = experiment_root / "overrides"

    override_paths = write_overrides_for_manifest(rows, overrides_dir)

    status_rows: list[dict[str, Any]] = []
    launch_rows: list[dict[str, str]] = []

    for row in rows:
        run_id = row["run_id"]
        if run_filter and run_filter not in run_id:
            status_rows.append(
                {
                    "run_id": run_id,
                    "status": "SKIPPED",
                    "return_code": "",
                    "started_at": "",
                    "ended_at": "",
                    "duration_sec": "",
                    "log_path": str(logs_dir / f"{run_id}.log"),
                    "run_dir": str(runs_dir / run_id),
                    "vol_floor": row["vol_floor"],
                    "adx_min": row["adx_min"],
                    "er_min": row["er_min"],
                    "er_lookback": row["er_lookback"],
                }
            )
            continue

        artifact_status = detect_run_artifact_status(runs_dir / run_id)
        should_skip = False
        if skip_completed and artifact_status.state == "SUCCESS":
            should_skip = True
        if retry_failed:
            should_skip = artifact_status.state != "FAILED"

        if should_skip:
            status_rows.append(
                {
                    "run_id": run_id,
                    "status": "SKIPPED",
                    "return_code": artifact_status.return_code or "",
                    "started_at": "",
                    "ended_at": "",
                    "duration_sec": "",
                    "log_path": str(logs_dir / f"{run_id}.log"),
                    "run_dir": str(runs_dir / run_id),
                    "vol_floor": row["vol_floor"],
                    "adx_min": row["adx_min"],
                    "er_min": row["er_min"],
                    "er_lookback": row["er_lookback"],
                }
            )
            continue

        launch_rows.append(row)

    _write_status_csv(status_dir / "grid_status.csv", sorted(status_rows, key=lambda item: item["run_id"]))

    def _to_status(row: dict[str, str], return_code: int, started_at: str, ended_at: str, duration: float) -> dict[str, Any]:
        run_id = row["run_id"]
        if dry_run:
            status = "PENDING"
        else:
            detected = detect_run_artifact_status(runs_dir / run_id)
            status = "SUCCESS" if return_code == 0 and detected.state == "SUCCESS" else "FAILED"
        return {
            "run_id": run_id,
            "status": status,
            "return_code": return_code,
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_sec": f"{duration:.3f}",
            "log_path": str(logs_dir / f"{run_id}.log"),
            "run_dir": str(runs_dir / run_id),
            "vol_floor": row["vol_floor"],
            "adx_min": row["adx_min"],
            "er_min": row["er_min"],
            "er_lookback": row["er_lookback"],
        }

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        in_flight: dict[Any, tuple[dict[str, str], str, float]] = {}

        for row in launch_rows:
            run_id = row["run_id"]
            command = build_run_command(
                base_config=base_config,
                data_path=data_path,
                out_dir=runs_dir,
                run_id=run_id,
                override_path=override_paths[run_id],
                python_executable=python_executable,
            )
            started_at = _utc_now()
            started_clock = monotonic()
            future = executor.submit(_execute_command, command, logs_dir / f"{run_id}.log", dry_run)
            in_flight[future] = (row, started_at, started_clock)
            status_rows.append(
                {
                    "run_id": run_id,
                    "status": "RUNNING",
                    "return_code": "",
                    "started_at": started_at,
                    "ended_at": "",
                    "duration_sec": "",
                    "log_path": str(logs_dir / f"{run_id}.log"),
                    "run_dir": str(runs_dir / run_id),
                    "vol_floor": row["vol_floor"],
                    "adx_min": row["adx_min"],
                    "er_min": row["er_min"],
                    "er_lookback": row["er_lookback"],
                }
            )

        while in_flight:
            done, _ = wait(in_flight.keys(), return_when=FIRST_COMPLETED)
            for future in done:
                row, started_at, started_clock = in_flight.pop(future)
                return_code = int(future.result())
                ended_at = _utc_now()
                duration = monotonic() - started_clock
                final_status = _to_status(row, return_code, started_at, ended_at, duration)
                status_rows = [entry for entry in status_rows if not (entry["run_id"] == row["run_id"] and entry["status"] == "RUNNING")]
                status_rows.append(final_status)
                _write_status_csv(status_dir / "grid_status.csv", sorted(status_rows, key=lambda item: item["run_id"]))

    status_rows = sorted(status_rows, key=lambda item: item["run_id"])
    failures = [row for row in status_rows if row.get("status") == "FAILED"]
    _write_status_csv(status_dir / "grid_status.csv", status_rows)
    _write_status_csv(status_dir / "failures.csv", failures)
    return status_rows, failures


def parse_spec_name(strategy_name: str) -> GridSpec:
    if strategy_name == "volfloor_donchian":
        return GridSpec(strategy_name="volfloor_donchian", exit_type="donchian_reversal")
    if strategy_name == "volfloor_ema_pullback":
        return GridSpec(strategy_name="volfloor_ema_pullback", exit_type="ema_trend_end")
    raise ValueError(f"Unsupported strategy_name={strategy_name!r}")


def cli_build_manifest(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Build deterministic 36-run H1B grid manifest and overrides")
    parser.add_argument("--experiment-root", required=True)
    parser.add_argument("--strategy", required=True, choices=("volfloor_donchian", "volfloor_ema_pullback"))
    args = parser.parse_args(argv)

    experiment_root = Path(args.experiment_root)
    spec = parse_spec_name(args.strategy)
    rows = build_grid_rows(spec)

    manifest_path = experiment_root / "manifests" / f"{args.strategy}_grid_36.csv"
    write_manifest_csv(rows, manifest_path)
    write_overrides_for_manifest(rows, experiment_root / "overrides")

    print(f"wrote manifest: {manifest_path}")
    print(f"rows: {len(rows)}")
    return 0


def cli_run_parallel_grid(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Run H1B grid in bounded process parallelism")
    parser.add_argument("--experiment-root", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--base-config", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--max-workers", type=int, default=10)
    parser.add_argument("--skip-completed", action="store_true")
    parser.add_argument("--retry-failed", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--run-filter")
    args = parser.parse_args(argv)

    if args.max_workers <= 0:
        raise ValueError("--max-workers must be > 0")

    rows = read_manifest_csv(Path(args.manifest))
    status_rows, failures = run_manifest_in_parallel(
        rows=rows,
        experiment_root=Path(args.experiment_root),
        base_config=Path(args.base_config),
        data_path=Path(args.data),
        max_workers=args.max_workers,
        skip_completed=bool(args.skip_completed),
        retry_failed=bool(args.retry_failed),
        dry_run=bool(args.dry_run),
        run_filter=args.run_filter,
        python_executable=sys.executable,
    )

    print(f"runs_total={len(status_rows)}")
    print(f"runs_failed={len(failures)}")
    return 1 if failures and not args.dry_run else 0


def cli_grid_status(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Summarize grid status csv")
    parser.add_argument("--status-csv", required=True)
    args = parser.parse_args(argv)

    with Path(args.status_csv).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        status_rows = [dict(row) for row in reader]

    summary: dict[str, int] = {}
    for row in status_rows:
        key = row.get("status", "UNKNOWN")
        summary[key] = summary.get(key, 0) + 1

    print("status_counts")
    for key in sorted(summary):
        print(f"  {key}: {summary[key]}")

    failed_rows = [row for row in status_rows if row.get("status") == "FAILED"]
    print(f"failed_runs={len(failed_rows)}")
    if failed_rows:
        print("recent_failed")
        for row in failed_rows[-5:]:
            print(f"  {row['run_id']} rc={row.get('return_code')}")

    return 0
