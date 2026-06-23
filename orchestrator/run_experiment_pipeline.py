#!/usr/bin/env python3
"""Orchestrate one hypothesis experiment pipeline end-to-end.

Example:
    python orchestrator/run_experiment_pipeline.py \
      --hypothesis research/hypotheses/l1_h7c_high_selectivity_regime.yaml \
      --name l1_h7c_high_selectivity_regime \
      --max-workers 6

Minimal smoke run suggestion:
    python orchestrator/init_research_db.py --db research_db/research.sqlite

    python orchestrator/run_experiment_pipeline.py \
      --hypothesis research/hypotheses/<some_existing_hypothesis>.yaml \
      --name smoke_test_hypothesis \
      --max-workers 1 \
      --skip-run \
      --skip-cleanup \
      --research-db research_db/research.sqlite
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from orchestrator.db import ResearchDB
from orchestrator.process_logging import CommandRunResult, PipelineCommandError, run_pipeline_command
from orchestrator.research_terminal.cards import (
    build_and_write_failure_cards,
    build_and_write_intelligence_cards,
)
from bt.paths import (
    resolve_command_log_dir,
    resolve_experiment_root,
    resolve_output_phase_root,
    resolve_pipeline_log_path,
    resolve_verdict_bundle_root,
)
from bt.validation.strategy_admission import (
    validate_hypothesis_admission,
    write_strategy_admission_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run stable+volatile experiment automation pipeline.")
    parser.add_argument("--hypothesis", required=True, help="Path to hypothesis YAML.")
    parser.add_argument("--name", required=True, help="Experiment name prefix.")
    parser.add_argument("--max-workers", type=int, default=6)
    parser.add_argument(
        "--volatile-max-workers",
        type=int,
        default=None,
        help="Optional worker cap for volatile research-panel runs; defaults to min(max-workers, 4).",
    )

    parser.add_argument("--phase", default="tier2")
    parser.add_argument("--config", default="configs/engine.yaml")
    parser.add_argument("--local-config", default="configs/local/engine.lab.yaml")
    parser.add_argument(
        "--stable-data",
        default=None,
        help="Legacy stable curated dataset path. If set, overrides research_data profile mode.",
    )
    parser.add_argument(
        "--vol-data",
        default=None,
        help="Legacy volatile curated dataset path. If set, overrides research_data profile mode.",
    )
    parser.add_argument("--data-root", default="research_data")
    parser.add_argument("--data-kind", default="research_panel")
    parser.add_argument("--exchange", default="binance")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--stable-manifest", default="research_data/manifests/stable_universe.parquet")
    parser.add_argument("--membership-path", default="research_data/manifests/volatile_universe_membership.parquet")
    parser.add_argument("--outputs-root", default="outputs")
    parser.add_argument("--max-workers-auto", action="store_true", default=False)
    parser.add_argument("--reserve-ram-gb", type=float, default=8.0)
    parser.add_argument("--max-ram-per-worker-gb", type=float, default=None)
    parser.add_argument("--min-free-ram-gb", type=float, default=6.0)
    parser.add_argument("--run-timeout-seconds", type=float, default=None)
    parser.add_argument("--fail-fast", action="store_true", default=False)
    parser.add_argument("--no-resume-strict", action="store_true", default=False)
    parser.add_argument(
        "--parallel-datasets",
        action="store_true",
        default=False,
        help="Run stable and volatile dataset backtests concurrently, splitting max workers across them.",
    )

    parser.add_argument("--retain-top-n", type=int, default=2)
    parser.add_argument("--retain-median", type=int, default=1)
    parser.add_argument("--retain-worst", type=int, default=1)

    parser.add_argument("--skip-run", action="store_true", default=False)
    parser.add_argument("--skip-truth-validation", action="store_true", default=False)
    parser.add_argument("--skip-analysis", action="store_true", default=False)
    parser.add_argument("--skip-extract", action="store_true", default=False)
    parser.add_argument("--skip-cleanup", action="store_true", default=False)
    parser.add_argument("--skip-cleanup-delete-nonretained-runs", action="store_true", default=False)

    parser.add_argument("--no-cleanup-delete-nonretained-runs", action="store_true", default=False)
    parser.add_argument("--no-cleanup-delete-logs", action="store_true", default=False)
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--research-db", default=None, help="Optional SQLite DB path for lifecycle tracking.")
    parser.add_argument("--failure-tail-lines", type=int, default=120)
    parser.add_argument("--command-log-dir", default=None)
    parser.add_argument("--no-command-log-capture", action="store_true", default=False)
    return parser.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def run_command(
    cmd: list[str],
    *,
    step: str,
    dry_run: bool,
    log_path: Path,
    commands_log: list[dict[str, Any]],
    command_log_dir: Path | None,
    failure_tail_lines: int,
    capture_logs: bool,
) -> None:
    printable = " ".join(cmd)
    print(f"$ {printable}")

    result = run_pipeline_command(
        cmd=cmd,
        step=step,
        cwd=PROJECT_ROOT,
        log_path=log_path,
        command_log_dir=command_log_dir,
        sequence_num=len(commands_log) + 1,
        dry_run=dry_run,
        capture_logs=capture_logs,
        failure_tail_lines=failure_tail_lines,
    )
    commands_log.append({
        "step": result.step,
        "cmd": result.cmd,
        "returncode": result.returncode,
        "cwd": result.cwd,
        "stdout_log": result.stdout_log,
        "stderr_log": result.stderr_log,
        "started_at": result.started_at,
        "completed_at": result.completed_at,
        "root_cause_hint": result.root_cause_hint,
    })


def build_manifest(
    *,
    hypothesis: Path,
    experiment_root: Path,
    phase: str,
    project_root: Path,
    dry_run: bool,
    log_path: Path,
    commands_log: list[dict[str, Any]],
    step: str,
    command_log_dir: Path | None,
    failure_tail_lines: int,
    capture_logs: bool,
) -> None:
    cmd = [
        sys.executable,
        str(project_root / "scripts" / "build_hypothesis_grid.py"),
        "--hypothesis",
        str(hypothesis),
        "--experiment-root",
        str(experiment_root),
        "--phase",
        phase,
    ]
    run_command(cmd, step=step, dry_run=dry_run, log_path=log_path, commands_log=commands_log, command_log_dir=command_log_dir, failure_tail_lines=failure_tail_lines, capture_logs=capture_logs)


def discover_manifest(experiment_root: Path) -> Path:
    manifest_dir = experiment_root / "manifests"
    candidates = sorted(manifest_dir.glob("*.csv"))
    if not candidates:
        raise FileNotFoundError(f"No manifest CSV found under {manifest_dir}")
    if len(candidates) == 1:
        return candidates[0]

    chosen = max(candidates, key=lambda p: p.stat().st_mtime)
    print(
        f"WARNING: Multiple manifests found under {manifest_dir}; using latest modified: {chosen}",
        file=sys.stderr,
    )
    return chosen


def run_backtest(
    *,
    experiment_root: Path,
    manifest_path: Path,
    config: str,
    local_config: str,
    data_path: str | None,
    data_root: str,
    data_kind: str,
    exchange: str,
    universe: str,
    timeframe: str,
    stable_manifest: str | None,
    membership_path: str | None,
    max_workers: int,
    max_workers_auto: bool,
    reserve_ram_gb: float,
    max_ram_per_worker_gb: float | None,
    min_free_ram_gb: float,
    run_timeout_seconds: float | None,
    fail_fast: bool,
    resume_strict: bool,
    phase: str,
    project_root: Path,
    dry_run: bool,
    log_path: Path,
    commands_log: list[dict[str, Any]],
    step: str,
    command_log_dir: Path | None,
    failure_tail_lines: int,
    capture_logs: bool,
) -> None:
    cmd = [
        sys.executable,
        str(project_root / "scripts" / "run_parallel_hypothesis_grid.py"),
        "--experiment-root",
        str(experiment_root),
        "--manifest",
        str(manifest_path),
        "--config",
        config,
        "--local-config",
        local_config,
        "--phase",
        phase,
        "--max-workers",
        str(max_workers),
        "--skip-completed",
        "--reserve-ram-gb",
        str(reserve_ram_gb),
        "--min-free-ram-gb",
        str(min_free_ram_gb),
    ]
    if max_workers_auto:
        cmd.append("--max-workers-auto")
    if max_ram_per_worker_gb is not None:
        cmd.extend(["--max-ram-per-worker-gb", str(max_ram_per_worker_gb)])
    if run_timeout_seconds is not None:
        cmd.extend(["--run-timeout-seconds", str(run_timeout_seconds)])
    if fail_fast:
        cmd.append("--fail-fast")
    else:
        cmd.append("--no-fail-fast")
    if resume_strict:
        cmd.append("--resume-strict")
    else:
        cmd.append("--no-resume-strict")
    if data_path:
        cmd.extend(["--data", data_path])
    else:
        cmd.extend(
            [
                "--data-root",
                data_root,
                "--data-kind",
                data_kind,
                "--exchange",
                exchange,
                "--universe",
                universe,
                "--timeframe",
                timeframe,
            ]
        )
        if universe == "stable" and stable_manifest:
            cmd.extend(["--stable-manifest", stable_manifest])
        if universe == "volatile" and membership_path:
            cmd.extend(["--membership-path", membership_path])
    run_command(cmd, step=step, dry_run=dry_run, log_path=log_path, commands_log=commands_log, command_log_dir=command_log_dir, failure_tail_lines=failure_tail_lines, capture_logs=capture_logs)


def run_truth_validation(
    *,
    experiment_root: Path,
    project_root: Path,
    dry_run: bool,
    log_path: Path,
    commands_log: list[dict[str, Any]],
    step: str,
    command_log_dir: Path | None,
    failure_tail_lines: int,
    capture_logs: bool,
) -> None:
    cmd = [
        sys.executable,
        str(project_root / "scripts" / "validate_experiment_truth.py"),
        "--experiment-root",
        str(experiment_root),
    ]
    run_command(cmd, step=step, dry_run=dry_run, log_path=log_path, commands_log=commands_log, command_log_dir=command_log_dir, failure_tail_lines=failure_tail_lines, capture_logs=capture_logs)


def split_dataset_workers(max_workers: int) -> tuple[int, int]:
    stable_workers = max(1, max_workers // 2)
    volatile_workers = max(1, max_workers - stable_workers)
    return stable_workers, volatile_workers


def resolve_volatile_workers(max_workers: int, volatile_max_workers: int | None, *, data_kind: str) -> int:
    if volatile_max_workers is not None:
        if volatile_max_workers <= 0:
            raise ValueError("--volatile-max-workers must be positive when provided")
        return min(max_workers, volatile_max_workers)
    if data_kind == "research_panel":
        return min(max_workers, 4)
    return max_workers


def run_post_analysis(
    *,
    experiment_root: Path,
    project_root: Path,
    dry_run: bool,
    log_path: Path,
    commands_log: list[dict[str, Any]],
    step: str,
    command_log_dir: Path | None,
    failure_tail_lines: int,
    capture_logs: bool,
) -> None:
    cmd = [
        sys.executable,
        str(project_root / "scripts" / "post_run_analysis.py"),
        "--experiment-root",
        str(experiment_root),
        "--runs-glob",
        "runs/*",
        "--skip-existing",
    ]
    run_command(cmd, step=step, dry_run=dry_run, log_path=log_path, commands_log=commands_log, command_log_dir=command_log_dir, failure_tail_lines=failure_tail_lines, capture_logs=capture_logs)


def extract_dataset(
    *,
    experiment_root: Path,
    project_root: Path,
    dry_run: bool,
    log_path: Path,
    commands_log: list[dict[str, Any]],
    step: str,
    command_log_dir: Path | None,
    failure_tail_lines: int,
    capture_logs: bool,
) -> None:
    cmd = [
        sys.executable,
        str(project_root / "scripts" / "extract_experiment_dataset.py"),
        "--experiment-root",
        str(experiment_root),
        "--runs-glob",
        "runs/*",
        "--skip-existing",
    ]
    run_command(cmd, step=step, dry_run=dry_run, log_path=log_path, commands_log=commands_log, command_log_dir=command_log_dir, failure_tail_lines=failure_tail_lines, capture_logs=capture_logs)


def cleanup_experiment(
    *,
    experiment_root: Path,
    retain_top_n: int,
    retain_median: int,
    retain_worst: int,
    delete_logs: bool,
    delete_nonretained_runs: bool,
    project_root: Path,
    dry_run: bool,
    log_path: Path,
    commands_log: list[dict[str, Any]],
    step: str,
    command_log_dir: Path | None,
    failure_tail_lines: int,
    capture_logs: bool,
) -> None:
    cmd = [
        sys.executable,
        str(project_root / "scripts" / "cleanup_experiment_runs.py"),
        "--experiment-root",
        str(experiment_root),
        "--runs-glob",
        "runs/*",
        "--retain-top-n",
        str(retain_top_n),
        "--retain-median",
        str(retain_median),
        "--retain-worst",
        str(retain_worst),
    ]
    if delete_logs:
        cmd.append("--delete-logs")
    if delete_nonretained_runs:
        cmd.append("--delete-nonretained-runs")

    run_command(cmd, step=step, dry_run=dry_run, log_path=log_path, commands_log=commands_log, command_log_dir=command_log_dir, failure_tail_lines=failure_tail_lines, capture_logs=capture_logs)


def collect_summary_files(experiment_root: Path) -> list[str]:
    summaries_dir = experiment_root / "summaries"
    if not summaries_dir.exists():
        return []
    return sorted(str(path.resolve()) for path in summaries_dir.glob("*.csv") if path.is_file())


def collect_dataset_files(experiment_root: Path) -> list[str]:
    research_dir = experiment_root / "research_data"
    files: list[str] = []
    for name in ("runs_dataset.parquet", "trades_dataset.parquet"):
        path = research_dir / name
        if path.exists():
            files.append(str(path.resolve()))
    return files


def collect_retained_runs(experiment_root: Path) -> list[str]:
    retained_csv = experiment_root / "research_data" / "retained_runs.csv"
    if not retained_csv.exists():
        return []

    retained_dirs: list[str] = []
    with retained_csv.open("r", encoding="utf-8") as fh:
        lines = [line.strip() for line in fh if line.strip()]
    if not lines:
        return []

    header = [h.strip() for h in lines[0].split(",")]
    try:
        run_id_idx = header.index("run_id")
    except ValueError:
        return []

    for row in lines[1:]:
        cols = [c.strip() for c in row.split(",")]
        if run_id_idx >= len(cols):
            continue
        run_id = cols[run_id_idx]
        run_dir = experiment_root / "runs" / run_id
        if run_dir.exists() and run_dir.is_dir():
            retained_dirs.append(str(run_dir.resolve()))
    return sorted(set(retained_dirs))


def create_verdict_bundle(
    *,
    name: str,
    hypothesis: Path,
    phase: str,
    outputs_root: Path,
    stable_root: Path,
    volatile_root: Path,
    stable_manifest: Path,
    volatile_manifest: Path,
    commands_log: list[dict[str, Any]],
    cleanup_ran: bool,
) -> Path:
    bundle_dir = resolve_verdict_bundle_root(outputs_root=outputs_root, phase=phase, experiment_name=name)
    bundle_dir.mkdir(parents=True, exist_ok=True)

    stable_summary_files = collect_summary_files(stable_root)
    volatile_summary_files = collect_summary_files(volatile_root)
    stable_dataset_files = collect_dataset_files(stable_root)
    volatile_dataset_files = collect_dataset_files(volatile_root)

    stable_retained = collect_retained_runs(stable_root) if cleanup_ran else []
    volatile_retained = collect_retained_runs(volatile_root) if cleanup_ran else []

    manifest = {
        "name": name,
        "hypothesis": str(hypothesis.resolve()),
        "phase": phase,
        "created_at": utc_now_iso(),
        "stable": {
            "experiment_root": str(stable_root.resolve()),
            "manifest": str(stable_manifest.resolve()),
            "summary_files": stable_summary_files,
            "dataset_files": stable_dataset_files,
            "retained_runs": stable_retained,
        },
        "volatile": {
            "experiment_root": str(volatile_root.resolve()),
            "manifest": str(volatile_manifest.resolve()),
            "summary_files": volatile_summary_files,
            "dataset_files": volatile_dataset_files,
            "retained_runs": volatile_retained,
        },
        "commands": commands_log,
    }

    manifest_path = bundle_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    readme_path = bundle_dir / "README.md"
    readme = f"""# Verdict Input Bundle: {name}

This bundle captures the key outputs for later LLM interpretation.

## Core inputs
- Hypothesis YAML: {hypothesis.resolve()}
- Stable experiment root: {stable_root.resolve()}
- Volatile experiment root: {volatile_root.resolve()}
- Stable manifest: {stable_manifest.resolve()}
- Volatile manifest: {volatile_manifest.resolve()}

## Expected interpretation artifacts
Use files listed in `manifest.json` under:
- `stable.summary_files` and `volatile.summary_files` (run_summary + strategy summary CSVs)
- `stable.dataset_files` and `volatile.dataset_files` (runs/trades datasets)
- `stable.retained_runs` and `volatile.retained_runs` (if cleanup ran)

Prefer passing paths from `manifest.json` instead of copying large parquet files.
"""
    readme_path.write_text(readme, encoding="utf-8")

    return bundle_dir


def strategy_terminal_cards_dir(*, outputs_root: Path, phase: str, name: str) -> Path:
    return outputs_root / phase / f"{name}_strategy_terminal_cards"


def verify_cleanup_prerequisites(stable_root: Path, volatile_root: Path) -> None:
    for label, root in (("stable", stable_root), ("volatile", volatile_root)):
        summary = root / "summaries" / "run_summary.csv"
        runs_dataset = root / "research_data" / "runs_dataset.parquet"
        trades_dataset = root / "research_data" / "trades_dataset.parquet"
        missing = [str(p) for p in (summary, runs_dataset, trades_dataset) if not p.exists()]
        if missing:
            raise FileNotFoundError(
                f"Refusing cleanup for {label}: missing post-analysis/extraction outputs: {missing}"
            )


def db_status_update(db: ResearchDB | None, pipeline_run_id: str | None, status: str, commands_log: list[dict[str, Any]]) -> None:
    if db is None or pipeline_run_id is None:
        return
    db.update_pipeline_run_status(pipeline_run_id, status, commands=commands_log)
    print(f"[db] Updated status: {status}")


def db_register_artifact(
    db: ResearchDB | None,
    *,
    artifact_type: str,
    path: Path,
    hypothesis_id: str | None = None,
    experiment_id: str | None = None,
    pipeline_run_id: str | None = None,
    description: str | None = None,
    metadata: Any = None,
) -> None:
    if db is None:
        return
    if not path.exists():
        return
    db.register_artifact(
        artifact_type=artifact_type,
        path=path,
        hypothesis_id=hypothesis_id,
        experiment_id=experiment_id,
        pipeline_run_id=pipeline_run_id,
        description=description,
        metadata=metadata,
    )
    print(f"[db] Registered artifact: {artifact_type} -> {path}")


def main() -> int:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[1]

    hypothesis = Path(args.hypothesis)
    if not hypothesis.exists() or not hypothesis.is_file():
        raise FileNotFoundError(f"Hypothesis path not found: {hypothesis}")

    outputs_root = Path(args.outputs_root)
    phase_root = resolve_output_phase_root(outputs_root=outputs_root, phase=args.phase)
    phase_root.mkdir(parents=True, exist_ok=True)

    stable_root = resolve_experiment_root(
        outputs_root=outputs_root,
        phase=args.phase,
        experiment_name=args.name,
        variant="stable",
    )
    volatile_root = resolve_experiment_root(
        outputs_root=outputs_root,
        phase=args.phase,
        experiment_name=args.name,
        variant="vol",
    )
    stable_root.mkdir(parents=True, exist_ok=True)
    volatile_root.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("EXPERIMENT OUTPUT ROOTS")
    print(f"Phase: {args.phase}")
    print("Stable root:")
    print(f"  {stable_root}")
    print("Vol root:")
    print(f"  {volatile_root}")
    print("=" * 80)

    log_path = resolve_pipeline_log_path(outputs_root=outputs_root, phase=args.phase, experiment_name=args.name)
    capture_command_logs = not args.no_command_log_capture
    command_log_dir = Path(args.command_log_dir) if args.command_log_dir else resolve_command_log_dir(outputs_root=outputs_root, phase=args.phase, experiment_name=args.name)
    command_manifest_path = command_log_dir / "command_log_manifest.json"
    commands_log: list[dict[str, Any]] = []
    log_path.write_text(f"Pipeline start: {utc_now_iso()}\n", encoding="utf-8")

    cleanup_delete_nonretained = not args.no_cleanup_delete_nonretained_runs
    cleanup_delete_logs = not args.no_cleanup_delete_logs
    if args.skip_cleanup_delete_nonretained_runs:
        cleanup_delete_nonretained = False

    db: ResearchDB | None = None
    hypothesis_id: str | None = None
    stable_experiment_id: str | None = None
    volatile_experiment_id: str | None = None
    pipeline_run_id: str | None = None

    if args.research_db:
        db = ResearchDB(args.research_db, repo_root=project_root)
        db.init_schema()
        hypothesis_id = db.upsert_hypothesis_by_name(
            name=args.name,
            yaml_path=hypothesis,
            status="IMPLEMENTED",
            metadata={"phase": args.phase},
        )
        print(f"[db] Created/upserted hypothesis: {hypothesis_id}")
        stable_experiment_id = db.create_experiment(
            hypothesis_id=hypothesis_id,
            name=f"{args.name}_stable",
            phase=args.phase,
            dataset_type="stable",
            experiment_root=stable_root,
            status="PENDING",
            max_workers=args.max_workers,
            config_path=args.config,
            local_config_path=args.local_config,
            data_path=args.stable_data or f"{args.data_kind}:{args.data_root}:stable",
        )
        volatile_experiment_id = db.create_experiment(
            hypothesis_id=hypothesis_id,
            name=f"{args.name}_volatile",
            phase=args.phase,
            dataset_type="volatile",
            experiment_root=volatile_root,
            status="PENDING",
            max_workers=args.max_workers,
            config_path=args.config,
            local_config_path=args.local_config,
            data_path=args.vol_data or f"{args.data_kind}:{args.data_root}:volatile",
        )
        print(f"[db] Created experiments: stable={stable_experiment_id}, volatile={volatile_experiment_id}")
        pipeline_run_id = db.create_pipeline_run(
            name=args.name,
            phase=args.phase,
            hypothesis_path=hypothesis,
            hypothesis_id=hypothesis_id,
            stable_experiment_id=stable_experiment_id,
            volatile_experiment_id=volatile_experiment_id,
            log_path=log_path,
            commands=commands_log,
        )
        print(f"[db] Created pipeline run: {pipeline_run_id}")
        db_register_artifact(
            db,
            artifact_type="hypothesis_yaml",
            path=hypothesis,
            hypothesis_id=hypothesis_id,
            pipeline_run_id=pipeline_run_id,
        )
        db_register_artifact(
            db,
            artifact_type="pipeline_log",
            path=log_path,
            hypothesis_id=hypothesis_id,
            pipeline_run_id=pipeline_run_id,
        )

    try:
        db_status_update(db, pipeline_run_id, "STRATEGY_ADMISSION", commands_log)
        admission_path = phase_root / f"{args.name}_strategy_admission.json"
        admission = validate_hypothesis_admission(hypothesis, require_truth_contract=True)
        write_strategy_admission_report(admission, admission_path)
        db_register_artifact(
            db,
            artifact_type="strategy_admission_report",
            path=admission_path,
            hypothesis_id=hypothesis_id,
            pipeline_run_id=pipeline_run_id,
        )
        if admission.status != "PASS":
            details = "; ".join(f"{issue.check}: {issue.message}" for issue in admission.issues)
            raise RuntimeError(f"Strategy admission failed before grid execution: {details}")
        print(f"[admission] PASS: {admission_path}")

        db_status_update(db, pipeline_run_id, "BUILDING_MANIFESTS", commands_log)
        print("[1/8] Building stable manifest")
        build_manifest(
            hypothesis=hypothesis,
            experiment_root=stable_root,
            phase=args.phase,
            project_root=project_root,
            dry_run=args.dry_run,
            log_path=log_path,
            commands_log=commands_log,
            step="build_manifest_stable",
            command_log_dir=command_log_dir,
            failure_tail_lines=args.failure_tail_lines,
            capture_logs=capture_command_logs,
        )

        print("[2/8] Building volatile manifest")
        build_manifest(
            hypothesis=hypothesis,
            experiment_root=volatile_root,
            phase=args.phase,
            project_root=project_root,
            dry_run=args.dry_run,
            log_path=log_path,
            commands_log=commands_log,
            step="build_manifest_volatile",
            command_log_dir=command_log_dir,
            failure_tail_lines=args.failure_tail_lines,
            capture_logs=capture_command_logs,
        )

        stable_manifest = discover_manifest(stable_root)
        volatile_manifest = discover_manifest(volatile_root)
        if db is not None:
            if stable_experiment_id:
                db.update_experiment(stable_experiment_id, manifest_path=stable_manifest, status="MANIFEST_BUILT")
                db_register_artifact(
                    db,
                    artifact_type="manifest_csv",
                    path=stable_manifest,
                    hypothesis_id=hypothesis_id,
                    experiment_id=stable_experiment_id,
                    pipeline_run_id=pipeline_run_id,
                )
            if volatile_experiment_id:
                db.update_experiment(volatile_experiment_id, manifest_path=volatile_manifest, status="MANIFEST_BUILT")
                db_register_artifact(
                    db,
                    artifact_type="manifest_csv",
                    path=volatile_manifest,
                    hypothesis_id=hypothesis_id,
                    experiment_id=volatile_experiment_id,
                    pipeline_run_id=pipeline_run_id,
                )

        db_status_update(db, pipeline_run_id, "RUNNING_BACKTESTS", commands_log)
        if not args.skip_run:
            if args.parallel_datasets:
                stable_workers, volatile_workers = split_dataset_workers(args.max_workers)
                volatile_workers = resolve_volatile_workers(
                    volatile_workers,
                    args.volatile_max_workers,
                    data_kind=args.data_kind,
                )
                print(
                    "[3/8] Running stable and volatile backtests concurrently "
                    f"(stable_workers={stable_workers}, volatile_workers={volatile_workers})"
                )
                with ThreadPoolExecutor(max_workers=2) as executor:
                    futures = [
                        executor.submit(
                            run_backtest,
                            experiment_root=stable_root,
                            manifest_path=stable_manifest,
                            config=args.config,
                            local_config=args.local_config,
                            data_path=args.stable_data,
                            data_root=args.data_root,
                            data_kind=args.data_kind,
                            exchange=args.exchange,
                            universe="stable",
                            timeframe=args.timeframe,
                            stable_manifest=args.stable_manifest,
                            membership_path=args.membership_path,
                            max_workers=stable_workers,
                            max_workers_auto=args.max_workers_auto,
                            reserve_ram_gb=args.reserve_ram_gb,
                            max_ram_per_worker_gb=args.max_ram_per_worker_gb,
                            min_free_ram_gb=args.min_free_ram_gb,
                            run_timeout_seconds=args.run_timeout_seconds,
                            fail_fast=args.fail_fast,
                            resume_strict=not args.no_resume_strict,
                            phase=args.phase,
                            project_root=project_root,
                            dry_run=args.dry_run,
                            log_path=log_path,
                            commands_log=commands_log,
                            step="run_backtest_stable",
                            command_log_dir=command_log_dir / "run_backtest_stable",
                            failure_tail_lines=args.failure_tail_lines,
                            capture_logs=capture_command_logs,
                        ),
                        executor.submit(
                            run_backtest,
                            experiment_root=volatile_root,
                            manifest_path=volatile_manifest,
                            config=args.config,
                            local_config=args.local_config,
                            data_path=args.vol_data,
                            data_root=args.data_root,
                            data_kind=args.data_kind,
                            exchange=args.exchange,
                            universe="volatile",
                            timeframe=args.timeframe,
                            stable_manifest=args.stable_manifest,
                            membership_path=args.membership_path,
                            max_workers=volatile_workers,
                            max_workers_auto=args.max_workers_auto,
                            reserve_ram_gb=args.reserve_ram_gb,
                            max_ram_per_worker_gb=args.max_ram_per_worker_gb,
                            min_free_ram_gb=args.min_free_ram_gb,
                            run_timeout_seconds=args.run_timeout_seconds,
                            fail_fast=args.fail_fast,
                            resume_strict=not args.no_resume_strict,
                            phase=args.phase,
                            project_root=project_root,
                            dry_run=args.dry_run,
                            log_path=log_path,
                            commands_log=commands_log,
                            step="run_backtest_volatile",
                            command_log_dir=command_log_dir / "run_backtest_volatile",
                            failure_tail_lines=args.failure_tail_lines,
                            capture_logs=capture_command_logs,
                        ),
                    ]
                    for future in futures:
                        future.result()
                print("[4/8] Stable and volatile backtests completed")
            else:
                volatile_workers = resolve_volatile_workers(
                    args.max_workers,
                    args.volatile_max_workers,
                    data_kind=args.data_kind,
                )
                print("[3/8] Running stable backtest")
                run_backtest(
                    experiment_root=stable_root,
                    manifest_path=stable_manifest,
                    config=args.config,
                    local_config=args.local_config,
                    data_path=args.stable_data,
                    data_root=args.data_root,
                    data_kind=args.data_kind,
                    exchange=args.exchange,
                    universe="stable",
                    timeframe=args.timeframe,
                    stable_manifest=args.stable_manifest,
                    membership_path=args.membership_path,
                    max_workers=args.max_workers,
                    max_workers_auto=args.max_workers_auto,
                    reserve_ram_gb=args.reserve_ram_gb,
                    max_ram_per_worker_gb=args.max_ram_per_worker_gb,
                    min_free_ram_gb=args.min_free_ram_gb,
                    run_timeout_seconds=args.run_timeout_seconds,
                    fail_fast=args.fail_fast,
                    resume_strict=not args.no_resume_strict,
                    phase=args.phase,
                    project_root=project_root,
                    dry_run=args.dry_run,
                    log_path=log_path,
                    commands_log=commands_log,
                    step="run_backtest_stable",
                    command_log_dir=command_log_dir,
                    failure_tail_lines=args.failure_tail_lines,
                    capture_logs=capture_command_logs,
                )

                print(f"[4/8] Running volatile backtest (workers={volatile_workers})")
                run_backtest(
                    experiment_root=volatile_root,
                    manifest_path=volatile_manifest,
                    config=args.config,
                    local_config=args.local_config,
                    data_path=args.vol_data,
                    data_root=args.data_root,
                    data_kind=args.data_kind,
                    exchange=args.exchange,
                    universe="volatile",
                    timeframe=args.timeframe,
                    stable_manifest=args.stable_manifest,
                    membership_path=args.membership_path,
                    max_workers=volatile_workers,
                    max_workers_auto=args.max_workers_auto,
                    reserve_ram_gb=args.reserve_ram_gb,
                    max_ram_per_worker_gb=args.max_ram_per_worker_gb,
                    min_free_ram_gb=args.min_free_ram_gb,
                    run_timeout_seconds=args.run_timeout_seconds,
                    fail_fast=args.fail_fast,
                    resume_strict=not args.no_resume_strict,
                    phase=args.phase,
                    project_root=project_root,
                    dry_run=args.dry_run,
                    log_path=log_path,
                    commands_log=commands_log,
                    step="run_backtest_volatile",
                    command_log_dir=command_log_dir,
                    failure_tail_lines=args.failure_tail_lines,
                    capture_logs=capture_command_logs,
                )
            if db is not None:
                if stable_experiment_id:
                    db.update_experiment_status(stable_experiment_id, "RUN_COMPLETE")
                if volatile_experiment_id:
                    db.update_experiment_status(volatile_experiment_id, "RUN_COMPLETE")
        else:
            print("[3/8] Running stable backtest (skipped)")
            print("[4/8] Running volatile backtest (skipped)")

        db_status_update(db, pipeline_run_id, "TRUTH_VALIDATION", commands_log)
        if not args.skip_truth_validation:
            print("[4.5/8] Validating experiment truth invariants")
            run_truth_validation(
                experiment_root=stable_root,
                project_root=project_root,
                dry_run=args.dry_run,
                log_path=log_path,
                commands_log=commands_log,
                step="truth_validation_stable",
                command_log_dir=command_log_dir,
                failure_tail_lines=args.failure_tail_lines,
                capture_logs=capture_command_logs,
            )
            run_truth_validation(
                experiment_root=volatile_root,
                project_root=project_root,
                dry_run=args.dry_run,
                log_path=log_path,
                commands_log=commands_log,
                step="truth_validation_volatile",
                command_log_dir=command_log_dir,
                failure_tail_lines=args.failure_tail_lines,
                capture_logs=capture_command_logs,
            )
            if db is not None:
                if stable_experiment_id:
                    db_register_artifact(
                        db,
                        artifact_type="truth_validation_report_json",
                        path=stable_root / "summaries" / "truth_validation_report.json",
                        hypothesis_id=hypothesis_id,
                        experiment_id=stable_experiment_id,
                        pipeline_run_id=pipeline_run_id,
                    )
                if volatile_experiment_id:
                    db_register_artifact(
                        db,
                        artifact_type="truth_validation_report_json",
                        path=volatile_root / "summaries" / "truth_validation_report.json",
                        hypothesis_id=hypothesis_id,
                        experiment_id=volatile_experiment_id,
                        pipeline_run_id=pipeline_run_id,
                    )
        else:
            print("[4.5/8] Validating experiment truth invariants (skipped)")

        db_status_update(db, pipeline_run_id, "POST_ANALYSIS", commands_log)
        if not args.skip_analysis:
            print("[5/8] Running post-run analysis")
            run_post_analysis(
                experiment_root=stable_root,
                project_root=project_root,
                dry_run=args.dry_run,
                log_path=log_path,
                commands_log=commands_log,
                step="post_analysis_stable",
                command_log_dir=command_log_dir,
                failure_tail_lines=args.failure_tail_lines,
                capture_logs=capture_command_logs,
            )
            run_post_analysis(
                experiment_root=volatile_root,
                project_root=project_root,
                dry_run=args.dry_run,
                log_path=log_path,
                commands_log=commands_log,
                step="post_analysis_volatile",
                command_log_dir=command_log_dir,
                failure_tail_lines=args.failure_tail_lines,
                capture_logs=capture_command_logs,
            )
            if db is not None:
                if stable_experiment_id:
                    db.update_experiment_status(stable_experiment_id, "POST_ANALYSIS_COMPLETE")
                if volatile_experiment_id:
                    db.update_experiment_status(volatile_experiment_id, "POST_ANALYSIS_COMPLETE")

                stable_summary = stable_root / "summaries" / "run_summary.csv"
                vol_summary = volatile_root / "summaries" / "run_summary.csv"
                db_register_artifact(
                    db,
                    artifact_type="run_summary_csv",
                    path=stable_summary,
                    hypothesis_id=hypothesis_id,
                    experiment_id=stable_experiment_id,
                    pipeline_run_id=pipeline_run_id,
                )
                db_register_artifact(
                    db,
                    artifact_type="run_summary_csv",
                    path=vol_summary,
                    hypothesis_id=hypothesis_id,
                    experiment_id=volatile_experiment_id,
                    pipeline_run_id=pipeline_run_id,
                )
                if stable_experiment_id and stable_summary.exists() and not args.dry_run:
                    imported = db.import_runs_from_summary_csv(stable_experiment_id, stable_summary)
                    print(f"[db] Imported runs from summary (stable): {imported}")
                if volatile_experiment_id and vol_summary.exists() and not args.dry_run:
                    imported = db.import_runs_from_summary_csv(volatile_experiment_id, vol_summary)
                    print(f"[db] Imported runs from summary (volatile): {imported}")
        else:
            print("[5/8] Running post-run analysis (skipped)")

        db_status_update(db, pipeline_run_id, "EXTRACTING_DATASETS", commands_log)
        if not args.skip_extract:
            print("[6/8] Extracting experiment datasets")
            extract_dataset(
                experiment_root=stable_root,
                project_root=project_root,
                dry_run=args.dry_run,
                log_path=log_path,
                commands_log=commands_log,
                step="extract_dataset_stable",
                command_log_dir=command_log_dir,
                failure_tail_lines=args.failure_tail_lines,
                capture_logs=capture_command_logs,
            )
            extract_dataset(
                experiment_root=volatile_root,
                project_root=project_root,
                dry_run=args.dry_run,
                log_path=log_path,
                commands_log=commands_log,
                step="extract_dataset_volatile",
                command_log_dir=command_log_dir,
                failure_tail_lines=args.failure_tail_lines,
                capture_logs=capture_command_logs,
            )
            if db is not None:
                if stable_experiment_id:
                    db.update_experiment_status(stable_experiment_id, "DATASET_EXTRACTED")
                if volatile_experiment_id:
                    db.update_experiment_status(volatile_experiment_id, "DATASET_EXTRACTED")
                for exp_id, root in ((stable_experiment_id, stable_root), (volatile_experiment_id, volatile_root)):
                    if exp_id is None:
                        continue
                    db_register_artifact(
                        db,
                        artifact_type="runs_dataset_parquet",
                        path=root / "research_data" / "runs_dataset.parquet",
                        hypothesis_id=hypothesis_id,
                        experiment_id=exp_id,
                        pipeline_run_id=pipeline_run_id,
                    )
                    db_register_artifact(
                        db,
                        artifact_type="trades_dataset_parquet",
                        path=root / "research_data" / "trades_dataset.parquet",
                        hypothesis_id=hypothesis_id,
                        experiment_id=exp_id,
                        pipeline_run_id=pipeline_run_id,
                    )
        else:
            print("[6/8] Extracting experiment datasets (skipped)")

        if not args.skip_truth_validation and not args.skip_extract:
            print("[6.5/8] Validating extracted research datasets")
            run_truth_validation(
                experiment_root=stable_root,
                project_root=project_root,
                dry_run=args.dry_run,
                log_path=log_path,
                commands_log=commands_log,
                step="truth_validation_extracted_stable",
                command_log_dir=command_log_dir,
                failure_tail_lines=args.failure_tail_lines,
                capture_logs=capture_command_logs,
            )
            run_truth_validation(
                experiment_root=volatile_root,
                project_root=project_root,
                dry_run=args.dry_run,
                log_path=log_path,
                commands_log=commands_log,
                step="truth_validation_extracted_volatile",
                command_log_dir=command_log_dir,
                failure_tail_lines=args.failure_tail_lines,
                capture_logs=capture_command_logs,
            )

        cleanup_ran = False
        db_status_update(db, pipeline_run_id, "CLEANING", commands_log)
        if not args.skip_cleanup:
            print("[7/8] Cleaning heavy run logs")
            if not args.dry_run:
                verify_cleanup_prerequisites(stable_root, volatile_root)
            cleanup_experiment(
                experiment_root=stable_root,
                retain_top_n=args.retain_top_n,
                retain_median=args.retain_median,
                retain_worst=args.retain_worst,
                delete_logs=cleanup_delete_logs,
                delete_nonretained_runs=cleanup_delete_nonretained,
                project_root=project_root,
                dry_run=args.dry_run,
                log_path=log_path,
                commands_log=commands_log,
                step="cleanup_stable",
                command_log_dir=command_log_dir,
                failure_tail_lines=args.failure_tail_lines,
                capture_logs=capture_command_logs,
            )
            cleanup_experiment(
                experiment_root=volatile_root,
                retain_top_n=args.retain_top_n,
                retain_median=args.retain_median,
                retain_worst=args.retain_worst,
                delete_logs=cleanup_delete_logs,
                delete_nonretained_runs=cleanup_delete_nonretained,
                project_root=project_root,
                dry_run=args.dry_run,
                log_path=log_path,
                commands_log=commands_log,
                step="cleanup_volatile",
                command_log_dir=command_log_dir,
                failure_tail_lines=args.failure_tail_lines,
                capture_logs=capture_command_logs,
            )
            cleanup_ran = True
            if db is not None:
                if stable_experiment_id:
                    db.update_experiment_status(stable_experiment_id, "CLEANED")
                if volatile_experiment_id:
                    db.update_experiment_status(volatile_experiment_id, "CLEANED")
        else:
            print("[7/8] Cleaning heavy run logs (skipped)")

        db_status_update(db, pipeline_run_id, "CREATING_VERDICT_BUNDLE", commands_log)
        print("[8/8] Creating verdict input bundle")
        bundle_dir = create_verdict_bundle(
            name=args.name,
            hypothesis=hypothesis,
            phase=args.phase,
            outputs_root=outputs_root,
            stable_root=stable_root,
            volatile_root=volatile_root,
            stable_manifest=stable_manifest,
            volatile_manifest=volatile_manifest,
            commands_log=commands_log,
            cleanup_ran=cleanup_ran,
        )

        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(f"Pipeline end: {utc_now_iso()}\n")
            fh.write("FINAL_STATUS: SUCCESS\n")
            fh.write(f"VERDICT_BUNDLE: {bundle_dir.resolve()}\n")

        if db is not None and pipeline_run_id is not None:
            db.complete_pipeline_run(pipeline_run_id, verdict_bundle_path=bundle_dir, commands=commands_log)
            print("[db] Updated status: COMPLETED")
            db_register_artifact(
                db,
                artifact_type="verdict_bundle",
                path=bundle_dir / "manifest.json",
                hypothesis_id=hypothesis_id,
                pipeline_run_id=pipeline_run_id,
            )

        try:
            cards_result = build_and_write_intelligence_cards(
                name=args.name,
                phase=args.phase,
                hypothesis_path=hypothesis,
                stable_root=stable_root,
                volatile_root=volatile_root,
                output_dir=strategy_terminal_cards_dir(outputs_root=outputs_root, phase=args.phase, name=args.name),
                project_root=project_root,
                pipeline_run_id=pipeline_run_id,
                verdict_bundle_dir=bundle_dir,
                command_log_dir=command_log_dir,
                pipeline_log_path=log_path,
                db=db,
                hypothesis_id=hypothesis_id,
            )
            print(f"[terminal] Strategy Research Terminal cards: {cards_result.bundle_json}")
        except Exception as card_exc:
            print(f"[terminal] WARNING: failed to write Strategy Research Terminal cards: {card_exc}", file=sys.stderr)

        print(f"Done. Verdict bundle: {bundle_dir}")
        return 0
    except Exception as exc:
        if isinstance(exc, PipelineCommandError):
            with log_path.open("a", encoding="utf-8") as fh:
                fh.write(exc.to_failure_block() + "\n")
            for artifact_type, path in (("command_stdout_log", exc.stdout_path), ("command_stderr_log", exc.stderr_path)):
                if path:
                    db_register_artifact(
                        db,
                        artifact_type=artifact_type,
                        path=Path(path),
                        hypothesis_id=hypothesis_id,
                        pipeline_run_id=pipeline_run_id,
                        metadata={
                            "step": exc.step,
                            "command": exc.cmd,
                            "returncode": exc.returncode,
                            "root_cause_hint": exc.root_cause_hint,
                        },
                    )
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(f"Pipeline end: {utc_now_iso()}\n")
            fh.write("FINAL_STATUS: FAILURE\n")
            fh.write(f"ERROR: {exc}\n")
        command_manifest_path.parent.mkdir(parents=True, exist_ok=True)
        command_manifest_path.write_text(json.dumps(commands_log, indent=2), encoding="utf-8")
        if db is not None and pipeline_run_id is not None:
            error_message = exc.compact_message() if isinstance(exc, PipelineCommandError) else str(exc)
            db.fail_pipeline_run(pipeline_run_id, error_message, commands=commands_log)
            if stable_experiment_id:
                db.update_experiment_status(stable_experiment_id, "FAILED")
            if volatile_experiment_id:
                db.update_experiment_status(volatile_experiment_id, "FAILED")
            print("[db] Updated status: FAILED")
        try:
            error_message = exc.compact_message() if isinstance(exc, PipelineCommandError) else str(exc)
            cards_result = build_and_write_failure_cards(
                name=args.name,
                phase=args.phase,
                hypothesis_path=hypothesis,
                stable_root=stable_root,
                volatile_root=volatile_root,
                output_dir=strategy_terminal_cards_dir(outputs_root=outputs_root, phase=args.phase, name=args.name),
                project_root=project_root,
                pipeline_run_id=pipeline_run_id,
                command_log_dir=command_log_dir,
                pipeline_log_path=log_path,
                error_message=error_message,
                db=db,
                hypothesis_id=hypothesis_id,
            )
            print(f"[terminal] Strategy Research Terminal failure cards: {cards_result.bundle_json}")
        except Exception as card_exc:
            print(f"[terminal] WARNING: failed to write Strategy Research Terminal failure cards: {card_exc}", file=sys.stderr)
        raise
    finally:
        if commands_log:
            command_manifest_path.parent.mkdir(parents=True, exist_ok=True)
            command_manifest_path.write_text(json.dumps(commands_log, indent=2), encoding="utf-8")
        if db is not None:
            db.close()


if __name__ == "__main__":
    raise SystemExit(main())
