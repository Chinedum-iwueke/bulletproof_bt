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
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from orchestrator.db import ResearchDB


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run stable+volatile experiment automation pipeline.")
    parser.add_argument("--hypothesis", required=True, help="Path to hypothesis YAML.")
    parser.add_argument("--name", required=True, help="Experiment name prefix.")
    parser.add_argument("--max-workers", type=int, default=6)

    parser.add_argument("--phase", default="tier2")
    parser.add_argument("--config", default="configs/engine.yaml")
    parser.add_argument("--local-config", default="configs/local/engine.lab.yaml")
    parser.add_argument(
        "--stable-data",
        default="/home/omenka/research_data/bt/curated/stable_data_1m_canonical",
    )
    parser.add_argument(
        "--vol-data",
        default="/home/omenka/research_data/bt/curated/vol_data_1m_canonical",
    )
    parser.add_argument("--outputs-root", default="outputs")

    parser.add_argument("--retain-top-n", type=int, default=2)
    parser.add_argument("--retain-median", type=int, default=1)
    parser.add_argument("--retain-worst", type=int, default=1)

    parser.add_argument("--skip-run", action="store_true", default=False)
    parser.add_argument("--skip-analysis", action="store_true", default=False)
    parser.add_argument("--skip-extract", action="store_true", default=False)
    parser.add_argument("--skip-cleanup", action="store_true", default=False)
    parser.add_argument("--skip-cleanup-delete-nonretained-runs", action="store_true", default=False)

    parser.add_argument("--no-cleanup-delete-nonretained-runs", action="store_true", default=False)
    parser.add_argument("--no-cleanup-delete-logs", action="store_true", default=False)
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--research-db", default=None, help="Optional SQLite DB path for lifecycle tracking.")
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
) -> None:
    printable = " ".join(cmd)
    print(f"$ {printable}")
    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(f"[{utc_now_iso()}] STEP={step}\n")
        fh.write(f"CMD: {printable}\n")

    commands_log.append({"step": step, "cmd": cmd})

    if dry_run:
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write("STATUS: DRY_RUN_SKIPPED\n\n")
        return

    subprocess.run(cmd, check=True)

    with log_path.open("a", encoding="utf-8") as fh:
        fh.write("STATUS: SUCCESS\n\n")


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
    run_command(cmd, step=step, dry_run=dry_run, log_path=log_path, commands_log=commands_log)


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
    data_path: str,
    max_workers: int,
    phase: str,
    project_root: Path,
    dry_run: bool,
    log_path: Path,
    commands_log: list[dict[str, Any]],
    step: str,
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
        "--data",
        data_path,
        "--phase",
        phase,
        "--max-workers",
        str(max_workers),
        "--skip-completed",
    ]
    run_command(cmd, step=step, dry_run=dry_run, log_path=log_path, commands_log=commands_log)


def run_post_analysis(
    *,
    experiment_root: Path,
    project_root: Path,
    dry_run: bool,
    log_path: Path,
    commands_log: list[dict[str, Any]],
    step: str,
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
    run_command(cmd, step=step, dry_run=dry_run, log_path=log_path, commands_log=commands_log)


def extract_dataset(
    *,
    experiment_root: Path,
    project_root: Path,
    dry_run: bool,
    log_path: Path,
    commands_log: list[dict[str, Any]],
    step: str,
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
    run_command(cmd, step=step, dry_run=dry_run, log_path=log_path, commands_log=commands_log)


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

    run_command(cmd, step=step, dry_run=dry_run, log_path=log_path, commands_log=commands_log)


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
    bundle_dir = outputs_root / f"{name}_verdict_bundle"
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
    )
    print(f"[db] Registered artifact: {artifact_type} -> {path}")


def main() -> int:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[1]

    hypothesis = Path(args.hypothesis)
    if not hypothesis.exists() or not hypothesis.is_file():
        raise FileNotFoundError(f"Hypothesis path not found: {hypothesis}")

    outputs_root = Path(args.outputs_root)
    outputs_root.mkdir(parents=True, exist_ok=True)

    stable_root = outputs_root / f"{args.name}_parallel_stable"
    volatile_root = outputs_root / f"{args.name}_parallel_vol"
    stable_root.mkdir(parents=True, exist_ok=True)
    volatile_root.mkdir(parents=True, exist_ok=True)

    log_path = outputs_root / f"{args.name}_pipeline.log"
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
            data_path=args.stable_data,
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
            data_path=args.vol_data,
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
            print("[3/8] Running stable backtest")
            run_backtest(
                experiment_root=stable_root,
                manifest_path=stable_manifest,
                config=args.config,
                local_config=args.local_config,
                data_path=args.stable_data,
                max_workers=args.max_workers,
                phase=args.phase,
                project_root=project_root,
                dry_run=args.dry_run,
                log_path=log_path,
                commands_log=commands_log,
                step="run_backtest_stable",
            )

            print("[4/8] Running volatile backtest")
            run_backtest(
                experiment_root=volatile_root,
                manifest_path=volatile_manifest,
                config=args.config,
                local_config=args.local_config,
                data_path=args.vol_data,
                max_workers=args.max_workers,
                phase=args.phase,
                project_root=project_root,
                dry_run=args.dry_run,
                log_path=log_path,
                commands_log=commands_log,
                step="run_backtest_volatile",
            )
            if db is not None:
                if stable_experiment_id:
                    db.update_experiment_status(stable_experiment_id, "RUN_COMPLETE")
                if volatile_experiment_id:
                    db.update_experiment_status(volatile_experiment_id, "RUN_COMPLETE")
        else:
            print("[3/8] Running stable backtest (skipped)")
            print("[4/8] Running volatile backtest (skipped)")

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
            )
            run_post_analysis(
                experiment_root=volatile_root,
                project_root=project_root,
                dry_run=args.dry_run,
                log_path=log_path,
                commands_log=commands_log,
                step="post_analysis_volatile",
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
            )
            extract_dataset(
                experiment_root=volatile_root,
                project_root=project_root,
                dry_run=args.dry_run,
                log_path=log_path,
                commands_log=commands_log,
                step="extract_dataset_volatile",
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

        print(f"Done. Verdict bundle: {bundle_dir}")
        return 0
    except Exception as exc:
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(f"Pipeline end: {utc_now_iso()}\n")
            fh.write("FINAL_STATUS: FAILURE\n")
            fh.write(f"ERROR: {exc}\n")
        if db is not None and pipeline_run_id is not None:
            db.fail_pipeline_run(pipeline_run_id, str(exc), commands=commands_log)
            if stable_experiment_id:
                db.update_experiment_status(stable_experiment_id, "FAILED")
            if volatile_experiment_id:
                db.update_experiment_status(volatile_experiment_id, "FAILED")
            print("[db] Updated status: FAILED")
        raise
    finally:
        if db is not None:
            db.close()


if __name__ == "__main__":
    raise SystemExit(main())
