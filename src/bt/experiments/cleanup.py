"""Production-grade cleanup pipeline for experiment run artifacts."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from time import perf_counter
from typing import Any

import pandas as pd

from bt.experiments.dataset_builder import REQUIRED_OUTPUT_FILES, extract_experiment_dataset

RUNS_DATASET_FILENAME = REQUIRED_OUTPUT_FILES["runs_dataset"]
RETENTION_PLAN_FILENAME = "retention_plan.json"
RETAINED_RUNS_FILENAME = "retained_runs.csv"
DELETED_RUNS_FILENAME = "deleted_runs.csv"
CLEANUP_LOG_FILENAME = "cleanup_log.json"

HEAVY_LOG_FILENAMES = ("decisions.jsonl", "fills.jsonl")
OPTIONAL_RETAINED_DELETE_FILENAMES = ("equity.csv",)


@dataclass(frozen=True)
class CleanupConfig:
    experiment_root: Path
    runs_glob: str = "runs/*"
    out_dir: Path | None = None
    retain_top_n: int = 5
    retain_median: int = 1
    retain_worst: int = 1
    ranking_metric: str = "net_pnl"
    delete_logs: bool = False
    delete_nonretained_runs: bool = False
    keep_equity_for_retained: bool = False
    skip_existing_extraction: bool = False
    overwrite_extraction: bool = False
    dry_run: bool = False
    verbose: bool = False


@dataclass
class RankedRun:
    run_id: str
    ranking_metric_value: float | None
    net_pnl: float | None
    sharpe: float | None
    max_drawdown: float | None
    trade_count: int | None


@dataclass
class DeletionRecord:
    run_id: str
    deletion_action: str
    deletion_reason: str
    existed_before_delete: bool
    delete_success: bool
    notes: str


@dataclass
class CleanupArtifacts:
    retention_plan: dict[str, Any]
    retained_runs: pd.DataFrame
    deleted_runs: pd.DataFrame
    cleanup_log: dict[str, Any]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _safe_int(value: Any) -> int | None:
    parsed = _safe_float(value)
    return int(parsed) if parsed is not None else None


def validate_extraction_outputs(out_dir: Path) -> tuple[bool, list[str]]:
    errors: list[str] = []
    output_paths = {name: out_dir / filename for name, filename in REQUIRED_OUTPUT_FILES.items()}
    for name, path in output_paths.items():
        if not path.exists():
            errors.append(f"missing required extraction output: {name} -> {path}")

    runs_dataset_path = out_dir / RUNS_DATASET_FILENAME
    if runs_dataset_path.exists():
        try:
            runs_df = pd.read_parquet(runs_dataset_path)
        except Exception as exc:  # pragma: no cover - defensive
            errors.append(f"failed to read runs dataset: {runs_dataset_path} ({exc})")
        else:
            if runs_df.empty:
                errors.append(f"runs dataset is empty: {runs_dataset_path}")
            if "run_id" not in runs_df.columns:
                errors.append("runs dataset missing required column: run_id")

    return (len(errors) == 0, errors)


def ensure_research_datasets(config: CleanupConfig) -> dict[str, Any]:
    out_dir = (config.out_dir if config.out_dir is not None else config.experiment_root / "research_data").resolve()

    valid_existing, existing_errors = validate_extraction_outputs(out_dir)
    if valid_existing and not config.overwrite_extraction:
        return {"status": "ok", "mode": "already_valid", "out_dir": str(out_dir), "warnings": []}

    result = extract_experiment_dataset(
        experiment_root=config.experiment_root,
        runs_glob=config.runs_glob,
        out_dir=out_dir,
        skip_existing=config.skip_existing_extraction,
        overwrite=config.overwrite_extraction,
        verbose=config.verbose,
    )

    valid_after, validation_errors = validate_extraction_outputs(out_dir)
    if not valid_after:
        raise ValueError(
            "extraction output validation failed after extraction attempt: "
            + "; ".join(validation_errors)
        )

    return {
        "status": "ok",
        "mode": result.get("status", "ok"),
        "out_dir": str(out_dir),
        "warnings": existing_errors,
        "extraction_result": result,
    }


def load_runs_dataset(out_dir: Path) -> pd.DataFrame:
    runs_dataset = out_dir / RUNS_DATASET_FILENAME
    if not runs_dataset.exists():
        raise ValueError(f"missing runs dataset: {runs_dataset}")
    runs_df = pd.read_parquet(runs_dataset)
    if runs_df.empty:
        raise ValueError(f"runs dataset is empty: {runs_dataset}")
    if "run_id" not in runs_df.columns:
        raise ValueError("runs_dataset.parquet is missing required run_id column")
    return runs_df.copy()


def rank_runs(runs_df: pd.DataFrame, ranking_metric: str) -> tuple[list[RankedRun], list[str]]:
    warnings: list[str] = []
    frame = runs_df.copy()
    for column in (ranking_metric, "net_pnl", "sharpe", "max_drawdown", "trade_count"):
        if column not in frame.columns:
            frame[column] = None

    ranking_numeric = pd.to_numeric(frame[ranking_metric], errors="coerce")
    frame["_ranking_metric_value"] = ranking_numeric

    valid = frame[frame["_ranking_metric_value"].notna()].copy()
    missing = frame[frame["_ranking_metric_value"].isna()].copy()
    if not missing.empty:
        warnings.append(
            f"{len(missing)} run(s) missing ranking metric {ranking_metric}; excluded from primary ranking"
        )

    if valid.empty:
        warnings.append(
            f"no runs have valid ranking metric {ranking_metric}; falling back to sharpe/max_drawdown ordering"
        )
        valid = frame.copy()

    valid["_sharpe"] = pd.to_numeric(valid["sharpe"], errors="coerce").fillna(float("-inf"))
    valid["_max_drawdown"] = pd.to_numeric(valid["max_drawdown"], errors="coerce").fillna(float("inf"))
    valid["_ranking_order"] = pd.to_numeric(valid["_ranking_metric_value"], errors="coerce").fillna(float("-inf"))

    ordered = valid.sort_values(
        by=["_ranking_order", "_sharpe", "_max_drawdown", "run_id"],
        ascending=[False, False, True, True],
    ).reset_index(drop=True)

    ranked_runs: list[RankedRun] = []
    for _, row in ordered.iterrows():
        ranked_runs.append(
            RankedRun(
                run_id=str(row["run_id"]),
                ranking_metric_value=_safe_float(row.get("_ranking_metric_value")),
                net_pnl=_safe_float(row.get("net_pnl")),
                sharpe=_safe_float(row.get("sharpe")),
                max_drawdown=_safe_float(row.get("max_drawdown")),
                trade_count=_safe_int(row.get("trade_count")),
            )
        )
    return ranked_runs, warnings


def select_retained_runs(
    ranked_runs: list[RankedRun],
    *,
    retain_top_n: int,
    retain_median: int,
    retain_worst: int,
) -> tuple[dict[str, set[str]], list[str]]:
    if not ranked_runs:
        raise ValueError("no ranked runs available for retention selection")

    reasons: dict[str, set[str]] = {}
    ordered = ranked_runs

    for run in ordered[: max(retain_top_n, 0)]:
        reasons.setdefault(run.run_id, set()).add("top")

    if retain_median > 0:
        median_idx = len(ordered) // 2
        reasons.setdefault(ordered[median_idx].run_id, set()).add("median")

    if retain_worst > 0:
        for run in ordered[-retain_worst:]:
            reasons.setdefault(run.run_id, set()).add("worst")

    retained = sorted(reasons.keys())
    if not retained:
        raise ValueError("retention policy selected zero runs; refusing cleanup")

    return reasons, retained


def build_retention_plan(
    *,
    config: CleanupConfig,
    retained_run_ids: list[str],
    deleted_run_ids: list[str],
) -> dict[str, Any]:
    return {
        "experiment_root": str(config.experiment_root),
        "created_at_utc": _utc_now_iso(),
        "ranking_metric": config.ranking_metric,
        "retain_top_n": config.retain_top_n,
        "retain_median": config.retain_median,
        "retain_worst": config.retain_worst,
        "delete_logs": config.delete_logs,
        "delete_nonretained_runs": config.delete_nonretained_runs,
        "keep_equity_for_retained": config.keep_equity_for_retained,
        "retained_run_ids": retained_run_ids,
        "deleted_run_ids": deleted_run_ids,
        "dry_run": config.dry_run,
    }


def _delete_path(path: Path, *, dry_run: bool) -> tuple[bool, str, int]:
    if not path.exists():
        return False, "path_missing", 0

    bytes_reclaimed = 0
    try:
        if path.is_file():
            bytes_reclaimed = path.stat().st_size
            if not dry_run:
                path.unlink()
            return True, "deleted_file", bytes_reclaimed

        if path.is_dir():
            files = [p for p in path.rglob("*") if p.is_file()]
            bytes_reclaimed = sum(p.stat().st_size for p in files)
            if not dry_run:
                for file_path in sorted(files, reverse=True):
                    file_path.unlink()
                for dir_path in sorted((p for p in path.rglob("*") if p.is_dir()), reverse=True):
                    dir_path.rmdir()
                path.rmdir()
            return True, "deleted_directory", bytes_reclaimed
    except Exception as exc:  # pragma: no cover - defensive
        return False, f"delete_failed: {exc}", 0

    return False, "unsupported_path", 0


def delete_run_logs(
    *,
    run_dirs: dict[str, Path],
    retained_run_ids: set[str],
    delete_logs: bool,
    keep_equity_for_retained: bool,
    dry_run: bool,
) -> tuple[list[DeletionRecord], list[str], list[str], int]:
    records: list[DeletionRecord] = []
    deleted_files: list[str] = []
    skipped: list[str] = []
    bytes_reclaimed = 0

    if not delete_logs:
        return records, deleted_files, skipped, bytes_reclaimed

    for run_id, run_dir in sorted(run_dirs.items()):
        targets = list(HEAVY_LOG_FILENAMES)
        if run_id in retained_run_ids and not keep_equity_for_retained:
            targets.extend(OPTIONAL_RETAINED_DELETE_FILENAMES)

        for filename in targets:
            target = run_dir / filename
            existed = target.exists()
            success, note, reclaimed = _delete_path(target, dry_run=dry_run)
            bytes_reclaimed += reclaimed
            if success:
                deleted_files.append(str(target))
            else:
                skipped.append(f"{target}: {note}")

            records.append(
                DeletionRecord(
                    run_id=run_id,
                    deletion_action=f"delete_file:{filename}",
                    deletion_reason="log_prune" if filename in HEAVY_LOG_FILENAMES else "retained_equity_prune",
                    existed_before_delete=existed,
                    delete_success=success,
                    notes=note,
                )
            )
    return records, deleted_files, skipped, bytes_reclaimed


def delete_nonretained_run_folders(
    *,
    run_dirs: dict[str, Path],
    retained_run_ids: set[str],
    enabled: bool,
    dry_run: bool,
) -> tuple[list[DeletionRecord], list[str], list[str], int]:
    records: list[DeletionRecord] = []
    deleted_folders: list[str] = []
    skipped: list[str] = []
    bytes_reclaimed = 0

    if not enabled:
        return records, deleted_folders, skipped, bytes_reclaimed

    for run_id, run_dir in sorted(run_dirs.items()):
        if run_id in retained_run_ids:
            continue
        existed = run_dir.exists()
        success, note, reclaimed = _delete_path(run_dir, dry_run=dry_run)
        bytes_reclaimed += reclaimed
        if success:
            deleted_folders.append(str(run_dir))
        else:
            skipped.append(f"{run_dir}: {note}")

        records.append(
            DeletionRecord(
                run_id=run_id,
                deletion_action="delete_folder:run_dir",
                deletion_reason="non_retained_folder_prune",
                existed_before_delete=existed,
                delete_success=success,
                notes=note,
            )
        )

    return records, deleted_folders, skipped, bytes_reclaimed


def write_cleanup_artifacts(*, out_dir: Path, artifacts: CleanupArtifacts) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / RETENTION_PLAN_FILENAME).write_text(
        json.dumps(artifacts.retention_plan, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    artifacts.retained_runs.to_csv(out_dir / RETAINED_RUNS_FILENAME, index=False)
    artifacts.deleted_runs.to_csv(out_dir / DELETED_RUNS_FILENAME, index=False)
    (out_dir / CLEANUP_LOG_FILENAME).write_text(
        json.dumps(artifacts.cleanup_log, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def run_experiment_cleanup(config: CleanupConfig) -> dict[str, Any]:
    start = perf_counter()
    experiment_root = config.experiment_root.resolve()

    if not experiment_root.exists():
        raise ValueError(f"experiment root does not exist: {experiment_root}")

    for required_dir in ("contract_snapshot", "manifests", "runs", "summaries"):
        if not (experiment_root / required_dir).exists():
            raise ValueError(f"experiment root missing required directory: {required_dir}")

    extraction_result = ensure_research_datasets(config)
    out_dir = Path(extraction_result["out_dir"]).resolve()
    runs_df = load_runs_dataset(out_dir)

    ranked_runs, ranking_warnings = rank_runs(runs_df, config.ranking_metric)
    reasons_map, retained_run_ids = select_retained_runs(
        ranked_runs,
        retain_top_n=config.retain_top_n,
        retain_median=config.retain_median,
        retain_worst=config.retain_worst,
    )

    ranked_lookup = {run.run_id: run for run in ranked_runs}

    run_dirs = {
        path.name: path
        for path in experiment_root.glob(config.runs_glob)
        if path.is_dir()
    }
    if not run_dirs:
        raise ValueError(f"no run directories discovered under {experiment_root} with glob={config.runs_glob!r}")

    deleted_run_ids = sorted(set(run_dirs.keys()) - set(retained_run_ids))

    retained_rows: list[dict[str, Any]] = []
    for run_id in retained_run_ids:
        ranked = ranked_lookup.get(run_id)
        retained_rows.append(
            {
                "run_id": run_id,
                "retention_reason": "|".join(sorted(reasons_map.get(run_id, {"unknown"}))),
                "ranking_metric_value": ranked.ranking_metric_value if ranked else None,
                "net_pnl": ranked.net_pnl if ranked else None,
                "sharpe": ranked.sharpe if ranked else None,
                "max_drawdown": ranked.max_drawdown if ranked else None,
                "trade_count": ranked.trade_count if ranked else None,
            }
        )

    deleted_plan_rows: list[dict[str, Any]] = []
    for run_id in sorted(run_dirs):
        if run_id in retained_run_ids:
            continue
        deleted_plan_rows.append(
            {
                "run_id": run_id,
                "deletion_action": "delete_folder:run_dir" if config.delete_nonretained_runs else "retain_folder",
                "deletion_reason": "non_retained_folder_prune" if config.delete_nonretained_runs else "non_retained_preserved",
                "existed_before_delete": run_dirs[run_id].exists(),
                "delete_success": False,
                "notes": "planned",
            }
        )

    retention_plan = build_retention_plan(
        config=config,
        retained_run_ids=retained_run_ids,
        deleted_run_ids=deleted_run_ids,
    )

    initial_artifacts = CleanupArtifacts(
        retention_plan=retention_plan,
        retained_runs=pd.DataFrame(retained_rows),
        deleted_runs=pd.DataFrame(deleted_plan_rows),
        cleanup_log={
            "created_at_utc": _utc_now_iso(),
            "stage": "plan_only",
            "warnings": ranking_warnings,
            "dry_run": config.dry_run,
        },
    )
    write_cleanup_artifacts(out_dir=out_dir, artifacts=initial_artifacts)

    log_records, deleted_files, skipped_logs, log_bytes = delete_run_logs(
        run_dirs=run_dirs,
        retained_run_ids=set(retained_run_ids),
        delete_logs=config.delete_logs,
        keep_equity_for_retained=config.keep_equity_for_retained,
        dry_run=config.dry_run,
    )
    folder_records, deleted_folders, skipped_folders, folder_bytes = delete_nonretained_run_folders(
        run_dirs=run_dirs,
        retained_run_ids=set(retained_run_ids),
        enabled=config.delete_nonretained_runs,
        dry_run=config.dry_run,
    )

    all_records = [*log_records, *folder_records]
    deleted_runs_df = pd.DataFrame(
        [
            {
                "run_id": record.run_id,
                "deletion_action": record.deletion_action,
                "deletion_reason": record.deletion_reason,
                "existed_before_delete": record.existed_before_delete,
                "delete_success": record.delete_success,
                "notes": record.notes,
            }
            for record in all_records
        ]
    )

    duration = perf_counter() - start
    cleanup_log = {
        "created_at_utc": _utc_now_iso(),
        "files_deleted": deleted_files,
        "folders_deleted": deleted_folders,
        "warnings": ranking_warnings,
        "skipped_deletions": [*skipped_logs, *skipped_folders],
        "total_bytes_reclaimed": int(log_bytes + folder_bytes),
        "duration_seconds": round(duration, 4),
        "dry_run": config.dry_run,
        "delete_logs": config.delete_logs,
        "delete_nonretained_runs": config.delete_nonretained_runs,
        "extraction": extraction_result,
    }

    final_artifacts = CleanupArtifacts(
        retention_plan=retention_plan,
        retained_runs=pd.DataFrame(retained_rows),
        deleted_runs=deleted_runs_df,
        cleanup_log=cleanup_log,
    )
    write_cleanup_artifacts(out_dir=out_dir, artifacts=final_artifacts)

    return {
        "status": "ok",
        "retained_run_count": len(retained_run_ids),
        "non_retained_run_count": len(deleted_run_ids),
        "out_dir": str(out_dir),
        "dry_run": config.dry_run,
        "files_deleted": len(deleted_files),
        "folders_deleted": len(deleted_folders),
    }
