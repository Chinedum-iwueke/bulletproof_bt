#!/usr/bin/env python3
"""Phase 10 State Discovery Agent."""
from __future__ import annotations

import argparse
from collections import defaultdict
import json
from pathlib import Path
import sys
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from orchestrator.db import ResearchDB
from state_discovery.dataset_loader import load_discovery_datasets
from state_discovery.state_bucket_analyzer import analyze_single_state_variables
from state_discovery.interaction_analyzer import analyze_joint_state_variables
from state_discovery.finding_ranker import classify_and_rank_findings
from state_discovery.report_writer import write_state_discovery_outputs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Discover structural state edge from completed experiment outputs.")
    parser.add_argument("--db", required=True)
    parser.add_argument("--experiments-root", default=None)
    parser.add_argument("--experiment-root", default=None)
    parser.add_argument("--output-dir", default="research/state_findings")
    parser.add_argument("--min-trades", type=int, default=30)
    parser.add_argument("--min-bucket-trades", type=int, default=10)
    parser.add_argument("--top-n", type=int, default=25)
    parser.add_argument("--include-negative-findings", action="store_true", default=False)
    parser.add_argument("--dataset-type", default=None)
    parser.add_argument("--hypothesis-id", default=None)
    parser.add_argument("--name", default=None)
    parser.add_argument("--write-db", action="store_true", default=False)
    parser.add_argument("--dry-run", action="store_true", default=False)
    return parser.parse_args()


def _aggregate_findings(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if df.empty:
        return df
    return df.sort_values(["finding_score", "n_trades"], ascending=[False, False]).reset_index(drop=True)


def _cross_aggregate(findings: pd.DataFrame) -> pd.DataFrame:
    if findings.empty:
        return findings
    grp = findings.groupby(["state_variable", "bucket"], dropna=False)
    rows: list[dict[str, Any]] = []
    for (state_var, bucket), part in grp:
        rows.append(
            {
                "state_variable": state_var,
                "bucket": bucket,
                "n_hypotheses": part["hypothesis_id"].nunique(dropna=True) if "hypothesis_id" in part.columns else None,
                "n_datasets": part["dataset_type"].nunique(dropna=True) if "dataset_type" in part.columns else None,
                "n_trades": int(part["n_trades"].sum()),
                "ev_r_net": float(pd.to_numeric(part["ev_r_net"], errors="coerce").mean()),
                "tail_5r_count": int(pd.to_numeric(part["tail_5r_count"], errors="coerce").sum()),
            }
        )
    return pd.DataFrame(rows)


def _write_db(db_path: Path, findings: pd.DataFrame, artifact_paths: dict[str, Path]) -> None:
    db = ResearchDB(db_path)
    db.init_schema()
    for _, row in findings.iterrows():
        db.create_state_finding(
            hypothesis_id=row.get("hypothesis_id"),
            experiment_id=row.get("experiment_id"),
            state_variable=str(row.get("state_variable")),
            bucket=str(row.get("bucket")),
            dataset_type=row.get("dataset_type"),
            n_trades=int(row.get("n_trades")) if pd.notna(row.get("n_trades")) else None,
            ev_r_net=float(row.get("ev_r_net")) if pd.notna(row.get("ev_r_net")) else None,
            median_r=float(row.get("median_r_net")) if pd.notna(row.get("median_r_net")) else None,
            p95_r=float(row.get("p95_r")) if pd.notna(row.get("p95_r")) else None,
            p99_r=float(row.get("p99_r")) if pd.notna(row.get("p99_r")) else None,
            max_r=float(row.get("max_r")) if pd.notna(row.get("max_r")) else None,
            min_r=float(row.get("min_r")) if pd.notna(row.get("min_r")) else None,
            notes=row.get("finding_type"),
            evidence={"finding_score": row.get("finding_score"), "weak_sample": row.get("weak_sample")},
        )
    for artifact_type, path in artifact_paths.items():
        db.register_artifact(artifact_type=artifact_type, path=path, description="state discovery output")
    db.close()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    datasets = load_discovery_datasets(
        db_path=db_path,
        experiments_root=Path(args.experiments_root) if args.experiments_root else None,
        experiment_root=Path(args.experiment_root) if args.experiment_root else None,
        dataset_type=args.dataset_type,
        hypothesis_id=args.hypothesis_id,
        name=args.name,
    )

    all_findings: list[pd.DataFrame] = []
    manifests: list[dict[str, Any]] = []
    missing_state_columns: set[str] = set()
    unavailable_analyses: set[str] = set()

    for ds in datasets:
        trades = ds.trades.copy()
        single_df, single_missing = analyze_single_state_variables(trades, min_bucket_trades=args.min_bucket_trades)
        joint_df, joint_missing = analyze_joint_state_variables(trades, min_bucket_trades=args.min_bucket_trades)
        metrics = pd.concat([single_df, joint_df], ignore_index=True) if not single_df.empty or not joint_df.empty else pd.DataFrame()
        ranked = classify_and_rank_findings(metrics, min_trades=args.min_trades, include_negative_findings=args.include_negative_findings)
        if ranked.empty and (single_missing or joint_missing):
            unavailable_analyses.update(single_missing)
            unavailable_analyses.update(joint_missing)
            missing_state_columns.update(single_missing)
        if not ranked.empty:
            ranked["experiment_root"] = str(ds.experiment_root)
            ranked["experiment_id"] = ds.experiment_id
            ranked["hypothesis_id"] = ds.hypothesis_id
            ranked["hypothesis_name"] = ds.hypothesis_name
            ranked["dataset_type"] = ds.dataset_type
            all_findings.append(ranked)

        manifests.append(
            {
                "experiment_root": str(ds.experiment_root),
                "experiment_id": ds.experiment_id,
                "hypothesis_id": ds.hypothesis_id,
                "hypothesis_name": ds.hypothesis_name,
                "dataset_type": ds.dataset_type,
                "n_trades": len(trades),
                "trade_schema_coverage": ds.schema_coverage,
                "source": ds.manifest,
            }
        )

    findings = _aggregate_findings(all_findings)
    cross = _cross_aggregate(findings)
    if not cross.empty:
        cross_path = Path(args.output_dir) / (f"{args.name}_cross_state_aggregate.csv" if args.experiment_root and args.name else "cross_state_aggregate.csv")
        cross_path.parent.mkdir(parents=True, exist_ok=True)
        cross.to_csv(cross_path, index=False)

    missing_payload = None
    if missing_state_columns:
        missing_payload = {
            "experiment_root": args.experiment_root or args.experiments_root,
            "missing_state_columns": sorted(missing_state_columns),
            "unavailable_analyses": sorted(unavailable_analyses),
            "trade_schema_coverage": [m.get("trade_schema_coverage") for m in manifests if m.get("trade_schema_coverage")],
            "recommendation": "Improve Phase-9 logging/state instrumentation. Under-instrumented experiments cannot localize structural edge.",
        }

    prefix = f"{args.name}_" if args.experiment_root and args.name else ""
    artifact_paths = write_state_discovery_outputs(
        output_dir=Path(args.output_dir),
        prefix=prefix,
        findings=findings,
        manifest={"datasets": manifests},
        missing_fields_payload=missing_payload,
        top_n=args.top_n,
    )

    if args.write_db and not args.dry_run and not findings.empty:
        _write_db(db_path, findings, artifact_paths)

    print(json.dumps({"datasets_scanned": len(datasets), "findings": len(findings), "output_dir": args.output_dir}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
