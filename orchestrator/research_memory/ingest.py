from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from .candidate_memory import insert_candidates, load_candidate_file
from .schema import ensure_research_memory_schema
from .state_memory import aggregate_buckets_from_trades, insert_state_buckets, load_state_file
from .trade_memory import infer_context_from_path, insert_trades, normalize_trade


def ingest_research_memory(
    conn,
    *,
    outputs_root: Path,
    experiment_roots: list[Path] | None = None,
    verdicts_dir: Path,
    state_findings_dir: Path,
    alpha_zoo_dir: Path,
    output_dir: Path,
) -> dict[str, Any]:
    ensure_research_memory_schema(conn)
    manifest: dict[str, Any] = {
        "experiments_scanned": 0,
        "trades_ingested": 0,
        "state_buckets_ingested": 0,
        "candidates_ingested": 0,
        "verdicts_ingested": 0,
        "skipped_experiments": [],
        "errors": [],
    }
    output_dir.mkdir(parents=True, exist_ok=True)

    roots = [path for path in (experiment_roots or []) if path.exists()]
    if not roots:
        roots = discover_experiment_roots(outputs_root)

    for exp_root in roots:
        manifest["experiments_scanned"] += 1
        try:
            counts = _ingest_experiment(conn, exp_root, outputs_root)
            if counts["trades"] == 0 and counts["state_buckets"] == 0:
                manifest["skipped_experiments"].append(str(exp_root))
            manifest["trades_ingested"] += counts["trades"]
            manifest["state_buckets_ingested"] += counts["state_buckets"]
        except Exception as exc:
            manifest["errors"].append({"path": str(exp_root), "error": str(exc)})

    try:
        aggregate_records = aggregate_buckets_from_trades(conn)
        manifest["state_buckets_ingested"] += insert_state_buckets(conn, aggregate_records)
    except Exception as exc:
        manifest["errors"].append({"path": "trade_aggregate", "error": str(exc)})

    for path in _safe_glob(state_findings_dir, ["*.json", "*.csv"]):
        try:
            records = load_state_file(path)
            manifest["state_buckets_ingested"] += insert_state_buckets(conn, records)
        except Exception as exc:
            manifest["errors"].append({"path": str(path), "error": str(exc)})

    for path in _safe_glob(alpha_zoo_dir, ["alpha_candidates.json", "alpha_candidates.csv", "*.json", "*.csv"]):
        try:
            records = load_candidate_file(path)
            manifest["candidates_ingested"] += insert_candidates(conn, records)
        except Exception as exc:
            manifest["errors"].append({"path": str(path), "error": str(exc)})

    for path in _safe_glob(verdicts_dir, ["*_verdict.json"]):
        try:
            _ingest_verdict_as_candidate(conn, path)
            manifest["verdicts_ingested"] += 1
        except Exception as exc:
            manifest["errors"].append({"path": str(path), "error": str(exc)})

    conn.commit()
    (output_dir / "ingestion_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return manifest


def discover_experiment_roots(outputs_root: Path) -> list[Path]:
    if not outputs_root.exists():
        return []
    roots: set[Path] = set()
    for path in outputs_root.rglob("trades_dataset.parquet"):
        if path.parent.name == "research_data":
            roots.add(path.parent.parent)
    for path in outputs_root.rglob("run_summary.csv"):
        if path.parent.name == "summaries":
            roots.add(path.parent.parent)
    for path in outputs_root.rglob("trades.csv"):
        if "runs" in path.parts:
            idx = path.parts.index("runs")
            roots.add(Path(*path.parts[:idx]))
        else:
            roots.add(path.parent)
    return sorted(roots)


def _ingest_experiment(conn, exp_root: Path, outputs_root: Path) -> dict[str, int]:
    context = infer_context_from_path(exp_root, outputs_root)
    validation = _load_validation(exp_root)
    context["metrics_valid"] = validation.get("metrics_valid", True)
    context["invalid_reason"] = validation.get("invalid_reason")
    state_count = _ingest_summary_buckets(conn, exp_root)

    dataset_path = exp_root / "research_data" / "trades_dataset.parquet"
    if dataset_path.exists():
        trades = pd.read_parquet(dataset_path)
        records = [normalize_trade(row, context=context, row_index=i) for i, row in enumerate(trades.to_dict("records"))]
        return {"trades": insert_trades(conn, records), "state_buckets": state_count}

    records = []
    row_offset = 0
    for trade_path in sorted((exp_root / "runs").rglob("trades.csv")) if (exp_root / "runs").exists() else []:
        run_context = dict(context)
        run_context["run_id"] = trade_path.parent.name
        run_validation = _load_validation(trade_path.parent)
        run_context["metrics_valid"] = run_validation.get("metrics_valid", context.get("metrics_valid", True))
        run_context["invalid_reason"] = run_validation.get("invalid_reason") or context.get("invalid_reason")
        frame = pd.read_csv(trade_path)
        frame["run_id"] = frame.get("run_id", trade_path.parent.name)
        records.extend(
            normalize_trade(row, context=run_context, row_index=row_offset + i)
            for i, row in enumerate(frame.to_dict("records"))
        )
        row_offset += len(frame)
    if not records:
        return {"trades": 0, "state_buckets": state_count}
    return {"trades": insert_trades(conn, records), "state_buckets": state_count}


def _ingest_summary_buckets(conn, exp_root: Path) -> int:
    count = 0
    summaries = exp_root / "summaries"
    if not summaries.exists():
        return 0
    for path in sorted(summaries.glob("ev_by_bucket*.csv")):
        records = load_state_file(path)
        count += insert_state_buckets(conn, records)
    return count


def _load_validation(exp_root: Path) -> dict[str, Any]:
    candidates = list(exp_root.rglob("performance_validation.json")) + list(exp_root.rglob("trade_schema_coverage.json"))
    metrics_valid = True
    reasons: list[str] = []
    for path in candidates:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        val = data.get("metrics_valid")
        if val is False or str(val).lower() == "false":
            metrics_valid = False
            reasons.append(f"{path.name}: metrics_valid=false")
        if data.get("valid") is False and "performance_validation" in path.name:
            metrics_valid = False
            reasons.append(f"{path.name}: valid=false")
    return {"metrics_valid": metrics_valid, "invalid_reason": "; ".join(reasons) or None}


def _ingest_verdict_as_candidate(conn, path: Path) -> None:
    from .candidate_memory import insert_candidates

    data = json.loads(path.read_text(encoding="utf-8"))
    verdict = data.get("verdict") or data.get("recommended_next_action")
    rec = {
        "id": f"verdict:{path.stem}",
        "candidate_id": data.get("hypothesis_id") or path.stem,
        "hypothesis_name": data.get("hypothesis_name") or data.get("name") or path.stem.replace("_verdict", ""),
        "run_id": data.get("pipeline_run_id"),
        "dataset_type": data.get("dataset_type"),
        "phase": data.get("phase"),
        "candidate_status": f"VERDICT_{verdict}" if verdict else "VERDICT",
        "rank_score": data.get("confidence"),
        "promotion_score": data.get("confidence") if verdict == "PROMOTE_TIER3" else None,
        "ev_r_net": None,
        "n_trades": None,
        "tail_5r_count": None,
        "tail_10r_count": None,
        "setup_class": data.get("setup_class"),
        "state_profile_json": json.dumps(data.get("evidence", data.get("evidence_json", {})), sort_keys=True, default=str),
        "recommended_action": verdict,
        "source_path": str(path),
        "created_at": pd.Timestamp.utcnow().replace(microsecond=0).isoformat(),
    }
    insert_candidates(conn, [rec])


def _safe_glob(root: Path, patterns: list[str]) -> list[Path]:
    if not root.exists():
        return []
    paths: list[Path] = []
    seen: set[Path] = set()
    for pattern in patterns:
        for path in root.rglob(pattern):
            if path not in seen:
                paths.append(path)
                seen.add(path)
    return sorted(paths)
