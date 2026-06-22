"""Strategy Research Terminal intelligence cards.

This module is intentionally read-only with respect to backtest truth artifacts.
It summarizes already-emitted research outputs and never feeds data back into the
event-driven engine or strategy decision path.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid5, NAMESPACE_URL

import pandas as pd

try:  # pragma: no cover - exercised when PyYAML is unavailable.
    import yaml
except Exception:  # pragma: no cover
    yaml = None  # type: ignore[assignment]


CARD_SCHEMA_VERSION = "strategy_research_terminal.card.v1"
CARD_BUNDLE_SCHEMA_VERSION = "strategy_research_terminal.bundle.v1"
CARD_TYPES = (
    "HypothesisCard",
    "RunQualityCard",
    "RegimeDependencyCard",
    "ExecutionDragCard",
    "FailureCauseCard",
    "VerdictCard",
    "NextExperimentCard",
)
RICH_STATE_COLUMNS = (
    "entry_state_funding_raw",
    "entry_state_funding_pctile",
    "entry_state_funding_z",
    "entry_state_oi_level",
    "entry_state_oi_accel",
    "entry_state_oi_accel_pctile",
    "entry_state_mark_price",
    "entry_state_index_price",
    "entry_state_basis_raw",
    "entry_state_basis_pct",
    "entry_state_basis_pctile",
    "entry_state_premium_pctile",
    "entry_state_crowding_proxy_pctile",
    "entry_state_constraint_stress_pctile",
    "entry_state_csi_source",
    "entry_state_csi_components_json",
)


@dataclass(frozen=True)
class CardWriteResult:
    output_dir: Path
    bundle_json: Path
    bundle_markdown: Path
    card_json_paths: dict[str, Path]
    cards: list[dict[str, Any]]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _stable_card_id(card_type: str, name: str, phase: str, pipeline_run_id: str | None) -> str:
    raw = f"{CARD_SCHEMA_VERSION}:{card_type}:{phase}:{name}:{pipeline_run_id or 'no-pipeline'}"
    return str(uuid5(NAMESPACE_URL, raw))


def _make_card(
    *,
    card_type: str,
    name: str,
    phase: str,
    hypothesis_name: str | None,
    pipeline_run_id: str | None,
    source_artifacts: dict[str, Any],
    data: dict[str, Any],
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    if card_type not in CARD_TYPES:
        raise ValueError(f"Unknown card_type={card_type!r}")
    return {
        "schema_version": CARD_SCHEMA_VERSION,
        "card_type": card_type,
        "card_id": _stable_card_id(card_type, name, phase, pipeline_run_id),
        "name": name,
        "phase": phase,
        "hypothesis_name": hypothesis_name or name,
        "pipeline_run_id": pipeline_run_id,
        "created_at": utc_now_iso(),
        "source_artifacts": source_artifacts,
        "data": data,
        "warnings": warnings or [],
    }


def _path_str(path: Path | None) -> str | None:
    return str(path.resolve()) if path is not None else None


def _read_json(path: Path | None, warnings: list[str]) -> dict[str, Any]:
    if path is None or not path.exists():
        if path is not None:
            warnings.append(f"Missing JSON artifact: {path}")
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        warnings.append(f"Could not parse JSON artifact {path}: {exc}")
        return {}
    return value if isinstance(value, dict) else {"value": value}


def _read_hypothesis(path: Path | None, warnings: list[str]) -> dict[str, Any]:
    if path is None or not path.exists():
        if path is not None:
            warnings.append(f"Missing hypothesis YAML: {path}")
        return {}
    if yaml is None:
        warnings.append("PyYAML unavailable; hypothesis YAML was not parsed.")
        return {}
    try:
        parsed = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        warnings.append(f"Could not parse hypothesis YAML {path}: {exc}")
        return {}
    return parsed if isinstance(parsed, dict) else {"raw": parsed}


def _read_csv(path: Path, warnings: list[str], *, columns: list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        warnings.append(f"Missing CSV artifact: {path}")
        return pd.DataFrame()
    try:
        return pd.read_csv(path, usecols=columns)
    except ValueError:
        try:
            return pd.read_csv(path)
        except Exception as exc:
            warnings.append(f"Could not read CSV artifact {path}: {exc}")
            return pd.DataFrame()
    except Exception as exc:
        warnings.append(f"Could not read CSV artifact {path}: {exc}")
        return pd.DataFrame()


def _read_parquet_columns(path: Path, warnings: list[str], columns: list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        warnings.append(f"Missing parquet artifact: {path}")
        return pd.DataFrame()
    try:
        return pd.read_parquet(path, columns=columns)
    except Exception:
        try:
            return pd.read_parquet(path)
        except Exception as exc:
            warnings.append(f"Could not read parquet artifact {path}: {exc}")
            return pd.DataFrame()


def _first_existing(paths: Iterable[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def _summarize_run_summary(root: Path, warnings: list[str]) -> dict[str, Any]:
    path = root / "summaries" / "run_summary.csv"
    frame = _read_csv(path, warnings)
    if frame.empty:
        return {"path": _path_str(path), "rows": 0, "available": False}
    numeric = frame.select_dtypes(include="number")
    metric_cols = [c for c in ("ev_r_net", "mean_r_net", "median_r_net", "total_pnl", "n_trades", "trade_count") if c in frame.columns]
    out: dict[str, Any] = {
        "path": _path_str(path),
        "available": True,
        "rows": int(len(frame)),
        "columns": list(frame.columns),
    }
    for col in metric_cols:
        series = pd.to_numeric(frame[col], errors="coerce")
        out[f"{col}_best"] = _safe_float(series.max())
        out[f"{col}_median"] = _safe_float(series.median())
        out[f"{col}_worst"] = _safe_float(series.min())
    if "metrics_valid" in frame.columns:
        out["metrics_valid_count"] = int(frame["metrics_valid"].fillna(False).astype(bool).sum())
    elif "metrics_validated" in frame.columns:
        out["metrics_valid_count"] = int(frame["metrics_validated"].fillna(False).astype(bool).sum())
    if not numeric.empty:
        out["numeric_columns"] = list(numeric.columns)
    return out


def _summarize_dataset(root: Path, warnings: list[str]) -> dict[str, Any]:
    runs_path = root / "research_data" / "runs_dataset.parquet"
    trades_path = root / "research_data" / "trades_dataset.parquet"
    runs = _read_parquet_columns(runs_path, warnings)
    trades = _read_parquet_columns(trades_path, warnings)
    rich_cols = sorted(c for c in trades.columns if c in RICH_STATE_COLUMNS)
    csi_sources: list[str] = []
    if "entry_state_csi_source" in trades.columns:
        csi_sources = sorted(str(v) for v in trades["entry_state_csi_source"].dropna().unique().tolist())
    return {
        "runs_dataset": _path_str(runs_path),
        "trades_dataset": _path_str(trades_path),
        "runs_rows": int(len(runs)) if not runs.empty else 0,
        "trades_rows": int(len(trades)) if not trades.empty else 0,
        "trade_columns": list(trades.columns) if not trades.empty else [],
        "rich_state_columns_present": rich_cols,
        "rich_data_available": bool(rich_cols),
        "csi_sources": csi_sources,
    }


def _summarize_structural_buckets(root: Path, warnings: list[str]) -> dict[str, Any]:
    summaries = root / "summaries"
    rich_files = {
        "funding": summaries / "ev_by_bucket_funding.csv",
        "oi_accel": summaries / "ev_by_bucket_oi_accel.csv",
        "basis": summaries / "ev_by_bucket_basis.csv",
        "joint_csi_funding": summaries / "ev_by_bucket_joint_csi_funding.csv",
        "joint_csi_oi_accel": summaries / "ev_by_bucket_joint_csi_oi_accel.csv",
        "joint_csi_basis": summaries / "ev_by_bucket_joint_csi_basis.csv",
    }
    out: dict[str, Any] = {"files": {}, "top_buckets": {}}
    for label, path in rich_files.items():
        if not path.exists():
            out["files"][label] = {"path": _path_str(path), "available": False}
            continue
        frame = _read_csv(path, warnings)
        out["files"][label] = {"path": _path_str(path), "available": True, "rows": int(len(frame))}
        if frame.empty:
            continue
        score_col = _first_column(frame, ("ev_r_net", "mean_r_net", "avg_r_net", "expectancy_r", "total_pnl"))
        bucket_col = _first_column(frame, ("bucket", "state_bucket", "joint_bucket", "label"))
        if score_col and bucket_col:
            ranked = frame.assign(_score=pd.to_numeric(frame[score_col], errors="coerce")).sort_values("_score", ascending=False)
            out["top_buckets"][label] = ranked[[bucket_col, score_col]].head(5).to_dict(orient="records")
    return out


def _collect_state_findings(name: str, phase: str, project_root: Path, warnings: list[str]) -> dict[str, Any]:
    candidates = [
        project_root / "research" / "state_findings" / phase,
        project_root / "research" / "state_findings",
    ]
    files: list[Path] = []
    for root in candidates:
        if root.exists():
            files.extend(sorted(root.glob(f"{name}*state_findings*.json")))
            files.extend(sorted(root.glob(f"{name}*.json")))
    files = sorted(set(files))
    findings: list[dict[str, Any]] = []
    for path in files[:12]:
        payload = _read_json(path, warnings)
        if payload:
            findings.append({"path": _path_str(path), "keys": sorted(payload.keys()), "payload": _compact_payload(payload)})
    return {"files": [_path_str(p) for p in files], "sample": findings[:6], "count": len(files)}


def _compact_payload(payload: dict[str, Any], *, max_items: int = 8) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in list(payload.items())[:max_items]:
        if isinstance(value, list):
            out[key] = value[:3]
        elif isinstance(value, dict):
            out[key] = {k: value[k] for k in list(value)[:5]}
        else:
            out[key] = value
    return out


def _first_column(frame: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    for col in candidates:
        if col in frame.columns:
            return col
    return None


def _safe_float(value: Any) -> float | None:
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _latest_verdict_payload(
    *,
    verdict_bundle_dir: Path | None,
    project_root: Path,
    name: str,
    phase: str,
    warnings: list[str],
) -> dict[str, Any]:
    manifest_path = verdict_bundle_dir / "manifest.json" if verdict_bundle_dir else None
    manifest = _read_json(manifest_path, warnings) if manifest_path else {}
    candidates = []
    if verdict_bundle_dir:
        candidates.extend(sorted(verdict_bundle_dir.glob("*.json")))
    verdict_root = project_root / "research" / "verdicts"
    if verdict_root.exists():
        candidates.extend(sorted((verdict_root / phase).glob(f"{name}*.json")) if (verdict_root / phase).exists() else [])
        candidates.extend(sorted(verdict_root.glob(f"{name}*.json")))
    latest = None
    for candidate in candidates:
        if candidate.name == "manifest.json":
            continue
        payload = _read_json(candidate, warnings)
        if payload:
            latest = {"path": _path_str(candidate), "payload": _compact_payload(payload, max_items=12)}
    return {
        "verdict_bundle_manifest": _path_str(manifest_path),
        "bundle_manifest_available": bool(manifest),
        "bundle_manifest": _compact_payload(manifest, max_items=12) if manifest else {},
        "latest_verdict_artifact": latest,
    }


def _recommended_action(verdict: dict[str, Any], run_quality: dict[str, Any], failure: dict[str, Any]) -> str:
    if failure.get("failure_detected"):
        return "FIX_PIPELINE_FAILURE"
    latest = verdict.get("latest_verdict_artifact") or {}
    payload = latest.get("payload") if isinstance(latest, dict) else {}
    for key in ("recommended_next_action", "verdict", "action"):
        value = payload.get(key) if isinstance(payload, dict) else None
        if value:
            return str(value)
    stable = run_quality.get("stable", {})
    volatile = run_quality.get("volatile", {})
    stable_rows = int(stable.get("rows", 0) or 0)
    volatile_rows = int(volatile.get("rows", 0) or 0)
    if stable_rows == 0 and volatile_rows == 0:
        return "NEEDS_RUN_ARTIFACTS"
    return "AWAIT_VERDICT_REVIEW"


def _failure_summary(
    *,
    status: str,
    error_message: str | None,
    command_log_dir: Path | None,
    pipeline_log_path: Path | None,
    warnings: list[str],
) -> dict[str, Any]:
    log_files: list[str] = []
    if command_log_dir and command_log_dir.exists():
        log_files = sorted(str(p.resolve()) for p in command_log_dir.glob("*.log"))[:20]
    tail = ""
    if pipeline_log_path and pipeline_log_path.exists():
        try:
            lines = pipeline_log_path.read_text(encoding="utf-8", errors="replace").splitlines()
            tail = "\n".join(lines[-80:])
        except Exception as exc:
            warnings.append(f"Could not read pipeline log {pipeline_log_path}: {exc}")
    failed = status.upper() == "FAILED" or bool(error_message)
    root_cause_hint = None
    if failed:
        text = f"{error_message or ''}\n{tail}".lower()
        if "missing" in text or "not found" in text:
            root_cause_hint = "missing_artifact_or_path"
        elif "memory" in text or "killed" in text or "oom" in text:
            root_cause_hint = "resource_pressure"
        elif "timeout" in text:
            root_cause_hint = "timeout"
        elif "traceback" in text:
            root_cause_hint = "python_exception"
        else:
            root_cause_hint = "unknown"
    return {
        "failure_detected": failed,
        "status": status,
        "error_message": error_message,
        "root_cause_hint": root_cause_hint,
        "pipeline_log": _path_str(pipeline_log_path),
        "command_log_dir": _path_str(command_log_dir),
        "command_log_files_sample": log_files,
        "pipeline_log_tail": tail,
    }


def build_cards(
    *,
    name: str,
    phase: str,
    hypothesis_path: Path | None,
    stable_root: Path | None,
    volatile_root: Path | None,
    project_root: Path | None = None,
    pipeline_run_id: str | None = None,
    verdict_bundle_dir: Path | None = None,
    command_log_dir: Path | None = None,
    pipeline_log_path: Path | None = None,
    status: str = "COMPLETED",
    error_message: str | None = None,
) -> list[dict[str, Any]]:
    project_root = project_root or Path.cwd()
    warnings: list[str] = []
    hypothesis = _read_hypothesis(hypothesis_path, warnings)
    hypothesis_name = str(hypothesis.get("name") or hypothesis.get("id") or name)

    stable_root = stable_root or Path()
    volatile_root = volatile_root or Path()
    stable_summary = _summarize_run_summary(stable_root, warnings)
    volatile_summary = _summarize_run_summary(volatile_root, warnings)
    stable_dataset = _summarize_dataset(stable_root, warnings)
    volatile_dataset = _summarize_dataset(volatile_root, warnings)
    stable_buckets = _summarize_structural_buckets(stable_root, warnings)
    volatile_buckets = _summarize_structural_buckets(volatile_root, warnings)
    state_findings = _collect_state_findings(name, phase, project_root, warnings)
    verdict = _latest_verdict_payload(
        verdict_bundle_dir=verdict_bundle_dir,
        project_root=project_root,
        name=name,
        phase=phase,
        warnings=warnings,
    )
    failure = _failure_summary(
        status=status,
        error_message=error_message,
        command_log_dir=command_log_dir,
        pipeline_log_path=pipeline_log_path,
        warnings=warnings,
    )

    common_sources = {
        "hypothesis_yaml": _path_str(hypothesis_path),
        "stable_root": _path_str(stable_root),
        "volatile_root": _path_str(volatile_root),
        "verdict_bundle_dir": _path_str(verdict_bundle_dir),
        "command_log_dir": _path_str(command_log_dir),
        "pipeline_log": _path_str(pipeline_log_path),
    }
    run_quality = {
        "stable": stable_summary,
        "volatile": volatile_summary,
        "stable_dataset": stable_dataset,
        "volatile_dataset": volatile_dataset,
    }
    rich_cols = sorted(set(stable_dataset["rich_state_columns_present"]) | set(volatile_dataset["rich_state_columns_present"]))
    recommendation = _recommended_action(verdict, run_quality, failure)

    cards = [
        _make_card(
            card_type="HypothesisCard",
            name=name,
            phase=phase,
            hypothesis_name=hypothesis_name,
            pipeline_run_id=pipeline_run_id,
            source_artifacts=common_sources,
            data={
                "hypothesis": hypothesis,
                "hypothesis_path": _path_str(hypothesis_path),
                "rich_state_columns_detected": rich_cols,
                "rich_data_available": bool(rich_cols),
                "stable_experiment_root": _path_str(stable_root),
                "volatile_experiment_root": _path_str(volatile_root),
            },
            warnings=warnings.copy(),
        ),
        _make_card(
            card_type="RunQualityCard",
            name=name,
            phase=phase,
            hypothesis_name=hypothesis_name,
            pipeline_run_id=pipeline_run_id,
            source_artifacts=common_sources,
            data=run_quality,
            warnings=warnings.copy(),
        ),
        _make_card(
            card_type="RegimeDependencyCard",
            name=name,
            phase=phase,
            hypothesis_name=hypothesis_name,
            pipeline_run_id=pipeline_run_id,
            source_artifacts=common_sources,
            data={
                "stable_structural_buckets": stable_buckets,
                "volatile_structural_buckets": volatile_buckets,
                "state_findings": state_findings,
                "rich_state_columns_detected": rich_cols,
            },
            warnings=warnings.copy(),
        ),
        _make_card(
            card_type="ExecutionDragCard",
            name=name,
            phase=phase,
            hypothesis_name=hypothesis_name,
            pipeline_run_id=pipeline_run_id,
            source_artifacts=common_sources,
            data=_execution_drag_data(stable_root, volatile_root, warnings),
            warnings=warnings.copy(),
        ),
        _make_card(
            card_type="FailureCauseCard",
            name=name,
            phase=phase,
            hypothesis_name=hypothesis_name,
            pipeline_run_id=pipeline_run_id,
            source_artifacts=common_sources,
            data=failure,
            warnings=warnings.copy(),
        ),
        _make_card(
            card_type="VerdictCard",
            name=name,
            phase=phase,
            hypothesis_name=hypothesis_name,
            pipeline_run_id=pipeline_run_id,
            source_artifacts=common_sources,
            data=verdict,
            warnings=warnings.copy(),
        ),
        _make_card(
            card_type="NextExperimentCard",
            name=name,
            phase=phase,
            hypothesis_name=hypothesis_name,
            pipeline_run_id=pipeline_run_id,
            source_artifacts=common_sources,
            data={
                "recommended_action": recommendation,
                "promotion_or_scrap_summary": _promotion_summary(recommendation),
                "requires_human_approval": recommendation not in {"NEEDS_RUN_ARTIFACTS", "FIX_PIPELINE_FAILURE"},
                "next_inputs_to_review": [
                    "VerdictCard",
                    "RunQualityCard",
                    "RegimeDependencyCard",
                    "ExecutionDragCard",
                ],
            },
            warnings=warnings.copy(),
        ),
    ]
    return cards


def _execution_drag_data(stable_root: Path, volatile_root: Path, warnings: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for label, root in (("stable", stable_root), ("volatile", volatile_root)):
        trades_path = root / "research_data" / "trades_dataset.parquet"
        cols = ["net_pnl", "gross_pnl", "fees_paid", "slippage_cost", "pnl_r", "r_multiple_gross"]
        trades = _read_parquet_columns(trades_path, warnings, columns=cols)
        if trades.empty:
            out[label] = {"available": False, "trades_dataset": _path_str(trades_path)}
            continue
        data: dict[str, Any] = {"available": True, "trades_dataset": _path_str(trades_path), "rows": int(len(trades))}
        if {"gross_pnl", "net_pnl"}.issubset(trades.columns):
            data["gross_to_net_drag"] = _safe_float(pd.to_numeric(trades["gross_pnl"], errors="coerce").sum() - pd.to_numeric(trades["net_pnl"], errors="coerce").sum())
        if "fees_paid" in trades.columns:
            data["fees_total"] = _safe_float(pd.to_numeric(trades["fees_paid"], errors="coerce").sum())
        if "slippage_cost" in trades.columns:
            data["slippage_total"] = _safe_float(pd.to_numeric(trades["slippage_cost"], errors="coerce").sum())
        if {"r_multiple_gross", "pnl_r"}.issubset(trades.columns):
            data["mean_r_drag"] = _safe_float(
                (pd.to_numeric(trades["r_multiple_gross"], errors="coerce") - pd.to_numeric(trades["pnl_r"], errors="coerce")).mean()
            )
        out[label] = data
    return out


def _promotion_summary(recommendation: str) -> str:
    if recommendation in {"PROMOTE_TIER3", "PROMOTE_FORWARD_TEST", "ADD_TO_ALPHA_ZOO"}:
        return "Promotion candidate; verify robustness and execution drag before approval."
    if recommendation == "SCRAP":
        return "Scrap candidate; retain evidence for memory and avoid duplicate future tests."
    if recommendation.startswith("REFINE") or recommendation in {"ADD_STATE_FILTER"}:
        return "Refinement candidate; formulate the next controlled experiment."
    if recommendation == "FIX_PIPELINE_FAILURE":
        return "Pipeline failed; fix infrastructure or artifact issue before judging strategy."
    if recommendation == "NEEDS_RUN_ARTIFACTS":
        return "Insufficient run artifacts; complete the pipeline before a strategy verdict."
    return "Await deterministic and human verdict review."


def write_cards(
    *,
    cards: list[dict[str, Any]],
    output_dir: Path,
) -> CardWriteResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    card_json_paths: dict[str, Path] = {}
    for card in cards:
        path = output_dir / f"{card['card_type']}.json"
        _atomic_write_text(path, json.dumps(card, indent=2, sort_keys=True))
        card_json_paths[str(card["card_type"])] = path

    bundle = {
        "schema_version": CARD_BUNDLE_SCHEMA_VERSION,
        "card_schema_version": CARD_SCHEMA_VERSION,
        "created_at": utc_now_iso(),
        "cards": cards,
    }
    bundle_json = output_dir / "cards.json"
    _atomic_write_text(bundle_json, json.dumps(bundle, indent=2, sort_keys=True))

    bundle_markdown = output_dir / "cards.md"
    _atomic_write_text(bundle_markdown, render_cards_markdown(cards))
    return CardWriteResult(
        output_dir=output_dir,
        bundle_json=bundle_json,
        bundle_markdown=bundle_markdown,
        card_json_paths=card_json_paths,
        cards=cards,
    )


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def render_cards_markdown(cards: list[dict[str, Any]]) -> str:
    lines = ["# Strategy Research Terminal Cards", ""]
    for card in cards:
        data = card.get("data", {})
        lines.extend(
            [
                f"## {card['card_type']}",
                "",
                f"- Hypothesis: `{card.get('hypothesis_name')}`",
                f"- Phase: `{card.get('phase')}`",
                f"- Schema: `{card.get('schema_version')}`",
            ]
        )
        if card.get("warnings"):
            lines.append(f"- Warnings: {len(card['warnings'])}")
        if card["card_type"] == "RunQualityCard":
            stable = data.get("stable", {})
            volatile = data.get("volatile", {})
            lines.append(f"- Stable runs summarized: {stable.get('rows', 0)}")
            lines.append(f"- Volatile runs summarized: {volatile.get('rows', 0)}")
        elif card["card_type"] == "RegimeDependencyCard":
            rich = data.get("rich_state_columns_detected", [])
            lines.append(f"- Rich state columns detected: {', '.join(rich) if rich else 'none'}")
        elif card["card_type"] == "FailureCauseCard":
            lines.append(f"- Failure detected: {data.get('failure_detected')}")
            if data.get("root_cause_hint"):
                lines.append(f"- Root cause hint: `{data.get('root_cause_hint')}`")
        elif card["card_type"] == "NextExperimentCard":
            lines.append(f"- Recommended action: `{data.get('recommended_action')}`")
            lines.append(f"- Summary: {data.get('promotion_or_scrap_summary')}")
        elif card["card_type"] == "VerdictCard":
            latest = data.get("latest_verdict_artifact")
            lines.append(f"- Latest verdict artifact: `{latest.get('path') if isinstance(latest, dict) else None}`")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def build_and_write_intelligence_cards(
    *,
    name: str,
    phase: str,
    hypothesis_path: Path,
    stable_root: Path,
    volatile_root: Path,
    output_dir: Path,
    project_root: Path | None = None,
    pipeline_run_id: str | None = None,
    verdict_bundle_dir: Path | None = None,
    command_log_dir: Path | None = None,
    pipeline_log_path: Path | None = None,
    db: Any = None,
    hypothesis_id: str | None = None,
) -> CardWriteResult:
    cards = build_cards(
        name=name,
        phase=phase,
        hypothesis_path=hypothesis_path,
        stable_root=stable_root,
        volatile_root=volatile_root,
        project_root=project_root,
        pipeline_run_id=pipeline_run_id,
        verdict_bundle_dir=verdict_bundle_dir,
        command_log_dir=command_log_dir,
        pipeline_log_path=pipeline_log_path,
        status="COMPLETED",
    )
    result = write_cards(cards=cards, output_dir=output_dir)
    _register_card_artifacts(db=db, result=result, hypothesis_id=hypothesis_id, pipeline_run_id=pipeline_run_id)
    return result


def build_and_write_failure_cards(
    *,
    name: str,
    phase: str,
    hypothesis_path: Path | None,
    stable_root: Path | None,
    volatile_root: Path | None,
    output_dir: Path,
    project_root: Path | None = None,
    pipeline_run_id: str | None = None,
    verdict_bundle_dir: Path | None = None,
    command_log_dir: Path | None = None,
    pipeline_log_path: Path | None = None,
    error_message: str | None = None,
    db: Any = None,
    hypothesis_id: str | None = None,
) -> CardWriteResult:
    cards = build_cards(
        name=name,
        phase=phase,
        hypothesis_path=hypothesis_path,
        stable_root=stable_root,
        volatile_root=volatile_root,
        project_root=project_root,
        pipeline_run_id=pipeline_run_id,
        verdict_bundle_dir=verdict_bundle_dir,
        command_log_dir=command_log_dir,
        pipeline_log_path=pipeline_log_path,
        status="FAILED",
        error_message=error_message,
    )
    result = write_cards(cards=cards, output_dir=output_dir)
    _register_card_artifacts(db=db, result=result, hypothesis_id=hypothesis_id, pipeline_run_id=pipeline_run_id)
    return result


def _register_card_artifacts(
    *,
    db: Any,
    result: CardWriteResult,
    hypothesis_id: str | None,
    pipeline_run_id: str | None,
) -> None:
    if db is None:
        return
    db.register_artifact(
        artifact_type="strategy_terminal_cards_json",
        path=result.bundle_json,
        hypothesis_id=hypothesis_id,
        pipeline_run_id=pipeline_run_id,
        description="Strategy Research Terminal intelligence card bundle JSON.",
        metadata={"schema_version": CARD_BUNDLE_SCHEMA_VERSION, "card_schema_version": CARD_SCHEMA_VERSION},
    )
    db.register_artifact(
        artifact_type="strategy_terminal_cards_markdown",
        path=result.bundle_markdown,
        hypothesis_id=hypothesis_id,
        pipeline_run_id=pipeline_run_id,
        description="Strategy Research Terminal intelligence card bundle Markdown.",
        metadata={"schema_version": CARD_BUNDLE_SCHEMA_VERSION, "card_schema_version": CARD_SCHEMA_VERSION},
    )
    for card_type, path in result.card_json_paths.items():
        db.register_artifact(
            artifact_type=f"strategy_terminal_card_{card_type}",
            path=path,
            hypothesis_id=hypothesis_id,
            pipeline_run_id=pipeline_run_id,
            description=f"{card_type} JSON artifact.",
            metadata={"schema_version": CARD_SCHEMA_VERSION, "card_type": card_type},
        )
