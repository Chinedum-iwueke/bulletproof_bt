from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def _df_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    return json.loads(df.to_json(orient="records"))


def write_state_discovery_outputs(
    *,
    output_dir: Path,
    prefix: str,
    findings: pd.DataFrame,
    manifest: dict[str, Any],
    missing_fields_payload: dict[str, Any] | None,
    top_n: int,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    json_path = output_dir / f"{prefix}state_findings.json"
    md_path = output_dir / f"{prefix}state_findings.md"
    csv_path = output_dir / f"{prefix}state_findings.csv"
    manifest_path = output_dir / f"{prefix}state_discovery_input_manifest.json"

    findings.to_csv(csv_path, index=False)
    json_path.write_text(json.dumps({"findings": _df_records(findings), "manifest": manifest}, indent=2), encoding="utf-8")
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    def _write_slice(name: str, finding_type: str) -> Path:
        path = output_dir / f"{prefix}{name}.csv"
        if "finding_type" not in findings.columns:
            pd.DataFrame().to_csv(path, index=False)
            return path
        findings[findings["finding_type"] == finding_type].head(top_n).to_csv(path, index=False)
        return path

    def _section(finding_type: str) -> str:
        if findings.empty or "finding_type" not in findings.columns:
            return "(none)"
        section = findings[findings["finding_type"] == finding_type].head(10)
        if section.empty:
            return "(none)"
        try:
            return section.to_markdown(index=False)
        except Exception:
            return section.to_string(index=False)

    paths["state_findings_json"] = json_path
    paths["state_findings_md"] = md_path
    paths["state_findings_csv"] = csv_path
    paths["top_positive_states_csv"] = _write_slice("top_positive_states", "POSITIVE_EDGE_STATE")
    paths["top_negative_states_csv"] = _write_slice("top_negative_states", "NEGATIVE_EDGE_STATE")
    paths["tail_generation_states_csv"] = _write_slice("tail_generation_states", "TAIL_GENERATION_STATE")
    paths["cost_killed_states_csv"] = _write_slice("cost_killed_states", "COST_KILLED_STATE")
    paths["exit_failure_states_csv"] = _write_slice("exit_failure_states", "EXIT_FAILURE_STATE")
    paths["state_discovery_input_manifest"] = manifest_path

    summary = [
        "# State Discovery Report",
        "",
        "## Executive Summary",
        f"- Total findings: {len(findings)}",
        f"- Positive states: {int((findings['finding_type'] == 'POSITIVE_EDGE_STATE').sum()) if ('finding_type' in findings.columns and not findings.empty) else 0}",
        f"- Negative states: {int((findings['finding_type'] == 'NEGATIVE_EDGE_STATE').sum()) if ('finding_type' in findings.columns and not findings.empty) else 0}",
        "",
        "## Strongest Positive States",
        _section("POSITIVE_EDGE_STATE"),
        "",
        "## Strongest Negative / Avoid States",
        _section("NEGATIVE_EDGE_STATE"),
        "",
        "## Tail Generation States",
        _section("TAIL_GENERATION_STATE"),
        "",
        "## Cost-Killed States",
        _section("COST_KILLED_STATE"),
        "",
        "## Exit Failure States",
        _section("EXIT_FAILURE_STATE"),
        "",
        "## Cross-Dataset Consistency",
        "- Use `state_findings.csv` grouped by `dataset_type` + `state_variable` + `bucket`.",
        "",
        "## Cross-Hypothesis Repeatability",
        "- Use `state_findings.csv` grouped by `hypothesis_id` + state bucket.",
        "",
        "## Recommended State Filters",
        "- Add gates on highest positive edge states with sufficient sample.",
        "- Block consistently negative and cost-killed states.",
        "",
        "## Recommended Next Research Actions",
        "- Promote robust high-EV/high-tail states to refined grid tests.",
        "- Add spread/liquidity filters where cost-killed states dominate.",
    ]
    md_path.write_text("\n".join(summary), encoding="utf-8")

    if missing_fields_payload:
        missing_path = output_dir / f"{prefix}state_discovery_missing_fields.json"
        missing_path.write_text(json.dumps(missing_fields_payload, indent=2), encoding="utf-8")
        paths["state_discovery_missing_fields"] = missing_path

    return paths
