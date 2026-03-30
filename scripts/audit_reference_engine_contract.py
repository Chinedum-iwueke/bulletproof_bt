#!/usr/bin/env python3
"""Audit the exact SaaS engine output contract for the rich reference trade artifact.

This script runs the real SaaS entrypoint:
    run_analysis_from_parsed_artifact(...)
and writes grounded audit artifacts to disk.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import sys

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from bt.saas.models import AnalysisRunConfig, ParsedArtifactInput
from bt.saas.service import StrategyRobustnessLabService, run_analysis_from_parsed_artifact


def _is_materially_populated(value: Any) -> bool:
    """Return True when value contains meaningful non-empty content."""

    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    if isinstance(value, (int, float, bool)):
        return True
    if isinstance(value, dict):
        if not value:
            return False
        return any(_is_materially_populated(v) for v in value.values())
    if isinstance(value, list):
        if not value:
            return False
        return any(_is_materially_populated(item) for item in value)
    return True


def _shape(value: Any) -> Any:
    """Return a lightweight recursive shape description for payload inspection."""

    if isinstance(value, dict):
        return {k: _shape(v) for k, v in value.items()}
    if isinstance(value, list):
        if not value:
            return []
        return [_shape(value[0])]
    return type(value).__name__


def _collect_key_paths_with_substring(obj: Any, needle: str, path: str = "") -> list[str]:
    matches: list[str] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            child_path = f"{path}.{key}" if path else key
            if needle in key.lower():
                matches.append(child_path)
            matches.extend(_collect_key_paths_with_substring(value, needle, child_path))
    elif isinstance(obj, list):
        for idx, value in enumerate(obj):
            child_path = f"{path}[{idx}]"
            matches.extend(_collect_key_paths_with_substring(value, needle, child_path))
    return matches


def build_parsed_artifact(reference_csv: Path) -> ParsedArtifactInput:
    service = StrategyRobustnessLabService()
    frame = pd.read_csv(reference_csv)
    return ParsedArtifactInput(
        artifact_kind="trade_csv",
        richness="trade_plus_metadata",
        trades=service._records_from_trade_frame(frame),  # noqa: SLF001
        strategy_metadata={
            "strategy_name": "reference_rich_fixture",
            "artifact_source": str(reference_csv),
        },
    )


def build_diagnostic_table(diagnostics: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    table: list[dict[str, Any]] = []
    for diagnostic_name, payload in diagnostics.items():
        figures = payload.get("figures", []) if isinstance(payload, dict) else []
        figure_rows: list[dict[str, Any]] = []
        figure_types: list[str] = []

        for figure in figures:
            if not isinstance(figure, dict):
                continue
            payload_obj = figure.get("payload")
            payload_keys = list(payload_obj.keys()) if isinstance(payload_obj, dict) else []
            figure_type = str(figure.get("type", ""))
            if figure_type:
                figure_types.append(figure_type)
            figure_rows.append(
                {
                    "id": figure.get("id"),
                    "type": figure.get("type"),
                    "payload_keys": payload_keys,
                    "payload_shape": _shape(payload_obj),
                    "payload_materially_populated": _is_materially_populated(payload_obj),
                }
            )

        table.append(
            {
                "diagnostic": diagnostic_name,
                "status": payload.get("status") if isinstance(payload, dict) else None,
                "summary_metric_keys": sorted(payload.get("summary_metrics", {}).keys())
                if isinstance(payload, dict) and isinstance(payload.get("summary_metrics"), dict)
                else [],
                "figure_count": len(figures),
                "figure_types": sorted(set(figure_types)),
                "figures": figure_rows,
            }
        )
    return table


def build_contract_summary(full_result: dict[str, Any]) -> dict[str, Any]:
    diagnostics = full_result.get("diagnostics", {})
    diagnostic_table = build_diagnostic_table(diagnostics)

    empty_or_placeholder_fields: dict[str, list[str]] = {}
    for row in diagnostic_table:
        empty_figures = [
            fig.get("id")
            for fig in row.get("figures", [])
            if not fig.get("payload_materially_populated", False)
        ]
        if empty_figures:
            empty_or_placeholder_fields[row["diagnostic"]] = [str(x) for x in empty_figures]

    assumptions_fields: dict[str, bool] = {}
    limitations_fields: dict[str, bool] = {}
    recommendations_fields: dict[str, bool] = {}
    for name, payload in diagnostics.items():
        if isinstance(payload, dict):
            assumptions_fields[name] = "assumptions" in payload
            limitations_fields[name] = "limitations" in payload
            recommendations_fields[name] = "recommendations" in payload

    report_payload = diagnostics.get("report", {}) if isinstance(diagnostics, dict) else {}

    benchmark_key_paths = sorted(
        set(
            _collect_key_paths_with_substring(full_result, "benchmark")
            + _collect_key_paths_with_substring(full_result, "excess_return")
        )
    )

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "top_level_keys": sorted(full_result.keys()),
        "diagnostics_present": sorted(diagnostics.keys()) if isinstance(diagnostics, dict) else [],
        "diagnostics": diagnostic_table,
        "assumptions_field_presence_by_diagnostic": assumptions_fields,
        "limitations_field_presence_by_diagnostic": limitations_fields,
        "recommendations_field_presence_by_diagnostic": recommendations_fields,
        "report_payload_shape": _shape(report_payload),
        "benchmark_related_fields": benchmark_key_paths,
        "placeholder_or_empty_figure_payloads": empty_or_placeholder_fields,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--reference-csv",
        default="examples/reference_artifacts/trades_rich_reference.csv",
        help="Path to rich reference trade CSV.",
    )
    parser.add_argument(
        "--out-dir",
        default="debug/audit/engine_contract_reference",
        help="Directory to write audit artifacts.",
    )
    parser.add_argument("--seed", type=int, default=21)
    parser.add_argument("--simulations", type=int, default=200)
    parser.add_argument("--account-size", type=float, default=100_000.0)
    parser.add_argument("--risk-per-trade-pct", type=float, default=0.01)
    args = parser.parse_args()

    reference_csv = Path(args.reference_csv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    parsed_artifact = build_parsed_artifact(reference_csv)
    config = AnalysisRunConfig(
        seed=args.seed,
        simulations=args.simulations,
        account_size=args.account_size,
        risk_per_trade_pct=args.risk_per_trade_pct,
    )

    # Real SaaS seam entrypoint.
    result = run_analysis_from_parsed_artifact(parsed_artifact, config=config)
    full_result = asdict(result)

    full_path = out_dir / "full_engine_result.json"
    full_path.write_text(json.dumps(full_result, indent=2, sort_keys=True), encoding="utf-8")

    summary = build_contract_summary(full_result)
    summary_path = out_dir / "contract_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    table_path = out_dir / "diagnostic_audit_table.json"
    table_path.write_text(json.dumps(summary["diagnostics"], indent=2, sort_keys=True), encoding="utf-8")

    human_summary_lines = [
        "# Engine Output Contract Audit (reference rich trade artifact)",
        "",
        f"- Generated at (UTC): {summary['generated_at_utc']}",
        f"- Entrypoint: bt.saas.service.run_analysis_from_parsed_artifact(parsed_artifact, config)",
        f"- Reference CSV: {reference_csv}",
        f"- Output directory: {out_dir}",
        "",
        "## Top-level keys",
        f"- {', '.join(summary['top_level_keys'])}",
        "",
        "## Diagnostics",
    ]

    for row in summary["diagnostics"]:
        human_summary_lines.append(f"### {row['diagnostic']}")
        human_summary_lines.append(f"- status: {row['status']}")
        human_summary_lines.append(
            "- summary_metric_keys: "
            + (", ".join(row["summary_metric_keys"]) if row["summary_metric_keys"] else "<none>")
        )
        human_summary_lines.append(f"- figure_count: {row['figure_count']}")
        human_summary_lines.append(
            "- figure_types: " + (", ".join(row["figure_types"]) if row["figure_types"] else "<none>")
        )
        for fig in row["figures"]:
            keys = ", ".join(fig["payload_keys"]) if fig["payload_keys"] else "<none>"
            human_summary_lines.append(
                f"  - figure id={fig['id']} type={fig['type']} populated={fig['payload_materially_populated']} payload_keys={keys}"
            )
        human_summary_lines.append("")

    human_summary_lines.extend(
        [
            "## Report payload shape",
            json.dumps(summary["report_payload_shape"], indent=2, sort_keys=True),
            "",
            "## Benchmark-related fields detected",
            *([f"- {path}" for path in summary["benchmark_related_fields"]] or ["- <none>"]),
            "",
            "## Placeholder / empty figure payloads",
            json.dumps(summary["placeholder_or_empty_figure_payloads"], indent=2, sort_keys=True),
            "",
        ]
    )

    md_path = out_dir / "contract_summary.md"
    md_path.write_text("\n".join(human_summary_lines), encoding="utf-8")

    print("Wrote audit artifacts:")
    for path in (full_path, summary_path, table_path, md_path):
        print(f"- {path}")


if __name__ == "__main__":
    main()
