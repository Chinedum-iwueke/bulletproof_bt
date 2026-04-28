from __future__ import annotations

from pathlib import Path
from typing import Any


def _markdown_table(rows: list[dict[str, Any]], limit: int = 10) -> str:
    if not rows:
        return "_No rows._"
    headers = ["run_id", "dataset", "ev_r_net", "n_trades", "max_drawdown", "robustness_score"]
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for row in rows[:limit]:
        values = [str(row.get(h, "")) for h in headers]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def write_markdown_verdict(output_path: Path, verdict: dict[str, Any], packet: dict[str, Any]) -> None:
    promo = verdict.get("promote_runs") or packet.get("promotion_candidates", [])
    body = f"""# Verdict: {verdict.get('verdict')}

## Confidence
{verdict.get('confidence')}

## Executive Summary
{verdict.get('summary')}

## What the Experiment Taught Us
{verdict.get('primary_reason')}

## Best Runs
{_markdown_table(packet.get('top_runs', []), limit=10)}

## Stable vs Volatile
- Stable: {packet.get('dataset_comparison', {}).get('stable')}
- Volatile: {packet.get('dataset_comparison', {}).get('volatile')}

## Promotion Candidates
{_markdown_table(promo, limit=10)}

## Salvage / Refinement Evidence
{packet.get('salvage_candidates')}

## Scrap Evidence
{packet.get('scrap_evidence')}

## Recommended Next Action
{verdict.get('recommended_next_action') or verdict.get('verdict')}

## Human Approval
- [ ] Approve recommendation
- [ ] Override: scrap
- [ ] Override: promote Tier3
- [ ] Override: refine manually
"""
    output_path.write_text(body, encoding="utf-8")
