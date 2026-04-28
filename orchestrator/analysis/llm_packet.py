from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _trim_hypothesis(text: str, max_chars: int = 2500) -> str:
    trimmed = text.strip()
    if len(trimmed) <= max_chars:
        return trimmed
    return trimmed[:max_chars] + "\n... [truncated]"


def _top_bottom(rows: list[dict[str, Any]], top_n: int, bottom_n: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ranked = sorted(rows, key=lambda r: float(r.get("robustness_score", -999999) or -999999), reverse=True)
    return ranked[:top_n], list(reversed(ranked[-bottom_n:])) if bottom_n > 0 else []


def build_llm_packet(
    *,
    name: str,
    hypothesis_text: str,
    input_files: dict[str, str | None],
    stable_rows: list[dict[str, Any]],
    vol_rows: list[dict[str, Any]],
    diagnostics: dict[str, Any],
    preliminary: dict[str, Any],
    max_top_runs: int,
    max_bottom_runs: int,
) -> dict[str, Any]:
    combined = list(stable_rows) + list(vol_rows)
    top_runs, bottom_runs = _top_bottom(combined, max_top_runs, max_bottom_runs)

    return {
        "name": name,
        "hypothesis_text_excerpt": _trim_hypothesis(hypothesis_text),
        "input_files": input_files,
        "run_counts": {
            "stable": len(stable_rows),
            "volatile": len(vol_rows),
            "total": len(combined),
        },
        "top_runs": top_runs,
        "bottom_runs": bottom_runs,
        "dataset_comparison": diagnostics.get("dataset_comparison", {}),
        "promotion_candidates": diagnostics.get("promotion_candidates", []),
        "salvage_candidates": diagnostics.get("salvage_candidates", []),
        "scrap_evidence": diagnostics.get("scrap_evidence", []),
        "failure_mode": diagnostics.get("failure_mode"),
        "preliminary_verdict": preliminary.get("preliminary_verdict"),
        "allowed_verdicts": preliminary.get("allowed_verdicts", []),
    }


def build_llm_prompt(packet: dict[str, Any]) -> str:
    instructions = """You are a rigorous systematic trading research analyst reviewing Bulletproof_bt experiment results.

Your job is to decide whether this hypothesis should be scrapped, refined, promoted to Tier3, or marked inconclusive.

Important principles:
- Do not promote based only on best EV.
- Consider sample size, drawdown, tail behavior, gross-to-net cost drag, MFE/MAE, stable vs volatile consistency, and parameter logic.
- Negative EV can still be informative if conditional structure exists.
- Positive EV can still be fragile if sample size is weak or driven by one lucky tail trade.
- If evidence is insufficient, say inconclusive.
- Do not invent values not present in the packet.
- Return strict JSON only.

Return JSON matching:
{
  "verdict": "...",
  "confidence": 0.0,
  "summary": "...",
  "primary_reason": "...",
  "promote_runs": [],
  "refine_from_runs": [],
  "scrap_reason": null,
  "dominant_failure_mode": "...",
  "evidence": {
    "best_ev_r_net": null,
    "median_ev_r_net": null,
    "positive_runs": null,
    "best_dataset": null,
    "notes": []
  },
  "recommended_next_tests": [
    {
      "type": "...",
      "description": "...",
      "grid_size": 24,
      "parent_runs": []
    }
  ],
  "human_approval_required": true
}
"""
    return instructions + "\n\nPACKET_JSON:\n" + json.dumps(packet, indent=2)


def write_packet_files(output_dir: Path, name: str, packet: dict[str, Any], prompt: str) -> tuple[Path, Path]:
    packet_path = output_dir / f"{name}_llm_packet.json"
    prompt_path = output_dir / f"{name}_llm_prompt.txt"
    packet_path.write_text(json.dumps(packet, indent=2), encoding="utf-8")
    prompt_path.write_text(prompt, encoding="utf-8")
    return packet_path, prompt_path
