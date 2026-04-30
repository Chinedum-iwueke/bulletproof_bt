#!/usr/bin/env python3
"""Interpret completed experiment outputs and produce rule/LLM verdict artifacts.

Example:
    python orchestrator/interpret_experiment_results.py \
      --db research_db/research.sqlite \
      --name l1_h7c_high_selectivity_regime \
      --hypothesis research/hypotheses/l1_h7c_high_selectivity_regime.yaml \
      --stable-root outputs/l1_h7c_high_selectivity_regime_parallel_stable \
      --vol-root outputs/l1_h7c_high_selectivity_regime_parallel_vol \
      --llm-provider ollama \
      --model qwen2.5:14b
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from orchestrator.analysis import (
    build_llm_packet,
    build_llm_prompt,
    compute_diagnostics,
    compute_preliminary_verdict,
    load_experiment_context,
    score_runs,
    write_markdown_verdict,
)
from orchestrator.analysis.llm_client import call_llm_json
from orchestrator.analysis.llm_packet import write_packet_files
from orchestrator.db import ResearchDB


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Interpret experiment results into a verdict.")
    parser.add_argument("--db", default=None)
    parser.add_argument("--name", required=True)
    parser.add_argument("--hypothesis", required=True)
    parser.add_argument("--stable-root", required=True)
    parser.add_argument("--vol-root", required=True)
    parser.add_argument("--output-dir", default="research/verdicts")
    parser.add_argument("--no-llm", action="store_true", default=False)
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--packet-only", action="store_true", default=False)
    parser.add_argument("--max-top-runs", type=int, default=10)
    parser.add_argument("--max-bottom-runs", type=int, default=5)
    parser.add_argument("--min-trades", type=int, default=30)
    parser.add_argument("--promotion-min-ev", type=float, default=0.05)
    parser.add_argument("--promotion-min-trades", type=int, default=50)
    parser.add_argument("--llm-provider", default="ollama")
    parser.add_argument("--ollama-url", default="http://127.0.0.1:11434/api/generate")
    parser.add_argument("--model", default="qwen2.5:14b")
    parser.add_argument("--llm-timeout-seconds", type=int, default=600)
    parser.add_argument("--num-ctx", type=int, default=8192)
    parser.add_argument("--temperature", type=float, default=0.1)
    parser.add_argument("--max-output-tokens", type=int, default=4000)
    parser.add_argument("--api-key-env", default="OPENAI_API_KEY")
    parser.add_argument("--state-discovery-json", default=None)
    parser.add_argument("--state-discovery-md", default=None)
    parser.add_argument("--state-discovery-dir", default=None)
    return parser.parse_args()


def _validate_verdict(verdict: dict[str, Any], allowed: list[str]) -> bool:
    v = verdict.get("verdict")
    return isinstance(v, str) and v in allowed


def _default_from_preliminary(preliminary: dict[str, Any], diagnostics: dict[str, Any]) -> dict[str, Any]:
    return {
        "verdict": preliminary["preliminary_verdict"],
        "confidence": 0.55,
        "summary": preliminary["preliminary_reason"],
        "primary_reason": preliminary["preliminary_reason"],
        "promote_runs": [],
        "refine_from_runs": [],
        "scrap_reason": None,
        "dominant_failure_mode": diagnostics.get("failure_mode"),
        "evidence": {
            "best_ev_r_net": None,
            "median_ev_r_net": None,
            "positive_runs": None,
            "best_dataset": None,
            "notes": ["rule_based_fallback"],
        },
        "recommended_next_tests": [],
        "human_approval_required": True,
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _state_discovery_summary(args: argparse.Namespace) -> dict[str, Any] | None:
    json_paths: list[Path] = []
    md_paths: list[Path] = []
    if args.state_discovery_json:
        json_paths.append(Path(args.state_discovery_json))
    if args.state_discovery_md:
        md_paths.append(Path(args.state_discovery_md))
    if args.state_discovery_dir:
        sd_dir = Path(args.state_discovery_dir)
        json_paths.extend(
            [
                sd_dir / f"{args.name}_stable_state_findings.json",
                sd_dir / f"{args.name}_vol_state_findings.json",
                sd_dir / f"{args.name}_combined_state_findings.json",
            ]
        )
        md_paths.extend(
            [
                sd_dir / f"{args.name}_stable_state_findings.md",
                sd_dir / f"{args.name}_vol_state_findings.md",
                sd_dir / f"{args.name}_combined_state_findings.md",
            ]
        )
    existing_json = [p for p in json_paths if p.exists()]
    existing_md = [p for p in md_paths if p.exists()]
    if not existing_json and not existing_md:
        return None

    findings: list[dict[str, Any]] = []
    missing_fields: list[dict[str, Any]] = []
    for path in existing_json:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict):
            fs = payload.get("findings", [])
            if isinstance(fs, list):
                findings.extend([f for f in fs if isinstance(f, dict)])
    for path in existing_md:
        if "missing_fields" in path.name:
            try:
                missing_fields.append(json.loads(path.read_text(encoding="utf-8")))
            except Exception:
                pass

    def top(find_type: str) -> list[dict[str, Any]]:
        subset = [f for f in findings if f.get("finding_type") == find_type]
        subset.sort(key=lambda x: (x.get("finding_score") or 0, x.get("n_trades") or 0), reverse=True)
        return subset[:5]

    return {
        "source_json_paths": [str(p) for p in existing_json],
        "source_md_paths": [str(p) for p in existing_md],
        "strongest_positive_states": top("POSITIVE_EDGE_STATE"),
        "strongest_negative_states": top("NEGATIVE_EDGE_STATE"),
        "tail_generation_states": top("TAIL_GENERATION_STATE"),
        "cost_killed_states": top("COST_KILLED_STATE"),
        "exit_failure_states": top("EXIT_FAILURE_STATE"),
        "missing_state_fields_warnings": missing_fields,
    }


def _register_db(
    args: argparse.Namespace,
    *,
    verdict_json_path: Path,
    verdict_md_path: Path,
    packet_path: Path,
    prompt_path: Path,
    final_verdict: dict[str, Any],
) -> None:
    if not args.db:
        return
    db = ResearchDB(args.db, repo_root=PROJECT_ROOT)
    db.init_schema()
    hid = db.upsert_hypothesis_by_name(
        name=args.name,
        yaml_path=args.hypothesis,
        status="INTERPRETED",
        metadata={"source": "interpret_experiment_results.py"},
    )
    verdict_map = {
        "SCRAP": "SCRAP",
        "REFINE_ENTIRE_GRID": "REFINE_GATE",
        "REFINE_ENTRY": "REFINE_ENTRY",
        "REFINE_EXIT": "REFINE_EXIT",
        "REFINE_GATE": "REFINE_GATE",
        "REFINE_TIMEFRAME": "REFINE_TIMEFRAME",
        "REFINE_UNIVERSE": "ADD_STATE_FILTER",
        "ADD_STATE_FILTER": "ADD_STATE_FILTER",
        "PROMOTE_SINGLE_RUN_TIER3": "PROMOTE_TIER3",
        "PROMOTE_MULTIPLE_RUNS_TIER3": "PROMOTE_TIER3",
        "PROMOTE_FAMILY_TIER3": "PROMOTE_TIER3",
        "PROMOTE_FORWARD_TEST": "PROMOTE_FORWARD_TEST",
        "ADD_TO_ALPHA_ZOO": "ADD_TO_ALPHA_ZOO",
        "INCONCLUSIVE_NEEDS_MORE_DATA": "INCONCLUSIVE",
    }
    mapped_verdict = verdict_map.get(final_verdict.get("verdict"), "INCONCLUSIVE")
    verdict_id = db.create_verdict(
        hypothesis_id=hid,
        verdict=mapped_verdict,
        confidence=final_verdict.get("confidence"),
        summary=final_verdict.get("summary"),
        evidence=final_verdict.get("evidence"),
        recommended_next_action=final_verdict.get("verdict"),
        memo_path=verdict_md_path,
        approved_by_user=0,
    )

    for artifact_type, path in (
        ("verdict_json", verdict_json_path),
        ("verdict_markdown", verdict_md_path),
        ("llm_packet_json", packet_path),
        ("llm_prompt_txt", prompt_path),
    ):
        db.register_artifact(
            artifact_type=artifact_type,
            path=path,
            hypothesis_id=hid,
            description=f"interpretation output: {artifact_type}",
        )

    if final_verdict.get("human_approval_required", True):
        db.enqueue(
            queue_name="approval_queue",
            item_type="verdict",
            item_id=verdict_id,
            status="WAITING_FOR_APPROVAL",
            priority=70,
            payload={
                "name": args.name,
                "verdict_json": str(verdict_json_path),
                "verdict_md": str(verdict_md_path),
                "recommended_next_action": final_verdict.get("verdict"),
                "promote_runs": final_verdict.get("promote_runs", []),
                "recommended_next_tests": final_verdict.get("recommended_next_tests", []),
            },
        )
    db.close()


def main() -> int:
    args = parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    context = load_experiment_context(
        name=args.name,
        hypothesis_path=Path(args.hypothesis),
        stable_root=Path(args.stable_root),
        vol_root=Path(args.vol_root),
    )

    stable_scored = score_runs(context.stable.summary_rows)
    volatile_scored = score_runs(context.volatile.summary_rows)

    diagnostics = compute_diagnostics(
        stable_scored,
        volatile_scored,
        promotion_min_ev=args.promotion_min_ev,
        promotion_min_trades=args.promotion_min_trades,
    )
    preliminary = compute_preliminary_verdict(diagnostics, min_trades=args.min_trades)

    input_files = {
        "hypothesis": str(context.hypothesis_path),
        "stable_run_summary": str(context.stable.summary_path) if context.stable.summary_path else None,
        "volatile_run_summary": str(context.volatile.summary_path) if context.volatile.summary_path else None,
        "stable_runs_dataset": str(context.stable.runs_dataset_path) if context.stable.runs_dataset_path else None,
        "volatile_runs_dataset": str(context.volatile.runs_dataset_path) if context.volatile.runs_dataset_path else None,
        "stable_trades_dataset": str(context.stable.trades_dataset_path) if context.stable.trades_dataset_path else None,
        "volatile_trades_dataset": str(context.volatile.trades_dataset_path) if context.volatile.trades_dataset_path else None,
        "stable_strategy_summaries": [str(p) for p in context.stable.strategy_summary_paths],
        "volatile_strategy_summaries": [str(p) for p in context.volatile.strategy_summary_paths],
        "state_discovery_json": args.state_discovery_json,
        "state_discovery_md": args.state_discovery_md,
        "state_discovery_dir": args.state_discovery_dir,
    }
    state_discovery_summary = _state_discovery_summary(args)
    if state_discovery_summary:
        input_files["state_discovery_sources"] = {
            "json_paths": state_discovery_summary["source_json_paths"],
            "md_paths": state_discovery_summary["source_md_paths"],
        }

    packet = build_llm_packet(
        name=args.name,
        hypothesis_text=context.hypothesis_text,
        input_files=input_files,
        stable_rows=stable_scored,
        vol_rows=volatile_scored,
        diagnostics=diagnostics,
        preliminary=preliminary,
        max_top_runs=args.max_top_runs,
        max_bottom_runs=args.max_bottom_runs,
    )
    if state_discovery_summary:
        packet["state_discovery_summary"] = state_discovery_summary
    prompt = (
        build_llm_prompt(packet)
        + "\n\nReturn only valid JSON. Do not include markdown. Do not include commentary."
    )
    packet_path, prompt_path = write_packet_files(output_dir, args.name, packet, prompt)

    preliminary_verdict = _default_from_preliminary(preliminary, diagnostics)
    final_verdict = dict(preliminary_verdict)
    final_verdict["llm_provider"] = args.llm_provider
    final_verdict["llm_model"] = args.model
    final_verdict["llm_used"] = False
    final_verdict["llm_parse_error"] = False

    raw_llm_output: str | None = None
    if not args.no_llm and not args.packet_only and args.llm_provider != "none":
        llm_result = call_llm_json(
            provider=args.llm_provider,
            model=args.model,
            prompt=prompt,
            temperature=args.temperature,
            max_output_tokens=args.max_output_tokens,
            api_key_env=args.api_key_env,
            ollama_url=args.ollama_url,
            timeout_seconds=args.llm_timeout_seconds,
            num_ctx=args.num_ctx,
        )
        raw_llm_output = llm_result.get("raw")
        parsed = llm_result.get("parsed", {})
        if isinstance(parsed, dict) and _validate_verdict(parsed, preliminary["allowed_verdicts"]):
            final_verdict.update(parsed)
            final_verdict["llm_used"] = True
        else:
            final_verdict["llm_parse_error"] = True
            if raw_llm_output:
                raw_output_path = output_dir / f"{args.name}_llm_raw_response.txt"
                raw_output_path.write_text(raw_llm_output, encoding="utf-8")
    elif args.llm_provider == "none":
        final_verdict["llm_provider"] = "none"

    verdict_json_path = output_dir / f"{args.name}_verdict.json"
    verdict_md_path = output_dir / f"{args.name}_verdict.md"

    if raw_llm_output is not None:
        final_verdict["raw_llm_output"] = raw_llm_output

    if args.packet_only:
        final_verdict["summary"] = "Packet-only mode enabled; no verdict inference executed."

    _write_json(verdict_json_path, final_verdict)
    write_markdown_verdict(verdict_md_path, final_verdict, packet)

    if args.db and not args.dry_run:
        _register_db(
            args,
            verdict_json_path=verdict_json_path,
            verdict_md_path=verdict_md_path,
            packet_path=packet_path,
            prompt_path=prompt_path,
            final_verdict=final_verdict,
        )

    print(f"Wrote verdict json: {verdict_json_path}")
    print(f"Wrote verdict markdown: {verdict_md_path}")
    print(f"Wrote packet json: {packet_path}")
    print(f"Wrote prompt txt: {prompt_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
