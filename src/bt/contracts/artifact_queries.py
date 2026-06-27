"""Canonical bounded artifact catalog and query semantics for the Program Copilot."""
from __future__ import annotations

from hashlib import sha256
import json
from typing import Any


ARTIFACT_TYPES = {"manifest", "run_config", "dataset", "trades", "metrics", "verdict_card", "log", "report", "incident", "spec", "memory"}
SENSITIVITY = {"public", "account_private", "program_private", "secret"}
QUERY_TYPES = {"run_metrics", "trade_cohorts", "verdict_cards", "first_failure", "assumption_lookup", "run_comparison", "artifact_manifest", "memory_search"}


def canonical_hash(value: Any) -> str:
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def catalog_artifact(entry: dict[str, Any]) -> dict[str, Any]:
    if entry.get("artifact_type") not in ARTIFACT_TYPES:
        raise ValueError("artifact_type_unsupported")
    if entry.get("sensitivity") not in SENSITIVITY:
        raise ValueError("artifact_sensitivity_invalid")
    required = ("catalog_id", "account_id", "program_id", "artifact_type", "object_id", "content_hash", "lineage", "summary", "searchable_text", "anchors", "schema")
    missing = [key for key in required if entry.get(key) is None]
    if missing:
        raise ValueError(f"artifact_catalog_missing:{','.join(missing)}")
    normalized = {**entry, "schema_version": "program_artifact_catalog_entry_v1"}
    return {**normalized, "catalog_hash": canonical_hash(normalized)}


def _citation(entry: dict[str, Any], anchor: dict[str, Any]) -> dict[str, Any]:
    return {"catalog_id": entry["catalog_id"], "object_id": entry["object_id"], "content_hash": entry["content_hash"], "anchor": anchor}


def query_artifacts(entries: list[dict[str, Any]], query: dict[str, Any], *, account_id: str, program_id: str, limit: int = 100) -> dict[str, Any]:
    query_type = str(query.get("query_type"))
    if query_type not in QUERY_TYPES:
        raise ValueError("artifact_query_unsupported")
    scoped = [entry for entry in entries if entry.get("account_id") == account_id and entry.get("program_id") == program_id and entry.get("sensitivity") != "secret"]
    if len(scoped) != len([entry for entry in entries if entry.get("program_id") == program_id]):
        pass  # Cross-tenant/secret entries are deliberately invisible.
    requested_ids = set(query.get("object_ids", []))
    if requested_ids:
        scoped = [entry for entry in scoped if entry.get("object_id") in requested_ids]
    artifact_map = {
        "run_metrics": {"metrics"}, "trade_cohorts": {"trades"}, "verdict_cards": {"verdict_card"},
        "first_failure": {"verdict_card", "log", "incident"}, "assumption_lookup": {"run_config", "spec", "manifest"},
        "run_comparison": {"metrics", "verdict_card"}, "artifact_manifest": ARTIFACT_TYPES, "memory_search": {"memory"},
    }
    selected = [entry for entry in scoped if entry.get("artifact_type") in artifact_map[query_type]][: max(1, min(limit, 500))]
    rows: list[dict[str, Any]] = []
    citations: list[dict[str, Any]] = []
    for entry in selected:
        payload = entry.get("query_payload", {}) if isinstance(entry.get("query_payload"), dict) else {}
        if query_type == "run_metrics":
            metrics = payload.get("metrics", payload)
            for key, value in list(metrics.items())[:limit] if isinstance(metrics, dict) else []:
                rows.append({"metric": key, "value": value, "unit": entry.get("units", {}).get(key), "sample": payload.get("sample"), "tier": payload.get("tier"), "run_id": entry.get("lineage", {}).get("run_id")})
        elif query_type == "trade_cohorts":
            trades = payload.get("trades", []) if isinstance(payload.get("trades"), list) else []
            rows.extend(trades[:limit])
        elif query_type == "first_failure":
            failures = payload.get("failures", []) if isinstance(payload.get("failures"), list) else []
            if failures:
                rows.append(sorted(failures, key=lambda item: str(item.get("timestamp", "")))[0])
        elif query_type == "run_comparison":
            rows.append({"object_id": entry["object_id"], "metrics": payload.get("metrics", {}), "verdict": payload.get("verdict")})
        elif query_type == "memory_search":
            needle = str(query.get("text", "")).lower()
            if not needle or needle in str(entry.get("searchable_text", "")).lower():
                rows.append({"object_id": entry["object_id"], "summary": entry["summary"], "score": entry.get("similarity", 1.0)})
        else:
            rows.append({"object_id": entry["object_id"], "artifact_type": entry["artifact_type"], "summary": entry["summary"], "payload": payload})
        anchor = entry.get("anchors", [{}])[0] if entry.get("anchors") else {}
        citations.append(_citation(entry, anchor))
    result = {"schema_version": "artifact_query_result_v1", "query_type": query_type, "rows": rows[:limit], "citations": citations, "result_count": min(len(rows), limit), "truncated": len(rows) > limit, "units_required": query_type in {"run_metrics", "run_comparison"}}
    return {**result, "result_hash": canonical_hash(result)}


def interpret_query_result(result: dict[str, Any], question: str) -> dict[str, Any]:
    rows = result.get("rows", [])
    facts = [{"statement": f"Canonical {result.get('query_type')} query returned {len(rows)} bounded row(s).", "citations": result.get("citations", [])[:3]}]
    unknowns = [] if rows else [{"statement": "The catalog contains no supported evidence for this question.", "reason": "insufficient_supported_artifacts"}]
    answer = {
        "schema_version": "cited_research_answer_v1", "question": question,
        "facts": facts, "inferences": [],
        "recommendations": [] if not rows else [{"statement": "Use the cited evidence to choose the next falsification test; do not promote from this answer alone.", "requires_confirmation": True}],
        "unknowns": unknowns, "citations": result.get("citations", []), "canonical_query_hash": result.get("result_hash"),
    }
    return {**answer, "answer_hash": canonical_hash(answer)}
