from __future__ import annotations

from typing import Any


COPILOT_TURN_SCHEMA_VERSION = "research_copilot_turn_v1"
CANDIDATE_HYPOTHESIS_SCHEMA_VERSION = "candidate_hypothesis_v1"
COPILOT_MODES = {"exploratory", "direct_instruction", "source_analysis", "artifact_analysis"}
PROVENANCE_STATES = {"stated", "extracted", "inferred", "recommended", "confirmed", "unresolved", "unsupported"}
IMPLEMENTATION_READINESS = {"likely_supported", "needs_capability_check", "data_blocked"}


def validate_candidate_hypothesis(candidate: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    required = {
        "claim", "mechanism", "observable_proxy", "expected_direction", "horizon",
        "entry_idea", "exit_idea", "required_datasets", "falsification_test",
        "failure_modes", "implementation_readiness", "rationale", "source_citations",
    }
    if candidate.get("schema_version") != CANDIDATE_HYPOTHESIS_SCHEMA_VERSION:
        errors.append("candidate_schema_version_invalid")
    errors.extend(f"candidate_missing_{field}" for field in sorted(required) if field not in candidate)
    if candidate.get("implementation_readiness") not in IMPLEMENTATION_READINESS:
        errors.append("candidate_implementation_readiness_invalid")
    for field in ("required_datasets", "failure_modes", "source_citations"):
        if field in candidate and not isinstance(candidate[field], list):
            errors.append(f"candidate_{field}_must_be_list")
    for citation in candidate.get("source_citations", []) if isinstance(candidate.get("source_citations"), list) else []:
        if not isinstance(citation, dict) or not citation.get("source_id") or not citation.get("chunk_id"):
            errors.append("candidate_source_citation_invalid")
    return errors


def validate_research_copilot_turn(turn: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if turn.get("schema_version") != COPILOT_TURN_SCHEMA_VERSION:
        errors.append("copilot_turn_schema_version_invalid")
    if turn.get("mode") not in COPILOT_MODES:
        errors.append("copilot_turn_mode_invalid")
    if not isinstance(turn.get("response"), str) or not turn.get("response", "").strip():
        errors.append("copilot_turn_response_required")
    candidates = turn.get("candidates")
    if not isinstance(candidates, list):
        errors.append("copilot_turn_candidates_must_be_list")
    else:
        for index, candidate in enumerate(candidates):
            if not isinstance(candidate, dict):
                errors.append(f"candidate_{index}_must_be_object")
            else:
                errors.extend(f"candidate_{index}_{error}" for error in validate_candidate_hypothesis(candidate))
    state = turn.get("research_state")
    if not isinstance(state, dict):
        errors.append("copilot_turn_research_state_must_be_object")
    else:
        for field, value in state.items():
            if not isinstance(value, dict):
                errors.append(f"research_state_{field}_must_be_object")
                continue
            provenance = value.get("provenance")
            if provenance not in PROVENANCE_STATES:
                errors.append(f"research_state_{field}_provenance_invalid")
            confidence = value.get("confidence")
            if not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
                errors.append(f"research_state_{field}_confidence_invalid")
    if not isinstance(turn.get("warnings"), list):
        errors.append("copilot_turn_warnings_must_be_list")
    return errors

