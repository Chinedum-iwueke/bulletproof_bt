"""Research tier naming and evidence-mode helpers."""
from __future__ import annotations

from typing import Any


PORTFOLIO_RESEARCH_MODE = "portfolio_backtest"
SIGNAL_EPISODE_RESEARCH_MODE = "signal_episode"


def normalize_research_phase(phase: str | None) -> str:
    """Normalize legacy and explicit research phase names.

    ``tier2`` remains accepted for backward compatibility and maps to the
    portfolio-simulation Tier 2B path.
    """
    value = str(phase or "tier2b").strip().lower()
    if value == "tier2":
        return "tier2b"
    if value in {"tier2a", "tier2b", "tier3", "validate"}:
        return value
    raise ValueError("phase must be one of: tier2a, tier2b, tier2, tier3, validate")


def phase_to_contract_phase(phase: str | None) -> str:
    normalized = normalize_research_phase(phase)
    if normalized in {"tier2a", "tier2b"}:
        return "tier2"
    return normalized


def research_mode_for_phase(phase: str | None) -> str:
    normalized = normalize_research_phase(phase)
    return SIGNAL_EPISODE_RESEARCH_MODE if normalized == "tier2a" else PORTFOLIO_RESEARCH_MODE


def evidence_type_for_mode(research_mode: str | None) -> str:
    if str(research_mode or "").strip().lower() == SIGNAL_EPISODE_RESEARCH_MODE:
        return "signal_outcome"
    return "portfolio_outcome"


def research_metadata_for_phase(phase: str | None) -> dict[str, Any]:
    normalized = normalize_research_phase(phase)
    mode = research_mode_for_phase(normalized)
    is_signal_episode = mode == SIGNAL_EPISODE_RESEARCH_MODE
    return {
        "research_tier": normalized,
        "contract_phase": phase_to_contract_phase(normalized),
        "research_mode": mode,
        "evidence_type": evidence_type_for_mode(mode),
        "portfolio_constraints_applied": not is_signal_episode,
        "capital_path_valid": not is_signal_episode,
        "deployability_evidence": not is_signal_episode,
        "signal_episode_evidence": is_signal_episode,
    }
