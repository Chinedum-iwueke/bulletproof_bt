from __future__ import annotations

from pathlib import Path

from bt.experiments.hypothesis_runner import build_runtime_override, resolve_phase_tiers
from bt.experiments.parallel_grid import build_hypothesis_manifest_rows
from bt.hypotheses.contract import HypothesisContract
from bt.research_tiers import (
    phase_to_contract_phase,
    research_metadata_for_phase,
    research_mode_for_phase,
)
from orchestrator.research_memory.trade_memory import normalize_trade


def test_tier2_alias_maps_to_portfolio_tier2b() -> None:
    assert phase_to_contract_phase("tier2") == "tier2"
    assert research_mode_for_phase("tier2") == "portfolio_backtest"
    assert research_metadata_for_phase("tier2")["research_tier"] == "tier2b"


def test_tier2a_uses_tier2_contract_rows_but_signal_episode_labels() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h1_vol_floor_trend.yaml")

    rows = build_hypothesis_manifest_rows(
        contract=contract,
        hypothesis_path=Path("research/hypotheses/l1_h1_vol_floor_trend.yaml"),
        phase="tier2a",
    )

    assert rows
    assert {row["phase"] for row in rows} == {"tier2a"}
    assert {row["tier"] for row in rows} == {"Tier2"}
    assert resolve_phase_tiers(contract, "tier2a") == ("Tier2",)


def test_tier2a_runtime_override_relaxes_portfolio_constraints_and_labels_evidence() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h1_vol_floor_trend.yaml")
    spec = contract.to_run_specs()[0]

    override = build_runtime_override(contract, spec, "Tier2", phase="tier2a")

    assert override["identity"]["research_tier"] == "tier2a"
    assert override["identity"]["research_mode"] == "signal_episode"
    assert override["research"]["evidence_type"] == "signal_outcome"
    assert override["research"]["capital_path_valid"] is False
    assert override["research"]["portfolio_constraints_applied"] is False
    assert override["outputs"]["decision_logging_profile"] == "research_sparse"
    assert override["risk"]["max_positions"] == 1_000_000
    assert override["risk"]["max_gross_notional_pct_equity"] is None


def test_tier2b_runtime_override_is_portfolio_evidence() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h1_vol_floor_trend.yaml")
    spec = contract.to_run_specs()[0]

    override = build_runtime_override(contract, spec, "Tier2", phase="tier2b")

    assert override["identity"]["research_tier"] == "tier2b"
    assert override["identity"]["research_mode"] == "portfolio_backtest"
    assert override["research"]["evidence_type"] == "portfolio_outcome"
    assert override["research"]["capital_path_valid"] is True
    assert "outputs" not in override


def test_research_memory_normalizes_signal_episode_labels() -> None:
    row = {
        "trade_id": "t1",
        "run_id": "run_1",
        "identity_ts_signal": "2024-01-01T00:00:00+00:00",
        "pnl_net": 10.0,
        "r_net": 1.0,
        "research_tier": "tier2a",
        "research_mode": "signal_episode",
        "evidence_type": "signal_outcome",
        "portfolio_constraints_applied": False,
        "capital_path_valid": False,
        "deployability_evidence": False,
        "signal_episode_evidence": True,
    }

    rec = normalize_trade(row, context={"experiment_root": "outputs/tier2a/demo_parallel_stable"}, row_index=0)

    assert rec["research_tier"] == "tier2a"
    assert rec["research_mode"] == "signal_episode"
    assert rec["evidence_type"] == "signal_outcome"
    assert rec["portfolio_constraints_applied"] == 0
    assert rec["capital_path_valid"] == 0
    assert rec["deployability_evidence"] == 0
    assert rec["signal_episode_evidence"] == 1
    assert rec["metrics_valid"] == 1
