from pathlib import Path

from bt.experiments.hypothesis_runner import build_runtime_override
from bt.experiments.parallel_grid import build_hypothesis_manifest
from bt.hypotheses.contract import HypothesisContract


def test_l1_h11_contracts_load_and_have_exact_24_grids() -> None:
    for path in [
        "research/hypotheses/l1_h11a.yaml",
        "research/hypotheses/l1_h11b.yaml",
        "research/hypotheses/l1_h11c.yaml",
    ]:
        contract = HypothesisContract.from_yaml(path)
        rows = contract.materialize_grid()
        assert len(rows) == 24
        assert contract.schema.execution_semantics["base_data_frequency_expected"] == "1m"
        assert contract.schema.execution_semantics["exit_monitoring_timeframe"] == "1m"
        assert contract.schema.execution_semantics["risk_accounting"] == "engine_canonical_R"


def test_l1_h11a_refined_contract_has_single_four_run_grid() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h11a_refined.yaml")
    rows = contract.materialize_grid()
    assert len(rows) == 4
    profiles = {row["params"]["h11a_tuning_profile"] for row in rows}
    assert profiles == {
        "h11a_1h_core_protected",
        "h11a_1h_quality_balanced",
        "h11a_15m_liquid_midvol_runner",
        "h11a_15m_explosive_moderate",
    }
    assert sum(row["params"]["signal_timeframe"] == "1h" for row in rows) == 2
    assert sum(row["params"]["signal_timeframe"] == "15m" for row in rows) == 2


def test_l1_h11b_refined_contract_has_single_four_run_grid() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h11b_refined.yaml")
    rows = contract.materialize_grid()
    assert len(rows) == 4
    profiles = {row["params"]["h11_tuning_profile"] for row in rows}
    assert profiles == {
        "h11b_1h_core_geometry",
        "h11b_1h_mild_basis_runner",
        "h11b_15m_midvol_funding_squeeze",
        "h11b_15m_liquid_mild_squeeze",
    }
    assert sum(row["params"]["signal_timeframe"] == "1h" for row in rows) == 2
    assert sum(row["params"]["signal_timeframe"] == "15m" for row in rows) == 2


def test_l1_h11c_refined_contract_has_single_four_run_grid() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h11c_refined.yaml")
    rows = contract.materialize_grid()
    assert len(rows) == 4
    profiles = {row["params"]["h11_tuning_profile"] for row in rows}
    assert profiles == {
        "h11c_1h_core_protected",
        "h11c_1h_mid_moderate_runner",
        "h11c_15m_fragile_extreme_runner",
        "h11c_15m_mid_fragile_basis_runner",
    }
    assert sum(row["params"]["signal_timeframe"] == "1h" for row in rows) == 2
    assert sum(row["params"]["signal_timeframe"] == "15m" for row in rows) == 2


def test_l1_h11_runner_compatibility_runtime_override_and_manifest(tmp_path: Path) -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h11a.yaml")
    spec = next(row for row in contract.to_run_specs() if row["params"]["signal_timeframe"] == "1h")
    override = build_runtime_override(contract, spec, "Tier2")
    assert override["strategy"]["name"] == "l1_h11_quality_filtered_continuation"
    assert override["strategy"]["timeframe"] == "1h"

    manifest = build_hypothesis_manifest(
        hypothesis_path=Path("research/hypotheses/l1_h11c.yaml"),
        experiment_root=tmp_path / "exp",
        phase="tier2",
    )
    assert manifest.is_file()


def test_l1_h11_tier2a_runtime_override_rejects_dust_risk_fills() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h11a_refined.yaml")
    spec = next(row for row in contract.to_run_specs() if row["params"]["signal_timeframe"] == "1h")

    override = build_runtime_override(contract, spec, "Tier2", phase="tier2a")

    assert override["research"]["research_mode"] == "signal_episode"
    assert override["risk"]["cap_policy"] == "allow_clip_with_truth"
    assert override["risk"]["min_risk_utilization_pct"] == 0.10
    assert override["risk"]["report_under_risked_trades"] is True
    assert override["risk"]["signal_episode_sizing_equity"] == "initial_cash"
