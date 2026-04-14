from pathlib import Path

from bt.experiments.hypothesis_runner import build_runtime_override
from bt.experiments.parallel_grid import build_hypothesis_manifest
from bt.hypotheses.contract import HypothesisContract


def test_l1_h11_contracts_load_and_have_exact_24_grids() -> None:
    for path in [
        "research/hypotheses/l1_h11a_quality_filtered_continuation.yaml",
        "research/hypotheses/l1_h11b_pullback_geometry_impulse.yaml",
        "research/hypotheses/l1_h11c_protection_discipline.yaml",
    ]:
        contract = HypothesisContract.from_yaml(path)
        rows = contract.materialize_grid()
        assert len(rows) == 24
        assert contract.schema.execution_semantics["base_data_frequency_expected"] == "1m"
        assert contract.schema.execution_semantics["exit_monitoring_timeframe"] == "1m"
        assert contract.schema.execution_semantics["risk_accounting"] == "engine_canonical_R"


def test_l1_h11_runner_compatibility_runtime_override_and_manifest(tmp_path: Path) -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h11a_quality_filtered_continuation.yaml")
    spec = next(row for row in contract.to_run_specs() if row["params"]["signal_timeframe"] == "1h")
    override = build_runtime_override(contract, spec, "Tier2")
    assert override["strategy"]["name"] == "l1_h11_quality_filtered_continuation"
    assert override["strategy"]["timeframe"] == "1h"

    manifest = build_hypothesis_manifest(
        hypothesis_path=Path("research/hypotheses/l1_h11c_protection_discipline.yaml"),
        experiment_root=tmp_path / "exp",
        phase="tier2",
    )
    assert manifest.is_file()
