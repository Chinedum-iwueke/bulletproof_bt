from pathlib import Path

from bt.experiments.hypothesis_runner import build_runtime_override
from bt.experiments.parallel_grid import build_hypothesis_manifest
from bt.hypotheses.contract import HypothesisContract


def test_runtime_override_uses_l1_h5b_strategy_name() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h5b_vol_managed_har_trend.yaml")
    spec = contract.to_run_specs()[0]
    override = build_runtime_override(contract, spec, "Tier2")
    assert override["strategy"]["name"] == "l1_h5b_vol_managed_har_trend"
    assert override["strategy"]["timeframe"] == "15m"


def test_parallel_manifest_build_for_l1_h5b(tmp_path: Path) -> None:
    manifest = build_hypothesis_manifest(
        hypothesis_path=Path("research/hypotheses/l1_h5b_vol_managed_har_trend.yaml"),
        experiment_root=tmp_path / "exp",
        phase="tier2",
    )
    assert manifest.is_file()
    assert manifest.name == "l1_h5b_vol_managed_har_trend_tier2_grid.csv"
