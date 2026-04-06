from pathlib import Path

from bt.experiments.hypothesis_runner import build_runtime_override
from bt.experiments.parallel_grid import build_hypothesis_manifest_rows
from bt.hypotheses.contract import HypothesisContract


HYPOTHESIS_PATH = "research/hypotheses/l1_h1c_volfloor_ema_pullback.yaml"


def test_l1_h1c_runtime_override_uses_registered_strategy_and_two_clock_defaults() -> None:
    contract = HypothesisContract.from_yaml(HYPOTHESIS_PATH)
    spec = contract.materialize_grid()[0]
    override = build_runtime_override(contract, spec, "Tier2")
    assert override["strategy"]["name"] == "volfloor_ema_pullback"
    assert override["strategy"]["timeframe"] == "15m"
    assert override["data"]["exit_timeframe"] == "1m"
    assert override["htf_resampler"]["timeframes"] == ["15m"]


def test_l1_h1c_manifest_rows_cover_expected_54_tier2_variants() -> None:
    contract = HypothesisContract.from_yaml(HYPOTHESIS_PATH)
    rows = build_hypothesis_manifest_rows(
        contract=contract,
        hypothesis_path=Path(HYPOTHESIS_PATH),
        phase="tier2",
    )
    assert len(rows) == 54
    assert {row["tier"] for row in rows} == {"Tier2"}
