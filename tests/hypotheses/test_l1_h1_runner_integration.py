import pytest

from bt.experiments.hypothesis_runner import run_hypothesis_contract, validation_status
from bt.hypotheses.contract import HypothesisContract
from bt.hypotheses.exceptions import MissingRequiredTierError


def _executor(spec: dict[str, object], tier: str) -> dict[str, object]:
    return {"num_trades": 1, "ev_r_gross": 0.2, "ev_r_net": 0.1, "tier": tier}


def test_phase_tier2_and_tier3_runs() -> None:
    c = HypothesisContract.from_yaml("research/hypotheses/l1_h1_vol_floor_trend.yaml")
    t2 = run_hypothesis_contract(c, executor=_executor, symbol="BTCUSDT", timeframe="15m", start_ts="2024-01-01", end_ts="2024-02-01", available_tiers={"Tier2", "Tier3"}, phase="tier2")
    t3 = run_hypothesis_contract(c, executor=_executor, symbol="BTCUSDT", timeframe="15m", start_ts="2024-01-01", end_ts="2024-02-01", available_tiers={"Tier2", "Tier3"}, phase="tier3")
    assert {r["tier"] for r in t2} == {"Tier2"}
    assert {r["tier"] for r in t3} == {"Tier3"}


def test_missing_phase_tier_fails_loudly() -> None:
    c = HypothesisContract.from_yaml("research/hypotheses/l1_h1_vol_floor_trend.yaml")
    with pytest.raises(MissingRequiredTierError):
        run_hypothesis_contract(c, executor=_executor, symbol="BTCUSDT", timeframe="15m", start_ts="2024-01-01", end_ts="2024-02-01", available_tiers={"Tier2"}, phase="tier3")


def test_validation_status_requires_both_tiers() -> None:
    c = HypothesisContract.from_yaml("research/hypotheses/l1_h1_vol_floor_trend.yaml")
    assert validation_status(c, {"Tier2"}) == "incomplete"
    assert validation_status(c, {"Tier2", "Tier3"}) == "validated"
