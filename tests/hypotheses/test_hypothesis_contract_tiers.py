from __future__ import annotations

import pytest

from bt.experiments.hypothesis_runner import run_hypothesis_contract
from bt.hypotheses.contract import HypothesisContract
from bt.hypotheses.exceptions import MissingRequiredTierError


def _executor(spec: dict[str, object], tier: str) -> dict[str, object]:
    return {"num_trades": 1, "tier_seen": tier}


def test_default_tiers_enforced_when_missing() -> None:
    contract = HypothesisContract.from_dict(
        {
            "hypothesis_id": "X",
            "title": "X",
            "description": "",
            "research_layer": "L1",
            "hypothesis_family": "f",
            "version": "1",
            "required_indicators": ["adx"],
            "parameter_grid": {"p": [1]},
        }
    )
    assert contract.required_tiers() == ("Tier2", "Tier3")


def test_missing_required_tier_fails_loudly() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h2.yaml")
    with pytest.raises(MissingRequiredTierError):
        run_hypothesis_contract(
            contract,
            executor=_executor,
            symbol="BTCUSDT",
            timeframe="1m",
            start_ts="2024-01-01",
            end_ts="2024-01-02",
            available_tiers={"Tier2"},
        )
