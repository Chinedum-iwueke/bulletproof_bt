from bt.hypotheses.contract import HypothesisContract


def test_l1_h1_contract_loads_and_tiers() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h1_vol_floor_trend.yaml")
    assert contract.schema.metadata.hypothesis_id == "L1-H1"
    assert contract.required_tiers() == ("Tier2", "Tier3")


def test_l1_h1_grid_deterministic() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h1_vol_floor_trend.yaml")
    one = contract.materialize_grid()
    two = contract.materialize_grid()
    assert one == two
    assert {v["params"]["tp_enabled"] for v in one} == {False, True}
