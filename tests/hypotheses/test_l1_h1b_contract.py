from bt.hypotheses.contract import HypothesisContract


def test_l1_h1b_contract_loads_and_grid_filters_irrelevant_activation_params() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h1b_salvage_trend.yaml")
    assert contract.schema.metadata.hypothesis_id == "L1-H1B"
    rows = contract.materialize_grid()
    assert rows
    params = [row["params"] for row in rows]
    assert all(p["tp_enabled"] is False for p in params)
    bars_mode = [p for p in params if p["trail_activation_mode"] == "bars"]
    profit_mode = [p for p in params if p["trail_activation_mode"] == "profit_r"]
    assert bars_mode and profit_mode
    assert {p["trail_activate_after_profit_r"] for p in bars_mode} == {0.5}
    assert {p["trail_activate_after_bars"] for p in profit_mode} == {1}
