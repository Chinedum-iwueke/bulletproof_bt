from bt.hypotheses.contract import HypothesisContract


def test_l1_h10a_contract_loads_and_grid_size() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h10a_mean_reversion_small_tp.yaml")
    rows = contract.materialize_grid()
    assert len(rows) == 24
    assert contract.schema.execution_semantics["risk_accounting"] == "engine_canonical_R"


def test_l1_h10b_contract_loads_and_grid_size() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h10b_breakout_scalping.yaml")
    rows = contract.materialize_grid()
    assert len(rows) == 24
    assert contract.schema.execution_semantics["exit_monitoring_timeframe"] == "1m"
