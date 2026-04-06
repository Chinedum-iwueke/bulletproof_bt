from bt.hypotheses.contract import HypothesisContract


def test_l1_h6a_contract_loads_and_locks_semantics() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h6a_vov_gate_mean_reversion.yaml")
    sem = contract.schema.execution_semantics
    assert contract.schema.metadata.hypothesis_id == "L1-H6A"
    assert sem["signal_timeframe"] == "5m"
    assert sem["base_data_frequency_expected"] == "1m"
    assert sem["baseline_reference"] == "L1-H2"
    assert sem["vol_proxy"] == "atr_over_close"
    assert sem["vov_model"] == "rolling_std_of_rv"
    assert sem["gate_model"] == "vov_quantile_gate"
    assert sem["stop_update_policy"] == "frozen_at_entry"
    assert sem["hold_time_unit"] == "signal_bars"


def test_l1_h6a_parameter_grid_is_exactly_24_runs() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h6a_vov_gate_mean_reversion.yaml")
    rows = contract.materialize_grid()
    assert len(rows) == 24
    assert rows == contract.materialize_grid()
