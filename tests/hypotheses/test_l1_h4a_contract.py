from bt.hypotheses.contract import HypothesisContract


def test_l1_h4a_contract_loads_and_locks_semantics() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h4a_liquidity_gate_mean_reversion.yaml")
    sem = contract.schema.execution_semantics
    assert contract.schema.metadata.hypothesis_id == "L1-H4A"
    assert sem["signal_timeframe"] == "5m"
    assert sem["base_data_frequency_expected"] == "1m"
    assert sem["liquidity_proxy"] == "bar_range_half_over_close"
    assert sem["gate_model"] == "spread_proxy_quantile_gate"
    assert sem["baseline_reference"] == "L1-H2"
    assert sem["stop_update_policy"] == "frozen_at_entry"
    assert sem["hold_time_unit"] == "signal_bars"


def test_l1_h4a_parameter_grid_is_deterministic() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h4a_liquidity_gate_mean_reversion.yaml")
    rows = contract.materialize_grid()
    assert len(rows) == 32
    assert rows == contract.materialize_grid()
