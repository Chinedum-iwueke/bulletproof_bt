from bt.hypotheses.contract import HypothesisContract


def test_l1_h5b_contract_loads_and_locks_semantics() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h5b_vol_managed_har_trend.yaml")
    sem = contract.schema.execution_semantics
    assert contract.schema.metadata.hypothesis_id == "L1-H5B"
    assert sem["signal_timeframe"] == "15m"
    assert sem["base_data_frequency_expected"] == "1m"
    assert sem["baseline_reference"] == "L1-H3A"
    assert sem["gate_model"] == "har_rv_percentile"
    assert sem["stop_model"] == "fixed_close_sqrt_rvhat_multiple"
    assert sem["overlay_type"] == "volatility_managed_exposure"
    assert sem["vol_estimator"] == "sqrt_mean_squared_returns"
    assert sem["sigma_reference_model"] == "rolling_median"
    assert sem["sizing_model"] == "qty_R_times_clipped_inverse_vol"
    assert sem["hold_time_unit"] == "signal_bars"
    assert sem["no_pyramiding"] is True


def test_l1_h5b_parameter_grid_is_deterministic() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h5b_vol_managed_har_trend.yaml")
    rows = contract.materialize_grid()
    assert len(rows) == 128
    assert rows == contract.materialize_grid()
