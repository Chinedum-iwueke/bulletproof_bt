from bt.hypotheses.contract import HypothesisContract


def test_l1_h5a_contract_loads_and_locks_semantics() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h5a_vol_managed_trend.yaml")
    sem = contract.schema.execution_semantics
    assert contract.schema.metadata.hypothesis_id == "L1-H5A"
    assert sem["signal_timeframe"] == "15m"
    assert sem["base_data_frequency_expected"] == "1m"
    assert sem["baseline_reference"] == "L1-H1"
    assert sem["overlay_type"] == "volatility_managed_exposure"
    assert sem["vol_estimator"] == "sqrt_mean_squared_returns"
    assert sem["sigma_reference_model"] == "rolling_median"
    assert sem["sizing_model"] == "qty_R_times_clipped_inverse_vol"
    assert sem["no_pyramiding"] is True


def test_l1_h5a_parameter_grid_is_deterministic() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h5a_vol_managed_trend.yaml")
    rows = contract.materialize_grid()
    assert len(rows) == 32
    assert rows == contract.materialize_grid()
