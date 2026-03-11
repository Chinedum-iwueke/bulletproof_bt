from bt.hypotheses.contract import HypothesisContract


def test_l1_h3b_contract_loads_and_locked_semantics() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h3b_har_rv_gate_mean_reversion.yaml")
    sem = contract.schema.execution_semantics
    assert contract.schema.metadata.hypothesis_id == "L1-H3B"
    assert sem["signal_timeframe"] == "5m"
    assert sem["base_data_frequency_expected"] == "1m"
    assert sem["strategy_family"] == "mean_reversion"
    assert sem["baseline_reference"] == "L1-H2"
    assert sem["gate_model"] == "har_rv_percentile_low"
    assert sem["stop_model"] == "fixed_close_sqrt_rvhat_multiple"
    assert sem["coefficient_refit_cadence"] == "daily_on_completed_signal_day"
    assert sem["fit_method"] == "deterministic_ols"
    assert sem["vwap_mode"] == "session"


def test_l1_h3b_grid_deterministic_and_preregistered() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h3b_har_rv_gate_mean_reversion.yaml")
    one = contract.materialize_grid()
    two = contract.materialize_grid()
    assert one == two
    assert {row["params"]["fit_window_days"] for row in one} == {180, 365}
    assert {row["params"]["gate_quantile_low"] for row in one} == {0.3, 0.7}
    assert {row["params"]["z0"] for row in one} == {0.8, 1.0}
    assert {row["params"]["k"] for row in one} == {1.5, 2.0}
    assert {row["params"]["T_hold"] for row in one} == {12, 24}
