from bt.hypotheses.contract import HypothesisContract


def test_l1_h1c_contract_loads_and_locks_semantics() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h1c_volfloor_ema_pullback.yaml")
    assert contract.schema.metadata.hypothesis_id == "L1-H1C"
    sem = contract.schema.execution_semantics
    assert sem["base_data_frequency_expected"] == "1m"
    assert sem["signal_timeframe"] == "15m"
    assert sem["exit_monitoring_timeframe"] == "1m"
    assert sem["stop_model"] == "fixed_atr_multiple_at_entry"
    assert sem["stop_update_policy"] == "frozen_at_entry"
    assert sem["exit_model"] == "ema_trend_end"


def test_l1_h1c_grid_is_deterministic_and_54_variants() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h1c_volfloor_ema_pullback.yaml")
    one = contract.materialize_grid()
    two = contract.materialize_grid()
    assert one == two
    assert len(one) == 54
    assert {row["params"]["er_lookback"] for row in one} == {10, 16, 20}
