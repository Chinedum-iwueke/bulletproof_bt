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


def test_l1_h1_contract_locks_two_clock_and_frozen_stop_semantics() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h1_vol_floor_trend.yaml")
    sem = contract.schema.execution_semantics
    assert sem["base_data_frequency_expected"] == "1m"
    assert sem["signal_timeframe"] == "15m"
    assert sem["exit_monitoring_timeframe"] == "1m"
    assert sem["atr_source_timeframe"] == "signal_timeframe"
    assert sem["stop_model"] == "fixed_atr_multiple"
    assert sem["stop_update_policy"] == "frozen_at_entry"
    assert sem["tp_update_policy"] == "frozen_at_entry"
    assert sem["hold_time_unit"] == "signal_bars"
