from __future__ import annotations

from pathlib import Path

from bt.experiments.hypothesis_runner import build_runtime_override, run_hypothesis_contract
from bt.hypotheses.contract import HypothesisContract


def _executor(spec: dict[str, object], tier: str) -> dict[str, object]:
    return {
        "num_trades": 2,
        "ev_r_gross": 0.1,
        "ev_r_net": 0.08,
        "pnl_gross": 10,
        "pnl_net": 8,
        "hit_rate": 0.5,
        "max_drawdown_r": -0.3,
        "mae_mean_r": -0.1,
        "mfe_mean_r": 0.2,
        "avg_hold_bars": 5,
    }


def test_runner_emits_variant_times_tier_rows() -> None:
    contract = HypothesisContract.from_dict(
        {
            "hypothesis_id": "X",
            "title": "X",
            "description": "",
            "research_layer": "L1",
            "hypothesis_family": "f",
            "version": "1",
            "required_indicators": ["adx"],
            "parameter_grid": {"a": [1, 2]},
            "evaluation": {"required_tiers": ["Tier2", "Tier3"]},
        }
    )
    rows = run_hypothesis_contract(
        contract,
        executor=_executor,
        symbol="BTCUSDT",
        timeframe="1m",
        start_ts="2024-01-01",
        end_ts="2024-01-02",
        available_tiers={"Tier2", "Tier3"},
        phase="validate",
    )
    assert len(rows) == 4


def test_all_hypothesis_signal_timeframe_grid_values_materialize_strictly() -> None:
    for path in sorted(Path("research/hypotheses").glob("*.yaml")):
        contract = HypothesisContract.from_yaml(path)
        expected = {str(item).lower() for item in contract.schema.parameter_grid.get("signal_timeframe", ())}
        if not expected:
            continue

        observed = {
            str(spec["params"]["signal_timeframe"]).lower()
            for spec in contract.to_run_specs()
            if "signal_timeframe" in spec["params"]
        }

        assert observed == expected, path


def test_runtime_override_uses_each_grid_signal_timeframe_as_only_htf_resampler() -> None:
    for path in sorted(Path("research/hypotheses").glob("*.yaml")):
        contract = HypothesisContract.from_yaml(path)
        expected = {str(item).lower() for item in contract.schema.parameter_grid.get("signal_timeframe", ())}
        if not expected:
            continue

        for signal_timeframe in expected:
            spec = next(
                spec
                for spec in contract.to_run_specs()
                if str(spec["params"].get("signal_timeframe", "")).lower() == signal_timeframe
            )
            override = build_runtime_override(contract, spec, "Tier2")
            strategy_name = str(override["strategy"]["name"]).lower()
            timeframes = [str(item).lower() for item in override["htf_resampler"]["timeframes"]]

            declared_timeframes = {
                str(item).lower()
                for item in contract.schema.execution_semantics.get("htf_timeframes", ())
            }
            if declared_timeframes:
                assert set(timeframes) == declared_timeframes, path
                assert signal_timeframe in timeframes, path
            elif strategy_name == "l1_h3c_regime_switch_trend":
                assert signal_timeframe in timeframes, path
            else:
                assert timeframes == [signal_timeframe], path
                assert str(override["strategy"]["timeframe"]).lower() == signal_timeframe, path


def test_runtime_override_carries_contract_identity_for_trade_truth() -> None:
    contract = HypothesisContract.from_yaml(Path("research/hypotheses/l1_h1c_volfloor_ema_pullback.yaml"))
    spec = contract.to_run_specs()[0]

    override = build_runtime_override(contract, spec, "Tier2")

    assert override["identity"]["hypothesis_id"] == "L1-H1C"
    assert override["identity"]["grid_id"] == spec["grid_id"]
    assert override["identity"]["tier"] == "Tier2"
    assert override["identity"]["strategy_id"] == "volfloor_ema_pullback"
