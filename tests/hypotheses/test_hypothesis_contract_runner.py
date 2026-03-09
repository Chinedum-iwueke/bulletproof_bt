from __future__ import annotations

from bt.experiments.hypothesis_runner import run_hypothesis_contract
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
