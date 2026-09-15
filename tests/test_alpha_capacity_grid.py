from scripts import run_alpha_research_assignment as runner
from bt.governance.research_bridge import BridgeError
import pytest


def test_parallel_grid_keeps_preregistered_order_and_caps_pool(monkeypatch):
    counts = []
    class Pool:
        def __init__(self, max_workers, mp_context):
            counts.append(max_workers)
            assert mp_context.get_start_method() == "spawn"
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def map(self, function, jobs):
            return map(function, jobs)
    monkeypatch.setattr(runner, "ProcessPoolExecutor", Pool)
    monkeypatch.setattr(runner, "execute_hypothesis_variant", lambda **job: job)
    jobs = [{"run_slug": f"row_{n}"} for n in range(8)]
    assert runner.execute_variant_grid(jobs, 8) == jobs
    assert counts == [8]
    assert runner.execute_variant_grid(jobs[:2], 8) == jobs[:2]
    assert counts[-1] == 2


@pytest.mark.parametrize("count,workers", [(0,8),(9,8),(8,9),(8,0)])
def test_invalid_grid_fails_before_execution(count, workers):
    with pytest.raises(BridgeError):
        runner.execute_variant_grid([{}] * count, workers)


def test_classic_engine_serial_parallel_trade_and_equity_parity(tmp_path):
    import numpy as np
    import pandas as pd
    import yaml
    from bt.hypotheses.contract import HypothesisContract

    source = yaml.safe_load(open("research/hypotheses/l1_h1_vol_floor_trend.yaml"))
    source["indicator_defaults"]["vol_percentile_window_days"] = 1
    source["parameter_grid"] = {
        "signal_timeframe": ["5m"], "theta_vol": [0.0], "k_atr": [2.0, 2.5],
        "T_hold": [4], "tp_enabled": [True], "m_atr": [2.0],
    }
    contract = HypothesisContract.from_dict(source)
    count = 6000
    close = 100 + np.arange(count) * 0.002 + np.sin(np.arange(count) / 80)
    data = tmp_path / "fixture.parquet"
    pd.DataFrame({"ts": pd.date_range("2025-01-04", periods=count, freq="min", tz="UTC"),
        "symbol": "BTCUSDT", "open": close, "high": close + .2, "low": close - .2,
        "close": close, "volume": 10000.0}).to_parquet(data)
    jobs = [dict(contract=contract, spec=spec, tier="Tier2", config_path="configs/engine.yaml",
                 data_path=str(data), out_root=str(tmp_path / "serial"), run_slug=f"row_{index}",
                 phase="tier2b") for index, spec in enumerate(contract.to_run_specs())]
    serial = runner.execute_variant_grid(jobs, 1)
    parallel = runner.execute_variant_grid([{**job, "out_root": str(tmp_path / "parallel")} for job in jobs], 2)
    from pathlib import Path
    for left, right in zip(serial, parallel, strict=True):
        for name in ("trades.csv", "equity.csv"):
            a = pd.read_csv(Path(left["run_dir"]) / name)
            b = pd.read_csv(Path(right["run_dir"]) / name)
            assert set(a.columns) == set(b.columns)
            pd.testing.assert_frame_equal(a, b.reindex(columns=a.columns), check_exact=True)
            if name == "trades.csv":
                assert len(a) > 0
