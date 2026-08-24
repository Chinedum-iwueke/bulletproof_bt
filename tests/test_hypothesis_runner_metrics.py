import json

from bt.experiments.hypothesis_runner import _read_run_metrics


def test_current_performance_schema_maps_to_bridge_metrics(tmp_path) -> None:
    (tmp_path / "performance.json").write_text(json.dumps({
        "schema_version": 2,
        "total_trades": 8,
        "ev_r_gross": -0.7,
        "ev_r_net": -0.8,
        "gross_pnl": -30.0,
        "net_pnl": -35.0,
        "win_rate": 0.25,
        "max_drawdown_pct": -0.12,
    }))
    result = _read_run_metrics(tmp_path)
    assert result["num_trades"] == 8
    assert result["ev_r_net"] == -0.8
    assert result["pnl_gross"] == -30.0
    assert result["max_drawdown_r"] == 0.12
