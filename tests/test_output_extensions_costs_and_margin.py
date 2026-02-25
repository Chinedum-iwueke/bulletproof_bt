from __future__ import annotations

import json
from pathlib import Path

import yaml

from bt.api import run_backtest
from bt.contracts.schema_versions import PERFORMANCE_SCHEMA_VERSION
from bt.logging.summary import write_summary_txt


def _write_basic_config(path: Path) -> None:
    config: dict[str, object] = {
        "initial_cash": 100000.0,
        "max_leverage": 5.0,
        "signal_delay_bars": 1,
        "strategy": {"name": "coinflip", "p_trade": 0.0, "cooldown_bars": 0, "seed": 7},
        "risk": {"max_positions": 1, "risk_per_trade_pct": 0.001},
    }
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")


def _run_minimal_backtest(tmp_path: Path, run_name: str) -> Path:
    config_path = tmp_path / f"{run_name}.yaml"
    _write_basic_config(config_path)
    return Path(
        run_backtest(
            config_path=str(config_path),
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "out"),
            run_name=run_name,
        )
    )


def test_performance_includes_schema_costs_and_margin(tmp_path: Path) -> None:
    run_dir = _run_minimal_backtest(tmp_path, "output_extensions")
    payload = json.loads((run_dir / "performance.json").read_text(encoding="utf-8"))

    assert payload["schema_version"] == PERFORMANCE_SCHEMA_VERSION

    costs = payload["costs"]
    assert set(costs.keys()) == {"fees_total", "slippage_total", "spread_total", "commission_total"}
    assert all(isinstance(costs[k], (float, int)) for k in costs)

    margin = payload["margin"]
    expected_margin_keys = {
        "peak_used_margin",
        "avg_used_margin",
        "peak_utilization_pct",
        "avg_utilization_pct",
        "min_free_margin",
        "min_free_margin_pct",
    }
    assert set(margin.keys()) == expected_margin_keys
    assert all(isinstance(margin[k], (float, int)) for k in margin)


def test_cost_breakdown_exists_and_matches_performance_costs(tmp_path: Path) -> None:
    run_dir = _run_minimal_backtest(tmp_path, "cost_breakdown")

    performance = json.loads((run_dir / "performance.json").read_text(encoding="utf-8"))
    cost_breakdown_path = run_dir / "cost_breakdown.json"
    assert cost_breakdown_path.exists()

    cost_breakdown = json.loads(cost_breakdown_path.read_text(encoding="utf-8"))
    assert cost_breakdown["schema_version"] == 1
    assert cost_breakdown["totals"] == performance["costs"]


def test_summary_contains_cost_drag_and_margin_utilization_sections(tmp_path: Path) -> None:
    run_dir = _run_minimal_backtest(tmp_path, "summary_extensions")
    write_summary_txt(run_dir)
    summary = (run_dir / "summary.txt").read_text(encoding="utf-8")

    assert "COST DRAG" in summary
    assert "MARGIN UTILIZATION" in summary
