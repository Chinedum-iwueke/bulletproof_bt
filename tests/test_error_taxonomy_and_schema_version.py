from __future__ import annotations

import inspect
import json
from pathlib import Path

import yaml

from bt.api import run_backtest
from bt.contracts.schema_versions import PERFORMANCE_SCHEMA_VERSION
from bt.core import reason_codes
from bt.risk import risk_engine


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


def test_run_status_contains_schema_version(tmp_path: Path) -> None:
    run_dir = _run_minimal_backtest(tmp_path, "status_schema")
    payload = json.loads((run_dir / "run_status.json").read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1


def test_performance_contains_schema_version(tmp_path: Path) -> None:
    run_dir = _run_minimal_backtest(tmp_path, "perf_schema")
    payload = json.loads((run_dir / "performance.json").read_text(encoding="utf-8"))
    assert payload["schema_version"] == PERFORMANCE_SCHEMA_VERSION


def test_reason_codes_are_centralized() -> None:
    source = inspect.getsource(risk_engine)
    assert "from bt.risk.reject_codes import" in source
    assert '"risk_rejected:' not in source

    assert reason_codes.RISK_REJECT_INSUFFICIENT_MARGIN == "risk_rejected:insufficient_margin"
    assert reason_codes.RISK_REJECT_MAX_POSITIONS == "risk_rejected:max_positions"
    assert reason_codes.RISK_REJECT_NOTIONAL_CAP == "risk_rejected:notional_cap"
    assert reason_codes.RISK_REJECT_STOP_UNRESOLVABLE == "risk_rejected:stop_unresolvable"
    assert reason_codes.RISK_REJECT_MIN_STOP_DISTANCE == "risk_rejected:min_stop_distance"
    assert reason_codes.FORCED_LIQUIDATION_END_OF_RUN == "liquidation:end_of_run"
    assert reason_codes.FORCED_LIQUIDATION_MARGIN == "liquidation:negative_free_margin"


def test_backward_compatibility_artifacts_keep_existing_keys(tmp_path: Path) -> None:
    run_dir = _run_minimal_backtest(tmp_path, "compat")

    run_status = json.loads((run_dir / "run_status.json").read_text(encoding="utf-8"))
    performance = json.loads((run_dir / "performance.json").read_text(encoding="utf-8"))

    assert run_status["status"] == "PASS"
    assert "status" in run_status
    assert "error_type" in run_status
    assert "total_trades" in performance
    assert "final_equity" in performance
