from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from bt.saas.service import IngestionError, StrategyRobustnessLabService


def _write_trade_log(path: Path) -> None:
    pd.DataFrame(
        {
            "entry_time": [
                "2024-01-01T00:00:00Z",
                "2024-01-01T01:00:00Z",
                "2024-01-01T02:00:00Z",
                "2024-01-01T03:00:00Z",
            ],
            "exit_time": [
                "2024-01-01T00:30:00Z",
                "2024-01-01T01:30:00Z",
                "2024-01-01T02:30:00Z",
                "2024-01-01T03:20:00Z",
            ],
            "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT", "BTCUSDT"],
            "direction": ["LONG", "LONG", "SHORT", "SHORT"],
            "entry_price": [100.0, 101.0, 103.0, 100.0],
            "exit_price": [102.0, 99.0, 101.0, 102.0],
            "quantity": [1.0, 1.0, 1.0, 1.0],
            "fees": [0.2, 0.2, 0.2, 0.2],
            "risk_amount": [2.0, 2.0, 2.0, 2.0],
            "mae_price": [0.5, 1.0, 0.7, 1.2],
            "mfe_price": [2.5, 0.8, 2.2, 0.9],
        }
    ).to_csv(path, index=False)


def test_trade_log_ingestion_normalizes_and_infers_pnl(tmp_path: Path) -> None:
    trade_log = tmp_path / "trade_log.csv"
    _write_trade_log(trade_log)

    service = StrategyRobustnessLabService()
    run = service.ingest_trade_log(trade_log, strategy_name="sample_strategy")

    assert run.source == "trade_log"
    assert run.metadata["strategy_name"] == "sample_strategy"
    assert len(run.trades) == 4
    assert run.trades["pnl_net"].notna().all()
    assert run.trades["r_multiple_net"].notna().all()


def test_invalid_upload_returns_actionable_error(tmp_path: Path) -> None:
    bad = tmp_path / "bad.csv"
    pd.DataFrame({"symbol": ["BTCUSDT"], "side": ["BUY"]}).to_csv(bad, index=False)

    service = StrategyRobustnessLabService()
    with pytest.raises(IngestionError, match="missing required columns"):
        service.ingest_trade_log(bad)


def test_monte_carlo_deterministic_for_same_seed(tmp_path: Path) -> None:
    trade_log = tmp_path / "trade_log.csv"
    _write_trade_log(trade_log)

    service = StrategyRobustnessLabService()
    run = service.ingest_trade_log(trade_log)

    payload_a = service.build_dashboard_payload(run, seed=11, simulations=300)
    payload_b = service.build_dashboard_payload(run, seed=11, simulations=300)

    assert payload_a["monte_carlo"]["worst_drawdown_pct"] == payload_b["monte_carlo"]["worst_drawdown_pct"]
    assert payload_a["monte_carlo"]["drawdown_distribution_pct"] == payload_b["monte_carlo"]["drawdown_distribution_pct"]


def test_risk_of_ruin_payload_contains_survivability_fields(tmp_path: Path) -> None:
    trade_log = tmp_path / "trade_log.csv"
    _write_trade_log(trade_log)

    service = StrategyRobustnessLabService()
    run = service.ingest_trade_log(trade_log)
    payload = service.build_dashboard_payload(
        run,
        seed=7,
        simulations=250,
        account_size=50_000.0,
        risk_per_trade_pct=0.01,
    )

    ror = payload["risk_of_ruin"]
    assert "probability_of_ruin" in ror
    assert ror["summary_metrics"]["probability_of_ruin"] is not None
    assert ror["summary_metrics"]["expected_stress_drawdown"] is not None
    assert ror["summary_metrics"]["survival_probability"] is not None
    assert "probability_drawdown_30" in ror
    assert "probability_drawdown_50" in ror
    assert "risk_scenarios" in ror and len(ror["risk_scenarios"]) >= 4
    assert ror["account_size"] == 50_000.0
    assert ror["projected_risk_capital_per_trade"] == 500.0


def test_risk_of_ruin_payload_is_limited_without_sizing_inputs(tmp_path: Path) -> None:
    trade_log = tmp_path / "trade_log.csv"
    _write_trade_log(trade_log)

    service = StrategyRobustnessLabService()
    run = service.ingest_trade_log(trade_log)
    payload = service.build_dashboard_payload(run, seed=7, simulations=250)

    ror = payload["risk_of_ruin"]
    assert ror["status"] == "limited"
    assert ror["summary_metrics"]["probability_of_ruin"] is None
    assert "account_size" in ror["metadata"]["missing_required_inputs"]
    assert "risk_per_trade_pct" in ror["metadata"]["missing_required_inputs"]


def test_dashboard_payload_structure_and_json_ready(tmp_path: Path) -> None:
    trade_log = tmp_path / "trade_log.csv"
    _write_trade_log(trade_log)

    service = StrategyRobustnessLabService()
    run = service.ingest_trade_log(trade_log)
    payload = service.build_dashboard_payload(run)

    expected = {
        "overview",
        "trade_distribution",
        "monte_carlo",
        "parameter_stability",
        "execution_sensitivity",
        "regime_analysis",
        "risk_of_ruin",
        "score",
        "validation_report",
    }
    assert expected.issubset(payload.keys())
    json.dumps(payload)


def test_execution_sensitivity_emits_scenarios_for_trade_only_input(tmp_path: Path) -> None:
    trade_log = tmp_path / "trade_log.csv"
    _write_trade_log(trade_log)

    service = StrategyRobustnessLabService()
    run = service.ingest_trade_log(trade_log)
    payload = service.build_dashboard_payload(run)["execution_sensitivity"]

    assert "summary_metrics" in payload
    assert "scenarios" in payload
    assert len(payload["scenarios"]) >= 2
    assert payload["scenarios"][0]["name"] == "baseline"
    assert "figures" in payload and payload["figures"]
    assert payload["summary_metrics"]["baseline_expectancy"] == pytest.approx(payload["baseline_ev_net"])
    assert isinstance(payload["interpretation"], dict)
    cautions = payload["interpretation"].get("cautions", [])
    assert isinstance(cautions, list)
    assert "metadata" in payload
    assert payload["metadata"]["scenario_count"] == len(payload["scenarios"])


def test_score_contract_and_weights_exposed(tmp_path: Path) -> None:
    trade_log = tmp_path / "trade_log.csv"
    _write_trade_log(trade_log)

    service = StrategyRobustnessLabService()
    run = service.ingest_trade_log(trade_log)
    payload = service.build_dashboard_payload(run)

    score = payload["score"]
    assert 0.0 <= score["overall"] <= 100.0
    for key in [
        "statistical_quality",
        "monte_carlo_stability",
        "drawdown_resilience",
        "execution_resilience",
        "parameter_stability",
    ]:
        assert key in score["sub_scores"]
    assert score["methodology"]["weights"]["statistical_quality"] == 0.25


def test_report_payload_assembly(tmp_path: Path) -> None:
    trade_log = tmp_path / "trade_log.csv"
    _write_trade_log(trade_log)

    service = StrategyRobustnessLabService()
    run = service.ingest_trade_log(trade_log)
    report = service.build_dashboard_payload(run, seed=99)["validation_report"]

    assert "strategy_summary" in report
    assert "assumptions" in report
    assert "performance_summary" in report
    assert "monte_carlo_diagnostics" in report
    assert "final_verdict" in report
    assert "executive_verdict" in report
    assert report["executive_verdict"]["status"] in {
        "robust",
        "conditional",
        "fragile",
        "not_deployment_ready",
    }
    assert report["confidence_level"]["level"] in {"high", "medium", "low"}
    assert "deployment_guidance" in report
    assert "diagnostics_summary" in report
    assert "key_metrics_snapshot" in report
    assert "report" in report
    assert "metadata" in report["report"]
    assert "report_figures" in report["report"]


def test_report_payload_assembly_for_richer_run_artifacts(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_report"
    run_dir.mkdir(parents=True)
    _write_trade_log(run_dir / "trades.csv")
    pd.DataFrame(
        {
            "ts": ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"],
            "equity": [100_000.0, 100_150.0],
        }
    ).to_csv(run_dir / "equity.csv", index=False)
    (run_dir / "performance.json").write_text(
        json.dumps(
            {
                "initial_equity": 100_000.0,
                "final_equity": 100_150.0,
                "ev_net": 12.5,
                "win_rate": 0.5,
                "max_drawdown_pct": -3.0,
                "total_trades": 4,
            }
        ),
        encoding="utf-8",
    )

    service = StrategyRobustnessLabService()
    run = service.ingest_run_artifacts(run_dir)
    report = service.build_dashboard_payload(
        run,
        seed=21,
        simulations=400,
        account_size=100_000.0,
        risk_per_trade_pct=0.01,
    )["validation_report"]

    report_native = report["report"]
    assert "executive_verdict" in report_native
    assert "confidence_level" in report_native
    assert "executive_summary" in report_native
    assert "diagnostics_summary" in report_native
    assert "methodology" in report_native
    assert "limitations" in report_native
    assert "deployment_guidance" in report_native
    assert "recommendations" in report_native
    assert "key_metrics_snapshot" in report_native
    assert "metadata" in report_native


def test_ingest_run_artifacts_reuses_existing_artifacts(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_a"
    run_dir.mkdir(parents=True)

    _write_trade_log(run_dir / "trades.csv")
    pd.DataFrame(
        {
            "ts": ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"],
            "equity": [100_000.0, 100_100.0],
        }
    ).to_csv(run_dir / "equity.csv", index=False)
    (run_dir / "performance.json").write_text(
        json.dumps(
            {
                "initial_equity": 100_000.0,
                "final_equity": 100_100.0,
                "ev_net": 25.0,
                "win_rate": 0.5,
                "max_drawdown_pct": -4.0,
                "total_trades": 4,
            }
        ),
        encoding="utf-8",
    )

    service = StrategyRobustnessLabService()
    run = service.ingest_run_artifacts(run_dir)

    assert run.source == "run_artifacts"
    assert run.metadata["strategy_name"] == "run_a"
    assert float(run.performance["final_equity"]) == 100_100.0


def test_ingest_parameter_sweep_bundle_supports_manifest_contract(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "sweep_bundle"
    runs_dir = bundle_dir / "runs"
    runs_dir.mkdir(parents=True)

    pd.DataFrame(
        {
            "entry_time": ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"],
            "exit_time": ["2024-01-01T00:10:00Z", "2024-01-01T01:10:00Z"],
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "side": ["LONG", "SHORT"],
            "entry_price": [100.0, 101.0],
            "exit_price": [101.0, 99.0],
            "quantity": [1.0, 1.0],
            "fees": [0.05, 0.05],
        }
    ).to_csv(runs_dir / "run_1.csv", index=False)
    pd.DataFrame(
        {
            "entry_time": ["2024-01-02T00:00:00Z", "2024-01-02T01:00:00Z"],
            "exit_time": ["2024-01-02T00:10:00Z", "2024-01-02T01:10:00Z"],
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "side": ["LONG", "SHORT"],
            "entry_price": [100.0, 101.0],
            "exit_price": [99.5, 102.0],
            "quantity": [1.0, 1.0],
            "fees": [0.05, 0.05],
        }
    ).to_csv(runs_dir / "run_2.csv", index=False)

    manifest = {
        "strategy_name": "grid_alpha",
        "parameter_names": ["lookback", "threshold"],
        "runs": [
            {
                "run_id": "run_1",
                "params": {"lookback": 10, "threshold": 1.2},
                "trades_file": "runs/run_1.csv",
                "summary": {"ev_net": 1.2},
            },
            {
                "run_id": "run_2",
                "params": {"lookback": 20, "threshold": 1.2},
                "trades_file": "runs/run_2.csv",
                "summary": {"ev_net": -0.4},
            },
        ],
    }
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    service = StrategyRobustnessLabService()
    parsed = service.ingest_parameter_sweep_bundle(bundle_dir)
    result = service.run_analysis_from_parsed_artifact(parsed)

    assert parsed.artifact_kind == "parameter_sweep"
    assert parsed.parameter_sweep is not None
    assert len(parsed.parameter_sweep.runs) == 2
    assert result.capability_profile.diagnostics["stability"].status in {"limited", "supported"}
    assert result.diagnostics["stability"]["status"] == "parameter_sweep"
    assert result.diagnostics["stability"]["metadata"]["dimensions"] == 2
    assert len(result.diagnostics["stability"]["heatmap"]) == 2


def test_ingest_parameter_sweep_bundle_rejects_inconsistent_params(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "bad_sweep"
    bundle_dir.mkdir(parents=True)
    manifest = {
        "parameter_names": ["lookback", "threshold"],
        "runs": [
            {"run_id": "run_1", "params": {"lookback": 10}, "summary": {"ev_net": 1.0}},
            {"run_id": "run_2", "params": {"lookback": 20, "threshold": 1.2}, "summary": {"ev_net": 0.8}},
        ],
    }
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    service = StrategyRobustnessLabService()
    with pytest.raises(IngestionError, match="parameter keys must exactly match"):
        service.ingest_parameter_sweep_bundle(bundle_dir)


def test_ingest_parameter_sweep_table_supports_secondary_mode(tmp_path: Path) -> None:
    table = tmp_path / "sweep_table.csv"
    pd.DataFrame(
        {
            "run_id": ["r1", "r1", "r2", "r2"],
            "lookback": [10, 10, 20, 20],
            "threshold": [1.0, 1.0, 1.0, 1.0],
            "entry_time": [
                "2024-01-01T00:00:00Z",
                "2024-01-01T01:00:00Z",
                "2024-01-02T00:00:00Z",
                "2024-01-02T01:00:00Z",
            ],
            "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT", "BTCUSDT"],
            "side": ["LONG", "SHORT", "LONG", "SHORT"],
            "entry_price": [100.0, 101.0, 102.0, 103.0],
            "exit_price": [101.0, 100.0, 101.0, 104.0],
            "quantity": [1.0, 1.0, 1.0, 1.0],
            "fees": [0.1, 0.1, 0.1, 0.1],
        }
    ).to_csv(table, index=False)

    service = StrategyRobustnessLabService()
    parsed = service.ingest_parameter_sweep_table(table, parameter_names=["lookback", "threshold"])

    assert parsed.parameter_sweep is not None
    assert len(parsed.parameter_sweep.runs) == 2
    assert parsed.parser_notes
