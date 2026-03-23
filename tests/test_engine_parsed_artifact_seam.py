from __future__ import annotations

from bt.saas.models import AnalysisRunConfig, NormalizedTradeRecord, ParsedArtifactInput
from bt.saas.service import StrategyRobustnessLabService


def _trade_only_artifact() -> ParsedArtifactInput:
    trades = [
        NormalizedTradeRecord(
            trade_id="t1",
            symbol="BTCUSDT",
            side="LONG",
            entry_time="2024-01-01T00:00:00Z",
            exit_time="2024-01-01T00:30:00Z",
            entry_price=100.0,
            exit_price=101.0,
            quantity=1.0,
            fees=0.1,
            mae=0.4,
            mfe=1.1,
        ),
        NormalizedTradeRecord(
            trade_id="t2",
            symbol="BTCUSDT",
            side="SHORT",
            entry_time="2024-01-01T01:00:00Z",
            exit_time="2024-01-01T01:30:00Z",
            entry_price=102.0,
            exit_price=101.0,
            quantity=1.0,
            fees=0.1,
            mae=0.6,
            mfe=1.3,
        ),
        NormalizedTradeRecord(
            trade_id="t3",
            symbol="BTCUSDT",
            side="LONG",
            entry_time="2024-01-01T02:00:00Z",
            exit_time="2024-01-01T02:30:00Z",
            entry_price=101.0,
            exit_price=99.5,
            quantity=1.0,
            fees=0.1,
            mae=1.5,
            mfe=0.8,
        ),
    ]
    return ParsedArtifactInput(
        artifact_kind="trade_csv",
        richness="trade_only",
        strategy_metadata={"strategy_name": "fixture_strategy"},
        trades=trades,
        parser_notes=["fixture parser note"],
    )


def _trade_only_artifact_without_excursion_or_exit() -> ParsedArtifactInput:
    trades = [
        NormalizedTradeRecord(
            trade_id="t1",
            symbol="BTCUSDT",
            side="LONG",
            entry_time="2024-01-01T00:00:00Z",
            entry_price=100.0,
            exit_price=101.0,
            quantity=1.0,
            fees=0.1,
        ),
        NormalizedTradeRecord(
            trade_id="t2",
            symbol="BTCUSDT",
            side="LONG",
            entry_time="2024-01-01T01:00:00Z",
            entry_price=101.0,
            exit_price=99.0,
            quantity=1.0,
            fees=0.1,
        ),
        NormalizedTradeRecord(
            trade_id="t3",
            symbol="BTCUSDT",
            side="SHORT",
            entry_time="2024-01-01T02:00:00Z",
            entry_price=103.0,
            exit_price=101.0,
            quantity=1.0,
            fees=0.1,
        ),
    ]
    return ParsedArtifactInput(
        artifact_kind="trade_csv",
        richness="trade_only",
        strategy_metadata={"strategy_name": "fixture_strategy"},
        trades=trades,
    )


def test_run_analysis_from_parsed_artifact_trade_only_degrades_honestly() -> None:
    service = StrategyRobustnessLabService()

    result = service.run_analysis_from_parsed_artifact(
        _trade_only_artifact(),
        config=AnalysisRunConfig(seed=5, simulations=100),
    )

    assert result.run_context.richness == "trade_only"
    assert result.capability_profile.diagnostics["overview"].status == "limited"
    assert result.capability_profile.diagnostics["stability"].status == "limited"
    assert result.capability_profile.diagnostics["regimes"].status == "limited"
    assert result.capability_profile.diagnostics["ruin"].status in {"supported", "limited"}

    assert "overview" in result.diagnostics
    assert "distribution" in result.diagnostics
    assert "monte_carlo" in result.diagnostics
    assert "execution" in result.diagnostics
    assert "report" in result.diagnostics
    assert result.diagnostics["distribution"]["available"] is True
    distribution = result.diagnostics["distribution"]
    assert distribution["summary_metrics"]["trade_count"] == 3
    for metric in [
        "expectancy",
        "win_rate",
        "median_return",
        "mean_return",
        "gross_profit",
        "gross_loss",
        "payoff_ratio",
        "profit_factor",
        "return_std",
    ]:
        assert metric in distribution["summary_metrics"]
    figure_ids = {figure["id"] for figure in distribution["figures"]}
    assert "trade_return_histogram" in figure_ids
    assert "win_loss_distribution" in figure_ids
    assert "mae_mfe_scatter" in figure_ids
    assert "duration_histogram" in figure_ids
    assert "shape_insights" in distribution["interpretation"]
    assert distribution["assumptions"]
    assert distribution["limitations"]
    assert distribution["recommendations"]
    assert distribution["metadata"]["available_subdiagnostics"]["histogram_available"] is True
    assert distribution["metadata"]["available_subdiagnostics"]["win_loss_available"] is True
    overview = result.diagnostics["overview"]
    assert overview["summary_metrics"]["posture"] in {
        "robust_under_current_assumptions",
        "promising_but_incomplete",
        "fragile_under_stress",
        "inconclusive_due_to_missing_context",
    }
    assert overview["summary_metrics"]["trade_count"] == 3
    assert "expectancy" in overview["summary_metrics"]
    assert "profit_factor" in overview["summary_metrics"]
    assert "payoff_ratio" in overview["summary_metrics"]
    assert "realized_max_drawdown_pct" in overview["summary_metrics"]
    assert "worst_mc_drawdown_pct" in overview["summary_metrics"]
    assert overview["figures"]
    assert overview["metadata"]["figure_provenance"]["equity_curve"] == "reconstructed_from_trades"
    assert isinstance(overview["interpretation"]["positives"], list)
    assert isinstance(overview["interpretation"]["cautions"], list)
    assert overview["verdict"]["verdict_reasons"]
    assert overview["assumptions"]
    assert overview["limitations"]
    assert overview["recommendations"]
    monte_carlo = result.diagnostics["monte_carlo"]
    assert monte_carlo["summary_metrics"]["worst_simulated_drawdown_pct"] <= 0.0
    assert "worst_drawdown" in monte_carlo["summary_metrics"]
    assert "p95_drawdown" in monte_carlo["summary_metrics"]
    assert "median_drawdown" in monte_carlo["summary_metrics"]
    assert "p_ruin" in monte_carlo["summary_metrics"]
    fan_chart = next(figure for figure in monte_carlo["figures"] if figure["type"] == "fan_chart")
    assert {"p5", "p25", "p50", "p75", "p95"}.issubset(fan_chart["bands"].keys())
    assert monte_carlo["drawdown_distribution"]["histogram_bins"]
    assert monte_carlo["interpretation"]["summary"]
    assert monte_carlo["assumptions"]
    assert monte_carlo["limitations"]
    assert monte_carlo["recommendations"]
    assert monte_carlo["metadata"]["method"] == "bootstrap_iid_trade_pnl"
    assert "limitations" in result.diagnostics["report"]
    assert "fixture parser note" in result.warnings


def test_distribution_trade_only_honestly_omits_unsupported_secondary_figures() -> None:
    service = StrategyRobustnessLabService()

    result = service.run_analysis_from_parsed_artifact(
        _trade_only_artifact_without_excursion_or_exit(),
        config=AnalysisRunConfig(seed=11, simulations=80),
    )

    distribution = result.diagnostics["distribution"]
    figure_ids = {figure["id"] for figure in distribution["figures"]}
    assert "trade_return_histogram" in figure_ids
    assert "win_loss_distribution" in figure_ids
    assert "mae_mfe_scatter" not in figure_ids
    assert "duration_histogram" not in figure_ids
    assert distribution["metadata"]["available_subdiagnostics"]["mae_mfe_available"] is False
    assert distribution["metadata"]["available_subdiagnostics"]["duration_available"] is False
    assert any("MAE/MFE" in limitation for limitation in distribution["limitations"])
    assert any("duration" in limitation.lower() for limitation in distribution["limitations"])


def test_run_analysis_from_parsed_artifact_is_deterministic_for_same_seed() -> None:
    service = StrategyRobustnessLabService()
    artifact = _trade_only_artifact()

    config = AnalysisRunConfig(seed=17, simulations=150)
    a = service.run_analysis_from_parsed_artifact(artifact, config=config)
    b = service.run_analysis_from_parsed_artifact(artifact, config=config)

    assert a.diagnostics["monte_carlo"]["drawdown_distribution_pct"] == b.diagnostics["monte_carlo"]["drawdown_distribution_pct"]
    assert a.diagnostics["ruin"]["probability_of_ruin"] == b.diagnostics["ruin"]["probability_of_ruin"]
    assert a.diagnostics["monte_carlo"]["figures"] == b.diagnostics["monte_carlo"]["figures"]


def test_run_analysis_from_parsed_artifact_respects_diagnostic_eligibility() -> None:
    service = StrategyRobustnessLabService()
    artifact = _trade_only_artifact()
    artifact = ParsedArtifactInput(
        artifact_kind=artifact.artifact_kind,
        richness=artifact.richness,
        strategy_metadata=artifact.strategy_metadata,
        trades=artifact.trades,
        parser_notes=artifact.parser_notes,
        diagnostic_eligibility={"regimes": False, "report": False},
    )

    result = service.run_analysis_from_parsed_artifact(artifact)

    assert result.diagnostics["regimes"]["status"] == "skipped"
    assert result.diagnostics["report"]["status"] == "skipped"
    assert result.capability_profile.diagnostics["regimes"].status == "unavailable"
    assert result.capability_profile.diagnostics["report"].status == "unavailable"
