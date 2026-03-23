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
    assert result.diagnostics["distribution"]["summary_metrics"]["trade_count"] == 3
    assert result.diagnostics["distribution"]["figures"]
    assert result.diagnostics["overview"]["summary_metrics"]["posture"] in {"robust_candidate", "caution"}
    assert result.diagnostics["monte_carlo"]["summary_metrics"]["worst_simulated_drawdown_pct"] <= 0.0
    assert "limitations" in result.diagnostics["report"]
    assert "fixture parser note" in result.warnings


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
