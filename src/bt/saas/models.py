"""Typed contracts for ingestion and engine-facing analysis seams."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


DiagnosticName = Literal[
    "overview",
    "distribution",
    "monte_carlo",
    "stability",
    "execution",
    "regimes",
    "ruin",
    "report",
]

CapabilityStatus = Literal["supported", "limited", "unavailable"]
ArtifactKind = Literal["trade_csv", "artifact_bundle", "parameter_sweep"]
ArtifactRichness = Literal[
    "trade_only",
    "trade_plus_metadata",
    "trade_plus_context",
    "research_complete",
]


@dataclass(frozen=True)
class ScorePayload:
    overall: float
    sub_scores: dict[str, float]
    methodology: dict[str, Any]


@dataclass(frozen=True)
class IngestedRun:
    source: str
    trades: Any
    equity: Any
    performance: dict[str, Any]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class NormalizedTradeRecord:
    symbol: str
    side: str
    entry_time: str
    exit_time: str | None = None
    entry_price: float | None = None
    exit_price: float | None = None
    quantity: float | None = None
    fees: float | None = None
    pnl: float | None = None
    gross_pnl: float | None = None
    pnl_pct: float | None = None
    slippage: float | None = None
    risk_amount: float | None = None
    stop_distance: float | None = None
    r_multiple_net: float | None = None
    r_multiple_gross: float | None = None
    mae: float | None = None
    mfe: float | None = None
    duration_seconds: float | None = None
    strategy_name: str | None = None
    timeframe: str | None = None
    market: str | None = None
    exchange: str | None = None
    trade_id: str | None = None

@dataclass(frozen=True)
class ParameterSweepRunInput:
    run_id: str
    params: dict[str, int | float | str | bool]
    trades: list[NormalizedTradeRecord]
    summary: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None


@dataclass(frozen=True)
class ParameterSweepInput:
    parameter_names: list[str]
    runs: list[ParameterSweepRunInput]
    assumptions: dict[str, Any] | None = None
    execution_context: dict[str, Any] | None = None

@dataclass(frozen=True)
class ParsedArtifactInput:
    artifact_kind: ArtifactKind
    richness: ArtifactRichness
    trades: list[NormalizedTradeRecord]
    strategy_metadata: dict[str, Any] = field(default_factory=dict)
    equity_curve: list[dict[str, Any]] | None = None
    assumptions: dict[str, Any] | None = None
    params: dict[str, Any] | None = None
    parameter_sweep: ParameterSweepInput | None = None
    ohlcv: list[dict[str, Any]] | None = None
    ohlcv_present: bool = False
    benchmark_present: bool = False
    parser_notes: list[str] = field(default_factory=list)
    diagnostic_eligibility: dict[str, bool] = field(default_factory=dict)


@dataclass(frozen=True)
class AnalysisRunConfig:
    seed: int = 42
    simulations: int = 1_000
    ruin_drawdown_levels: tuple[float, ...] = (0.30, 0.50)
    account_size: float | None = None
    risk_per_trade_pct: float | None = None
    benchmark: dict[str, Any] | None = None


@dataclass(frozen=True)
class DiagnosticCapability:
    status: CapabilityStatus
    reason: str
    required_inputs: list[str]
    optional_enrichments: list[str]


@dataclass(frozen=True)
class AnalysisCapabilityProfile:
    diagnostics: dict[DiagnosticName, DiagnosticCapability]
    artifact_capabilities: dict[str, bool] = field(default_factory=dict)


@dataclass(frozen=True)
class EngineRunContext:
    artifact_kind: ArtifactKind
    richness: ArtifactRichness
    trade_count: int
    ohlcv_present: bool
    benchmark_present: bool
    has_assumptions: bool
    has_params: bool
    has_parameter_sweep: bool


@dataclass(frozen=True)
class EngineAnalysisResult:
    run_context: EngineRunContext
    capability_profile: AnalysisCapabilityProfile
    warnings: list[str]
    diagnostics: dict[DiagnosticName, dict[str, Any]]
    raw_payload: dict[str, Any]
