"""First-class multi-strategy portfolio allocation surface."""
from __future__ import annotations

from bt.portfolio_engine.candidate_service import (
    PortfolioCandidateError,
    PortfolioCandidatePolicy,
    evaluate_portfolio_candidates,
    finalize_candidate,
    validate_portfolio_candidate_dossier,
)
from bt.portfolio_engine.config import load_portfolio_config
from bt.portfolio_engine.deployment import (
    PortfolioEventLogger,
    PortfolioExecutionAdapter,
    PortfolioLiveRunner,
    PortfolioStateStore,
    run_portfolio_demo,
    run_portfolio_live,
)
from bt.portfolio_engine.models import (
    HypothesisId,
    PortfolioAllocationPolicy,
    PortfolioConfig,
    PortfolioEquityCurve,
    PortfolioId,
    PortfolioOrderIntent,
    PortfolioPosition,
    PortfolioRunState,
    PortfolioSignal,
    PortfolioTrade,
    StrategyAllocationConfig,
    StrategyId,
    StrategyRunState,
)
from bt.portfolio_engine.runner import run_portfolio_backtest

__all__ = [
    "HypothesisId",
    "PortfolioAllocationPolicy",
    "PortfolioCandidateError",
    "PortfolioCandidatePolicy",
    "PortfolioConfig",
    "PortfolioEquityCurve",
    "PortfolioEventLogger",
    "PortfolioExecutionAdapter",
    "PortfolioId",
    "PortfolioLiveRunner",
    "PortfolioOrderIntent",
    "PortfolioPosition",
    "PortfolioRunState",
    "PortfolioSignal",
    "PortfolioStateStore",
    "PortfolioTrade",
    "StrategyAllocationConfig",
    "StrategyId",
    "StrategyRunState",
    "evaluate_portfolio_candidates",
    "finalize_candidate",
    "load_portfolio_config",
    "run_portfolio_backtest",
    "run_portfolio_demo",
    "run_portfolio_live",
    "validate_portfolio_candidate_dossier",
]
