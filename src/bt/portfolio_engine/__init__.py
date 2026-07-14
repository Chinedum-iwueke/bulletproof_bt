"""First-class multi-strategy portfolio allocation surface."""
from __future__ import annotations

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
    "PortfolioConfig",
    "PortfolioEquityCurve",
    "PortfolioId",
    "PortfolioOrderIntent",
    "PortfolioPosition",
    "PortfolioRunState",
    "PortfolioSignal",
    "PortfolioTrade",
    "StrategyAllocationConfig",
    "StrategyId",
    "StrategyRunState",
    "PortfolioEventLogger",
    "PortfolioExecutionAdapter",
    "PortfolioLiveRunner",
    "PortfolioStateStore",
    "load_portfolio_config",
    "run_portfolio_backtest",
    "run_portfolio_demo",
    "run_portfolio_live",
]
