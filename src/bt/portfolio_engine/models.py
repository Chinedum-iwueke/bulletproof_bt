"""Domain models for multi-strategy portfolio allocation."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import pandas as pd


StrategyId = str
HypothesisId = str
PortfolioId = str

AllocationPolicyType = Literal[
    "fixed_weight",
    "equal_weight",
    "risk_parity_simple",
    "vol_target",
    "manual_capital_buckets",
]

ConflictPolicy = Literal[
    "allow_hedged",
    "net_exposure",
    "highest_confidence",
    "highest_expected_value",
    "block_conflict",
]


@dataclass(frozen=True)
class PortfolioAllocationPolicy:
    type: AllocationPolicyType = "equal_weight"
    rebalance_frequency: str = "daily"
    max_strategy_weight: float = 1.0
    max_symbol_exposure: float = 1.0
    max_total_gross_exposure: float = 1.0
    max_total_net_exposure: float = 1.0
    max_positions_per_strategy: int | None = None
    max_positions_portfolio: int | None = None
    max_daily_loss_pct: float | None = None
    max_strategy_daily_loss_pct: float | None = None
    max_drawdown_pct: float | None = None
    conflict_policy: ConflictPolicy = "block_conflict"
    target_volatility: float | None = None


@dataclass(frozen=True)
class StrategyAllocationConfig:
    strategy_id: StrategyId
    hypothesis_id: HypothesisId
    enabled: bool = True
    weight: float | None = None
    capital: float | None = None
    config_path: Path | None = None
    data_path: Path | None = None
    overrides: dict[str, Any] = field(default_factory=dict)
    expected_volatility: float | None = None


@dataclass(frozen=True)
class PortfolioConfig:
    portfolio_id: PortfolioId
    starting_equity: float
    base_currency: str = "USDT"
    allocation_policy: PortfolioAllocationPolicy = field(default_factory=PortfolioAllocationPolicy)
    strategies: tuple[StrategyAllocationConfig, ...] = ()
    data_path: Path | None = None
    output_dir: Path = Path("outputs/portfolios")
    deployment: dict[str, Any] = field(default_factory=dict)

    @property
    def enabled_strategies(self) -> tuple[StrategyAllocationConfig, ...]:
        return tuple(strategy for strategy in self.strategies if strategy.enabled)


@dataclass
class StrategyRunState:
    strategy_id: StrategyId
    hypothesis_id: HypothesisId
    allocated_capital: float
    equity: float
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    gross_exposure: float = 0.0
    net_exposure: float = 0.0
    active_positions: int = 0
    disabled: bool = False
    disable_reason: str | None = None


@dataclass
class PortfolioRunState:
    portfolio_id: PortfolioId
    starting_equity: float
    equity: float
    strategy_states: dict[StrategyId, StrategyRunState] = field(default_factory=dict)
    gross_exposure: float = 0.0
    net_exposure: float = 0.0
    active_positions: int = 0
    high_watermark: float | None = None
    kill_switch_triggered: bool = False
    kill_switch_reason: str | None = None


@dataclass(frozen=True)
class PortfolioSignal:
    portfolio_id: PortfolioId
    strategy_id: StrategyId
    hypothesis_id: HypothesisId
    ts: pd.Timestamp
    symbol: str
    side: str
    confidence: float = 1.0
    expected_value: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PortfolioOrderIntent:
    portfolio_id: PortfolioId
    strategy_id: StrategyId
    hypothesis_id: HypothesisId
    ts: pd.Timestamp
    symbol: str
    side: str
    qty: float
    notional: float
    approved: bool
    risk_rejection_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PortfolioPosition:
    portfolio_id: PortfolioId
    strategy_id: StrategyId
    hypothesis_id: HypothesisId
    symbol: str
    side: str
    qty: float
    notional: float
    avg_entry_price: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PortfolioTrade:
    portfolio_id: PortfolioId
    strategy_id: StrategyId
    hypothesis_id: HypothesisId
    symbol: str
    side: str
    qty: float
    notional: float
    entry_time: str | None
    exit_time: str | None
    entry_price: float | None
    exit_price: float | None
    pnl: float
    fees: float = 0.0
    slippage: float = 0.0
    risk_rejection_reason: str | None = None


@dataclass(frozen=True)
class PortfolioEquityCurve:
    portfolio_id: PortfolioId
    points: tuple[dict[str, Any], ...]

