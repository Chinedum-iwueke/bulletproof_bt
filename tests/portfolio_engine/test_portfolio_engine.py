from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bt.portfolio_engine.allocation import resolve_capital_buckets, resolve_strategy_weights
from bt.portfolio_engine.config import load_portfolio_config
from bt.portfolio_engine.deployment import run_portfolio_demo, run_portfolio_live
from bt.portfolio_engine.models import (
    PortfolioAllocationPolicy,
    PortfolioConfig,
    PortfolioOrderIntent,
    PortfolioRunState,
    PortfolioSignal,
    StrategyAllocationConfig,
    StrategyRunState,
)
from bt.portfolio_engine.risk import PortfolioRiskCoordinator
from bt.portfolio_engine.runner import run_portfolio_backtest


ROOT = Path(__file__).resolve().parents[2]


def _portfolio(tmp_path: Path, *, policy: PortfolioAllocationPolicy | None = None) -> PortfolioConfig:
    return PortfolioConfig(
        portfolio_id="test_portfolio",
        starting_equity=100_000.0,
        data_path=ROOT / "data/curated/sample.csv",
        output_dir=tmp_path,
        allocation_policy=policy or PortfolioAllocationPolicy(type="equal_weight"),
        strategies=(
            StrategyAllocationConfig(
                strategy_id="s1",
                hypothesis_id="h1",
                weight=0.75,
                config_path=ROOT / "configs/engine.yaml",
                overrides={"strategy": {"name": "coinflip", "p_trade": 0.0}},
            ),
            StrategyAllocationConfig(
                strategy_id="s2",
                hypothesis_id="h2",
                weight=0.25,
                config_path=ROOT / "configs/engine.yaml",
                overrides={"strategy": {"name": "coinflip", "p_trade": 0.0}},
            ),
        ),
    )


def test_equal_weight_portfolio_allocation(tmp_path: Path) -> None:
    weights = resolve_strategy_weights(_portfolio(tmp_path))
    assert weights == {"s1": pytest.approx(0.5), "s2": pytest.approx(0.5)}
    assert resolve_capital_buckets(_portfolio(tmp_path))["s1"] == pytest.approx(50_000)


def test_fixed_weight_portfolio_allocation(tmp_path: Path) -> None:
    portfolio = _portfolio(tmp_path, policy=PortfolioAllocationPolicy(type="fixed_weight", max_strategy_weight=1.0))
    weights = resolve_strategy_weights(portfolio)
    assert weights["s1"] == pytest.approx(0.75)
    assert weights["s2"] == pytest.approx(0.25)


def test_per_strategy_capital_isolation(tmp_path: Path) -> None:
    buckets = resolve_capital_buckets(_portfolio(tmp_path))
    assert sum(buckets.values()) == pytest.approx(100_000)
    assert buckets["s1"] == buckets["s2"]


def test_same_symbol_same_direction_allowed_within_limits(tmp_path: Path) -> None:
    portfolio = _portfolio(tmp_path, policy=PortfolioAllocationPolicy(type="equal_weight", max_symbol_exposure=1.0))
    coordinator = PortfolioRiskCoordinator(portfolio)
    signals = [
        PortfolioSignal("p", "s1", "h1", pd.Timestamp("2026-01-01", tz="UTC"), "BTC", "BUY"),
        PortfolioSignal("p", "s2", "h2", pd.Timestamp("2026-01-01", tz="UTC"), "BTC", "BUY"),
    ]
    assert len(coordinator.resolve_signal_conflicts(signals)) == 2


def test_same_symbol_opposite_direction_block_conflict(tmp_path: Path) -> None:
    portfolio = _portfolio(tmp_path, policy=PortfolioAllocationPolicy(type="equal_weight", conflict_policy="block_conflict"))
    coordinator = PortfolioRiskCoordinator(portfolio)
    signals = [
        PortfolioSignal("p", "s1", "h1", pd.Timestamp("2026-01-01", tz="UTC"), "BTC", "BUY"),
        PortfolioSignal("p", "s2", "h2", pd.Timestamp("2026-01-01", tz="UTC"), "BTC", "SELL"),
    ]
    assert coordinator.resolve_signal_conflicts(signals) == []
    assert coordinator.conflict_events[-1]["event"] == "conflict_blocked"


def test_portfolio_max_exposure_rejection(tmp_path: Path) -> None:
    portfolio = _portfolio(tmp_path, policy=PortfolioAllocationPolicy(type="equal_weight", max_total_gross_exposure=0.1))
    coordinator = PortfolioRiskCoordinator(portfolio)
    state = PortfolioRunState("p", 100_000, 100_000)
    state.strategy_states["s1"] = StrategyRunState("s1", "h1", 50_000, 50_000)
    order = PortfolioOrderIntent("p", "s1", "h1", pd.Timestamp("2026-01-01", tz="UTC"), "BTC", "BUY", 1, 20_000, True)
    approved = coordinator.approve_order(order, state)
    assert approved.approved is False
    assert approved.risk_rejection_reason == "max_total_gross_exposure"


def test_per_strategy_loss_kill_switch(tmp_path: Path) -> None:
    portfolio = _portfolio(tmp_path, policy=PortfolioAllocationPolicy(type="equal_weight", max_strategy_daily_loss_pct=0.01))
    coordinator = PortfolioRiskCoordinator(portfolio)
    state = PortfolioRunState("p", 100_000, 100_000)
    state.strategy_states["s1"] = StrategyRunState("s1", "h1", 50_000, 50_000, realized_pnl=-600)
    assert coordinator.check_strategy_state(state, "s1") is False
    assert state.strategy_states["s1"].disabled is True


def test_portfolio_drawdown_kill_switch(tmp_path: Path) -> None:
    portfolio = _portfolio(tmp_path, policy=PortfolioAllocationPolicy(type="equal_weight", max_drawdown_pct=0.05))
    coordinator = PortfolioRiskCoordinator(portfolio)
    state = PortfolioRunState("p", 100_000, 94_000, high_watermark=100_000)
    assert coordinator.check_state(state) is False
    assert state.kill_switch_triggered is True


def test_portfolio_backtest_writes_tagged_artifacts(tmp_path: Path) -> None:
    run_dir = run_portfolio_backtest(_portfolio(tmp_path))
    assert (run_dir / "portfolio_summary.json").exists()
    assert (run_dir / "strategy_contributions.csv").exists()
    assert (run_dir / "portfolio_equity_curve.csv").exists()
    orders = pd.read_csv(run_dir / "portfolio_orders.csv")
    assert {"portfolio_id", "strategy_id", "hypothesis_id"}.issubset(orders.columns)


def test_demo_runner_processes_multiple_strategies(tmp_path: Path) -> None:
    run_dir = run_portfolio_demo(_portfolio(tmp_path))
    assert (run_dir / "deployment_events.jsonl").exists()
    assert (run_dir / "portfolio_state.json").exists()


def test_live_mode_refuses_without_explicit_confirmation(tmp_path: Path) -> None:
    portfolio = PortfolioConfig(
        portfolio_id="live_refusal",
        starting_equity=100_000,
        output_dir=tmp_path,
        deployment={"mode": "live", "confirm_live_trading": False},
        strategies=(StrategyAllocationConfig("s1", "h1", config_path=ROOT / "configs/engine.yaml"),),
    )
    with pytest.raises(ValueError, match="confirm_live_trading"):
        run_portfolio_live(portfolio)


def test_portfolio_config_loader_example() -> None:
    config = load_portfolio_config(ROOT / "configs/portfolios/equal_weight_demo.yaml")
    assert config.portfolio_id == "equal_weight_demo"
    assert len(config.enabled_strategies) == 2

