"""Demo/paper/live-readiness runners for portfolio configs."""
from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Protocol

from bt.logging.formatting import write_json_deterministic
from bt.portfolio_engine.allocation import resolve_capital_buckets
from bt.portfolio_engine.config import load_portfolio_config
from bt.portfolio_engine.models import PortfolioConfig, PortfolioRunState, StrategyRunState
from bt.portfolio_engine.risk import PortfolioRiskCoordinator


class PortfolioExecutionAdapter(Protocol):
    def submit(self, intent: dict[str, Any]) -> dict[str, Any]:
        ...


class PortfolioStateStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))

    def save(self, payload: dict[str, Any]) -> None:
        write_json_deterministic(self.path, payload)


class PortfolioEventLogger:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path

    def event(self, event: str, **payload: Any) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"event": event, **payload}, sort_keys=True) + "\n")


def _initial_state(config: PortfolioConfig) -> PortfolioRunState:
    buckets = resolve_capital_buckets(config)
    state = PortfolioRunState(
        portfolio_id=config.portfolio_id,
        starting_equity=config.starting_equity,
        equity=config.starting_equity,
        high_watermark=config.starting_equity,
    )
    for strategy in config.enabled_strategies:
        capital = buckets[strategy.strategy_id]
        state.strategy_states[strategy.strategy_id] = StrategyRunState(
            strategy_id=strategy.strategy_id,
            hypothesis_id=strategy.hypothesis_id,
            allocated_capital=capital,
            equity=capital,
        )
    return state


class PortfolioLiveRunner:
    def __init__(self, config: PortfolioConfig, *, run_dir: Path | None = None) -> None:
        self.config = config
        self.run_dir = run_dir or Path(config.output_dir) / f"{config.portfolio_id}_deployment"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.state_store = PortfolioStateStore(self.run_dir / "portfolio_state.json")
        self.events = PortfolioEventLogger(self.run_dir / "deployment_events.jsonl")
        self.risk = PortfolioRiskCoordinator(config)

    def _mode(self) -> str:
        deployment = self.config.deployment if isinstance(self.config.deployment, dict) else {}
        return str(deployment.get("mode", "dry_run"))

    def validate_live_safety(self) -> None:
        deployment = self.config.deployment if isinstance(self.config.deployment, dict) else {}
        if self._mode() == "live" and deployment.get("confirm_live_trading") is not True:
            raise ValueError(
                "Refusing live portfolio execution: deployment.confirm_live_trading must be true when deployment.mode is live"
            )

    def run_once(self) -> Path:
        self.validate_live_safety()
        mode = self._mode()
        loaded = self.state_store.load()
        if loaded:
            self.events.event("portfolio_state_loaded", portfolio_id=self.config.portfolio_id, mode=mode)
        state = _initial_state(self.config)
        self.risk.check_state(state)
        self.state_store.save(
            {
                "portfolio_id": self.config.portfolio_id,
                "mode": mode,
                "state": asdict(state),
                "risk_events": self.risk.risk_events,
            }
        )
        self.events.event("portfolio_runner_started", portfolio_id=self.config.portfolio_id, mode=mode)
        self.events.event("portfolio_runner_idle", portfolio_id=self.config.portfolio_id, mode=mode)
        return self.run_dir


def run_portfolio_demo(config: PortfolioConfig | str | Path) -> Path:
    portfolio = load_portfolio_config(config) if not isinstance(config, PortfolioConfig) else config
    deployment = dict(portfolio.deployment)
    deployment["mode"] = "paper" if deployment.get("mode") is None else deployment.get("mode")
    if deployment["mode"] == "live":
        deployment["mode"] = "paper"
    portfolio = PortfolioConfig(
        portfolio_id=portfolio.portfolio_id,
        starting_equity=portfolio.starting_equity,
        base_currency=portfolio.base_currency,
        allocation_policy=portfolio.allocation_policy,
        strategies=portfolio.strategies,
        data_path=portfolio.data_path,
        output_dir=portfolio.output_dir,
        deployment=deployment,
    )
    return PortfolioLiveRunner(portfolio, run_dir=Path(portfolio.output_dir) / f"{portfolio.portfolio_id}_demo").run_once()


def run_portfolio_live(config: PortfolioConfig | str | Path) -> Path:
    portfolio = load_portfolio_config(config) if not isinstance(config, PortfolioConfig) else config
    return PortfolioLiveRunner(portfolio, run_dir=Path(portfolio.output_dir) / f"{portfolio.portfolio_id}_live").run_once()

