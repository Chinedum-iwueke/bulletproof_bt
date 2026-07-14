"""Portfolio config parsing."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from bt.config import load_yaml
from bt.portfolio_engine.models import (
    PortfolioAllocationPolicy,
    PortfolioConfig,
    StrategyAllocationConfig,
)


def _path_or_none(value: Any, *, base_dir: Path) -> Path | None:
    if value is None:
        return None
    path = Path(str(value))
    return path if path.is_absolute() else (base_dir / path).resolve()


def load_portfolio_config(path: str | Path) -> PortfolioConfig:
    config_path = Path(path)
    payload = load_yaml(config_path)
    base_dir = config_path.parent

    policy_payload = payload.get("allocation_policy") if isinstance(payload.get("allocation_policy"), dict) else {}
    policy = PortfolioAllocationPolicy(
        type=str(policy_payload.get("type", "equal_weight")),
        rebalance_frequency=str(policy_payload.get("rebalance_frequency", "daily")),
        max_strategy_weight=float(policy_payload.get("max_strategy_weight", 1.0)),
        max_symbol_exposure=float(policy_payload.get("max_symbol_exposure", 1.0)),
        max_total_gross_exposure=float(policy_payload.get("max_total_gross_exposure", 1.0)),
        max_total_net_exposure=float(policy_payload.get("max_total_net_exposure", 1.0)),
        max_positions_per_strategy=policy_payload.get("max_positions_per_strategy"),
        max_positions_portfolio=policy_payload.get("max_positions_portfolio"),
        max_daily_loss_pct=policy_payload.get("max_daily_loss_pct"),
        max_strategy_daily_loss_pct=policy_payload.get("max_strategy_daily_loss_pct"),
        max_drawdown_pct=policy_payload.get("max_drawdown_pct"),
        conflict_policy=str(policy_payload.get("conflict_policy", "block_conflict")),
        target_volatility=policy_payload.get("target_volatility"),
    )

    strategies_raw = payload.get("strategies")
    if not isinstance(strategies_raw, list) or not strategies_raw:
        raise ValueError("portfolio config requires a non-empty strategies list")

    strategies: list[StrategyAllocationConfig] = []
    for item in strategies_raw:
        if not isinstance(item, dict):
            raise ValueError("each portfolio strategy entry must be a mapping")
        strategies.append(
            StrategyAllocationConfig(
                strategy_id=str(item["strategy_id"]),
                hypothesis_id=str(item.get("hypothesis_id", item["strategy_id"])),
                enabled=bool(item.get("enabled", True)),
                weight=None if item.get("weight") is None else float(item.get("weight")),
                capital=None if item.get("capital") is None else float(item.get("capital")),
                config_path=_path_or_none(item.get("config_path"), base_dir=base_dir),
                data_path=_path_or_none(item.get("data_path"), base_dir=base_dir),
                overrides=item.get("overrides") if isinstance(item.get("overrides"), dict) else {},
                expected_volatility=(
                    None if item.get("expected_volatility") is None else float(item.get("expected_volatility"))
                ),
            )
        )

    return PortfolioConfig(
        portfolio_id=str(payload["portfolio_id"]),
        starting_equity=float(payload.get("starting_equity", payload.get("initial_cash", 100000.0))),
        base_currency=str(payload.get("base_currency", "USDT")),
        allocation_policy=policy,
        strategies=tuple(strategies),
        data_path=_path_or_none(payload.get("data_path"), base_dir=base_dir),
        output_dir=_path_or_none(payload.get("output_dir", "outputs/portfolios"), base_dir=base_dir) or Path("outputs/portfolios"),
        deployment=payload.get("deployment") if isinstance(payload.get("deployment"), dict) else {},
    )
