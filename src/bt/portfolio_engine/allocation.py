"""Deterministic allocation policies for strategy sleeves."""
from __future__ import annotations

from bt.portfolio_engine.models import PortfolioConfig, StrategyId


def _enabled_weights(config: PortfolioConfig) -> dict[StrategyId, float]:
    enabled = config.enabled_strategies
    if not enabled:
        raise ValueError("PortfolioConfig must include at least one enabled strategy")

    policy = config.allocation_policy
    if policy.type == "equal_weight":
        return {strategy.strategy_id: 1.0 / len(enabled) for strategy in enabled}

    if policy.type == "fixed_weight":
        weights = {strategy.strategy_id: float(strategy.weight or 0.0) for strategy in enabled}
        total = sum(weights.values())
        if total <= 0:
            raise ValueError("fixed_weight policy requires positive strategy weights")
        return {key: value / total for key, value in weights.items()}

    if policy.type == "manual_capital_buckets":
        capitals = {strategy.strategy_id: float(strategy.capital or 0.0) for strategy in enabled}
        total = sum(capitals.values())
        if total <= 0:
            raise ValueError("manual_capital_buckets policy requires positive strategy capital values")
        return {key: value / total for key, value in capitals.items()}

    if policy.type == "risk_parity_simple":
        inv_vol: dict[StrategyId, float] = {}
        for strategy in enabled:
            vol = float(strategy.expected_volatility or strategy.overrides.get("expected_volatility", 1.0) or 1.0)
            inv_vol[strategy.strategy_id] = 1.0 / max(vol, 1e-12)
        total = sum(inv_vol.values())
        return {key: value / total for key, value in inv_vol.items()}

    if policy.type == "vol_target":
        target = float(policy.target_volatility or 1.0)
        raw: dict[StrategyId, float] = {}
        for strategy in enabled:
            vol = float(strategy.expected_volatility or strategy.overrides.get("expected_volatility", target) or target)
            raw[strategy.strategy_id] = min(target / max(vol, 1e-12), 1.0)
        total = sum(raw.values())
        if total <= 0:
            raise ValueError("vol_target policy produced zero allocation")
        return {key: value / total for key, value in raw.items()}

    raise ValueError(f"Unsupported allocation policy: {policy.type}")


def resolve_strategy_weights(config: PortfolioConfig) -> dict[StrategyId, float]:
    weights = _enabled_weights(config)
    cap = float(config.allocation_policy.max_strategy_weight)
    if cap <= 0:
        raise ValueError("allocation_policy.max_strategy_weight must be > 0")
    capped = {key: min(value, cap) for key, value in weights.items()}
    total = sum(capped.values())
    if total <= 0:
        raise ValueError("strategy weights sum to zero after max_strategy_weight cap")
    return {key: value / total for key, value in capped.items()}


def resolve_capital_buckets(config: PortfolioConfig) -> dict[StrategyId, float]:
    weights = resolve_strategy_weights(config)
    return {key: float(config.starting_equity) * value for key, value in weights.items()}

