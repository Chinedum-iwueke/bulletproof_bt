"""Strategy definitions and registry helpers."""
from __future__ import annotations

from collections.abc import Callable
import inspect

from bt.strategy.base import Strategy


STRATEGY_REGISTRY: dict[str, type[Strategy]] = {}


def register_strategy(name: str) -> Callable[[type[Strategy]], type[Strategy]]:
    """Register a strategy class by name."""

    def decorator(cls: type[Strategy]) -> type[Strategy]:
        STRATEGY_REGISTRY[name] = cls
        return cls

    return decorator


def make_strategy(name: str, **kwargs: object) -> Strategy:
    """Instantiate a strategy from the global registry."""
    strategy_cls = STRATEGY_REGISTRY.get(name)
    if strategy_cls is None:
        available = ", ".join(sorted(STRATEGY_REGISTRY)) or "<none>"
        raise ValueError(f"Unknown strategy '{name}'. Available: {available}")
    signature = inspect.signature(strategy_cls)
    accepted = {
        key: value
        for key, value in kwargs.items()
        if key in signature.parameters
    }
    return strategy_cls(**accepted)


from bt.strategy.coinflip import CoinFlipStrategy  # noqa: E402
from bt.strategy.volfloor_donchian import VolFloorDonchianStrategy  # noqa: E402
from bt.strategy.volfloor_ema_pullback import VolFloorEmaPullbackStrategy  # noqa: E402
from bt.strategy.l1_h1_vol_floor_trend import L1H1VolFloorTrendStrategy  # noqa: E402

__all__ = [
    "STRATEGY_REGISTRY",
    "register_strategy",
    "make_strategy",
    "CoinFlipStrategy",
    "VolFloorDonchianStrategy",
    "VolFloorEmaPullbackStrategy",
    "L1H1VolFloorTrendStrategy",
]
