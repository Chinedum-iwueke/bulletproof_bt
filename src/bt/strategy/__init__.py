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
from bt.strategy.l1_h2_compression_mean_reversion import L1H2CompressionMeanReversionStrategy  # noqa: E402
from bt.strategy.l1_h3_har_rv_gate_trend import L1H3HarRVGateTrendStrategy  # noqa: E402
from bt.strategy.l1_h3b_har_rv_gate_mean_reversion import L1H3BHarRVGateMeanReversionStrategy  # noqa: E402
from bt.strategy.l1_h3c_har_regime_switch import L1H3CHarRegimeSwitchStrategy  # noqa: E402
from bt.strategy.l1_h4a_liquidity_gate_mean_reversion import L1H4ALiquidityGateMeanReversionStrategy  # noqa: E402
from bt.strategy.l1_h4b_liquidity_gate_size_adjusted_mean_reversion import L1H4BLiquidityGateSizeAdjustedMeanReversionStrategy  # noqa: E402
from bt.strategy.l1_h1b_salvage import L1H1BSalvageStrategy  # noqa: E402
from bt.strategy.l1_h2b_confirmed_fade import L1H2BConfirmedFadeStrategy  # noqa: E402
from bt.strategy.l1_h5a_vol_managed_trend import L1H5AVolManagedTrendStrategy  # noqa: E402
from bt.strategy.l1_h5b_vol_managed_har_trend import L1H5BVolManagedHarTrendStrategy  # noqa: E402
from bt.strategy.l1_h6a_vov_gate_mean_reversion import L1H6AVovGateMeanReversionStrategy  # noqa: E402
from bt.strategy.l1_h7_squeeze_expansion_pullback import L1H7SqueezeExpansionPullbackStrategy  # noqa: E402
from bt.strategy.l1_h8_trend_continuation_pullback import L1H8TrendContinuationPullbackStrategy  # noqa: E402

__all__ = [
    "STRATEGY_REGISTRY",
    "register_strategy",
    "make_strategy",
    "CoinFlipStrategy",
    "VolFloorDonchianStrategy",
    "VolFloorEmaPullbackStrategy",
    "L1H1VolFloorTrendStrategy",
    "L1H2CompressionMeanReversionStrategy",
    "L1H3HarRVGateTrendStrategy",
    "L1H3BHarRVGateMeanReversionStrategy",
    "L1H3CHarRegimeSwitchStrategy",
    "L1H4ALiquidityGateMeanReversionStrategy",
    "L1H4BLiquidityGateSizeAdjustedMeanReversionStrategy",
    "L1H1BSalvageStrategy",
    "L1H2BConfirmedFadeStrategy",
    "L1H5AVolManagedTrendStrategy",
    "L1H5BVolManagedHarTrendStrategy",
    "L1H6AVovGateMeanReversionStrategy",
    "L1H7SqueezeExpansionPullbackStrategy",
    "L1H8TrendContinuationPullbackStrategy",
]
