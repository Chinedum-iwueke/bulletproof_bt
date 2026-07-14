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
from bt.strategy.l1_h9_momentum_breakout import L1H9MomentumBreakoutStrategy  # noqa: E402
from bt.strategy.research_graph_v1 import ResearchGraphV1Strategy  # noqa: E402
from bt.strategy.l1_h10a_mean_reversion_small_tp import L1H10AMeanReversionSmallTPStrategy  # noqa: E402
from bt.strategy.l1_h10b_breakout_scalping import L1H10BBreakoutScalpingStrategy  # noqa: E402
from bt.strategy.l1_h11_quality_filtered_continuation import L1H11QualityFilteredContinuationStrategy  # noqa: E402
from bt.strategy.l7_h1_csi_gated_displacement_trend import L7H1CSIGatedDisplacementTrendStrategy  # noqa: E402
from bt.strategy.l2_h1_htf_trend_filter_pullback import L2H1HTFTrendFilterPullbackStrategy  # noqa: E402
from bt.strategy.l2_h3_reference_price_reversion import L2H3ReferencePriceReversionStrategy  # noqa: E402
from bt.strategy.l2_h4_prior_day_extreme_traps import L2H4PriorDayExtremeTrapsStrategy  # noqa: E402
from bt.strategy.l2_h5_htf_trend_funding_stress import L2H5HTFTrendFundingStressStrategy  # noqa: E402

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
    "L1H9MomentumBreakoutStrategy",
    "L1H10AMeanReversionSmallTPStrategy",
    "L1H10BBreakoutScalpingStrategy",
    "L1H11QualityFilteredContinuationStrategy",
    "L7H1CSIGatedDisplacementTrendStrategy",
    "L2H1HTFTrendFilterPullbackStrategy",
    "L2H3ReferencePriceReversionStrategy",
    "L2H4PriorDayExtremeTrapsStrategy",
    "L2H5HTFTrendFundingStressStrategy",
]
