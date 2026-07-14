"""Registry of fast-path kernels that are safe for strategy families.

The registry is deliberately conservative. A strategy may be listed here only
when the kernel preserves no-lookahead/event semantics and the classic engine
continues to own risk, execution, accounting, and artifact writing.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class FamilyKernel:
    strategy_name: str
    kernel_name: str
    mode: str
    description: str
    numerical_signal_kernel: bool = False


@dataclass(frozen=True)
class StrategyFeatureRequest:
    feature_name: str
    required: bool = True
    params: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyAdapterSpec:
    """Fast-path adapter contract for a strategy family.

    The adapter declares data/feature needs and scheduler eligibility. It does
    not own fills, PnL, risk, or artifacts; those stay in the classic engine.
    """

    strategy_name: str
    kernel: FamilyKernel
    feature_requests: tuple[StrategyFeatureRequest, ...] = ()
    candidate_scheduler: str = "generic_sparse_events"
    parity_required: bool = True
    truth_layer: str = "classic_engine"

    @property
    def kernel_name(self) -> str:
        return self.kernel.kernel_name

    def to_json(self) -> dict[str, object]:
        return {
            "strategy_name": self.strategy_name,
            "kernel_name": self.kernel.kernel_name,
            "mode": self.kernel.mode,
            "feature_requests": [
                {"feature_name": req.feature_name, "required": req.required, "params": dict(req.params)}
                for req in self.feature_requests
            ],
            "candidate_scheduler": self.candidate_scheduler,
            "parity_required": self.parity_required,
            "truth_layer": self.truth_layer,
        }


HTF_EVENT_SCHEDULE_KERNEL = "htf_event_schedule"


_HTF_EVENT_STRATEGIES = {
    "l1_h1_vol_floor_trend",
    "l1_h1b_salvage",
    "volfloor_ema_pullback",
    "l1_h2_compression_mean_reversion",
    "l1_h3_har_rv_gate_trend",
    "l1_h3b_har_rv_gate_mean_reversion",
    "l1_h4a_liquidity_gate_mean_reversion",
    "l1_h4b_liquidity_gate_size_adjusted_mean_reversion",
    "l1_h5a_vol_managed_trend",
    "l1_h5b_vol_managed_har_trend",
    "l1_h6a_vov_gate_mean_reversion",
    "l1_h7_squeeze_expansion_pullback",
    "l1_h8_trend_continuation_pullback",
    "l1_h9_momentum_breakout",
    "l1_h11_quality_filtered_continuation",
    "l2_h1_htf_trend_filter_pullback",
}


def adapter_for_strategy(strategy_name: str) -> StrategyAdapterSpec | None:
    name = str(strategy_name)
    if name == "l7_h1_csi_gated_displacement_trend":
        kernel = FamilyKernel(
            strategy_name=name,
            kernel_name="l7_h1_csi_displacement",
            mode="l7_h1_family_kernel",
            description=(
                "L7-H1 compiled causal feature/event adapter; classic engine remains "
                "source of truth for execution and accounting."
            ),
            numerical_signal_kernel=True,
        )
        return StrategyAdapterSpec(
            strategy_name=name,
            kernel=kernel,
            feature_requests=(
                StrategyFeatureRequest("engine_state", required=False),
                StrategyFeatureRequest("l7h1_csi_displacement", required=False, params={"signal_timeframes": ("15m", "1h")}),
            ),
        )
    if name in _HTF_EVENT_STRATEGIES:
        kernel = FamilyKernel(
            strategy_name=name,
            kernel_name=HTF_EVENT_SCHEDULE_KERNEL,
            mode="htf_event_schedule_kernel",
            description=(
                "Compiled two-clock event-schedule adapter skips flat/no-HTF-event "
                "minutes; classic strategy logic still runs on every actionable bar."
            ),
            numerical_signal_kernel=False,
        )
        return StrategyAdapterSpec(
            strategy_name=name,
            kernel=kernel,
            feature_requests=(
                StrategyFeatureRequest("engine_state", required=False),
                StrategyFeatureRequest("htf_context", required=False, params={"signal_timeframes": ("1m", "5m", "15m", "1h")}),
            ),
        )
    return None


def kernel_for_strategy(strategy_name: str) -> FamilyKernel | None:
    adapter = adapter_for_strategy(strategy_name)
    return adapter.kernel if adapter is not None else None


def registered_kernel_strategies() -> tuple[str, ...]:
    return tuple(sorted((*_HTF_EVENT_STRATEGIES, "l7_h1_csi_gated_displacement_trend")))


__all__ = [
    "FamilyKernel",
    "StrategyAdapterSpec",
    "StrategyFeatureRequest",
    "adapter_for_strategy",
    "kernel_for_strategy",
    "registered_kernel_strategies",
]
