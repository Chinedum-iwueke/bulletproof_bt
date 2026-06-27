"""Registry of fast-path kernels that are safe for strategy families.

The registry is deliberately conservative. A strategy may be listed here only
when the kernel preserves no-lookahead/event semantics and the classic engine
continues to own risk, execution, accounting, and artifact writing.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FamilyKernel:
    strategy_name: str
    kernel_name: str
    mode: str
    description: str
    numerical_signal_kernel: bool = False


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
}


def kernel_for_strategy(strategy_name: str) -> FamilyKernel | None:
    name = str(strategy_name)
    if name == "l7_h1_csi_gated_displacement_trend":
        return FamilyKernel(
            strategy_name=name,
            kernel_name="l7_h1_csi_displacement",
            mode="l7_h1_family_kernel",
            description=(
                "L7-H1 compiled causal feature/event adapter; classic engine remains "
                "source of truth for execution and accounting."
            ),
            numerical_signal_kernel=True,
        )
    if name in _HTF_EVENT_STRATEGIES:
        return FamilyKernel(
            strategy_name=name,
            kernel_name=HTF_EVENT_SCHEDULE_KERNEL,
            mode="htf_event_schedule_kernel",
            description=(
                "Compiled two-clock event-schedule adapter skips flat/no-HTF-event "
                "minutes; classic strategy logic still runs on every actionable bar."
            ),
            numerical_signal_kernel=False,
        )
    return None


def registered_kernel_strategies() -> tuple[str, ...]:
    return tuple(sorted((*_HTF_EVENT_STRATEGIES, "l7_h1_csi_gated_displacement_trend")))


__all__ = ["FamilyKernel", "kernel_for_strategy", "registered_kernel_strategies"]
