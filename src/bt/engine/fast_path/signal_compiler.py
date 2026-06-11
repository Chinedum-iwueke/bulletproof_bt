"""Fast-path support detection and signal compilation metadata."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FastPathSupport:
    supported: bool
    reason: str
    strategy_name: str


def inspect_support(config: dict[str, Any]) -> FastPathSupport:
    strategy_cfg = config.get("strategy") if isinstance(config.get("strategy"), dict) else {}
    strategy_name = str(strategy_cfg.get("name", "coinflip"))
    data_cfg = config.get("data") if isinstance(config.get("data"), dict) else {}
    if data_cfg.get("dataset_kind") == "research_panel" and strategy_name == "l7_h1_csi_gated_displacement_trend":
        if strategy_cfg.get("use_compiled_features") is False:
            return FastPathSupport(
                False,
                "compiled L7-H1 features disabled by strategy.use_compiled_features=false; classic strategy feature path is active",
                strategy_name,
            )
        if strategy_cfg.get("use_compiled_event_kernel") is True:
            return FastPathSupport(
                True,
                "compiled L7-H1 event adapter enabled; classic risk/execution/portfolio/logging remain source of truth",
                strategy_name,
            )
        return FastPathSupport(
            True,
            "compiled L7-H1 family feature kernel attached; classic engine remains source of truth for execution",
            strategy_name,
        )
    if data_cfg.get("dataset_kind") == "research_panel" and data_cfg.get("htf_context_source") == "precomputed":
        return FastPathSupport(
            True,
            "precomputed HTF context enabled; classic strategy/risk/execution remain source of truth",
            strategy_name,
        )
    if data_cfg.get("dataset_kind") == "research_panel":
        return FastPathSupport(False, "research_panel strategies require classic event semantics for now", strategy_name)
    if strategy_name != "coinflip":
        return FastPathSupport(False, f"strategy {strategy_name!r} is not supported by the prototype kernel", strategy_name)
    return FastPathSupport(False, "coinflip kernel intentionally disabled until full parity adapter is expanded", strategy_name)
