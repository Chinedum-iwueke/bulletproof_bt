"""Fast-path support detection and signal compilation metadata."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from bt.engine.fast_path.family_kernels import adapter_for_strategy


@dataclass(frozen=True)
class FastPathSupport:
    supported: bool
    reason: str
    strategy_name: str
    mode: str = "classic_fallback"
    kernel_name: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


def inspect_support(config: dict[str, Any]) -> FastPathSupport:
    strategy_cfg = config.get("strategy") if isinstance(config.get("strategy"), dict) else {}
    strategy_name = str(strategy_cfg.get("name", "coinflip"))
    data_cfg = config.get("data") if isinstance(config.get("data"), dict) else {}
    adapter = adapter_for_strategy(strategy_name)
    family_kernel = adapter.kernel if adapter is not None else None
    dataset_kind = data_cfg.get("dataset_kind")
    details: dict[str, Any] = {
        "strategy_name": strategy_name,
        "dataset_kind": dataset_kind,
        "htf_context_source": data_cfg.get("htf_context_source"),
        "candidate_event_mode": data_cfg.get("candidate_event_mode"),
        "columnar_candidate_events": bool(data_cfg.get("columnar_candidate_events", False)),
        "has_htf_resampler": bool(config.get("htf_resampler")),
        "family_kernel_registered": family_kernel is not None,
        "strategy_adapter_registered": adapter is not None,
        "strategy_adapter": adapter.to_json() if adapter is not None else None,
        "feature_requests": [req.feature_name for req in adapter.feature_requests] if adapter is not None else [],
        "candidate_scheduler": adapter.candidate_scheduler if adapter is not None else None,
        "strategy_use_compiled_features": strategy_cfg.get("use_compiled_features"),
        "strategy_use_compiled_event_kernel": strategy_cfg.get("use_compiled_event_kernel"),
        "compiled_event_source": strategy_cfg.get("compiled_event_source"),
    }
    if data_cfg.get("dataset_kind") == "research_panel" and strategy_name == "l7_h1_csi_gated_displacement_trend":
        if strategy_cfg.get("use_compiled_features") is False:
            return FastPathSupport(
                False,
                "compiled L7-H1 features disabled by strategy.use_compiled_features=false; classic strategy feature path is active",
                strategy_name,
                mode="classic_fallback",
                kernel_name="l7_h1_csi_displacement",
                details={**details, "active_path": "classic_strategy_features_disabled"},
            )
        if strategy_cfg.get("use_compiled_event_kernel") is True:
            source = str(strategy_cfg.get("compiled_event_source", "columns") or "columns").strip().lower()
            return FastPathSupport(
                True,
                "compiled L7-H1 event adapter enabled; classic risk/execution/portfolio/logging remain source of truth",
                strategy_name,
                mode=f"classic_with_l7h1_{source}_event_adapter",
                kernel_name="l7_h1_csi_displacement",
                details={**details, "active_path": "l7h1_event_adapter", "event_source": source},
            )
        return FastPathSupport(
            True,
            "compiled L7-H1 family feature kernel attached; classic engine remains source of truth for execution",
            strategy_name,
            mode="classic_with_compiled_l7h1_features",
            kernel_name="l7_h1_csi_displacement",
            details={**details, "active_path": "l7h1_compiled_features"},
        )
    if (
        data_cfg.get("dataset_kind") == "research_panel"
        and family_kernel is not None
        and data_cfg.get("htf_context_source") == "precomputed"
    ):
        return FastPathSupport(
            True,
            f"{family_kernel.mode} enabled on precomputed context; classic strategy/risk/execution remain source of truth on actionable bars",
            strategy_name,
            mode="classic_with_compiled_htf_event_kernel_precomputed",
            kernel_name=family_kernel.kernel_name,
            details={**details, "active_path": family_kernel.mode, "context_source": "precomputed"},
        )
    if (
        data_cfg.get("dataset_kind") == "research_panel"
        and family_kernel is not None
        and config.get("htf_resampler")
    ):
        return FastPathSupport(
            True,
            f"{family_kernel.mode} enabled on streaming context; classic strategy/risk/execution remain source of truth on actionable bars",
            strategy_name,
            mode="classic_with_compiled_htf_event_kernel_streaming",
            kernel_name=family_kernel.kernel_name,
            details={**details, "active_path": family_kernel.mode, "context_source": "streaming"},
        )
    if data_cfg.get("dataset_kind") == "research_panel":
        return FastPathSupport(
            False,
            "research_panel strategies require classic event semantics for now",
            strategy_name,
            details={**details, "active_path": "unsupported_research_panel"},
        )
    if strategy_name != "coinflip":
        return FastPathSupport(
            False,
            f"strategy {strategy_name!r} is not supported by the prototype kernel",
            strategy_name,
            details={**details, "active_path": "unsupported_strategy"},
        )
    return FastPathSupport(
        False,
        "coinflip kernel intentionally disabled until full parity adapter is expanded",
        strategy_name,
        details={**details, "active_path": "prototype_disabled"},
    )
