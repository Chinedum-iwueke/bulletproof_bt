"""Fast-path selector and fallback-safe batch runner."""
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any

from bt.engine.fast_path.result_adapter import FastPathResult, write_fast_path_status
from bt.engine.fast_path.signal_compiler import inspect_support
from bt.engine.fast_path.timing import TimingRecorder


@dataclass(frozen=True)
class FastPathDecision:
    requested: str
    selected: str
    reason: str


def resolve_execution_engine(config: dict[str, Any]) -> str:
    raw = os.environ.get("BULLETPROOF_EXECUTION_ENGINE")
    if raw is None:
        raw = config.get("execution_engine", config.get("engine", "classic"))
    value = str(raw or "classic").strip().lower()
    if value not in {"classic", "auto", "fast_path"}:
        raise ValueError("execution_engine must be one of: classic, auto, fast_path")
    return value


def run_fast_path_if_supported(
    *,
    config: dict[str, Any],
    data_path: str,
    run_dir: Path,
    timing: TimingRecorder,
) -> FastPathResult:
    with timing.stage("fast_path.resolve_execution_engine"):
        requested = resolve_execution_engine(config)
    timing.event(
        "fast_path.requested",
        requested=requested,
        data_kind=(config.get("data") or {}).get("dataset_kind") if isinstance(config.get("data"), dict) else None,
        strategy=(config.get("strategy") or {}).get("name") if isinstance(config.get("strategy"), dict) else None,
    )
    if requested == "classic":
        result = FastPathResult(
            False,
            "classic",
            "classic execution requested",
            {"requested": requested, "active_path": "classic_requested"},
        )
        with timing.stage("fast_path.write_status", mode=result.mode):
            write_fast_path_status(run_dir, result)
        return result

    with timing.stage("fast_path.support_check", requested=requested):
        support = inspect_support(config)
    timing.event(
        "fast_path.support_result",
        requested=requested,
        supported=support.supported,
        mode=support.mode,
        strategy=support.strategy_name,
        kernel_name=support.kernel_name,
        reason=support.reason,
        **support.details,
    )

    if not support.supported:
        result = FastPathResult(
            False,
            support.mode or "classic_fallback",
            support.reason,
            {"requested": requested, "strategy_name": support.strategy_name, "kernel_name": support.kernel_name, **support.details},
        )
        with timing.stage("fast_path.write_status", mode=result.mode):
            write_fast_path_status(run_dir, result)
        if requested == "fast_path":
            # Hard fast_path still falls back instead of failing because research
            # daemon safety requires unsupported hypotheses to keep running.
            return result
        return result

    if support.mode in {
        "classic_with_compiled_l7h1_features",
        "classic_with_l7h1_columns_event_adapter",
        "classic_with_l7h1_online_event_adapter",
        "classic_with_compiled_htf_event_kernel_precomputed",
        "classic_with_compiled_htf_event_kernel_streaming",
    }:
        result = FastPathResult(
            False,
            support.mode,
            support.reason,
            {"requested": requested, "strategy_name": support.strategy_name, "kernel_name": support.kernel_name, **support.details},
        )
        with timing.stage("fast_path.write_status", mode=result.mode):
            write_fast_path_status(run_dir, result)
        return result

    # Placeholder for future supported kernels. Keeping this fallback until
    # parity-expanded avoids silently changing strategy behavior.
    result = FastPathResult(
        False,
        "classic_fallback",
        "no enabled kernel adapter",
        {"requested": requested, "strategy_name": support.strategy_name, "kernel_name": support.kernel_name, **support.details},
    )
    with timing.stage("fast_path.write_status", mode=result.mode):
        write_fast_path_status(run_dir, result)
    return result
