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
    requested = resolve_execution_engine(config)
    if requested == "classic":
        result = FastPathResult(False, "classic", "classic execution requested")
        write_fast_path_status(run_dir, result)
        return result

    with timing.stage("fast_path.support_check", requested=requested):
        support = inspect_support(config)

    if not support.supported:
        result = FastPathResult(False, "classic_fallback", support.reason)
        write_fast_path_status(run_dir, result)
        if requested == "fast_path":
            # Hard fast_path still falls back instead of failing because research
            # daemon safety requires unsupported hypotheses to keep running.
            return result
        return result

    if support.strategy_name == "l7_h1_csi_gated_displacement_trend":
        strategy_cfg = config.get("strategy") if isinstance(config.get("strategy"), dict) else {}
        if strategy_cfg.get("use_compiled_event_kernel") is True:
            source = str(strategy_cfg.get("compiled_event_source", "columns") or "columns").strip().lower()
            mode = f"classic_with_l7h1_{source}_event_adapter"
        else:
            mode = "classic_with_compiled_l7h1_features"
        result = FastPathResult(False, mode, support.reason)
        write_fast_path_status(run_dir, result)
        return result
    data_cfg = config.get("data") if isinstance(config.get("data"), dict) else {}
    if data_cfg.get("htf_context_source") == "precomputed":
        result = FastPathResult(False, "classic_with_precomputed_htf_context", support.reason)
        write_fast_path_status(run_dir, result)
        return result

    # Placeholder for future supported kernels. Keeping this fallback until
    # parity-expanded avoids silently changing strategy behavior.
    result = FastPathResult(False, "classic_fallback", "no enabled kernel adapter")
    write_fast_path_status(run_dir, result)
    return result
