"""Deprecated fast-path selector and fallback-safe batch runner.

The fast-path research work is intentionally retained as importable reference
code, but production backtests must currently run through the classic
event-driven engine.  The selector therefore records what was requested and
then explicitly falls back to classic execution for every non-classic request.
"""
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any

from bt.engine.fast_path.result_adapter import FastPathResult, write_fast_path_status
from bt.engine.fast_path.timing import TimingRecorder

FAST_PATH_DEPRECATED = True
FAST_PATH_DEPRECATION_REASON = (
    "fast_path is deprecated and disabled for production runs; "
    "classic event-driven execution is the source of truth"
)


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
            {
                "requested": requested,
                "actual_engine": "classic",
                "active_path": "classic_requested",
                "fast_path_deprecated": False,
            },
        )
        with timing.stage("fast_path.write_status", mode=result.mode):
            write_fast_path_status(run_dir, result)
        return result

    result = FastPathResult(
        False,
        "classic_deprecated_fallback",
        FAST_PATH_DEPRECATION_REASON,
        {
            "requested": requested,
            "actual_engine": "classic",
            "active_path": "classic_deprecated_fallback",
            "fast_path_deprecated": FAST_PATH_DEPRECATED,
            "deprecation_reason": FAST_PATH_DEPRECATION_REASON,
        },
    )
    timing.event(
        "fast_path.deprecated_fallback",
        requested=requested,
        selected="classic",
        reason=FAST_PATH_DEPRECATION_REASON,
    )
    with timing.stage("fast_path.write_status", mode=result.mode):
        write_fast_path_status(run_dir, result)
    return result
