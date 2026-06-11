"""Conservative fast-path execution seam.

The fast path is opt-in and fallback-safe. It never replaces the classic engine
unless the batch runner can prove support for the current strategy/config.
"""
from __future__ import annotations

from bt.engine.fast_path.batch_runner import FastPathDecision, run_fast_path_if_supported
from bt.engine.fast_path.timing import TimingRecorder

__all__ = ["FastPathDecision", "TimingRecorder", "run_fast_path_if_supported"]

