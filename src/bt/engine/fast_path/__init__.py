"""Deprecated fast-path execution seam.

The reference modules remain importable for audits and possible future design
work. Production backtests currently resolve to the classic event-driven engine.
"""
from __future__ import annotations

from bt.engine.fast_path.batch_runner import FastPathDecision, run_fast_path_if_supported
from bt.engine.fast_path.timing import TimingRecorder

__all__ = ["FastPathDecision", "TimingRecorder", "run_fast_path_if_supported"]
