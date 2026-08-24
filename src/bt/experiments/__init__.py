"""Experiment runners with a lazy public import boundary."""
from __future__ import annotations

from typing import Any

__all__ = ["run_hypothesis_contract"]


def __getattr__(name: str) -> Any:
    if name == "run_hypothesis_contract":
        from bt.experiments.hypothesis_runner import run_hypothesis_contract

        return run_hypothesis_contract
    raise AttributeError(name)
