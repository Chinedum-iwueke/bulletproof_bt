"""Typed response contracts for Strategy Robustness Lab V1."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ScorePayload:
    overall: float
    sub_scores: dict[str, float]
    methodology: dict[str, Any]


@dataclass(frozen=True)
class IngestedRun:
    source: str
    trades: Any
    equity: Any
    performance: dict[str, Any]
    metadata: dict[str, Any]
