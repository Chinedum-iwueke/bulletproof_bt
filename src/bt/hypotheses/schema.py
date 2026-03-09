"""Typed schema for pre-registered hypothesis contracts."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Metadata:
    hypothesis_id: str
    title: str
    description: str
    research_layer: str
    hypothesis_family: str
    version: str
    author: str
    created_at: str


@dataclass(frozen=True)
class EvaluationSpec:
    required_tiers: tuple[str, ...] = ("Tier2", "Tier3")


@dataclass(frozen=True)
class LoggingSpec:
    schema_version: str = "1.0"
    required_fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class RuntimeControls:
    enabled: bool = True
    max_variants: int | None = None
    tags: tuple[str, ...] = ()
    notes: str = ""


@dataclass(frozen=True)
class HypothesisSchema:
    metadata: Metadata
    required_indicators: tuple[str, ...]
    indicator_defaults: dict[str, Any] = field(default_factory=dict)
    parameter_grid: dict[str, tuple[Any, ...]] = field(default_factory=dict)
    gates: tuple[dict[str, Any], ...] = ()
    entry: dict[str, Any] = field(default_factory=dict)
    exit: dict[str, Any] = field(default_factory=dict)
    evaluation: EvaluationSpec = field(default_factory=EvaluationSpec)
    logging: LoggingSpec = field(default_factory=LoggingSpec)
    runtime: RuntimeControls = field(default_factory=RuntimeControls)
