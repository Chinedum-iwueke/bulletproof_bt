from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from bt.contracts.schema_versions import (
    BENCHMARK_METRICS_SCHEMA_VERSION,
    COMPARISON_SUMMARY_SCHEMA_VERSION,
    PERFORMANCE_SCHEMA_VERSION,
)

ARTIFACTS_MANIFEST_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ArtifactEntry:
    name: str
    required: bool
    present: bool
    description: str
    schema_version: int | None
    conditional_on: str | None


def _is_benchmark_enabled(config: dict[str, Any]) -> bool:
    benchmark_cfg = config.get("benchmark")
    return bool(benchmark_cfg.get("enabled", False)) if isinstance(benchmark_cfg, dict) else False


def _artifact_definitions() -> list[ArtifactEntry]:
    return [
        ArtifactEntry(
            name="benchmark_equity.csv",
            required=False,
            present=False,
            description="Benchmark equity curve sampled over run timeline.",
            schema_version=None,
            conditional_on="benchmark.enabled",
        ),
        ArtifactEntry(
            name="benchmark_metrics.json",
            required=False,
            present=False,
            description="Aggregate benchmark performance metrics.",
            schema_version=BENCHMARK_METRICS_SCHEMA_VERSION,
            conditional_on="benchmark.enabled",
        ),
        ArtifactEntry(
            name="comparison_summary.json",
            required=False,
            present=False,
            description="Strategy versus benchmark comparison summary.",
            schema_version=COMPARISON_SUMMARY_SCHEMA_VERSION,
            conditional_on="benchmark.enabled",
        ),
        ArtifactEntry(
            name="cost_breakdown.json",
            required=False,
            present=False,
            description="Machine-readable cost totals and reporting notes.",
            schema_version=1,
            conditional_on=None,
        ),
        ArtifactEntry(
            name="config_used.yaml",
            required=True,
            present=False,
            description="Fully resolved configuration used for the run.",
            schema_version=None,
            conditional_on=None,
        ),
        ArtifactEntry(
            name="data_scope.json",
            required=False,
            present=False,
            description="Applied data-scope knobs and effective date range.",
            schema_version=None,
            conditional_on="data.scope_knobs_active",
        ),
        ArtifactEntry(
            name="decisions.jsonl",
            required=True,
            present=False,
            description="Per-step strategy decisions in JSON lines format.",
            schema_version=None,
            conditional_on=None,
        ),
        ArtifactEntry(
            name="equity.csv",
            required=True,
            present=False,
            description="Strategy equity curve time series.",
            schema_version=None,
            conditional_on=None,
        ),
        ArtifactEntry(
            name="fills.jsonl",
            required=True,
            present=False,
            description="Order fill events in JSON lines format.",
            schema_version=None,
            conditional_on=None,
        ),
        ArtifactEntry(
            name="performance.json",
            required=True,
            present=False,
            description="Primary run performance metrics payload.",
            schema_version=PERFORMANCE_SCHEMA_VERSION,
            conditional_on=None,
        ),
        ArtifactEntry(
            name="performance_by_bucket.csv",
            required=True,
            present=False,
            description="Performance metrics grouped by configured buckets.",
            schema_version=None,
            conditional_on=None,
        ),
        ArtifactEntry(
            name="run_status.json",
            required=True,
            present=False,
            description="Run status and diagnostics for pass/fail handling.",
            schema_version=None,
            conditional_on=None,
        ),
        ArtifactEntry(
            name="summary.txt",
            required=False,
            present=False,
            description="Human-readable run summary text report.",
            schema_version=None,
            conditional_on="summary.enabled",
        ),
        ArtifactEntry(
            name="trades.csv",
            required=True,
            present=False,
            description="Executed trade ledger in CSV format.",
            schema_version=None,
            conditional_on=None,
        ),
    ]


def write_artifacts_manifest(
    run_dir: Path,
    *,
    config: dict[str, Any],
) -> Path:
    """
    Write run_dir/artifacts_manifest.json.

    Rules:
    - Deterministic ordering of entries (by 'name').
    - 'present' is derived from filesystem existence at write-time.
    - Include top-level:
        schema_version
        benchmark_enabled (bool)
        data_scope_active (bool)
        artifacts: list[ArtifactEntry as dict]
    - Return the written path.
    """
    benchmark_enabled = _is_benchmark_enabled(config)
    data_scope_active = (run_dir / "data_scope.json").exists()

    artifacts = [
        asdict(ArtifactEntry(**{**asdict(entry), "present": (run_dir / entry.name).exists()}))
        for entry in sorted(_artifact_definitions(), key=lambda item: item.name)
    ]

    payload: dict[str, Any] = {
        "schema_version": ARTIFACTS_MANIFEST_SCHEMA_VERSION,
        "benchmark_enabled": benchmark_enabled,
        "data_scope_active": data_scope_active,
        "artifacts": artifacts,
    }

    manifest_path = run_dir / "artifacts_manifest.json"
    manifest_path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return manifest_path
