"""Result adapter contract for fast-path compact outputs."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from pathlib import Path

from bt.logging.formatting import write_json_deterministic


@dataclass(frozen=True)
class FastPathResult:
    handled: bool
    mode: str
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)


def write_fast_path_status(run_dir: Path, result: FastPathResult) -> None:
    metadata = dict(result.metadata)
    write_json_deterministic(
        run_dir / "fast_path_status.json",
        {
            "handled": result.handled,
            "mode": result.mode,
            "reason": result.reason,
            "requested_engine": metadata.get("requested"),
            "actual_engine": metadata.get("actual_engine", "classic" if not result.handled else result.mode),
            "fast_path_deprecated": bool(metadata.get("fast_path_deprecated", False)),
            "metadata": metadata,
            "schema_version": 1,
        },
    )
