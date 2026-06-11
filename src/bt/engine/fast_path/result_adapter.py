"""Result adapter contract for fast-path compact outputs."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from bt.logging.formatting import write_json_deterministic


@dataclass(frozen=True)
class FastPathResult:
    handled: bool
    mode: str
    reason: str


def write_fast_path_status(run_dir: Path, result: FastPathResult) -> None:
    write_json_deterministic(
        run_dir / "fast_path_status.json",
        {
            "handled": result.handled,
            "mode": result.mode,
            "reason": result.reason,
            "schema_version": 1,
        },
    )

