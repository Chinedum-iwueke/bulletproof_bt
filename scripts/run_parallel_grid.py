"""Process-level parallel runner for H1B manifests."""
from __future__ import annotations

from bt.experiments.parallel_grid import cli_run_parallel_grid


if __name__ == "__main__":
    raise SystemExit(cli_run_parallel_grid())
