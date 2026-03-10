"""Build generic manifest for hypothesis-parallel execution."""
from __future__ import annotations

from bt.experiments.parallel_grid import cli_build_hypothesis_manifest


if __name__ == "__main__":
    raise SystemExit(cli_build_hypothesis_manifest())
