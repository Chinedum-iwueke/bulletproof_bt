"""Build H1B manifest + overrides for volfloor_ema_pullback."""
from __future__ import annotations

import sys

from bt.experiments.parallel_grid import cli_build_manifest


if __name__ == "__main__":
    raise SystemExit(cli_build_manifest(["--strategy", "volfloor_ema_pullback", *sys.argv[1:]]))
