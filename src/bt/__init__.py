"""Stable public API for the ``bt`` package.

Only symbols exported here are part of the compatibility promise for users.
Internal modules and implementation details are intentionally excluded from the
public surface and may change without notice.
"""

from bt._version import __version__
from bt.saas.service import run_analysis_from_parsed_artifact
from bt.api import run_backtest, run_grid

__all__ = ["run_backtest", "run_grid", "__version__"]

