"""Stable public API for the ``bt`` package.

Only symbols exported here are part of the compatibility promise for users.
Internal modules and implementation details are intentionally excluded from the
public surface and may change without notice.
"""

from bt._version import __version__


def run_backtest(*args, **kwargs):
    from bt.api import run_backtest as _run_backtest

    return _run_backtest(*args, **kwargs)


def run_grid(*args, **kwargs):
    from bt.api import run_grid as _run_grid

    return _run_grid(*args, **kwargs)


def run_analysis_from_parsed_artifact(*args, **kwargs):
    from bt.saas.service import run_analysis_from_parsed_artifact as _run_analysis_from_parsed_artifact

    return _run_analysis_from_parsed_artifact(*args, **kwargs)

__all__ = [
    "run_backtest",
    "run_grid",
    "run_analysis_from_parsed_artifact",
    "__version__",
]
