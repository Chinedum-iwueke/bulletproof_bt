"""Optional Numba kernel facade for future supported strategies."""
from __future__ import annotations

from typing import Callable


def optional_njit() -> Callable:
    try:
        from numba import njit
    except Exception:
        def _identity(fn):
            return fn

        return _identity
    return njit


njit = optional_njit()


@njit
def count_positive_closes(close):  # pragma: no cover - compiled when numba exists
    total = 0
    for idx in range(close.shape[0]):
        if close[idx] > 0:
            total += 1
    return total

