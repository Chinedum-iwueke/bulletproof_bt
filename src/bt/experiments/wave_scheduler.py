"""Chunked scheduling helpers for process-pool experiment runners."""
from __future__ import annotations

from typing import Iterable, Iterator, TypeVar

T = TypeVar("T")


def resolve_wave_size(*, max_workers: int, requested_wave_size: int | None = None) -> int:
    if max_workers <= 0:
        raise ValueError("max_workers must be positive")
    if requested_wave_size is None:
        return max_workers * 2
    if requested_wave_size <= 0:
        raise ValueError("requested wave size must be positive")
    return max_workers if requested_wave_size < max_workers else requested_wave_size


def iter_waves(items: Iterable[T], *, wave_size: int) -> Iterator[list[T]]:
    if wave_size <= 0:
        raise ValueError("wave_size must be positive")
    chunk: list[T] = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= wave_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk
