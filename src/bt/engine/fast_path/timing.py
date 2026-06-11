"""Lightweight timing instrumentation for classic and fast-path runs."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import json
from pathlib import Path
from time import perf_counter
from typing import Iterator


@dataclass
class TimingEvent:
    stage: str
    seconds: float
    metadata: dict[str, object]


class TimingRecorder:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.events: list[TimingEvent] = []

    @contextmanager
    def stage(self, name: str, **metadata: object) -> Iterator[None]:
        start = perf_counter()
        try:
            yield
        finally:
            self.events.append(TimingEvent(name, perf_counter() - start, dict(metadata)))

    def event(self, name: str, **metadata: object) -> None:
        self.events.append(TimingEvent(name, 0.0, dict(metadata)))

    def write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "events": [
                {"stage": item.stage, "seconds": item.seconds, "metadata": item.metadata}
                for item in self.events
            ],
            "total_seconds": sum(item.seconds for item in self.events),
        }
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
        tmp.replace(self.path)

