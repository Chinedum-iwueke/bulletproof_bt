from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class AlphaCandidate:
    identity: dict[str, Any]
    performance: dict[str, Any]
    tail: dict[str, Any]
    cost: dict[str, Any]
    state_profile: dict[str, Any]
    zoo_metadata: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return {
            **self.identity,
            **self.performance,
            **self.tail,
            **self.cost,
            **self.state_profile,
            **self.zoo_metadata,
        }
