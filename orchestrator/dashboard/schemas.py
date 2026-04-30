from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DashboardConfig:
    db_path: str
    repo_root: str
    enable_actions: bool = False
