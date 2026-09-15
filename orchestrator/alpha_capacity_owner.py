"""Bind native queue work to its still-living Hermes lease wrapper."""
from __future__ import annotations

import os
from pathlib import Path


def process_identity(pid: int) -> str | None:
    try:
        # Fields after the command's closing parenthesis start at proc field 3.
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]
    except (OSError, IndexError):
        return None


def current_owner() -> dict:
    pid = os.getpid()
    identity = process_identity(pid)
    if identity is None:
        raise RuntimeError("Cannot bind execution to a Linux process identity")
    return {"owner_pid": pid, "owner_start_ticks": identity}


def owner_alive(payload: dict) -> bool:
    pid = payload.get("owner_pid")
    return isinstance(pid, int) and pid > 0 and (
        process_identity(pid) == payload.get("owner_start_ticks")
        and payload.get("owner_start_ticks") is not None
    )
