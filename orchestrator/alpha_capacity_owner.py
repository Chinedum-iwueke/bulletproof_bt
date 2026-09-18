"""Bind native queue work to its still-living Hermes lease wrapper."""
from __future__ import annotations

import os
from pathlib import Path


def boot_identity() -> str | None:
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    except OSError:
        return None


def process_identity(pid: int) -> str | None:
    try:
        # Fields after the command's closing parenthesis start at proc field 3.
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]
    except (OSError, IndexError):
        return None


def current_owner() -> dict:
    pid = os.getpid()
    identity = process_identity(pid)
    boot_id = boot_identity()
    if identity is None or boot_id is None:
        raise RuntimeError("Cannot bind execution to a Linux process identity")
    return {
        "owner_pid": pid,
        "owner_start_ticks": identity,
        "owner_boot_id": boot_id,
    }


def owner_alive(payload: dict) -> bool:
    pid = payload.get("owner_pid")
    expected_boot = payload.get("owner_boot_id")
    current_boot = boot_identity()
    if expected_boot is not None and expected_boot != current_boot:
        return False
    return (
        isinstance(pid, int)
        and pid > 0
        and process_identity(pid) == payload.get("owner_start_ticks")
        and payload.get("owner_start_ticks") is not None
    )
