"""Worker bootstrap utilities for parallel hypothesis runs."""
from __future__ import annotations

import faulthandler
import json
import os
import resource
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any, TextIO

THREAD_CAP_ENV_VARS = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "POLARS_MAX_THREADS",
)


@dataclass(frozen=True)
class WorkerBootstrapResult:
    effective_thread_caps: dict[str, str]
    changed_thread_caps: dict[str, str]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def apply_thread_caps(default_threads: str = "1") -> WorkerBootstrapResult:
    effective: dict[str, str] = {}
    changed: dict[str, str] = {}
    for key in THREAD_CAP_ENV_VARS:
        if key in os.environ:
            effective[key] = os.environ[key]
            continue
        os.environ[key] = default_threads
        effective[key] = default_threads
        changed[key] = default_threads
    return WorkerBootstrapResult(effective_thread_caps=effective, changed_thread_caps=changed)


def memory_snapshot_mb() -> float | None:
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF)
    except Exception:
        return None
    value = float(usage.ru_maxrss)
    # Linux is KiB, macOS is bytes.
    if value > 1_000_000_000:
        return value / (1024.0 * 1024.0)
    return value / 1024.0


class WorkerLogger:
    """Small structured worker logger writing newline-delimited JSON events."""

    def __init__(self, run_dir: Path) -> None:
        self._run_dir = run_dir
        self._run_dir.mkdir(parents=True, exist_ok=True)
        self._log_path = self._run_dir / "worker.log"
        self._handle: TextIO = self._log_path.open("a", encoding="utf-8")
        self._phase_started: dict[str, float] = {}

    def close(self) -> None:
        self._handle.close()

    def event(self, phase: str, **payload: Any) -> None:
        body = {
            "ts": utc_now_iso(),
            "phase": phase,
            "pid": os.getpid(),
            "memory_mb": memory_snapshot_mb(),
            **payload,
        }
        self._handle.write(json.dumps(body, sort_keys=True) + "\n")
        self._handle.flush()

    def start_phase(self, name: str, **payload: Any) -> None:
        self._phase_started[name] = monotonic()
        self.event(f"{name}:start", **payload)

    def finish_phase(self, name: str, **payload: Any) -> None:
        started = self._phase_started.get(name)
        duration = None if started is None else monotonic() - started
        self.event(f"{name}:finish", duration_sec=duration, **payload)


def enable_worker_faulthandler(run_dir: Path) -> Path:
    run_dir.mkdir(parents=True, exist_ok=True)
    fault_path = run_dir / "faulthandler.log"
    handle = fault_path.open("a", encoding="utf-8")
    faulthandler.enable(file=handle, all_threads=True)
    return fault_path


def write_worker_exception(run_dir: Path, exc: BaseException) -> Path:
    path = run_dir / "worker_exception.txt"
    payload = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    path.write_text(payload, encoding="utf-8")
    return path
