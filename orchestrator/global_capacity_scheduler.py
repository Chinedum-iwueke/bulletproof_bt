#!/usr/bin/env python3
"""Global capacity scheduler for research daemon jobs.

This module coordinates whole daemon jobs. It never changes strategy logic,
market data, fills, PnL, or run artifacts. When memory pressure is high it
pauses an entire process group with SIGSTOP; when pressure recovers it resumes
the same process group with SIGCONT. That preserves deterministic execution and
only changes wall-clock timing.
"""
from __future__ import annotations

import argparse
import fcntl
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import logging
import math
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from typing import Any
from uuid import uuid4

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from bt.experiments.resource_controls import MemorySnapshot, memory_snapshot  # noqa: E402
from orchestrator.db import ResearchDB  # noqa: E402


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class CapacitySchedulerConfig:
    queue_name: str = "approved_backtests"
    poll_seconds: float = 30.0
    target_workers: int = 28
    max_workers_per_job: int = 8
    max_concurrent_jobs: int = 4
    min_free_ram_gb: float = 8.0
    pause_free_ram_gb: float = 6.0
    resume_free_ram_gb: float = 12.0
    stale_lock_refresh_seconds: float = 60.0
    log_path: str = "logs/research_capacity_scheduler.log"
    state_path: str = "logs/research_capacity_scheduler_state.json"
    child_log_dir: str = "logs/research_capacity_scheduler_jobs"
    estimated_worker_ram_gb: float = 2.0
    max_job_attempts: int = 2


@dataclass
class ManagedJob:
    locked_by: str
    pid: int
    pgid: int
    queue_id: str | None
    name: str | None
    estimated_workers: int
    priority: int
    status: str
    launched_at: str
    paused_at: str | None = None
    completed_at: str | None = None
    returncode: int | None = None
    stdout_log: str | None = None
    stderr_log: str | None = None
    last_lock_refresh_at: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the global research capacity scheduler.")
    parser.add_argument("--db", default="research_db/research.sqlite")
    parser.add_argument("--config", default="orchestrator/daemon_config.yaml")
    parser.add_argument("--queue-name", default=None)
    parser.add_argument("--target-workers", type=int, default=None)
    parser.add_argument("--max-workers-per-job", type=int, default=None)
    parser.add_argument("--max-concurrent-jobs", type=int, default=None)
    parser.add_argument("--min-free-ram-gb", type=float, default=None)
    parser.add_argument("--pause-free-ram-gb", type=float, default=None)
    parser.add_argument("--resume-free-ram-gb", type=float, default=None)
    parser.add_argument("--poll-seconds", type=float, default=None)
    parser.add_argument("--once", action="store_true", default=False)
    parser.add_argument("--dry-run", action="store_true", default=False)
    return parser.parse_args()


def load_daemon_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Daemon config not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError("Daemon config must be a YAML mapping")
    return data


def load_capacity_config(daemon_config: dict[str, Any], args: argparse.Namespace) -> CapacitySchedulerConfig:
    block = daemon_config.get("capacity_scheduler") or {}
    if not isinstance(block, dict):
        raise ValueError("capacity_scheduler config must be a YAML mapping")
    cfg = CapacitySchedulerConfig(
        queue_name=str(args.queue_name or block.get("queue_name") or daemon_config.get("queue_name", "approved_backtests")),
        poll_seconds=float(args.poll_seconds if args.poll_seconds is not None else block.get("poll_seconds", 30)),
        target_workers=int(args.target_workers if args.target_workers is not None else block.get("target_workers", 28)),
        max_workers_per_job=int(
            args.max_workers_per_job if args.max_workers_per_job is not None else block.get("max_workers_per_job", 8)
        ),
        max_concurrent_jobs=int(
            args.max_concurrent_jobs if args.max_concurrent_jobs is not None else block.get("max_concurrent_jobs", 4)
        ),
        min_free_ram_gb=float(
            args.min_free_ram_gb if args.min_free_ram_gb is not None else block.get("min_free_ram_gb", 8)
        ),
        pause_free_ram_gb=float(
            args.pause_free_ram_gb if args.pause_free_ram_gb is not None else block.get("pause_free_ram_gb", 6)
        ),
        resume_free_ram_gb=float(
            args.resume_free_ram_gb if args.resume_free_ram_gb is not None else block.get("resume_free_ram_gb", 12)
        ),
        stale_lock_refresh_seconds=float(block.get("stale_lock_refresh_seconds", 60)),
        log_path=str(block.get("log_path", "logs/research_capacity_scheduler.log")),
        state_path=str(block.get("state_path", "logs/research_capacity_scheduler_state.json")),
        child_log_dir=str(block.get("child_log_dir", "logs/research_capacity_scheduler_jobs")),
        estimated_worker_ram_gb=float(block.get("estimated_worker_ram_gb", 2.0)),
        max_job_attempts=int(
            block.get("max_job_attempts", daemon_config.get("max_job_attempts", 2))
        ),
    )
    if cfg.target_workers <= 0:
        raise ValueError("target_workers must be positive")
    if cfg.max_workers_per_job <= 0:
        raise ValueError("max_workers_per_job must be positive")
    if cfg.max_concurrent_jobs <= 0:
        raise ValueError("max_concurrent_jobs must be positive")
    if not math.isfinite(cfg.estimated_worker_ram_gb) or cfg.estimated_worker_ram_gb <= 0:
        raise ValueError("estimated_worker_ram_gb must be finite and positive")
    if cfg.max_job_attempts <= 0:
        raise ValueError("max_job_attempts must be positive")
    if cfg.pause_free_ram_gb >= cfg.resume_free_ram_gb:
        raise ValueError("pause_free_ram_gb must be lower than resume_free_ram_gb")
    return cfg


def configure_logging(path: Path) -> logging.Logger:
    path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("research_capacity_scheduler")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    file_handler = logging.FileHandler(path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger


def active_worker_slots(jobs: list[ManagedJob]) -> int:
    return sum(job.estimated_workers for job in jobs if job.status == "running")


def paused_worker_slots(jobs: list[ManagedJob]) -> int:
    return sum(job.estimated_workers for job in jobs if job.status == "paused")


def should_pause_for_memory(snapshot: MemorySnapshot | None, cfg: CapacitySchedulerConfig) -> bool:
    return snapshot is not None and snapshot.available_gb < cfg.pause_free_ram_gb


def should_resume_for_memory(snapshot: MemorySnapshot | None, cfg: CapacitySchedulerConfig) -> bool:
    return snapshot is None or snapshot.available_gb >= cfg.resume_free_ram_gb


def estimate_worker_slots(payload: dict[str, Any], cfg: CapacitySchedulerConfig, daemon_config: dict[str, Any]) -> int:
    """Estimate the global capacity a queue item will consume.

    The scheduler does not rewrite payload worker counts. If a payload asks for
    parallel stable+volatile datasets, the pipeline splits max_workers between
    both datasets, so the total worker budget remains max_workers.
    """
    default_workers = int(daemon_config.get("default_max_workers", 6))
    max_workers = int(payload.get("max_workers") or default_workers)
    return max(1, max_workers)


def external_locked_worker_slots(db: ResearchDB, queue_name: str, cfg: CapacitySchedulerConfig, daemon_config: dict[str, Any], managed_owners: set[str] | None = None) -> int:
    """Estimate slots already consumed by locked jobs this scheduler did not launch."""
    rows = db.connect().execute(
        """
        SELECT payload_json, locked_by
        FROM queues
        WHERE queue_name = ?
          AND status = 'LOCKED'
        """,
        (queue_name,),
    ).fetchall()
    total = 0
    for row in rows:
        if row["locked_by"] in (managed_owners or set()):
            continue
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except Exception:
            payload = {}
        total += estimate_worker_slots(payload, cfg, daemon_config)
    return total


def queue_counts(db: ResearchDB, queue_name: str) -> dict[str, int]:
    rows = db.connect().execute(
        "SELECT status, COUNT(*) AS n FROM queues WHERE queue_name = ? GROUP BY status",
        (queue_name,),
    ).fetchall()
    return {str(row["status"]): int(row["n"]) for row in rows}


def refresh_queue_lock(db: ResearchDB, locked_by: str) -> None:
    now = utc_now_iso()
    db.connect().execute(
        """
        UPDATE queues
        SET locked_at = ?, updated_at = ?
        WHERE locked_by = ? AND status = 'LOCKED'
        """,
        (now, now, locked_by),
    )
    db.connect().commit()


def find_locked_queue_item(db: ResearchDB, locked_by: str) -> tuple[str | None, str | None]:
    row = db.connect().execute(
        """
        SELECT id, payload_json
        FROM queues
        WHERE locked_by = ? AND status = 'LOCKED'
        ORDER BY locked_at DESC
        LIMIT 1
        """,
        (locked_by,),
    ).fetchone()
    if row is None:
        return None, None
    payload = json.loads(row["payload_json"] or "{}")
    return str(row["id"]), str(payload.get("name") or "")


def reconcile_orphaned_capacity_locks(
    db: ResearchDB,
    *,
    queue_name: str,
    hostname: str,
    max_job_attempts: int,
) -> dict[str, int]:
    """Resolve locks owned by a prior scheduler process on this host.

    The scheduler's systemd unit owns its complete child cgroup, so acquiring the
    exclusive leader lock proves that no prior managed child can still be valid.
    A governed job may be requeued only while its Hermes wrapper is still alive;
    otherwise it must fail and let the control plane issue a fresh task attempt.
    """
    from orchestrator.alpha_capacity_owner import owner_alive

    rows = db.connect().execute(
        """
        SELECT id, payload_json, attempts
        FROM queues
        WHERE queue_name = ?
          AND status = 'LOCKED'
          AND locked_by LIKE ?
        ORDER BY locked_at ASC
        """,
        (queue_name, f"capacity:{hostname}:%"),
    ).fetchall()
    result = {"requeued": 0, "failed_dead_owner": 0, "failed_attempts": 0}
    for row in rows:
        queue_id = str(row["id"])
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            payload = {}
        attempts = int(row["attempts"] or 0)
        governed = payload.get("kind") == "governed_alpha_assignment"
        if governed and not owner_alive(payload):
            db.mark_queue_failed(
                queue_id,
                "capacity scheduler restart detected dead Hermes lease owner; "
                "partial artifacts retained",
            )
            result["failed_dead_owner"] += 1
        elif attempts >= max_job_attempts:
            db.mark_queue_failed(
                queue_id,
                "capacity scheduler restart exhausted native queue attempts; "
                "partial artifacts retained",
            )
            result["failed_attempts"] += 1
        else:
            db.release_queue_lock(
                queue_id,
                "capacity scheduler restart recovered orphan lock; partial artifacts retained",
            )
            result["requeued"] += 1
    return result


def pending_queue_candidates(db: ResearchDB, queue_name: str, *, limit: int = 25) -> list[sqlite3.Row]:
    now = utc_now_iso()
    return list(
        db.connect()
        .execute(
            """
            SELECT *
            FROM queues
            WHERE queue_name = ?
              AND status = 'PENDING'
              AND (available_after IS NULL OR available_after <= ?)
            ORDER BY priority DESC, created_at ASC
            LIMIT ?
            """,
            (queue_name, now, limit),
        )
        .fetchall()
    )


def process_tree_rss_gb(root_pid: int) -> float | None:
    try:
        import psutil  # type: ignore
    except Exception:
        psutil = None
    if psutil is not None:
        try:
            proc = psutil.Process(root_pid)
            rss = proc.memory_info().rss
            for child in proc.children(recursive=True):
                try:
                    rss += child.memory_info().rss
                except Exception:
                    continue
            return float(rss) / (1024**3)
        except Exception:
            return None

    try:
        pids = {root_pid}
        changed = True
        while changed:
            changed = False
            for stat_path in Path("/proc").glob("[0-9]*/stat"):
                try:
                    text = stat_path.read_text(encoding="utf-8")
                    parts = text.split()
                    pid = int(parts[0])
                    ppid = int(parts[3])
                    if ppid in pids and pid not in pids:
                        pids.add(pid)
                        changed = True
                except Exception:
                    continue
        page_size = os.sysconf("SC_PAGE_SIZE")
        rss_pages = 0
        for pid in pids:
            try:
                statm = Path(f"/proc/{pid}/statm").read_text(encoding="utf-8").split()
                rss_pages += int(statm[1])
            except Exception:
                continue
        return float(rss_pages * page_size) / (1024**3)
    except Exception:
        return None


class CapacityScheduler:
    def __init__(self, *, db_path: Path, daemon_config_path: Path, daemon_config: dict[str, Any], cfg: CapacitySchedulerConfig, dry_run: bool = False) -> None:
        self.db_path = db_path
        self.daemon_config_path = daemon_config_path
        self.daemon_config = daemon_config
        self.cfg = cfg
        self.dry_run = dry_run
        self.db = ResearchDB(db_path, repo_root=PROJECT_ROOT)
        self.db.init_schema()
        self._leader_lock = db_path.with_suffix(".capacity.lock").open("a")
        try:
            fcntl.flock(self._leader_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._leader_lock.close()
            self.db.close()
            raise RuntimeError("A capacity scheduler already owns this database") from None
        self.logger = configure_logging(PROJECT_ROOT / cfg.log_path)
        self.jobs: list[ManagedJob] = []
        self.startup_recovery = reconcile_orphaned_capacity_locks(
            self.db,
            queue_name=cfg.queue_name,
            hostname=socket.gethostname(),
            max_job_attempts=cfg.max_job_attempts,
        )
        if any(self.startup_recovery.values()):
            self.logger.warning(
                "reconciled orphaned capacity locks: %s", self.startup_recovery
            )
        self.shutdown_requested = False
        signal.signal(signal.SIGINT, self._request_shutdown)
        signal.signal(signal.SIGTERM, self._request_shutdown)

    def _external_slots(self) -> int:
        return external_locked_worker_slots(self.db, self.cfg.queue_name, self.cfg,
                                            self.daemon_config, {job.locked_by for job in self.jobs})

    def _request_shutdown(self, signum: int, _frame: Any) -> None:
        self.logger.info("Received signal %s; scheduler shutdown requested.", signum)
        self.shutdown_requested = True

    def _write_state(self) -> None:
        snap = memory_snapshot()
        payload: dict[str, Any] = {
            "updated_at": utc_now_iso(),
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "config": asdict(self.cfg),
            "queue_counts": queue_counts(self.db, self.cfg.queue_name),
            "worker_slots": {
                "running": active_worker_slots(self.jobs),
                "paused": paused_worker_slots(self.jobs),
                "external_locked": self._external_slots(),
                "target": self.cfg.target_workers,
            },
            "jobs": [asdict(job) | {"rss_gb": process_tree_rss_gb(job.pid)} for job in self.jobs],
            "startup_recovery": self.startup_recovery,
        }
        if snap is not None:
            payload["memory"] = asdict(snap)
        path = PROJECT_ROOT / self.cfg.state_path
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(path)

    def _reap_jobs(self) -> None:
        kept: list[ManagedJob] = []
        for job in self.jobs:
            try:
                pid, status = os.waitpid(job.pid, os.WNOHANG)
            except ChildProcessError:
                pid, status = job.pid, 0
            if pid == 0:
                if job.queue_id:
                    row = self.db.connect().execute(
                        "SELECT status, payload_json FROM queues WHERE id = ?",
                        (job.queue_id,),
                    ).fetchone()
                    payload = json.loads(row["payload_json"]) if row else {}
                    from orchestrator.alpha_capacity_owner import owner_alive

                    if row and payload.get("kind") == "governed_alpha_assignment" and (row["status"] == "FAILED" or not owner_alive(payload)):
                        if row["status"] != "FAILED":
                            self.db.mark_queue_failed(job.queue_id, "Hermes execution lease owner disappeared")
                        try:
                            os.killpg(job.pgid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                kept.append(job)
                continue
            if os.WIFEXITED(status):
                job.returncode = os.WEXITSTATUS(status)
            elif os.WIFSIGNALED(status):
                job.returncode = -os.WTERMSIG(status)
            else:
                job.returncode = None
            job.status = "completed"
            job.completed_at = utc_now_iso()
            if job.queue_id:
                row = self.db.connect().execute("SELECT status, payload_json FROM queues WHERE id = ?", (job.queue_id,)).fetchone()
                if row and row["status"] == "LOCKED" and json.loads(row["payload_json"]).get("kind") == "governed_alpha_assignment":
                    self.db.mark_queue_failed(job.queue_id, "Capacity child exited without a terminal receipt")
            self.logger.info(
                "job exited: locked_by=%s queue_id=%s name=%s returncode=%s",
                job.locked_by,
                job.queue_id,
                job.name,
                job.returncode,
            )
        self.jobs = kept

    def _send_group(self, job: ManagedJob, sig: signal.Signals) -> None:
        os.killpg(job.pgid, sig)

    def _pause_one_job(self) -> bool:
        running = [job for job in self.jobs if job.status == "running"]
        if not running:
            return False
        # Pause the largest capacity consumer first; tie-break on lower priority.
        job = sorted(running, key=lambda j: (j.estimated_workers, -j.priority), reverse=True)[0]
        self._send_group(job, signal.SIGSTOP)
        job.status = "paused"
        job.paused_at = utc_now_iso()
        refresh_queue_lock(self.db, job.locked_by)
        job.last_lock_refresh_at = utc_now_iso()
        self.logger.warning(
            "paused job for memory pressure: locked_by=%s queue_id=%s name=%s estimated_workers=%s",
            job.locked_by,
            job.queue_id,
            job.name,
            job.estimated_workers,
        )
        return True

    def _resume_jobs_if_possible(self, snap: MemorySnapshot | None) -> None:
        if not should_resume_for_memory(snap, self.cfg):
            return
        for job in sorted([j for j in self.jobs if j.status == "paused"], key=lambda j: j.priority, reverse=True):
            external_slots = self._external_slots()
            if external_slots + active_worker_slots(self.jobs) + job.estimated_workers > min(self.cfg.target_workers, max(1, (os.cpu_count() or 1) - 2)):
                continue
            self._send_group(job, signal.SIGCONT)
            job.status = "running"
            job.paused_at = None
            refresh_queue_lock(self.db, job.locked_by)
            job.last_lock_refresh_at = utc_now_iso()
            self.logger.info("resumed job: locked_by=%s queue_id=%s name=%s", job.locked_by, job.queue_id, job.name)

    def _refresh_managed_locks(self) -> None:
        now = datetime.now(timezone.utc)
        for job in self.jobs:
            if job.status not in {"running", "paused"}:
                continue
            last = datetime.fromisoformat(job.last_lock_refresh_at) if job.last_lock_refresh_at else None
            if last is None or (now - last).total_seconds() >= self.cfg.stale_lock_refresh_seconds:
                refresh_queue_lock(self.db, job.locked_by)
                job.last_lock_refresh_at = utc_now_iso()

    def _select_launch_candidate(self) -> tuple[sqlite3.Row | None, dict[str, Any], int]:
        remaining_slots = self.cfg.target_workers - (
            active_worker_slots(self.jobs)
            + self._external_slots()
        )
        if remaining_slots <= 0:
            return None, {}, 0
        for row in pending_queue_candidates(self.db, self.cfg.queue_name):
            if row["id"] in {job.queue_id for job in self.jobs}:
                continue
            payload = json.loads(row["payload_json"] or "{}")
            estimated_workers = estimate_worker_slots(payload, self.cfg, self.daemon_config)
            if estimated_workers <= min(remaining_slots, self.cfg.max_workers_per_job):
                return row, payload, estimated_workers
        return None, {}, 0

    def _launch_next_if_capacity(self, snap: MemorySnapshot | None) -> bool:
        if len(self.jobs) >= self.cfg.max_concurrent_jobs:
            return False
        if snap is None:
            self.logger.warning("launch backpressure: memory telemetry unavailable")
            return False
        if snap.available_gb < self.cfg.min_free_ram_gb:
            self.logger.info(
                "launch backpressure: available_gb=%.2f below min_free_ram_gb=%.2f",
                snap.available_gb,
                self.cfg.min_free_ram_gb,
            )
            return False

        row, payload, estimated_workers = self._select_launch_candidate()
        if row is None:
            return False
        if estimated_workers > max(1, (os.cpu_count() or 1) - 2):
            return False
        if os.getloadavg()[0] + estimated_workers > max(1, (os.cpu_count() or 1) - 2):
            self.logger.info("launch backpressure: host CPU load leaves insufficient headroom")
            return False
        reserved_unrealized = sum(
            max(0.0, job.estimated_workers * self.cfg.estimated_worker_ram_gb
                - (process_tree_rss_gb(job.pid) or 0.0)) for job in self.jobs
        )
        if snap.available_gb < self.cfg.min_free_ram_gb + reserved_unrealized + estimated_workers * self.cfg.estimated_worker_ram_gb:
            self.logger.info("launch backpressure: insufficient conservative RAM reservation")
            return False

        external_slots = self._external_slots()
        if external_slots + active_worker_slots(self.jobs) + estimated_workers > min(self.cfg.target_workers, max(1, (os.cpu_count() or 1) - 2)):
            return False

        priority = int(row["priority"] or 0)
        locked_by = f"capacity:{socket.gethostname()}:{uuid4().hex[:8]}"
        cmd = [
            sys.executable,
            str(PROJECT_ROOT / "orchestrator" / "research_daemon.py"),
            "--db",
            str(self.db_path),
            "--config",
            str(self.daemon_config_path),
            "--once",
            "--locked-by",
            locked_by,
            "--queue-id",
            str(row["id"]),
        ]
        if payload.get("kind") == "governed_alpha_assignment":
            cmd = [sys.executable, str(PROJECT_ROOT / "scripts/run_alpha_capacity_job.py"),
                   "--db", str(self.db_path), "--queue-id", str(row["id"]),
                   "--locked-by", locked_by, "--queue-name", self.cfg.queue_name]
        child_dir = PROJECT_ROOT / self.cfg.child_log_dir
        child_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = child_dir / f"{locked_by.replace(':', '_')}.stdout.log"
        stderr_path = child_dir / f"{locked_by.replace(':', '_')}.stderr.log"
        name = str(payload.get("name") or "")

        self.logger.info(
            "launching job: queue_id=%s name=%s priority=%s estimated_workers=%s cmd=%s",
            row["id"],
            name,
            priority,
            estimated_workers,
            " ".join(cmd),
        )
        if self.dry_run:
            return False
        stdout = stdout_path.open("ab")
        stderr = stderr_path.open("ab")
        proc = subprocess.Popen(
            cmd,
            cwd=PROJECT_ROOT,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
            env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONDONTWRITEBYTECODE": "1"},
        )
        stdout.close()
        stderr.close()
        queue_id: str | None = str(row["id"])
        locked_name: str | None = name
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            claimed_id, claimed_name = find_locked_queue_item(self.db, locked_by)
            if claimed_id:
                queue_id, locked_name = claimed_id, claimed_name
                break
            if proc.poll() is not None:
                break
            time.sleep(0.25)
        self.jobs.append(
            ManagedJob(
                locked_by=locked_by,
                pid=proc.pid,
                pgid=proc.pid,
                queue_id=queue_id,
                name=locked_name or name,
                estimated_workers=estimated_workers,
                priority=priority,
                status="running",
                launched_at=utc_now_iso(),
                stdout_log=str(stdout_path),
                stderr_log=str(stderr_path),
                last_lock_refresh_at=utc_now_iso(),
            )
        )
        return True

    def tick(self) -> None:
        self._reap_jobs()
        snap = memory_snapshot()
        if should_pause_for_memory(snap, self.cfg):
            while should_pause_for_memory(snap, self.cfg) and self._pause_one_job():
                time.sleep(1.0)
                snap = memory_snapshot()
        self._refresh_managed_locks()
        self._resume_jobs_if_possible(snap)
        launched = True
        while launched and not should_pause_for_memory(snap, self.cfg):
            launched = self._launch_next_if_capacity(snap)
            snap = memory_snapshot()
        self._write_state()

    def run(self, *, once: bool = False) -> int:
        self.logger.info("capacity scheduler started config=%s", asdict(self.cfg))
        try:
            while not self.shutdown_requested:
                self.tick()
                if once:
                    break
                time.sleep(self.cfg.poll_seconds)
        finally:
            if self.shutdown_requested:
                for job in self.jobs:
                    try:
                        if job.status == "paused":
                            self._send_group(job, signal.SIGCONT)
                        self._send_group(job, signal.SIGTERM)
                    except Exception as exc:
                        self.logger.warning("failed to terminate job locked_by=%s: %s", job.locked_by, exc)
            self._write_state()
            self.db.close()
        return 0


def main() -> int:
    args = parse_args()
    daemon_config_path = Path(args.config)
    daemon_config = load_daemon_config(daemon_config_path)
    cfg = load_capacity_config(daemon_config, args)
    scheduler = CapacityScheduler(
        db_path=Path(args.db),
        daemon_config_path=daemon_config_path,
        daemon_config=daemon_config,
        cfg=cfg,
        dry_run=bool(args.dry_run),
    )
    return scheduler.run(once=bool(args.once))


if __name__ == "__main__":
    raise SystemExit(main())
