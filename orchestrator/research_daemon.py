#!/usr/bin/env python3
"""24/7 research queue runner daemon.

Example:
    python orchestrator/research_daemon.py \
      --db research_db/research.sqlite \
      --config orchestrator/daemon_config.yaml
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
from typing import Any
from uuid import uuid4

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from orchestrator.db import ResearchDB

REQUIRED_PAYLOAD_KEYS = {"hypothesis", "name"}


class GracefulShutdown:
    def __init__(self) -> None:
        self.stop_requested = False

    def request_stop(self, signum: int, _frame: Any) -> None:
        self.stop_requested = True
        logging.getLogger("research_daemon").info("Received signal %s; shutdown requested.", signum)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run continuous research queue daemon.")
    parser.add_argument("--db", default="research_db/research.sqlite")
    parser.add_argument("--config", default="orchestrator/daemon_config.yaml")
    parser.add_argument("--once", action="store_true", default=False)
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--queue-name", default=None)
    parser.add_argument("--poll-interval", type=int, default=None)
    parser.add_argument("--locked-by", default=None)
    parser.add_argument("--max-workers", type=int, default=None)
    return parser.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Daemon config not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError("Daemon config must be a YAML mapping/object")
    return data


def configure_logging(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("research_daemon")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logger.propagate = False
    return logger


def build_locked_by(config: dict[str, Any], cli_locked_by: str | None) -> str:
    if cli_locked_by:
        return cli_locked_by
    prefix = str(config.get("locked_by_prefix", "research-daemon"))
    return f"{prefix}:{socket.gethostname()}:{uuid4().hex[:8]}"


def merge_payload_with_defaults(payload: dict[str, Any], config: dict[str, Any], cli_max_workers: int | None) -> dict[str, Any]:
    merged: dict[str, Any] = {
        "hypothesis": payload.get("hypothesis"),
        "name": payload.get("name"),
        "phase": payload.get("phase", config.get("default_phase", "tier2")),
        "max_workers": payload.get("max_workers", cli_max_workers if cli_max_workers is not None else config.get("default_max_workers", 6)),
        "config": payload.get("config", config.get("default_config", "configs/engine.yaml")),
        "local_config": payload.get("local_config", config.get("default_local_config", "configs/local/engine.lab.yaml")),
        "stable_data": payload.get("stable_data", config.get("stable_data")),
        "vol_data": payload.get("vol_data", config.get("vol_data")),
        "outputs_root": payload.get("outputs_root", config.get("outputs_root", "outputs")),
        "retain_top_n": payload.get("retain_top_n", config.get("retain_top_n", 2)),
        "retain_median": payload.get("retain_median", config.get("retain_median", 1)),
        "retain_worst": payload.get("retain_worst", config.get("retain_worst", 1)),
        "cleanup_delete_logs": payload.get("cleanup_delete_logs", config.get("cleanup_delete_logs", True)),
        "cleanup_delete_nonretained_runs": payload.get(
            "cleanup_delete_nonretained_runs",
            config.get("cleanup_delete_nonretained_runs", True),
        ),
    }

    missing = [key for key in REQUIRED_PAYLOAD_KEYS if not merged.get(key)]
    if missing:
        raise ValueError(f"Queue payload missing required keys: {missing}")
    return merged


def build_pipeline_command(db_path: Path, merged_payload: dict[str, Any]) -> list[str]:
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "orchestrator" / "run_experiment_pipeline.py"),
        "--hypothesis",
        str(merged_payload["hypothesis"]),
        "--name",
        str(merged_payload["name"]),
        "--phase",
        str(merged_payload["phase"]),
        "--max-workers",
        str(merged_payload["max_workers"]),
        "--config",
        str(merged_payload["config"]),
        "--local-config",
        str(merged_payload["local_config"]),
        "--stable-data",
        str(merged_payload["stable_data"]),
        "--vol-data",
        str(merged_payload["vol_data"]),
        "--outputs-root",
        str(merged_payload["outputs_root"]),
        "--retain-top-n",
        str(merged_payload["retain_top_n"]),
        "--retain-median",
        str(merged_payload["retain_median"]),
        "--retain-worst",
        str(merged_payload["retain_worst"]),
        "--research-db",
        str(db_path),
    ]

    if not bool(merged_payload["cleanup_delete_logs"]):
        cmd.append("--no-cleanup-delete-logs")
    if not bool(merged_payload["cleanup_delete_nonretained_runs"]):
        cmd.append("--no-cleanup-delete-nonretained-runs")
    return cmd


def run_pipeline_command(cmd: list[str], logger: logging.Logger) -> int:
    logger.info("Pipeline command: %s", " ".join(cmd))
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert process.stdout is not None
    for line in process.stdout:
        logger.info("[pipeline] %s", line.rstrip("\n"))
    return process.wait()


def run_logged_command(cmd: list[str], logger: logging.Logger, prefix: str) -> int:
    logger.info("%s command: %s", prefix, " ".join(cmd))
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert process.stdout is not None
    for line in process.stdout:
        logger.info("[%s] %s", prefix.lower(), line.rstrip("\n"))
    return process.wait()


def write_heartbeat(
    heartbeat_path: Path,
    *,
    daemon_id: str,
    started_at: str,
    current_queue_id: str | None,
    current_job_name: str | None,
    status: str,
) -> None:
    heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "daemon_id": daemon_id,
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "started_at": started_at,
        "last_heartbeat_at": utc_now_iso(),
        "current_queue_id": current_queue_id,
        "current_job_name": current_job_name,
        "status": status,
    }
    heartbeat_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def build_interpret_command(db_path: Path, merged_payload: dict[str, Any], config: dict[str, Any]) -> list[str]:
    name = str(merged_payload["name"])
    outputs_root = str(merged_payload["outputs_root"])
    model = str(config.get("default_interpreter_model", "gpt-5.4-mini"))
    output_dir = str(config.get("verdict_output_dir", "research/verdicts"))
    return [
        sys.executable,
        str(PROJECT_ROOT / "orchestrator" / "interpret_experiment_results.py"),
        "--db",
        str(db_path),
        "--name",
        name,
        "--hypothesis",
        str(merged_payload["hypothesis"]),
        "--stable-root",
        str(Path(outputs_root) / f"{name}_parallel_stable"),
        "--vol-root",
        str(Path(outputs_root) / f"{name}_parallel_vol"),
        "--model",
        model,
        "--output-dir",
        output_dir,
    ]

def build_state_discovery_command(db_path: Path, merged_payload: dict[str, Any], config: dict[str, Any]) -> list[str]:
    name = str(merged_payload["name"])
    outputs_root = str(merged_payload["outputs_root"])
    out_dir = str(config.get("state_discovery_output_dir", "research/state_findings"))
    return [
        sys.executable,
        str(PROJECT_ROOT / "orchestrator" / "state_discovery.py"),
        "--db",
        str(db_path),
        "--experiment-root",
        str(Path(outputs_root) / f"{name}_parallel_stable"),
        "--name",
        name,
        "--output-dir",
        out_dir,
    ]


def main() -> int:
    args = parse_args()
    config = load_config(Path(args.config))

    queue_name = args.queue_name or str(config.get("queue_name", "approved_backtests"))
    poll_interval = int(args.poll_interval if args.poll_interval is not None else config.get("poll_interval_seconds", 60))
    max_job_attempts = int(config.get("max_job_attempts", 2))
    stale_lock_minutes = int(config.get("stale_lock_minutes", 720))
    locked_by = build_locked_by(config, args.locked_by)

    log_path = Path(config.get("log_path", "logs/research_daemon.log"))
    heartbeat_path = Path(config.get("heartbeat_path", "logs/research_daemon_heartbeat.json"))
    logger = configure_logging(log_path)

    shutdown = GracefulShutdown()
    signal.signal(signal.SIGINT, shutdown.request_stop)
    signal.signal(signal.SIGTERM, shutdown.request_stop)

    daemon_id = uuid4().hex
    started_at = utc_now_iso()
    current_queue_id: str | None = None
    current_job_name: str | None = None

    db = ResearchDB(args.db, repo_root=PROJECT_ROOT)
    db.init_schema()

    logger.info("Daemon started: id=%s", daemon_id)
    logger.info("Database path: %s", args.db)
    logger.info("Config path: %s", args.config)
    logger.info("Queue name: %s", queue_name)
    logger.info("Locked by: %s", locked_by)

    while not shutdown.stop_requested:
        try:
            stale_result = db.release_stale_locks(queue_name, stale_lock_minutes, max_job_attempts)
            if stale_result["requeued"] or stale_result["failed"]:
                logger.info("Released stale locks: %s", stale_result)

            write_heartbeat(
                heartbeat_path,
                daemon_id=daemon_id,
                started_at=started_at,
                current_queue_id=current_queue_id,
                current_job_name=current_job_name,
                status="idle",
            )

            if args.dry_run:
                row = db.peek_next_pending(queue_name)
                if row is None:
                    logger.info("No pending jobs (dry-run mode).")
                    if args.once:
                        break
                    time.sleep(poll_interval)
                    continue
                payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
                merged_payload = merge_payload_with_defaults(payload, config, args.max_workers)
                cmd = build_pipeline_command(Path(args.db), merged_payload)
                logger.info("[dry-run] Next queue item id=%s name=%s", row["id"], merged_payload["name"])
                logger.info("[dry-run] Command: %s", " ".join(cmd))
                break

            row = db.dequeue_next(queue_name, locked_by)
            if row is None:
                logger.info("No pending jobs; sleeping %ss", poll_interval)
                if args.once:
                    break
                time.sleep(poll_interval)
                continue

            current_queue_id = str(row["id"])
            payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
            merged_payload = merge_payload_with_defaults(payload, config, args.max_workers)
            current_job_name = str(merged_payload["name"])
            logger.info("Locked queue item id=%s item_id=%s attempts=%s", row["id"], row["item_id"], row["attempts"])

            write_heartbeat(
                heartbeat_path,
                daemon_id=daemon_id,
                started_at=started_at,
                current_queue_id=current_queue_id,
                current_job_name=current_job_name,
                status="running",
            )

            cmd = build_pipeline_command(Path(args.db), merged_payload)
            return_code = run_pipeline_command(cmd, logger)
            if return_code == 0:
                db.mark_queue_done(current_queue_id)
                logger.info("Queue item DONE id=%s name=%s", current_queue_id, current_job_name)
                if bool(config.get("run_interpretation_after_pipeline", True)):
                    try:
                        interp_cmd = build_interpret_command(Path(args.db), merged_payload, config)
                        interp_code = run_logged_command(interp_cmd, logger, "INTERPRETER")
                        if interp_code != 0:
                            logger.error("Interpreter failed for name=%s with return code=%s", current_job_name, interp_code)
                    except Exception as interp_exc:
                        logger.exception("Interpreter exception for name=%s: %s", current_job_name, interp_exc)
                if bool(config.get("run_state_discovery_after_interpretation", False)):
                    try:
                        sd_cmd = build_state_discovery_command(Path(args.db), merged_payload, config)
                        sd_code = run_logged_command(sd_cmd, logger, "STATE_DISCOVERY")
                        if sd_code != 0:
                            logger.error("State discovery failed for name=%s with return code=%s", current_job_name, sd_code)
                    except Exception as sd_exc:
                        logger.exception("State discovery exception for name=%s: %s", current_job_name, sd_exc)
            else:
                error = f"pipeline failed with return code {return_code}"
                db.mark_queue_failed(current_queue_id, error)
                logger.error("Queue item FAILED id=%s name=%s error=%s", current_queue_id, current_job_name, error)

            current_queue_id = None
            current_job_name = None

            if args.once:
                break
        except Exception as exc:
            logger.exception("Daemon loop error: %s", exc)
            if current_queue_id is not None:
                db.mark_queue_failed(current_queue_id, str(exc))
                logger.error("Queue item FAILED id=%s error=%s", current_queue_id, exc)
                current_queue_id = None
                current_job_name = None
            if args.once:
                break
            time.sleep(poll_interval)

    write_heartbeat(
        heartbeat_path,
        daemon_id=daemon_id,
        started_at=started_at,
        current_queue_id=current_queue_id,
        current_job_name=current_job_name,
        status="shutting_down",
    )
    logger.info("Graceful shutdown complete.")
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
