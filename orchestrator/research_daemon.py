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
import gc
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
import shlex

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from orchestrator.db import ResearchDB
from orchestrator.process_logging import CommandExecutionError, run_logged_command
from bt.experiments.resource_controls import memory_snapshot
from bt.paths import (
    resolve_daemon_command_log_dir,
    resolve_existing_experiment_root,
    resolve_pipeline_log_path,
    resolve_phase_artifact_dir,
    resolve_verdict_bundle_root,
)
from orchestrator.research_terminal.cards import build_and_write_intelligence_cards

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
    data_mode = str(payload.get("data_mode", config.get("data_mode", "research_panel")))
    if data_mode not in {"research_panel", "legacy_curated"}:
        raise ValueError(f"Unsupported daemon data_mode: {data_mode}")
    stable_data = payload.get("stable_data", config.get("stable_data")) if data_mode == "legacy_curated" else None
    vol_data = payload.get("vol_data", config.get("vol_data")) if data_mode == "legacy_curated" else None
    merged: dict[str, Any] = {
        "hypothesis": payload.get("hypothesis"),
        "name": payload.get("name"),
        "phase": payload.get("phase", config.get("default_phase", "tier2")),
        "max_workers": payload.get("max_workers", cli_max_workers if cli_max_workers is not None else config.get("default_max_workers", 6)),
        "volatile_max_workers": payload.get("volatile_max_workers", config.get("volatile_max_workers")),
        "config": payload.get("config", config.get("default_config", "configs/engine.yaml")),
        "local_config": payload.get("local_config", config.get("default_local_config", "configs/local/engine.lab.yaml")),
        "data_mode": data_mode,
        "stable_data": stable_data,
        "vol_data": vol_data,
        "data_root": payload.get("data_root", config.get("data_root", "research_data")),
        "data_kind": payload.get("data_kind", config.get("data_kind", "research_panel")),
        "exchange": payload.get("exchange", config.get("exchange", "binance")),
        "timeframe": payload.get("timeframe", config.get("timeframe", "1m")),
        "stable_manifest": payload.get(
            "stable_manifest",
            config.get("stable_manifest", "research_data/manifests/stable_universe.parquet"),
        ),
        "membership_path": payload.get(
            "membership_path",
            config.get("membership_path", "research_data/manifests/volatile_universe_membership.parquet"),
        ),
        "max_workers_auto": payload.get("max_workers_auto", config.get("runner_max_workers_auto", False)),
        "reserve_ram_gb": payload.get("reserve_ram_gb", config.get("runner_reserve_ram_gb", 8)),
        "max_ram_per_worker_gb": payload.get("max_ram_per_worker_gb", config.get("runner_max_ram_per_worker_gb")),
        "min_free_ram_gb": payload.get("min_free_ram_gb", config.get("runner_min_free_ram_gb", 6)),
        "run_timeout_seconds": payload.get("run_timeout_seconds", config.get("runner_run_timeout_seconds")),
        "fail_fast": payload.get("fail_fast", config.get("runner_fail_fast", False)),
        "resume_strict": payload.get("resume_strict", config.get("runner_resume_strict", True)),
        "parallel_datasets": payload.get("parallel_datasets", config.get("runner_parallel_datasets", False)),
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
        "--data-root",
        str(merged_payload["data_root"]),
        "--data-kind",
        str(merged_payload["data_kind"]),
        "--exchange",
        str(merged_payload["exchange"]),
        "--timeframe",
        str(merged_payload["timeframe"]),
        "--stable-manifest",
        str(merged_payload["stable_manifest"]),
        "--membership-path",
        str(merged_payload["membership_path"]),
        "--reserve-ram-gb",
        str(merged_payload["reserve_ram_gb"]),
        "--min-free-ram-gb",
        str(merged_payload["min_free_ram_gb"]),
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
    if merged_payload.get("volatile_max_workers") is not None:
        cmd.extend(["--volatile-max-workers", str(merged_payload["volatile_max_workers"])])
    if bool(merged_payload.get("max_workers_auto")):
        cmd.append("--max-workers-auto")
    if merged_payload.get("max_ram_per_worker_gb") is not None:
        cmd.extend(["--max-ram-per-worker-gb", str(merged_payload["max_ram_per_worker_gb"])])
    if merged_payload.get("run_timeout_seconds") is not None:
        cmd.extend(["--run-timeout-seconds", str(merged_payload["run_timeout_seconds"])])
    if bool(merged_payload.get("fail_fast")):
        cmd.append("--fail-fast")
    if not bool(merged_payload.get("resume_strict", True)):
        cmd.append("--no-resume-strict")
    if bool(merged_payload.get("parallel_datasets")):
        cmd.append("--parallel-datasets")
    if merged_payload.get("stable_data"):
        cmd.extend(["--stable-data", str(merged_payload["stable_data"])])
    if merged_payload.get("vol_data"):
        cmd.extend(["--vol-data", str(merged_payload["vol_data"])])
    if not bool(merged_payload["cleanup_delete_logs"]):
        cmd.append("--no-cleanup-delete-logs")
    if not bool(merged_payload["cleanup_delete_nonretained_runs"]):
        cmd.append("--no-cleanup-delete-nonretained-runs")
    return cmd


def _daemon_command_log_dir(queue_id: str, name: str, outputs_root: str, phase: str = "tier2") -> Path:
    return resolve_daemon_command_log_dir(outputs_root=outputs_root, phase=phase, experiment_name=name)


def fetch_latest_pipeline_failure(db: ResearchDB, *, name: str) -> dict[str, Any] | None:
    row = db.connect().execute(
        """
        SELECT error_message, log_path, completed_at
        FROM pipeline_runs
        WHERE name = ?
        ORDER BY completed_at DESC
        LIMIT 1
        """,
        (name,),
    ).fetchone()
    if row is None:
        return None
    return {"error_message": row["error_message"], "log_path": row["log_path"], "completed_at": row["completed_at"]}


def _log_daemon_failure(logger: logging.Logger, *, stage: str, queue_id: str, name: str, exc: CommandExecutionError) -> None:
    r = exc.result
    logger.error(
        "\n%s\nDAEMON STAGE FAILED\nStage: %s\nQueue ID: %s\nJob name: %s\nExit code: %s\nCommand:\n  %s\nSTDOUT log:\n  %s\nSTDERR log:\n  %s\n\nSTDOUT tail:\n%s\n\nSTDERR tail:\n%s\n\nRoot-cause hint:\n  %s\n%s",
        "=" * 80, stage, queue_id, name, r.returncode, shlex.join(r.cmd), r.stdout_log, r.stderr_log, r.stdout_tail, r.stderr_tail, r.root_cause_hint or "No automatic hint detected.", "=" * 80,
    )


def _run_daemon_stage(*, cmd: list[str], stage: str, logger: logging.Logger, queue_id: str, name: str, command_log_dir: Path, db: ResearchDB | None = None, artifact_types: tuple[str, str] | None = None) -> tuple[int, dict[str, str] | None]:
    try:
        result = run_logged_command(stage=stage, cmd=cmd, log_dir=command_log_dir, logger=logger, queue_id=queue_id, job_name=name, cwd=PROJECT_ROOT)
        artifacts = {"stdout": result.stdout_log, "stderr": result.stderr_log}
        if db and artifact_types:
            db.register_artifact(artifact_type=artifact_types[0], path=Path(result.stdout_log), description=f"daemon {stage} stdout")
            db.register_artifact(artifact_type=artifact_types[1], path=Path(result.stderr_log), description=f"daemon {stage} stderr")
        return 0, artifacts
    except CommandExecutionError as exc:
        _log_daemon_failure(logger, stage=stage, queue_id=queue_id, name=name, exc=exc)
        r = exc.result
        return r.returncode, {"stdout": r.stdout_log, "stderr": r.stderr_log, "root_cause_hint": r.root_cause_hint or ""}


def write_heartbeat(
    heartbeat_path: Path,
    *,
    daemon_id: str,
    started_at: str,
    current_queue_id: str | None,
    current_job_name: str | None,
    current_stage: str | None = None,
    status: str,
    queue_counts: dict[str, int] | None = None,
) -> None:
    heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
    mem = memory_snapshot()
    payload = {
        "daemon_id": daemon_id,
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "started_at": started_at,
        "last_heartbeat_at": utc_now_iso(),
        "uptime_seconds": max(0.0, (datetime.now(timezone.utc) - datetime.fromisoformat(started_at)).total_seconds()),
        "current_queue_id": current_queue_id,
        "current_job_name": current_job_name,
        "current_stage": current_stage,
        "status": status,
        "queue_counts": queue_counts or {},
    }
    if mem is not None:
        payload["memory"] = {
            "available_gb": round(mem.available_gb, 3),
            "used_gb": round(mem.used_gb, 3),
            "total_gb": round(mem.total_gb, 3),
            "source": mem.source,
        }
    heartbeat_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def queue_counts(db: ResearchDB, queue_name: str) -> dict[str, int]:
    rows = db.connect().execute(
        "SELECT status, COUNT(*) AS n FROM queues WHERE queue_name = ? GROUP BY status",
        (queue_name,),
    ).fetchall()
    return {str(row["status"]): int(row["n"]) for row in rows}


def build_interpret_command(db_path: Path, merged_payload: dict[str, Any], config: dict[str, Any]) -> list[str]:
    name = str(merged_payload["name"])
    outputs_root = str(merged_payload["outputs_root"])
    phase = str(merged_payload.get("phase", "tier2"))
    stable_root = resolve_existing_experiment_root(
        outputs_root=outputs_root,
        phase=phase,
        experiment_name=name,
        variant="stable",
    )
    vol_root = resolve_existing_experiment_root(
        outputs_root=outputs_root,
        phase=phase,
        experiment_name=name,
        variant="vol",
    )
    model = str(config.get("default_interpreter_model", "qwen2.5:14b"))
    llm_provider = str(config.get("default_llm_provider", "ollama"))
    ollama_url = str(config.get("default_ollama_url", "http://127.0.0.1:11434/api/generate"))
    llm_timeout = int(config.get("default_llm_timeout_seconds", 600))
    num_ctx = int(config.get("default_num_ctx", 8192))
    temperature = float(config.get("default_temperature", 0.1))
    output_dir = str(resolve_phase_artifact_dir(artifact_root=config.get("verdict_output_dir", "research/verdicts"), phase=phase))
    state_discovery_dir = str(resolve_phase_artifact_dir(artifact_root=config.get("state_discovery_output_dir", "research/state_findings"), phase=phase))
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "orchestrator" / "interpret_experiment_results.py"),
        "--db",
        str(db_path),
        "--name",
        name,
        "--hypothesis",
        str(merged_payload["hypothesis"]),
        "--stable-root",
        str(stable_root),
        "--vol-root",
        str(vol_root),
        "--llm-provider",
        llm_provider,
        "--model",
        model,
        "--ollama-url",
        ollama_url,
        "--llm-timeout-seconds",
        str(llm_timeout),
        "--num-ctx",
        str(num_ctx),
        "--temperature",
        str(temperature),
        "--output-dir",
        output_dir,
        "--phase",
        phase,
    ]
    if bool(config.get("include_state_discovery_in_verdict", True)):
        cmd.extend(["--state-discovery-dir", state_discovery_dir])
    return cmd

def build_state_discovery_command(
    db_path: Path,
    merged_payload: dict[str, Any],
    config: dict[str, Any],
    *,
    mode: str,
) -> list[str]:
    name = str(merged_payload["name"])
    outputs_root = Path(str(merged_payload["outputs_root"]))
    phase = str(merged_payload.get("phase", "tier2"))
    out_dir = str(resolve_phase_artifact_dir(artifact_root=config.get("state_discovery_output_dir", "research/state_findings"), phase=phase))
    min_trades = int(config.get("state_discovery_min_trades", 30))
    min_bucket = int(config.get("state_discovery_min_bucket_trades", 10))
    top_n = int(config.get("state_discovery_top_n", 25))
    write_db = bool(config.get("state_discovery_write_db", True))

    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "orchestrator" / "state_discovery.py"),
        "--db",
        str(db_path),
        "--output-dir",
        out_dir,
        "--phase",
        phase,
        "--min-trades",
        str(min_trades),
        "--min-bucket-trades",
        str(min_bucket),
        "--top-n",
        str(top_n),
    ]

    if mode == "stable":
        cmd.extend([
            "--experiment-root",
            str(resolve_existing_experiment_root(outputs_root=outputs_root, phase=phase, experiment_name=name, variant="stable")),
            "--name",
            f"{name}_stable",
        ])
    elif mode == "vol":
        cmd.extend([
            "--experiment-root",
            str(resolve_existing_experiment_root(outputs_root=outputs_root, phase=phase, experiment_name=name, variant="vol")),
            "--name",
            f"{name}_vol",
        ])
    elif mode == "combined":
        cmd.extend([
            "--stable-root", str(resolve_existing_experiment_root(outputs_root=outputs_root, phase=phase, experiment_name=name, variant="stable")),
            "--vol-root", str(resolve_existing_experiment_root(outputs_root=outputs_root, phase=phase, experiment_name=name, variant="vol")),
            "--name", f"{name}_combined",
        ])
    else:
        raise ValueError(f"Unsupported state discovery mode: {mode}")

    if write_db:
        cmd.append("--write-db")
    return cmd


def build_research_memory_command(db_path: Path, merged_payload: dict[str, Any], config: dict[str, Any]) -> list[str]:
    phase = str(merged_payload.get("phase", "tier2"))
    outputs_root = Path(str(merged_payload.get("outputs_root", config.get("outputs_root", "outputs"))))
    name = str(merged_payload.get("name", "")).strip()
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "orchestrator" / "research_memory.py"),
        "--db",
        str(db_path),
        "--outputs-root",
        str(outputs_root),
        "--verdicts-dir",
        str(resolve_phase_artifact_dir(artifact_root=config.get("verdict_output_dir", "research/verdicts"), phase=phase)),
        "--state-findings-dir",
        str(resolve_phase_artifact_dir(artifact_root=config.get("state_discovery_output_dir", "research/state_findings"), phase=phase)),
        "--alpha-zoo-dir",
        str(resolve_phase_artifact_dir(artifact_root=config.get("alpha_zoo_dir", "research/alpha_zoo"), phase=phase)),
        "--output-dir",
        str(resolve_phase_artifact_dir(artifact_root=config.get("research_memory_output_dir", "research/memory"), phase=phase)),
        "--write-db",
    ]
    if name:
        for variant in ("stable", "vol"):
            path = resolve_existing_experiment_root(outputs_root=outputs_root, phase=phase, experiment_name=name, variant=variant)
            if path.exists():
                cmd.extend(["--experiment-root", str(path)])
    return cmd


def refresh_terminal_cards(
    *,
    db: ResearchDB,
    merged_payload: dict[str, Any],
    command_log_dir: Path,
) -> Path:
    outputs_root = Path(str(merged_payload.get("outputs_root", "outputs")))
    phase = str(merged_payload.get("phase", "tier2"))
    name = str(merged_payload["name"])
    result = build_and_write_intelligence_cards(
        name=name,
        phase=phase,
        hypothesis_path=Path(str(merged_payload["hypothesis"])),
        stable_root=resolve_existing_experiment_root(
            outputs_root=outputs_root, phase=phase, experiment_name=name, variant="stable"
        ),
        volatile_root=resolve_existing_experiment_root(
            outputs_root=outputs_root, phase=phase, experiment_name=name, variant="vol"
        ),
        output_dir=outputs_root / phase / f"{name}_strategy_terminal_cards",
        project_root=PROJECT_ROOT,
        verdict_bundle_dir=resolve_verdict_bundle_root(
            outputs_root=outputs_root, phase=phase, experiment_name=name
        ),
        command_log_dir=command_log_dir,
        pipeline_log_path=resolve_pipeline_log_path(
            outputs_root=outputs_root, phase=phase, experiment_name=name
        ),
        db=db,
    )
    return result.bundle_json


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
                current_stage=None,
                status="idle",
                queue_counts=queue_counts(db, queue_name),
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
                current_stage="001_pipeline",
                status="running",
                queue_counts=queue_counts(db, queue_name),
            )

            cmd = build_pipeline_command(Path(args.db), merged_payload)
            command_log_dir = _daemon_command_log_dir(
                current_queue_id,
                current_job_name,
                str(merged_payload.get("outputs_root", "outputs")),
                str(merged_payload.get("phase", "tier2")),
            )
            post_agent_warnings: list[dict[str, Any]] = []
            return_code, _ = _run_daemon_stage(
                cmd=cmd,
                stage="001_pipeline",
                logger=logger,
                queue_id=current_queue_id,
                name=current_job_name,
                command_log_dir=command_log_dir,
                db=db,
                artifact_types=("daemon_pipeline_stdout_log", "daemon_pipeline_stderr_log"),
            )
            if return_code == 0:
                run_state_discovery = bool(config.get("run_state_discovery_after_pipeline", True))
                if run_state_discovery:
                    sd_cmd = build_state_discovery_command(Path(args.db), merged_payload, config, mode="combined")
                    sd_code, sd_art = _run_daemon_stage(cmd=sd_cmd, stage="002_state_discovery", logger=logger, queue_id=current_queue_id, name=current_job_name, command_log_dir=command_log_dir, db=db, artifact_types=("daemon_state_discovery_stdout_log", "daemon_state_discovery_stderr_log"))
                    if sd_code != 0:
                        if sd_art:
                            db.register_artifact(artifact_type="state_discovery_error", path=Path(sd_art.get("stderr") or sd_art.get("stdout") or "logs/research_daemon.log"), description="state discovery error", metadata={"queue_id": current_queue_id, "name": current_job_name, "returncode": sd_code, "root_cause_hint": sd_art.get("root_cause_hint"), "stdout_log": sd_art.get("stdout"), "stderr_log": sd_art.get("stderr")})
                        raise RuntimeError(f"required state discovery stage failed with return code {sd_code}")

                run_interp = bool(config.get("run_interpretation_after_pipeline", True))
                interp_after_sd = bool(config.get("interpretation_after_state_discovery", True))
                if run_interp and (interp_after_sd or not run_state_discovery):
                    try:
                        interp_cmd = build_interpret_command(Path(args.db), merged_payload, config)
                        interp_code, interp_art = _run_daemon_stage(cmd=interp_cmd, stage="003_interpreter", logger=logger, queue_id=current_queue_id, name=current_job_name, command_log_dir=command_log_dir, db=db, artifact_types=("daemon_interpreter_stdout_log", "daemon_interpreter_stderr_log"))
                        if interp_code != 0:
                            if interp_art:
                                db.register_artifact(artifact_type="interpreter_error", path=Path(interp_art.get("stderr") or interp_art.get("stdout") or "logs/research_daemon.log"), description="interpreter error", metadata={"queue_id": current_queue_id, "name": current_job_name, "returncode": interp_code, "root_cause_hint": interp_art.get("root_cause_hint"), "stdout_log": interp_art.get("stdout"), "stderr_log": interp_art.get("stderr")})
                            raise RuntimeError(f"required interpreter stage failed with return code {interp_code}")
                    except Exception as interp_exc:
                        logger.exception("Interpreter exception for name=%s: %s", current_job_name, interp_exc)
                        raise
                run_memory = bool(config.get("run_research_memory_after_pipeline", True))
                if run_memory:
                    mem_cmd = build_research_memory_command(Path(args.db), merged_payload, config)
                    mem_code, mem_art = _run_daemon_stage(cmd=mem_cmd, stage="004_research_memory", logger=logger, queue_id=current_queue_id, name=current_job_name, command_log_dir=command_log_dir, db=db, artifact_types=("daemon_research_memory_stdout_log", "daemon_research_memory_stderr_log"))
                    if mem_code != 0:
                        if mem_art:
                            db.register_artifact(artifact_type="research_memory_error", path=Path(mem_art.get("stderr") or mem_art.get("stdout") or "logs/research_daemon.log"), description="research memory error", metadata={"queue_id": current_queue_id, "name": current_job_name, "returncode": mem_code, "root_cause_hint": mem_art.get("root_cause_hint"), "stdout_log": mem_art.get("stdout"), "stderr_log": mem_art.get("stderr")})
                        raise RuntimeError(f"required research memory stage failed with return code {mem_code}")

                cards_path = refresh_terminal_cards(
                    db=db,
                    merged_payload=merged_payload,
                    command_log_dir=command_log_dir,
                )
                logger.info("Refreshed final Strategy Research Terminal cards: %s", cards_path)
                db.mark_queue_done(current_queue_id)
                logger.info("Queue item DONE id=%s name=%s after all required research stages", current_queue_id, current_job_name)
            else:
                failure_info = fetch_latest_pipeline_failure(db, name=current_job_name or "")
                compact = failure_info["error_message"] if failure_info and failure_info.get("error_message") else f"pipeline failed with return code {return_code}"
                log_path_hint = failure_info.get("log_path") if failure_info else None
                error = f"{compact}; return_code={return_code}; pipeline_log={log_path_hint}"
                db.mark_queue_failed(current_queue_id, error)
                logger.error(
                    "Queue item FAILED id=%s name=%s return_code=%s pipeline_log=%s error=%s",
                    current_queue_id, current_job_name, return_code, log_path_hint, compact
                )

            current_queue_id = None
            current_job_name = None
            gc.collect()

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
        current_stage="shutdown",
        status="shutting_down",
        queue_counts=queue_counts(db, queue_name),
    )
    logger.info("Graceful shutdown complete.")
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
