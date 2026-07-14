from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import re
import shlex
import subprocess
import threading
import time
from typing import Any, TextIO

DEFAULT_FAILURE_TAIL_LINES = 120
_MANIFEST_NAME = "command_log_manifest.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _safe_stage_name(stage: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", stage).strip("_") or "stage"


@dataclass
class CommandResult:
    stage: str
    cmd: list[str]
    returncode: int
    cwd: str | None
    stdout_log: str
    stderr_log: str
    stdout_tail: str
    stderr_tail: str
    started_at: str
    completed_at: str
    duration_seconds: float
    root_cause_hint: str | None

    @property
    def step(self) -> str:
        return self.stage


class CommandExecutionError(RuntimeError):
    def __init__(self, result: CommandResult) -> None:
        self.result = result
        super().__init__(f"{result.stage} failed exit={result.returncode} hint={result.root_cause_hint or 'none'}")

    @property
    def step(self) -> str:
        return self.result.step

    @property
    def cmd(self) -> list[str]:
        return self.result.cmd

    @property
    def returncode(self) -> int:
        return self.result.returncode

    @property
    def root_cause_hint(self) -> str | None:
        return self.result.root_cause_hint

    @property
    def stdout_path(self) -> str:
        return self.result.stdout_log

    @property
    def stderr_path(self) -> str:
        return self.result.stderr_log

    def compact_message(self) -> str:
        return (
            f"{self.result.stage} failed exit={self.result.returncode} "
            f'root_cause="{self.result.root_cause_hint or ""}" '
            f'stderr_log="{self.result.stderr_log}"'
        )

    def to_failure_block(self) -> str:
        return (
            f"Step: {self.result.stage}\n"
            f"Exit code: {self.result.returncode}\n"
            f"STDOUT log: {self.result.stdout_log}\n"
            f"STDERR log: {self.result.stderr_log}\n"
            f"Root-cause hint: {self.result.root_cause_hint or 'none'}"
        )


# Backward-compatibility aliases used by run_experiment_pipeline and tests.
CommandRunResult = CommandResult
PipelineCommandError = CommandExecutionError


def detect_root_cause(stdout_tail: str, stderr_tail: str) -> str | None:
    text = f"{stderr_tail}\n{stdout_tail}".strip()
    if not text:
        return None
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    joined = "\n".join(lines)

    if "Traceback (most recent call last)" in joined:
        for line in reversed(lines):
            if ":" in line and not line.startswith("File "):
                return line

    for marker in ("ModuleNotFoundError", "FileNotFoundError", "KeyError", "ValueError", "TypeError", "RuntimeError"):
        for line in reversed(lines):
            if marker in line:
                return line

    if "TimeoutError" in joined or "timed out" in joined.lower():
        return "TimeoutError/timed out detected."
    if "Killed" in joined:
        return "Process was killed; possible OOM/memory pressure."
    if "Permission denied" in joined:
        return "Permission denied detected."
    return None


def _append_manifest(log_dir: Path, *, queue_id: str | None, job_name: str | None, command: dict[str, Any]) -> None:
    manifest_path = log_dir / _MANIFEST_NAME
    now = utc_now_iso()
    payload: dict[str, Any]
    if manifest_path.exists():
        raw_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        # Backward compatibility: older command manifests were a raw list of command
        # entries; normalize them to the current object schema.
        if isinstance(raw_payload, list):
            payload = {
                "job_name": job_name,
                "queue_id": queue_id,
                "created_at": now,
                "updated_at": now,
                "commands": raw_payload,
            }
        else:
            payload = raw_payload
    else:
        payload = {
            "job_name": job_name,
            "queue_id": queue_id,
            "created_at": now,
            "updated_at": now,
            "commands": [],
        }
    payload["job_name"] = job_name or payload.get("job_name")
    payload["queue_id"] = queue_id or payload.get("queue_id")
    payload["updated_at"] = now
    payload.setdefault("commands", []).append(command)
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def run_logged_command(*, stage: str, cmd: list[str], log_dir: Path, logger: logging.Logger, queue_id: str | None = None, job_name: str | None = None, cwd: Path | None = None, env: dict | None = None, tail_lines: int = 120, check: bool = True) -> CommandResult:
    log_dir.mkdir(parents=True, exist_ok=True)
    safe_stage = _safe_stage_name(stage)
    stdout_path = log_dir / f"{safe_stage}.stdout.log"
    stderr_path = log_dir / f"{safe_stage}.stderr.log"

    logger.info("\n%s\nDAEMON STAGE STARTED\nStage: %s\nJob name: %s\nCommand:\n  %s\n%s", "=" * 80, stage, job_name or "", shlex.join(cmd), "=" * 80)

    out_tail: deque[str] = deque(maxlen=tail_lines)
    err_tail: deque[str] = deque(maxlen=tail_lines)
    started_iso = utc_now_iso()
    t0 = time.time()

    with stdout_path.open("w", encoding="utf-8") as out_fh, stderr_path.open("w", encoding="utf-8") as err_fh:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            cwd=str(cwd) if cwd else None,
            env=env,
        )

        def _reader(stream: TextIO, sink: TextIO, tail: deque[str]) -> None:
            for line in iter(stream.readline, ""):
                line = line.rstrip("\n")
                sink.write(line + "\n")
                sink.flush()
                tail.append(line)
                logger.info("[%s] %s", stage, line)
            stream.close()

        assert process.stdout is not None and process.stderr is not None
        t_out = threading.Thread(target=_reader, args=(process.stdout, out_fh, out_tail), daemon=True)
        t_err = threading.Thread(target=_reader, args=(process.stderr, err_fh, err_tail), daemon=True)
        t_out.start(); t_err.start()
        returncode = process.wait()
        t_out.join(); t_err.join()

    completed_iso = utc_now_iso()
    duration = time.time() - t0
    out_text = "\n".join(out_tail)
    err_text = "\n".join(err_tail)
    result = CommandResult(stage=stage, cmd=cmd, returncode=returncode, cwd=str(cwd) if cwd else None, stdout_log=str(stdout_path), stderr_log=str(stderr_path), stdout_tail=out_text, stderr_tail=err_text, started_at=started_iso, completed_at=completed_iso, duration_seconds=duration, root_cause_hint=detect_root_cause(out_text, err_text))

    _append_manifest(log_dir, queue_id=queue_id, job_name=job_name, command={
        "stage": result.stage,
        "cmd": result.cmd,
        "returncode": result.returncode,
        "stdout_log": result.stdout_log,
        "stderr_log": result.stderr_log,
        "started_at": result.started_at,
        "completed_at": result.completed_at,
        "duration_seconds": result.duration_seconds,
        "root_cause_hint": result.root_cause_hint,
    })

    if returncode != 0 and check:
        logger.error("\n%s\nDAEMON STAGE FAILED\nStage: %s\nQueue ID: %s\nJob name: %s\nExit code: %s\nCommand:\n  %s\nSTDOUT log:\n  %s\nSTDERR log:\n  %s\n\nSTDOUT tail:\n%s\n\nSTDERR tail:\n%s\n\nRoot-cause hint:\n  %s\n%s", "=" * 80, stage, queue_id or "", job_name or "", returncode, shlex.join(cmd), result.stdout_log, result.stderr_log, out_text, err_text, result.root_cause_hint or "No automatic hint detected.", "=" * 80)
        raise CommandExecutionError(result)

    logger.info("\n%s\nDAEMON STAGE COMPLETED\nStage: %s\nJob name: %s\nDuration seconds: %.2f\nSTDOUT log: %s\nSTDERR log: %s\n%s", "=" * 80, stage, job_name or "", duration, result.stdout_log, result.stderr_log, "=" * 80)
    return result


def run_pipeline_command(
    *,
    cmd: list[str],
    step: str,
    cwd: Path,
    log_path: Path,
    command_log_dir: Path | None,
    sequence_num: int,
    dry_run: bool,
    capture_logs: bool,
    failure_tail_lines: int,
    env: dict | None = None,
) -> CommandResult:
    """Compatibility wrapper for legacy pipeline runner API."""
    logger = logging.getLogger("orchestrator.process_logging.pipeline")

    if dry_run:
        started = utc_now_iso()
        return CommandResult(
            stage=step,
            cmd=cmd,
            returncode=0,
            cwd=str(cwd),
            stdout_log="",
            stderr_log="",
            stdout_tail="",
            stderr_tail="",
            started_at=started,
            completed_at=started,
            duration_seconds=0.0,
            root_cause_hint=None,
        )

    if command_log_dir is None:
        command_log_dir = cwd / "outputs" / "command_logs"

    result = run_logged_command(
        stage=step,
        cmd=cmd,
        log_dir=command_log_dir,
        logger=logger,
        cwd=cwd,
        env=env,
        tail_lines=failure_tail_lines,
        check=False,
    )

    if result.returncode != 0:
        raise PipelineCommandError(result)
    return result
