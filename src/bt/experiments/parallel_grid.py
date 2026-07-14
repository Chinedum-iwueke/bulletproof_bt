"""Generalized manifest-driven parallel hypothesis runner."""
from __future__ import annotations

import argparse
import csv
import gc
import hashlib
import inspect
import json
import multiprocessing
import os
import shutil
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any

import yaml

from bt.experiments.hypothesis_runner import execute_hypothesis_variant, resolve_phase_tiers
from bt.experiments.manifest import decode_params, encode_params, read_manifest_csv, write_manifest_csv
from bt.experiments.precompute_cache import PrecomputeRegistry, build_registry, stable_cache_key
from bt.experiments.resource_controls import memory_snapshot, resolve_auto_workers, wait_for_memory
from bt.experiments.shared_data import SharedDatasetPlan, build_shared_dataset_plan, write_shared_cache_manifest
from bt.experiments.status import atomic_write_json, detect_run_artifact_status, write_status_csv
from bt.experiments.wave_scheduler import iter_waves, resolve_wave_size
from bt.experiments.worker_bootstrap import (
    WorkerLogger,
    apply_thread_caps,
    enable_worker_faulthandler,
    write_worker_exception,
)
from bt.hypotheses.contract import HypothesisContract
from bt.research_tiers import phase_to_contract_phase, research_metadata_for_phase
from bt.research_orchestration.data_profiles import (
    preflight_research_data_profile,
    resolve_data_profile,
    write_data_profile_config,
)


_PROCESS_POOL_SUPPORTS_MAX_TASKS = "max_tasks_per_child" in inspect.signature(ProcessPoolExecutor).parameters


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_worker_traceback(run_dir: Path) -> str:
    path = run_dir / "worker_exception.txt"
    if not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def _slug_value(value: Any) -> str:
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, float):
        return str(value).replace(".", "p")
    return str(value).replace("/", "_")


def _variant_slug(grid_id: str, params: dict[str, Any]) -> str:
    suffix = "_".join(f"{key}-{_slug_value(params[key])}" for key in sorted(params.keys()))
    if not suffix:
        return grid_id

    full_slug = f"{grid_id}__{suffix}"
    max_slug_len = 160
    if len(full_slug) <= max_slug_len:
        return full_slug

    digest = hashlib.sha1(full_slug.encode("utf-8")).hexdigest()[:12]
    remaining = max_slug_len - len(grid_id) - len("____h-") - len(digest)
    trimmed_suffix = suffix[:max(remaining, 0)].rstrip("_-")
    return f"{grid_id}__{trimmed_suffix}__h-{digest}" if trimmed_suffix else f"{grid_id}__h-{digest}"


def _normalized_output_dir(*, row_id: str, variant_id: str, tier: str, params_json: str) -> str:
    params = decode_params(params_json)
    run_slug = _variant_slug(variant_id, params)
    return f"runs/{row_id}__{run_slug}__{tier.lower()}"


def build_hypothesis_manifest_rows(*, contract: HypothesisContract, hypothesis_path: Path, phase: str) -> list[dict[str, str]]:
    tiers = resolve_phase_tiers(contract, phase)
    specs = contract.to_run_specs()
    rows: list[dict[str, str]] = []
    row_idx = 1
    for spec in specs:
        params = spec["params"]
        run_slug = _variant_slug(spec["grid_id"], params)
        for tier in tiers:
            output_dir = f"runs/row_{row_idx:05d}__{run_slug}__{tier.lower()}"
            rows.append(
                {
                    "row_id": f"row_{row_idx:05d}",
                    "hypothesis_id": str(spec["hypothesis_id"]),
                    "hypothesis_path": str(hypothesis_path),
                    "phase": phase,
                    "tier": tier,
                    "variant_id": str(spec["grid_id"]),
                    "config_hash": str(spec["config_hash"]),
                    "params_json": encode_params(params),
                    "run_slug": f"{run_slug}__{tier.lower()}",
                    "output_dir": output_dir,
                    "expected_status": "pending",
                    "enabled": "true",
                    "notes": "",
                }
            )
            row_idx += 1
    return rows


def _manifest_name(hypothesis_path: Path, phase: str) -> str:
    return f"{hypothesis_path.stem}_{phase.lower()}_grid.csv"


def build_hypothesis_manifest(
    *,
    hypothesis_path: Path,
    experiment_root: Path,
    phase: str,
) -> Path:
    if not hypothesis_path.exists():
        raise ValueError(f"Hypothesis file does not exist: {hypothesis_path}")
    contract = HypothesisContract.from_yaml(hypothesis_path)
    rows = build_hypothesis_manifest_rows(contract=contract, hypothesis_path=hypothesis_path, phase=phase)
    manifests_dir = experiment_root / "manifests"
    manifest_path = manifests_dir / _manifest_name(hypothesis_path, phase)
    write_manifest_csv(rows, manifest_path)

    snapshot_dir = experiment_root / "contract_snapshot"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = snapshot_dir / hypothesis_path.name
    snapshot_path.write_text(hypothesis_path.read_text(encoding="utf-8"), encoding="utf-8")

    summary_dir = experiment_root / "summaries"
    summary_dir.mkdir(parents=True, exist_ok=True)
    summary_payload = {
        "hypothesis_id": contract.schema.metadata.hypothesis_id,
        "phase": phase,
        "contract_phase": phase_to_contract_phase(phase),
        **research_metadata_for_phase(phase),
        "rows": len(rows),
        "tiers": list(resolve_phase_tiers(contract, phase)),
    }
    (summary_dir / f"{hypothesis_path.stem}_{phase}_grid_summary.json").write_text(
        json.dumps(summary_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return manifest_path


def _read_hypothesis_rows_jsonl(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    key_values: dict[str, Any] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if isinstance(payload, dict) and payload.get("grid_id") and payload.get("tier"):
            key = f"{payload['grid_id']}::{payload['tier']}"
            key_values[key] = payload
    return key_values


def _materialize_phase_rollup(experiment_root: Path, manifest_rows: list[dict[str, str]], status_rows: list[dict[str, Any]]) -> None:
    by_key = _read_hypothesis_rows_jsonl(experiment_root / "hypothesis_rows.jsonl")
    status_by_row = {str(row["row_id"]): row for row in status_rows}
    out_rows: list[dict[str, Any]] = []
    for row in manifest_rows:
        status_row = status_by_row.get(row["row_id"], {})
        if status_row.get("status") != "COMPLETED":
            continue
        metrics = by_key.get(f"{row['variant_id']}::{row['tier']}", {})
        run_dir = metrics.get("run_dir", "")
        perf_path: Path | None = None
        output_dir = status_row.get("output_dir") or row.get("output_dir")
        if output_dir:
            candidate = experiment_root / str(output_dir) / "performance.json"
            if candidate.exists():
                perf_path = candidate
        if perf_path is None and run_dir:
            candidate = Path(str(run_dir)) / "performance.json"
            if not candidate.is_absolute():
                candidate = experiment_root / candidate
            if candidate.exists():
                perf_path = candidate
        if perf_path is not None:
            try:
                performance = json.loads(perf_path.read_text(encoding="utf-8"))
                metrics = {
                    **metrics,
                    "num_trades": metrics.get("num_trades", performance.get("total_trades")),
                    "ev_r_net": metrics.get("ev_r_net", performance.get("ev_r_net")),
                    "pnl_net": metrics.get("pnl_net", performance.get("net_pnl")),
                    "max_drawdown_r": metrics.get("max_drawdown_r", performance.get("max_drawdown_pct")),
                    "run_dir": metrics.get("run_dir", str(perf_path.parent)),
                }
            except (OSError, json.JSONDecodeError):
                pass
        out_rows.append(
            {
                "row_id": row["row_id"],
                "variant_id": row["variant_id"],
                "tier": row["tier"],
                "params_json": row["params_json"],
                "output_dir": row["output_dir"],
                "num_trades": metrics.get("num_trades", ""),
                "ev_r_net": metrics.get("ev_r_net", ""),
                "pnl_net": metrics.get("pnl_net", ""),
                "max_drawdown_r": metrics.get("max_drawdown_r", ""),
                "run_dir": metrics.get("run_dir", ""),
            }
        )

    path = experiment_root / "summaries" / "phase_rollup.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "row_id",
        "variant_id",
        "tier",
        "params_json",
        "output_dir",
        "num_trades",
        "ev_r_net",
        "pnl_net",
        "max_drawdown_r",
        "run_dir",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for out_row in out_rows:
            writer.writerow(out_row)




def _materialize_phase_segment_rollups(experiment_root: Path, manifest_rows: list[dict[str, str]], status_rows: list[dict[str, Any]]) -> None:
    status_by_row = {str(row["row_id"]): row for row in status_rows}
    out_rows: list[dict[str, Any]] = []
    for row in manifest_rows:
        if status_by_row.get(row["row_id"], {}).get("status") != "COMPLETED":
            continue
        run_dir = experiment_root / row["output_dir"]
        segment_path = run_dir / "segment_rollups.csv"
        if not segment_path.exists():
            continue
        with segment_path.open("r", encoding="utf-8", newline="") as handle:
            for payload in csv.DictReader(handle):
                out_rows.append(
                    {
                        "row_id": row["row_id"],
                        "variant_id": row["variant_id"],
                        "tier": row["tier"],
                        "hypothesis_id": row["hypothesis_id"],
                        "run_dir": str(run_dir),
                        **payload,
                    }
                )

    out_rows = sorted(out_rows, key=lambda item: (item["row_id"], str(item.get("grouping_keys", "")), str(item.get("segment_value_json", ""))))
    path = experiment_root / "summaries" / "phase_segment_rollups.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "row_id",
        "variant_id",
        "tier",
        "hypothesis_id",
        "run_dir",
        "schema_version",
        "grouping_keys",
        "segment_value_json",
        "source_run_dir",
        "n_trades",
        "ev_r_net",
        "win_rate",
        "avg_win_r",
        "avg_loss_r",
        "payoff_ratio",
        "avg_hold_bars",
        "pnl_net",
        "max_loss_r",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for out in out_rows:
            writer.writerow({field: out.get(field, "") for field in fields})
def _execute_manifest_row(
    *,
    row: dict[str, str],
    config_path: str,
    local_config: str | None,
    data_path: str,
    experiment_root: str,
    override_paths: list[str],
    shared_dataset_plan: dict[str, Any],
    precompute_registry: dict[str, dict[str, Any]],
) -> tuple[int, str]:
    run_dir = Path(experiment_root) / row["output_dir"]
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    run_started_at = _utc_now()
    atomic_write_json(
        run_dir / "run_status.json",
        {
            "status": "RUNNING",
            "started_at": run_started_at,
            "completed_at": None,
            "duration_seconds": None,
            "error_message": "",
            "pid": os.getpid(),
            "worker": {"pid": os.getpid()},
            "runner_status_schema_version": 1,
        },
    )
    logger = WorkerLogger(run_dir)
    faulthandler_path = enable_worker_faulthandler(run_dir)
    bootstrap = apply_thread_caps(default_threads="1")
    logger.event(
        "bootstrap",
        effective_thread_caps=bootstrap.effective_thread_caps,
        changed_thread_caps=bootstrap.changed_thread_caps,
        faulthandler_log=str(faulthandler_path),
    )

    params = decode_params(row["params_json"])
    signal_timeframe = str(params.get("signal_timeframe", params.get("timeframe", "15m"))).lower()
    signature_key = stable_cache_key(
        dataset_id=str(shared_dataset_plan.get("dataset_id", "")),
        timeframe=signal_timeframe,
        family=str(row["hypothesis_id"]),
        params={k: params.get(k) for k in sorted(params.keys())},
        engine_version="parallel_grid_v2",
    )
    precompute = precompute_registry.get(signature_key)

    run_context = {
        "row_id": row["row_id"],
        "hypothesis_id": row["hypothesis_id"],
        "variant_id": row["variant_id"],
        "tier": row["tier"],
        "run_dir": str(run_dir),
        "dataset": shared_dataset_plan,
        "precompute": precompute,
        "effective_thread_caps": bootstrap.effective_thread_caps,
        "pid": os.getpid(),
    }
    (run_dir / "run_context.json").write_text(json.dumps(run_context, indent=2, sort_keys=True), encoding="utf-8")

    logger.start_phase("dataset_attach", dataset_source=shared_dataset_plan.get("source"))
    logger.finish_phase("dataset_attach", dataset_id=shared_dataset_plan.get("dataset_id"))
    logger.start_phase("precompute_attach", cache_key=signature_key)
    logger.finish_phase("precompute_attach", precompute_status=(precompute or {}).get("status", "cold_built"))

    contract = HypothesisContract.from_yaml(row["hypothesis_path"])
    spec = {
        "hypothesis_id": row["hypothesis_id"],
        "grid_id": row["variant_id"],
        "config_hash": row["config_hash"],
        "params": params,
    }
    run_slug = Path(row["output_dir"]).name
    try:
        logger.start_phase("execution")
        execute_hypothesis_variant(
            contract=contract,
            spec=spec,
            tier=row["tier"],
            config_path=config_path,
            data_path=data_path,
            out_root=str(Path(experiment_root) / "runs"),
            local_config=local_config,
            override_paths=override_paths,
            run_slug=run_slug,
            phase=row.get("phase", "tier2b"),
        )
        # The engine writes the authoritative PASS status and required truth
        # artifacts. The runner intentionally does not overwrite successful
        # run_status.json, preserving engine-level semantics and metadata.
        logger.finish_phase("execution", status="completed")
        logger.event("completion", status="completed")
        logger.close()
        return 0, ""
    except Exception as exc:
        write_worker_exception(run_dir, exc)
        atomic_write_json(
            run_dir / "run_status.json",
            {
                "status": "FAIL",
                "started_at": run_started_at,
                "completed_at": _utc_now(),
                "duration_seconds": None,
                "error_message": str(exc),
                "traceback_path": str(run_dir / "worker_exception.txt"),
                "pid": os.getpid(),
                "worker": {"pid": os.getpid()},
                "runner_status_schema_version": 1,
            },
        )
        logger.finish_phase("execution", status="failed", error=str(exc))
        logger.event("completion", status="failed", error=str(exc))
        logger.close()
        return 1, str(exc)


def _build_preprocessing_signatures(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    unique: dict[str, dict[str, Any]] = {}
    for row in rows:
        params = decode_params(row["params_json"])
        signal_timeframe = str(params.get("signal_timeframe", params.get("timeframe", "15m"))).lower()
        signature = {
            "strategy": row["hypothesis_id"],
            "signal_timeframe": signal_timeframe,
            "invariants": {key: params[key] for key in sorted(params.keys())},
        }
        key = json.dumps(signature, sort_keys=True)
        unique[key] = signature
    return list(unique.values())


def run_hypothesis_manifest_in_parallel(
    *,
    manifest_rows: list[dict[str, str]],
    experiment_root: Path,
    config_path: Path,
    local_config: Path | None,
    data_path: Path,
    max_workers: int,
    skip_completed: bool,
    override_paths: list[Path],
    dry_run: bool,
    run_timeout_seconds: float | None = None,
    min_free_ram_gb: float = 6.0,
    fail_fast: bool = False,
    resume_strict: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    status_rows: list[dict[str, Any]] = []
    launch_rows: list[dict[str, str]] = []

    if not config_path.exists():
        raise ValueError(f"--config file does not exist: {config_path}")
    if local_config and not local_config.exists():
        raise ValueError(f"--local-config file does not exist: {local_config}")
    if not data_path.exists():
        raise ValueError(f"--data path does not exist: {data_path}")

    for override in override_paths:
        if not override.exists():
            raise ValueError(f"--override file does not exist: {override}")

    normalized_rows: list[dict[str, str]] = []
    for row in manifest_rows:
        normalized = dict(row)
        normalized["output_dir"] = _normalized_output_dir(
            row_id=normalized["row_id"],
            variant_id=normalized["variant_id"],
            tier=normalized["tier"],
            params_json=normalized["params_json"],
        )
        normalized["run_slug"] = Path(normalized["output_dir"]).name
        normalized_rows.append(normalized)

    shared_dataset: SharedDatasetPlan = build_shared_dataset_plan(dataset_path=data_path)
    write_shared_cache_manifest(experiment_root, shared_dataset)
    precompute: PrecomputeRegistry = build_registry(
        experiment_root=experiment_root,
        dataset_id=shared_dataset.dataset_id,
        preprocessing_signatures=_build_preprocessing_signatures(normalized_rows),
        engine_version="parallel_grid_v2",
    )

    for row in normalized_rows:
        row_out_dir = experiment_root / row["output_dir"]
        completed_state = detect_run_artifact_status(row_out_dir, strict_resume=resume_strict)
        if row["enabled"] == "false":
            status_rows.append({
                "row_id": row["row_id"],
                "variant_id": row["variant_id"],
                "tier": row["tier"],
                "status": "SKIPPED",
                "return_code": "",
                "started_at": "",
                "ended_at": "",
                "duration_sec": "",
                "output_dir": row["output_dir"],
                "error_message": "disabled in manifest",
                "traceback": "",
            })
            continue
        if skip_completed and completed_state.state == "SUCCESS":
            status_rows.append({
                "row_id": row["row_id"],
                "variant_id": row["variant_id"],
                "tier": row["tier"],
                "status": "SKIPPED",
                "return_code": "0",
                "started_at": "",
                "ended_at": "",
                "duration_sec": "",
                "output_dir": row["output_dir"],
                "error_message": "already completed",
                "traceback": "",
            })
            continue
        launch_rows.append(row)

    write_status_csv(experiment_root / "summaries" / "manifest_status.csv", sorted(status_rows, key=lambda x: x["row_id"]))

    if dry_run:
        for row in launch_rows:
            status_rows.append(
                {
                    "row_id": row["row_id"],
                    "variant_id": row["variant_id"],
                    "tier": row["tier"],
                    "status": "PENDING",
                    "return_code": "",
                    "started_at": _utc_now(),
                    "ended_at": "",
                    "duration_sec": "",
                    "output_dir": row["output_dir"],
                    "error_message": "",
                    "traceback": "",
                }
            )
    else:
        ctx = multiprocessing.get_context("spawn")
        wave_size = resolve_wave_size(max_workers=max_workers)
        if not _PROCESS_POOL_SUPPORTS_MAX_TASKS:
            # Older Python builds cannot recycle workers within one pool. Keep
            # each wave to one task per worker so the pool exits between waves
            # and releases allocator state before the next batch of runs.
            wave_size = max_workers
        failure_records: list[dict[str, Any]] = []
        completed_count = 0
        skipped_count = len([row for row in status_rows if row["status"] == "SKIPPED"])
        total_count = skipped_count + len(launch_rows)
        for wave_idx, wave_rows in enumerate(iter_waves(launch_rows, wave_size=wave_size), start=1):
            wait_for_memory(min_free_ram_gb=min_free_ram_gb, logger=lambda msg: print(f"[resource] {msg}", flush=True))
            # Recycle workers after each manifest row when the local Python
            # runtime supports it. Volatile research-panel runs can legitimately
            # build large per-run state and pandas/pyarrow allocators do not
            # always return freed memory to the OS inside a long-lived process.
            # Process recycling is semantics-neutral: each row is already an
            # isolated deterministic backtest with its own config and artifact
            # directory.
            pool_kwargs: dict[str, Any] = {"max_workers": max_workers, "mp_context": ctx}
            if _PROCESS_POOL_SUPPORTS_MAX_TASKS:
                pool_kwargs["max_tasks_per_child"] = 1
            with ProcessPoolExecutor(**pool_kwargs) as executor:
                future_context: dict[Any, dict[str, Any]] = {}
                for row in wave_rows:
                    started_at = _utc_now()
                    started_clock = monotonic()
                    context = {
                        "manifest_row_index": int(str(row["row_id"]).replace("row_", "") or 0),
                        "row_id": row["row_id"],
                        "run_id": row["run_slug"],
                        "hypothesis_id": row["hypothesis_id"],
                        "variant_id": row["variant_id"],
                        "tier": row["tier"],
                        "params_snapshot": decode_params(row["params_json"]),
                        "run_dir": str(experiment_root / row["output_dir"]),
                        "started_at": started_at,
                        "started_clock": started_clock,
                        "wave": wave_idx,
                    }
                    future = executor.submit(
                        _execute_manifest_row,
                        row=row,
                        config_path=str(config_path),
                        local_config=str(local_config) if local_config else None,
                        data_path=str(data_path),
                        experiment_root=str(experiment_root),
                        override_paths=[str(item) for item in override_paths],
                        shared_dataset_plan={
                            "dataset_path": shared_dataset.dataset_path,
                            "dataset_id": shared_dataset.dataset_id,
                            "dataset_mode": shared_dataset.dataset_mode,
                            "source": shared_dataset.source,
                            "metadata": shared_dataset.metadata,
                        },
                        precompute_registry={key: artifact.metadata | {"status": artifact.status} for key, artifact in precompute.artifacts.items()},
                    )
                    future_context[future] = context
                    status_rows.append(
                        {
                            "row_id": row["row_id"],
                            "variant_id": row["variant_id"],
                            "tier": row["tier"],
                            "status": "RUNNING",
                            "return_code": "",
                            "started_at": started_at,
                            "ended_at": "",
                            "duration_sec": "",
                            "output_dir": row["output_dir"],
                            "error_message": "",
                            "traceback": "",
                        }
                    )

                pending = set(future_context.keys())
                while pending:
                    done, pending = wait(pending, timeout=5.0, return_when=FIRST_COMPLETED)
                    if not done:
                        if run_timeout_seconds is not None:
                            timed_out = [
                                future
                                for future in pending
                                if monotonic() - float(future_context[future]["started_clock"]) > run_timeout_seconds
                            ]
                            for future in timed_out:
                                future.cancel()
                                pending.remove(future)
                                done.add(future)
                        if done:
                            # Timed-out futures are handled below like normal completions so
                            # they receive run_status.json and manifest_status.csv rows.
                            pass
                        else:
                            snap = memory_snapshot()
                            msg = (
                                f"progress completed={completed_count} skipped={skipped_count} "
                                f"failed={len(failure_records)} running={len(pending)} total={total_count}"
                            )
                            if snap is not None:
                                msg += f" mem_available_gb={snap.available_gb:.2f}"
                            print(msg, flush=True)
                            continue
                    for future in done:
                        if future not in future_context:
                            continue
                        context = future_context[future]
                        started_clock = float(context["started_clock"])
                        try:
                            if run_timeout_seconds is not None and monotonic() - started_clock > run_timeout_seconds and future.cancelled():
                                return_code, error_message = 1, f"run timeout after {run_timeout_seconds}s"
                            else:
                                return_code, error_message = future.result()
                        except BrokenProcessPool as exc:
                            return_code = 1
                            error_message = f"BrokenProcessPool: {exc}"
                        except Exception as exc:
                            return_code = 1
                            error_message = f"{type(exc).__name__}: {exc}"
                        ended_at = _utc_now()
                        duration = monotonic() - started_clock
                        row_id = str(context["row_id"])
                        row = next(item for item in wave_rows if item["row_id"] == row_id)
                        artifact_state = detect_run_artifact_status(experiment_root / row["output_dir"], strict_resume=resume_strict)
                        # A worker process can die after the engine has already written a complete
                        # PASS artifact set. In that case the on-disk run contract is the safer
                        # source of truth for resume behavior than the pool-level exception.
                        status = "COMPLETED" if artifact_state.state == "SUCCESS" else "FAILED"
                        status_rows = [
                            existing
                            for existing in status_rows
                            if not (existing["row_id"] == row_id and existing["status"] == "RUNNING")
                        ]
                        status_rows.append(
                            {
                                "row_id": row_id,
                                "variant_id": row["variant_id"],
                                "tier": row["tier"],
                                "status": status,
                                "return_code": str(return_code),
                                "started_at": str(context["started_at"]),
                                "ended_at": ended_at,
                                "duration_sec": f"{duration:.3f}",
                                "output_dir": row["output_dir"],
                                "error_message": error_message or artifact_state.message,
                                "traceback": _read_worker_traceback(experiment_root / row["output_dir"]) if status == "FAILED" else "",
                            }
                        )
                        if status == "FAILED":
                            run_dir = experiment_root / row["output_dir"]
                            existing_state = detect_run_artifact_status(run_dir, strict_resume=False)
                            if existing_state.state in {"MISSING", "INCOMPLETE", "RUNNING", "PENDING", "SKIPPED"} or "timeout" in str(error_message).lower():
                                atomic_write_json(
                                    run_dir / "run_status.json",
                                    {
                                        "status": "FAIL",
                                        "started_at": str(context["started_at"]),
                                        "completed_at": ended_at,
                                        "duration_seconds": duration,
                                        "error_message": error_message or artifact_state.message,
                                        "traceback_path": str(run_dir / "worker_exception.txt") if (run_dir / "worker_exception.txt").exists() else "",
                                        "pid": os.getpid(),
                                        "worker": {"pid": os.getpid()},
                                        "runner_status_schema_version": 1,
                                    },
                                )
                            failure_records.append(
                                {
                                    **{k: v for k, v in context.items() if k != "started_clock"},
                                    "failed_at": ended_at,
                                    "error_message": error_message or artifact_state.message,
                                    "traceback": _read_worker_traceback(experiment_root / row["output_dir"]),
                                }
                            )
                            if fail_fast:
                                executor.shutdown(wait=False, cancel_futures=True)
                                raise RuntimeError(f"fail-fast: row_id={row_id} failed: {error_message or artifact_state.message}")
                        else:
                            completed_count += 1
                        snap = memory_snapshot()
                        finished = completed_count + len(failure_records)
                        avg_duration = sum(
                            float(row.get("duration_sec") or 0.0)
                            for row in status_rows
                            if row.get("status") in {"COMPLETED", "FAILED"}
                        ) / max(finished, 1)
                        remaining = max(len(launch_rows) - finished, 0)
                        msg = (
                            f"progress completed={completed_count} skipped={skipped_count} failed={len(failure_records)} "
                            f"running={len(pending)} total={total_count} avg_sec_per_run={avg_duration:.1f} "
                            f"eta_sec={avg_duration * remaining:.0f}"
                        )
                        if snap is not None:
                            msg += f" mem_available_gb={snap.available_gb:.2f}"
                        print(msg, flush=True)
                        write_status_csv(experiment_root / "summaries" / "manifest_status.csv", sorted(status_rows, key=lambda x: x["row_id"]))

                future_context.clear()
                gc.collect()
        (experiment_root / "summaries" / "parallel_failures.json").write_text(
            json.dumps(failure_records, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    status_rows = sorted(status_rows, key=lambda x: x["row_id"])
    failures = [row for row in status_rows if row["status"] == "FAILED"]
    write_status_csv(experiment_root / "summaries" / "manifest_status.csv", status_rows)
    write_status_csv(experiment_root / "summaries" / "failures.csv", failures)
    _materialize_phase_rollup(experiment_root, normalized_rows, status_rows)
    _materialize_phase_segment_rollups(experiment_root, normalized_rows, status_rows)
    return status_rows, failures


def resolve_parallel_grid_data_args(args: argparse.Namespace, experiment_root: Path) -> tuple[Path, list[Path]]:
    """Resolve legacy --data or new research_data profile arguments."""
    if args.data:
        data_path = Path(args.data)
        if not data_path.exists():
            raise ValueError(f"--data path does not exist: {data_path}")
        return data_path, []
    if args.data_kind != "research_panel" or not args.data_root:
        raise ValueError("Provide legacy --data or new --data-root with --data-kind research_panel")
    profile = resolve_data_profile(
        universe=args.universe,
        data_root=args.data_root,
        data_kind=args.data_kind,
        exchange=args.exchange,
        timeframe=args.timeframe,
        stable_manifest=args.stable_manifest,
        membership_path=args.membership_path,
    )
    local_config_arg = getattr(args, "local_config", None)
    symbols_subset, max_symbols = _preflight_scope_from_local_config(Path(local_config_arg) if local_config_arg else None)
    preflight_research_data_profile(profile, symbols_subset=symbols_subset, max_symbols=max_symbols)
    override_path = write_data_profile_config(
        profile,
        (experiment_root / "summaries" / f"data_profile_{profile.universe}.yaml").resolve(),
    )
    return profile.root, [override_path]


def _preflight_scope_from_local_config(local_config: Path | None) -> tuple[list[str] | None, int | None]:
    if local_config is None or not local_config.exists():
        return None, None
    with local_config.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        return None, None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None, None
    raw_symbols = data.get("symbols_subset", data.get("symbols"))
    symbols_subset = None
    if isinstance(raw_symbols, str):
        symbols_subset = [item.strip() for item in raw_symbols.split(",") if item.strip()]
    elif isinstance(raw_symbols, list):
        symbols_subset = [str(item).strip() for item in raw_symbols if str(item).strip()]
    max_symbols = data.get("max_symbols")
    return symbols_subset or None, int(max_symbols) if max_symbols else None


def cli_build_hypothesis_manifest(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build manifest for any hypothesis contract")
    parser.add_argument("--hypothesis", required=True)
    parser.add_argument("--experiment-root", required=True)
    parser.add_argument("--phase", choices=("tier2a", "tier2b", "tier2", "tier3", "validate"), default="tier2b")
    args = parser.parse_args(argv)

    manifest = build_hypothesis_manifest(
        hypothesis_path=Path(args.hypothesis),
        experiment_root=Path(args.experiment_root),
        phase=args.phase,
    )
    print(f"wrote manifest: {manifest}")
    return 0


def cli_run_parallel_hypothesis_grid(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a hypothesis manifest in process-level parallelism")
    parser.add_argument("--experiment-root", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--local-config")
    parser.add_argument("--data")
    parser.add_argument("--data-root")
    parser.add_argument("--data-kind", choices=("research_panel",))
    parser.add_argument("--exchange", default="binance")
    parser.add_argument("--universe", choices=("stable", "volatile"), default="stable")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--membership-path")
    parser.add_argument("--stable-manifest")
    parser.add_argument("--phase", choices=("tier2a", "tier2b", "tier2", "tier3", "validate"))
    parser.add_argument("--override", action="append", default=[])
    parser.add_argument("--max-workers", type=int, default=6)
    parser.add_argument("--max-workers-auto", action="store_true", default=False)
    parser.add_argument("--reserve-ram-gb", type=float, default=8.0)
    parser.add_argument("--max-ram-per-worker-gb", type=float)
    parser.add_argument("--min-free-ram-gb", type=float, default=6.0)
    parser.add_argument("--run-timeout-seconds", type=float)
    parser.add_argument("--fail-fast", dest="fail_fast", action="store_true", default=False)
    parser.add_argument("--no-fail-fast", dest="fail_fast", action="store_false")
    parser.add_argument("--resume-strict", dest="resume_strict", action="store_true", default=True)
    parser.add_argument("--no-resume-strict", dest="resume_strict", action="store_false")
    parser.add_argument("--skip-completed", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    if args.max_workers <= 0:
        raise ValueError("--max-workers must be > 0")
    resolved_max_workers = resolve_auto_workers(
        requested_max_workers=args.max_workers,
        use_auto=bool(args.max_workers_auto),
        reserve_ram_gb=float(args.reserve_ram_gb),
        min_free_ram_gb=float(args.min_free_ram_gb),
        max_ram_per_worker_gb=args.max_ram_per_worker_gb,
    )
    if resolved_max_workers != args.max_workers:
        print(f"resolved_max_workers={resolved_max_workers} requested_max_workers={args.max_workers}", flush=True)

    experiment_root = Path(args.experiment_root)
    data_path, generated_overrides = resolve_parallel_grid_data_args(args, experiment_root)

    manifest_rows = read_manifest_csv(Path(args.manifest))
    if args.phase:
        manifest_rows = [row for row in manifest_rows if row["phase"] == args.phase]

    statuses, failures = run_hypothesis_manifest_in_parallel(
        manifest_rows=manifest_rows,
        experiment_root=experiment_root,
        config_path=Path(args.config),
        local_config=Path(args.local_config) if args.local_config else None,
        data_path=data_path,
        max_workers=resolved_max_workers,
        skip_completed=bool(args.skip_completed),
        override_paths=[Path(p) for p in args.override] + generated_overrides,
        dry_run=bool(args.dry_run),
        run_timeout_seconds=args.run_timeout_seconds,
        min_free_ram_gb=float(args.min_free_ram_gb),
        fail_fast=bool(args.fail_fast),
        resume_strict=bool(args.resume_strict),
    )
    print(f"runs_total={len(statuses)}")
    print(f"runs_failed={len(failures)}")
    return 1 if failures and not args.dry_run else 0


def cli_run_parallel_grid(argv: list[str] | None = None) -> int:
    """Backward-compatible wrapper for legacy script argument names."""
    parser = argparse.ArgumentParser(description="Backward-compatible wrapper for run_parallel_grid.py")
    parser.add_argument("--experiment-root", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--base-config", required=True)
    parser.add_argument("--local-config")
    parser.add_argument("--data", required=True)
    parser.add_argument("--max-workers", type=int, default=6)
    parser.add_argument("--skip-completed", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    return cli_run_parallel_hypothesis_grid(
        [
            "--experiment-root",
            args.experiment_root,
            "--manifest",
            args.manifest,
            "--config",
            args.base_config,
            "--data",
            args.data,
            "--max-workers",
            str(args.max_workers),
            *( ["--local-config", args.local_config] if args.local_config else []),
            *( ["--skip-completed"] if args.skip_completed else []),
            *( ["--dry-run"] if args.dry_run else []),
        ]
    )


def cli_build_manifest(argv: list[str] | None = None) -> int:
    """Backward-compatible stub kept for legacy scripts."""
    parser = argparse.ArgumentParser(description="Deprecated strategy-specific manifest builder")
    parser.add_argument("--experiment-root", required=True)
    parser.add_argument("--strategy", required=True)
    args = parser.parse_args(argv)
    raise ValueError(
        f"Legacy --strategy flow is deprecated ({args.strategy!r}). Use scripts/build_hypothesis_grid.py --hypothesis ..."
    )
