#!/usr/bin/env python3
"""Compare classic versus compiled-feature strategy-family paths."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from bt.config import deep_merge, load_yaml
from bt.experiments.hypothesis_runner import execute_hypothesis_variant
from bt.hypotheses.contract import HypothesisContract
from bt.research_orchestration.data_profiles import resolve_data_profile, write_data_profile_config


@dataclass(frozen=True)
class ComparisonTask:
    path_mode: str
    universe: str
    signal_timeframe: str
    spec: dict[str, Any]
    data_profile_path: Path
    local_config_path: Path
    out_root: Path
    run_slug: str

    def to_json(self) -> dict[str, Any]:
        return {
            "path_mode": self.path_mode,
            "universe": self.universe,
            "signal_timeframe": self.signal_timeframe,
            "spec": self.spec,
            "data_profile_path": str(self.data_profile_path),
            "local_config_path": str(self.local_config_path),
            "out_root": str(self.out_root),
            "run_slug": self.run_slug,
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "ComparisonTask":
        return cls(
            path_mode=str(payload["path_mode"]),
            universe=str(payload["universe"]),
            signal_timeframe=str(payload["signal_timeframe"]),
            spec=dict(payload["spec"]),
            data_profile_path=Path(payload["data_profile_path"]),
            local_config_path=Path(payload["local_config_path"]),
            out_root=Path(payload["out_root"]),
            run_slug=str(payload["run_slug"]),
        )


def _load_yaml_or_empty(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    return load_yaml(path)


def _write_yaml(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _spec_for_timeframe(contract: HypothesisContract, signal_timeframe: str) -> dict[str, Any]:
    wanted = str(signal_timeframe).lower()
    for spec in contract.to_run_specs():
        params = spec.get("params", {})
        tf = str(params.get("signal_timeframe", params.get("timeframe", ""))).lower()
        if tf == wanted:
            return spec
    raise ValueError(f"No grid spec found with signal_timeframe={signal_timeframe}")


def _make_local_config(
    *,
    base_local: dict[str, Any],
    start: str,
    end: str,
    path_mode: str,
) -> dict[str, Any]:
    cfg = dict(base_local)
    cfg = deep_merge(
        cfg,
        {
            "execution_engine": "classic" if path_mode == "classic" else "auto",
            "data": {
                "date_range": {"start": start, "end": end},
            },
        },
    )
    return cfg


def _run_task(task: ComparisonTask, *, config_path: str, data_root: str, hypothesis_path: str) -> dict[str, Any]:
    contract = HypothesisContract.from_yaml(hypothesis_path)
    result = execute_hypothesis_variant(
        contract=contract,
        spec=task.spec,
        tier="Tier2",
        config_path=config_path,
        data_path=data_root,
        out_root=str(task.out_root),
        local_config=str(task.local_config_path),
        override_paths=[str(task.data_profile_path)],
        run_slug=task.run_slug,
    )
    run_dir = Path(str(result["run_dir"]))
    return {
        "path_mode": task.path_mode,
        "universe": task.universe,
        "signal_timeframe": task.signal_timeframe,
        "run_dir": str(run_dir),
        "status": "completed",
        "performance": _load_json(run_dir / "performance.json"),
        "timing": _load_json(run_dir / "run_timing.json"),
        "fast_path_status": _load_json(run_dir / "fast_path_status.json"),
        "trades_hash": _sha256(run_dir / "trades.csv"),
        "trades_semantic_hash": _semantic_csv_hash(
            run_dir / "trades.csv",
            ignore_columns={"run_id", "identity_run_id"},
        ),
        "equity_hash": _sha256(run_dir / "equity.csv"),
    }


def _run_worker_payload(worker_task: Path) -> int:
    payload = json.loads(worker_task.read_text(encoding="utf-8"))
    task = ComparisonTask.from_json(payload["task"])
    result_path = Path(payload["result_path"])
    try:
        result = _run_task(
            task,
            config_path=str(payload["config_path"]),
            data_root=str(payload["data_root"]),
            hypothesis_path=str(payload["hypothesis_path"]),
        )
    except Exception as exc:
        result = {
            "path_mode": task.path_mode,
            "universe": task.universe,
            "signal_timeframe": task.signal_timeframe,
            "run_dir": "",
            "status": "failed",
            "error": str(exc),
            "performance": {},
            "timing": {},
            "fast_path_status": {},
            "trades_hash": "",
            "trades_semantic_hash": "",
            "equity_hash": "",
        }
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    return 0 if result.get("status") == "completed" else 1


def _run_tasks_subprocess(
    tasks: list[ComparisonTask],
    *,
    config_path: str,
    data_root: str,
    hypothesis_path: str,
    root: Path,
    max_workers: int,
    timeout_seconds: int | None,
) -> list[dict[str, Any]]:
    work_dir = root / "comparison_worker_payloads"
    log_dir = root / "comparison_worker_logs"
    work_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    pending = list(tasks)
    running: dict[subprocess.Popen[str], tuple[ComparisonTask, Path, Any, Any, float]] = {}
    results: list[dict[str, Any]] = []
    script_path = Path(__file__).resolve()
    last_heartbeat = 0.0

    def launch(task: ComparisonTask) -> None:
        safe_slug = task.run_slug.replace("/", "_")
        payload_path = work_dir / f"{safe_slug}.json"
        result_path = work_dir / f"{safe_slug}.result.json"
        stdout = (log_dir / f"{safe_slug}.stdout.log").open("w", encoding="utf-8")
        stderr = (log_dir / f"{safe_slug}.stderr.log").open("w", encoding="utf-8")
        payload_path.write_text(
            json.dumps(
                {
                    "task": task.to_json(),
                    "config_path": config_path,
                    "data_root": data_root,
                    "hypothesis_path": hypothesis_path,
                    "result_path": str(result_path),
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        cmd = [sys.executable, str(script_path), "--worker-task", str(payload_path)]
        proc = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, text=True)
        running[proc] = (task, result_path, stdout, stderr, time.monotonic())

    max_workers = max(1, int(max_workers))
    while pending or running:
        while pending and len(running) < max_workers:
            launch(pending.pop(0))
        now = time.monotonic()
        if running and now - last_heartbeat >= 15.0:
            details = []
            for _proc, (task, _result_path, _stdout, _stderr, started) in running.items():
                details.append(f"{task.path_mode}:{task.universe}:{task.signal_timeframe}:{int(now-started)}s")
            print(
                "comparison heartbeat "
                f"running={len(running)} pending={len(pending)} "
                + ",".join(details),
                flush=True,
            )
            last_heartbeat = now
        for proc, (task, result_path, stdout, stderr, started) in list(running.items()):
            rc = proc.poll()
            if rc is None and timeout_seconds is not None and timeout_seconds > 0:
                if time.monotonic() - started > timeout_seconds:
                    proc.terminate()
                    try:
                        proc.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    rc = proc.returncode if proc.returncode is not None else -9
            if rc is None:
                continue
            stdout.close()
            stderr.close()
            running.pop(proc, None)
            if result_path.exists():
                result = json.loads(result_path.read_text(encoding="utf-8"))
            else:
                result = {
                    "path_mode": task.path_mode,
                    "universe": task.universe,
                    "signal_timeframe": task.signal_timeframe,
                    "run_dir": "",
                    "status": "failed",
                    "error": f"worker exited rc={rc} without result file",
                    "performance": {},
                    "timing": {},
                    "fast_path_status": {},
                    "trades_hash": "",
                    "trades_semantic_hash": "",
                    "equity_hash": "",
                }
            results.append(result)
            print(f"{result['status']} {task.path_mode} {task.universe} {task.signal_timeframe}", flush=True)
        if running:
            time.sleep(2.0)
    return results


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256(path: Path) -> str:
    if not path.exists():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _semantic_csv_hash(path: Path, *, ignore_columns: set[str] | None = None) -> str:
    if not path.exists():
        return ""
    frame = pd.read_csv(path)
    for column in sorted(ignore_columns or set()):
        if column in frame.columns:
            frame = frame.drop(columns=[column])
    ordered = frame.reindex(columns=sorted(frame.columns))
    payload = ordered.to_csv(index=False, lineterminator="\n")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _stage_seconds(timing: dict[str, Any], stage: str) -> float:
    for event in timing.get("events", []):
        if event.get("stage") == stage:
            return float(event.get("seconds") or 0.0)
    return 0.0


def _metric(payload: dict[str, Any], key: str) -> float | str | None:
    value = payload.get(key)
    if isinstance(value, (int, float)):
        return float(value)
    return value


def _same_number(a: Any, b: Any, *, tol: float) -> bool:
    try:
        fa = float(a)
        fb = float(b)
    except (TypeError, ValueError):
        return a == b
    if math.isnan(fa) and math.isnan(fb):
        return True
    return abs(fa - fb) <= tol


def _build_comparison(results: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    run_rows: list[dict[str, Any]] = []
    for result in results:
        perf = result.get("performance", {})
        timing = result.get("timing", {})
        run_rows.append(
            {
                "path_mode": result["path_mode"],
                "universe": result["universe"],
                "signal_timeframe": result["signal_timeframe"],
                "run_dir": result["run_dir"],
                "fast_mode": result.get("fast_path_status", {}).get("mode", ""),
                "total_seconds": timing.get("total_seconds", ""),
                "engine_run_seconds": _stage_seconds(timing, "engine.run"),
                "data_load_seconds": _stage_seconds(timing, "data.load"),
                "total_trades": perf.get("total_trades", ""),
                "final_equity": perf.get("final_equity", ""),
                "net_pnl": perf.get("net_pnl", ""),
                "ev_r_net": perf.get("ev_r_net", ""),
                "win_rate": perf.get("win_rate", ""),
                "trades_hash": result.get("trades_hash", ""),
                "trades_semantic_hash": result.get("trades_semantic_hash", ""),
                "equity_hash": result.get("equity_hash", ""),
            }
        )

    pair_rows: list[dict[str, Any]] = []
    by_key = {(r["path_mode"], r["universe"], r["signal_timeframe"]): r for r in run_rows}
    for universe in sorted({r["universe"] for r in run_rows}):
        for tf in sorted({r["signal_timeframe"] for r in run_rows}):
            classic = by_key.get(("classic", universe, tf))
            fast = by_key.get(("fast", universe, tf))
            if not classic or not fast:
                continue
            metric_keys = ("total_trades", "final_equity", "net_pnl", "ev_r_net", "win_rate")
            same_metrics = all(_same_number(classic.get(key), fast.get(key), tol=1e-9) for key in metric_keys)
            classic_engine = float(classic.get("engine_run_seconds") or 0.0)
            fast_engine = float(fast.get("engine_run_seconds") or 0.0)
            speedup = classic_engine / fast_engine if fast_engine > 0 else None
            pair_rows.append(
                {
                    "universe": universe,
                    "signal_timeframe": tf,
                    "same_metrics": same_metrics,
                    "same_trades_csv": classic.get("trades_hash") == fast.get("trades_hash"),
                    "same_trades_semantic_csv": classic.get("trades_semantic_hash") == fast.get("trades_semantic_hash"),
                    "same_equity_csv": classic.get("equity_hash") == fast.get("equity_hash"),
                    "classic_engine_seconds": classic_engine,
                    "fast_engine_seconds": fast_engine,
                    "engine_speedup": speedup,
                    "classic_total_seconds": classic.get("total_seconds"),
                    "fast_total_seconds": fast.get("total_seconds"),
                }
            )
    return run_rows, pair_rows


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare classic and compiled strategy-family paths")
    parser.add_argument("--worker-task", help=argparse.SUPPRESS)
    parser.add_argument("--hypothesis", default="research/hypotheses/l7_h1_csi_gated_displacement_trend.yaml")
    parser.add_argument("--config", default="configs/engine.yaml")
    parser.add_argument("--local-config", default="configs/local/engine.lab.yaml")
    parser.add_argument("--data-root", default="research_data")
    parser.add_argument("--exchange", default="binance")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--membership-path", default="research_data/manifests/volatile_universe_membership.parquet")
    parser.add_argument("--stable-manifest", default="research_data/manifests/stable_universe.parquet")
    parser.add_argument("--start", default="2025-05-05")
    parser.add_argument("--end", default="2025-05-19")
    parser.add_argument("--signal-timeframes", default="15m,1h")
    parser.add_argument("--universes", default="stable,volatile")
    parser.add_argument("--path-modes", default="classic,fast")
    parser.add_argument("--output-root", default="outputs/kernel_comparison/l7_h1_2w")
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--run-timeout-seconds", type=int, default=0)
    parser.add_argument("--clean", action="store_true")
    args = parser.parse_args()
    if args.worker_task:
        return _run_worker_payload(Path(args.worker_task))

    root = Path(args.output_root)
    if args.clean and root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    contract = HypothesisContract.from_yaml(args.hypothesis)
    base_local = _load_yaml_or_empty(Path(args.local_config))
    signal_timeframes = [item.strip().lower() for item in args.signal_timeframes.split(",") if item.strip()]
    universes = [item.strip().lower() for item in args.universes.split(",") if item.strip()]
    path_modes = [item.strip().lower() for item in args.path_modes.split(",") if item.strip()]
    for universe in universes:
        if universe not in {"stable", "volatile"}:
            raise ValueError("--universes may contain only stable,volatile")
    for path_mode in path_modes:
        if path_mode not in {"classic", "fast"}:
            raise ValueError("--path-modes may contain only classic,fast")

    profile_paths: dict[str, Path] = {}
    for universe in universes:
        profile = resolve_data_profile(
            universe=universe,
            data_root=args.data_root,
            data_kind="research_panel",
            exchange=args.exchange,
            timeframe=args.timeframe,
            stable_manifest=args.stable_manifest,
            membership_path=args.membership_path,
        )
        profile_paths[universe] = write_data_profile_config(profile, root / "profiles" / f"{universe}.yaml")

    tasks: list[ComparisonTask] = []
    for path_mode in path_modes:
        local_cfg = _make_local_config(base_local=base_local, start=args.start, end=args.end, path_mode=path_mode)
        local_path = _write_yaml(root / "overrides" / f"{path_mode}.yaml", local_cfg)
        for universe in universes:
            for tf in signal_timeframes:
                spec = _spec_for_timeframe(contract, tf)
                spec = dict(spec)
                spec["params"] = dict(spec.get("params", {}))
                spec["params"]["use_compiled_features"] = path_mode == "fast"
                spec["params"]["use_compiled_event_kernel"] = path_mode == "fast"
                run_slug = f"{path_mode}__{universe}__{tf}__{spec['grid_id']}__tier2"
                tasks.append(
                    ComparisonTask(
                        path_mode=path_mode,
                        universe=universe,
                        signal_timeframe=tf,
                        spec=spec,
                        data_profile_path=profile_paths[universe],
                        local_config_path=local_path,
                        out_root=root / "runs",
                        run_slug=run_slug,
                    )
                )

    results = _run_tasks_subprocess(
        tasks,
        config_path=args.config,
        data_root=args.data_root,
        hypothesis_path=args.hypothesis,
        root=root,
        max_workers=args.max_workers,
        timeout_seconds=args.run_timeout_seconds if args.run_timeout_seconds > 0 else None,
    )

    run_rows, pair_rows = _build_comparison(results)
    _write_csv(root / "comparison_runs.csv", run_rows)
    _write_csv(root / "comparison_pairs.csv", pair_rows)
    (root / "comparison_results.json").write_text(
        json.dumps({"runs": run_rows, "pairs": pair_rows, "raw": results}, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    all_completed = all(r.get("status") == "completed" for r in results)
    all_same = bool(pair_rows) and all(bool(r.get("same_metrics")) for r in pair_rows)
    print(f"comparison_root={root}")
    print(f"all_completed={all_completed} all_metric_pairs_same={all_same}")
    return 0 if all_completed else 1


if __name__ == "__main__":
    raise SystemExit(main())
