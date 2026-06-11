#!/usr/bin/env python3
"""Run stable classic-vs-fast parity gates for all hypothesis contracts.

Each hypothesis/timeframe pair is compared independently so long matrix runs
can be resumed safely. A passed gate proves output parity for that family and
timeframe; it does not imply a speedup unless the recorded timing says so.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

import pandas as pd

from bt.hypotheses.contract import HypothesisContract


def _timeframes(path: Path) -> list[str]:
    contract = HypothesisContract.from_yaml(path)
    out = {
        str(spec.get("params", {}).get("signal_timeframe", spec.get("params", {}).get("timeframe", ""))).lower()
        for spec in contract.to_run_specs()
    }
    return sorted(tf for tf in out if tf)


def _strategy(path: Path) -> str:
    contract = HypothesisContract.from_yaml(path)
    return str(contract.schema.entry.get("strategy", ""))


def _run_gate(
    *,
    path: Path,
    tf: str,
    strategy: str,
    root: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    out = root / path.stem / tf
    end = _gate_end(args.start, args.end, tf, args.duration_by_timeframe_map)
    cmd = [
        sys.executable,
        "scripts/compare_strategy_family_kernel.py",
        "--hypothesis",
        str(path),
        "--config",
        args.config,
        "--local-config",
        args.local_config,
        "--data-root",
        args.data_root,
        "--exchange",
        args.exchange,
        "--timeframe",
        args.timeframe,
        "--stable-manifest",
        args.stable_manifest,
        "--start",
        args.start,
        "--end",
        end,
        "--signal-timeframes",
        tf,
        "--universes",
        "stable",
        "--path-modes",
        "classic,fast",
        "--output-root",
        str(out),
        "--max-workers",
        str(args.max_workers),
        "--run-timeout-seconds",
        str(args.run_timeout_seconds),
    ]
    if args.clean:
        cmd.append("--clean")
    print(f"running {path.name} strategy={strategy} timeframe={tf}", flush=True)
    proc = subprocess.run(cmd, text=True)
    pair_rows = _read_pairs(out)
    status = "passed" if proc.returncode == 0 and _passed(pair_rows, [tf]) else "failed"
    row = {
        "hypothesis": path.name,
        "strategy": strategy,
        "signal_timeframe": tf,
        "status": status,
        "returncode": proc.returncode,
        "output_root": str(out),
        "pairs": json.dumps(pair_rows, sort_keys=True, default=str),
    }
    if status != "passed":
        print(f"FAILED {path.name} timeframe={tf}; continuing to record all gates", flush=True)
    else:
        print(f"passed {path.name} timeframe={tf}", flush=True)
    return row


def _read_pairs(path: Path) -> list[dict[str, Any]]:
    pairs = path / "comparison_pairs.csv"
    if not pairs.exists() or pairs.stat().st_size == 0:
        return []
    return pd.read_csv(pairs).to_dict("records")


def _passed(rows: list[dict[str, Any]], expected_tfs: list[str]) -> bool:
    if not rows:
        return False
    by_tf = {str(row.get("signal_timeframe")): row for row in rows}
    for tf in expected_tfs:
        row = by_tf.get(tf)
        if row is None:
            return False
        if not bool(row.get("same_metrics")):
            return False
        if not bool(row.get("same_equity_csv")):
            return False
        if "same_trades_semantic_csv" in row and not bool(row.get("same_trades_semantic_csv")):
            return False
    return True


def _parse_duration_by_timeframe(raw: str) -> dict[str, pd.Timedelta]:
    out: dict[str, pd.Timedelta] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        key, sep, value = item.partition("=")
        if not sep:
            raise ValueError("--duration-by-timeframe entries must look like 15m=45m")
        out[key.strip().lower()] = pd.Timedelta(value.strip())
    return out


def _gate_end(start: str, default_end: str, tf: str, durations: dict[str, pd.Timedelta]) -> str:
    duration = durations.get(tf)
    if duration is None:
        return default_end
    return (pd.Timestamp(start) + duration).isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hypotheses-dir", default="research/hypotheses")
    parser.add_argument("--output-root", default="outputs/kernel_comparison/all_stable_fast_paths")
    parser.add_argument("--config", default="configs/engine.yaml")
    parser.add_argument("--local-config", default="configs/local/engine.lab.yaml")
    parser.add_argument("--data-root", default="research_data")
    parser.add_argument("--exchange", default="binance")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--stable-manifest", default="research_data/manifests/stable_universe.parquet")
    parser.add_argument("--start", default="2025-05-05")
    parser.add_argument("--end", default="2025-05-07")
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument(
        "--gate-workers",
        type=int,
        default=1,
        help="Number of independent hypothesis/timeframe parity gates to run concurrently.",
    )
    parser.add_argument("--run-timeout-seconds", type=int, default=3600)
    parser.add_argument(
        "--duration-by-timeframe",
        default="",
        help="Comma-separated parity durations by signal timeframe, e.g. 5m=45m,15m=45m,1h=6h.",
    )
    parser.add_argument("--only", default="", help="Comma-separated hypothesis YAML names to run")
    parser.add_argument("--clean", action="store_true")
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Reuse existing passed hypothesis/timeframe gates when present.",
    )
    args = parser.parse_args()
    args.duration_by_timeframe_map = _parse_duration_by_timeframe(args.duration_by_timeframe)

    root = Path(args.output_root)
    if args.clean and root.exists():
        import shutil

        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    selected = {item.strip() for item in args.only.split(",") if item.strip()}
    hypotheses = [
        path
        for path in sorted(Path(args.hypotheses_dir).glob("*.yaml"))
        if path.name != "sample_pipeline_smoke.yaml" and (not selected or path.name in selected)
    ]
    matrix_path = root / "stable_fast_path_matrix.csv"
    rows: list[dict[str, Any]] = []
    if args.resume and matrix_path.exists() and matrix_path.stat().st_size > 0:
        rows = pd.read_csv(matrix_path).to_dict("records")
    seen = {
        (str(row.get("hypothesis")), str(row.get("signal_timeframe"))): row
        for row in rows
        if str(row.get("status")) == "passed"
    }
    pending_gates: list[tuple[Path, str, str]] = []
    for path in hypotheses:
        tfs = _timeframes(path)
        strategy = _strategy(path)
        for tf in tfs:
            if args.resume and (path.name, tf) in seen:
                print(f"resume-skip passed {path.name} timeframe={tf}", flush=True)
                continue
            pending_gates.append((path, tf, strategy))

    write_lock = threading.Lock()

    def record_row(row: dict[str, Any]) -> None:
        nonlocal rows
        with write_lock:
            rows = [
                existing
                for existing in rows
                if not (
                    str(existing.get("hypothesis")) == str(row.get("hypothesis"))
                    and str(existing.get("signal_timeframe")) == str(row.get("signal_timeframe"))
                )
            ]
            rows.append(row)
            pd.DataFrame(rows).sort_values(["hypothesis", "signal_timeframe"]).to_csv(matrix_path, index=False)

    gate_workers = max(1, int(args.gate_workers))
    if gate_workers == 1:
        for path, tf, strategy in pending_gates:
            record_row(_run_gate(path=path, tf=tf, strategy=strategy, root=root, args=args))
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=gate_workers) as pool:
            futures = [
                pool.submit(_run_gate, path=path, tf=tf, strategy=strategy, root=root, args=args)
                for path, tf, strategy in pending_gates
            ]
            for future in concurrent.futures.as_completed(futures):
                record_row(future.result())
    failed = [row for row in rows if row["status"] != "passed"]
    print(f"matrix_root={root}")
    print(f"passed={len(rows)-len(failed)} failed={len(failed)} total={len(rows)}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
